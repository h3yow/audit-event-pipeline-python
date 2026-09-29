# 本地审计事件流水线（采集 · 转发 · 合规留存）

用本地存储模拟事件来源与下游接收方，实现审计事件的**归一化采集、规则分发、
失败重试、幂等去重、版本化留存与清理留痕**。无外部服务依赖，sqlite + 本地
JSONL 文件即可完整运行与验证。

## 快速开始

```bash
uv sync --group dev

# 运行全部单测（日志中会打印原始事件与判定依据）
uv run pytest -q

# 端到端演示：采集 -> 故障/重试 -> 重复投递 -> 策略变更 -> 清理留痕
uv run python demo.py --data-dir ./demo_data
```

## 目录结构

```
audit_pipeline/
├── models.py            # 统一审计事件模型、拒绝原因、投递状态等共享类型
├── source_adapters.py   # 异构来源适配器（web_app / gateway 两种格式）
├── normalizer.py        # 归一化入口，按来源分派，拒绝原因可区分
├── storage.py           # sqlite 持久化（事件/拒绝/投递/策略版本/清理痕迹）
├── sinks.py             # 下游接收方（本地 JSONL 模拟，支持故障注入）
├── dispatcher.py        # 规则路由、按 sink 去重、缓冲重试
├── retention.py         # 版本化留存策略、可复现判定、清理留痕
├── pipeline.py          # 门面：串联采集→存储→分发→留存
├── logging_setup.py     # 日志配置（INFO 打印原始事件与判定依据）
└── errors.py            # 自定义异常
tests/                   # 23 个单测，覆盖四类关键场景
demo.py                  # 本地端到端演示
```

## 统一事件模型

异构来源经适配器归一为 `AuditEvent`（`audit_pipeline/models.py`）：

| 字段 | 含义 |
| --- | --- |
| `event_id` | 全局唯一事件 ID，幂等去重的键 |
| `source` | 来源标识（`web_app` / `gateway`） |
| `category` | 统一大类：`login` / `data_access` / `permission_change` / `system_op` / `unknown` |
| `severity` | `info` / `warning` / `critical` |
| `actor` / `action` | 操作主体与动作 |
| `occurred_at` | 事件发生时间（UTC，留存判定锚点） |
| `received_at` | 流水线接收时间（UTC） |
| `raw` | 原始 payload，原样保留，便于追溯 |

内置两种来源格式：

- **web_app**：snake_case，`{event_id,type,user,ts(ISO8601),severity}`，
  `type` 如 `user.login` / `data.export` / `role.grant`；
- **gateway**：异形字段，`{id,event_type,subject,op,@timestamp("YYYY/MM/DD HH:MM:SS"),level}`，
  枚举为大写（`DATA_EXPORT` / `CRITICAL`）。

### 拒绝原因（不静默丢弃，按原因区分）

| RejectCode | 触发条件 |
| --- | --- |
| `unknown_source` | 来源未在归一化器注册 |
| `missing_field` | 必填字段缺失（如无 `event_id`） |
| `invalid_format` | 字段类型/取值非法（如 user 是数字、severity 不可识别、payload 不是对象） |
| `illegal_timestamp` | 时间字段存在但无法解析 |
| `unknown_category` | 无法映射到统一类别 |

被拒事件完整写入 `rejected_events` 表（含原始 payload、原因、详情、时间）。

## 分发规则

- **路由**：`RoutingRule(name, categories, min_severity)`，事件类别命中且
  严重级别达标才投递给该 sink。内置示例：`siem` 收全量；`alerter` 只收
  `system_op` / `permission_change`（可设最低级别）。
- **下游模拟**：`LocalFileSink` 以 JSONL 追加写，维护送达计数、已送达
  event_id 集合与 CRITICAL 告警计数。
- **失败缓冲与重试**：sink 抛 `SinkUnavailableError` 时状态置 `pending`
  （`failure_code=sink_unavailable`），事件保留；`retry_pending()` 重试。
  达到 `max_attempts`（默认 3）置 `failed`（`retry_exhausted`），
  事件与最后错误均保留，绝不静默丢失。
- **幂等去重**：投递状态以 `(event_id, sink)` 持久化。已 `delivered` 的
  组合再次收到同一事件直接跳过——下游**不重复计数、不重复告警**。
- 故障注入：`sink.set_available(False)` 持续不可用；`sink.fail_times(n)`
  模拟 n 次瞬时故障后自动恢复。

## 留存与清理

- **策略版本化**：`RetentionPolicy(version, retention_days, default_days)`，
  版本号必须单调递增，历史版本不可覆盖。不同类别保留天数不同，未覆盖类别
  用 `default_days`。
- **判定可复现（关键设计）**：事件**入库时**绑定当时的最新策略版本
  （`events.policy_version`）。此后无论策略如何修改（延长或缩短），该事件
  的过期判定始终使用其绑定版本的规则。因此同一时刻、同年龄但绑定不同版本
  的事件可以有不同判定——这正是"策略改过之后对已有事件的判定可复现"。
  存量未绑定版本的事件回退到最新策略，并在日志中显式说明。
- **清理留痕**：清理时先向 `cleanup_audit` 写一条记录（事件 id、来源、类别、
  发生时间、绑定策略版本、具体判定依据、删除时间），再将事件**软删除**
  （`retained=0`）。事件物理行仍在库中，任何清理动作都可查；重复清理不会
  对同一事件重复留痕。

## 存储表（sqlite）

| 表 | 作用 |
| --- | --- |
| `events` | 已接受事件，`event_id` 主键幂等，含 `policy_version` 与 `retained` |
| `rejected_events` | 被拒事件及区分化原因 |
| `delivery_state` | `(event_id, sink)` 投递状态/尝试次数/失败码 |
| `retention_policies` | 策略版本与规则 JSON |
| `cleanup_audit` | 清理动作痕迹 |

## 如何验证（测试场景对应关系）

| 场景 | 测试 |
| --- | --- |
| 重复投递不重复计数/告警 | `tests/test_dispatcher.py::test_duplicate_delivery_not_double_counted_or_alerted`、`tests/test_pipeline.py::test_end_to_end_duplicate_delivery` |
| 下游暂时不可用→缓冲→重试成功 | `tests/test_dispatcher.py::test_sink_outage_buffered_then_retried_and_succeeds` |
| 重试耗尽标记 failed 但事件保留 | `tests/test_dispatcher.py::test_retry_exhausted_marked_failed_but_event_retained` |
| 留存策略变更后判定可复现 | `tests/test_retention.py::test_policy_change_does_not_change_existing_events_judgement` |
| 分类别保留时长 + 清理留痕 | `tests/test_retention.py::test_category_specific_retention_differ`、`test_cleanup_soft_deletes_and_leaves_audit_trail` |
| 拒绝原因可区分 | `tests/test_normalizer.py::test_missing_field_distinct_from_bad_format`、`tests/test_pipeline.py::test_end_to_end_invalid_events_distinguished_and_archived` |
| 日志含原始事件与判定依据 | 各模块的 `test_logs_*` / `test_expiry_logs_raw_event_and_basis` |

```bash
# 查看带日志的详细判定过程（原始事件、命中规则、绑定策略版本等）
uv run pytest -q --log-cli-level=INFO
```

更多设计决策见 [docs/DESIGN.md](docs/DESIGN.md)。
