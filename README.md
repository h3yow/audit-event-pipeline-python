# 本地审计事件采集、转发与合规留存流水线

用本地文件（JSONL）模拟事件来源与下游接收方，实现审计事件的
**采集归一化 → 入库 → 规则分发（去重 + 故障缓冲重试）→ 留存清理（全程留痕）**，
保证在重复投递、下游暂时不可用、留存策略调整之后，系统状态一致且每一步可解释、可复现。

## 运行

```bash
uv sync                  # 安装依赖
uv run python main.py    # 端到端演示，状态写入 ./data/*.jsonl
uv run pytest            # 全部测试（单测日志实时打印原始事件与判定依据）
```

## 统一事件模型（audit_pipeline/models.py）

不同来源的原始事件统一归一化为 `AuditEvent`：

| 字段 | 类型 | 说明 |
|---|---|---|
| `event_id` | str | 全局唯一 ID，去重与留存判定的主键 |
| `timestamp` | datetime | 事件时间（ISO 8601 或 epoch，naive 视为 UTC） |
| `source` | str | 来源格式标识：`app-json` / `legacy` / `syslog` |
| `category` | EventCategory | `security` / `access` / `system` / `data` |
| `actor` / `action` | str | 操作者与操作 |
| `detail` | dict | 附加字段 |

## 采集与拒绝（audit_pipeline/ingestion.py）

- 三种来源格式各有字段映射表（如 `legacy` 的 `uid/who/op/klass` → 统一字段），
  `syslog` 为 `k=v` 键值对文本。
- 不合法事件**绝不静默丢弃**：抛出 `EventRejected` 并落入拒件库
  `dead_letters.jsonl`（含原始报文），拒绝原因可区分：
  - `missing_field` 必填字段缺失（指明缺哪个）
  - `invalid_format` 原始报文无法解析
  - `invalid_field_type` 字段类型/取值不合法
  - `unknown_category` 类别无法映射
  - `invalid_timestamp` 时间戳缺失或不可解析

## 分发与可靠性（audit_pipeline/dispatch.py）

- **路由**：`RoutingRule` 按类别把事件发往一个或多个下游
  （如 security → security+ops，access → security）。
- **去重**：以 `(下游, event_id)` 为准，下游已含该事件即跳过，
  重复投递不会重复计数或重复告警；`DispatchResult.deduplicated` 标记整体去重。
- **故障缓冲**：下游抛 `SinkUnavailable` 时事件进入本地缓冲 `buffer.jsonl`
  （落盘，重启不丢），`flush_buffer()` 在下游恢复后重试；
  缓冲内同样按 `(下游, event_id)` 去重，补投不会产生重复。

## 留存与清理（audit_pipeline/retention.py）

- `RetentionPolicy` 按类别配置保留天数（示例：security 365 / data 180 /
  access 90 / system 30），策略带 `version`。
- `evaluate_event(event, policy, now)` 是**纯函数**：策略调整后，
  用旧版本策略对同一事件重新求值即可复现当时判定；
  策略未覆盖的类别默认保留（保守不删）。
- `run_retention` 对事件库执行清理：**每条事件的判定（保留或清理）
  和每次运行汇总都写入 `purge_log.jsonl`**，包含策略版本、事件 ID、
  类别与判定依据（保留天数、已存天数），绝不无记录删除。

## 测试与验证（tests/）

```bash
uv run pytest -v        # 日志中可见每条原始事件与判定依据
```

- `test_ingestion.py`：三种来源归一化；五种拒绝原因逐一区分；拒件留档原始报文。
- `test_dispatch.py`：按类别路由；**重复投递**去重不重复计数；
  **下游异常**时缓冲、恢复后重试且只补投一次；缓冲落盘可跨重启恢复。
- `test_retention.py`：分类保留时长判定；**策略变更**后新旧版本判定各自可复现；
  清理后事件库状态与 `purge_log.jsonl` 留痕一致。

## 目录结构

```
audit_pipeline/
  models.py      # 统一事件模型、拒绝原因枚举、序列化
  ingestion.py   # 多来源归一化与校验（拒绝原因可区分）
  dispatch.py    # 路由规则、本地下游、去重、缓冲重试
  retention.py   # 留存策略（版本化）、判定与清理留痕
  storage.py     # JSONL 事件库 / 拒件库 / 清理日志
  pipeline.py    # 采集→入库→分发 端到端组装
main.py          # 本地演示脚本
tests/           # pytest 测试（log_cli 实时打印判定依据）
```
