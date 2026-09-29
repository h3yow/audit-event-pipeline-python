# 设计说明：状态一致性与可追溯性

本文补充 README，集中说明三个核心问题：重复投递为什么不会重复计数、下游
故障时事件为什么不会丢、策略修改后判定为什么能复现。

## 1. 总体流程

```
本地 spool.jsonl ──┐
（两种异构格式）    │
                   ▼
            ┌─────────────┐   拒绝(5 种原因) ──► rejected_events（原样留档）
            │ Normalizer  │
            └─────┬───────┘
                  │ accepted (AuditEvent)
                  ▼
            ┌─────────────┐ event_id 幂等 upsert，绑定当前策略版本
            │ LocalStorage│
            └─────┬───────┘
                  ▼
            ┌─────────────┐  按类别+严重级别匹配 RoutingRule
            │ Dispatcher  │◄─► delivery_state(event_id,sink)
            └─────┬───────┘
                  │ deliver
                  ▼
        ┌────────┴────────┐
        ▼                 ▼
   siem.jsonl       alerter.jsonl   （本地文件模拟下游）

RetentionService:
  RetentionPolicy v1, v2, ... ──► retention_policies
  事件绑版本判定 ──► 先写 cleanup_audit ──► 软删除(retained=0)
```

## 2. 幂等与去重

幂等分两层：

1. **事件层**：`events.event_id` 主键，`INSERT OR IGNORE`。重复 `ingest`
   返回 `duplicate`，不重复分发。
2. **投递层**：`delivery_state` 主键 `(event_id, sink)`。即便绕过事件层
   直接调用 `dispatcher.dispatch`，已 `delivered` 的组合也会被跳过，
   尝试次数不增加。这保证下游文件中的记录、计数、CRITICAL 告警均恰好一次
   （at-least-once 投递 + 接收侧幂等状态 = 有效一次）。

## 3. 故障与重试的状态机

```
(无记录) ──deliver成功──► delivered（终态，重复投递跳过）
   │
   └──SinkUnavailable──► pending ──retry成功──► delivered
                            │
                            └──attempts==max──► failed(retry_exhausted)
```

- `pending`：下游暂时写不进去，事件在 `events`、状态在 `delivery_state`，
  随时可由 `retry_pending()` 重放；
- `failed`：不是丢弃，而是"当前无法送达"的显式终态，保留最后错误，
  运维可在下游恢复后人工重置或重新入队；
- 瞬时故障用 `fail_times(n)` 模拟，持续故障用 `set_available(False)`。

## 4. 留存判定的时间点一致性

- 事件时间：以 `occurred_at` 为锚点（业务发生时间），而非清理执行时间；
  时钟通过 `clock: Callable[[], datetime]` 注入，测试中固定为常量，判定
  结果完全确定、可复现。
- 规则时间：以事件入库时的策略版本为准。策略表只追加、不修改（版本单调
  递增），因此任何历史时刻对某事件的判定都能用 `(occurred_at,
  policy_version)` 重新算出相同结果。
- 若业务上确实需要"新策略立即对存量生效"，应登记新版本后显式做一次
  数据迁移并留痕；本实现默认选择**不溯及既往**，以保证审计语义稳定。

## 5. 删除的合规约束

合规场景下"无记录地删除"是被禁止的。`cleanup_expired` 的固定顺序：

1. 逐条按绑定版本判定是否过期，并打 INFO 日志（含原始事件、保留天数、
   截止线、结论）；
2. 过期事件先 `INSERT cleanup_audit`（含判定依据文本）；
3. 再 `UPDATE events SET retained=0`（软删除，行保留可追溯）；
4. `list_events()` 默认只返回留存中的事件。

## 6. 可观测性

`audit_pipeline` 命名空间的 logger 在每个判定点输出：

- 归一化：`原始事件=...`、`拒绝原因=...` / `category=... -> 接受`；
- 分发：`命中下游=[...]`、`第 N 次写入失败(...)`、`重复投递跳过`；
- 留存：`绑定策略 vN`、`保留天数=D 截止线=... -> 过期/保留`；
- 清理：`清理动作 event_id ... 已清理并留痕`。

`pytest --log-cli-level=INFO` 即可在单测输出中核对原始事件与判定依据。
