Status: open
Label: ready-for-agent
Assignee: unassigned

# SDB-NQ-02 — 建立持续用量账本、epoch 与 datastore 格式围栏

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 建立受保护的 `QuotaUsageMeta`、active/staged epoch、table rule bucket、per-table field/record counter keys 与统一 quota transaction facade。
- 新 database 以 ready 空账本开始；无策略 database 仍持续计量但不限额。
- 实现 `uninitialized → rebuilding → ready` 与 `corrupt` 持久状态、database maintenance read-only fence 和 staged epoch 原子切换。
- 建立 fork-required storage version 与结构化 format marker；普通启动只检查，不静默迁移。
- 明确 database 删除、raw restore、前缀复制和离线维护后的 dirty/rebuild 行为。

## Acceptance criteria

- [ ] counter 与业务 KV 可在同一事务/savepoint 中原子提交或回滚。
- [ ] staged rebuild 失败或进程崩溃后不会把未校验 epoch 设为 active，也不会开放写入。
- [ ] database 删除清理全部 policy/usage/epoch keys；raw restore 必须重建。
- [ ] frozen key/revision fixtures 与 format compatibility tests 齐全。
- [ ] vanilla/旧 fork 对 fork-required marker 拒绝启动。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md) 的 policy/format shape
- Blocks: [`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)
