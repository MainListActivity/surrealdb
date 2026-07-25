Status: done
Label: done
Assignee: /root

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

- [x] counter 与业务 KV 可在同一事务/savepoint 中原子提交或回滚。
- [x] staged rebuild 失败或进程崩溃后不会把未校验 epoch 设为 active，也不会开放写入。
- [x] database 删除清理全部 policy/usage/epoch keys；raw restore 必须重建。
- [x] frozen key/revision fixtures 与 format compatibility tests 齐全。
- [x] vanilla/旧 fork 对 fork-required marker 拒绝启动。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md) 的 policy/format shape
- Blocks: [`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)

## Comments

- 2026-07-25：完成 `QuotaUsageMeta` 状态机、active/staged epoch、table rule bucket 与 per-table field/record typed counters，以及与业务 KV 共用事务/savepoint 的 quota facade。counter、staged writer、policy mutation 与 maintenance 状态切换通过条件写形成提交级冲突；staged epoch 以可信完整快照分批复核后才可切换。
- 2026-07-25：新 datastore 原子写入 high-bit fork-required storage version 与结构化 `!vf` marker；旧/vanilla、缺失/损坏/更新格式和未完成迁移均 fail-closed，服务端永久兼容错误不进入启动重试。raw restore/前缀复制契约要求先提交 maintenance fence，之后只能通过可信 rebuild 恢复。
- 2026-07-25：database 延迟回收覆盖 policy、usage 和全部 epoch keys；动态 `USE` target 的嵌套写不能绕过 maintenance fence。验证：`surrealdb-core` 全量 2,912 passed / 9 ignored；usage 13/13；storage format 8/8；reclaim 1/1；server startup retry 1/1；Clippy `-D warnings` 通过。
- 2026-07-25：本票交付账本/围栏基础设施。table/field 与 record 的真实 mutation entry wiring、无策略持续计量分别由 SDB-NQ-03/04 接入；一致扫描、在途事务排空与 `REBUILD QUOTA` 命令由 SDB-NQ-05 完成。
