Status: open
Label: ready-for-agent
Assignee: unassigned

# SDB-NQ-05 — 交付 INFO、REBUILD、结构化错误与观测

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 实现专用 quota INFO 文本/STRUCTURE、轻量 database INFO 摘要，以及 legacy/streaming 两条执行路径。
- 实现同步 REBUILD QUOTA/IF NEEDED：取得 maintenance fence，扫描一致快照，写 staged epoch，校验并原子切换。
- 定义稳定 `format_version` DTO，包含 policy、ledger、usage、limit origin、unmatched、remaining/exceeded。
- 所有 quota errors 经 core/server HTTP/WebSocket/Rust SDK 保留 `code/retryable/details`；超限 violation 规范排序并裁剪敏感信息。
- 增加 operation result、审计事件、结构化日志与低基数 metrics；普通 export 继续排除 quota。

## Acceptance criteria

- [ ] INFO 文本可 parser 往返，STRUCTURE 排序稳定且不暴露内部 KV。
- [ ] database Owner 可读但不能 REBUILD；root/ns Owner 可读和重建。
- [ ] corrupt/rebuilding/unknown 账本不返回旧 usage 充当真值。
- [ ] REBUILD 扫描值与独立 table/field/record 扫描一致，崩溃恢复和 IF NEEDED no-op 测试通过。
- [ ] HTTP、WebSocket、Rust SDK 与目标 surrealdb-js fixtures 保留结构化错误，无字符串解析。
- [ ] metrics 无 namespace/database/table/rule 高基数 label，日志无记录内容或完整 query。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md)、[`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)、[`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)
- Blocks: [`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)、[`surreal_ck：实现 NativeQuotaClient、reconciler 与四类恢复循环`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/04-native-client-reconciler-sweeps.md)
