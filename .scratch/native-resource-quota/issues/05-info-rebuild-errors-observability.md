Status: done
Label: done
Assignee: /root

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

- [x] INFO 文本可 parser 往返，STRUCTURE 排序稳定且不暴露内部 KV。
- [x] database Owner 可读但不能 REBUILD；root/ns Owner 可读和重建。
- [x] corrupt/rebuilding/unknown 账本不返回旧 usage 充当真值。
- [x] REBUILD 扫描值与独立 table/field/record 扫描一致，崩溃恢复和 IF NEEDED no-op 测试通过。
- [x] HTTP、WebSocket、Rust SDK 与目标 surrealdb-js fixtures 保留结构化错误，无字符串解析。
- [x] metrics 无 namespace/database/table/rule 高基数 label，日志无记录内容或完整 query。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md)、[`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)、[`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)
- Blocks: [`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)、[`surreal_ck：实现 NativeQuotaClient、reconciler 与四类恢复循环`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/04-native-client-reconciler-sweeps.md)

## Comments

- 2026-07-25：新增 `INFO FOR QUOTA ON DATABASE ... [STRUCTURE]`，legacy/streaming executor 共用稳定 `format_version = 1` DTO；输出 policy、ledger、usage、limit origin、latest change，按规则和表名规范排序，账本非 ready 时隐藏 usage。
- 2026-07-25：新增 `REBUILD QUOTA [IF NEEDED] ON DATABASE ...`，仅 root/ns Owner 可执行；以独立事务提交 maintenance fence、扫描一致快照、暂存并校验新 epoch、原子激活，旧 epoch 仅在激活后尽力清理。崩溃恢复和 ready no-op 均有回归测试。
- 2026-07-25：公开 `QuotaError { code, retryable, details }`，HTTP/WebSocket `QueryResult` 与 Rust 类型均完成序列化往返；超限违规按 resource/table/rule 稳定排序并限制为 64 项，事务竞争映射为可重试 `quota_conflict`。
- 2026-07-25：DEFINE/ALTER/REMOVE/REBUILD 返回稳定 operation result，策略变更在业务提交后才发送审计事件；低基数 metrics 仅含 operation/outcome，日志不含 query、记录数据或租户标识，普通 export 继续排除全部 quota 元数据。
- 2026-07-25：验证：quota 专项 64/64；server observe 22/22；core/server Clippy `-D warnings` 通过；核心库广域回归在 `RUST_MIN_STACK=16MiB` 且排除父提交已知索引 flake 后为 3628 passed / 9 ignored / 1 filtered。默认 2MiB 测试线程栈会使独立 IAM signin 用例栈溢出，该用例在 16MiB 下独立通过，与 quota 路径无关。
