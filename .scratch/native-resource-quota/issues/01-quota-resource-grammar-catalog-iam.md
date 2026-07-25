Status: done
Label: done
Assignee: /root

# SDB-NQ-01 — 建立 QUOTA grammar、catalog 与父层 IAM

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 按已锁定 grammar 接入 DEFINE/ALTER/REMOVE QUOTA、两层 AST、双向转换、visitor、`ToSql` 与 parser 往返。
- 建立 database-scoped singleton `QuotaPolicyDefinition` revisioned catalog value、typed KV key、category、provider、transaction cache 与失效。
- 实现完整快照、稳定 rule id、generation guard、OVERWRITE/IF EXISTS/no-op 语义和 regex 定义时编译校验。
- 新增独立 IAM `ResourceKind`；修改要求 root/目标 namespace Owner，database Owner/Editor/RECORD 身份拒绝，查看权与修改权分开。
- 配额不得进入普通 database export/import。

## Acceptance criteria

- [x] DEFINE/ALTER/REMOVE 的正反 parser、格式化和 AST revision 测试齐全。
- [x] catalog frozen bytes、revision compatibility、typed key 编码和 cache lookup 测试齐全。
- [x] 相同规范化策略是 no-op 且不推进 generation；冲突 generation 返回结构化错误。
- [x] root/ns Owner 成功，namespace 越界、namespace Editor、database Owner 和 RECORD 身份全部被拒。
- [x] database Owner 的 import 无法夹带 quota DDL。

## Dependencies

- Blocked by: none
- Blocks: [`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)、[`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)

## Comments

- 2026-07-25：完成 grammar、两层 AST、catalog/KV/cache、generation/no-op、父层 IAM 与 import/export 边界。补充 `!qg` 单调高水位，防止 REMOVE 后重建产生 ABA。验证：`surrealdb-core` 全量 3,562 passed / 9 ignored；quota 定向 12/12；language test 三种 planner 3/3。
