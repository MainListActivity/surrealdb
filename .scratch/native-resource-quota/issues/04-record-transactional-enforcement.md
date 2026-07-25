Status: done
Label: done
Assignee: /root

# SDB-NQ-04 — 接入 record 与全部 typed mutation 事务强制

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 在统一后的 typed record existence mutation seam 接入 per-table record counter 和有效限额解析。
- 覆盖 CREATE、INSERT、UPSERT、RELATE、UPDATE 产生新记录、DELETE、批量/range、语义 import、SDK bulk 与内部派生写入。
- 以事务最终 signed net delta 聚合同一 counter；多 violation 预提交聚合并限制数量。
- 所有 counter 条件更新必须与资源写入同事务，依赖 backend 可验证的冲突语义，不能先查后写。
- 实现 limit 内、at-limit、over-limit 非恶化、删除释放、等量置换和跨表不可抵扣。

## Acceptance criteria

- [x] 每种写语句与内部路径都有成功、临界、拒绝、rollback 和 batch atomic test。
- [x] N 个并发客户端争抢 K 个剩余名额时恰有 K 个成功且最终 counter/record 数一致。
- [x] CREATE/DELETE no-op、UPSERT existing/new、RELATE 边、级联副作用与整表删除不重复计量。
- [x] 多表事务分别检查各 counter，不跨表抵扣；全部违规一次性结构化返回。
- [x] 无策略 database 仍正确累计，后续应用策略无需扫描 records。

## Dependencies

- Blocked by: [`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)
- Blocks: [`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)

## Comments

- 2026-07-25：在 `TableProvider::{put_record,set_record,del_record}` typed mutation seam 接入 per-table signed record delta；create-only、set-if-absent 与实际 delete 分别形成 `+1/0/-1`，并复用 transaction savepoint、generation fence、active epoch 和条件 counter 更新。整表删除会清除删除前的 record delta，并以重建后的最终记录数结算。
- 2026-07-25：收敛普通/聚合物化视图的两处直接 `RecordKey` 写入旁路。CREATE、INSERT/ON DUPLICATE、UPSERT existing/new、UPDATE、RELATE/OR UPDATE、DELETE、级联边删除、range、语义 import、批量写与 view 派生写均经过统一 facade；无策略 database 持续累计。
- 2026-07-25：pre-commit 逐 counter 聚合违规，稳定排序并最多返回 64 项，额外项用 `truncated` 标识；跨 table/resource 不抵扣。覆盖 at-limit、超额非恶化、等量置换、table replacement、rollback/cancel、策略 generation 切换，以及 12 个并发客户端争抢 5 个名额时恰好 5 个成功。
- 2026-07-25：验证：quota 专属回归 51/51；Clippy `-D warnings` 通过；排除一个已在父提交 `e6609583e` 独立复现的索引构建清理 flake 后，`surrealdb-core` 为 2937 passed / 9 ignored / 1 filtered。未排除的全量运行只有 `define_index_concurrent_cancel_cleans_uncommitted_build_artifacts` 失败；该测试在父提交隔离循环第 4 次同样失败，不由本票引入。
