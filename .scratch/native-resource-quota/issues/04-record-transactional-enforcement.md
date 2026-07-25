Status: open
Label: ready-for-agent
Assignee: unassigned

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

- [ ] 每种写语句与内部路径都有成功、临界、拒绝、rollback 和 batch atomic test。
- [ ] N 个并发客户端争抢 K 个剩余名额时恰有 K 个成功且最终 counter/record 数一致。
- [ ] CREATE/DELETE no-op、UPSERT existing/new、RELATE 边、级联副作用与整表删除不重复计量。
- [ ] 多表事务分别检查各 counter，不跨表抵扣；全部违规一次性结构化返回。
- [ ] 无策略 database 仍正确累计，后续应用策略无需扫描 records。

## Dependencies

- Blocked by: [`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)
- Blocks: [`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)
