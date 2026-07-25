Status: open
Label: ready-for-agent
Assignee: unassigned

# SDB-NQ-07 — 认证持久 backend、并发一致性与性能

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 建立 backend-neutral hard-quota contract suite，逐 backend 运行事务竞争、冲突重试、multi-node、崩溃/重启、网络故障和 rebuild。
- 对无策略持续计量、有限策略、regex 多匹配和批量写建立 benchmark，记录吞吐、p95/p99 延迟、KV 写放大与存储增长。
- 注入 counter 写前后、业务 mutation 前后、commit unknown、policy generation race、rebuild staged/flip 等故障。
- 产出 production backend allowlist；memory 仅作开发/CI，不自动认证其它 storage feature。
- 把认证结果纳入 capability/readiness 与兼容 manifest。

## Acceptance criteria

- [ ] 每个 allowlisted backend 在高并发下从不超过 limit，最终 counter 与真实资源一致。
- [ ] crash/retry/commit unknown 不双扣、不漏扣、不把 staged epoch 当 active。
- [ ] multi-node mixed request 的一致性结果与单节点相同。
- [ ] 基准有固定数据集、baseline 与可重复命令；回归阈值由 release manifest 明示。
- [ ] 首期至少一个持久 backend 进入 production-certified，否则不能发布 stable。

## Dependencies

- Blocked by: [`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)
- Blocks: [`建立私有 fork 发布与供应链门`](08-fork-release-supply-chain.md)、[`surreal_ck：实现旧事件配额盘点、回填与分批切换 conductor`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/09-legacy-quota-migration-conductor.md)
