Status: done
Label: done
Assignee: /root

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

- [x] 每个 allowlisted backend 在高并发下从不超过 limit，最终 counter 与真实资源一致。
- [x] crash/retry/commit unknown 不双扣、不漏扣、不把 staged epoch 当 active。
- [x] multi-node mixed request 的一致性结果与单节点相同。
- [x] 基准有固定数据集、baseline 与可重复命令；回归阈值由 release manifest 明示。
- [x] 首期至少一个持久 backend 进入 production-certified，否则不能发布 stable。

## Dependencies

- Blocked by: [`接入 table/field catalog 事务强制`](03-table-field-transactional-enforcement.md)、[`接入 record 与全部 typed mutation 事务强制`](04-record-transactional-enforcement.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)
- Blocks: [`建立私有 fork 发布与供应链门`](08-fork-release-supply-chain.md)、[`surreal_ck：实现旧事件配额盘点、回填与分批切换 conductor`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/09-legacy-quota-migration-conductor.md)

## Completion notes

- 2026-07-25：新增 backend-neutral `native-quota-contract-v1`，同一套测试在 memory
  与 RocksDB 上覆盖无策略持续计量、EXACT/REGEX 重叠规则、批量原子性、双节点
  CREATE/INSERT/UPSERT 高竞争、策略 generation race 和 rebuild epoch staged/flip。
- 2026-07-25：新增仅测试构建可用的六类 quota fault site：业务 mutation 前/后、
  counter 写前/后、commit 前、commit outcome unknown。提交前故障整体回滚；
  outcome unknown 验证固定 record id 重试不双扣。
- 2026-07-25：RocksDB 通过 72 客户端争抢 24 个 record 名额的双节点混合写契约；
  最终恰有 24 次成功，ledger 与独立物理扫描均为 24。
- 2026-07-25：RocksDB 持久认证使用子进程在 staged rebuild 提交后直接退出，
  不调用 datastore shutdown；父进程重开同一目录后确认旧 active epoch 保留、
  staged epoch 不误激活、写入 fail-closed，并可通过 REBUILD 恢复。
- 2026-07-25：`compatibility/native-quota-v1.json` 将 RocksDB 标记为首个
  `production=true`、`hard_quota_certified=true`、持久重启已认证的 backend；
  memory 仍为开发/CI，SurrealKV/TiKV/IndxDB 保持未认证。RocksDB 为嵌入式 backend，
  网络故障标记为不适用；网络型 backend 必须另行认证。
- 2026-07-25：新增 RocksDB 固定 benchmark 与
  `compatibility/native-quota-rocksdb-v1.json` 基线，覆盖无策略计量、有限策略、
  regex 多匹配和 16-record batch；报告吞吐、p95/p99、KV 写/字节放大和存储增长，
  manifest 明示 baseline/candidate 命令及五类回归阈值。
- 2026-07-30：最终双仓复审把 backend-neutral 合约扩展为 5 项；memory 与 RocksDB
  同时认证 RELATE/级联释放、materialized view 派生写、semantic import 语句边界和
  record range 实际基数。candidate discovery 门同步要求至少发现 5 项，防止新增语义
  仅在 memory 单测通过却被错误标记为持久后端已认证。
- 2026-07-30：最终压力复审在 memory backend 稳定复现提交期条件写丢失：两个并发
  事务可基于同一 quota usage 旧值完成投影并同时提交。memory backend 现于共享短
  临界区内，按最新已提交值复核条件写后再提交；原双节点 mixed
  CREATE/INSERT/UPSERT 压测连续 100 轮无越额。回归结果为 memory KVS 34/34、
  memory quota backend 5/5、RocksDB quota backend 5/5。memory 仍仅用于开发与 CI，
  生产候选继续使用 RocksDB。
