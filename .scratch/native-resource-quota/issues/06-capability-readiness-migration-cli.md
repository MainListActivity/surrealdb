Status: open
Label: ready-for-agent
Assignee: unassigned

# SDB-NQ-06 — 交付 capability、readiness、格式迁移与匹配 CLI

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 增加稳定 `GET /capabilities`，返回 fork/build/quota/INFO/error/storage/catalog/usage/backend contract。
- readiness 支持要求 `native-quota-v1`；编译能力、format marker、migration state 或 backend allowlist 不合格时 fail-closed。
- 提供显式 datastore preflight/migrate/status CLI：snapshot 前置、maintenance fence、幂等格式转换、marker 原子推进。
- CLI 对 server capability/datastore format 预检；不匹配时拒绝 quota 管理、格式迁移、backup/restore 和直接 datastore 操作。
- 产生机器可读兼容 manifest 与 OCI labels 所需元数据；保留普通 `/version` semver 行为。

## Acceptance criteria

- [ ] capability `format_version` 与 major/range 匹配测试齐全，未知字段/major 可安全处理。
- [ ] readiness 在 quota feature 缺失、format 不兼容、migration 非 clean、backend 未认证时不 ready。
- [ ] 新 datastore 写 marker；既有 datastore 普通启动返回 migration_required，不静默转换。
- [ ] migration 中断可重入，格式推进后旧 CLI/binary 拒绝原地 downgrade。
- [ ] capability 不泄露凭证、policy、database 列表或业务信息。

## Dependencies

- Blocked by: [`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)
- Blocks: [`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)、[`建立私有 fork 发布与供应链门`](08-fork-release-supply-chain.md)、[`surreal_ck：建立跨仓 quota contract、SDK 固定版本与启动能力门`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/01-contract-sdk-capability-gate.md)
