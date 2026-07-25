Status: done
Label: done
Assignee: /root

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

- [x] capability `format_version` 与 major/range 匹配测试齐全，未知字段/major 可安全处理。
- [x] readiness 在 quota feature 缺失、format 不兼容、migration 非 clean、backend 未认证时不 ready。
- [x] 新 datastore 写 marker；既有 datastore 普通启动返回 migration_required，不静默转换。
- [x] migration 中断可重入，格式推进后旧 CLI/binary 拒绝原地 downgrade。
- [x] capability 不泄露凭证、policy、database 列表或业务信息。

## Dependencies

- Blocked by: [`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)、[`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)
- Blocks: [`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)、[`建立私有 fork 发布与供应链门`](08-fork-release-supply-chain.md)、[`surreal_ck：建立跨仓 quota contract、SDK 固定版本与启动能力门`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/01-contract-sdk-capability-gate.md)

## Completion notes

- 2026-07-25：交付稳定、无鉴权的 `GET /capabilities` 与机器可读
  `compatibility/native-quota-v1.json`；响应显式声明 fork/build/quota/INFO/error、
  storage/catalog/usage/backend/CLI 契约，远程 CLI 会逐项 fail-closed 校验。
- 2026-07-25：`/ready` 默认要求 `native-quota-v1`，也支持
  `?require=native-quota-v1,...`；storage marker、migration state、contract major 或 backend
  allowlist 不匹配时返回 not-ready。
- 2026-07-25：新增 `surreal datastore status|preflight|migrate`。迁移要求 snapshot
  引用与 offline 确认，先原子写入 fork-required storage major + `in_progress` marker，再逐
  database 重建 usage epoch，最终原子推进为 `clean`；中断可重入、完成后幂等。
- 2026-07-25：远程 SQL/import/export 在执行前要求匹配 capability 与精确 fork/CLI
  release；本地直接 datastore 操作由 high-bit storage major、marker 和当前 binary
  共同围栏。
- 2026-07-25：memory backend 仅作为开发/CI hard-quota 认证项，`production=false`；
  RocksDB/SurrealKV/TiKV/IndxDB 暂不认证并因此 not-ready，认证工作留给 SDB-NQ-07。
  `surrealdb-js` 认证版本列表暂为空，待下游 SCK-NQ-01 contract suite 固定后写入。
