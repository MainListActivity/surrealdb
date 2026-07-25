Status: open
Label: ready-for-agent

# SurrealDB 原生资源配额实施规格

更新时间：2026-07-25

## 一句话

在 MainListActivity 的 SurrealDB 私有 stable fork 中实现 database-scoped、父层授权、事务精确且不可被 database Owner 绕过的 table/field/record 原生配额，并提供稳定 SurrealQL、结构化错误、用量重建、能力发现、格式围栏和可认证发布链。

## 规格来源

- canonical 决策地图：[`surreal_ck/.scratch/native-resource-quota-wayfinder/PRD.md`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota-wayfinder/PRD.md)
- 资源定义扩展面研究：[`surrealdb-resource-definition-extension.md`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota-wayfinder/research/surrealdb-resource-definition-extension.md)
- 事务强制扩展面研究：[`surrealdb-transactional-enforcement-extension.md`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota-wayfinder/research/surrealdb-transactional-enforcement-extension.md)
- SurrealQL/错误契约：[`设计原生配额 SurrealQL、错误与可观测契约`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota-wayfinder/issues/08-native-surrealql-errors-observability.md)
- 维护与发布契约：[`确定定制 SurrealDB 的维护、兼容与发布策略`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota-wayfinder/issues/12-fork-maintenance-compatibility-release.md)

实现中若本规格与上述已关闭决策冲突，以 canonical 决策票为准；不得自行改变资源口径、权限或并发语义。

## 锁定范围

- 每个 database 最多一份当前 `QuotaPolicyDefinition`；规则包含稳定 rule id、TABLE/FIELD/RECORD、EXACT/REGEX selector、有限 `u64`/UNLIMITED limit。
- table 对全部命中规则的集合桶计数；field/record 对每张命中表独立计数。field/record 精确规则覆盖正则，否则命中正则取最小上限。
- root Owner 与目标 namespace Owner 可修改；database Owner 只可查看，不能 DEFINE/ALTER/REMOVE/REBUILD。
- counter 与真实 mutation 同事务；批量按最终有符号净增量原子判断，并发不超卖。
- 超额时允许 projected 不高于 current 的净零/下降事务，禁止进一步增长。
- 无策略也持续计量但不限制；升级前 database 通过只读 maintenance fence 回填账本。
- quota 元数据不进入普通 export/import，不能由普通 database backup 恢复。
- 所有协议保留稳定 `code/retryable/details`；不接受仅有 message 的错误契约。

## 不做

- Plus/Pro/Max、价格、订阅、override 或 surreal_ck 业务实体。
- index、存储字节、LIVE、并发查询或请求速率配额。
- table-name 任意 SurrealQL 表达式、prefix/glob 专用 selector 或 rule priority。
- 取消订阅后的严格只读 suspension。
- 在线 rebuild/catch-up、毫秒级 scheduled policy 或 namespace 继承模板。
- 让 vanilla SurrealDB 打开 quota datastore，或提供生产 disable flag。

## 代码边界

主要修改位于：

- `surrealdb/core/src/syn/**`、`sql/statements/**`、`expr/statements/**`
- `surrealdb/core/src/catalog/**`、`key/database/**`、`kvs/cache/**`、`kvs/tx.rs`
- table catalog mutation、typed record mutation 与整表前缀删除的统一事务路径
- `surrealdb/core/src/iam/**`、`err/**`
- legacy/streaming INFO 路径、server HTTP/RPC 错误编码与 readiness/capability route
- CLI datastore migration/backup/restore preflight
- `language-tests/**`、`surrealdb/tests/**`、`tests/**` 与 backend contract suites

私有 quota 改动保持集中、可重放；不得散布 surreal_ck 套餐特例。

## 实施路线图

| 名称 | 主体 | 依赖 |
|---|---|---|
| [`建立 QUOTA grammar、catalog 与父层 IAM`](issues/01-quota-resource-grammar-catalog-iam.md) | parser、两层 AST、revisioned catalog、KV key/cache、DEFINE/ALTER/REMOVE、IAM | 无 |
| [`建立持续用量账本、epoch 与 datastore 格式围栏`](issues/02-usage-ledger-epoch-format-fence.md) | QuotaUsage、counter key、maintenance fence、storage marker | QUOTA resource catalog shape |
| [`接入 table/field catalog 事务强制`](issues/03-table-field-transactional-enforcement.md) | DEFINE/ALTER/REMOVE TABLE/FIELD、view/relation/no-op、整表结算 | grammar/catalog + ledger |
| [`接入 record 与全部 typed mutation 事务强制`](issues/04-record-transactional-enforcement.md) | CREATE/INSERT/UPSERT/RELATE/DELETE/import/bulk/savepoint | ledger；可与 table/field 后半并行 |
| [`交付 INFO、REBUILD、结构化错误与观测`](issues/05-info-rebuild-errors-observability.md) | INFO STRUCTURE、REBUILD、error DTO、日志/metrics/export 边界 | grammar/catalog + ledger + 两类 enforcement |
| [`交付 capability、readiness、格式迁移与匹配 CLI`](issues/06-capability-readiness-migration-cli.md) | `/capabilities`、required readiness、format migrator、CLI preflight | ledger format + INFO/error contract |
| [`认证持久 backend、并发一致性与性能`](issues/07-backend-certification-fault-performance.md) | backend allowlist、竞争/崩溃/恢复/benchmark | 完整 enforcement + rebuild/capability |
| [`建立私有 fork 发布与供应链门`](issues/08-fork-release-supply-chain.md) | upstream sync、release line、multi-arch image、SBOM/signing、manifest | capability + 至少一个 certified backend |

## 跨仓依赖门

- grammar、STRUCTURE DTO、error DTO 和 capability fixture 冻结后，surreal_ck 才能完成 NativeQuotaClient。
- 第一个供联调的 candidate image 必须已经支持 table/field/record、REBUILD、HTTP+WebSocket 结构化错误和 `native-quota-v1` capability。
- surreal_ck 不以 nightly SHA 作为生产最低版本；首个通过本规格全部 gate 的签名 stable fork release 才是最低版本。
- fork release 只有通过 `/Users/y/IdeaProjects/surreal_ck` 的 downstream contract/E2E 才能晋级 stable。

## 完成定义

- database Owner 能创建普通 schema，却无法修改、移除、导入或绕过其 quota。
- 所有 CREATE/INSERT/UPSERT/RELATE/import/bulk/table/field/remove 路径均按同一事务账本计量；N 个并发客户端争抢最后 K 个名额时恰有 K 个成功。
- exact 与 regex（至少 `^ent_`）正确匹配；table、field、record 三类资源达到上限后返回相同稳定错误模型。
- rollback/savepoint/statement failure 不泄漏或重复释放名额；删除、整表删除、净零置换和已超额非恶化语义符合决策票。
- 既有 datastore 在 maintenance 下回填真实 table/field/record，用量与独立扫描一致；重建崩溃后保持只读并可恢复。
- HTTP、WebSocket、Rust SDK 与认证版本 surrealdb-js 都保留 `code/retryable/details`。
- vanilla/旧 binary 拒绝打开 fork-required datastore；未知 quota/storage format fail-closed，格式推进后旧 CLI 拒绝破坏性操作。
- `/capabilities` 与 required readiness 可被机器验证，未认证 backend 不 ready。
- 至少一个持久 backend 通过并发、故障注入、重启恢复和 rebuild 套件；memory 仅限开发/CI。
- 发布同一签名 multi-arch digest、匹配 CLI、SBOM、provenance、兼容 manifest，且 downstream surreal_ck 测试全绿。

## 必跑验证

- `cargo make fmt`
- `cargo make ci-clippy`
- quota 相关 Rust 单元/集成测试
- quota language tests
- HTTP/WebSocket/CLI 兼容测试
- 每个拟认证 backend 的并发与故障注入 suite
- catalog/revision/key frozen fixture 测试
- surreal_ck downstream contract 与端到端测试

全仓 `cargo test` 和完整 CI 仍是 release gate；局部测试通过不能替代。

