# TiKV native quota 可行性与认证差距评估

任务：`98ba14f2-7601-4345-9e5a-88a09da86665`；执行：`engineering-codex-ea8e83`；日期：2026-09-30。

## 决策摘要

本报告是立项评估，不是 production-certified 证明。生产必须保持 TiKV 集群；既有仅认证 RocksDB 的候选不能进入 TiKV 生产。任何本地通过结果都不能直接扩充 release allowlist。

**NO-GO：当前代码/候选不得直接获得 TiKV 生产认证，也不得解除生产 cutover 冻结。建议 GO：经理立项限定的 TiKV 错误适配修复与认证交付链。** 本地 3 PD + 3 TiKV 实测五组契约为 **4 通过、1 失败**，失败是竞争冲突分类缺口，单独重跑再次复现。此结果不等于证明底层配额超发：成功数与最终 ledger/物理计数断言因测试 panic 未执行到。

已通过的四组说明通用计量、模拟故障原子性、generation/rebuild cache-restart 与扩展 record 路径可在本地 TiKV 上运行；它们仍不能代替真实集群故障、持久重启、完整备份恢复和发布认证。

本次只改测试 wiring 与 TiKV 串行测试标记（与既有共享集群测试共用 serial lock），不修改生产配额、事务、OIDC、兼容清单或发布脚本。故意保留失败断言，未吞掉错误、放宽上限或把存储错误一概当 retryable；PR 的 TiKV 竞争检查预期为红。

## 环境与复现

- 基线：fork `origin/main`，`505b3c829`（非现有已签名候选的重新认证）。
- 分支：`task/98ba14f2`；worktree：`/Users/y/IdeaProjects/surrealdb-task-98ba14f2`。
- macOS arm64，16 GiB 内存。本机没有 `tiup`、`docker` 命令；2379 原先没有监听。
- 从 PingCAP 官方镜像下载 TiUP v1.16.5 到 `/tmp/sck-98ba-tiup-bin`，`TIUP_HOME=/tmp/sck-98ba-tiup-home`；没有安装全局工具。
- 首次组件 manifest URL 返回 404；改用 TiUP 正式元数据流程。首次初始化因缺 `bin/root.json` 失败；把官方 root manifest 放入这个隔离目录后安装成功。
- PD / TiKV 均为 v8.5.8，playground v1.17.1；3 PD + 3 TiKV，全部绑定 `127.0.0.1`。PD 端点：2379、2382、2384；TiKV：20160、20161、20162。
- `tiup playground display` 确认六个进程；PD `/pd/api/v1/stores` 确认三个 store 全为 `Up`。
- 仓库要求 Rust 1.95，当前 Rust 镜像返回 403，未改 toolchain 或 mirror。使用已安装、且与 `rust-toolchain.nightly` 一致的 `nightly-2026-05-11`：rustc 1.97.0-nightly、cargo 1.97.0-nightly。
- 本地端点、集群版本只代表这个实验环境；生产 TiKV/PD 版本、TLS、keyspace/API version、提交模式和容量尚未核验。

实际启动命令（所有 shell 均经 RTK）：

```sh
rtk proxy env TIUP_HOME=/tmp/sck-98ba-tiup-home /tmp/sck-98ba-tiup-bin/tiup install pd tikv playground
rtk proxy env TIUP_HOME=/tmp/sck-98ba-tiup-home /tmp/sck-98ba-tiup-bin/tiup playground v8.5.8 --mode tikv-slim --kv 3 --pd 3 --db 0 --ticdc 0 --tiflash 0 --without-monitor --host 127.0.0.1 --tag sck-98ba14f2
rtk proxy env TIUP_HOME=/tmp/sck-98ba-tiup-home /tmp/sck-98ba-tiup-bin/tiup playground display
rtk proxy cargo +nightly-2026-05-11 test --locked -p surrealdb-core --no-default-features --features kv-tikv --lib kvs::tests::tikv::quota_ -- --test-threads=1 --nocapture
rtk proxy cargo +nightly-2026-05-11 fmt --all --check
```

测试工厂在每次 setup 时删除测试集群的全部 key range，必须只连接一次性的专用本地集群，禁止指向共享或生产集群。串行运行是必要条件：本轮 `--test-threads=1`；新挂载的契约加上与 raw 等既有 TiKV 测试相同的 serial lock，仓库 nextest 配置也已有 `serial-kvs` 组。重启/独立节点认证需要另写不清库的 reopen 工厂，不能重复调用这个 setup。

## 五组契约实测

| 测试（`kvs::tests::tikv::`） | 本轮结果 | 覆盖与限制 |
| --- | --- | --- |
| `quota_no_policy_metering_and_regex_contract` | PASS | 无 policy 计量，exact/regex 重叠规则，table/field/record 上限及物理计数 |
| `quota_multi_node_mixed_contention_contract` | FAIL，独立重跑亦 FAIL | 72 客户端混合 CREATE/INSERT/UPSERT，争用 24 名额；收到未分类的聚合 WriteConflict，panic；成功数、ledger、物理行数均为 24 的终态断言未到达 |
| `quota_atomic_fault_and_commit_unknown_contract` | PASS | 六个通用注点，回滚原子性、成功后模拟 unknown、重复业务 ID 不重复计费 |
| `quota_generation_and_rebuild_epoch_contract` | PASS | generation stale writer 被拒、staged epoch 不提前激活、cache restart 后 fail-closed / REBUILD |
| `quota_extended_record_semantics_contract` | PASS | 关系边及级联删除、view、部分 import、range create/delete |

首轮编译成功（4m40s），测试 0.87s，退出 101：`4 passed; 1 failed; 0 ignored; 2817 filtered out`。最终串行锁版本重新编译 54.74s、测试 1.43s，仍为同样 4 PASS / 1 FAIL；单独重跑竞争也失败。`cargo +nightly-2026-05-11 fmt --all --check` 两次退出 0。`cargo +nightly-2026-05-11 clippy --locked -p surrealdb-core --no-default-features --features kv-tikv --lib --tests -- -D warnings` 未能启动：已安装 nightly 没有 cargo-clippy；遵守不安装全局工具的岗位限制，没有新增组件。Rust 1.95 和 Clippy 必须由后续 CI 补验；本地用 nightly 的 test 编译通过，不标成指定 stable/Clippy 已通过。

可审阅的逐字输出摘录、每份本地日志 SHA256 和 store 状态见 [`tikv-local-test-evidence.txt`](tikv-local-test-evidence.txt)。只截取每轮一个代表性 panic，未把省略的重复报错或临时日志位置当持久化完整日志。实验结束关闭本任务的六个集群进程，没有触碰其他进程。

竞争最小复现命令：在相同专用集群运行上面的 test 命令，把 filter 换成 `kvs::tests::tikv::quota_multi_node_mixed_contention_contract` 并加 `--exact`；退出 101。

### 失败原因与下一步修复边界

实测原始错误为 `There was a problem with a transaction: Multiple key errors: [KeyError(... conflict: Some(WriteConflict ... reason: Optimistic) ...)]`。冲突 key 尾部是 `!qg`，说明 generation reassert 走到了 TiKV 真正的乐观 write-conflict，而不是单纯运行中断或连不上集群。

`surrealdb-tikv-client 0.5.0` 返回的顶层为 `MultipleKeyErrors`，嵌套冲突没有进入 `kvs/err.rs::From<tikv::Error>` 的顶层 `KeyError.conflict` 分支，落成非 retryable 的 `Transaction`。`tx.rs::commit` 因而没有输出 QuotaConflict，测试的 `is_expected_contention` 拒绝该错误并 panic。这是本轮直接证据支持的 liveness / 错误契约缺陷，不是配额计数原子性已经失败的证据。

后续修复应通过类型匹配递归归类已知聚合冲突，增加全冲突、混合错误、非冲突与 unknown 的回归测试；**禁止解析 `WriteConflict` 英文字符串、禁止把任何 MultipleKeyErrors 都视为可重试**。修复后需重新证明 72 客户端收敛至 24，随后再开展独立节点与最后一名额压力；本轮不改生产错误适配，保留真实差距供经理定范围。

测试中的 512 次冲突重试属于测试驱动，不代表用户端自动重试或生产 SLO 已通过。现有竞争用例使用 `fork_for_test_with_node_id`，两个 facade 有独立 node ID 与 cache，但共享 transaction factory / TiKV client，并非两个独立 SurrealDB 服务进程。

## issue 12 认证维度与缺口

| 维度 | 代码核对 | 正式认证还需补齐 |
| --- | --- | --- |
| 事务竞争 / 冲突重试 | `quota.rs::flush_quota_usage` 同事务对 meta、generation 和 usage 执行条件写；TiKV `putc` 读取当前值、比较、写入同一乐观事务；`tx.rs::commit` 把已 flush 的可重试 commit conflict 变成 QuotaConflict | 不能用普通 blind-write 的 LWW 测试推断 quota 安全或不安全。需真实多客户端/多计算进程、反复争最后一个名额、不同 region、冷 cache / policy 更新、多租户并发与冲突耗尽测试；必须核对 driver 的实际错误形状 |
| 错误分类 | `kvs/err.rs` 只把顶层 TiKV `KeyError.conflict` 映射成 TransactionConflict；driver 还有 ExtractedErrors、MultipleKeyErrors 和 UndeterminedError，落入通用 Transaction 分支 | 测试聚合错误里的冲突能否进入生产 retryable 路径；unknown 不能当普通失败盲重试。故障实测后再决定修复范围，不能扩大为“所有存储错误可重试” |
| 故障注入 | 六个 QuotaFaultSite 在通用 tx/quota 层，未被 kv-mem / RocksDB gate 限制；TiKV 也会走业务 mutation → counter flush → backend commit | 这仅模拟返回错误，未模拟 TiKV prewrite / primary commit / secondary resolution 的部分网络失败。需跨 region 的 PD/leader 故障、节点丢失、超时和真实 commit-unknown，独立读回 ledger/业务/错误 DTO |
| 重启恢复 | `Datastore::restart` 清 cache，但保留 transaction_factory。RocksDB 的子进程 reopen 专项仅在 kv-rocksdb 下启用 | 增加独立计算进程 kill/reopen、TiKV 单节点/leader 切换、完整集群 stop/start；确认 committed ledger/marker 持久化与 staged epoch fence。不是把 RocksDB 目录复制到 TiKV 节点 |
| rebuild 一致性 | 通用契约验证 generation fence、staged epoch、取消激活及重新 rebuild；native migrator 已按 database 扫描、验证、激活 ledger | 增加大规模实际 REBUILD 对独立物理 table/field/record 扫描，kill 于各持久化边界、重试与并行 writer、删除/expunge、policy 变化；验证 region 分裂与 GC 条件下扫描一致性 |
| 格式 marker / migration | `!v` / `!vf` 是全局 KV key；native_quota.rs 没有本地路径依赖，迁移先提交 in_progress，再逐库 rebuild，最后 clean | 同一 TiKV datastore 全计算节点读同一 marker；需验证匹配 CLI 可接受 tikv 路径、断点续跑、双 migrator 排他、旧 fork/vanilla fail-closed。CLI 的 offline 与 snapshot 是人工声明，不证明真实集群停写或备份已恢复 |
| 备份 / 快照 | 普通 SurrealQL export 不保存 quota ledger / 全局 marker，也不代表所有库一致快照。官方 TiKV-BR 文档只承诺 RawKV | 必须选并验证覆盖此 fork **Transactional API** 的完整备份，包含所有 database、policy、usage、!v/!vf、身份与控制面。RawKV BR 或 TiDB schema backup 不能未经验证直接套用；备份可恢复性是独立硬门禁 |
| release / runtime | 本轮仅启用测试，未触碰 manifest / capability / 镜像 | 固定生产相符 TiKV/PD/client/API/keyspace/提交选项与资源；完整 CI、HTTP/WSS 结构化错误、双仓 E2E、p95/冲突 SLO、multi-arch、签名新 candidate / receipt。后续独立交付链才可申请 allowlist |

六个注点位置：业务前后在 `tx.rs` 的 set / put / putc；counter 前后在 `quota.rs`；BeforeCommit 在底层 `tr.commit()` 前；CommitOutcomeUnknown 在底层 commit 成功之后。因此 unknown 契约通过也只能说明“模拟丢失成功回执后按业务 ID 读回”的逻辑，不能证明真实 TiKV 不确定事务已解决。

备份路线优先考虑：独立认证的全 transactional-keyspace snapshot/restore（需验证 PD/TSO、MVCC/GC、keyspace 与版本适配）。若现有工具不满足，可评估停写条件下 fork 全 KV 一致性导出/恢复工具；需要保存并验证内部格式与 key，不能退化成每库 SurrealQL export。普通 export + policy inventory + REBUILD 只可用作另行设计的逻辑重建路线，不等价于原 datastore snapshot，尤其不能证明旧 marker、身份和跨库关系完整。

## cutover runbook 阶段一的改写建议

以下是交给后续交付链的要求，本轮没有修改 surreal_ck runbook，也没有执行生产动作。

1. inventory 增加 TiKV/PD 版本、集群 ID、PD endpoints、TLS/API/keyspace、GC safepoint、SurrealDB 全计算节点、当前 digest 与 marker；记录 `_system`、active workspace、platform_content 等全部 datastore 范围。
2. 冻结公网 WSS 之外，还必须停止全部 SurrealDB 节点和后台/直连/控制面 writer，排空事务；维护 fence 作用于共享集群。`--confirm-offline` 仅是确认参数，需独立证据证明无其他 writer。
3. snapshot id 改为带 cluster/keyspace、统一时间点/TSO、覆盖范围、工具版本、checksum、恢复测试报告的备份 receipt。恢复演练目标是新的隔离 TiKV 集群/明确 keyspace，不能是本地 RocksDB 目录。必须实际读回 policy、ledger、marker 与全部关键身份/业务数据。
4. “启动 RocksDB，format migration”改为同 release 的 CLI 连接冻结的 TiKV datastore，由唯一迁移执行者迁移一次；捕捉 `in_progress` / `clean`、各库 rebuild 和 checkpoint。当前核心代码是可复用候选，TiKV CLI 与断点恢复尚需专项认证，不能先写成已经支持。
5. 保持 inventory/import/prepare/assert-reopen 的业务职责，但 maintenance-evidence 要替换本地路径/marker 证据为共享 datastore receipt。逐库 REBUILD、独立物理扫描、fresh INFO 后启动全部同 digest 计算节点；每个节点 readiness/marker/backend 认证通过再恢复入口。禁止不同格式或 counter 语义混部。
6. 数据回退写成保留故障集群只读、恢复备份到新隔离 TiKV datastore，再审核切换 PD/keyspace 连接；不能覆盖任意单个 TiKV 数据目录、不能恢复一部分 store、不能原地降级 marker。恢复 RTO/RPO、容量和地址切换方式须经演练。
7. RocksDB baseline 的性能阈值要补充已批准的 TiKV 同拓扑基线；保留 false-negative、counter mismatch、commit unknown、recovery failed 的零容忍，不以吞吐理由削弱强制配额。

## 认证工作量粗估与停止条件

以下是工程工作日范围，供经理拆链；不是交付期限或资源购买承诺，假设现有免费/已拥有测试资源可运行生产相符集群。

| 工作包 | 粗估 | 完成条件 |
| --- | --- | --- |
| 五组实测失败最小复现、driver 错误/竞争路径定界与修复 | 2–5 日 | 明确 safety/liveness 边界；修复后重跑且独立复核 |
| 多计算节点/多 region、冲突重试和真实故障矩阵 | 3–5 日 | 无超发、无漂移，unknown 可确定读回，错误 DTO 保真 |
| marker/migrator、重启和 rebuild crash/resume | 3–5 日 | 真进程与集群恢复、扫描一致、旧格式 fail-closed |
| transactional 全范围 backup/restore 与 runbook 演练 | 4–8 日 | 完整恢复 receipt、checksum/marker/ledger 审计、RTO/RPO |
| release-line 回移、双仓 CI/E2E/SLO 与签名晋级 | 2–4 日 | 新候选固定 TiKV profile，receipt 支持后续 cutover |

合计 14–27 工程工作日，另需独立工程/QA 与运营门禁；可并行部分不代表本轮授权创建 agent。备份工具适配或内核一致性问题可能使范围扩大。

停止条件：任一超发 / counter drift 无法解释、真实 unknown 不可确定、无法得到包含 protected KV 状态的可恢复备份，或 production profile 无法复现，均不得申请生产认证。应把明确复现、修复成本和替代方案交经理重新决定；保持生产 TiKV，不以单机绕过红线。

## 来源与证据边界

- 任务服务 context 与 history；老板的 TiKV 集群红线来自任务服务，未把其他同事意见当老板决定。
- surreal_ck `.scratch/native-resource-quota-wayfinder/issues/07-transactional-consumption-semantics.md`、`12-fork-maintenance-compatibility-release.md`；`docs/runbooks/native-quota-release-cutover.md`（只读）。
- 本 fork：`surrealdb/core/src/kvs/tests/{mod,quota_backend_contract,quota_rocksdb_certification}.rs`，`kvs/{quota,tx,tikv/mod,err,ds,native_quota,export}.rs`，`surrealdb/server/src/cli/datastore.rs`，`.config/nextest.toml`。
- 安装的 `surrealdb-tikv-client 0.5.0`：`common/errors.rs`、`transaction/{transaction,buffer,requests}.rs`。仅阅读，未修改 registry 或 node_modules。
- [TiKV 官方 RawKV BR 文档](https://tikv.org/docs/dev/concepts/explore-tikv-features/backup-restore/)（2026-09-30 查询）：文档明确该工具支持 RawKV，未取得它覆盖 SurrealDB transactional data 的证明。
- 本次没有访问生产数据库、部署、付费资源或外部联系。正式生产 profile、backup/restore、真实故障、集群恢复和 release acceptance 均未验证。
