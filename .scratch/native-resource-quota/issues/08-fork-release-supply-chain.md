Status: done
Label: done
Assignee: /root

# SDB-NQ-08 — 建立私有 fork 发布与供应链门

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 配置官方只读 upstream、每周同步 PR、`releases/sck-<major>.<minor>` stable line 与禁止 force-push 规则。
- 定义 fork release/version/OCI label、不可覆盖版本/SHA tag 和 digest 晋级；不使用官方镜像 namespace。
- 一次构建 amd64/arm64 image 和匹配 CLI，生成 SBOM、provenance、漏洞报告并签名。
- CI gate 串联 upstream CI、quota suites、format/capability、backend certification、multi-arch smoke，并提供可被 surreal_ck downstream contract/E2E 消费的签名 release candidate。
- 发布兼容 manifest，列出 SDK/CLI、contract/format、certified backend 与 mixed-version/rollback 声明。

## Acceptance criteria

- [x] 同一签名 digest 从 CI→canary→staging→production 晋级，环境不重建。
- [x] nightly 与 stable channel 隔离；production 只 pin digest。
- [x] 未通过任一 quota/backend gate 的 candidate 无法产生；未通过 surreal_ck 双仓验收的 candidate 无法晋级 stable。
- [x] 匹配 CLI、manifest、image labels、capability 文档引用相同 git SHA/release。
- [x] 上一 production release line 的 90 日支持与格式不可降级说明进入 release 文档。
- [x] 本任务以发布签名 candidate 与可工作的晋级门为完成；同一 digest 的 stable 晋级由 surreal_ck 双仓发布验收触发，避免仓间循环依赖。

## Dependencies

- Blocked by: [`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)
- Blocks: [`surreal_ck：完成双仓 E2E、部署切换与发布验收`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/10-cross-repo-e2e-release.md)

## Completion notes

- 2026-07-25：官方 upstream 被固定为无 push URL 的只读 remote；每周同步只创建
  `automation/upstream-sync-*` PR，冲突只开 issue，不再直推 `main` 或反向推送官方仓库。
  `releases/sck-*` ruleset 禁止删除/force-push，并要求 review 后合并。
- 2026-07-25：fork 官方 `Release` 工作流在非 `surrealdb/surrealdb` 仓库完整跳过；
  stable/nightly 使用独立 GHCR repository，candidate gate 只接受 manifest 指定的 stable
  repository、`candidate` channel 和受保护 `releases/sck-3.3` 上通过精确 SHA CI 的提交。
- 2026-07-25：candidate workflow 串联 capability/format/release 契约、memory/RocksDB
  hard-quota suite、RocksDB crash/restart、固定 performance baseline、amd64/arm64 CLI 与
  multi-arch image smoke。任一 gate、HIGH/CRITICAL 漏洞检查或身份校验失败时，不创建
  GitHub pre-release。
- 2026-07-25：同一次 CLI build 组装 multi-arch image；release/full-SHA tag 不可覆盖，
  中断重试只能复用两个 tag 已共同指向的 digest。镜像生成 SPDX SBOM、BuildKit
  provenance、漏洞报告与 keyless Cosign 签名；CLI 和 candidate manifest 也签名。
- 2026-07-25：签名 candidate manifest 将 CLI、OCI labels、运行中 RocksDB
  `/capabilities`、兼容清单绑定到同一完整 git SHA、fork release 与 manifest revision；
  同时固定 surrealdb-js `2.0.8`、production-certified RocksDB、format/mixed-version
  与 rollback contract。
- 2026-07-25：canary/staging/production 不含 build/push，只验证原 candidate 签名并
  smoke 同一 digest 后生成签名晋级收据。production 不在手动选项中，只接受
  `surreal_ck` 指定 workflow 的 keyless 签名验收 dispatch，且只记录 digest reference。
- 2026-07-25：`doc/NATIVE_QUOTA_RELEASES.md` 记录环境配置、双仓 dispatch 契约、
  exact-release-only mixed-version 策略、data format 不可降级和上一 production line
  至少 90 日支持窗口。
- 2026-07-29：双仓真机验收发现 quota admission 在 implicit commit 失败时被包装为
  `Query/NotExecuted`；executor 现只对 native quota commit error 保留
  `Quota` kind/details，并由 RocksDB + surrealdb-js HTTP/WSS E2E 锁定错误保真。
- 2026-07-29：candidate workflow 原先按源文件模块名过滤宏展开的 backend contracts，
  会产生 0-test 假绿。门禁现先枚举并要求每个 backend 至少发现 4 个
  `kvs::tests::<backend>::quota_*` 测试，再按完整生成路径执行。
- 2026-07-30：首条 protected storage line 收紧为 exact-release-only；marker 的旧字段名
  为保持 revision-1 编码而保留，但较低或较高 fork release 均 fail-closed。新增
  `ResourceKind::Quota` 保留在当前 revision 5，并重生成 `revision.lock`：这样既不改变
  v3.1.1 既有枚举值的 frozen wire header，又由 exact-release storage marker 阻止
  vanilla 或其它 fork release 解释 fork-only quota 数据。
