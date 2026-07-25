Status: open
Label: ready-for-agent
Assignee: unassigned

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

- [ ] 同一签名 digest 从 CI→canary→staging→production 晋级，环境不重建。
- [ ] nightly 与 stable channel 隔离；production 只 pin digest。
- [ ] 未通过任一 quota/backend gate 的 candidate 无法产生；未通过 surreal_ck 双仓验收的 candidate 无法晋级 stable。
- [ ] 匹配 CLI、manifest、image labels、capability 文档引用相同 git SHA/release。
- [ ] 上一 production release line 的 90 日支持与格式不可降级说明进入 release 文档。
- [ ] 本任务以发布签名 candidate 与可工作的晋级门为完成；同一 digest 的 stable 晋级由 surreal_ck 双仓发布验收触发，避免仓间循环依赖。

## Dependencies

- Blocked by: [`交付 capability、readiness、格式迁移与匹配 CLI`](06-capability-readiness-migration-cli.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)
- Blocks: [`surreal_ck：完成双仓 E2E、部署切换与发布验收`](/Users/y/IdeaProjects/surreal_ck/.scratch/native-resource-quota/issues/10-cross-repo-e2e-release.md)
