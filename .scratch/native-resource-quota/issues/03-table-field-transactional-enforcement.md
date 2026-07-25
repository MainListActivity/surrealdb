Status: done
Label: done
Assignee: /root

# SDB-NQ-03 — 接入 table/field catalog 事务强制

## Parent

[`SurrealDB 原生资源配额实施规格`](../PRD.md)

## What to build

- 在真实 table/field catalog 存在状态转换处计算 signed net delta，不在 parser 或业务元数据层计数。
- table 同时消费所有命中 exact/regex 规则桶；field 按命中表独立计数并实现 exact-over-regex/min-regex 解析。
- 覆盖普通表、relation、view、显式 nested field、OVERWRITE/no-op、IF EXISTS、REMOVE、EXPUNGE 与整表删除结算。
- table 删除直接清理该表 field/record counter 并结算 table buckets，不逐 record 递减。
- 接入 generation fence、超额非恶化准入和 quota transaction facade。

## Acceptance criteria

- [x] `^ent_`、exact、重叠 table bucket 和 field 规则解析符合决策。
- [x] 隐式 id/in/out 与 SCHEMALESS 未定义属性不计，所有显式 field 各计一项。
- [x] 覆写/no-op/删除不存在目标不误增减；整表删除正确释放所有相关计数。
- [x] 同事务删除后创建按最终净增量判断，rollback/savepoint 不泄漏。
- [x] 策略切换并发事务要么按旧 generation 完成，要么整体返回 policy changed。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md)、[`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)
- Blocks: [`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)

## Comments

- 2026-07-25：table/field 真实 catalog 存在状态转换已接入 transaction-local signed delta；commit 前按最终净变化解析规则并对每个 counter 只做一次条件写。table 同时消费全部 exact/regex 桶；field 实现 exact-over-regex、否则最小 finite regex（exact unlimited 同样覆盖 regex）。
- 2026-07-25：策略创建/切换会按当前 table catalog 初始化新 generation 桶，降低限额允许形成 over-limit，之后只允许非恶化变化。在途旧 generation 写通过 `Qg`/`Qm` fence 与策略切换整体冲突并由事务重试重新绑定；稳定 `policy_changed` 协议映射留在 SDB-NQ-05。
- 2026-07-25：覆盖显式 nested field、relation 隐式 in/out、SCHEMALESS 动态属性、view、OVERWRITE/no-op、IF EXISTS、REMOVE、AND EXPUNGE、整表 field/record 清零、删除后重建、savepoint rollback 与并发 generation switch。无策略 field 用量持续计量。
- 2026-07-25：事务 quota state 使用堆上状态，避免放大深层 IAM async future 的默认线程栈。验证：table/field 10/10、既有 quota 7/7、usage 14/14、IAM bearer 回归通过；`surrealdb-core` 全量 2,922 passed / 9 ignored；Clippy `-D warnings` 与全仓 rustfmt check 通过。
