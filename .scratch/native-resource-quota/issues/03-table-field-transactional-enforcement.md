Status: open
Label: ready-for-agent
Assignee: unassigned

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

- [ ] `^ent_`、exact、重叠 table bucket 和 field 规则解析符合决策。
- [ ] 隐式 id/in/out 与 SCHEMALESS 未定义属性不计，所有显式 field 各计一项。
- [ ] 覆写/no-op/删除不存在目标不误增减；整表删除正确释放所有相关计数。
- [ ] 同事务删除后创建按最终净增量判断，rollback/savepoint 不泄漏。
- [ ] 策略切换并发事务要么按旧 generation 完成，要么整体返回 policy changed。

## Dependencies

- Blocked by: [`建立 QUOTA grammar、catalog 与父层 IAM`](01-quota-resource-grammar-catalog-iam.md)、[`建立持续用量账本、epoch 与 datastore 格式围栏`](02-usage-ledger-epoch-format-fence.md)
- Blocks: [`交付 INFO、REBUILD、结构化错误与观测`](05-info-rebuild-errors-observability.md)、[`认证持久 backend、并发一致性与性能`](07-backend-certification-fault-performance.md)
