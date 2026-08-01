/**
[env]
imports = ["upgrade/define/table_type_import.surql"]

[test]
upgrade = true

[[test.results]]
match = "$result.tables.any != NONE AND $result.tables.normal != NONE AND $result.tables.relation != NONE AND $result.tables.relation_enforced != NONE AND $result.tables.relation_none != NONE"

*/
INFO FOR DB;
