/**
[env]
imports = ["upgrade/define/table_import.surql"]

[test]
upgrade = true

[[test.results]]
match = "$result.tables.table != NONE AND $result.tables.table_full != NONE AND $result.tables.table_specific != NONE"
*/

INFO FOR DB;
