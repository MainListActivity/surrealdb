
/**
[env]
imports = ["upgrade/define/param_import.surql"]

[test]
upgrade = true

[[test.results]]
match = "$result.params.comment != NONE AND $result.params.permissions_full != NONE AND $result.params.permissions_none != NONE AND $result.params.permissions_specifics != NONE AND $result.params.value != NONE"


*/
INFO FOR DB;
