/**
[env]
imports = ["upgrade/define/function_import.surql"]

[test]
upgrade = true

[[test.results]]
match = "$result.functions.test_function_1 != NONE AND $result.functions.test_function_2 != NONE"
*/
INFO FOR DB;
