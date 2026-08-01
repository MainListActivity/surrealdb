/**
[env]
imports = ["upgrade/define/user_duration_import.surql"]

[test]
upgrade = true

[[test.results]]
match = "$result.users.user_session_duration != NONE AND $result.users.user_session_none != NONE AND $result.users.user_token_duration != NONE"
*/
INFO FOR DB;
