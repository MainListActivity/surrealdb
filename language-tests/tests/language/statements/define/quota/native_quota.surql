/**
[test]
reason = "Database quota policies enforce singleton creation, optimistic generation guards, canonical no-op updates, and guarded removal."

[[test.results]]
match = "$result.operation = 'define_quota' AND $result.changed = true AND $result.after.generation = 1"

[[test.results]]
error = "The quota policy for database 'test' already exists"

[[test.results]]
match = "$result.operation = 'define_quota' AND $result.changed = false AND $result.after.generation = 1"

[[test.results]]
error = "The quota policy for database 'test' requires `EXPECT GENERATION`"

[[test.results]]
error = "The quota policy for database 'test' has generation 1, expected 9"

[[test.results]]
match = "$result.operation = 'define_quota' AND $result.changed = false AND $result.after.generation = 1"

[[test.results]]
match = "$result.operation = 'alter_quota' AND $result.changed = false AND $result.after.generation = 1"

[[test.results]]
match = "$result.operation = 'alter_quota' AND $result.changed = true AND $result.after.generation = 2"

[[test.results]]
error = "The quota policy for database 'test' has generation 2, expected 1"

[[test.results]]
match = "$result.operation = 'remove_quota' AND $result.changed = true AND $result.after.generation = 3"

[[test.results]]
match = "$result.operation = 'remove_quota' AND $result.changed = false AND $result.after.generation = 3"

[[test.results]]
error = "The quota policy for database 'test' does not exist"

[[test.results]]
match = "$result.operation = 'define_quota' AND $result.changed = true AND $result.after.generation = 4"

[[test.results]]
error = "The quota policy for database 'test' has generation 4, expected 2"
*/

DEFINE QUOTA ON DATABASE test
	RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100
	RULE fields FOR FIELD MATCH EXACT user LIMIT 20;

DEFINE QUOTA ON DATABASE test;

DEFINE QUOTA IF NOT EXISTS ON DATABASE test;

DEFINE QUOTA OVERWRITE ON DATABASE test;

DEFINE QUOTA OVERWRITE ON DATABASE test EXPECT GENERATION 9;

DEFINE QUOTA OVERWRITE ON DATABASE test EXPECT GENERATION 1
	RULE fields FOR FIELD MATCH EXACT user LIMIT 20
	RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100;

ALTER QUOTA ON DATABASE test EXPECT GENERATION 1
	SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 100
	DROP RULE IF EXISTS missing;

ALTER QUOTA ON DATABASE test EXPECT GENERATION 1
	SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 101;

ALTER QUOTA ON DATABASE test EXPECT GENERATION 1
	SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 102;

REMOVE QUOTA ON DATABASE test EXPECT GENERATION 2;

REMOVE QUOTA IF EXISTS ON DATABASE test EXPECT GENERATION 999;

REMOVE QUOTA ON DATABASE test EXPECT GENERATION 2;

DEFINE QUOTA ON DATABASE test
	RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 101;

ALTER QUOTA ON DATABASE test EXPECT GENERATION 2
	SET RULE records FOR RECORD MATCH REGEX /^ent/ LIMIT 102;
