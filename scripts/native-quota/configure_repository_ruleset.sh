#!/usr/bin/env bash
set -euo pipefail

: "${GH_TOKEN:?GH_TOKEN must be a GitHub App token with repository administration:write}"
: "${GH_REPO:?GH_REPO must identify the private fork}"

if [[ "$GH_REPO" == "surrealdb/surrealdb" ]]; then
	echo "refusing to configure the official upstream repository" >&2
	exit 1
fi

ruleset=".github/rulesets/native-quota-release-lines.json"
if [[ ! -f "$ruleset" ]]; then
	echo "missing ruleset contract: $ruleset" >&2
	exit 1
fi

name="$(jq -er '.name' "$ruleset")"
existing="$(
	gh api "repos/${GH_REPO}/rulesets" \
		--jq ".[] | select(.name == \"${name}\") | .id" |
		head -n1
)"

if [[ -n "$existing" ]]; then
	gh api \
		--method PUT \
		"repos/${GH_REPO}/rulesets/${existing}" \
		--input "$ruleset" >/dev/null
	echo "updated ruleset ${name} (${existing})"
else
	gh api \
		--method POST \
		"repos/${GH_REPO}/rulesets" \
		--input "$ruleset" >/dev/null
	echo "created ruleset ${name}"
fi

gh api "repos/${GH_REPO}/rulesets" \
	--jq ".[] | select(.name == \"${name}\") | {
		id,
		name,
		enforcement,
		target
	}"
