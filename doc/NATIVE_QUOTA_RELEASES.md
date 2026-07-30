# Native quota fork release and rollback policy

This document applies only to the MainListActivity native-quota fork. The
upstream `doc/RELEASING.md` process remains disabled on forks and must not be
used for native-quota artifacts.

## Release identity

The machine-readable contract is
[`compatibility/native-quota-v1.json`](../compatibility/native-quota-v1.json).
For each release it pins:

- the fork release and compatibility manifest revision;
- exact supported SurrealDB JavaScript SDK releases and protocols;
- matching CLI release;
- quota, INFO, error, storage, catalog, and usage format revisions;
- production-certified storage backends;
- mixed-version, rollback, and support policy;
- stable and nightly image repositories and required attestations.

The stable image repository is
`ghcr.io/mainlistactivity/surrealdb-native-quota`. The isolated nightly
repository is `ghcr.io/mainlistactivity/surrealdb-native-quota-nightly`.
Official SurrealDB image namespaces are forbidden. A nightly artifact cannot be
promoted: the candidate verifier accepts only a `candidate` manifest from the
stable repository, built on the manifest's `releases/sck-<major>.<minor>`
branch.

## Upstream and stable branches

The `upstream` Git remote is read-only. The weekly workflow fetches
`https://github.com/surrealdb/surrealdb.git`, disables that remote's push URL,
and opens a PR into fork `main`; it never merges or pushes `main`. Merge
conflicts create an issue for manual resolution.

Stable work lives on `releases/sck-<major>.<minor>`. Apply
`native-quota-release-lines` with the manual
`Configure native quota repository policy` workflow before cutting the first
release branch. The ruleset rejects deletion and non-fast-forward updates and
requires a reviewed PR. Its GitHub App needs only repository
administration-write on this fork.

## Candidate build

`Native quota release candidate` starts only after the exact release-line SHA
has a successful upstream `CI` run, or after a manual request that proves the
same fact through the Actions API. It then runs:

1. compatibility, capability, format, and release-contract tests;
2. memory and RocksDB hard-quota contract suites, including RocksDB
   restart/fault certification;
3. the fixed-workload RocksDB regression benchmark;
4. one amd64 and one arm64 CLI build for the same release and Git SHA;
5. one multi-architecture OCI build from those CLI artifacts;
6. multi-architecture smoke tests and a RocksDB `/capabilities` identity check;
7. an SPDX SBOM, BuildKit SLSA provenance, HIGH/CRITICAL vulnerability gate,
   keyless image signature, CLI blob signatures, and signed candidate manifest.

The release and full-SHA OCI tags are checked for existence before publishing
and are never overwritten. The GitHub pre-release is created last, so no
candidate can exist when an earlier quota, backend, performance, capability,
architecture, vulnerability, or signing gate fails.

The candidate manifest proves that CLI artifacts, image labels, the live
capability document, compatibility manifest, fork release, and source all use
the same full Git SHA and release. Deploy the `repository@sha256:...` reference
from this manifest, never a tag.

## Promotion

Create GitHub environments named:

- `native-quota-candidate`
- `native-quota-canary`
- `native-quota-staging`
- `native-quota-production`
- `native-quota-repository-admin`

Configure required reviewers and deployment protection rules for the three
promotion environments. Candidate publication has package-write permission;
promotion has package-read permission and cannot build or push an image.

Promotion is strictly `canary -> staging -> production`. Each stage:

- downloads and verifies the signed candidate;
- verifies the image signature and every prior promotion receipt;
- smokes the exact amd64/arm64 digest without rebuilding;
- creates a signed, immutable receipt whose deployment reference is the same
  digest.

Canary and staging are manual. Production is intentionally absent from the
manual choice list. It is triggered only by a
`native-quota-stable-accepted` repository dispatch from the downstream
`MainListActivity/surreal_ck` acceptance workflow. The dispatch contains:

```json
{
  "event_type": "native-quota-stable-accepted",
  "client_payload": {
    "candidate_tag": "sck-3.3.0-native-quota.1-candidate.123",
    "release": "3.3.0-native-quota.1",
    "git_sha": "<40 lowercase hex>",
    "digest": "sha256:<64 lowercase hex>",
    "acceptance_statement_b64": "<base64 JSON>",
    "acceptance_bundle_b64": "<base64 Sigstore bundle>"
  }
}
```

The signed acceptance statement must identify the downstream repository and
same release/SHA/digest, link its Actions run, set its decision to `accepted`,
and mark contract, E2E, and deployment gates `passed`. Promotion verifies the
Sigstore certificate identity against the exact downstream workflow path
recorded in the compatibility manifest. A forged dispatch without that keyless
workflow identity cannot promote.

The downstream workflow may explicitly record
`waived_no_certificate` for its own duplicate verification of the candidate
manifest and image signatures. That waiver does not weaken promotion:
this workflow still verifies the candidate manifest, image, prior receipts, and
the downstream acceptance statement with GitHub OIDC keyless identities before
production. Keyless signing and verification do not require a project-owned
long-lived certificate. Digest, manifest hashes, SBOM, provenance,
vulnerability, multi-architecture, capability, backend, and E2E gates remain
mandatory under the downstream waiver.

Production creates the stable GitHub release but creates no production OCI
tag. Runtime configuration must pin the digest recorded in the production
receipt. This keeps CI, canary, staging, and production on one signed digest.

## Mixed versions, rollback, and support

Native-quota storage is `exact-release-only` for the initial line:

- do not mix vanilla SurrealDB, older fork binaries, or unlisted fork releases
  in one datastore or cluster;
- do not use a nightly image in canary, staging, or production;
- a process/configuration rollback is allowed only to a release that declares
  the same protected storage, catalog, and usage formats;
- datastore format migration is one-way. Do not attempt an in-place data-format
  downgrade. Restore a pre-migration backup into a separate datastore if a
  data rollback is required.

After promoting a new production line, the immediately previous production
line receives security and critical quota-correctness fixes for at least 90
days. Keep its digest, matching CLI, manifest, SBOM, provenance, signatures,
vulnerability report, and restore-tested backup available for that window.
The 90-day promise is support for a format-compatible process line, not
permission to downgrade a datastore after migration.
