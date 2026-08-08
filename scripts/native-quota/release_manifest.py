#!/usr/bin/env python3
"""Render and verify immutable native-quota release and promotion manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
STAGES = ["canary", "staging", "production"]


class ManifestError(ValueError):
    """Release contract validation failed."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ManifestError(f"{path} must contain a JSON object")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ManifestError(message)


def compatibility_contract(compatibility: dict[str, Any]) -> dict[str, Any]:
    supply = compatibility.get("release_supply_chain")
    require(isinstance(supply, dict), "compatibility manifest lacks release_supply_chain")
    require(
        supply.get("candidate_manifest_format") == 1,
        "unsupported candidate manifest format",
    )
    require(
        supply.get("promotion_environments") == STAGES,
        "promotion environments must be canary -> staging -> production",
    )
    repository = supply.get("image_repository")
    nightly_repository = supply.get("nightly_image_repository")
    require(
        isinstance(repository, str) and repository.startswith("ghcr.io/"),
        "candidate image repository must be a GHCR repository",
    )
    require(
        isinstance(nightly_repository, str) and nightly_repository.startswith("ghcr.io/"),
        "nightly image repository must be a GHCR repository",
    )
    require(repository != nightly_repository, "nightly and stable repositories must differ")
    require(
        "surrealdb/surrealdb" not in repository
        and "surrealdb/surrealdb" not in nightly_repository,
        "fork release must not use an official SurrealDB image namespace",
    )
    require(
        supply.get("production_reference") == "digest-only",
        "production must use a digest-only image reference",
    )
    return supply


def find_cli_artifacts(directory: Path) -> list[dict[str, str]]:
    artifacts = []
    for path in sorted(directory.rglob("*.tgz")):
        if "linux-amd64" in path.name:
            architecture = "amd64"
        elif "linux-arm64" in path.name:
            architecture = "arm64"
        else:
            continue
        artifacts.append(
            {
                "architecture": architecture,
                "name": path.name,
                "sha256": file_sha256(path),
            }
        )
    require(
        {item["architecture"] for item in artifacts} == {"amd64", "arm64"},
        "matching amd64 and arm64 CLI archives are required",
    )
    require(len(artifacts) == 2, "exactly one CLI archive per architecture is required")
    return artifacts


def platform_digests_from_index(index: dict[str, Any]) -> dict[str, str]:
    """Extract the immutable Linux platform manifests from an OCI index."""
    manifests = index.get("manifests")
    require(isinstance(manifests, list), "image index lacks manifests")
    platform_digests: dict[str, str] = {}
    for manifest in manifests:
        if not isinstance(manifest, dict):
            continue
        platform = manifest.get("platform")
        if not isinstance(platform, dict):
            continue
        if platform.get("os") != "linux" or platform.get("architecture") not in {"amd64", "arm64"}:
            continue
        architecture = platform["architecture"]
        digest = manifest.get("digest")
        require(isinstance(digest, str) and bool(DIGEST_RE.fullmatch(digest)), "image platform digest is invalid")
        require(architecture not in platform_digests, "image index contains duplicate platform manifests")
        platform_digests[architecture] = digest
    require(set(platform_digests) == {"amd64", "arm64"}, "matching amd64 and arm64 image manifests are required")
    return platform_digests


def render_candidate(args: argparse.Namespace) -> None:
    compatibility = load_json(args.compatibility)
    supply = compatibility_contract(compatibility)
    capability = load_json(args.capability)
    image_index = load_json(args.image_index)
    platform_digests = platform_digests_from_index(image_index)
    sha = args.git_sha.lower()
    digest = args.image_digest.lower()
    require(bool(SHA_RE.fullmatch(sha)), "git SHA must be full 40-character lowercase hex")
    require(bool(DIGEST_RE.fullmatch(digest)), "image digest must be sha256:<64 hex>")
    release = compatibility.get("fork_release")
    revision = compatibility.get("manifest_revision")
    repository = supply["image_repository"]
    require(args.repository == repository, "workflow image repository differs from compatibility")
    require(capability.get("manifest_revision") == revision, "capability manifest revision differs")
    require(capability.get("fork", {}).get("id") == compatibility.get("fork_id"), "capability fork differs")
    require(capability.get("fork", {}).get("release") == release, "capability release differs")
    require(capability.get("build", {}).get("git_sha") == sha, "capability git SHA differs")
    require(capability.get("cli") == compatibility.get("cli"), "capability CLI contract differs")
    require(
        capability.get("backend", {}).get("production") is True,
        "candidate capability must report a production-certified backend",
    )
    labels = dict(compatibility.get("oci_labels", {}))
    labels.update(
        {
            "org.opencontainers.image.revision": sha,
            "org.opencontainers.image.version": release,
            "io.mainlistactivity.surrealdb.channel": "candidate",
        }
    )
    candidate = {
        "format_version": supply["candidate_manifest_format"],
        "channel": "candidate",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "repository": args.source_repository,
            "git_sha": sha,
            "stable_branch": supply["stable_branch"],
            "ci_run_url": args.ci_run_url,
        },
        "fork": {
            "id": compatibility["fork_id"],
            "release": release,
            "manifest_revision": revision,
        },
        "image": {
            "repository": repository,
            "digest": digest,
            "reference": f"{repository}@{digest}",
            "immutable_tags": [release, f"sha-{sha}"],
            "platform_digests": platform_digests,
            "labels": labels,
        },
        "cli": {
            "release": compatibility["cli"]["release"],
            "git_sha": sha,
            "artifacts": find_cli_artifacts(args.artifacts_dir),
        },
        "capability": {
            "release": capability["fork"]["release"],
            "git_sha": capability["build"]["git_sha"],
            "manifest_revision": capability["manifest_revision"],
            "document": args.capability.name,
            "sha256": file_sha256(args.capability),
        },
        "compatibility": {
            "document": args.compatibility.name,
            "sha256": file_sha256(args.compatibility),
            "contracts": compatibility["contracts"],
            "formats": compatibility["formats"],
            "sdk": compatibility["sdk"],
            "production_backends": [
                backend["name"]
                for backend in compatibility["backends"]
                if backend.get("production") is True
            ],
            "mixed_version": compatibility["mixed_version"],
            "support": compatibility["support"],
        },
        "evidence": {
            "image_signature": {
                "kind": "keyless-cosign",
                "verification_document": args.image_signature_verification.name,
                "sha256": file_sha256(args.image_signature_verification),
            },
            "candidate_manifest_signature": "native-quota-release-candidate.sigstore.json",
            "sbom": {"document": args.sbom.name, "sha256": file_sha256(args.sbom)},
            "provenance": {
                "document": args.provenance.name,
                "sha256": file_sha256(args.provenance),
            },
            "image_index": {
                "document": args.image_index.name,
                "sha256": file_sha256(args.image_index),
            },
            "vulnerability_report": {
                "document": args.vulnerability_report.name,
                "sha256": file_sha256(args.vulnerability_report),
            },
            "required_attestations": supply["required_attestations"],
        },
        "promotion": {
            "digest": digest,
            "ordered_environments": STAGES,
            "production_reference": supply["production_reference"],
            "stable_requires_downstream_acceptance": True,
            "downstream_repository": supply["downstream_repository"],
            "downstream_acceptance_workflow": supply["downstream_acceptance_workflow"],
        },
    }
    verify_candidate(candidate, compatibility, sha, digest)
    write_json(args.output, candidate)


def verify_candidate(
    candidate: dict[str, Any],
    compatibility: dict[str, Any],
    expected_sha: str | None = None,
    expected_digest: str | None = None,
) -> None:
    supply = compatibility_contract(compatibility)
    source = candidate.get("source", {})
    fork = candidate.get("fork", {})
    image = candidate.get("image", {})
    cli = candidate.get("cli", {})
    capability = candidate.get("capability", {})
    promotion = candidate.get("promotion", {})
    evidence = candidate.get("evidence", {})
    sha = source.get("git_sha")
    digest = image.get("digest")
    release = compatibility.get("fork_release")
    revision = compatibility.get("manifest_revision")

    require(candidate.get("format_version") == 1, "unsupported candidate format")
    require(candidate.get("channel") == "candidate", "only candidate channel is promotable")
    require(isinstance(sha, str) and bool(SHA_RE.fullmatch(sha)), "invalid source git SHA")
    require(isinstance(digest, str) and bool(DIGEST_RE.fullmatch(digest)), "invalid image digest")
    if expected_sha:
        require(sha == expected_sha.lower(), "candidate git SHA differs from expected SHA")
    if expected_digest:
        require(digest == expected_digest.lower(), "candidate digest differs from expected digest")
    require(source.get("stable_branch") == supply["stable_branch"], "stable branch differs")
    require(fork.get("id") == compatibility.get("fork_id"), "fork identity differs")
    require(fork.get("release") == release, "fork release differs")
    require(fork.get("manifest_revision") == revision, "manifest revision differs")
    require(image.get("repository") == supply["image_repository"], "image repository differs")
    require(image.get("reference") == f"{supply['image_repository']}@{digest}", "image reference is not digest-pinned")
    require(image.get("immutable_tags") == [release, f"sha-{sha}"], "immutable tags differ")
    platform_digests = image.get("platform_digests")
    require(
        isinstance(platform_digests, dict)
        and set(platform_digests) == {"amd64", "arm64"}
        and all(isinstance(value, str) and bool(DIGEST_RE.fullmatch(value)) for value in platform_digests.values()),
        "image platform digests are incomplete",
    )
    labels = image.get("labels", {})
    require(labels.get("org.opencontainers.image.revision") == sha, "image revision label differs")
    require(labels.get("org.opencontainers.image.version") == release, "image version label differs")
    require(
        labels.get("io.mainlistactivity.surrealdb.manifest-revision") == revision,
        "image manifest revision label differs",
    )
    require(cli.get("release") == release and cli.get("git_sha") == sha, "CLI identity differs")
    architectures = {item.get("architecture") for item in cli.get("artifacts", [])}
    require(architectures == {"amd64", "arm64"}, "CLI architectures are incomplete")
    require(
        capability.get("release") == release
        and capability.get("git_sha") == sha
        and capability.get("manifest_revision") == revision,
        "capability identity differs",
    )
    require(promotion.get("digest") == digest, "promotion digest differs")
    require(promotion.get("ordered_environments") == STAGES, "promotion order differs")
    require(promotion.get("production_reference") == "digest-only", "production reference differs")
    require(
        promotion.get("stable_requires_downstream_acceptance") is True,
        "stable promotion lacks downstream gate",
    )
    required = set(supply["required_attestations"])
    require(required.issubset(set(evidence.get("required_attestations", []))), "release evidence is incomplete")
    for evidence_name in [
        "image_signature",
        "sbom",
        "provenance",
        "image_index",
        "vulnerability_report",
    ]:
        evidence_item = evidence.get(evidence_name, {})
        require(
            isinstance(evidence_item, dict)
            and isinstance(evidence_item.get("sha256"), str)
            and bool(re.fullmatch(r"[0-9a-f]{64}", evidence_item["sha256"])),
            f"{evidence_name} is not cryptographically bound to the candidate",
        )
    require(
        candidate.get("compatibility", {}).get("support", {}).get(
            "previous_production_line_days"
        )
        >= 90,
        "previous production line support is shorter than 90 days",
    )
    require(
        candidate.get("compatibility", {})
        .get("mixed_version", {})
        .get("data_format_downgrade_supported")
        is False,
        "candidate incorrectly allows datastore format downgrade",
    )


def verify_candidate_command(args: argparse.Namespace) -> None:
    candidate = load_json(args.candidate)
    compatibility = load_json(args.compatibility)
    verify_candidate(candidate, compatibility, args.expected_sha, args.expected_digest)
    require(
        candidate.get("compatibility", {}).get("sha256")
        == file_sha256(args.compatibility),
        "candidate compatibility document hash differs",
    )


def render_receipt(args: argparse.Namespace) -> None:
    candidate = load_json(args.candidate)
    compatibility = load_json(args.compatibility)
    verify_candidate(candidate, compatibility, args.expected_sha, args.expected_digest)
    require(args.stage in STAGES, f"unknown promotion stage {args.stage}")
    previous = STAGES[STAGES.index(args.stage) - 1] if args.stage != "canary" else None
    receipt = {
        "format_version": 1,
        "stage": args.stage,
        "previous_stage": previous,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "candidate": {
            "release": candidate["fork"]["release"],
            "git_sha": candidate["source"]["git_sha"],
            "manifest_revision": candidate["fork"]["manifest_revision"],
            "digest": candidate["image"]["digest"],
            "reference": candidate["image"]["reference"],
        },
        "promotion": {
            "actor": args.actor,
            "run_url": args.run_url,
            "rebuild": False,
            "deployment_reference": candidate["image"]["reference"],
            "downstream_acceptance_run_url": args.downstream_acceptance_run_url,
        },
    }
    write_json(args.output, receipt)


def verify_receipt(args: argparse.Namespace) -> None:
    candidate = load_json(args.candidate)
    compatibility = load_json(args.compatibility)
    verify_candidate(candidate, compatibility, args.expected_sha, args.expected_digest)
    require(
        candidate.get("compatibility", {}).get("sha256")
        == file_sha256(args.compatibility),
        "candidate compatibility document hash differs",
    )
    receipt = load_json(args.receipt)
    require(receipt.get("format_version") == 1, "unsupported receipt format")
    require(receipt.get("stage") == args.stage, "receipt stage differs")
    expected_previous = STAGES[STAGES.index(args.stage) - 1] if args.stage != "canary" else None
    require(receipt.get("previous_stage") == expected_previous, "receipt previous stage differs")
    identity = receipt.get("candidate", {})
    require(identity.get("release") == candidate["fork"]["release"], "receipt release differs")
    require(identity.get("git_sha") == candidate["source"]["git_sha"], "receipt SHA differs")
    require(identity.get("digest") == candidate["image"]["digest"], "receipt digest differs")
    require(identity.get("reference") == candidate["image"]["reference"], "receipt reference differs")
    require(receipt.get("promotion", {}).get("rebuild") is False, "receipt records a rebuild")
    require(
        receipt.get("promotion", {}).get("deployment_reference") == candidate["image"]["reference"],
        "receipt deployment is not digest-pinned",
    )
    if args.stage == "production":
        require(
            bool(receipt.get("promotion", {}).get("downstream_acceptance_run_url")),
            "production receipt lacks downstream acceptance",
        )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    render = commands.add_parser("render")
    render.add_argument("--compatibility", type=Path, required=True)
    render.add_argument("--capability", type=Path, required=True)
    render.add_argument("--artifacts-dir", type=Path, required=True)
    render.add_argument("--sbom", type=Path, required=True)
    render.add_argument("--provenance", type=Path, required=True)
    render.add_argument("--image-index", type=Path, required=True)
    render.add_argument("--vulnerability-report", type=Path, required=True)
    render.add_argument("--image-signature-verification", type=Path, required=True)
    render.add_argument("--git-sha", required=True)
    render.add_argument("--image-digest", required=True)
    render.add_argument("--repository", required=True)
    render.add_argument("--source-repository", required=True)
    render.add_argument("--ci-run-url", required=True)
    render.add_argument("--output", type=Path, required=True)
    render.set_defaults(function=render_candidate)

    verify = commands.add_parser("verify")
    verify.add_argument("--candidate", type=Path, required=True)
    verify.add_argument("--compatibility", type=Path, required=True)
    verify.add_argument("--expected-sha")
    verify.add_argument("--expected-digest")
    verify.set_defaults(function=verify_candidate_command)

    receipt = commands.add_parser("receipt")
    receipt.add_argument("--candidate", type=Path, required=True)
    receipt.add_argument("--compatibility", type=Path, required=True)
    receipt.add_argument("--stage", choices=STAGES, required=True)
    receipt.add_argument("--expected-sha", required=True)
    receipt.add_argument("--expected-digest", required=True)
    receipt.add_argument("--actor", required=True)
    receipt.add_argument("--run-url", required=True)
    receipt.add_argument("--downstream-acceptance-run-url", default="")
    receipt.add_argument("--output", type=Path, required=True)
    receipt.set_defaults(function=render_receipt)

    verify_promotion = commands.add_parser("verify-receipt")
    verify_promotion.add_argument("--candidate", type=Path, required=True)
    verify_promotion.add_argument("--compatibility", type=Path, required=True)
    verify_promotion.add_argument("--receipt", type=Path, required=True)
    verify_promotion.add_argument("--stage", choices=STAGES, required=True)
    verify_promotion.add_argument("--expected-sha")
    verify_promotion.add_argument("--expected-digest")
    verify_promotion.set_defaults(function=verify_receipt)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        args.function(args)
    except ManifestError as error:
        print(f"native-quota release manifest error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
