#!/usr/bin/env python3

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("release_manifest.py")
SPEC = importlib.util.spec_from_file_location("release_manifest", MODULE_PATH)
assert SPEC and SPEC.loader
release_manifest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_manifest)


class ReleaseManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).parents[2]
        self.compatibility_path = self.root / "compatibility/native-quota-v1.json"
        self.compatibility = json.loads(self.compatibility_path.read_text())
        self.sha = "a" * 40
        self.digest = "sha256:" + "b" * 64
        self.candidate = {
            "format_version": 1,
            "channel": "candidate",
            "source": {
                "repository": "MainListActivity/surrealdb",
                "git_sha": self.sha,
                "stable_branch": "releases/sck-3.3",
                "ci_run_url": "https://github.com/MainListActivity/surrealdb/actions/runs/1",
            },
            "fork": {
                "id": self.compatibility["fork_id"],
                "release": self.compatibility["fork_release"],
                "manifest_revision": self.compatibility["manifest_revision"],
            },
            "image": {
                "repository": self.compatibility["release_supply_chain"]["image_repository"],
                "digest": self.digest,
                "reference": (
                    self.compatibility["release_supply_chain"]["image_repository"]
                    + "@"
                    + self.digest
                ),
                "immutable_tags": [
                    self.compatibility["fork_release"],
                    "sha-" + self.sha,
                ],
                "platform_digests": {
                    "amd64": "sha256:" + "c" * 64,
                    "arm64": "sha256:" + "d" * 64,
                },
                "labels": {
                    **self.compatibility["oci_labels"],
                    "org.opencontainers.image.revision": self.sha,
                    "org.opencontainers.image.version": self.compatibility["fork_release"],
                },
            },
            "cli": {
                "release": self.compatibility["fork_release"],
                "git_sha": self.sha,
                "artifacts": [
                    {"architecture": "amd64", "name": "a.tgz", "sha256": "1" * 64},
                    {"architecture": "arm64", "name": "b.tgz", "sha256": "2" * 64},
                ],
            },
            "capability": {
                "release": self.compatibility["fork_release"],
                "git_sha": self.sha,
                "manifest_revision": self.compatibility["manifest_revision"],
            },
            "compatibility": {
                "sha256": release_manifest.file_sha256(self.compatibility_path),
                "support": self.compatibility["support"],
                "mixed_version": self.compatibility["mixed_version"],
            },
            "evidence": {
                "image_signature": {"sha256": "3" * 64},
                "sbom": {"sha256": "4" * 64},
                "provenance": {"sha256": "5" * 64},
                "image_index": {"sha256": "7" * 64},
                "vulnerability_report": {"sha256": "6" * 64},
                "required_attestations": self.compatibility["release_supply_chain"][
                    "required_attestations"
                ]
            },
            "promotion": {
                "digest": self.digest,
                "ordered_environments": ["canary", "staging", "production"],
                "production_reference": "digest-only",
                "stable_requires_downstream_acceptance": True,
            },
        }

    def test_valid_candidate(self) -> None:
        release_manifest.verify_candidate(
            self.candidate, self.compatibility, self.sha, self.digest
        )

    def test_rejects_digest_drift(self) -> None:
        self.candidate["promotion"]["digest"] = "sha256:" + "c" * 64
        with self.assertRaisesRegex(release_manifest.ManifestError, "promotion digest"):
            release_manifest.verify_candidate(self.candidate, self.compatibility)

    def test_rejects_official_or_shared_nightly_namespace(self) -> None:
        compatibility = json.loads(json.dumps(self.compatibility))
        compatibility["release_supply_chain"]["nightly_image_repository"] = compatibility[
            "release_supply_chain"
        ]["image_repository"]
        with self.assertRaisesRegex(release_manifest.ManifestError, "must differ"):
            release_manifest.compatibility_contract(compatibility)

    def test_production_receipt_requires_downstream_acceptance(self) -> None:
        receipt = {
            "format_version": 1,
            "stage": "production",
            "previous_stage": "staging",
            "candidate": {
                "release": self.compatibility["fork_release"],
                "git_sha": self.sha,
                "digest": self.digest,
                "reference": self.candidate["image"]["reference"],
            },
            "promotion": {
                "rebuild": False,
                "deployment_reference": self.candidate["image"]["reference"],
                "downstream_acceptance_run_url": "",
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            candidate_path = Path(temporary) / "candidate.json"
            receipt_path = Path(temporary) / "receipt.json"
            candidate_path.write_text(json.dumps(self.candidate))
            receipt_path.write_text(json.dumps(receipt))
            args = type(
                "Args",
                (),
                {
                    "candidate": candidate_path,
                    "compatibility": self.compatibility_path,
                    "receipt": receipt_path,
                    "stage": "production",
                    "expected_sha": self.sha,
                    "expected_digest": self.digest,
                },
            )()
            with self.assertRaisesRegex(
                release_manifest.ManifestError, "downstream acceptance"
            ):
                release_manifest.verify_receipt(args)

    def test_canary_receipt_preserves_candidate_digest(self) -> None:
        receipt = {
            "format_version": 1,
            "stage": "canary",
            "previous_stage": None,
            "candidate": {
                "release": self.compatibility["fork_release"],
                "git_sha": self.sha,
                "digest": self.digest,
                "reference": self.candidate["image"]["reference"],
            },
            "promotion": {
                "rebuild": False,
                "deployment_reference": self.candidate["image"]["reference"],
                "downstream_acceptance_run_url": "",
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            candidate_path = Path(temporary) / "candidate.json"
            receipt_path = Path(temporary) / "receipt.json"
            candidate_path.write_text(json.dumps(self.candidate))
            receipt_path.write_text(json.dumps(receipt))
            args = type(
                "Args",
                (),
                {
                    "candidate": candidate_path,
                    "compatibility": self.compatibility_path,
                    "receipt": receipt_path,
                    "stage": "canary",
                    "expected_sha": self.sha,
                    "expected_digest": self.digest,
                },
            )()
            release_manifest.verify_receipt(args)


if __name__ == "__main__":
    unittest.main()
