#!/usr/bin/env python3
"""Check CLI downloads against the already signature-verified candidate manifest."""
import argparse
import hashlib
import json
import re
import tarfile
from pathlib import Path


def verify(candidate: Path, directory: Path) -> None:
    manifest = json.loads(candidate.read_text())
    artifacts = manifest['cli']['artifacts']
    expected_arches = {'amd64': 62, 'arm64': 183}
    if len(artifacts) != 2 or {a['architecture'] for a in artifacts} != set(expected_arches):
        raise ValueError('candidate must list exactly one CLI for each architecture')
    for item in artifacts:
        arch = item['architecture']
        name = f'surreal-native-quota.linux-{arch}.tgz'
        if item['name'] != name or not re.fullmatch(r'[0-9a-f]{64}', item['sha256']):
            raise ValueError(f'invalid candidate CLI identity for {arch}')
        archive = directory / name
        with archive.open('rb') as source:
            digest = hashlib.file_digest(source, 'sha256').hexdigest()
        checksum = (directory / f'surreal-native-quota.linux-{arch}.txt').read_text().strip()
        if digest != item['sha256'] or checksum != digest:
            raise ValueError(f'CLI checksum mismatch for {arch}')
        bundle = json.loads((directory / f'{name}.sigstore.json').read_text())
        if not isinstance(bundle, dict) or not bundle:
            raise ValueError(f'missing signature bundle for {arch}')
        with tarfile.open(archive, 'r:gz') as tar:
            members = tar.getmembers()
            if (len(members) != 1 or members[0].name != 'surreal'
                    or not members[0].isfile() or not members[0].mode & 0o111):
                raise ValueError(f'unexpected CLI archive layout for {arch}')
            stream = tar.extractfile(members[0])
            if stream is None:
                raise ValueError(f'cannot read CLI binary for {arch}')
            header = stream.read(20)
        if (len(header) < 20 or header[:4] != b'\x7fELF' or header[4:6] != b'\x02\x01'
                or int.from_bytes(header[18:20], 'little') != expected_arches[arch]):
            raise ValueError(f'CLI ELF architecture mismatch for {arch}')
        print(f'{arch}: archive, checksum, ELF architecture and signature bundle structure verified')


def verify_evidence(candidate: Path, directory: Path) -> None:
    manifest = json.loads(candidate.read_text())
    evidence = manifest['evidence']
    expected = [
        (manifest['compatibility'], 'document', 'native-quota-v1.json'),
        (manifest['capability'], 'document', 'native-quota-capabilities.json'),
        (evidence['image_signature'], 'verification_document', 'image-signature-verification.json'),
        (evidence['sbom'], 'document', 'native-quota-image.spdx.json'),
        (evidence['provenance'], 'document', 'native-quota-image.provenance.intoto.jsonl'),
        (evidence['image_index'], 'document', 'native-quota-image.index.json'),
        (evidence['vulnerability_report'], 'document', 'native-quota-image.vulnerabilities.json'),
    ]
    for item, key, name in expected:
        if item[key] != name or not re.fullmatch(r'[0-9a-f]{64}', item['sha256']):
            raise ValueError(f'invalid evidence identity: {name}')
        with (directory / name).open('rb') as source:
            digest = hashlib.file_digest(source, 'sha256').hexdigest()
        if digest != item['sha256']:
            raise ValueError(f'evidence checksum mismatch: {name}')
    print('compatibility, capability and supply-chain evidence checksums verified')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', required=True, type=Path)
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--verify-evidence', action='store_true')
    args = parser.parse_args()
    verify(args.candidate, args.directory)
    if args.verify_evidence:
        verify_evidence(args.candidate, args.directory)
