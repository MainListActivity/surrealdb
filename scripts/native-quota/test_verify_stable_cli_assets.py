import importlib.util
import io
import json
import hashlib
import tarfile
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('verify_assets', Path(__file__).with_name('verify_stable_cli_assets.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class VerifyCliAssets(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.manifest = {'cli': {'artifacts': []}}
        for arch, machine in [('amd64', 62), ('arm64', 183)]:
            name = f'surreal-native-quota.linux-{arch}.tgz'
            header = bytearray(64)
            header[:6] = b'\x7fELF\x02\x01'
            header[18:20] = machine.to_bytes(2, 'little')
            with tarfile.open(self.root / name, 'w:gz') as tar:
                info = tarfile.TarInfo('surreal')
                info.size = len(header)
                info.mode = 0o755
                tar.addfile(info, io.BytesIO(header))
            digest = hashlib.sha256((self.root / name).read_bytes()).hexdigest()
            (self.root / f'surreal-native-quota.linux-{arch}.txt').write_text(digest + '\n')
            (self.root / f'{name}.sigstore.json').write_text('{"bundle":true}')
            self.manifest['cli']['artifacts'].append({'architecture': arch, 'name': name, 'sha256': digest})
        self.candidate = self.root / 'candidate.json'
        self.candidate.write_text(json.dumps(self.manifest))

    def test_valid_archives(self):
        module.verify(self.candidate, self.root)

    def test_rejects_changed_checksum(self):
        (self.root / 'surreal-native-quota.linux-arm64.txt').write_text('0' * 64)
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            module.verify(self.candidate, self.root)

    def test_rejects_wrong_elf_architecture_even_with_matching_digest(self):
        item = self.manifest['cli']['artifacts'][1]
        source = self.root / self.manifest['cli']['artifacts'][0]['name']
        target = self.root / item['name']
        target.write_bytes(source.read_bytes())
        item['sha256'] = hashlib.sha256(target.read_bytes()).hexdigest()
        (self.root / 'surreal-native-quota.linux-arm64.txt').write_text(item['sha256'])
        self.candidate.write_text(json.dumps(self.manifest))
        with self.assertRaisesRegex(ValueError, 'ELF architecture mismatch'):
            module.verify(self.candidate, self.root)

    def test_evidence_is_bound_to_signed_manifest(self):
        names = {
            'image_signature': ('verification_document', 'image-signature-verification.json'),
            'sbom': ('document', 'native-quota-image.spdx.json'),
            'provenance': ('document', 'native-quota-image.provenance.intoto.jsonl'),
            'image_index': ('document', 'native-quota-image.index.json'),
            'vulnerability_report': ('document', 'native-quota-image.vulnerabilities.json'),
        }
        self.manifest['evidence'] = {}
        for key, (field, name) in names.items():
            data = name.encode()
            (self.root / name).write_bytes(data)
            self.manifest['evidence'][key] = {field: name, 'sha256': hashlib.sha256(data).hexdigest()}
        for key, name in [('compatibility', 'native-quota-v1.json'), ('capability', 'native-quota-capabilities.json')]:
            data = name.encode()
            (self.root / name).write_bytes(data)
            self.manifest[key] = {'document': name, 'sha256': hashlib.sha256(data).hexdigest()}
        self.candidate.write_text(json.dumps(self.manifest))
        module.verify_evidence(self.candidate, self.root)
        (self.root / 'native-quota-image.spdx.json').write_text('tampered')
        with self.assertRaisesRegex(ValueError, 'evidence checksum mismatch'):
            module.verify_evidence(self.candidate, self.root)

    def test_rejects_path_substitution(self):
        self.manifest['cli']['artifacts'][0]['name'] = '../outside.tgz'
        self.candidate.write_text(json.dumps(self.manifest))
        with self.assertRaisesRegex(ValueError, 'invalid candidate CLI identity'):
            module.verify(self.candidate, self.root)


if __name__ == '__main__':
    unittest.main()
