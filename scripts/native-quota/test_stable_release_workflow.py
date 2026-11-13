"""Exercise the stable-release step without creating a GitHub release.

Cryptographic verification is tested separately against a real signed candidate;
these tests mock external commands to verify publication ordering and failure paths.
"""
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / '.github/workflows/native-quota-promote.yml'

MOCK_GH = r'''#!/usr/bin/env python3
import json, os, shutil, sys
from pathlib import Path
a=sys.argv[1:]; state=Path(os.environ['MOCK_STATE']); uploaded=Path(os.environ['MOCK_UPLOADED']); ref=Path(os.environ['MOCK_REFERENCE']); events=Path(os.environ['MOCK_EVENTS'])
with events.open('a') as f:f.write(json.dumps(a)+'\n')
releases=json.loads(state.read_text())
if a[:2]==['release','download']:
 d=Path(a[a.index('--dir')+1]);d.mkdir(parents=True,exist_ok=True)
 source=ref if a[2]=='candidate' else uploaded
 for p in source.iterdir():shutil.copy2(p,d/p.name)
elif a[:2]==['release','create']:
 tag=a[2]; paths=[]
 for x in a[3:]:
  if x.startswith('--'):break
  paths.append(Path(x))
 assets=[]
 for i,p in enumerate(paths,1):shutil.copy2(p,uploaded/p.name);assets.append({'name':p.name,'id':i})
 releases=[{'id':123,'tag_name':tag,'target_commitish':a[a.index('--target')+1],'draft':True,'assets':assets}];state.write_text(json.dumps(releases))
elif a[:2]==['api','--paginate']:print(json.dumps(releases))
elif a[:3]==['api','--method','PATCH']:
 assert releases and 'draft=false' in a and 'make_latest=true' in a
 releases[0]['draft']=False;state.write_text(json.dumps(releases));print('{}')
elif a[0]=='api' and '/releases/assets/' in ' '.join(a):
 i=int(next(x for x in a if '/releases/assets/' in x).rsplit('/',1)[1]);p=uploaded/releases[0]['assets'][i-1]['name'];data=p.read_bytes()
 if os.environ.get('MOCK_TAMPER')=='1' and p.name=='surreal-native-quota.linux-amd64.tgz':data=bytes([data[0]^1])+data[1:]
 sys.stdout.buffer.write(data)
elif a[0]=='api' and '/releases/latest' in ' '.join(a):print(123)
elif a[0]=='api' and '/releases/123' in ' '.join(a):print(str(not releases[0]['draft']).lower())
else:raise SystemExit('unexpected mock gh command '+repr(a))
'''


class StableReleaseWorkflow(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.work = root / 'work'; self.work.mkdir()
        self.bin = root / 'bin'; self.bin.mkdir()
        self.reference = root / 'reference'; self.reference.mkdir()
        self.uploaded = root / 'uploaded'; self.uploaded.mkdir()
        self.state = root / 'state.json'; self.state.write_text('[]')
        self.events = root / 'events.jsonl'
        scripts = self.work / 'scripts/native-quota'; scripts.mkdir(parents=True)
        shutil.copy2(ROOT / 'scripts/native-quota/verify_stable_cli_assets.py', scripts)
        self.candidate = {'cli': {'artifacts': []}, 'evidence': {}}
        for arch, machine in [('amd64', 62), ('arm64', 183)]:
            name = f'surreal-native-quota.linux-{arch}.tgz'
            header = bytearray(64); header[:6] = b'\x7fELF\x02\x01'; header[18:20] = machine.to_bytes(2, 'little')
            with tarfile.open(self.reference / name, 'w:gz') as tar:
                item = tarfile.TarInfo('surreal'); item.mode = 0o755; item.size = len(header)
                tar.addfile(item, io.BytesIO(header))
            digest = hashlib.sha256((self.reference / name).read_bytes()).hexdigest()
            (self.reference / f'surreal-native-quota.linux-{arch}.txt').write_text(digest + '\n')
            (self.reference / f'{name}.sigstore.json').write_text('{"mock":true}')
            self.candidate['cli']['artifacts'].append({'architecture': arch, 'name': name, 'sha256': digest})
        evidence = {
            'image_signature': ('verification_document', 'image-signature-verification.json'),
            'sbom': ('document', 'native-quota-image.spdx.json'),
            'provenance': ('document', 'native-quota-image.provenance.intoto.jsonl'),
            'image_index': ('document', 'native-quota-image.index.json'),
            'vulnerability_report': ('document', 'native-quota-image.vulnerabilities.json'),
        }
        for key, (field, name) in evidence.items():
            data = name.encode(); (self.reference / name).write_bytes(data)
            self.candidate['evidence'][key] = {field: name, 'sha256': hashlib.sha256(data).hexdigest()}
        for key, name in [('compatibility', 'native-quota-v1.json'), ('capability', 'native-quota-capabilities.json')]:
            data = name.encode(); (self.reference / name).write_bytes(data)
            self.candidate[key] = {'document': name, 'sha256': hashlib.sha256(data).hexdigest()}
        (self.reference / 'native-quota-release-candidate.json').write_text(json.dumps(self.candidate))
        (self.reference / 'native-quota-release-candidate.sigstore.json').write_text('{}')
        for stage in ['canary', 'staging', 'production']:
            for suffix in ['json', 'sigstore.json']:
                (self.reference / f'promotion-{stage}.{suffix}').write_text('{}')
        for name in ['native-quota-release-candidate.json', 'native-quota-release-candidate.sigstore.json', 'native-quota-v1.json'] + [f'promotion-{stage}.{suffix}' for stage in ['canary', 'staging', 'production'] for suffix in ['json', 'sigstore.json']]:
            shutil.copy2(self.reference / name, self.work / name)
        (self.bin / 'gh').write_text(MOCK_GH); (self.bin / 'gh').chmod(0o755)
        (self.bin / 'cosign').write_text('#!/bin/sh\n[ "${MOCK_BAD_SIGNATURE:-0}" != 1 ]\n'); (self.bin / 'cosign').chmod(0o755)
        (self.bin / 'docker').write_text('#!/bin/sh\nprintf "%s+sha.%.12s for linux on aarch64\\n" "$RELEASE" "$SHA"\n'); (self.bin / 'docker').chmod(0o755)
        (self.bin / 'tar').write_text('#!/bin/sh\nwhile [ "$1" != "-C" ]; do shift; done\nmkdir -p "$2"\nprintf \'#!/bin/bash\\nprintf "%%s+sha.%%.12s for linux on x86_64\\\\n" "$RELEASE" "$SHA"\\n\' > "$2/surreal"\nchmod +x "$2/surreal"\n'); (self.bin / 'tar').chmod(0o755)
        self.env = os.environ.copy()
        self.env.update(PATH=str(self.bin)+':'+self.env['PATH'], MOCK_STATE=str(self.state), MOCK_UPLOADED=str(self.uploaded), MOCK_REFERENCE=str(self.reference), MOCK_EVENTS=str(self.events), GITHUB_REPOSITORY='MainListActivity/surrealdb', RELEASE='4.0.1-native-quota.1', SHA='a'*40, IMAGE_REFERENCE='ghcr.io/example@sha256:'+'b'*64, CANDIDATE_TAG='candidate')
        text = WORKFLOW.read_text().split('      - name: Publish and re-download verified stable release assets\n', 1)[1].split('      - name: Upload promotion receipt\n', 1)[0]
        self.step = self.work / 'step.sh'; self.step.write_text(textwrap.dedent(text.split('        run: |\n', 1)[1]))

    def run_step(self, **settings):
        return subprocess.run(['bash', str(self.step)], cwd=self.work, env=self.env | settings, capture_output=True, text=True, timeout=60)

    def test_verified_draft_is_published_then_downloaded(self):
        result = self.run_step()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        release = json.loads(self.state.read_text())[0]
        self.assertFalse(release['draft']); self.assertEqual(len(release['assets']), 21)
        events = [json.loads(line) for line in self.events.read_text().splitlines()]
        publish = next(i for i, args in enumerate(events) if args[:3] == ['api', '--method', 'PATCH'])
        self.assertGreaterEqual(sum('/releases/assets/' in ' '.join(args) for args in events[:publish]), 21)
        self.assertTrue(any(args[:2] == ['release', 'download'] and args[2] == 'sck-4.0.1-native-quota.1' for args in events[publish+1:]))

    def test_tampered_download_keeps_draft_unpublished(self):
        result = self.run_step(MOCK_TAMPER='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(json.loads(self.state.read_text())[0]['draft'])

    def test_bad_signature_prevents_release_creation(self):
        result = self.run_step(MOCK_BAD_SIGNATURE='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads(self.state.read_text()), [])


if __name__ == '__main__':
    unittest.main()
