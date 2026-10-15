import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import release


class ReleaseTests(unittest.TestCase):
    def source(self, root, directory, payload=b'connector', version='1.0.0', revoked=None):
        folder = root / directory
        folder.mkdir()
        sha = hashlib.sha256(payload).hexdigest()
        name = sha + '-connector'
        (folder / name).write_bytes(payload)
        package = {'hello': {'connector': {'id': 'mongodb', 'version': version, 'sha256': sha}}, 'platform': 'linux', 'files': [{'target': name, 'length': len(payload), 'sha256': sha}]}
        (folder / 'catalog.json').write_text(json.dumps({'format': 1, 'packages': [package], 'revoked': revoked or []}))
        return folder

    def test_history_and_revocations_survive(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = self.source(root, 'old', revoked=['a'*64])
            new = self.source(root, 'new', b'new', '1.1.0')
            release.merge_targets([old, new], root / 'merged')
            catalog = json.loads((root / 'merged/catalog.json').read_text())
            self.assertEqual(len(catalog['packages']), 2)
            self.assertEqual(catalog['revoked'], ['a'*64])

    def test_republished_identity_and_tampering_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = self.source(root, 'old')
            changed = self.source(root, 'new', b'changed')
            with self.assertRaisesRegex(ValueError, 'cannot be replaced'):
                release.merge_targets([old, changed], root / 'conflict')
            next(p for p in old.iterdir() if p.name != 'catalog.json').write_bytes(b'bad')
            with self.assertRaisesRegex(ValueError, 'integrity'):
                release.merge_targets([old], root / 'tampered')

    def test_offline_bundle_rejects_path_traversal(self):
        for name in ['../escape', '/root', 'targets/../escape', 'targets/a/b', 'targets/a\\b']:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                with zipfile.ZipFile(root / 'bad.zip', 'w') as z:
                    z.writestr(name, b'bad')
                with self.assertRaises(ValueError):
                    release.extract_bundle(root / 'bad.zip', root / 'out')

    def test_version_tag_must_match_workspace(self):
        with patch.object(release, 'tomllib') as toml:
            toml.loads.return_value = {'workspace': {'package': {'version': '1.2.3'}}}
            self.assertEqual(release.version('v1.2.3'), '1.2.3')
            for tag in ['main', 'v1.2.4', 'v01.2.3', 'v1.2.3;echo bad']:
                with self.assertRaises(ValueError):
                    release.version(tag)

    def test_release_notes_use_offline_registry_for_prereleases(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            public_root = root / 'root.json'
            public_root.write_text('{}')
            for version in ('1.2.3', '1.2.3-rc.1'):
                args = argparse.Namespace(tag='v'+version, repo='coldctl/coldctl', out=root/'assets')
                release.write_notes(args, version, public_root)
                notes = (root/'release-notes.md').read_text()
                command = f'coldctl connector install postgres --version {version}'
                if '-rc.' in version:
                    self.assertIn(command + ' --file PATH_TO_EXTRACTED_REGISTRY', notes)
                    self.assertIn('PRERELEASE', notes)
                else:
                    self.assertIn(command + '\n', notes)
                    self.assertNotIn('PATH_TO_EXTRACTED_REGISTRY', notes)
                self.assertNotIn('See RELEASING.md', notes)

    def test_publish_refuses_existing_release_before_mutation(self):
        import argparse
        args = argparse.Namespace(tag='v1.2.3', repo='coldctl/coldctl')
        with patch.object(release, 'version'), patch.object(release, 'command', return_value='[[{"tag_name":"v1.2.3"}]]'), patch.object(release.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'already exists'):
                release.publish(args)
            run.assert_not_called()

    def test_publish_verifies_uploaded_digest_before_publishing(self):
        import argparse
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            asset = out / 'asset.zip'
            asset.write_bytes(b'archive')
            args = argparse.Namespace(tag='v1.2.3', repo='coldctl/coldctl', out=out, notes=out.parent / 'notes.md')
            for valid in (False, True):
                uploaded = {'assets': [{'name': asset.name, 'size': asset.stat().st_size,
                                        'digest': 'sha256:' + (release.digest(asset) if valid else '0'*64)}]}
                with patch.object(release, 'version'), patch.object(release, 'command', side_effect=['[[]]', json.dumps(uploaded)]), patch.object(release.subprocess, 'run') as run:
                    if valid:
                        release.publish(args)
                    else:
                        with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                            release.publish(args)
                    calls = [call.args[0] for call in run.call_args_list]
                    self.assertIn('--draft', calls[0])
                    self.assertEqual(any('edit' in call for call in calls), valid)
                    if valid:
                        self.assertIn('--draft=false', calls[-1])


if __name__ == '__main__':
    unittest.main()
