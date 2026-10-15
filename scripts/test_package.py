"""Packaging contract tests; no Rust build, private keys or network needed."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import package


class PackageTests(unittest.TestCase):
    def repository(self, directory):
        root = Path(directory) / 'repo'
        root.mkdir()
        for name in package.REQUIRED_INPUTS:
            (root / name).write_text('required ' + name, encoding='utf-8')
        return root

    def test_missing_optional_guides_do_not_block(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.repository(tmp)
            included, omitted = package.package_inputs(root)
            self.assertEqual(included, package.REQUIRED_INPUTS)
            self.assertEqual(omitted, package.OPTIONAL_GUIDES)
            # Sibling docs must not become unreviewed release inputs.
            sibling = Path(tmp) / 'agentremovedtemp'
            sibling.mkdir()
            (sibling / 'RELEASING.md').write_text('external copy')
            self.assertEqual(package.package_inputs(root), (included, omitted))

    def test_required_files_still_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.repository(tmp)
            (root / 'LICENSE').unlink()
            with self.assertRaisesRegex(ValueError, 'LICENSE'):
                package.package_inputs(root)

    def test_restored_guides_are_included_and_inventory_is_truthful(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.repository(tmp)
            (root / 'SHADOW_VALIDATION.md').write_text('operator guide')
            (root / '.env').write_text('must never ship')
            binary = Path(tmp) / 'coldctl.exe'
            binary.write_bytes(b'synthetic executable fixture')
            out = Path(tmp) / 'package'
            out.mkdir()
            package.populate_package(out, binary, root, {'version': '1.2.3'})
            info = json.loads((out / 'BUILD-INFO.json').read_text())
            self.assertIn('SHADOW_VALIDATION.md', info['included_documents'])
            self.assertNotIn('SHADOW_VALIDATION.md', info['omitted_optional_guides'])
            self.assertIn('RELEASING.md', info['omitted_optional_guides'])
            self.assertEqual((out / binary.name).read_bytes(), binary.read_bytes())
            self.assertFalse((out / '.env').exists())
            self.assertIn('signed registry', (out / 'INSTALL.txt').read_text())

    def test_preflight_never_builds(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = self.repository(tmp)
            with patch.object(package, 'ROOT', root), patch('sys.argv', ['package.py', '--check-inputs']), patch.object(package.subprocess, 'run') as run:
                package.main()
                run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
