"""Assemble and publish immutable GitHub release assets, retaining signed connector history."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
COMPONENTS = ('coldctl', 'coldctl-connector-postgres', 'coldctl-connector-mysql', 'coldctl-connector-mongodb')
PLATFORMS = {'x86_64-pc-windows-msvc': ('windows-x64', '.zip'), 'x86_64-unknown-linux-gnu': ('linux-x64', '.tar.gz')}
NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9._-]{0,255}\Z')
TAG = re.compile(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-(?:alpha|beta|rc)\.[1-9][0-9]*)?\Z')

def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def cargo(example, *args):
    subprocess.run(['cargo', 'run', '--locked', '-p', 'coldctl-core', '--example', example, '--', *map(str, args)], cwd=ROOT, check=True)

def digest(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()

def version(tag):
    if not TAG.fullmatch(tag):
        raise ValueError('Use a vMAJOR.MINOR.PATCH tag, optionally -alpha.N, -beta.N or -rc.N')
    expected = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))['workspace']['package']['version']
    if tag != 'v' + expected:
        raise ValueError('Release tag must match the Cargo workspace version')
    return expected

def extract_bundle(archive, out):
    out.mkdir()
    total = 0
    with zipfile.ZipFile(archive) as bundle:
        seen = set()
        for entry in bundle.infolist():
            parts = entry.filename.split('/')
            if len(parts) != 2 or parts[0] not in ('metadata', 'targets') or not NAME.fullmatch(parts[1]):
                raise ValueError('Invalid registry archive path')
            if entry.filename in seen or (entry.external_attr >> 16) & 0o170000 == 0o120000:
                raise ValueError('Duplicate or linked registry archive entry')
            seen.add(entry.filename)
            total += entry.file_size
            if entry.file_size > 256*1024*1024 or total > 1500*1024*1024:
                raise ValueError('Registry archive exceeds size budget')
            target = out.joinpath(*parts)
            target.parent.mkdir(exist_ok=True)
            with bundle.open(entry) as source, target.open('xb') as destination:
                shutil.copyfileobj(source, destination)

def merge_targets(directories, out):
    out.mkdir()
    packages, revoked = {}, set()
    for directory in directories:
        catalog = json.loads((directory / 'catalog.json').read_text(encoding='utf-8'))
        if catalog['format'] != 1:
            raise ValueError('Unsupported catalog')
        revoked.update(catalog['revoked'])
        for package in catalog['packages']:
            pin = package['hello']['connector']
            identity = (pin['id'], pin['version'], package['platform'])
            if identity in packages and packages[identity] != package:
                raise ValueError('Published connector versions cannot be replaced; bump the version')
            packages[identity] = package
            for entry in package['files']:
                name = entry['target']
                if not NAME.fullmatch(name):
                    raise ValueError('Invalid target filename')
                source = directory / name
                if source.is_symlink() or not source.is_file() or source.stat().st_size != entry['length'] or digest(source) != entry['sha256']:
                    raise ValueError('Connector target integrity mismatch')
                target = out / name
                if target.exists():
                    if digest(target) != entry['sha256']:
                        raise ValueError('Conflicting target payload')
                else:
                    shutil.copyfile(source, target)
    if len(packages) > 256 or sum(f.stat().st_size for f in out.iterdir()) > 1500*1024*1024:
        raise ValueError('Registry retention limit reached; review before publishing')
    (out / 'catalog.json').write_text(json.dumps({'format': 1, 'packages': [packages[k] for k in sorted(packages)], 'revoked': sorted(revoked)}, indent=2) + '\n', encoding='utf-8')

def prepare(args):
    release_version = version(args.tag)
    root = ROOT / 'release' / 'root.json'
    if not root.is_file() or not os.environ.get('COLDCTL_TUF_ONLINE_KEY'):
        raise ValueError('Configure release/root.json and the COLDCTL_TUF_ONLINE_KEY Actions secret first')
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    # Stable alias names make portal links work without updating the website per version.
    for platform, (alias, extension) in PLATFORMS.items():
        for component in COMPONENTS:
            name = f'{component}-{release_version}-{platform}{extension}'
            candidates = list(args.artifacts.rglob(name))
            if len(candidates) != 1:
                raise ValueError(f'Expected exactly one {name}')
            source = candidates[0]
            checksum = source.with_name(name + '.sha256').read_text(encoding='ascii').split()
            if checksum != [digest(source), name]:
                raise ValueError('Packaged archive checksum mismatch')
            shutil.copyfile(source, out / name)
            if component == 'coldctl':
                shutil.copyfile(source, out / f'coldctl-{alias}{extension}')
    with tempfile.TemporaryDirectory() as temporary:
        work = Path(temporary)
        catalogs = [p.parent for p in args.artifacts.rglob('catalog.json')]
        if len(catalogs) != 6:
            raise ValueError('Expected three native connectors for both platforms')
        if args.previous:
            previous = work / 'previous'
            extract_bundle(args.previous, previous)
            verified = work / 'verified'
            cargo('verify_connector_registry', root, previous, verified)
            catalogs.insert(0, verified)
        merged = work / 'merged'
        merge_targets(catalogs, merged)
        key = work / 'online.private.pem'
        descriptor = os.open(key, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as output:
            output.write(os.environ['COLDCTL_TUF_ONLINE_KEY'])
        now = datetime.now(timezone.utc)
        metadata_version = int(now.timestamp())
        if args.previous:
            old_version = json.loads((previous / 'metadata/timestamp.json').read_text())['signed']['version']
            metadata_version = max(metadata_version, old_version + 1)
        expires = (now + timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%SZ')
        signed = work / 'signed'
        cargo('sign_connector_registry', root, merged, signed, metadata_version, expires, key)
        root_version = json.loads(root.read_text())['signed']['version']
        shutil.copyfile(root, signed / 'metadata' / f'{root_version}.root.json')
        # Verify the exact output before it becomes release material.
        cargo('verify_connector_registry', root, signed, work / 'checked')
        with zipfile.ZipFile(out / 'connector-registry.zip', 'x', zipfile.ZIP_DEFLATED) as archive:
            for category in ('metadata', 'targets'):
                for source in sorted((signed / category).iterdir()):
                    archive.write(source, f'{category}/{source.name}')
                    shutil.copyfile(source, out / f'{category}--{source.name}')
        shutil.copyfile(root, out / 'root.json')
    entries = [{'name': p.name, 'sha256': digest(p), 'size': p.stat().st_size} for p in sorted(out.iterdir())]
    manifest = {'version': release_version, 'tag': args.tag, 'commit': os.environ.get('GITHUB_SHA'), 'registry_root_sha256': digest(root), 'assets': entries}
    (out / 'release.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    (out / 'SHA256SUMS').write_text(''.join(f'{digest(p)}  {p.name}\n' for p in sorted(out.iterdir())), encoding='ascii')
    write_notes(args, release_version, root)


def write_notes(args, release_version, root):
    out = args.out.resolve()
    install = (f"coldctl connector install postgres --version {release_version} --file PATH_TO_EXTRACTED_REGISTRY"
               if '-' in args.tag else f"coldctl connector install postgres --version {release_version}")
    channel = ("PRERELEASE: extract this release's connector-registry.zip and use its offline registry in a separate data directory. Do not install this preview version from the stable registry."
               if '-' in args.tag else "Stable release: install connectors from the authenticated online registry.")
    (out.parent / 'release-notes.md').write_text(f'''Coldctl {args.tag}

Download `coldctl-windows-x64.zip` or `coldctl-linux-x64.tar.gz`, extract it, and add the binary to PATH.
Database support is installed separately. Windows x64 and Linux x64 GNU are supported.
Linux packages are built on Ubuntu 24.04; older glibc distributions are not qualified.

Verify `SHA256SUMS` with `gh attestation verify SHA256SUMS --repo {args.repo}` before checking download hashes.
The attested checksums cover every release asset. Archives also include BUILD-INFO.json.

{channel}

Stable registry URL: https://github.com/{args.repo}/releases/latest/download/
Public root SHA256: `{digest(root)}`
Obtain root.json and independently verify its fingerprint against the committed release/root.json.
Then run:

```text
coldctl init
coldctl connector registry --url https://github.com/{args.repo}/releases/latest/download/ --root root.json --root-sha256 {digest(root)}
{install}
```

Replace postgres with mysql or mongodb as needed. The root key never enters CI.
Prerelease assets are previews; the latest registry URL resolves to the latest stable release.
`connector-registry.zip` provides a signed offline repository for `connector install --file`.
Operator documentation: https://www.coldctl.com/docs. Bundled INSTALL.txt explains installation; BUILD-INFO.json lists available guides.
''', encoding='utf-8')

def previous(args):
    # No silent empty-history fallback on API/network errors.
    pages = json.loads(command('gh', 'api', '--paginate', '--slurp', f'repos/{args.repo}/releases?per_page=100'))
    releases = [r for page in pages for r in page if not r['draft'] and not r['prerelease']]
    candidates = [r for r in releases if any(a['name'] == 'connector-registry.zip' for a in r['assets'])]
    def numeric(r):
        tag = r['tag_name']
        if not TAG.fullmatch(tag) or '-' in tag:
            raise ValueError('Unexpected stable release tag; review release history')
        return tuple(map(int, tag[1:].split('.')))
    if candidates:
        latest = max(candidates, key=numeric)
        if '-' not in args.tag and tuple(map(int, args.tag[1:].split('.'))) <= numeric(latest):
            raise ValueError('Stable versions must increase; refusing registry rollback')
        args.out.mkdir(parents=True, exist_ok=False)
        subprocess.run(['gh', 'release', 'download', latest['tag_name'], '--repo', args.repo, '--pattern', 'connector-registry.zip', '--dir', str(args.out)], check=True)

def publish(args):
    version(args.tag)
    # Refuse replacement, including drafts left by an interrupted upload. Operators
    # can inspect/delete an incomplete draft and rerun; published assets stay intact.
    pages = json.loads(command('gh', 'api', '--paginate', '--slurp', f'repos/{args.repo}/releases?per_page=100'))
    if any(r['tag_name'] == args.tag for page in pages for r in page):
        raise ValueError('Release already exists; never overwrite published assets')
    subprocess.run(['gh', 'release', 'create', args.tag, '--repo', args.repo, '--verify-tag', '--draft', '--title', f'Coldctl {args.tag}', '--notes-file', str(args.notes)], check=True)
    files = sorted(args.out.iterdir())
    for offset in range(0, len(files), 20):
        subprocess.run(['gh', 'release', 'upload', args.tag, '--repo', args.repo, *map(str, files[offset:offset+20])], check=True)
    uploaded = json.loads(command('gh', 'api', f'repos/{args.repo}/releases/tags/{args.tag}'))['assets']
    if {a['name']: a['size'] for a in uploaded} != {p.name: p.stat().st_size for p in files}:
        raise ValueError('Uploaded asset inventory mismatch; release remains a draft')
    if any(a.get('digest') != 'sha256:' + digest(args.out / a['name']) for a in uploaded):
        raise ValueError('Uploaded asset digest mismatch; release remains a draft')
    subprocess.run(['gh', 'release', 'edit', args.tag, '--repo', args.repo, '--draft=false', '--prerelease=' + str('-' in args.tag).lower(), '--latest=' + str('-' not in args.tag).lower()], check=True)

def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('mode', choices=('validate', 'previous', 'prepare', 'publish'))
    parser.add_argument('--tag', required=True)
    parser.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY', 'coldctl/coldctl'))
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--previous', type=Path)
    parser.add_argument('--out', type=Path)
    parser.add_argument('--notes', type=Path)
    args = parser.parse_args()
    version(args.tag)
    if args.mode != 'validate':
        globals()[args.mode](args)

if __name__ == '__main__':
    main()
