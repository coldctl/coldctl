# GitHub distribution

Normal pushes to main/master run checks. Pushing a version tag builds Windows x64
and Linux x64 GNU packages, signs the connector registry, uploads every asset to a
draft GitHub Release, then publishes it. The website's `/download` links follow the
latest stable release; a website deployment is not needed for each new version.
No release is published merely by committing ordinary changes.

## One-time setup

1. Create the first trust root on a trusted operator machine:
   `powershell -File scripts/bootstrap-registry.ps1` (or invoke the script from PowerShell).
   It refuses existing keys, restricts the private directory ACL and generates two
   independent random Ed25519 keys. Only `release/root.json` is committed. Never
   commit `secrets/` or put private keys into release assets.
2. Back up `secrets/connector-signing/keys/root.private.pem` offline. It controls root
   rotation and must never enter GitHub Actions. Keep a secure recovery copy of the
   online key too. The bootstrap root expires after two years.
3. In **coldctl/coldctl → Settings → Secrets and variables → Actions**, add
   `COLDCTL_TUF_ONLINE_KEY` with the entire contents of `online.private.pem`.
   It can instead be a secret of the `release` environment used by the publish job.
   Never paste that value into a chat, command argument or issue.
4. In **Settings → General → Releases**, enable release immutability. The workflow
   attaches all assets before publishing and refuses to overwrite an existing
   release. Restrict creation of `v*` tags to release operators, protect the default
   branch, and configure the `release` environment's approvers as appropriate.
5. For anonymous public downloads the repository/releases must be public. A private
   repository needs an authenticated distribution design; the public portal and
   registry downloader do not embed GitHub credentials.

The first bootstrap needs no existing key or registry. Subsequent key rotation is
NOT another bootstrap: retain the original trusted root and publish the consecutive
old/new-authorized TUF root chain. Plan rotation before root expiry. This initial
publisher deliberately stops on an expired or differently trusted previous registry;
recovery/rotation requires a reviewed publisher update, never disabling verification.

## Publish a release

Update the workspace version and matching local dependency versions, update
Cargo.lock, commit, and push those changes. The tag must match Cargo.toml exactly:

```powershell
# Example for the current workspace version; choose an unused version for later releases.
git tag -a v0.1.1 -m 'Coldctl 0.1.1'
git push origin v0.1.1
```

Watch **Actions → release packages**. Manual runs must select an existing version
tag, not a branch. `vX.Y.Z-alpha.N`, `-beta.N` and `-rc.N` are prereleases and do not
replace the stable download links. Stable versions must increase monotonically.
Publishing requires CI, live database qualification, audit, both platform builds,
checksums, signed-registry verification and GitHub provenance attestation to pass.

If upload fails, the release remains a draft. Inspect it, remove only that incomplete
draft if appropriate, and rerun. Published versions and their connector identities
must never be replaced. Bump the version for corrected binaries. GitHub CLI is used
on the hosted runner; operators do not need it for tagging from their local checkout.

## Downloads and verification

- `coldctl-windows-x64.zip` and `coldctl-linux-x64.tar.gz`: stable alias filenames.
- Versioned archives for the base and each PostgreSQL/MySQL/MongoDB connector.
- `SHA256SUMS`: attested checksums for every asset, including root and registry files.
- `release.json`: version, source commit, root fingerprint and asset hashes.
- `root.json`: public bootstrap root; independently confirm its fingerprint against
  the committed `release/root.json` and a trusted operator channel.
- `connector-registry.zip`: authenticated offline repository, retaining previous
  stable connector versions and revocations.
- `metadata--*` / `targets--*`: flat GitHub assets used by the online TUF transport.

Verify the checksum file first:

```text
gh attestation verify SHA256SUMS --repo coldctl/coldctl
```

Then compare downloaded archives with the matching SHA256SUMS entries (`Get-FileHash`
on Windows or `sha256sum` on Linux). Linux packages are built on Ubuntu 24.04 and
older glibc distributions are not qualified. These are development releases with
explicit database/version limits, not a production-scale performance guarantee.

## Install a database connector

Extract the base binary, add it to PATH and obtain the public root and independently
verified fingerprint. Release notes contain the exact version and fingerprint:

```text
coldctl init
coldctl connector registry --url https://github.com/coldctl/coldctl/releases/latest/download/ --root root.json --root-sha256 VERIFIED_FINGERPRINT
coldctl connector install mongodb --version 0.1.1
coldctl connector doctor
```

Use `postgres` or `mysql` as needed. No development bypass is needed. The transport
maps virtual metadata/targets directories to flat release asset names. Only this
GitHub Release URL shape can follow redirects, and only to HTTPS GitHub asset hosts;
TUF still verifies metadata, sizes, hashes, expiry and rollback. Other registries
retain their no-redirect behavior.

For an offline installation, extract connector-registry.zip and add
`--file PATH_TO_EXTRACTED_REPOSITORY` to install. Trust bootstrap is still required;
a root inside a downloaded bundle is never implicitly trusted. Prereleases are
previews: install from their signed offline bundle in a separate state directory,
so preview metadata does not advance a production installation's rollback state.

Source accounts/configuration retain their existing environment-variable workflow.
MongoDB initially requires an 8.0 replica-set primary, frozen ordinary collections,
ObjectId keys, top-level Date retention and documents <=30 KiB. MySQL initially
supports 8.4/InnoDB and the documented bounded scalar types. Source deletion is off.

## Registry lifetime and recovery

Metadata expires after 365 days. Publish a new version before expiry even if only
refreshing distribution metadata; immutable release assets cannot be edited in place.
Installed and approved packages continue to work offline. The publisher verifies
the previous stable bundle using the committed trust root before carrying history
forward. Missing/corrupt referenced files, API/network errors, expired signatures,
changed package identities and lost revocation evidence must not be bypassed.

The initial retention budget is 256 packages and 1.5 GiB of registry targets; exceeding
it stops publication for an explicit retention/hosting review. All historical stable
packages are carried forward, rather than making pinned jobs depend on just the newest
binary. Protect existing releases from deletion. GitHub hosting provides availability,
while TUF and the independently pinned root provide connector authenticity.

No GitHub settings, secret upload, tag push or production release happens merely by
running the local tests. The one-time GitHub settings above require repository access.

References: [GitHub release links](https://docs.github.com/en/repositories/releasing-projects-on-github/linking-to-releases),
[immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases).
