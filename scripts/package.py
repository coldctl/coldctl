"""Build and smoke-test a versioned native package; never uploads or publishes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
REQUIRED_INPUTS = ["LICENSE", "README.md", "Cargo.lock"]
OPTIONAL_GUIDES = ["RELEASING.md", "SINKS.md", "SCHEDULER.md", "DELETION.md", "CLOUD_AUTH.md", "MONITORING.md", "BACKGROUND_SYNC.md", "RELEASE_QUALIFICATION.md", "PRODUCTION_PROTECTION.md", "SHADOW_VALIDATION.md"]

def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def package_inputs(root):
    missing = [name for name in REQUIRED_INPUTS if not (root / name).is_file()]
    if missing:
        raise ValueError("Missing required package inputs: " + ", ".join(missing))
    included = REQUIRED_INPUTS + [name for name in OPTIONAL_GUIDES if (root / name).is_file()]
    for name in included:
        if (root / name).is_symlink():
            raise ValueError("Package inputs must be regular repository files: " + name)
    return included, [name for name in OPTIONAL_GUIDES if name not in included]


def populate_package(folder, binary, root, info):
    included, omitted = package_inputs(root)
    shutil.copy2(binary, folder / binary.name)
    for name in included:
        shutil.copy2(root / name, folder / name)
    info = dict(info, included_documents=included, omitted_optional_guides=omitted)
    (folder / "BUILD-INFO.json").write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
    (folder / "INSTALL.txt").write_text(
        "ColdCTL native distribution\n\n"
        "Verify the release SHA256SUMS attestation and archive checksum before use.\n"
        "Extract into a version-specific directory and add that directory to PATH.\n"
        "Run coldctl --version, coldctl init, then coldctl status.\n"
        "Database connectors are installed separately through the signed registry;\n"
        "use the exact trust-root fingerprint and instructions in the GitHub release notes.\n"
        "Prereleases use their signed offline connector-registry.zip in isolated state.\n"
        "Connector archives are not standalone installers; use coldctl connector install.\n"
        "Stop workers and back up the state directory before upgrade or rollback.\n"
        "Do not roll older binaries over upgraded state; retain a matching state backup.\n"
        "See https://www.coldctl.com/docs and https://github.com/coldctl/coldctl/releases\n"
        "for operator documentation. Some guides are maintained outside this repository\n"
        "and are not bundled yet; BUILD-INFO.json lists included and omitted guides.\n"
        "Linux x64 builds use Ubuntu 24.04; older glibc platforms are not qualified.\n",
        encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--check-inputs", action="store_true", help="Validate package documentation without building or publishing")
    parser.add_argument("--target", required=False, choices=["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"])
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--component", choices=["coldctl", "coldctl-connector-postgres", "coldctl-connector-mysql", "coldctl-connector-mongodb"], default="coldctl")
    args = parser.parse_args()
    included, omitted = package_inputs(ROOT)
    if omitted: print("Optional guides not bundled: " + ", ".join(omitted))
    if args.check_inputs:
        print("Package inputs verified; no build or publication performed")
        return
    if args.target is None: parser.error("--target is required for packaging")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    dirty = bool(run("git", "status", "--porcelain"))
    if args.require_clean and dirty:
        raise SystemExit("Release packaging requires a clean checkout")
    tag = os.environ.get("GITHUB_REF", "")
    if tag.startswith("refs/tags/") and tag != "refs/tags/v" + version:
        raise SystemExit("Release tag must match Cargo.toml version")
    subprocess.run(["cargo", "build", "--locked", "--release", "-p", args.component, "--target", args.target], cwd=ROOT, check=True)
    binary_name = args.component + (".exe" if "windows" in args.target else "")
    binary = ROOT / "target" / args.target / "release" / binary_name
    if args.component == "coldctl" and run(str(binary), "--version") != "coldctl " + version:
        raise SystemExit("Binary version mismatch")
    # Run the exact packaged executable with a clean installation and repeated init.
    with tempfile.TemporaryDirectory(prefix="coldctl-package-") as tmp:
        for command in (["init", "init", "status"] if args.component == "coldctl" else []):
            subprocess.run([str(binary), "--data-dir", str(Path(tmp) / "state"), command], check=True)
    name = args.component + "-" + version + "-" + args.target
    output = ROOT / "target" / "packages"
    output.mkdir(parents=True, exist_ok=True)
    extension = ".zip" if "windows" in args.target else ".tar.gz"
    archive = output / (name + extension)
    if archive.exists():
        raise SystemExit("Package already exists; move it aside before rebuilding")
    with tempfile.TemporaryDirectory(prefix="coldctl-package-") as tmp:
        folder = Path(tmp) / name
        folder.mkdir()
        info = {"component": args.component, "version": version, "target": args.target, "commit": run("git", "rev-parse", "HEAD"),
                "dirty": dirty, "rustc": run("rustc", "--version"), "cargo": run("cargo", "--version"),
                "lockfile_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "github_run_id": os.environ.get("GITHUB_RUN_ID"), "state_schema": 17, "manifest_formats": [2,3,4]}
        populate_package(folder, binary, ROOT, info)
        if extension == ".zip":
            with zipfile.ZipFile(archive, "x", zipfile.ZIP_DEFLATED) as z:
                for file in sorted(folder.rglob("*")):
                    if file.is_file():
                        z.write(file, file.relative_to(folder.parent))
        else:
            with tarfile.open(archive, "x:gz") as tar:
                tar.add(folder, arcname=name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    archive.with_name(archive.name + ".sha256").write_text(digest + "  " + archive.name + "\n", encoding="ascii")
    print(str(archive))
    print("SHA256 " + digest)

if __name__ == "__main__":
    main()
