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

def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()

def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--target", required=True, choices=["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"])
    parser.add_argument("--require-clean", action="store_true")
    parser.add_argument("--component", choices=["coldctl", "coldctl-connector-postgres", "coldctl-connector-mysql", "coldctl-connector-mongodb"], default="coldctl")
    args = parser.parse_args()
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
        shutil.copy2(binary, folder / binary_name)
        for path in ["LICENSE", "README.md", "Cargo.lock", "PRODUCTION_RELEASE_CHECKLIST.md", "CONNECTOR_RUNTIME.md", "CONNECTOR_INSTALLATION.md", "MYSQL_CONNECTOR.md", "MONGODB_PHASE5.md"]:
            shutil.copy2(ROOT / path, folder / path)
        shutil.copytree(ROOT / "docs", folder / "docs")
        info = {"component": args.component, "version": version, "target": args.target, "commit": run("git", "rev-parse", "HEAD"),
                "dirty": dirty, "rustc": run("rustc", "--version"), "cargo": run("cargo", "--version"),
                "lockfile_sha256": hashlib.sha256((ROOT / "Cargo.lock").read_bytes()).hexdigest(),
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "github_run_id": os.environ.get("GITHUB_RUN_ID"), "state_schema": 11, "manifest_formats": [2,3,4]}
        (folder / "BUILD-INFO.json").write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
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
