"""Prepare flat TUF targets from a locally built connector. Does not sign or publish."""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess


def hello(binary):
    # Only use a locally built, trusted executable: this step executes it.
    process = subprocess.Popen([str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        data, _ = process.communicate(b"", timeout=15)
    except BaseException:
        process.kill()
        process.wait()
        raise
    offset = 0

    def frame(kind):
        nonlocal offset
        header = data[offset:offset + 24]
        if len(header) != 24 or header[:8] != b"CCTL\x00\x01\x00\x00" or header[8] != kind or header[9:20] != bytes(11):
            raise ValueError("invalid connector Hello frame")
        size = struct.unpack(">I", header[20:24])[0]
        if not 0 < size <= 1024 * 1024:
            raise ValueError("invalid Hello size")
        offset += 24
        payload = data[offset:offset + size]
        offset += size
        if len(payload) != size:
            raise ValueError("truncated Hello")
        return payload

    metadata = json.loads(frame(1))
    if not 0 < metadata["bytes"] <= 1024 * 1024 or metadata["chunks"] != 1:
        raise ValueError("invalid Hello metadata")
    body = frame(2)
    if offset != len(data) or len(body) != metadata["bytes"] or hashlib.sha256(body).hexdigest() != metadata["sha256"]:
        raise ValueError("Hello digest or framing mismatch")
    result = json.loads(body)
    if result["connector"]["sha256"] != hashlib.sha256(binary.read_bytes()).hexdigest():
        raise ValueError("Hello executable digest mismatch")
    return result


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--target", choices=["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"], required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    binary = args.binary.resolve(strict=True)
    identity = hello(binary)
    args.out.mkdir(parents=True, exist_ok=False)
    entrypoint = "connector.exe" if "windows" in args.target else "connector"
    schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", "description": "Coldctl database configuration. Credentials are environment references; native validation is authoritative.", "type": "object", "properties": {"host": {"type": "string"}, "port": {"type": "integer", "minimum": 1, "maximum": 65535}, "database": {"type": "string"}, "user": {"type": "string"}, "tls": {"enum": ["require", "disable"]}, "password_env": {"type": ["string", "null"]}}, "required": ["host", "port", "database", "user", "tls"], "additionalProperties": False}
    if identity["connector"]["id"] in ("mysql", "mongodb"):
        schema["properties"]["ca_env"] = {"type": ["string", "null"]}
    # Cargo.lock is the complete resolved dependency inventory, not an SBOM/license attestation.
    inventory = {"format": "cargo-lock", "contents": (root / "Cargo.lock").read_text(encoding="utf-8")}
    files = []
    for name, data in [(entrypoint, binary.read_bytes()), ("LICENSE", (root / "LICENSE").read_bytes()), ("config-schema.json", json.dumps(schema).encode()), ("dependencies.json", json.dumps(inventory).encode())]:
        digest = hashlib.sha256(data).hexdigest()
        target = digest + "-" + name
        (args.out / target).write_bytes(data)
        files.append({"name": name, "target": target, "length": len(data), "sha256": digest})
    package = {"hello": identity, "platform": args.target, "agent": ">=0.1.1, <0.2.0", "entrypoint": entrypoint, "files": files, "requirements": [("Mozilla roots or supplied PEM CA" if identity["connector"]["id"] == "mongodb" else "OS certificate trust store"), identity["connector"]["id"] + " server qualification as documented by Coldctl"]}
    (args.out / "catalog.json").write_text(json.dumps({"format": 1, "packages": [package], "revoked": []}, indent=2) + "\n", encoding="utf-8")
    print("Unsigned targets prepared. Merge the catalog with existing releases, then sign with the approved TUF keys.")


if __name__ == "__main__":
    main()
