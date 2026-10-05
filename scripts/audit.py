"""Fail closed on audit errors, vulnerabilities and warnings except the documented exception."""
import argparse
from datetime import date, datetime, timezone
import json
from pathlib import Path
import subprocess

EXPIRES = date(2026, 12, 31)

def findings(report, today):
    errors = []
    if report["settings"]["ignore"]:
        errors.append("Undocumented cargo-audit ignores are configured")
    if report["vulnerabilities"]["found"] or report["vulnerabilities"]["count"]:
        errors.append("Vulnerabilities are present")
    for kind, items in report["warnings"].items():
        for item in items:
            advisory = item.get("advisory") or {}
            package = item["package"]
            accepted = (kind == "unmaintained" and advisory.get("id") == "RUSTSEC-2024-0436"
                        and advisory.get("informational") == "unmaintained"
                        and package["name"] == "paste" and package["version"] == "1.0.15"
                        and today <= EXPIRES)
            if not accepted:
                errors.append(kind + ": " + package["name"] + " " + package["version"])
    return errors

def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--audit-bin", default="cargo-audit")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([args.audit_bin, "audit", "--json"], cwd=root, capture_output=True, text=True)
    output = root / "target" / "audit.json"
    output.parent.mkdir(exist_ok=True)
    output.write_text(result.stdout, encoding="utf-8")
    if result.returncode:
        raise SystemExit("cargo-audit failed (including possible network/registry failure); inspect target/audit.json and rerun cargo-audit for diagnostics")
    report = json.loads(result.stdout)
    errors = findings(report, datetime.now(timezone.utc).date())
    if errors:
        raise SystemExit("Audit blocked: " + "; ".join(errors))
    print("Audit passed: no vulnerabilities; only documented, unexpired warnings accepted. See docs/SECURITY_REVIEW.md and target/audit.json.")

if __name__ == "__main__":
    main()
