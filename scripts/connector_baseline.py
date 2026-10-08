"""Record a comparable single-executable footprint; does not build or publish."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import platform
import subprocess
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()
def main():
    parser=argparse.ArgumentParser(__doc__)
    parser.add_argument("--binary",required=True)
    parser.add_argument("--target",required=True)
    parser.add_argument("--profile",required=True,choices=["debug","release"])
    parser.add_argument("--output",default="CONNECTOR_BASELINE.json")
    args=parser.parse_args()
    binary=Path(args.binary).resolve()
    version=command(str(binary),"--version")
    expected=tomllib.loads((ROOT/"Cargo.toml").read_text())["workspace"]["package"]["version"]
    if version != "coldctl " + expected:
        raise SystemExit("Binary version differs from workspace; rebuild first")
    data=binary.read_bytes()
    compressed=io.BytesIO()
    with zipfile.ZipFile(compressed,"w",compression=zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        entry=zipfile.ZipInfo(binary.name, date_time=(1980,1,1,0,0,0))
        entry.compress_type=zipfile.ZIP_DEFLATED
        archive.writestr(entry,data,compresslevel=9)
    dependencies=command("cargo","tree","--offline","-p","coldctl-connector-protocol","--edges","normal","--prefix","none","--format","{p}")
    names=sorted({line.split()[0] for line in dependencies.splitlines()})
    assert not {"tokio-postgres","mysql","mongodb","rusqlite","arrow-array","parquet"}.intersection(names)
    report={"purpose":"Phase 1 baseline before database driver extraction; not a size-reduction claim", "version":version,"target":args.target,"profile":args.profile,"host_os":platform.system(),"rustc":command("rustc","--version"),"commit":command("git","rev-parse","HEAD"),"working_tree_dirty":bool(command("git","status","--porcelain")),"cargo_lock_sha256":hashlib.sha256((ROOT/"Cargo.lock").read_bytes()).hexdigest(),"executable_bytes":len(data),"executable_sha256":hashlib.sha256(data).hexdigest(),"single_binary_zip_deflate9_bytes":len(compressed.getvalue()),"compression_scope":"Executable only; excludes docs, signing metadata, dependencies installed outside executable", "protocol_dependency_packages":names,"job_peak_rss_bytes":None,"throughput_rows_per_second":None,"unmeasured":"Job memory/throughput require controlled database fixtures and are not inferred from binary size"}
    (ROOT/args.output).write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({k:report[k] for k in ["version","target","executable_bytes","single_binary_zip_deflate9_bytes"]}))
if __name__=="__main__": main()
