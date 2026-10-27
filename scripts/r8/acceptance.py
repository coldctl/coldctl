#!/usr/bin/env python3
"""Destructive only to an explicitly disposable R8 container/database; run as postgres.

Requires /build/debug binaries, PostgreSQL 16+ with pg_repack/pgstattuple, and
a small tmpfs at /r8-space. Every test creates fresh local state and tables.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
import time
import uuid

BIN = os.environ.get("COLDCTL_CLI", "/build/debug/coldctl")
os.environ["COLDCTL_POSTGRES_CONNECTOR"] = "/build/debug/coldctl-connector-postgres"
os.environ["COLDCTL_ALLOW_UNVERIFIED_CONNECTOR"] = "1"
os.environ["PGPASSWORD"] = "coldctl_r8_disposable"
os.environ["PGHOST"] = "127.0.0.1"
os.environ["PGUSER"] = "postgres"
os.environ["PGDATABASE"] = "coldctl_r8_acceptance"
os.environ["R8_SOURCE"] = "postgres://postgres:coldctl_r8_disposable@127.0.0.1:5432/coldctl_r8_acceptance?sslmode=disable"
RESULTS = []


def sql(query):
    p = subprocess.run(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", query], text=True, capture_output=True, timeout=45)
    assert p.returncode == 0, p.stderr
    return p.stdout.strip()


def record(name, **evidence):
    RESULTS.append(dict(test=name, passed=True, **evidence))
    print(json.dumps(RESULTS[-1]), flush=True)


class Fixture:
    def __init__(self, tablespace=False, tls=False):
        self.root = Path(tempfile.mkdtemp(prefix="coldctl-r8-"))
        self.state = self.root / "state"
        self.table = "r8_" + uuid.uuid4().hex
        sql(f"CREATE TABLE public.{self.table}(id bigint PRIMARY KEY,created_at timestamptz,payload text) WITH (autovacuum_enabled=false)" + (" TABLESPACE r8_space" if tablespace else ""))
        sql(f"ALTER TABLE public.{self.table} ALTER COLUMN payload SET STORAGE EXTERNAL")
        sql(f"INSERT INTO public.{self.table} SELECT n,CASE WHEN n<=1800 THEN now()-interval '500 days' ELSE now() END,(SELECT string_agg(md5(n::text||':'||g::text),'') FROM generate_series(1,32)g) FROM generate_series(1,2000)n")
        self.cli("init", json_output=False)
        args = ["source", "add", "postgres", "--name", "source", "--url-env", "R8_TLS_SOURCE" if tls else "R8_SOURCE"]
        if tls:
            args += ["--tls-ca-env", "R8_CA"]
        self.cli(*args)
        self.cli("destination", "add", "local", "--name", "disk", "--path", str(self.root / "archives"))
        self.cli("policy", "create", "--name", "events", "--source", "source", "--destination", "disk", "--table", self.table, "--time-column", "created_at", "--older-than-days", "365", "--batch-size", "500")
        job = self.cli("archive", "run", "events", "--source-stability", "live-values", "--min-free-bytes", "1")
        self.job = job["id"]
        assert job["rows_processed"] == 1800, job
        deletion = self.cli("archive", "delete", self.job, "--confirm-delete", "--confirm-original-source")
        assert deletion["completed"] and deletion["rows_deleted"] == 1800, deletion
        assert sql(f"SELECT count(*) FROM public.{self.table}") == "200"
        self.survivors = sql(f"SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM public.{self.table}")

    def command(self, *args, json_output=True):
        return [BIN, "--data-dir", str(self.state), *args, *(["--output", "json"] if json_output else [])]

    def cli(self, *args, json_output=True, ok=True):
        p = subprocess.run(self.command(*args, json_output=json_output), text=True, capture_output=True, timeout=100)
        assert (p.returncode == 0) == ok, (args, p.returncode, p.stdout, p.stderr)
        if not ok:
            return json.loads(p.stdout) if p.stdout.strip().startswith("{") else None
        return json.loads(p.stdout) if json_output else p.stdout

    def reclaim_args(self, wait=1, duration=30):
        now = int(time.time() * 1000)
        return ["archive", "reclaim", self.job, "--confirm-original-source", "--window-start-unix-ms", str(now-1000), "--window-end-unix-ms", str(now+60000), "--wait-timeout-seconds", str(wait), "--max-duration-seconds", str(duration)]

    def check_survivors(self):
        assert sql(f"SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM public.{self.table}") == self.survivors

    def holder(self):
        p = subprocess.Popen(["psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        p.stdin.write(f"BEGIN; LOCK public.{self.table} IN ACCESS SHARE MODE; SELECT pg_backend_pid();\n")
        p.stdin.flush()
        pid = int(p.stdout.readline().strip())
        return p, pid

    @staticmethod
    def release(p):
        p.communicate("ROLLBACK;\n", timeout=10)
        assert p.returncode == 0


def main():
    if not Path("/.dockerenv").exists():
        raise RuntimeError("Run only in the disposable R8 Docker fixture")
    if Path("/proc/1/comm").read_text().strip() == "postgres":
        raise RuntimeError("Start the co-located fixture with docker --init; PostgreSQL must not reap orphan helpers")
    sql("CREATE EXTENSION IF NOT EXISTS pg_repack")
    sql("CREATE EXTENSION IF NOT EXISTS pgstattuple")
    f = Fixture()
    outcome = f.cli("archive", "outcomes", f.job)
    assert outcome["evidence"]["before_deletion"]["heap_free_bytes"] is not None
    original = sql(f"SELECT payload FROM public.{f.table} WHERE id=2000")
    stop = threading.Event()
    writes, errors = [], []

    def writer():
        try:
            while not stop.is_set():
                value = f"concurrent-write-{len(writes)+1}"
                sql(f"UPDATE public.{f.table} SET payload='{value}' WHERE id=2000")
                writes.append(value)
                stop.wait(0.02)
        except Exception as error:
            errors.append(str(error))

    thread = threading.Thread(target=writer)
    thread.start()
    try:
        r = f.cli(*f.reclaim_args(wait=5))
    finally:
        stop.set()
        thread.join(timeout=15)
    assert not thread.is_alive() and not errors and writes, (errors, writes)
    assert sql(f"SELECT payload FROM public.{f.table} WHERE id=2000") == writes[-1]
    sql(f"UPDATE public.{f.table} SET payload='{original}' WHERE id=2000")
    assert r["method"] == "pg_repack" and r["status"] == "completed", r
    assert r["before"]["filenode"] != r["after"]["filenode"], r
    assert r["after"]["physical_allocated_bytes"] < r["before"]["physical_allocated_bytes"], r
    f.check_survivors()
    record("real_repack_and_allocated_blocks", before=r["before"], after=r["after"], concurrent_writes=len(writes))

    args = f.reclaim_args()
    args[args.index("--window-end-unix-ms")+1] = str(int(time.time()*1000)-1)
    f.cli(*args, ok=False)
    assert f.cli("archive", "outcomes", f.job)["evidence"]["reclaim_state"] == "completed"
    f.check_survivors()
    record("expired_customer_window_refuses_without_mutation")

    holder, pid = f.holder()
    try:
        r = f.cli(*f.reclaim_args(), ok=False)
        assert r and r["status"] == "gave_up", r
        assert r["before"]["filenode"] == r["after"]["filenode"], r
        assert sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}") == "1"
    finally:
        f.release(holder)
    f.check_survivors()
    record("lock_timeout_gives_up_without_killing_application", status=r["status"])

    # Interrupt while real pg_repack is blocked by an application transaction.
    holder, pid = f.holder()
    child = subprocess.Popen(f.command(*f.reclaim_args(wait=30, duration=45)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic()+20
        while sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='coldctl-reclaim'") == "0":
            assert time.monotonic() < deadline, "helper never started"
            assert child.poll() is None, child.communicate()
            time.sleep(0.1)
        child.send_signal(signal.SIGINT)
        child.communicate(timeout=10)
        assert child.returncode != 0
        assert sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}") == "1"
    finally:
        if child.poll() is None:
            child.kill(); child.communicate()
        f.release(holder)
    assert f.cli("archive", "outcomes", f.job)["evidence"]["reclaim_state"] == "running"
    f.cli(*f.reclaim_args(), ok=False)
    assert sql("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='repack' AND c.relname LIKE 'log_%'") == "0"
    f.cli("archive", "reclaim-acknowledge", f.job, "--confirm-inspected-source", json_output=False)
    assert f.cli(*f.reclaim_args())["status"] == "completed"
    f.check_survivors()
    record("interrupt_requires_inspection_and_explicit_acknowledgment")

    # Abrupt parent death cannot execute Rust Drop cleanup. The helper's own
    # deadline must still end its work, while durable state remains unfinalized.
    holder, pid = f.holder()
    child = subprocess.Popen(f.command(*f.reclaim_args(wait=5, duration=8)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic()+20
        while sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='coldctl-reclaim'") == "0":
            assert time.monotonic() < deadline, "helper never started before crash"
            assert child.poll() is None, child.communicate()
            time.sleep(0.1)
        child.kill()
        child.communicate(timeout=10)
        assert f.cli("archive", "outcomes", f.job)["evidence"]["reclaim_state"] == "running"
        f.cli(*f.reclaim_args(), ok=False)
        deadline = time.monotonic()+15
        while sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='coldctl-reclaim'") != "0":
            assert time.monotonic() < deadline, "orphan helper outlived its deadline"
            time.sleep(0.2)
        assert sql(f"SELECT count(*) FROM pg_stat_activity WHERE pid={pid}") == "1"
    finally:
        if child.poll() is None:
            child.kill(); child.communicate()
        f.release(holder)
    assert sql("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='repack' AND c.relname LIKE 'log_%'") == "0"
    f.cli("archive", "reclaim-acknowledge", f.job, "--confirm-inspected-source", json_output=False)
    f.check_survivors()
    record("abrupt_parent_death_is_bounded_and_retry_requires_inspection")

    Path("/r8-space/tables").mkdir(exist_ok=True)
    if sql("SELECT count(*) FROM pg_tablespace WHERE spcname='r8_space'") == "0":
        sql("CREATE TABLESPACE r8_space LOCATION '/r8-space/tables'")
    disk = Fixture(tablespace=True)
    sizes = disk.cli("archive", "outcomes", disk.job)["evidence"]["after_deletion"]
    required = 2*(sizes["table_bytes"]+sizes["index_bytes"])
    filler = Path("/r8-space/filler")
    try:
        free = os.statvfs(filler.parent).f_bavail * os.statvfs(filler.parent).f_frsize
        with filler.open("wb") as stream:
            os.posix_fallocate(stream.fileno(), 0, free-max(required//4, 262144))
        r = disk.cli(*disk.reclaim_args(), ok=False)
        assert r and r["status"] == "refused" and "insufficient server disk" in r["failure"], r
        disk.check_survivors()
    finally:
        filler.unlink(missing_ok=True)
    assert disk.cli(*disk.reclaim_args())["status"] == "completed"
    disk.check_survivors()
    record("real_insufficient_disk_gate_and_custom_tablespace")

    os.environ["R8_TLS_SOURCE"] = "postgres://postgres:coldctl_r8_disposable@localhost:5432/coldctl_r8_acceptance?sslmode=require"
    os.environ["R8_CA"] = Path("/r8-tls/server.crt").read_text()
    tls = Fixture(tls=True)
    assert tls.cli(*tls.reclaim_args())["status"] == "completed"
    tls.check_survivors()
    record("custom_ca_tls_repack")
    print(json.dumps({"passed": len(RESULTS), "results": RESULTS}), flush=True)


if __name__ == "__main__":
    main()
