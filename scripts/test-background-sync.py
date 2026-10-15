"""Disposable HTTP fixture for background Cloud sync. No real Cloud credentials."""
import contextlib, http.server, json, os, pathlib, socket, sqlite3, subprocess, tempfile, threading, time, urllib.request, uuid
ROOT = pathlib.Path(__file__).resolve().parents[1]
EXE = ROOT / 'target' / 'debug' / ('coldctl.exe' if os.name == 'nt' else 'coldctl')
requests = []
class Receiver(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        assert self.path in ['/api/agents/sync/v2/cleanup','/api/agents/sync/v2/pages','/api/agents/sync/v2/commit']
        assert self.headers['Authorization'] == 'Bearer coldctl_fixture_secret'
        raw=self.rfile.read(int(self.headers['Content-Length']))
        assert b'PRIVATE_FILTER_VALUE' not in raw and b'PRIVATE_CONNECTION' not in raw
        payload=json.loads(raw)
        if self.path.endswith('/pages'):
            requests.append(payload['report'])
            self.send_response(503 if len(requests) == 1 else 200)
        else:
            self.send_response(200)
        self.end_headers()
    def log_message(self, *args): pass

def wait_for(check):
    for _ in range(100):
        try:
            value = check()
            if value: return value
        except (OSError, sqlite3.Error, ValueError): pass
        time.sleep(.1)
    raise AssertionError('condition timed out')

with tempfile.TemporaryDirectory(prefix='coldctl-cloud-sync-') as directory:
    base = [str(EXE), '--data-dir', directory]
    subprocess.run(base + ['init'], check=True, capture_output=True)
    plan={'policy':{'source':'fixture','destination':'fixture','config':{'schema':'public','table':'events','equals_value':'PRIVATE_FILTER_VALUE'}},'connection':'PRIVATE_CONNECTION'}
    with contextlib.closing(sqlite3.connect(pathlib.Path(directory)/'state.db')) as c:
        for _ in range(250):
            c.execute("INSERT INTO jobs(id,policy_name,plan_json,status,started_at) VALUES(?,'fixture',?,'completed','2026-10-14T00:00:00Z')",(str(uuid.uuid4()),json.dumps(plan)))
        first=c.execute('SELECT id FROM jobs LIMIT 1').fetchone()[0]
        c.execute("INSERT INTO archive_deletions(job_id,manifest_sha256,rows_deleted,rows_absent,completed) VALUES(?,'fixture-hash',1,1,1)",(first,))
        c.commit()
    preview=subprocess.run(base+['agent','sync','--dry-run'],capture_output=True,text=True,check=True)
    pages=json.loads(preview.stdout)
    assert len(pages)>=3 and sum(len(p['jobs']) for p in pages)==250
    assert 'PRIVATE_CONNECTION' not in preview.stdout and 'PRIVATE_FILTER_VALUE' not in preview.stdout
    (ROOT/'target'/'cloud-v2-fixture.json').write_text(preview.stdout,encoding='utf-8')
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = dict(os.environ, COLDCTL_CLOUD_TOKEN='coldctl_fixture_secret')
    args = base + ['agent', 'sync', '--watch', '--interval-seconds', '30', '--cloud-url', f'http://127.0.0.1:{server.server_port}', '--allow-local-http']
    db = pathlib.Path(directory) / 'cloud-sync.sqlite'
    def status():
        with contextlib.closing(sqlite3.connect(db)) as c:
            return c.execute('SELECT failures,error,succeeded,due FROM sync_status').fetchone()
    p = subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for(lambda: status() and status()[0] == 1)
        assert status()[3] > time.time()
        blocked = subprocess.run(args, env=env, capture_output=True, timeout=10)
        assert blocked.returncode != 0 and b'already running' in blocked.stderr
    finally:
        p.terminate(); p.wait(timeout=10)
    saved = subprocess.run(base+['agent','sync-status'],capture_output=True,text=True,check=True)
    assert json.loads(saved.stdout)['consecutive_failures'] == 1
    assert 'coldctl_fixture_secret' not in saved.stdout
    # Make the persisted retry due without a 30-second test sleep.
    with contextlib.closing(sqlite3.connect(db)) as c:
        c.execute('UPDATE sync_status SET due=0'); c.commit()
    p = subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_for(lambda: status() and status()[2] is not None)
        assert status()[0] == 0 and status()[1] is None
        assert requests[1]['capturedAtUnixMs'] > requests[0]['capturedAtUnixMs']
        assert requests[1]['agentId'] == requests[0]['agentId']
        assert sum(len(r['jobs']) for r in requests[1:]) == 250
        assert all(len(r['jobs'])<=100 for r in requests)
    finally:
        p.terminate(); p.wait(timeout=10)
        server.shutdown(); server.server_close()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); monitor_port = sock.getsockname()[1]
    monitor = subprocess.Popen(base + ['agent','monitor','serve','--listen',f'127.0.0.1:{monitor_port}'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        def metrics():
            with urllib.request.urlopen(f'http://127.0.0.1:{monitor_port}/metrics',timeout=2) as response:
                return response.read().decode()
        text=wait_for(metrics)
        assert 'coldctl_cloud_sync_consecutive_failures 0' in text
        assert f'coldctl_cloud_sync_last_success_timestamp_seconds {status()[2]}' in text
    finally:
        monitor.terminate(); monitor.wait(timeout=10)
print('PASS: background retries, persisted failure, exclusive lock, restart, fresh snapshot and recovery')
