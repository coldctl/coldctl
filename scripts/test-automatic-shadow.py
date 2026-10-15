"""Disposable coldctl-auto-shadow-pg + existing Moto; fresh bucket and agent/gateway state."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
EXE = '.exe' if os.name == 'nt' else ''
BIN = ROOT / f'target/debug/coldctl{EXE}'
GATEWAY = ROOT.parents[1] / f'archive-gateway/target/debug/coldctl-archive{EXE}'


def command(args, env=None):
    result = subprocess.run([str(x) for x in args], env=env, capture_output=True, text=True, timeout=180)
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def pg(sql):
    return command(['docker', 'exec', 'coldctl-auto-shadow-pg', 'psql', '-U', 'postgres',
                    '-d', 'coldctl_test_source', '-At', '-v', 'ON_ERROR_STOP=1', '-c', sql]).strip()


def bucket_operation(bucket, create):
    # Only the unique disposable bucket made by this invocation is changed.
    code = "import boto3; r=boto3.resource('s3',endpoint_url='http://localhost:5000',region_name='us-east-1',aws_access_key_id='coldctl_fixture',aws_secret_access_key='coldctl_fixture_only'); "
    code += f"r.create_bucket(Bucket={bucket!r})" if create else f"b=r.Bucket({bucket!r}); b.objects.all().delete(); b.delete()"
    command(['docker', 'exec', 'coldctl-s3-emulator', 'python', '-c', code])


@contextlib.contextmanager
def database(path):
    db = sqlite3.connect(path)
    try:
        with db:
            yield db
    finally:
        db.close()


def main():
    bucket = 'coldctl-auto-shadow-' + uuid.uuid4().hex
    env = dict(os.environ, COLDCTL_ALLOW_UNVERIFIED_CONNECTOR='1',
               COLDCTL_POSTGRES_CONNECTOR=str(ROOT / f'target/debug/coldctl-connector-postgres{EXE}'),
               SHADOW_PG_PASSWORD='coldctl_fixture', AWS_ACCESS_KEY_ID='coldctl_fixture',
               AWS_SECRET_ACCESS_KEY='coldctl_fixture_only',
               SHADOW_READ_TOKEN='synthetic-reader-token-at-least-32-bytes',
               SHADOW_ADMIN_TOKEN='synthetic-admin-token-at-least-32-bytes')
    bucket_operation(bucket, True)
    pg("CREATE TABLE automatic_orders(id bigint PRIMARY KEY, created_at timestamp NOT NULL, note text); INSERT INTO automatic_orders VALUES(1,'2020-01-01','PRIVATE_AUTO_SENTINEL'),(2,'2020-01-02',NULL),(3,'2020-01-03','third');")
    try:
        with tempfile.TemporaryDirectory(prefix='coldctl-auto-shadow-') as temp:
            t = Path(temp)
            state = t / 'agent'
            def cli(*args):
                return command([BIN, '--data-dir', state, *args], env)
            def tick():
                # Advance only due times in disposable fixture state, avoiding minute-long sleeps.
                with database(state / 'state.db') as db:
                    db.execute("UPDATE archive_queue SET due_at=0 WHERE status='pending'")
                cli('agent', 'scheduler', 'run', '--once')
            def schedule():
                return json.loads(cli('agent', 'scheduler', 'recurring', 'show', schedule_id))
            def queue():
                return json.loads(cli('agent', 'scheduler', 'show', schedule()['current_queue']))
            def retry():
                tick()  # Recurring admission observes the failed queue before explicit retry.
                cli('agent', 'scheduler', 'recurring', 'retry', schedule_id, '--confirm-original-source')
            def next_cycle():
                # First tick observes previous completion; then advance the next cycle's clock.
                tick()
                with database(state / 'state.db') as db:
                    db.execute('UPDATE recurring_archives SET next_due=0 WHERE id=?', (schedule_id,))
                tick()

            cli('init')
            cli('source', 'add', 'postgres', '--name', 'pg', '--url', 'postgres://postgres@localhost:15448/coldctl_test_source?sslmode=disable', '--password-env', 'SHADOW_PG_PASSWORD')
            cli('destination', 'add', 's3', '--name', 's3', '--bucket', bucket, '--region', 'us-east-1', '--prefix', 'orders', '--endpoint', 'http://127.0.0.1:19000', '--allow-http')
            cli('policy', 'create', '--name', 'orders', '--source', 'pg', '--destination', 's3', '--schema', 'public', '--table', 'automatic_orders', '--time-column', 'created_at', '--older-than-days', '365', '--batch-size', '2')
            dataset = hashlib.sha256(b'["demo","orders"]').hexdigest()
            config = {'gateway_url': 'http://127.0.0.1:18088', 'dataset_id': dataset,
                      'token_env': 'SHADOW_READ_TOKEN', 'refresh_token_env': 'SHADOW_ADMIN_TOKEN'}
            policy = {'require_gateway_shadow': True, 'shadow_max_age_seconds': 3600,
                      'automatic_shadow': {'orders': config}}
            policy_file = t / 'protection.json'
            def configure():
                policy_file.write_text(json.dumps(policy))
                cli('protection', 'set', 'pg', '--config', policy_file)
            configure()
            layout = t / 'layout.json'
            layout.write_text(json.dumps([{'prefix': '', 'tenant': 'demo', 'name': 'orders', 'source': 'fixture'}]))
            gateway_config = t / 'gateway.toml'
            gateway_config.write_text(f'''state_dir = {json.dumps((t/'gateway').as_posix())}
layout_file = {json.dumps(layout.as_posix())}
[remote]
prefix = "orders"
[remote.provider]
kind = "s3"
bucket = "{bucket}"
region = "us-east-1"
endpoint = "http://127.0.0.1:19000"
allow_http = true
[server]
listen = "127.0.0.1:18088"
[[users]]
name = "reader"
token_env = "SHADOW_READ_TOKEN"
tenants = ["demo"]
[[users]]
name = "refresher"
token_env = "SHADOW_ADMIN_TOKEN"
tenants = ["demo"]
admin = true
''')
            with open(t / 'gateway.log', 'w') as log:
                server = subprocess.Popen([str(GATEWAY), '--config', str(gateway_config), 'serve'], env=env, stdout=log, stderr=log)
                try:
                    for _ in range(50):
                        try:
                            urllib.request.urlopen('http://127.0.0.1:18088/health', timeout=2).close()
                            break
                        except OSError:
                            time.sleep(.2)
                    else:
                        raise AssertionError('gateway failed to start')
                    schedule_id = json.loads(cli('agent', 'scheduler', 'recurring', 'create', 'orders', '--every-seconds', '60', '--source-stability', 'immutable-rows', '--confirm-original-source', '--delete-after-archive', '--min-free-bytes', '1'))['id']

                    # Transient refresh outage retries without deleting or advancing the job.
                    config['gateway_url'] = 'http://127.0.0.1:1'
                    configure()
                    tick()
                    waiting = queue()
                    assert waiting['status'] == 'pending' and waiting['failures'] == 1, waiting
                    assert pg('SELECT count(*) FROM automatic_orders') == '3'
                    config['gateway_url'] = 'http://127.0.0.1:18088'
                    configure()
                    # No manual refresh or shadow command. Bad query auth must block deletion.
                    read_token = env['SHADOW_READ_TOKEN']
                    env['SHADOW_READ_TOKEN'] = 'incorrect-synthetic-token'
                    tick()
                    first = queue()
                    assert first['status'] == 'failed', first
                    job = first['job_id']
                    assert pg('SELECT count(*) FROM automatic_orders') == '3'
                    reports = json.loads(cli('archive', 'shadow-reports', job))
                    assert reports[0]['status'] == 'error' and reports[0]['automatic_binding']
                    env['SHADOW_READ_TOKEN'] = read_token
                    retry()
                    tick()
                    assert pg('SELECT count(*) FROM automatic_orders') == '1'
                    assert queue()['job_id'] == job
                    reports = json.loads(cli('archive', 'shadow-reports', job))
                    assert reports[0]['status'] == 'passed' and reports[0]['gateway_lookups'] == 3, reports
                    with database(state / 'shadow.sqlite') as db:
                        assert db.execute('SELECT count(*) FROM automatic_shadow_receipts').fetchone()[0] == 1

                    # A changed dataset cannot reuse the receipt after partial deletion.
                    config['dataset_id'] = 'different-dataset'
                    configure()
                    tick()
                    assert queue()['status'] == 'failed'
                    assert pg('SELECT count(*) FROM automatic_orders') == '1'
                    config['dataset_id'] = dataset
                    policy['shadow_max_age_seconds'] = 1
                    configure()
                    time.sleep(1.1)
                    retry()
                    tick()  # New worker, expired report: receipt resumes the same deletion safely.
                    assert pg('SELECT count(*) FROM automatic_orders') == '0'
                    assert queue()['status'] == 'completed'
                    assert len(json.loads(cli('archive', 'shadow-reports', job))) == len(reports)
                    policy['shadow_max_age_seconds'] = 3600
                    configure()

                    # An empty cycle must complete, then a newly eligible record needs fresh evidence.
                    next_cycle()
                    empty = queue()
                    assert empty['status'] == 'completed', empty
                    pg("INSERT INTO automatic_orders VALUES(4,'2020-01-04','next cycle')")
                    next_cycle()
                    new = queue()
                    assert new['status'] == 'completed' and new['job_id'] != job, new
                    report = json.loads(cli('archive', 'shadow-reports', new['job_id']))[0]
                    assert report['gateway_lookups'] == 1 and report['status'] == 'passed'
                    assert pg('SELECT count(*) FROM automatic_orders') == '0'
                    history = cli('archive', 'shadow-reports', job)
                    assert 'PRIVATE_AUTO_SENTINEL' not in history and read_token not in history
                    # Stop while a full validation is running: retained rows must survive.
                    tick()
                    pg("INSERT INTO automatic_orders VALUES(5,'2020-01-05','stop me'),(6,'2020-01-06','stop too')")
                    policy['min_batch_delay_ms'] = 3000
                    policy['max_batch_delay_ms'] = 3000
                    configure()
                    with database(state / 'state.db') as db:
                        db.execute('UPDATE recurring_archives SET next_due=0 WHERE id=?', (schedule_id,))
                    worker = subprocess.Popen([str(BIN), '--data-dir', str(state), 'agent', 'scheduler', 'run', '--once'], env=env, stdout=log, stderr=log)
                    try:
                        deadline = time.monotonic() + 60
                        while time.monotonic() < deadline:
                            with database(state / 'shadow.sqlite') as db:
                                row = db.execute("SELECT body FROM shadow_reports ORDER BY rowid DESC LIMIT 1").fetchone()
                            if row and json.loads(row[0])['status'] == 'running':
                                stopped_job = json.loads(row[0])['job_id']
                                break
                            time.sleep(.1)
                        else:
                            raise AssertionError('automatic shadow never started')
                        cli('agent', 'scheduler', 'recurring', 'stop', schedule_id)
                        assert worker.wait(timeout=40) == 0
                    finally:
                        if worker.poll() is None:
                            worker.terminate()
                            worker.wait(timeout=20)
                    assert pg('SELECT count(*) FROM automatic_orders') == '2'
                    assert json.loads(cli('archive', 'shadow-reports', stopped_job))[0]['status'] == 'incomplete'
                    before = len(json.loads(cli('jobs', 'list', '--output', 'json')))
                    tick()
                    assert len(json.loads(cli('jobs', 'list', '--output', 'json'))) == before
                finally:
                    server.terminate()
                    server.wait(timeout=20)
        print('PASS: automatic S3 catalog refresh/shadow/delete; failed auth; receipt/config binding; restart after expiry; empty and new cycles; stop; redaction.')
    finally:
        pg('DROP TABLE IF EXISTS automatic_orders')
        bucket_operation(bucket, False)


if __name__ == '__main__':
    main()
