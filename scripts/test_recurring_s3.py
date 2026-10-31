"""Destructive only inside named disposable loopback fixtures; never customer DBs.

Requires coldctl-recurring-pg (15446), coldctl-recurring-mongo (15443),
coldctl-s3-emulator (19000), built CLI/connectors and initialized Mongo fixture.
Advances due times only in this script's fresh temporary SQLite state.
"""
import json
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
EXT = '.exe' if os.name == 'nt' else ''
BIN = ROOT / 'target' / 'debug' / ('coldctl' + EXT)


def connect_state(path):
    conn = sqlite3.connect(path)
    # Ubuntu 22.04's Python links SQLite 3.37. The production CLI bundles
    # newer SQLite; this fixture clock is needed by both queue and schedule
    # monitoring triggers, so install it on every fixture connection.
    if sqlite3.sqlite_version_info < (3, 38, 0):
        conn.create_function('unixepoch', 0, lambda: int(time.time()))
    return conn


def run(args, env=None):
    result = subprocess.run([str(a) for a in args], env=env, capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise AssertionError(f'fixture command failed: {args[0]}\n{result.stderr}')
    return result.stdout.strip()


def pg(sql):
    return run(['docker', 'exec', 'coldctl-recurring-pg', 'psql', '-U', 'postgres', '-d', 'coldctl_test_source', '-At', '-v', 'ON_ERROR_STOP=1', '-c', sql])


def mongo(js):
    return run(['docker', 'exec', 'coldctl-recurring-mongo', 'mongosh', '--quiet', '--username', 'fixture_admin', '--password', 'coldctl_mongodb_fixture_only', '--authenticationDatabase', 'admin', '--eval', js])


def main():
    env = dict(os.environ, COLDCTL_ALLOW_UNVERIFIED_CONNECTOR='1',
               AWS_ACCESS_KEY_ID='coldctl_fixture', AWS_SECRET_ACCESS_KEY='coldctl_fixture_only',
               RECURRING_PG_PASSWORD='coldctl_recurring_fixture',
               RECURRING_MONGO_PASSWORD='coldctl_mongodb_fixture_only')
    for engine in ['postgres', 'mongodb']:
        env[f'COLDCTL_{engine.upper()}_CONNECTOR'] = str(ROOT / 'target' / 'debug' / f'coldctl-connector-{engine}{EXT}')
    env['RECURRING_MONGO_CA'] = run(['docker', 'exec', 'coldctl-recurring-mongo', 'cat', '/tmp/ca.crt'])
    mongo("const d=db.getSiblingDB('coldctl_mongo_source'); if(!d.getUser('deletion_writer')) d.createUser({user:'deletion_writer',pwd:'coldctl_mongodb_fixture_only',roles:[{role:'readWrite',db:'coldctl_mongo_source'},{role:'clusterMonitor',db:'admin'}]});")
    name = 'recurring_' + uuid.uuid4().hex
    pg(f"CREATE TABLE public.{name}(id bigint PRIMARY KEY,created_at timestamptz NOT NULL,note text); INSERT INTO public.{name} VALUES(10,now()-interval '500 days','old'),(20,now()-interval '500 days','old'),(30,now(),'recent');")
    collection = f"db.getSiblingDB('coldctl_mongo_source').getCollection('{name}')"
    mongo(f"{collection}.insertMany([{{_id:ObjectId('101010101010101010101010'),created_at:new Date(0),note:'old'}},{{_id:ObjectId('202020202020202020202020'),created_at:new Date(0),note:'old'}},{{_id:ObjectId('303030303030303030303030'),created_at:new Date(),note:'recent'}}]);")
    try:
        with tempfile.TemporaryDirectory(prefix='coldctl-recurring-') as temp:
            state = Path(temp)

            def cli(*args):
                return run([BIN, '--data-dir', state, *args], env)

            def due():
                with closing(connect_state(state / 'state.db')) as conn, conn:
                    conn.execute("UPDATE archive_queue SET due_at=0 WHERE status='pending'")

            def cycles(expected):
                for _ in range(40):
                    due()
                    cli('agent', 'scheduler', 'run', '--once')  # Restart worker each slice.
                    schedules = json.loads(cli('agent', 'scheduler', 'recurring', 'list'))
                    assert all(s['status'] == 'active' for s in schedules), schedules
                    if all(s['cycles'] == expected for s in schedules):
                        print(f'Completed cycle {expected} for both sources.', flush=True)
                        return schedules
                raise AssertionError('cycles did not complete')

            cli('init')
            cli('source', 'add', 'postgres', '--name', 'pg', '--url', 'postgres://postgres@localhost:15446/coldctl_test_source?sslmode=disable', '--password-env', 'RECURRING_PG_PASSWORD')
            cli('source', 'add', 'mongodb', '--name', 'mongo', '--url', 'mongodb://deletion_writer@localhost:15443/coldctl_mongo_source', '--password-env', 'RECURRING_MONGO_PASSWORD', '--tls-ca-env', 'RECURRING_MONGO_CA')
            cli('destination', 'add', 's3', '--name', 's3', '--bucket', 'coldctl-test', '--region', 'us-east-1', '--prefix', f'recurring/{name}', '--endpoint', 'http://127.0.0.1:19000', '--allow-http')
            for source, schema, stability in [('pg', 'public', 'immutable-rows'), ('mongo', 'coldctl_mongo_source', 'quiescent-copy')]:
                cli('policy', 'create', '--name', source, '--source', source, '--destination', 's3', '--schema', schema, '--table', name, '--time-column', 'created_at', '--older-than-days', '90', '--batch-size', '1')
                # Advance schedules explicitly below. A short real-time interval can
                # start the faster source's next cycle while the slower one is running.
                cli('agent', 'scheduler', 'recurring', 'create', source, '--every-seconds', '86400', '--source-stability', stability, '--confirm-original-source', '--delete-after-archive', '--max-rows', '1', '--min-free-bytes', '1')
            cycles(1)
            assert pg(f'SELECT count(*) FROM public.{name}') == '1'
            assert mongo(f'print({collection}.countDocuments({{}}))') == '1'
            pg(f"INSERT INTO public.{name} VALUES(1,now()-interval '500 days','late low key')")
            mongo(f"{collection}.insertOne({{_id:ObjectId('010101010101010101010101'),created_at:new Date(0),note:'late low key'}})")
            with closing(connect_state(state / 'state.db')) as conn, conn:
                conn.execute('UPDATE recurring_archives SET next_due=0')
            cycles(2)
            assert pg(f'SELECT count(*) FROM public.{name}') == '1'
            assert mongo(f'print({collection}.countDocuments({{}}))') == '1'
            # An empty cycle must finalize safely and wait rather than busy-loop.
            with closing(connect_state(state / 'state.db')) as conn, conn:
                conn.execute('UPDATE recurring_archives SET next_due=0')
            schedules = cycles(3)
            queues = json.loads(cli('agent', 'scheduler', 'list'))
            assert len(queues) == 6 and all(q['status'] == 'completed' for q in queues)
            for q in queues:
                cli('archive', 'verify', q['job_id'])
            with closing(connect_state(state / 'state.db')) as conn, conn:
                assert conn.execute('SELECT sum(rows_deleted) FROM archive_deletions').fetchone()[0] == 6
                assert conn.execute('SELECT count(*) FROM archive_deletions WHERE completed=1').fetchone()[0] == 6
            for s in schedules:
                cli('agent', 'scheduler', 'recurring', 'stop', s['id'])
            cli('agent', 'scheduler', 'run', '--once')
            assert len(json.loads(cli('agent', 'scheduler', 'list'))) == 6
            print('PASS: PostgreSQL and MongoDB, three S3 cycles each, restart every slice, late lower keys, empty cycles, recent records preserved, remote verification, stop.')
    finally:
        pg(f'DROP TABLE IF EXISTS public.{name}')
        mongo(f'{collection}.drop()')


if __name__ == '__main__':
    main()
