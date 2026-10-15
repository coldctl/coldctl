"""Only disposable coldctl-protection-pg fixture; gateway and agent use fresh temp state."""
import json, os, subprocess, tempfile, time, urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BIN=ROOT/'target/debug/coldctl.exe'
GATEWAY=ROOT.parents[1]/'archive-gateway/target/debug/coldctl-archive.exe'
def command(args,env=None,ok=True):
 r=subprocess.run([str(x) for x in args],capture_output=True,text=True,env=env,timeout=90)
 if ok and r.returncode: raise AssertionError(r.stdout + r.stderr)
 if not ok: assert r.returncode, 'expected nonzero exit'
 return r.stdout

def pg(sql):return command(['docker','exec','coldctl-protection-pg','psql','-U','postgres','-d','coldctl_test_source','-At','-v','ON_ERROR_STOP=1','-c',sql]).strip()
def main():
 env=dict(os.environ,COLDCTL_ALLOW_UNVERIFIED_CONNECTOR='1',COLDCTL_POSTGRES_CONNECTOR=str(ROOT/'target/debug/coldctl-connector-postgres.exe'),SHADOW_PG_PASSWORD='coldctl_fixture',SHADOW_GATEWAY_TOKEN='synthetic-shadow-test-token-at-least-32-bytes')
 pg("CREATE TABLE shadow_orders(id bigint PRIMARY KEY, created_at timestamp NOT NULL, note text, amount numeric(20,6), active boolean); INSERT INTO shadow_orders VALUES(1,'2020-01-01 00:00:00.000001','PRIVATE_ROW_SENTINEL',1.000001,true),(2,'2020-01-02',NULL,2.000002,false),(3,'2020-01-03','unicode',3.000003,true);")
 try:
  with tempfile.TemporaryDirectory(prefix='coldctl-shadow-') as temp:
   t=Path(temp);state=t/'agent';archive=t/'archive';server=None
   def cli(*args,ok=True):return command([BIN,'--data-dir',state,*args],env,ok)
   cli('init');cli('source','add','postgres','--name','pg','--url','postgres://postgres@localhost:15447/coldctl_test_source?sslmode=disable','--password-env','SHADOW_PG_PASSWORD')
   cli('destination','add','local','--name','disk','--path',archive)
   cli('policy','create','--name','orders','--source','pg','--destination','disk','--schema','public','--table','shadow_orders','--time-column','created_at','--older-than-days','365','--batch-size','2')
   policy=t/'protection.json';policy.write_text(json.dumps({'max_replica_lag_ms':2000,'max_cpu_percent':80,'sample_max_age_seconds':300,'min_batch_delay_ms':1,'max_batch_delay_ms':10,'require_gateway_shadow':True,'shadow_max_age_seconds':3600}))
   cli('protection','set','pg','--config',policy)
   source_id=json.loads(cli('protection','status','pg'))['source_id']
   sample=t/'sample.json'
   def health(lag):
    time.sleep(.005);sample.write_text(json.dumps({'source_id':source_id,'observed_at_unix_ms':int(time.time()*1000),'replica_lag_ms':lag,'cpu_percent':20,'lock_waiters':0}));cli('protection','sample','pg','--file',sample)
   cli('archive','run','orders','--source-stability','immutable-rows',ok=False)
   health(3000);cli('archive','run','orders','--source-stability','immutable-rows',ok=False)
   assert not json.loads(cli('jobs','list','--output','json'))
   health(0);cli('archive','run','orders','--source-stability','immutable-rows','--min-free-bytes','1')
   job=json.loads(cli('jobs','list','--output','json'))[0]['id']
   cli('archive','delete',job,'--confirm-delete','--confirm-original-source',ok=False)
   assert json.loads(cli('archive','shadow',job,'--confirm-original-source'))['status']=='passed'
   cli('archive','delete',job,'--confirm-delete','--confirm-original-source',ok=False)
   layout=t/'layout.json';layout.write_text(json.dumps([{'prefix':job,'tenant':'demo','name':'orders','source':'fixture'}]))
   config=t/'gateway.toml';config.write_text(f'storage = {json.dumps(archive.as_posix())}\nstate_dir = {json.dumps((t/"gateway").as_posix())}\nlayout_file = {json.dumps(layout.as_posix())}\n[server]\nlisten = "127.0.0.1:18087"\n[[users]]\nname="tester"\ntoken_env="SHADOW_GATEWAY_TOKEN"\ntenants=["demo"]\nadmin=true\n')
   command([GATEWAY,'--config',config,'catalog','refresh'],env)
   log=open(t/'gateway.log','w')
   try:
    server=subprocess.Popen([str(GATEWAY),'--config',str(config),'serve'],env=env,stdout=log,stderr=log)
    def datasets():
     req=urllib.request.Request('http://127.0.0.1:18087/api/datasets',headers={'Authorization':'Bearer '+env['SHADOW_GATEWAY_TOKEN']})
     with urllib.request.urlopen(req,timeout=2) as r:return json.load(r)
    for _ in range(50):
     try: data=datasets();break
     except OSError:time.sleep(.2)
    else:raise AssertionError('gateway failed to start')
    dataset=data[0]['id'];extra=['--gateway-url','http://127.0.0.1:18087','--dataset-id',dataset,'--token-env','SHADOW_GATEWAY_TOKEN']
    report=json.loads(cli('archive','shadow',job,'--confirm-original-source',*extra));assert report['status']=='passed' and report['rows']==3 and report['gateway_lookups']==3,report
    report=json.loads(cli('archive','shadow',job,'--confirm-original-source','--max-batches','1',*extra,ok=False));assert report['status']=='incomplete'
    cli('archive','delete',job,'--confirm-delete','--confirm-original-source',ok=False)
    saved_token=env['SHADOW_GATEWAY_TOKEN'];env['SHADOW_GATEWAY_TOKEN']='wrong-synthetic-token'
    report=json.loads(cli('archive','shadow',job,'--confirm-original-source',*extra,ok=False));assert report['status']=='error',report
    env['SHADOW_GATEWAY_TOKEN']=saved_token
    cli('archive','delete',job,'--confirm-delete','--confirm-original-source',ok=False)
    pg("UPDATE shadow_orders SET note='CHANGED_PRIVATE_VALUE' WHERE id=1")
    report=json.loads(cli('archive','shadow',job,'--confirm-original-source',*extra,ok=False));assert report['status']=='mismatch' and report['source_mismatches']==1,report
    pg("UPDATE shadow_orders SET note='PRIVATE_ROW_SENTINEL' WHERE id=1")
    history=cli('archive','shadow-reports',job);assert 'PRIVATE_ROW_SENTINEL' not in history and 'CHANGED_PRIVATE_VALUE' not in history and env['SHADOW_GATEWAY_TOKEN'] not in history
    assert pg('SELECT count(*) FROM shadow_orders')=='3'
    cli('archive','shadow',job,'--confirm-original-source',*extra)
    health(3000);cli('archive','delete',job,'--confirm-delete','--confirm-original-source',ok=False)
    assert pg('SELECT count(*) FROM shadow_orders')=='3'
    health(0);cli('archive','delete',job,'--confirm-delete','--confirm-original-source','--max-batches','1')
    assert pg('SELECT count(*) FROM shadow_orders')=='1'
    cli('archive','shadow',job,'--confirm-original-source',ok=False)
   finally:
    if server:server.terminate();server.wait(timeout=15)
    log.close()
  print('PASS: fail-closed admission, healthy export, real gateway/source comparisons, incomplete runs, mutation detection, redaction and deletion protection.')
 finally:pg('DROP TABLE IF EXISTS shadow_orders')
if __name__=='__main__':main()
