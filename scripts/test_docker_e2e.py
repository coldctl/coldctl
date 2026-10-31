"""R10a: signed connector, real PostgreSQL archive, Keycloak login, upgrade/rollback.

Uses only uniquely named disposable Docker resources. Requires an extracted signed
registry, the repository's trusted root, and the sibling Gateway checkout. Does
not access customer databases, buckets or credentials. Docker Engine 26+ required.
"""
import argparse
import contextlib
import hashlib
from http.cookiejar import CookieJar, DefaultCookiePolicy
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def docker(*args, check=True):
    p = subprocess.run(["docker", *map(str, args)], text=True, capture_output=True, timeout=300)
    if check and p.returncode:
        raise RuntimeError(f"Docker {args[0]} failed: {p.stderr[-4000:]}")
    return p


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--gateway-repo", type=Path, required=True)
    parser.add_argument("--agent", default="coldctl-agent:r10a-candidate")
    parser.add_argument("--gateway", default="coldctl-gateway:r10a-candidate")
    parser.add_argument("--baseline-agent", default="coldctl-agent:r10a-baseline-de62817")
    parser.add_argument("--baseline-gateway", default="coldctl-gateway:r10a-baseline-6b95fea")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    trusted = (root / "release/root.json").read_bytes()
    assert (args.registry / "root.json").read_bytes() == trusted, "registry root is not enrolled"
    root_sha = hashlib.sha256(trusted).hexdigest()
    name = "coldctl-r10a-e2e-" + uuid.uuid4().hex[:10]
    volume, network = name + "-data", name + "-net"
    helper, pg, identity, gateway = [name + s for s in ("-files", "-pg", "-oidc", "-gateway")]
    gate_port, oidc_port = free_port(), free_port()
    origin = f"http://127.0.0.1:{gate_port}"
    issuer = f"http://127.0.0.1:{oidc_port}/realms/coldctl-test"
    auth_token = "r10a-disposable-bearer-token-0123456789"
    resources = []
    realm_temp = tempfile.TemporaryDirectory(prefix="coldctl-r10a-realm-", dir=root / "target")
    common = ["--init", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
              "--memory=2g", "--memory-swap=2g", "--cpus=2", "--pids-limit=128",
              "--tmpfs", "/tmp:size=268435456,mode=1777,noexec,nosuid,nodev"]

    def mount(subdir, target, readonly=False):
        return ["--mount", f"type=volume,src={volume},dst={target},volume-subpath={subdir}" + (",readonly" if readonly else "")]

    def files(script, *values):
        return docker("exec", helper, "sh", "-ec", script, "fixture", *values).stdout

    def write(path, value):
        files('printf "%s" "$2" > "/fixture/$1"; chown 10001:10001 "/fixture/$1"; chmod 0600 "/fixture/$1"', path, value)

    def cli(image, *command):
        return docker("run", "--rm", *common, "--network", network,
                      *mount("agent", "/var/lib/coldctl"), *mount("archives", "/archives"),
                      *mount("agent-secrets", "/run/secrets", True),
                      "--mount", f"type=bind,src={args.registry.resolve()},dst=/bootstrap,readonly",
                      image, *command).stdout

    def sql(statement):
        return docker("exec", pg, "psql", "-U", "postgres", "-d", "fixture", "-At",
                      "-v", "ON_ERROR_STOP=1", "-c", statement).stdout.strip()

    def http(path, token=None, body=None, opener=None, expected=200):
        request = urllib.request.Request(origin + path, data=None if body is None else json.dumps(body).encode())
        if token:
            request.add_header("Authorization", "Bearer " + token)
        if body is not None:
            request.add_header("Content-Type", "application/json")
            request.add_header("Origin", origin)
        client = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            response = client.open(request, timeout=15)
        except urllib.error.HTTPError as error:
            response = error
        assert response.code == expected, f"HTTP {path}: {response.code}, expected {expected}"
        with response:
            data = response.read()
        return json.loads(data) if data else None

    def start_gateway(image):
        resources.append(gateway)
        docker("run", "-d", "--name", gateway, *common, "--network", "container:" + identity,
               *mount("gateway", "/var/lib/coldctl-gateway"), *mount("archives", "/archives", True),
               *mount("config", "/etc/coldctl", True), *mount("gateway-secrets", "/run/secrets", True), image)
        for _ in range(90):
            try:
                http("/ready")
                return
            except (OSError, AssertionError):
                time.sleep(1)
        raise AssertionError(docker("logs", "--tail", "20", gateway).stdout)

    def stop_gateway():
        docker("stop", "--time=30", gateway)
        state = json.loads(docker("inspect", gateway).stdout)[0]["State"]
        assert state["ExitCode"] == 0 and not state["OOMKilled"], state
        docker("rm", gateway)
        resources.remove(gateway)

    def query():
        http("/api/datasets", expected=401)
        datasets = http("/api/datasets", auth_token)
        entries = datasets if isinstance(datasets, list) else datasets["datasets"]
        assert len(entries) == 1, datasets
        result = http(f"/api/datasets/{entries[0]['id']}/search", auth_token, {"columns": ["id", "note"]})
        assert len(result["records"]) == 2, result
        assert {(int(r["data"]["id"]), r["data"]["note"]) for r in result["records"]} == {(10, "old-a"), (20, "old-b")}
        return entries[0]["id"]

    try:
        docker("network", "create", network)
        docker("volume", "create", volume)
        resources.append(helper)
        docker("run", "-d", "--name", helper, "--user=0", "--entrypoint=sh", "-v", volume + ":/fixture",
               args.baseline_agent, "-c", "sleep infinity")
        files("mkdir -p /fixture/agent /fixture/gateway /fixture/archives /fixture/config /fixture/agent-secrets /fixture/gateway-secrets; chown -R 10001:10001 /fixture")
        enrollment = json.loads(docker("run", "--rm", args.baseline_gateway, "signing-key").stdout)
        for key, value in {"PG_PASSWORD": "r10a_disposable_pg", "COLDCTL_ARCHIVE_BINDING": "r10a-fixture",
                           "COLDCTL_ARCHIVE_SIGNING_SEED": enrollment["seed"]}.items():
            write("agent-secrets/" + key, value)
        write("gateway-secrets/COLDCTL_ARCHIVE_TOKEN", auth_token)
        write("gateway-secrets/COLDCTL_GATEWAY_OIDC_SECRET", "disposable-keycloak-test-secret")
        resources.append(pg)
        docker("run", "-d", "--init", "--name", pg, "--network", network, "--network-alias", "pg",
               "-e", "POSTGRES_PASSWORD=r10a_disposable_pg", "-e", "POSTGRES_DB=fixture", "postgres:16")
        for _ in range(60):
            if docker("exec", pg, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "fixture", check=False).returncode == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("PostgreSQL startup timed out")
        sql("CREATE TABLE events(id bigint PRIMARY KEY, created_at timestamptz NOT NULL, note text); "
            "INSERT INTO events VALUES(10,now()-interval '500 days','old-a'),(20,now()-interval '500 days','old-b'),(30,now(),'recent');")
        cli(args.baseline_agent, "init")
        cli(args.baseline_agent, "connector", "registry", "--url", "https://github.com/coldctl/coldctl/releases/latest/download/",
            "--root", "/bootstrap/root.json", "--root-sha256", root_sha)
        cli(args.baseline_agent, "connector", "install", "postgres", "--version", "0.1.2", "--file", "/bootstrap")
        cli(args.baseline_agent, "connector", "doctor")
        cli(args.baseline_agent, "source", "add", "postgres", "--name", "pg", "--url",
            "postgres://postgres@pg:5432/fixture?sslmode=disable", "--password-env", "PG_PASSWORD")
        cli(args.baseline_agent, "source", "test", "pg")
        cli(args.baseline_agent, "destination", "add", "local", "--name", "local", "--path", "/archives")
        cli(args.baseline_agent, "policy", "create", "--name", "retained", "--source", "pg", "--destination", "local",
            "--schema", "public", "--table", "events", "--time-column", "created_at", "--older-than-days", "90", "--batch-size", "1")
        policy = json.loads(cli(args.baseline_agent, "policy", "show", "retained", "--output", "json"))
        cli(args.baseline_agent, "agent", "scheduler", "enqueue", "retained", "--source-stability", "immutable-rows",
            "--confirm-original-source", "--max-rows", "1", "--min-free-bytes", "1")
        cli(args.baseline_agent, "agent", "scheduler", "run", "--once")
        files("cp -a /fixture/agent /fixture/snapshot-agent; cp -a /fixture/archives /fixture/snapshot-archives")
        print("PASS: official signed connector installed; interrupted archive snapshotted", flush=True)

        def finish(image):
            cli(image, "init")
            for _ in range(40):
                cli(image, "agent", "scheduler", "run", "--once")
                queue = json.loads(cli(image, "agent", "scheduler", "list"))[0]
                assert queue["status"] not in ["failed", "cancelled"], queue
                if queue["status"] == "completed":
                    cli(image, "archive", "verify", queue["job_id"])
                    return queue["job_id"]
                time.sleep(1)
            raise AssertionError("archive did not finish")

        job = finish(args.agent)
        assert sql("SELECT count(*) FROM events") == "3", "archive alone deleted source data"
        config = f'''storage = "/archives"
state_dir = "/var/lib/coldctl-gateway"
[server]
listen = "0.0.0.0:8080"
[trust]
binding = "r10a-fixture"
tenant = "fixture"
public_keys = [{json.dumps(enrollment['public_key'])}]
[oidc]
issuer = {json.dumps(issuer)}
client_id = "coldctl-gateway"
client_secret_env = "COLDCTL_GATEWAY_OIDC_SECRET"
redirect_uri = {json.dumps(origin + '/auth/callback')}
session_seconds = 300
[[oidc.subjects]]
subject = "8aa1b7a3-f110-4a5a-8b1a-20956659bc71"
admin = false
[[oidc.subjects.grants]]
tenant = "fixture"
dataset = {json.dumps(policy['id'])}
columns = ["id", "created_at"]
[[users]]
name = "fixture"
token_env = "COLDCTL_ARCHIVE_TOKEN"
tenants = ["fixture"]
admin = true
[[users.grants]]
tenant = "fixture"
dataset = {json.dumps(policy['id'])}
columns = ["id", "created_at", "note"]
'''
        write("config/gateway.toml", config)
        with contextlib.nullcontext(realm_temp.name) as temp:
            realm = json.loads((args.gateway_repo / "tests/keycloak-realm.json").read_text())
            realm["clients"][0]["redirectUris"] = [origin + "/auth/callback"]
            realm_path = Path(temp) / "realm.json"
            realm_path.write_text(json.dumps(realm), encoding="utf-8")
            resources.append(identity)
            docker("run", "-d", "--init", "--name", identity, "--network", network, "--memory=1g",
                   "-p", f"127.0.0.1:{oidc_port}:{oidc_port}", "-p", f"127.0.0.1:{gate_port}:8080",
                   "--mount", f"type=bind,src={realm_path.resolve()},dst=/opt/keycloak/data/import/realm.json,readonly",
                   "quay.io/keycloak/keycloak:26.4.7", "start-dev", "--import-realm", "--http-port", oidc_port,
                   "--hostname", f"http://127.0.0.1:{oidc_port}")
            discovery = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for _ in range(300):
                try:
                    with discovery.open(issuer + "/.well-known/openid-configuration", timeout=2):
                        break
                except OSError:
                    time.sleep(1)
            else:
                logs = docker("logs", "--tail", "60", identity, check=False)
                state = docker("inspect", "--format", "{{json .State}}", identity, check=False)
                raise AssertionError(f"Keycloak startup timed out: {state.stdout}\n{logs.stdout}\n{logs.stderr}")
            start_gateway(args.baseline_gateway)
            dataset = query()
            stop_gateway()
            files("cp -a /fixture/gateway /fixture/snapshot-gateway")
            start_gateway(args.gateway)
            assert query() == dataset
            class LoopbackIdentityCookies(DefaultCookiePolicy):
                def return_ok_secure(self, cookie, request):
                    # Keycloak treats loopback as a secure browser context. urllib
                    # does not, so mirror that behavior only for this fixture issuer.
                    if request.full_url.startswith(issuer + "/") and cookie.domain == "127.0.0.1":
                        return True
                    return super().return_ok_secure(cookie, request)

            jar = CookieJar(policy=LoopbackIdentityCookies())
            browser = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPCookieProcessor(jar))
            with browser.open(origin + "/auth/login", timeout=20) as response:
                html = response.read().decode()
            import html as html_module
            action = html_module.unescape(re.search(r'id="kc-form-login"[^>]*action="([^"]+)"', html).group(1))
            credentials = urllib.parse.urlencode({"username": "archive-reader", "password": "disposable-reader-password", "credentialId": ""}).encode()
            try:
                with browser.open(action, data=credentials, timeout=20) as response:
                    assert response.url.startswith(origin)
            except urllib.error.HTTPError as error:
                # Omit the URL, which can contain the authorization code.
                page = error.read().decode()
                messages = re.findall(r'<(?:span|p)[^>]*(?:class="[^"]*(?:alert|instruction)[^"]*"|id="input-error")[^>]*>(.*?)</(?:span|p)>', page, re.S)
                cookie_flags = [(c.name, c.secure, c.domain, c.path) for c in jar]
                raise AssertionError(f"Login HTTP {error.code}: {messages}; cookie flags: {cookie_flags}") from None
            visible = http(f"/api/datasets/{dataset}/search", body={"columns": ["id"]}, opener=browser)
            assert len(visible["records"]) == 2
            assert all(set(r["data"]) == {"id"} for r in visible["records"])
            http(f"/api/datasets/{dataset}/search", body={"columns": ["note"]}, opener=browser, expected=400)
            stop_gateway()
            # Restore into new directories, retaining the upgraded evidence for the test's lifetime.
            files("mv /fixture/agent /fixture/upgraded-agent; cp -a /fixture/snapshot-agent /fixture/agent; "
                  "mv /fixture/archives /fixture/upgraded-archives; cp -a /fixture/snapshot-archives /fixture/archives; "
                  "mv /fixture/gateway /fixture/upgraded-gateway; cp -a /fixture/snapshot-gateway /fixture/gateway")
            assert finish(args.baseline_agent) == job
            start_gateway(args.baseline_gateway)
            http("/api/datasets", opener=browser, expected=401)
            assert query() == dataset
            stop_gateway()
            print("PASS: trusted retrieval, Keycloak login/column denial, image upgrade, backup rollback and session invalidation", flush=True)
        cli(args.agent, "archive", "delete", job, "--confirm-delete", "--confirm-original-source")
        assert sql("SELECT id FROM events ORDER BY id") == "30"
        print("PASS: verified source deletion preserved the recent row", flush=True)
    finally:
        for resource in reversed(resources):
            docker("rm", "-f", "-v", resource, check=False)
        docker("volume", "rm", volume, check=False)
        docker("network", "rm", network, check=False)
        realm_temp.cleanup()


if __name__ == "__main__":
    main()
