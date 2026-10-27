# Disposable R8 acceptance fixture

The Python harness performs real archive, deletion and reclamation against a
dedicated PostgreSQL 16 database. It must run as the container's `postgres` OS
user, beside the database, to qualify the verified-filesystem disk gate. It never
connects to a production database. The password in these commands is a public
disposable fixture credential.

**Use `--init` when co-locating the agent with PostgreSQL in one container.**
PostgreSQL as PID 1 can reap a killed orphaned helper and treat it as a crashed
server child, restarting the database. A real init process must reap unrelated
children. The harness rejects that unsafe fixture layout before running tests.

From the repository root (adapt the absolute bind source on Windows):

```sh
docker build -t coldctl-r8-tests -f scripts/r8/Dockerfile scripts/r8
docker run -d --init --name coldctl-r8-linux \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  --mount type=volume,source=coldctl-r8-build,target=/build \
  --tmpfs /r8-space:rw,size=64m,mode=1777 \
  -e POSTGRES_PASSWORD=coldctl_r8_disposable \
  -e POSTGRES_DB=coldctl_r8_acceptance coldctl-r8-tests
docker exec -w /workspace -e CARGO_TARGET_DIR=/build \
  -e CARGO_PROFILE_DEV_DEBUG=0 -e CARGO_INCREMENTAL=0 coldctl-r8-linux \
  cargo build --locked -p coldctl -p coldctl-connector-postgres -j 2
```

When a network proxy's CA is unavailable inside the image, keep TLS verification
enabled. Fetch with a trusted host Cargo installation, copy **only** its registry
`cache` and `index` directories into `/usr/local/cargo/registry/`, and pass
`--offline` to the container build. Do not copy credential/configuration files.

Create a test-only localhost certificate inside the container:

```sh
docker exec coldctl-r8-linux sh -c 'mkdir -p /r8-tls; openssl req -x509 -newkey rsa:2048 -nodes -keyout /r8-tls/server.key -out /r8-tls/server.crt -days 2 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1; chown postgres:postgres /r8-tls/server.key; chmod 600 /r8-tls/server.key'
docker exec -u postgres coldctl-r8-linux psql -d coldctl_r8_acceptance -c "ALTER SYSTEM SET ssl_cert_file='/r8-tls/server.crt'"
docker exec -u postgres coldctl-r8-linux psql -d coldctl_r8_acceptance -c "ALTER SYSTEM SET ssl_key_file='/r8-tls/server.key'"
docker exec -u postgres coldctl-r8-linux psql -d coldctl_r8_acceptance -c "ALTER SYSTEM SET ssl='on'"
docker exec -u postgres coldctl-r8-linux psql -d coldctl_r8_acceptance -c 'SELECT pg_reload_conf()'
docker exec -u postgres coldctl-r8-linux python3 -u /workspace/scripts/r8/acceptance.py
```

The harness checks actual repack/allocated-block reduction, expired windows,
competing application-session survival, interrupt/acknowledgment recovery,
insufficient disk on a 64 MiB tmpfs, custom tablespaces and custom-CA TLS. Rows
remaining after verified deletion must retain their exact payload digest.

Run `crates/coldctl-core/tests/deletion_postgres.rs` separately against a disposable
database **without** the pg_repack extension; it qualifies concurrent R1a writes,
deletion recovery and the ordinary VACUUM fallback. Set
`COLDCTL_TEST_ROLE_PASSWORD=coldctl_test_role_password` for privilege tests, and
run with `--ignored --test-threads=1`.

After collecting results, remove only the named fixture and its anonymous data
volume with `docker rm -f -v coldctl-r8-linux`. The named build-cache volume and
test image may be retained for incremental qualification runs.
