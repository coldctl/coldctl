# Docker deployment (R10a)

The first deployment path is a single Linux x86-64 Docker host with Compose v2 or later,
one Agent worker per state directory, and a separately deployed Archive Gateway.
The runtime is Ubuntu 24.04, matching the signed Linux connector packages.
Docker Desktop with Linux containers is suitable for qualification; the supported
operating model is a Linux VM with persistent local storage. ARM64, Kubernetes,
multiple workers sharing SQLite, and network-filesystem state are outside this path.

## Build or obtain the image

From this repository:

```sh
docker build --platform linux/amd64 -t coldctl-agent:r10a-local .
python3 scripts/test_docker.py coldctl-agent:r10a-local
```

The base image contains the Agent only. Install signed connectors into persistent
state using the existing registry flow; do not enable the unverified development
connector escape hatch in a deployment. The container does not bundle `pg_repack`.
Ordinary VACUUM remains available through the PostgreSQL connector; a qualified,
version-matched repack client requires a separately reviewed derived image and the
R8 preflight. Do not assume host-side tools are available inside the container.

Maintainers can run **Docker qualification and publication** with `publish=false`
first. With `publish=true` on `main`, the workflow uses the `release` environment,
builds and tests the image, pushes a commit-specific GHCR tag, then creates signed
GitHub build provenance for its digest. Configure environment reviewers and restrict
publication to reviewed main commits whose normal CI has passed. No `latest` tag is
published. A failed attestation means the pushed digest is not approved for use.
This implementation does not itself publish an image or change GHCR visibility.

Before deploying a published digest, authenticate to GHCR if private, then verify:

```sh
gh attestation verify oci://ghcr.io/coldctl/coldctl@sha256:RELEASE_DIGEST \
  --repo coldctl/coldctl \
  --signer-workflow coldctl/coldctl/.github/workflows/container.yml \
  --source-ref refs/heads/main --source-digest REVIEWED_COMMIT_SHA
docker pull ghcr.io/coldctl/coldctl@sha256:RELEASE_DIGEST
```

Use that exact digest in `COLDCTL_AGENT_IMAGE`. See GitHub's
[attestation verification documentation](https://cli.github.com/manual/gh_attestation_verify).
Local tags are for local qualification and have no release attestation.

## Install

Provision dedicated persistent filesystems or enforced project quotas before
creating these directories. Example initial budgets: 10 GiB for Agent state and
100 GiB for local archive/staging; size archives for retention and workload. Bind
mounts have no Docker disk quota, so the filesystem quota is mandatory for a hard
disk bound. Keep capacity alerts and at least 1 GiB free for state recovery. Do not
place these directories on the host root filesystem without an enforced quota.
The Agent state filesystem must permit execution: approved connector binaries run
from `state/connectors`. The `/tmp` mount remains `noexec`.

On the Linux host:

```sh
sudo install -d -o 10001 -g 10001 -m 0700 /srv/coldctl/agent
sudo install -d -o 10001 -g 10001 -m 0750 /srv/coldctl/archives
sudo install -d -o 10001 -g 10001 -m 0700 /srv/coldctl/secrets/agent
cd deploy/docker
cp .env.example .env
```

Edit `.env` with absolute host paths and the verified image digest (or the local
test tag). It contains paths, not credentials. Populate the secret directory using
your secret manager: one file per environment variable, owned by UID 10001 and
mode 0400. For example `PG_PASSWORD`, `COLDCTL_ARCHIVE_SIGNING_SEED`,
`COLDCTL_ARCHIVE_BINDING`, and storage credentials referenced by your configuration.
Each file is a literal UTF-8 value, at most 64 KiB; embedded PEM newlines work and
trailing newlines are stripped. Never store shell scripts in this directory.
Files override matching environment variables. Changes take effect on restart.
Use different secret directories and read-only storage credentials for the Gateway.

```sh
docker compose config --quiet
docker compose run --rm agent init
docker compose run --rm agent status
```

Mount an independently verified release registry directory only during connector
installation. Replace the placeholders with the exact release URL, root digest,
and connector version. Extract `connector-registry.zip` into `/srv/coldctl/registry`
and place the separately downloaded, independently verified `root.json` alongside
its `metadata` and `targets` directories. Do not derive trust from a downloaded
bundle without independently checking its root fingerprint.

```sh
docker compose run --rm -v /srv/coldctl/registry:/bootstrap:ro agent connector registry \
  --url https://YOUR_APPROVED_REGISTRY/ --root /bootstrap/root.json \
  --root-sha256 VERIFIED_ROOT_SHA256
docker compose run --rm -v /srv/coldctl/registry:/bootstrap:ro agent connector install postgres \
  --version EXACT_CONNECTOR_VERSION --file /bootstrap
docker compose run --rm agent connector doctor
```

Configure sources, destinations and policies through `docker compose run --rm agent
<command>`. Use `/archives` for paths inside the container and real reachable DNS
names for databases: `localhost` is the container itself. Do not migrate Windows
state by copying it blindly: saved paths and connector platforms differ. Use a new
Linux state directory and configure the trial there.

For trusted Gateway discovery, use the same enrolled signing binding and seed on
every resumed job; the Gateway receives only the public key. Follow `SCHEDULER.md`
and `R1A-LIVE-POSTGRES-DESIGN.md` to review and explicitly enqueue/enroll policies.
Starting a container does not authorize deletion or create schedules.

```sh
docker compose up -d
docker compose logs --tail=100 agent
docker compose run --rm agent agent scheduler list
docker compose stop
```

The worker receives SIGTERM and stops at the next safe batch boundary. Compose
allows two minutes before SIGKILL; checkpoints allow recovery after forced stops,
but lost source-commit attribution must still be reported as described in R1a/R8.
There is deliberately no fake HTTP health check for the worker. Check container
exit/restart state, scheduler job progress, failures, and source/storage reachability.
`unless-stopped` handles process exits, not unhealthy business workflows.

## Resource envelope

Compose enforces 2 CPUs, 2 GiB RAM with no additional swap, 128 PIDs, a read-only
root filesystem, and a 256 MiB `/tmp` tmpfs (charged to RAM). Logs rotate at
10 MiB x 3. A non-root UID 10001 and dropped capabilities are used, with an init
process to forward signals and reap connector processes. No Docker socket is mounted.

State includes SQLite, connector packages, checkpoints, and any local archive
staging. Keep all persistent writes within quota-backed mounts. Use execution
controls such as batch size, delay and minimum free bytes to limit source impact;
cgroup limits bound this container, not database-side resource use. OOM or ENOSPC
can interrupt a batch; do not treat restart alone as successful completion. Inspect
the durable job outcome before retrying. Measure your workload before increasing
limits; the defaults are an initial operating envelope, not throughput promises.

## Upgrade, backup and rollback

1. Stop the Agent and any CLI jobs; stop the Gateway if taking a coordinated local
   archive snapshot. Confirm the worker exited; preserve the old image digest.
2. Snapshot/copy the entire Agent state directory, including `connectors`, with
   ownership and permissions intact. Record the image digest, source configuration,
   signing-key version and archive object references. Back up local archives and
   separately protect the secret-manager versions. A SQLite-only `state backup`
   does not include connectors or archive files.
3. Verify and pull the new digest. Change `.env`, run `docker compose run --rm agent
   init`, then `connector doctor`, `status`, and source/destination checks. Preserve
   old connector versions referenced by unfinished jobs.
4. Start one worker with `docker compose up -d`; inspect the first completed batch
   and its checkpoint/outcome before continuing the deployment.
5. On failure, stop it. Restore the complete matching pre-upgrade state snapshot
   to a separate prepared directory and select the old image digest. Never run an
   older binary on migrated state. If work ran after the backup, reconcile source
   commits and newly published archives before resuming; rollback does not undo
   source deletion. Retain the failed state for investigation.

Never use `docker compose down -v` as an upgrade or delete state to clear an error.
Test restoration on an isolated host before relying on backups.

## Acceptance scope

Run `python scripts/test_docker_e2e.py --registry PATH_TO_EXTRACTED_REGISTRY
--gateway-repo PATH_TO_GATEWAY --agent NEW_AGENT_IMAGE --gateway NEW_GATEWAY_IMAGE
--baseline-agent OLD_AGENT_IMAGE --baseline-gateway OLD_GATEWAY_IMAGE` for the
cross-service deployment trial. The registry must include `root.json` identical to
the enrolled `release/root.json`, plus its signed metadata and connector targets.
The fixture uses the published PostgreSQL connector 0.1.2 and disposable PostgreSQL
and Keycloak containers. It checks archive resume, trusted retrieval, SSO column
restrictions, coordinated backup restoration and explicit verified deletion.
It exercises local archive storage; it is not a cloud-store or production-load trial.
Every container, network and volume it creates has a unique name and is removed
afterward. Keep both old image tags available before starting.

Agent and Gateway Docker builds lock their shared Cargo caches. Concurrent builds
may wait for that lock, avoiding crate-unpacking races without rebuilding completed
layers.

`scripts/test_docker.py` checks fresh initialization, non-root/read-only operation,
state persistence, exclusive worker admission, two graceful SIGTERM/restart cycles,
and malformed secret rejection in disposable Docker volumes. Existing R1a/R8 tests
cover data transaction recovery. A full signed-connector cloud archive trial and
design-partner load/upgrade qualification remain deployment acceptance gates.

Local qualification passed on 2026-10-28 with Docker Desktop's Linux engine
29.9.2 and Compose 5.6.1: the release image built, Compose limits parsed correctly,
and all lifecycle assertions above passed against the final image. The image is
retained locally as `coldctl-agent:r10a-local`. Test containers and their disposable
state volumes were removed. No image has been published by this local run.
