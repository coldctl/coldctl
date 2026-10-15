# Release qualification — current candidate

This checkout is a development candidate, not a published or production-qualified release.
The old backlog under agentremovedtemp is historical and must not be used as the capability list.

## Implemented release gaps

- Cloud schema-v2 pages: up to 100 metadata records, 40 KiB and 24,000 UTF-16 units per page;
  up to 1,024 pages / 102,400 records per report, with explicit failure above the bound.
  An atomic commit publishes only complete immutable generations. Page/commit retries
  are idempotent; stale or conflicting uploads cannot replace newer reports.
- Paged dashboard reads with explicit Load next page and loaded-only totals. No 100-installation
  listing ceiling on the v2 API. Refresh is required if the current generation changes mid-read.
- Schedule, queue/retry, deletion, connector-version and active monitoring-failure metadata.
  No connection details, raw errors, predicate values, query filters or customer records.
- Gateway export status and completed artifacts persist. Restart marks PREPARING work
  INTERRUPTED; users explicitly resubmit. READY artifacts are checksum-validated on recovery.
  Owner/current tenant authorization still gates downloads; tickets remain short-lived and in memory.
- Packaging preflight checks every bundled guide. Cloud package checks include v2 Functions.

## Rollout and recovery

Deploy Cloud/API and UI first, then upgrade agents. Old schema-v1 upload remains supported;
new agents require v2 and fail visibly against old Cloud. Preview the new metadata with
`agent sync --dry-run` (an array of pages). Preserve a state backup and sidecar backups before
upgrade. Stop gateway before backing up its catalog, exports.sqlite and exports directory.
Source and Cloud sidecars do not replace source/target database backups.

Uploads expire one hour after capture. Best-effort cleanup per sync removes abandoned or
superseded pages older than 24 hours in that installation; active pages are retained. Retired
installations that never sync again require operator cleanup through the authenticated cleanup
endpoint or storage administration. Do not delete active generation pages. Clock accuracy matters.
The sync snapshot is collected sequentially and is an observation, not a transaction across all
local subsystems. Active monitoring failures reflect the monitor's last sample.

## Mandatory external release gates — not satisfied by local tests

Real-cloud and 100-GB qualification are deferred at the user's request (2026-10-14).
They remain release gates, not implementation blockers.

- Hosted Windows/Linux CI and signed release builds from a clean, reviewed commit.
- Clean-machine connector trust bootstrap, install, upgrade and rollback/restore drills;
  verify GitHub attestation, release protections and offline root-key recovery.
- Real AWS IAM/web identity/EC2 and Azure managed identity/RBAC, TLS and OAuth tests on
  customer-controlled infrastructure. Emulators do not qualify these integrations.
- Representative 100-GB scale trial: agree source latency/CPU/I/O, memory and disk budgets;
  measure source impact, cancellation/resume, archive coverage and restored values.
- Sustained recurring deletion and process/host failure drills at publication/checkpoint
  boundaries, disk-full, permission loss and lost acknowledgements on each supported engine.
- Fresh vulnerability audit and explicit review of any exceptions on the release commit.

No production rollout, Git tag, publication or real customer-data deletion is performed by
these local qualification steps. Record exact commit, platform, configuration and evidence
for each gate before declaring a production release.

## Local evidence — 2026-10-14, Windows development checkout

- Agent/core/Cloud Rust suite: 135 passed, 14 ignored; CLI build and strict Clippy passed.
- Background-sync subprocess test: 250 jobs, paged upload, retry, exclusive lock,
  persistent failure, restart, fresh snapshot, monitoring metrics and recovery passed.
  The Cloud strict allowlist also accepted the actual Rust-generated report pages.
- Cloud/Azurite integration harness: 69 checks passed, including partial-generation
  isolation, immutable retries, pagination, authorization and cleanup.
- Gateway: 21 standard tests plus both S3/Azure emulator tests passed; build and strict
  Clippy passed. Recovery checks cover interrupted jobs and artifact checksum failures.
- Release scripts: 6 tests passed; audit-policy scripts: 5 tests passed. Packaging input
  preflight and the published Azure Functions package check (28 functions) passed.
  Audit-policy tests are not a vulnerability audit: cargo-audit is not installed here.
- Website production build and lint passed; generated documentation covers 64 CLI commands
  and 21 guides. The build reports an advisory JavaScript chunk-size warning.

This evidence uses local fixtures and an uncommitted working tree. Ignored database tests,
hosted release builds, actual cloud identities and large-scale fault trials are not covered.

## Production protection and shadow validation - 2026-10-15

Implemented source-scoped UTC windows, fail-closed external lag/CPU/lock telemetry,
adaptive batch delays, and health checks on manual, recurring and deletion paths.
Protection holds preserve scheduler retry budgets. An optional deletion gate requires
a recent complete gateway shadow pass tied to the archive manifest. The initial manual gate is now supplemented by automatic scheduler validation (see below).
Automatic live-query mirroring is not implemented.

Shadow validation compares retained source rows, verified archive values and optional
customer-hosted gateway ID lookups. Durable local reports omit records and credentials;
partial, failed and interrupted reports cannot authorize deletion. See
PRODUCTION_PROTECTION.md and SHADOW_VALIDATION.md for setup and precise limitations.

Local disposable PostgreSQL 16 plus the real gateway passed healthy export, missing/high
lag admission, partial-batch comparison, source mutation detection, source-only/partial/
failed-auth evidence rejection, redaction and guarded deletion. This fixture uses local
storage, synthetic records and an unverified development connector, not real cloud or
100-GB qualification. New MySQL/MongoDB shadow end-to-end qualification remains pending.
Website documentation now covers 70 CLI commands and 23 guides; build and SEO checks
passed for 109 public pages. Packaging includes both new operator guides.

Final agent/core regression run: 138 passed, 0 failed, 14 ignored with the CI development-connector environment. Strict Clippy, formatting, CLI-reference consistency and package-input checks passed. Disposable PostgreSQL fixture was removed after validation.


## Automatic shadow gate - 2026-10-15

Opt-in per-policy gateway configuration in source protection now lets queued and
recurring deletion refresh the gateway catalog and run a full comparison automatically.
No per-job operator approval is needed after initial setup and explicit recurring
source-deletion authorization. A durable job/manifest/configuration-bound receipt
allows the same partially deleted job to resume after report expiry. Missing evidence
or configuration drift blocks continuation; a new job always requires new validation.
Health/window holds, bounded transient retries, permanent failures and stop remain
visible through queue status and local shadow reports. Receipts are not Cloud-synced.

Local end-to-end validation uses disposable PostgreSQL 16, a unique Moto S3 bucket and
the real gateway with separate read/admin tokens. It covers transient refresh outage,
failed query authentication, automatic refresh and full comparison, partial deletion,
worker restart, evidence expiry, changed-dataset rejection, empty cycles, new-cycle
validation, stop during active validation and redaction. No manual shadow or refresh
commands are used by this fixture. Native metric collectors, MongoDB-specific automatic
shadow end-to-end qualification and real-cloud/100-GB trials remain release-validation
work; this local fixture does not establish production readiness.

Automatic-shadow validation results: 141 core/CLI tests passed, 0 failed, 14 ignored. Strict Clippy, formatting, package inputs, website build/SEO (110 pages), and generated CLI/LLM documentation consistency passed.
