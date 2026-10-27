# R8: PostgreSQL outcomes and bounded reclamation

Target: local/self-hosted PostgreSQL 16+ first. Managed-cloud disk checks and provider
qualification remain separate. Reclamation is explicit; recurring deletion never
automatically launches maintenance.

## Evidence

`coldctl archive outcomes JOB` reports the existing confirmed-deletion, absent,
deferred and recovery-uncertain counters, archive bytes, and durable outcome
evidence. Recovery-uncertain absence is a subset of absence, not another deletion
count. Legacy attribution remains identified by the deletion checkpoint.

An updated PostgreSQL connector captures the first baseline after acquiring the
table's coldctl advisory lock, before deleting any rows. Resuming preserves that
baseline. A completed deletion captures another observation. Older jobs and
connectors have unknown measurements; upgrading does not invent historical sizes.
Measurement or persistence errors stop the operation; a post-deletion measurement
failure does not undo already committed deletion.

Sizes use `pg_table_size` (including TOAST and its indexes) and `pg_indexes_size`
(the table's indexes). These are whole-table observations at recorded times,
not sizes attributed to one archive. Do not sum snapshots across jobs. Optional
`pgstattuple` measurements report exact heap free space, excluding TOAST and
indexes. Missing extension, permission or bounded-query failure means unknown,
not zero. The optional scan may add up to the connection's 10-second statement
timeout to each observation.

On Unix, a successful verified-filesystem preflight also measures allocated
filesystem blocks for table, TOAST, index forks and segments. The net decrease
before/after reclamation is an observed reduction in relation allocation. It does
not prove causal savings under concurrent writes, increase provisioned disk
capacity, or imply a lower bill. Other platforms report this measure as unknown.

`archive record-billing` records operator-observed invoice totals in minor currency
units, a three-letter currency and two completed, nonoverlapping time intervals.
Periods and workloads may differ; no normalized or causal savings are calculated.
No invoice, account identifiers or free-form billing notes are uploaded. Projections
remain explicitly uncalculated.

## Maintenance

```text
coldctl archive reclaim JOB --confirm-original-source \
  --window-start-unix-ms START --window-end-unix-ms END \
  --wait-timeout-seconds 5 --max-duration-seconds 240
```

START/END are UTC Unix milliseconds. The window must already be active and span
at most 24 hours. Each invocation is independently bounded to 1–290 seconds
(default 240), within the connector's existing five-minute protocol limit. The
duration must exceed the lock wait plus one second. This first implementation is
for tables that can finish in that bound; long-running maintenance needs a future
supervised start/poll protocol. An incomplete repack is not automatically resumed.

Requirements: completed original single-table archive, completed verified deletion,
unchanged source/database/table signature, and owner or superuser maintenance
privilege. Imported archives cannot authorize maintenance. Local job/policy locks
and a PostgreSQL session advisory lock coordinate updated coldctl deletion and
reclamation across agents. Application DDL and older coldctl agents do not observe
that advisory lock; do not run competing schema maintenance or old agents.

When the extension and executable are present, invoke `pg_repack` directly without
a shell, always with `--wait-timeout`, `--no-kill-backend`, `--jobs=1`, `--no-order`,
`--no-analyze`, `--no-password` and `--no-superuser-check`. Credentials travel in
the child environment, never command arguments or durable reports. Only the helper's sessions receive bounded statement/idle timeouts and one-second disconnected-client checks. TLS uses
verify-full and the configured CA (or libpq's system trust); there is no downgrade.
Use libpq supporting `sslrootcert=system` when relying on system roots.

Disk preflight requires access to the server's actual data/tablespace directories.
Run the agent/connector on the database host as an identity able to create a
temporary proof file there; PostgreSQL must be permitted to read that file.
Typically this means the PostgreSQL OS account and a database superuser. When
co-locating in a container, use an init process (`docker run --init`); PostgreSQL
must not act as PID 1 and reap unrelated helper processes. The
connector creates a random temporary proof, asks PostgreSQL to read it, and only
then trusts the local filesystem's available bytes. Every table, index, TOAST and
database-default tablespace filesystem must have at least twice the combined
table/index size. This is deliberately conservative for split tablespaces. The
proof file is removed on normal exit; after a forced process kill, only orphaned
`.coldctl-space-proof-*` files may need operator cleanup.

Remote, container-remapped, inaccessible, or insufficient filesystems fail closed.
Free space is a preflight observation, not a reservation; concurrent growth can
still exhaust disk. Never substitute the agent's unrelated disk or an old estimate.

If either pg_repack component is unavailable, run only
`VACUUM (TRUNCATE FALSE)`. This makes space reusable without table-tail truncation
and never claims physical return. A failed repack/preflight is not silently changed
into a VACUUM success. VACUUM FULL is never issued.

A zero exit from pg_repack is insufficient: unchanged relation filenode means
`gave_up`, not completed. Repack failures and expired windows require inspection;
only our helper is killed/reaped, never application sessions. The durable report
is marked running before the request. A transport/process crash leaves it running
and blocks another reclaim until explicitly acknowledged:

```text
coldctl archive reclaim-acknowledge JOB --confirm-inspected-source
```

Inspect source state, remaining pg_repack artifacts and sessions first. Follow the
[pg_repack recovery documentation](https://reorg.github.io/pg_repack/) where needed;
coldctl never drops the extension automatically. Acknowledgment records inspection,
does not repeat the operation, and does not turn unknown results into success.
Evidence stores the latest maintenance attempt in a separate secured outcomes.sqlite
database; the primary archive/deletion checkpoint format is unchanged.

## Cloud and rollout

Deploy Cloud's strict outcome allowlist before syncing updated agents. Outcome
records remain schema-v2 paged metadata and contain only numeric measurements,
timestamps, enumerated methods and billing totals. Paths, credentials, raw errors,
filenodes and row values remain local. Jobs/archives and outcome detail views show
the four measures, measurement times, unknown values and attribution limits.

## Acceptance gates

- Unit tests: invalid/expired windows and duration limits, disk arithmetic/overflow,
  insufficient disk, mandatory no-kill flags, secrets absent from arguments,
  helper timeout and reaping, ordered billing periods.
- Existing PostgreSQL live-deletion integration test: baseline survives resume,
  advisory lock excludes competing maintenance, after-deletion sizes appear,
  VACUUM fallback preserves filenode, Cloud projection omits private metadata.
- Cloud contract: unknown vs zero, negative sizes, unexpected keys, invalid method,
  missing job, and overlapping invoice periods; old deletion records stay accepted.
- Required self-hosted Unix qualification: actual pg_repack completion and allocated
  blocks before/after; insufficient filesystem gate; a held application transaction
  causes clean give-up without cancellation; kill/restart requires inspection; custom
  tablespaces and TLS; concurrent R1a writes remain intact. Do not claim this gate
  passed based on unit tests or a VACUUM-only fixture.
- Managed-cloud qualification remains pending and must supply a trustworthy
  server-filesystem measurement mechanism before enabling pg_repack there.

## Local verification (2026-10-27)

- Core regression suite: 98 passed, 9 environment-dependent tests ignored.
- PostgreSQL connector: 9 passed, 3 ignored (two require PostgreSQL; one is the
  subprocess-only timeout helper). The timeout test invokes that helper itself.
- Strict workspace/all-targets Clippy passes.
- Cloud outcome and legacy/live-deletion contract checks pass; frontend lint,
  production build and generated CLI reference validation pass.
- Live PostgreSQL deletion suite: **7 passed**, including concurrent writes,
  legacy/live privilege boundaries, recovery attribution, preserved baseline and
  the ordinary VACUUM fallback. Executed against disposable PostgreSQL 16.
- Linux acceptance: **7 passed** on PostgreSQL **16.15**, pg_repack **1.5.3**,
  Rust **1.88**, with an init-enabled Debian bookworm fixture:
  - Real repack with **14 concurrent successful application writes**; survivor
    payload digests preserved. Relation allocation fell from **2,441,216** to
    **286,720 bytes** (observed net reduction **2,154,496 bytes**, not a cost claim).
  - Expired customer window refused without mutation.
  - Lock timeout gave up without killing the competing application session.
  - SIGINT preserved unfinalized state and required explicit inspection/acknowledgment.
  - Abrupt parent SIGKILL remained bounded; retry required inspection and the
    competing application session survived.
  - Real insufficient-space refusal on a **64 MiB tmpfs**, followed by successful
    reclamation in the custom tablespace after removing the test filler.
  - End-to-end custom-CA TLS reclamation.
- Post-run checks: zero coldctl maintenance sessions, zero residual repack log
  tables, and no server recovery in the corrected fixture.
- The initial co-located fixture omitted `--init`. PostgreSQL was PID 1 and treated
  killed orphaned helpers as failed server children, causing recovery. Process
  tracing identified the helper PID, not an application backend. The fixture now
  uses an init reaper and the harness rejects the unsafe layout. No production
  code change was needed for that fixture failure.
- Reproduction: [scripts/r8/README.md](scripts/r8/README.md), Dockerfile and executable
  Python acceptance harness. Tests used disposable databases only. Managed-cloud
  qualification, larger-table performance and the 290-second attempt limit remain
  separate deployment constraints.
