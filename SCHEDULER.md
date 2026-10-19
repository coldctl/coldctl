# Long-running archival scheduler

The local scheduler persists a queue in `state.db` and runs each requested archive
to completion using the existing archive checkpoints. It supports delayed starts,
automatic resume after a worker restart, and bounded retries for transient errors.
It deletes PostgreSQL, MySQL or MongoDB source records only with explicit `--delete-after-archive`
authorization; see [verified deletion](DELETION.md). Recurring PostgreSQL, MySQL and MongoDB
archives to S3 or Azure Blob are available through the explicitly authorized recurring workflow below.
Ordinary `enqueue --allow-repeat` still acknowledges possible duplicate exports.

## Recurring archive and verified deletion

Run `coldctl init` to upgrade to schema 16. Create policies using an S3 or Azure Blob destination,
then explicitly authorize recurring deletion for each policy:

```powershell
coldctl agent scheduler recurring create pg_orders --every-seconds 86400 --source-stability immutable-rows --confirm-original-source --delete-after-archive
coldctl agent scheduler recurring create mongo_events --every-seconds 86400 --source-stability quiescent-copy --confirm-original-source --delete-after-archive
coldctl agent scheduler recurring create mysql_events --every-seconds 86400 --source-stability quiescent-copy --confirm-original-source --delete-after-archive
coldctl agent scheduler run
```

The worker runs in the foreground until interrupted. Install it with your service
manager for unattended operation, using the same data directory, connector packages,
database credentials and destination credential environment on every restart. This does not
install a service or change customer databases merely by registering a schedule.

Each schedule admits one cycle immediately. It freezes a UTC cutoff calculated as
admission time minus the policy's retention days, then exports, publishes and verifies
destination objects, runs automatic gateway shadow validation when configured in the source
protection policy, and deletes matching unchanged source records through the existing bounded
deletion path. Subsequent cycles start no sooner than the configured delay after the
worker observes successful deletion completion. Missed intervals coalesce into one
cycle; there is no catch-up burst. Backward wall-clock changes delay admission, while
forward changes can make one cycle due. A restart retains the same queue/job/cutoff.

Every new cycle scans all remaining eligible records, starting without a previous
cycle's key cursor. Late or backdated arrivals between cycles are therefore included
in a subsequent cycle, even with keys below the previous maximum. This is a full
eligible-range rescan, not CDC or a global deduplication service. Records reinserted
after confirmed deletion can produce another archived version. Index the retention
predicate appropriately and measure source impact.

The stable-source contracts still apply for the entire cycle, including downtime and
retries: PostgreSQL eligible rows and membership must remain stable; MySQL and MongoDB
require `quiescent-copy` for each complete cycle. No automatic application write fence
is installed. Do not use this contract to claim snapshot consistency for a continuously
changing source. Source identity/schema drift or changed archived rows fail closed.
MySQL uses its verified InnoDB row-locking deletion path and native UTC DATETIME
cutoff representation; the queue retains the original RFC3339 admission timestamp.

The policy, source configuration and destination configuration are frozen at schedule
creation; changed configuration blocks a new cycle. Credential values can rotate
behind the same environment references. Existing jobs for the policy must have
completed verified deletion before enrollment or another cycle. A failed or partial
deletion never advances to a fresh export. Ordinary manual new runs/queue entries for
the scheduled policy are rejected while the schedule remains active or failed.

```powershell
coldctl agent scheduler recurring list
coldctl agent scheduler recurring show SCHEDULE_ID
coldctl agent scheduler show QUEUE_ID
coldctl agent scheduler recurring retry SCHEDULE_ID --confirm-original-source
coldctl agent scheduler recurring stop SCHEDULE_ID
```

Schedule output includes next due time, completed cycles, active queue and failure.
Queue output includes its schedule ID and frozen cutoff; job and deletion commands
retain the detailed durable history. Transient failures use the normal bounded
backoff. Permanent/exhausted failures hold the schedule for explicit retry of the
same cycle. A cancelled cycle cannot be skipped by retrying the schedule.

Ctrl+C stops the worker safely and allows restart. `recurring stop` is terminal: it
prevents further admissions and cancels queued work, with an in-flight batch allowed
to commit. Already deleted rows are not restored. Preserve state backups and destination data.
The initial interface keeps stopped schedule history and reserves its policy ID;
recover any incomplete cycle manually and use a deliberately reviewed new policy
to enroll again. Scheduling supports one worker per state directory, not multiple
machines sharing state or overlapping archival policies for the same records.

### Validation (2026-10-14)

Core, CLI and migration regressions passed, including frozen cutoff recovery,
deletion evidence gating, failure retry, stop/preflight races, configuration/schema
drift and upgrades from every supported schema prefix. Strict Clippy and formatting
passed for the changed core/CLI targets. The generated command reference matches CLI help.

`python scripts/test_recurring_s3.py` passed against disposable PostgreSQL 15,
MongoDB 8.0 replica-set and Moto fixtures. Each source completed three cycles:
initial old rows, a backdated lower-key arrival, then an empty cycle. The worker
restarted between slices; recent records remained, six archived records were
deleted, all six S3 archives verified, and stopping prevented new admissions.
The script closes its temporary SQLite connections before Windows cleanup and
removes only its uniquely named source tables/collections. The CI workflow runs
this fixture separately. Real AWS and production-scale qualification remain pending.

### Azure validation (2026-10-16)

A separate disposable Aiven PostgreSQL table completed three real Azure Blob
cycles using 60-second intervals: two old rows, a late lower-key arrival, then an
empty cycle. Restarting the worker between slices retained each job and cutoff.
All three archives verified, deletion checkpoints accounted for three rows, and
stopping prevented a fourth cycle after the next due time. The existing 5,109-row
table retained its exact content fingerprint. This was bounded functional
validation with account-key authentication, without automatic Gateway shadow
gating; production load, crash/soak behavior and MongoDB-to-Azure remain unqualified.

### MySQL Azure validation (2026-10-17)

A separate Aiven MySQL 8.4 table completed three real 60-second recurring cycles
to Azure: two old rows, one late lower-key row, and an empty cycle. The first
cycle resumed one-row slices across worker processes with unchanged job/cutoff;
later cycles used one continuous worker. All archives and deletion checkpoints
verified. Stopping prevented a fourth admission and preserved a newly inserted
old row beyond the next due time. The schedule and worker were stopped after
the test. Use `demo/mysql-trial/test.ps1 -Recurring` from the parent workspace
to repeat this isolated fixture. This is bounded functional validation without
automatic Gateway shadow gating, not production-load or long-term qualification.

## Start an archive

Run `coldctl init` to upgrade existing state. Configure a source, destination and
policy using the usual commands, then enqueue that policy:

```sh
coldctl agent scheduler enqueue orders --source-stability immutable-rows --confirm-original-source
coldctl agent scheduler run
```

Use the same `--data-dir PATH` for every command when using a custom state directory.
Enqueue prints a queue ID. The archive job ID appears once preflight succeeds;
inspect data progress using `coldctl jobs show JOB_ID`.

`--confirm-original-source` authorizes future automatic resumes. The original
database and eligible rows must remain stable for the entire queued archive,
including downtime and retries. Resume still verifies archive files and source
identity. Credentials and connector configuration must be available to the worker's
environment, including when started by a service manager.

Useful enqueue options:

- `--delay-seconds 3600`: first eligible execution one hour after enqueue.
- `--max-duration-seconds 300`: five-minute execution slices (default: 60 seconds).
- `--max-rows 10000`: additionally cap rows per slice, not total archive rows.
- `--batch-delay-ms`, `--query-timeout-ms`, `--lock-timeout-ms`, `--min-free-bytes`:
  use the existing archive execution controls.
- `--max-retries 5`: maximum consecutive transient retries (default: 5, maximum: 100).
- `--allow-repeat`: explicitly acknowledge possible duplicate exports from a new job.

Time limits are checked at safe batch boundaries, not hard process deadlines.
Scheduled slice budgets begin after preflight and resume verification; those checks
can add substantial time for large archives and run again on every resume.
The queue requires a positive time slice; zero cannot disable this limit. Each
paused slice is requeued with a one-second delay so other due work can run.
Only one scheduler worker runs per state directory, processing one archive at a time.
The existing job and policy locks also prevent concurrent manual archive writers.

## Adopt an interrupted archive

```sh
coldctl agent scheduler enqueue orders --job JOB_ID --source-stability immutable-rows --confirm-original-source
```

This retains the original job's immutable plan, cutoff, upper key, connector pin
and committed files. Saved execution controls are retained except that the default
time slice becomes 60 seconds; explicit flags override them. The source-stability
flag does not replace an adopted job's saved safety contract. Completed, imported,
and cancelled jobs cannot be adopted. Each policy can have one active queue entry.

## Inspect, stop, and recover

```sh
coldctl agent scheduler list
coldctl agent scheduler show QUEUE_ID
coldctl agent scheduler cancel QUEUE_ID
coldctl agent scheduler retry QUEUE_ID --confirm-original-source
```

These commands print JSON. `due_at` is a UTC Unix timestamp. Queue and job IDs are
different: scheduler commands take queue IDs, and `jobs` commands take job IDs.

Ctrl+C (or SIGTERM on Unix / Ctrl+Break on Windows) stops the worker after the
current safe batch boundary. Restarting `agent scheduler run` recovers claimed
entries and resumes their assigned jobs. Job creation and queue assignment commit
in the same transaction. Completed jobs are reconciled without starting a new export.
An OS-backed worker lock is released on exit or crash; do not delete lock files.

Queue cancellation persists immediately and requests the active job to stop at its
next batch boundary. Cancelling through `jobs cancel` is also respected by the
scheduler. Cancellation is terminal for the queue; retry only accepts failed entries.

Transient failures retry after 30, 60, 120 seconds and so on, capped at one hour.
A successful slice resets the consecutive failure count. Permanent errors and
exhausted retries leave the entry failed, with a sanitized diagnostic. Fix the cause
and use `scheduler retry` to retain the same job and checkpoints. A failure of the
state database itself stops the worker rather than losing queue state.

## Unattended operation

Run `coldctl --data-dir ABSOLUTE_PATH agent scheduler run` under your service
manager with a restart policy and the required credential environment references.
The CLI does not install a service or detach itself. Avoid tying the worker to an
interactive terminal for unattended operation. `run --once` processes at most one
due slice and exits; it does not drain the queue or wait for future entries. Its
exit status reports worker health; inspect the emitted queue status for archive failures.

Back up state and archives using the existing recovery workflow. A state backup
includes the queue, but does not replace the archive files. Keep each state directory
local to one machine; this scheduler is not a distributed coordinator.


## Automatic shadow gate

See [SHADOW_VALIDATION.md](SHADOW_VALIDATION.md#automatic-recurring-validation) for
one-time per-policy gateway configuration, catalog mapping, separate read/refresh
credentials, durable deletion receipts and recovery behavior. This removes the manual
per-job validation step without weakening explicit deletion authorization. The feature
is opt-in; configuring only `require_gateway_shadow` retains the manual gate.
