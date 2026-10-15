# Production protection

Source-scoped policies cover manual export/resume, scheduler cycles, verified deletion
and shadow reads. Checks run before source connection where possible and between batches.
An in-flight batch can finish; this is not hard real-time isolation or a write fence.
Existing connector query/lock deadlines and row budgets remain in effect.

## Configure

Save a JSON policy and apply it to the source:

```json
{
  "window_start_utc_minute": 1320,
  "window_end_utc_minute": 360,
  "max_replica_lag_ms": 2000,
  "max_cpu_percent": 80,
  "max_lock_waiters": 5,
  "sample_max_age_seconds": 30,
  "min_batch_delay_ms": 100,
  "max_batch_delay_ms": 2000,
  "require_gateway_shadow": false,
  "shadow_max_age_seconds": 3600
}
```

```powershell
coldctl protection set local1 --config protection.json
coldctl protection status local1
coldctl protection sample local1 --file health.json
coldctl protection clear local1 --confirm
```

The example window is 22:00–06:00 UTC daily, start inclusive/end exclusive. Omit
both window fields for all-day operation. Equal endpoints are invalid. Thresholds
are exclusive: reaching a configured limit pauses work. Missing, stale or future
samples and missing required metrics also block admission. Replacing a policy clears
its previous sample. No policy means existing execution behavior.

## Monitoring integration

This phase accepts **trusted external telemetry**, not native database polling.
Your existing monitoring collector must periodically submit a JSON sample:

```json
{
  "source_id": "UUID_FROM_PROTECTION_STATUS",
  "observed_at_unix_ms": 1792000000000,
  "replica_lag_ms": 250,
  "cpu_percent": 30,
  "lock_waiters": 0
}
```

Replace the timestamp with the actual measurement time; the example is intentionally
not fresh. Use the source UUID shown by status. Samples must be newer than the saved
sample and cannot be dated in the future. Unavailable measurements are null, never
zero. The collector must aggregate the worst lag across all replicas requiring
protection and measure the intended source host. ColdCTL cannot establish coverage
or authenticity of operator-supplied metrics. Do not refresh timestamps on old data.
Update sample files atomically before invoking the sample command.

Adaptive delay rises quadratically from the minimum to maximum with the highest
metric/threshold ratio. At a threshold, work pauses instead. The policy is rechecked
after the delay. Scheduled work retries protection holds without spending its normal
failure retry budget; manual paused jobs require resume. Invalid state or configuration
fails closed. Status shows the last admission reason; job pauses also retain a reason.

## Deletion evidence and scope

Set require_gateway_shadow=true to require a recent full passing gateway shadow report
for the same job manifest before deletion starts. Set shadow_max_age_seconds from 1
to 86400. This does not authorize deletion: explicit deletion confirmation and the
existing source-value checks are still required. The latest report must pass; partial,
failed, empty or source-only runs do not qualify. A report may expire during a partially
completed deletion; this requires operator recovery/review, not bypassing the gate.
Recurring automatic deletion can run shadow validation itself when `automatic_shadow`
contains an entry for the archive policy name. Without an entry, the gate still waits
for manual validation. See SHADOW_VALIDATION.md for setup, retry and receipt semantics.


Policies, samples and decisions live in protection.sqlite. Stop workers before backing
up this sidecar with state.db and shadow.sqlite. Protect the data directory and its ACLs;
removing the protection database removes configured guards. Multiple independent agent
installations are not coordinated, and total load depends on all writers/readers.
Native replica discovery/polling, dynamic source batch sizing and predictive load control
remain future work. Existing MySQL/MongoDB/PostgreSQL source stability requirements apply.
