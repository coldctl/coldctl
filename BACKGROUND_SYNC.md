# Background Cloud synchronization

Cloud receives only the schema-v2 lifecycle metadata allowlist. Database
records, credentials, connection strings and equality-filter values stay local.
Names and schema/table identifiers ARE part of the metadata: review `agent sync
--dry-run` before enabling uploads.

```powershell
# Provide a lifecycle-scoped token via the service environment / secret manager.
coldctl agent sync --cloud-url https://console.example.com --watch --interval-seconds 300
coldctl agent sync-status
```

Run the first command as a separate service alongside the archive scheduler, using
its same `--data-dir`. It stays in the foreground until Ctrl+C or service termination;
no service is installed automatically. Without `--watch`, sync remains one-shot.
`--dry-run` stays offline and cannot be combined with `--watch`.

The first attempt is immediate unless a retry/interval is already scheduled in
`cloud-sync.sqlite`. Success schedules the next upload after the configured interval
(30–86400 seconds; default 300). Failures retry after 30 seconds, doubling to one hour.
Authentication, contract and size failures are visible and retried at that bounded
rate too; correct the underlying configuration. Restart after changing service token
environment variables. HTTP timeouts are 30 seconds; redirects are never followed.
Only numeric loopback HTTP can be explicitly enabled for local fixtures.

A per-state-directory lock excludes concurrent manual and background syncs. Each
attempt collects fresh metadata. There is no unbounded payload queue or event replay:
intermediate progress can be coalesced during outages. A cancelled or interrupted
upload is never recorded as successful, even if Cloud accepted it. Restart sends a
fresh report. Cloud's existing newer-report checks remain authoritative; keep clocks
accurate. Changing Cloud origin resets local sync bookkeeping for the new destination.

Status persists last attempt, last success, consecutive failures, sanitized error and
next attempt time. The timestamp is a schedule, not proof the process is alive.
The separate agent monitor exposes `coldctl_cloud_sync_last_success_timestamp_seconds`
and `coldctl_cloud_sync_consecutive_failures`, and observes `cloud_sync` incidents for
its configured webhook. Monitor sampling may coalesce failures that recover between
samples. Use external monitoring for a stopped sync process or stale last-success time.

The v2 contract uses up to 1,024 immutable pages, each limited to 100 records,
40 KiB and 24,000 UTF-16 units (at most 102,400 records total). It rejects larger
reports without truncation. A commit publishes the complete generation atomically;
failed uploads leave the previous report visible. `--dry-run` prints an array of
metadata pages. Deploy Cloud v2 before upgrading agents; there is no silent fallback.

Reports include queue retries, recurring schedules and local deletion authorization,
confirmed deletions separately from absent/uncertain outcomes, connector versions and
active monitoring failures. Raw errors and filter values remain local. Reports are
sequential observations, not a transaction across all local subsystems.

The Cloud dashboard loads pages explicitly and labels loaded-only totals. It asks
for a refresh if the report changes during pagination. Uploads expire after one hour;
best-effort cleanup during sync reclaims superseded/abandoned pages older than 24 hours.
Retired installations need operator cleanup. Active reports are retained.

Background sync never executes remote commands, alters archive jobs or deletes source
data. Cloud availability does not gate archival. `cloud-sync.sqlite` is local bookkeeping
and is not included in `state backup`. See [release qualification](RELEASE_QUALIFICATION.md).

Validation fixture: `python scripts/test-background-sync.py` after building the CLI.
