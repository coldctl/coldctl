# Operational monitoring

Stop workers and run `coldctl init` to migrate local state to schema 17. The agent
records job, queue and recurring-schedule failure/recovery transitions in the same
SQLite transaction as the operational change. It retains the latest 10,000 source
transitions, including changes made while the monitor is stopped.

Run the monitor as a separate service alongside the scheduler:

```powershell
coldctl agent monitor serve --listen 127.0.0.1:9464 --token-env COLDCTL_METRICS_TOKEN --webhook-url-env COLDCTL_ALERT_URL --webhook-token-env COLDCTL_ALERT_TOKEN
```

Set those environment values through your service manager or secret manager. The
metrics token needs 32–4096 bytes. Omit both webhook flags to disable alert delivery.
Polling defaults to 10 seconds (`--poll-seconds`, 1–300). Only one monitor may run
per state directory; pass the same `--data-dir` as the scheduler.

## Endpoints

- `GET /metrics`: Prometheus text format, including job/queue/schedule status
  counts, archived rows/bytes, deleted rows, oldest due queue age, sample freshness,
  active incidents, pending alerts and delivery backlog age.
- `GET /failures`: latest 100 persisted incident transitions, delivery state,
  attempt count and last HTTP status, as JSON.
- `GET /ready`: 200 after a fresh successful state sample, 503 if the sample fails
  or becomes stale. This reports monitor readiness, not worker liveness or archive
  correctness. Protect all endpoints with `Authorization: Bearer TOKEN` when
  `--token-env` is configured.
- `coldctl agent monitor status`: inspect the latest persisted history offline.

Loopback is the default. Non-loopback listeners require a token. Terminate TLS at
trusted ingress; do not expose the plaintext listener to untrusted networks.
No database names, record values, query filters, raw errors or resource IDs appear
as metric labels. Row/byte aggregates are gauges over local records, not proof of
unique data archived or disk capacity reclaimed.

## Alert delivery contract

The generic HTTP webhook receives JSON with `id`, `service_id`, `component`,
`kind`, `entity_id`, `state` (`firing` or `resolved`) and `occurred_at` (Unix seconds).
`service_id` persists in the monitor database; the agent's entity ID is an internal
job/queue/schedule UUID. Gateway incidents use a bounded operation kind and
`entity_id: "service"`. Names, credentials, endpoint URLs and error text are absent.
Map the service ID to your deployment in the receiver. Slack/PagerDuty and other
provider-specific payloads require a receiver adapter; no shell commands are run.

Only 2xx acknowledges delivery. Requests have a 10-second timeout and never follow
redirects. Failed deliveries retry after 30 seconds, doubling to at most one hour.
Unsent events survive restart and are retained until delivered. Delivery is
at least once: deduplicate using `Idempotency-Key`, which equals the JSON event ID.
Recovery cannot overtake an earlier undelivered firing event for the same incident.
The dispatcher processes at most one event per second, with no network calls on
the archival worker path. A slow or unavailable receiver does not stop archival.

Webhook URL and optional bearer token are environment references. HTTPS is required;
`--allow-loopback-http` permits numeric loopback HTTP only for testing. Secret URL
and response/error bodies are never saved to monitoring state. A malformed or
missing secret fails startup; runtime delivery failures remain visible in the outbox.
Events observed with alerts disabled are retained as `disabled` and are not replayed
merely by enabling a receiver later. Previously pending deliveries resume when
hooks are enabled again; changing the receiver sends that backlog to the new URL.

## Retention, outages and backups

`monitoring.sqlite` stores incident history and delivery state separately from
`state.db`. Delivered/disabled history and resolved incidents expire after 30 days;
active incidents and pending alerts are retained. Stop the monitor before copying
its SQLite database for backup. `coldctl state backup` includes the source journal
but does not include the monitoring sidecar or its delivery acknowledgements.
Restore matching state and monitoring backups. If the source journal regresses
behind the saved cursor, monitoring fails readiness rather than skipping new events;
review pending alerts before replacing a mismatched monitoring sidecar.

If more than 10,000 source transitions occur while monitoring is absent, the
monitor exposes `coldctl_agent_journal_gap` and increments the durable
`coldctl_monitor_journal_gaps_total`; current failure states are reconciled after
catch-up. Missing historical transitions cannot be reconstructed. Monitor disk
space because pending alerts are intentionally not discarded during receiver outages.

Use external scrape/process alerts for a dead monitor, stopped worker, stale
samples, growing oldest-due queue age or webhook backlog. A process cannot send
its own outage notification after it has stopped. The monitor does not install a
service, restart workers, resume failed archives, or synchronize Cloud metadata.

## Gateway

The gateway uses the same durable outbox design in its own `state_dir` and the
existing administrator bearer credentials:

- `GET /metrics`: persisted HTTP outcome counters, available query workers and
  monitoring incident/backlog metrics.
- `GET /api/admin/monitoring/failures`: latest 100 incident transitions.

Ordinary tenant users cannot access these endpoints. Add to the gateway TOML:

```toml
[alerts]
webhook_url_env = "COLDCTL_ALERT_URL"
webhook_token_env = "COLDCTL_ALERT_TOKEN"
# allow_loopback_http = true # numeric loopback test receivers only
```

Gateway failures include server errors, query/record/download execution rejections
(including invalid filters and resource-limit failures), background export failures
and failed catalog refreshes. Authentication and malformed HTTP input are counted
without emitting incidents. Incidents are aggregated per operation and resolve on
its next successful operation; this does not prove every prior request is recoverable.
HTTP streaming errors after response headers are outside these outcome counters.
CLI catalog refresh records failures even while the server is stopped; queued
gateway alerts are delivered when `serve` runs. This does not make export jobs durable.
`cloud_sync` incidents and Cloud last-success/consecutive-failure gauges are sampled from background sync state. See [Background sync](BACKGROUND_SYNC.md).
