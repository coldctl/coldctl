# Read-only shadow validation

1. Create an archive with `archive run` without enabling source deletion.
2. Keep the original eligible records stable and present.
3. Refresh the customer-hosted gateway catalog, mapping this archive to a dataset.
4. Compare the retained source, verified archive and gateway ID lookup results:

```powershell
coldctl archive shadow JOB_ID --confirm-original-source --gateway-url https://archive.example.com --dataset-id DATASET_ID --token-env COLDCTL_GATEWAY_TOKEN
coldctl archive shadow-reports JOB_ID
```

The token environment variable must already exist; never put its value in a URL.
HTTPS is required except for loopback HTTP fixtures. Redirects are disabled. The
explicitly configured gateway receives archived key values through authenticated
requests; use a trusted customer-operated endpoint and a least-privilege tenant token.

Omit all three gateway flags for source-versus-archive comparison only. Such a pass
does not qualify gateway retrieval. `--max-batches N` bounds a trial; an unfinished
comparison returns nonzero and status incomplete. Full archive integrity verification
still runs before the bounded source/gateway comparison. Use
`--max-duration-seconds 300` for a five-minute cooperative budget, including
verification. The synchronous full integrity-verification pass may finish before
cancellation is observed; this is not a hard wall-clock deadline.
Expiration produces an incomplete report, never a pass. Ctrl+C also stops at a safe boundary.
Runs do not delete, restore or change source records. Explicit deletion remains separate.

## Comparison contract

The job must be a completed original archive with its original source identity and
pinned connector, stable-source contract and no deletion history. The process holds
job/policy locks, verifies archive integrity, re-reads the saved key range, and compares
every supported column. It then checks primary keys in batches through the actual
gateway search API using typed `IN` filters (64 keys by default; override with
`--gateway-batch-size 1..100`). Wide rows automatically reduce the batch size to
leave room beneath the response-size cap. Results are matched by key, independent of order.
Missing/duplicate/extra results, truncation or value
differences are failures. A dataset containing duplicate archive generations may fail.

This workflow validates **batched ID membership retrieval**, not arbitrary application
queries, joins, aggregations, collation or hot/cold routing. PostgreSQL native text
representations, MySQL text and raw BSON hex are compared exactly; floats are compared
in their declared precision. Non-finite floats fail rather than silently normalizing.
Timestamps/decimals are not rounded to hide differences. No mutable-source snapshot or
eligibility outside the saved archive bounds is claimed. An empty range proves no
retrieval behavior. MongoDB comparison is of the supported raw-BSON envelope, not
arbitrary nested-field filtering. Validation runs explicitly or automatically in the scheduler; it does not mirror
live application requests.

Gateway responses are capped at 1 MiB and each request has a 30-second timeout. Source
queries use the job timeouts. Production protection applies to source reads and gateway
requests. Requests remain sequential and bounded to avoid flooding the Gateway;
batching amortizes network and query overhead across keys. Reduce batch size for
wide rows or a Gateway configured with a small result limit. Start with bounded trials and
measure source, gateway and object-store impact. A rerun starts from the beginning;
partial reports are evidence, not resumable certification.

## Durable evidence

### MySQL Azure trial (2026-10-17)

A separate Aiven MySQL 8.4 fixture passed source/archive and full Gateway shadow
comparison for 40 retained archived rows, with zero mismatches and two batched
Gateway requests. Exact text keys, decimals, NULL and binary hex survived lookup,
record detail and JSONL export. A controlled source change produced a mismatch;
restoring it produced a full pass and the original source fingerprint. This was
manual validation on a small fixture, without automatic scheduler shadow gating.
The reusable parent-workspace command is `./demo/mysql-trial/test.ps1 -GatewayShadow`.

### Report storage

Reports in shadow.sqlite include job/run IDs, manifest checksum, timestamps, batches,
rows, gateway lookup count (keys checked), gateway HTTP request count,
source-mismatch batch count and gateway-mismatch row count. Gateway counters are
saved after every successful request; completed source-batch counters advance only
after all its Gateway checks finish. Old reports default their request count to zero.
No row values, raw errors, gateway tokens or key identifiers are persisted. Reports are
running, incomplete, mismatch, error or passed. A process crash leaves running without
a finish timestamp; it must never be treated as a pass. The CLI lists the latest 100
reports for a job; older records remain in SQLite. Successful comparison is an observation
at that time, not proof of historical authenticity or a permanent retrieval guarantee.

An optional production policy can require a recent full gateway pass before deletion;
see PRODUCTION_PROTECTION.md. Stop workers before backing up sidecars together. These
local reports are not tamper-proof attestations and are not synchronized to Cloud yet.

## Automatic recurring validation

Configure this before starting a recurring schedule. Merge these fields into the
source's existing protection JSON, preserving its operating windows and health limits:

```json
{
  "require_gateway_shadow": true,
  "shadow_max_age_seconds": 3600,
  "automatic_shadow": {
    "pg_orders": {
      "gateway_url": "https://archive.example.com",
      "dataset_id": "DATASET_ID_FROM_GATEWAY",
      "token_env": "COLDCTL_GATEWAY_READ_TOKEN",
      "refresh_token_env": "COLDCTL_GATEWAY_REFRESH_TOKEN"
    }
  }
}
```

`pg_orders` is the exact archive policy name, not the source name. Different policies
can use different datasets. Apply the merged JSON with
`coldctl protection set SOURCE --config protection.json`. This clears the old health
sample; publish a fresh sample if health thresholds are configured. Secret **values**
remain in the service environment. Use a tenant-scoped read token for lookups and a
separate administrator token for the existing gateway catalog-refresh endpoint.
The refresh token currently has gateway administrator authority; scope deployment
and network access accordingly. URL redirects remain disabled.

Configure a trusted gateway layout **once**, mapping the policy's dedicated storage
prefix to a stable tenant/dataset, so future job directories are included automatically.
For a gateway whose remote prefix is already that dedicated policy prefix:

```json
[{"prefix":"","tenant":"customer","name":"orders","source":"postgres"}]
```

Do not use an empty-prefix mapping over storage shared by unrelated tenants or tables.
Do not map only the first job UUID. Set `dataset_id` to the stable ID from the gateway
for that tenant/dataset; an empty dataset may not be listed until its first archive.
The current gateway ID is SHA-256 of the compact UTF-8 JSON array `["tenant","name"]`.
Catalog refresh discovers completed manifests. Schema drift or duplicate primary keys
across archive generations can fail the comparison; no automatic schema merge or
record deduplication is performed.

Then authorize and run the existing scheduler:

```powershell
coldctl agent scheduler recurring create pg_orders --every-seconds 86400 --source-stability immutable-rows --confirm-original-source --delete-after-archive
coldctl agent scheduler run
```

The configured worker sequence is export -> verify -> refresh gateway catalog ->
full source/archive/gateway comparison -> verified deletion. Ordinary queued jobs with
`--delete-after-archive` use the same configured gate. This does not grant deletion
authority to a schedule that lacks the existing explicit confirmations. Recurring
supports PostgreSQL, MySQL and MongoDB to S3, Azure or GCS, with their documented
stable-source contracts. MySQL and MongoDB require `quiescent-copy`; continuously
mutating sources do not acquire snapshot consistency from this gate.

Before initial deletion, the latest automatic report must be complete, recent, and
bound to the job, manifest and exact gateway configuration. Manual source-only or
partial reports cannot substitute. Empty verified archives complete without deleting
anything and without claiming retrieval coverage. Transient network, timeout, 429 and
5xx failures use bounded queue retries. Authentication/configuration errors and value
mismatches fail the cycle; inspect `archive shadow-reports JOB_ID`, fix the cause, then
explicitly retry the failed schedule. A failed cycle never advances to a new export.

At deletion admission, `shadow.sqlite` stores a durable authorization receipt containing
the passing report, manifest hash and gateway configuration hash. Once a deletion journal
exists, restarting that **same job** uses the receipt even if the report has aged out.
Requiring another full source comparison would be impossible after rows were deleted.
Every deletion invocation still verifies archive integrity; every batch checks health
and unchanged source values. The receipt does not continuously recheck gateway availability
and cannot authorize a new job or a changed manifest/dataset/configuration. Missing or
corrupt receipts fail closed. Keep `shadow.sqlite` and the main state database together
in stopped-worker backups. Receipts are local operational evidence, not tamper-proof
attestations, and are not synchronized to Cloud.

A worker crash during validation leaves a running/unfinalized report; restart compares
from the beginning. Health/window holds and shutdown leave incomplete evidence and
requeue safely. Recurring stop is checked during validation and again before deletion.
An in-flight operation can finish, and a delete batch already in progress can commit.
Shadow validation currently runs a full job per attempt, outside the export time-slice
budget, with bounded individual requests (refresh 120 seconds; lookups 30 seconds).
Choose a validation age large enough for the full run; long comparisons can delay other
queued jobs. It checks every key in bounded batches; qualify throughput before the pilot.

### Recurring parent-child groups (MySQL and PostgreSQL)

For `agent scheduler group`, configure `automatic_shadow` for every group policy
and keep `require_gateway_shadow` enabled. A group with partial automatic coverage
is rejected before deletion. Use a distinct dataset mapping for each member table;
parent and child primary keys may overlap and must not share one mixed dataset.

After all parent-first archives are complete and verified, the scheduler runs the
configured full source/Gateway comparisons for every member before deleting any
child. Child comparisons use the exact archived parent selection, including children
newer than their own retention cutoff. Parent or child mismatch, failed refresh,
failed lookup, or incomplete evidence blocks the whole group. Inspect each job's
`archive shadow-reports` and use `agent scheduler group retry GROUP_ID
--confirm-original-source` after resolving a terminal failure.

Each deletion slice rechecks all-member evidence and archive integrity. Passing
reports for unstarted members must still meet the configured age; members whose
deletion journals have begun use the existing durable receipts. This allows restart
between child and parent deletion without treating deleted children as mismatches.
New cycles have new jobs and require their own reports. No new deletion permission
is granted by this configuration. Source quiescence remains required for the full
cycle, and a group is not an atomic cross-table transaction.
