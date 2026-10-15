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
still runs before the bounded source/gateway comparison. Ctrl+C also stops at a safe boundary.
Runs do not delete, restore or change source records. Explicit deletion remains separate.

## Comparison contract

The job must be a completed original archive with its original source identity and
pinned connector, stable-source contract and no deletion history. The process holds
job/policy locks, verifies archive integrity, re-reads the saved key range, and compares
every supported column. It then performs one equality lookup per archived primary key
through the actual gateway search API. Missing/duplicate results, truncation or value
differences are failures. A dataset containing duplicate archive generations may fail.

This initial workflow validates **ID equality lookups**, not arbitrary application
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
requests. Large runs make one gateway request per row; start with bounded trials and
measure source, gateway and object-store impact. A rerun starts from the beginning;
partial reports are evidence, not resumable certification.

## Durable evidence

Reports in shadow.sqlite include job/run IDs, manifest checksum, timestamps, batches,
rows, gateway lookup count, source-mismatch batch count and gateway-mismatch row count.
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
source/sink support is unchanged: PostgreSQL or MongoDB to S3, with their documented
stable-source contracts. Continuously mutating MongoDB collections are not qualified.

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
queued jobs. It makes one lookup per record, so qualify throughput before the pilot.
