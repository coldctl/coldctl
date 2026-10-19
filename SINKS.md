# Customer-owned archive destinations

The agent supports local directories, S3-compatible object storage, Azure Blob Storage and Google Cloud Storage. Archive
execution, verification, restore and verified source deletion use the same
`ArchiveDestination` interface. Provider details are confined to their adapters and the
destination factory; future sinks implement this contract without changing the
source connectors or archive loop. This is a built-in sink adapter, not yet a
separately downloadable sink plugin.

## Configuration

Run `coldctl init` to upgrade existing state to schema 18. Existing local and S3
destinations, policies, jobs and manifests remain usable. Configuration commands
do not resolve credentials or contact storage.

```powershell
$env:AWS_ACCESS_KEY_ID = '<customer-provided access key>'
$env:AWS_SECRET_ACCESS_KEY = '<customer-provided secret>'
coldctl destination add s3 --name customer-s3 --bucket customer-archive --region us-east-1 --prefix coldctl
coldctl destination test customer-s3
coldctl destination show customer-s3 --output json
```

Use `--access-key-env`, `--secret-key-env` and optionally `--session-token-env` to
reference different environment names. Only names are saved, never secret values.
Credentials are resolved on storage operations; show/list work without them.
Environment credentials remain supported. Use `--role-auth` for AWS web identity or EC2 IMDSv2 credentials with automatic refresh. See [cloud authentication](CLOUD_AUTH.md) for AWS and Azure identity modes.

Point a normal archive policy's `--destination` at `customer-s3`. The existing
archive, resume, verify, restore, deletion and scheduler commands then use that
sink. Each job freezes its destination configuration; later configuration removal
or replacement does not retarget that job. Credential values may change under the
saved environment references. Cloud reports still exclude storage configuration.

`--staging-path` selects durable local staging (default: DATA_DIR/staging/NAME).
Keep this directory across worker restarts. The saved path is staging for remote sinks,
and final archive storage for local destinations. Do not remove pending staging
files while a job is resumable. Successful batches remove their staging files.

## S3 emulator

Moto is a development S3 emulator. Its credentials below are public fixtures;
it does not qualify real AWS IAM, durability, KMS or retention behavior.
If the development-test container `coldctl-s3-emulator` is already running on
port 19000, reuse its `coldctl-test` bucket and skip the two Docker setup commands.

```powershell
docker compose -f scripts/compose.s3.yml up -d
docker compose -f scripts/compose.s3.yml exec -T s3 python -c "import boto3; boto3.client('s3',endpoint_url='http://localhost:5000',region_name='us-east-1',aws_access_key_id='coldctl_fixture',aws_secret_access_key='coldctl_fixture_only').create_bucket(Bucket='coldctl-test')"
$env:AWS_ACCESS_KEY_ID = 'coldctl_fixture'
$env:AWS_SECRET_ACCESS_KEY = 'coldctl_fixture_only'
$env:COLDCTL_TEST_S3_ENDPOINT = 'http://127.0.0.1:19000'
coldctl destination add s3 --name emulator --bucket coldctl-test --region us-east-1 --prefix trial --endpoint http://127.0.0.1:19000 --allow-http
coldctl destination test emulator
cargo test -p coldctl-core --lib s3_ -- --ignored
```

The endpoint must be an HTTPS origin. HTTP requires `--allow-http` and is limited
to loopback emulator addresses. URLs with credentials, query parameters or paths
are rejected. The bucket must already exist; Coldctl does not provision buckets.
Stop the emulator with `docker compose -f scripts/compose.s3.yml down` when done;
its test data is disposable. Do not delete production source rows based on archives
held only in an emulator.
For the manually started development-test container, use `docker stop coldctl-s3-emulator`.

## Azure Blob Storage and Azurite

The container must already exist. Production uses the public Azure Blob endpoint
by default; `--endpoint` supports a custom HTTPS origin. Credentials use a storage
account key from the named environment variable. SAS, managed identity and Entra ID
authentication are not exposed in this release. No connection string or key is saved.

```powershell
$env:AZURE_STORAGE_ACCOUNT_KEY = '<customer storage account key>'
coldctl init
coldctl destination add azure --name customer-azure --account mystorageaccount --container archives --prefix coldctl
coldctl destination test customer-azure
```

Use `--account-key-env` for a different variable. `--staging-path` and all archive,
resume, verify, restore and verified deletion flows work as with S3. Set a policy's
`--destination customer-azure`. Cloud receives only destination ID/name/kind.
Azure account keys grant broad storage-account access: use a dedicated archive
account and keep the key in the customer's worker environment. Rotate it there.
Coldctl requires blob read/write and deletion of its unique access-test probes;
it never deletes archive blobs. Customer encryption, immutability and lifecycle
policies are external configuration. Immutability rules must permit probe cleanup.

For local development, Azurite runs with a public fixture account and key. If
`coldctl-azurite-emulator` is already running on port 11000, skip `compose up` and
reuse it. Run these commands from the agent repository:

```powershell
docker compose -f scripts/compose.azure.yml up -d
python scripts/init-azurite.py
$env:AZURE_STORAGE_ACCOUNT_KEY = 'Y29sZGN0bC1henVyaXRlLWZpeHR1cmUta2V5LW9ubHk='
$env:COLDCTL_TEST_AZURE_ENDPOINT = 'http://127.0.0.1:11000/coldctltest'
coldctl init
coldctl destination add azure --name azure-emulator --account coldctltest --container coldctl-test --prefix trial --endpoint http://127.0.0.1:11000/coldctltest --allow-http
coldctl destination test azure-emulator
cargo test -p coldctl-core --lib azure_ -- --ignored
```

HTTP requires explicit `--allow-http` and a literal loopback endpoint. Azurite's
endpoint includes the account path; other paths, credentials and query strings
(including SAS tokens) are rejected. SDK ambient emulator endpoint overrides are
not used. The helper creates only the local fixture container and never Azure resources.
Stop with `docker compose -f scripts/compose.azure.yml down`, or
`docker stop coldctl-azurite-emulator` for the manually started container.
Fixture data is disposable; never delete production source rows against it.
Azurite coverage does not qualify Azure durability, authorization, firewalls,
private endpoints, encryption, retention or service throttling. Real Azure
qualification remains separate work. The independent archive gateway now reads
completed manifests from both S3 and Azure; see its README and remote TOML examples.

## Sink contract and recovery

- Keys are relative, validated object keys. Sink configuration owns the storage
  namespace; source engines do not choose buckets or endpoints.
- Publication is create-only. S3 and Azure use conditional PUT (`If-None-Match: *`). An
  existing key is accepted during resume only if its content matches the durable
  checkpoint. An incompatible provider must fail, never fall back to overwrite.
- Every publication is read back and compared with SHA-256 and length. ETags are
  not content checksums. Reads used for verification/restore/deletion return a
  seekable, checksum-verified temporary file; remote objects are not trusted merely
  because they have the expected name or metadata.
- The journal records a staged batch before upload and commits progress after
  remote verification. Lost acknowledgements are reconciled against the same key
  and digest on resume. The manifest is published last, after every batch commits.
- Publication and reads are bounded to 128 MiB per object. Upload retry is at the
  batch/object boundary, not multipart-part resume. PUT payload memory is bounded;
  downloads stream to temporary disk and are removed on close. Existing archive
  row limits normally produce much smaller batches. No entire-archive local copy
  is required. Source reads remain sequential.
- Verification and resume read committed objects in full. Budget storage requests,
  transfer cost and local read-back space; short scheduler slices can repeatedly
  rescan a large archive. Large-archive throughput and slice sizing require qualification.
- SDK requests have timeouts and bounded retries. Retryable remote failures retain
  journal/staging state and are eligible for scheduler retry. Authentication,
  permission, missing-object and precondition failures stop for operator action.
- State backups remain essential. Restore local state and pending staging to
  resume jobs. If only the bucket survives, download a complete UUID job directory
  using your storage tooling and use existing local archive inspection/import;
  imported archives cannot authorize source deletion. Direct bucket import is deferred.
- Temporary read-back files are cleaned on normal close. Abrupt host termination
  can leave staging/orphan files; automatic broad deletion is intentionally absent.

Configure bucket access for GetObject and PutObject on the selected prefix,
ListBucket as needed to distinguish missing keys, and DeleteObject for only the
`.coldctl-probe-*` keys used by `destination test`. Coldctl does not delete archive
objects. Use customer-managed bucket encryption, retention, versioning and access
controls appropriate for the deployment. These remain customer configuration;
emulator tests cannot establish their correctness. Objects must remain immutable
while verifying or deleting source data. Out-of-band replacement/removal causes
verification failure; a successful upload cannot guarantee future availability.

Real S3 qualification, IAM/KMS failure tests, automatic archive
expiry, multipart uploads and additional sink adapters are separate follow-ups.

Implementation references: [object_store S3 adapter](https://docs.rs/object_store/0.12.5/object_store/aws/index.html)
and [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).


## Google Cloud Storage

Run `coldctl init` to upgrade to schema 18 before adding a GCS destination. Existing
policies, jobs, queues and recurring schedules retain their IDs and configurations.
The bucket must already exist; coldctl does not create cloud buckets.

```powershell
# Optional explicit credential file; the variable holds a path, not JSON contents.
$env:GOOGLE_APPLICATION_CREDENTIALS = 'C:\credentials\coldctl-service-account.json'
coldctl destination add gcs --name customer-gcs --bucket customer-archive --prefix coldctl
coldctl destination test customer-gcs
```

Use `--credentials-env COLDCTL_GCS_CREDENTIALS` to select another variable containing
an absolute ADC JSON file path. Only its name is stored. Without an explicit variable,
the adapter checks `GOOGLE_APPLICATION_CREDENTIALS`, the local gcloud ADC file, then
attached GCP instance credentials. Tokens refresh through the storage SDK. Missing
explicit credentials fail rather than falling back. Service-account and authorized-user
ADC files are supported. The pinned SDK does **not** support external-account workload
identity federation files; these fail explicitly. Credential files cannot override
storage endpoints or disable authentication. Keep files private to the service account.

Use the normal archive/resume/verify/restore/shadow commands with this destination.
Recurring PostgreSQL/MySQL/MongoDB archives also accept GCS and retain their existing
stable-source requirements and verified-deletion gates. Publication is create-only,
reads are SHA-256 verified, and pending batches use durable local staging. No source
deletion is implied by configuring a bucket. Cloud sync sends only destination ID,
name and kind (`gcs`), never the bucket, prefix or credential configuration. The Cloud
API must be upgraded to accept the new kind before syncing GCS metadata.

Grant the archive writer object create/get permissions and delete permission for its
unique access-test probe. `roles/storage.objectUser` at bucket scope includes these;
a narrower custom role is possible. Give the Gateway separate read/list permissions
(e.g. bucket-scoped `roles/storage.objectViewer`). Retention locks can prevent probe
cleanup; failed cleanup is reported as a failed access test. Customer KMS permissions,
retention policies, IAM and real GCP identity still need qualification in your account.
See [Google authentication](https://cloud.google.com/storage/docs/authentication) and
[Storage IAM roles](https://cloud.google.com/iam/docs/roles-permissions/storage).

### Local GCS qualification

The pinned XML-compatible emulator fork recommended by the
[object_store project](https://github.com/apache/arrow-rs-object-store/blob/main/CONTRIBUTING.md)
exercises protocol behavior; it does not qualify Google IAM, durability or retention guarantees.
It ignores generation-match create preconditions. Coldctl correctly rejects that
endpoint in `destination test`. The archive tests use a loopback-only Python proxy
that models create-only conflicts under a lock; this is a test fixture, not proof
of GCS concurrency behavior. Real GCP qualification remains required.
Emulator mode is anonymous and only allows explicit HTTP loopback endpoints. It does
not resolve Google credentials. Do not use emulator archives to authorize live deletion.

```powershell
docker compose -f scripts/compose.gcs.yml up -d
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:14443/storage/v1/b -ContentType application/json -Body '{"name":"coldctl-test"}'
./scripts/test-gcs.ps1
# In archive-gateway, retrieval uses the emulator directly:
$env:COLDCTL_TEST_GCS_ENDPOINT = 'http://127.0.0.1:14443'
# cargo test --locked --lib gcs_gateway_end_to_end -- --ignored
docker compose -f scripts/compose.gcs.yml down
```
