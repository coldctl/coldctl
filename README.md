# coldctl

Cold data lifecycle tooling for operational databases. Analyze, archive, verify,
and retrieve historical data while keeping storage and execution on your infrastructure.

ColdCTL is designed to support multiple database engines and storage destinations.
The current agent supports optional **PostgreSQL, MySQL and MongoDB connectors**
and local, customer-owned S3-compatible, Azure Blob or Google Cloud Storage archives. See [destination setup](SINKS.md) for GCS credentials and qualification limits. See [cloud storage authentication](CLOUD_AUTH.md) for AWS roles, Azure managed identity, and a remote trial checklist.

## Current capabilities

- Discover SQL tables or MongoDB collections and assess archival candidates.
- Define retention policies, inspect plans, and export stable rows in bounded batches.
- Verify archives, resume interrupted jobs, back up state, and restore supported
  values into a separate target database using the matching connector.
- Optionally synchronize allowlisted lifecycle metadata with ColdCTL Cloud.
  Database contents and archive files stay local; Cloud cannot execute agent commands.

This is an active-development release candidate. Verified PostgreSQL, MySQL and MongoDB source deletion
is opt-in. General archive querying and mutable-source snapshot consistency are not implemented. Production
qualification remains in progress. See [distribution](#distribution).

PostgreSQL verified deletion uses one `DELETE ... RETURNING` per bounded batch
and compares the exact deleted row versions against the verified archive before
committing. Any mismatch rolls back the entire batch; missing rows are counted
for safe replay. A `SHARE UPDATE EXCLUSIVE` table lock protects schema and safety
checks while allowing ordinary writes to unrelated rows. Target rows remain
locked until commit, and the table guard can contend with VACUUM and DDL.
The two-second lock timeout bounds lock waits. Archive integrity, source identity,
schema, trigger and foreign-key checks still apply.

MySQL verified deletion uses a READ COMMITTED InnoDB transaction, bounded
primary-key locking reads and one bulk delete per batch. Metadata locks protect
schema, trigger and incoming foreign-key checks while permitting unrelated DML.
Only locked rows whose complete values match the archive are deleted; mismatches
roll back the batch. Target-row writers and DDL can still contend. Direct TRIGGER
and PROCESS privileges are required for safety inspection. Unrelated database
privilege revokes are accepted; target-database or ambiguous revokes fail closed.

## Get started

Once a release is published, download a Windows or Linux binary from [GitHub Releases](https://github.com/coldctl/coldctl/releases).
See [installation and verification](#distribution). Alternatively, build from source
with Rust and the platform's native build prerequisites:

```bash
cargo build --release --locked -p coldctl
```

Add `target/release` to your PATH, then run:

```bash
coldctl --help
coldctl init
coldctl status
```

The Windows executable is `coldctl.exe`. SQLite is bundled. Use
`--data-dir <path>` consistently to select an isolated state directory. Supply
credentials through environment references and start with a bounded trial on
stable data; the [cloud documentation](https://www.coldctl.com/docs) covers workflows.

## Distribution

The base agent and PostgreSQL, MySQL and MongoDB connectors ship separately.
Supported package targets are Windows x64 and Linux x64 (Ubuntu 24.04 baseline;
older glibc distributions are not yet qualified).

Before installing a release, follow its release notes to verify the GitHub
attestation and SHA-256 checksum. Extract the base agent into a version-specific
directory, add it to PATH, then run `coldctl --version`, `coldctl init` and
`coldctl status`. Bootstrap connector trust using the exact root fingerprint in
the release notes, then install only the database connectors you need.
Prereleases use their own signed offline `connector-registry.zip` and an isolated
`--data-dir`; follow that release's offline installation command.

For maintainers, run **distribution check (no publication)** from GitHub Actions
after pushing the changes. It runs the reusable CI checks and builds the base
agent and all three separate connectors on Windows and Linux. Download the
`qualification-<commit>-<target>` artifacts from the workflow run within 14 days.
These are unsigned qualification packages; they are not a published release or
an installable signed connector registry.

The release workflow handles version tags matching `Cargo.toml` (`v<version>`).
Before the first release, configure the `release` GitHub environment with the
`COLDCTL_TUF_ONLINE_KEY` secret corresponding to the existing `release/root.json`,
and review environment approval rules and repository Actions permissions.
Keep offline root keys private and preserve the existing trust root.
Run qualification first, then create the chosen release tag when ready to publish.

Packages always include the executable, license, README, lockfile, `INSTALL.txt`
and `BUILD-INFO.json`. Operator guides temporarily maintained outside this
repository are optional; the build inventory lists which guides are included or
omitted. Packaging never reads the external documentation directory. Current
operator instructions are available in the [online documentation](https://www.coldctl.com/docs).
Stop workers and back up state before upgrading; rollback requires a matching
state backup rather than running an older binary against upgraded state.

## Documentation

For trusted Archive Gateway discovery, set both `COLDCTL_ARCHIVE_BINDING` and
`COLDCTL_ARCHIVE_SIGNING_SEED` in the archiving worker's environment. The seed is
a standard-base64 32-byte Ed25519 secret; enroll its public key and a unique
storage-namespace binding with the Gateway administrator. Newly completed
manifests include a signature over their contents and export-start time. Keep
the same signing configuration during job recovery. With neither variable set,
legacy unsigned manifests remain unchanged; partial or invalid configuration
fails manifest publication. Signed manifests are limited to 8 MiB. See the
Gateway README for enrollment, key rotation, OIDC and grant configuration.

- [Local and customer-owned S3 destinations](SINKS.md)
- [Long-running archival scheduler](SCHEDULER.md)
- [Verified source deletion](DELETION.md)
- [GitHub downloads, connector signing and release procedure](#distribution)
- [Cloud documentation and command reference](https://www.coldctl.com/docs)

The agent is a Cargo workspace: `coldctl` is the CLI, `coldctl-core` owns local
archive operations, and `coldctl-cloud` implements optional metadata sync.

## License

[Apache-2.0](LICENSE)

### Optional PostgreSQL connector

For a managed PostgreSQL service with a private CA, load its PEM certificate
contents into an environment variable and reference it when adding the source:

```powershell
$env:COLDCTL_POSTGRES_CA = Get-Content 'C:\certificates\provider-ca.pem' -Raw
coldctl source add postgres --name managed-pg --url 'postgres://reader@db.example.com:5432/app?sslmode=require' --password-env PG_PASSWORD --tls-ca-env COLDCTL_POSTGRES_CA
coldctl source test managed-pg
```

Only the environment variable name is saved. Supply the same variable to the
worker on each restart. The variable contains PEM text, not a filename. Bundles
may contain multiple certificates, up to 64 KiB. A configured bundle supplies
the source's trusted roots without modifying the operating system's trust store;
without a bundle, system roots remain the default. Hostname and certificate
verification stay enabled. Custom CAs cannot be combined with `sslmode=disable`.
Both password-free `--url` and full credential `--url-env` sources support this.

Use an updated agent and PostgreSQL connector together. The connector advertises
`postgres-ca-v1` and requires host support for `safe-diagnostics-v1`; an older
connector is rejected when custom CA configuration would otherwise be ignored.
Existing source configurations without CA references remain compatible.
TLS trust failures, malformed CA bundles, database connectivity failures and
unsupported PostgreSQL equality-filter types have distinct, sanitized errors.
Policy `--equals-column` still supports text/varchar/char only; this change
improves its error message rather than adding numeric equality filtering.

Database commands now require the separate `coldctl-connector-postgres` executable.
For development, run `cargo build --workspace`, set
`COLDCTL_ALLOW_UNVERIFIED_CONNECTOR=1`, then set `COLDCTL_POSTGRES_CONNECTOR` to its
absolute path. Existing source/password environment references
continue to work. See [connector setup](RELEASING.md) for PowerShell
commands, compatibility, packaging and tests. For signed packages and offline bundles,
see [managed connector installation](RELEASING.md).

### MySQL connector

MySQL 8.4/InnoDB support is available as the separate `coldctl-connector-mysql`
executable. It supports discovery, analysis, bounded export/resume, unsigned keys,
and transactional restore into a separate MySQL database. Run `coldctl init` to
upgrade existing state before adding MySQL sources. See [setup, supported types,
privileges and verification](RELEASING.md). PostgreSQL remains compatible.

MySQL `source discover <source>` reports visible foreign keys, ordered child/parent
column mappings, and `ON DELETE` / `ON UPDATE` actions in table and JSON output.
Archive plans warn about incoming references. Discovery is advisory: privileges
can hide relationships, and undeclared application relationships are not detected.

MySQL single-job deletion conservatively blocks every incoming foreign key, including
`CASCADE`, `SET NULL`, self references and empty child tables. It does not assume
children were archived by another job. Every deletion batch checks authoritative
InnoDB metadata while holding a write-compatible metadata lock on the parent;
unreadable metadata fails closed and requires appropriate PROCESS privileges.
Foreign key checks must remain enabled. Archive-only remains available, but
explicit coordinated deletion is described below. Relationship
discovery applies to MySQL and PostgreSQL; rebuild the CLI and matching connector
together when upgrading from a development build.

#### Coordinated MySQL and PostgreSQL policies

Create one retention policy per table, including every incoming child table.
By default each policy selects its own rows. Add `--select-children` to select
descendants through archived parent FK values instead. Confirm that all selected
tables remain quiescent during export.

```sh
coldctl archive group-run --policies orders items item_details --confirm-quiescent-source
coldctl archive group-run --policies orders items item_details --select-children --confirm-quiescent-source
coldctl archive group-delete --jobs ORDERS_JOB ITEMS_JOB DETAILS_JOB --confirm-delete --confirm-original-source
```

`group-run` archives and verifies every member without deleting anything.
`group-delete` re-verifies all archives before any deletion, orders descendants
before parents, and uses durable per-job deletion journals. Parent batches lock
their keys and check all incoming FK mappings against authoritative InnoDB
metadata. Any remaining reference blocks deletion: CASCADE and SET NULL must
never remove or alter unarchived children. Ordinary `archive delete` retains its
blanket incoming-FK refusal.

Groups support 2 to 32 distinct MySQL or PostgreSQL tables on one source. Cycles, self references,
missing visible child tables and incomplete/imported jobs are rejected. Hidden
metadata or unreadable child tables fail closed in the connector. Foreign key
enforcement must remain enabled for concurrent application writers.

This is resumable child-first work, not an atomic cross-table transaction: a
failure can leave already verified child rows deleted while parents remain.
Retry `group-delete` with the same job IDs after resolving the cause. If export
was interrupted, use `jobs list`, resume incomplete jobs and archive any remaining
policies individually before supplying all completed IDs. Archive immutability and
the existing original-source, trigger, schema and protection checks still apply.

With `--select-children`, roots use their normal retention/equality rules. Each
descendant instead selects the union of rows matching any archived in-group parent
FK tuple, regardless of the child's age or equality filter. New children of old
parents are included; old children of unselected parents are excluded. Multiple
FK matches do not duplicate a child row, composite FK order is preserved, and an
empty parent archive selects no children. Tables/policies remain explicit; child
policies currently still require a valid time column, although that filter is ignored.

Parents are archived and verified first. Their exact referenced values are streamed
in bounded batches into indexed, connection-local MySQL temporary tables; the source
account needs `CREATE TEMPORARY TABLES` as well as its normal read privileges.
No persistent source tables are created by selection. Temporary storage scales with
the referenced parent values, with up to 64 incoming selection FKs per child.
The saved child plan binds parent job IDs, manifest hashes, row counts and FK mappings.
Resume and shadow validation rebuild the selection from those verified archives;
keep parent jobs and archives available. Group deletion requires those exact parent
jobs and re-verifies all archives before deleting any rows. New children arriving
after the child archive still block parent deletion. This is not a live snapshot
or CDC mechanism; quiescence remains required through export/resume.

PostgreSQL supports the same group commands, automatic child selection, recurring
cycles and Gateway shadow gate. It uses typed, indexed temporary tables and needs
TEMP privilege on the database. Foreign-key columns must have identical built-in
types, modifiers and collations, with built-in equality. Groups still require a
single integer primary key on each member, ordinary non-partitioned tables and
source quiescence. Cycles, inheritance, RLS and user-triggered deletion are rejected.

PostgreSQL parent deletion uses READ COMMITTED, locks the selected parent rows
FOR UPDATE, and checks every incoming FK under metadata guards before DELETE.
It requires origin replication mode and enabled referential-integrity triggers.
Remaining children block both CASCADE and SET NULL; ordinary single-table deletion
continues to reject all incoming FKs. These locks permit ordinary writes to other
rows but can delay writers to selected parent keys and conflicting DDL/maintenance.

#### Recurring parent-child archival

Run `coldctl init` to upgrade local state, then create a group schedule:

```sh
coldctl agent scheduler group create --policies orders items item_details --every-seconds 3600 --confirm-quiescent-source --confirm-original-source --delete-after-archive
coldctl agent scheduler run
coldctl agent scheduler group list
coldctl agent scheduler group show GROUP_ID
coldctl agent scheduler group retry GROUP_ID --confirm-original-source
coldctl agent scheduler group stop GROUP_ID
```

Automatic child selection is always enabled for recurring groups. Each cycle
freezes the root retention cutoff, archives parents before descendants, verifies
all member archives, then deletes child-first in bounded batches. Every deletion
slice re-verifies the full group. The next cycle starts only after every member's
deletion journal completes, followed by the configured delay (60 seconds minimum).
It rescans eligible roots, including backdated arrivals with smaller primary keys.
Local, S3, Azure Blob and GCS destinations use the existing archive paths.

The foreground worker must remain running; use a service manager for unattended
operation. Queue entries, archive job IDs, phase and deletion progress survive
worker restarts. Transient errors use the existing retry backoff; terminal errors
hold the whole group. Retry continues the same cycle. Stop prevents further cycles
and requests cancellation at a safe boundary; an in-flight batch may finish.
Policies are reserved while a group is active or failed. Configuration changes
hold the schedule; restore its original configuration before retrying.

All member tables must remain quiescent through each complete cycle and its
retries. This does not acquire a database-wide snapshot or coordinate application
writers. A late unarchived child blocks its parent; previously verified children
may already have been deleted. Keep every parent archive needed by descendants.
Existing source-protection checks still apply. To enable automatic Gateway shadow
validation, set `require_gateway_shadow: true` and an `automatic_shadow` entry for
**every member policy** in the source protection configuration. Each entry names
its Gateway dataset and read/refresh credential environment variables; see
[shadow configuration](SHADOW_VALIDATION.md). Partial group coverage blocks deletion.

Before deleting any member, the scheduler validates the whole group against the
source and Gateway, including newer children selected from archived parent keys.
A mismatch or Gateway failure holds the group before source deletion. Retry keeps
the same jobs. Once a member has begun verified deletion, its manifest-bound receipt
supports further slices and recovery without comparing already deleted source rows.
Unstarted members require current passing reports. Cached valid evidence can be
reused; this is not a requirement for continuous Gateway availability after approval.
Quiescence and immutable archives remain required. Full archive verification still
runs before every deletion slice; large groups and archives need performance testing.


### MongoDB connector

MongoDB 8.0 replica-set support is available as the separate
`coldctl-connector-mongodb` executable, with raw BSON archives and transactional
restore. See [MongoDB setup, limits and qualification](RELEASING.md).

Operational monitoring: see [metrics, durable failures and alert hooks](MONITORING.md).
`agent sync --watch` enables opt-in background metadata uploads. See [Background Cloud sync](BACKGROUND_SYNC.md) for service setup, retries, limits and status.

Production protection: see [PRODUCTION_PROTECTION.md](PRODUCTION_PROTECTION.md) for source-scoped operating windows, monitored lag/load admission and adaptive delays. Read-only source/archive/gateway comparisons are documented in [SHADOW_VALIDATION.md](SHADOW_VALIDATION.md).
