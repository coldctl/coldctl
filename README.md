# coldctl

Cold data lifecycle tooling for operational databases. Analyze, archive, verify,
and retrieve historical data while keeping storage and execution on your infrastructure.

ColdCTL is designed to support multiple database engines and storage destinations.
The current agent supports optional **PostgreSQL, MySQL and MongoDB connectors**
and local, customer-owned S3-compatible, or Azure Blob archive storage. See [cloud storage authentication](CLOUD_AUTH.md) for AWS roles, Azure managed identity, and a remote trial checklist.

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

### MongoDB connector

MongoDB 8.0 replica-set support is available as the separate
`coldctl-connector-mongodb` executable, with raw BSON archives and transactional
restore. See [MongoDB setup, limits and qualification](RELEASING.md).

Operational monitoring: see [metrics, durable failures and alert hooks](MONITORING.md).
`agent sync --watch` enables opt-in background metadata uploads. See [Background Cloud sync](BACKGROUND_SYNC.md) for service setup, retries, limits and status.

Production protection: see [PRODUCTION_PROTECTION.md](PRODUCTION_PROTECTION.md) for source-scoped operating windows, monitored lag/load admission and adaptive delays. Read-only source/archive/gateway comparisons are documented in [SHADOW_VALIDATION.md](SHADOW_VALIDATION.md).
