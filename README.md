# coldctl

Cold data lifecycle tooling for operational databases. Analyze, archive, verify,
and retrieve historical data while keeping storage and execution on your infrastructure.

ColdCTL is designed to support multiple database engines and storage destinations.
The current agent supports optional **PostgreSQL, MySQL and MongoDB connectors**
and local, customer-owned S3-compatible, or Azure Blob archive storage. See [destination setup](SINKS.md).

## Current capabilities

- Discover SQL tables or MongoDB collections and assess archival candidates.
- Define retention policies, inspect plans, and export stable rows in bounded batches.
- Verify archives, resume interrupted jobs, back up state, and restore supported
  values into a separate target database using the matching connector.
- Optionally synchronize allowlisted lifecycle metadata with ColdCTL Cloud.
  Database contents and archive files stay local; Cloud cannot execute agent commands.

This is an active-development release candidate. Verified PostgreSQL, MySQL and MongoDB source deletion
is opt-in. General archive querying and mutable-source snapshot consistency are not implemented. Production
qualification remains in progress. See [distribution and supported limits](RELEASING.md).

## Get started

Download a published Windows or Linux binary from [GitHub Releases](https://github.com/coldctl/coldctl/releases).
See [installation and verification](RELEASING.md). Alternatively, build from source
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

## Documentation

- [Local and customer-owned S3 destinations](SINKS.md)
- [Long-running archival scheduler](SCHEDULER.md)
- [Verified source deletion](DELETION.md)
- [GitHub downloads, connector signing and release procedure](RELEASING.md)
- [Cloud documentation and command reference](https://www.coldctl.com/docs)

The agent is a Cargo workspace: `coldctl` is the CLI, `coldctl-core` owns local
archive operations, and `coldctl-cloud` implements optional metadata sync.

## License

[Apache-2.0](LICENSE)

### Optional PostgreSQL connector

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
