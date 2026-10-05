# coldctl

Cold data lifecycle tooling for operational databases. Analyze, archive, verify,
and retrieve historical data while keeping storage and execution on your infrastructure.

ColdCTL is designed to support multiple database engines and storage destinations.
The current agent supports **PostgreSQL and local Parquet storage**; additional
databases and destinations, including object storage, are planned.

## Current capabilities

- Discover PostgreSQL schemas and assess archival candidates from metadata.
- Define retention policies, inspect plans, and export stable rows in bounded batches.
- Verify archives, resume interrupted jobs, back up state, and restore supported
  values into a separate PostgreSQL target.
- Optionally synchronize allowlisted lifecycle metadata with ColdCTL Cloud.
  Database contents and archive files stay local; Cloud cannot execute agent commands.

This is an active-development release candidate. Source deletion, general archive
querying, and mutable-source snapshot consistency are not implemented. Production
qualification remains in progress; see the [validation record](docs/RELEASE_VALIDATION.md).

## Get started

Build from source with Rust and the platform's native build prerequisites:

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
stable data; the [operator runbook](docs/OPERATIONS.md) covers prerequisites and setup.

## Documentation

- [Cloud documentation and command reference](https://www.coldctl.com/docs)
- [Detailed CLI workflows](docs/CLI_GUIDE.md)
- [Operator runbook](docs/OPERATIONS.md)
- [Architecture](docs/architecture.md), [archive format](docs/archive-format.md),
  and [Cloud sync protocol](docs/protocol.md)
- [Release procedure](docs/RELEASING.md) and [validation evidence](docs/RELEASE_VALIDATION.md)

The agent is a Cargo workspace: `coldctl` is the CLI, `coldctl-core` owns local
archive operations, and `coldctl-cloud` implements optional metadata sync.

## License

[Apache-2.0](LICENSE)
