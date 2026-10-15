# Verified source deletion

Source deletion is an explicit step after archival. It removes only archived
primary keys whose current values still match a verified, completed archive.
PostgreSQL, MySQL 8.4 InnoDB and MongoDB 8.0 replica sets are supported.

Upgrade local state and install an updated connector for your engine advertising
`verified-delete-v1`. For an existing completed archive:

```sh
coldctl init
coldctl archive delete JOB_ID --confirm-delete --confirm-original-source --max-batches 1
coldctl archive deletion-status JOB_ID
```

Repeat `archive delete` with the same job ID to continue, or omit `--max-batches`
to process all remaining batches. Ctrl+C stops between batches. Deletion-status
prints `null` if no deletion checkpoint exists yet. Archive and deletion commands
must use the same `--data-dir` as the original job.

`--confirm-delete` authorizes irreversible source deletion. Confirmation of the
original source asserts that the original database and eligible rows remain
stable and archive files remain immutable throughout deletion and retries.
Do not reinsert or reuse archived primary keys during deletion or retries: a
reinserted row with identical values cannot be distinguished from the original.
Keep the archives: source deletion does not remove archive files.

## What is checked

- The job must be completed, locally created, and have its original safety
  contract and checkpoints. Imported archives cannot authorize deletion.
- The full manifest and every batch are verified before connecting for deletion.
  Each batch is checked again, and decoding uses the same bytes whose digest was
  verified, before sending its rows to the connector.
- The configured source ID and engine, recorded source identity, table or
  collection identity, primary key, columns and schema signature must match the
  original archive. Identity details vary by engine below.
- Inside each transaction, the connector compares every existing row with its
  archived values before deleting that key. Any mismatch rolls back the entire
  batch, including earlier deletes in that batch. Diagnostics omit row values.
- PostgreSQL tables with row-level security, inheritance, partitions, user triggers, rules,
  or incoming foreign keys are refused. In particular, deletion cannot cascade
  into unarchived child tables.

## PostgreSQL

The PostgreSQL role needs schema `USAGE`, table `SELECT` and `DELETE`. Use the same
role recorded by the archive and grant `DELETE` when enabling deletion; changing
the source username fails the identity check. No source journal table or schema
creation privilege is required. A new approved connector can delete an older
archive, while the original source checks still apply.

Each batch holds a `SHARE ROW EXCLUSIVE` table lock while validating and deleting.
This temporarily blocks other writers, but permits ordinary reads. Batches use the
archive's original batch size, at most 1,000 rows. Lock acquisition has a two-second
timeout, statements a ten-second timeout, and a batch a 120-second connector
deadline. Earlier committed batches stay deleted if a later batch fails.

## MySQL

MySQL 8.4 InnoDB deletion preserves unsigned keys, including `BIGINT UNSIGNED`
values above the signed 64-bit range. It checks the server UUID and original
table identity/schema, then compares all archived column values before deleting.
Tables with triggers or incoming foreign keys are refused, including child
tables in other databases. Changed or oversized rows roll back the entire batch.

The role needs `SELECT`, `DELETE`, `LOCK TABLES`, `TRIGGER`, and global `PROCESS`
for InnoDB identity/foreign-key metadata. `TRIGGER` must be directly granted on
the table, database, or globally; role-only grants and partial revokes are refused
because an empty trigger listing otherwise does not prove that no triggers exist.
No source journal or schema creation privileges are needed.

Each batch (at most 400 rows) uses `SET autocommit=0`, `LOCK TABLES ... WRITE`,
comparison/deletion, `COMMIT`, then `UNLOCK TABLES`. The write lock blocks other
readers and writers while the bounded batch runs. Errors roll back before
unlocking. See [MySQL transaction and table-lock rules](https://dev.mysql.com/doc/refman/8.4/en/lock-tables.html).

## MongoDB

MongoDB deletion requires the existing `quiescent-copy` stability contract and
an authenticated MongoDB 8.0 replica set. It checks the endpoint, deployment,
collection UUID and options. Each batch (at most 100 documents) uses a snapshot
transaction with majority commit and compares exact archived BSON bytes, including
numeric types and field order, before deleting by ObjectId. Oversized changed
documents fail the batch; they are never treated as absent. Concurrent changes
that conflict with deletion abort the transaction.

The source role needs the existing archive/discovery permissions plus `remove`
on the source collection (`readWrite` plus the connector's `clusterMonitor`
permissions also works). No source journal or restore ledger is created.
Collection identity is rechecked after the transaction's first read acquires
its collection lock. See [MongoDB transaction behavior](https://www.mongodb.com/docs/manual/core/transactions-production-consideration/).

## Restart and uncertain commits

Deletion progress commits to local SQLite only after the source transaction
acknowledges its commit. If the process dies between those commits, rerunning the
same command revisits the same keys: already absent rows are counted and remaining
rows must still match the archive. A lost connector response fails the command;
inspect progress and rerun the same job. Do not create another archive to retry
deletion.

`rows_processed` counts archived rows reconciled. `rows_deleted` counts deletions
whose acknowledgements were recorded locally. `rows_absent` includes rows already
absent and deletions whose acknowledgement was lost. These counters do not claim
to distinguish those two causes. A completed deletion is terminal: running it
again does not remove subsequently inserted rows.

The original archive manifest remains immutable and describes the export phase.
Its `source_deleted: false` field is not updated. Use `archive deletion-status` for
the subsequent deletion record, and preserve state backups as well as archives.

## Run through the scheduler

```sh
coldctl agent scheduler enqueue orders --source-stability immutable-rows --confirm-original-source --delete-after-archive
coldctl agent scheduler run
```

For MongoDB use `--source-stability quiescent-copy`.

The flag explicitly authorizes deletion; existing queue entries remain archive-only.
The scheduler verifies the completed archive and deletes one batch per turn,
resuming from deletion checkpoints after restart. The queue becomes completed
only after both archive and deletion finish. `scheduler cancel QUEUE_ID` stops
future batches; an in-flight batch may commit. Previously deleted rows are not
restored by cancellation. Retry a failed queue entry after resolving its cause,
using the existing `scheduler retry` command and original-source confirmation.

## Integration tests

Build both native connectors, then run their ignored `verified_deletion` tests
against disposable databases only:

```sh
cargo build -p coldctl-connector-mysql -p coldctl-connector-mongodb
cargo test -p coldctl-connector-mysql -p coldctl-connector-mongodb verified_deletion -- --ignored
```

Set `COLDCTL_ALLOW_UNVERIFIED_CONNECTOR=1`, `COLDCTL_MYSQL_CONNECTOR` and
`COLDCTL_MONGODB_CONNECTOR` to the freshly built development binaries. MySQL needs
`COLDCTL_TEST_MYSQL_URL` (an admin URL for the disposable `coldctl_test_source`
database), `COLDCTL_TEST_MYSQL_CA` (PEM text), and a `coldctl_test_restore` database
for cross-database foreign-key tests. MongoDB needs `COLDCTL_TEST_MONGODB_PORT`,
`COLDCTL_TEST_MONGODB_CA` (PEM text), and `COLDCTL_TEST_MONGODB_PASSWORD` set to the
fixture password `coldctl_mongodb_fixture_only`. Create `deletion_writer` in
`coldctl_mongo_source` with that password, `readWrite` on that database and
`clusterMonitor` on `admin`. Use a single-node authenticated replica set with
TLS certificates valid for `localhost`.

Tests cover changed-record rollback, oversized records, source-commit replay,
deletion checkpoints, terminal completion, and scheduler restart. MySQL also
checks unsigned keys, missing privileges, triggers and hidden cross-database
foreign keys; MongoDB checks BSON type changes and collection replacement.
