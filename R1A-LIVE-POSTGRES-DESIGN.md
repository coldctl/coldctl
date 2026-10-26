# R1a: live PostgreSQL archival with value-equivalent deletion

Status: implementation in progress. Value equivalence selected by the user on 23 October 2026. The live connector transaction, schema 20 deletion ledger, CLI/scheduler enrollment checks, and Cloud deferred/uncertain counters are implemented locally. Acceptance qualification is ongoing; this does not mark the Phase 1 launch gate complete. Baseline: local state schema 19, upgraded transactionally to 20.

## Rollout

Deploy the Cloud API accepting the optional paired `deferredRows` / `uncertainAbsentRows` fields before enabling live agents; older API strict allowlists reject those reports. Existing agents retain their original payload shape. Upgrade state with `coldctl init`, then install the matching PostgreSQL connector advertising `postgres-live-values-v1`. Old pinned jobs keep their original mode and connector.

Choose `--source-stability live-values` for a new archive or schedule. Grant SELECT and DELETE on the source table and UPDATE on at least one column (plus schema USAGE). Enrollment checks these privileges; the deletion transaction rechecks them. No grants are issued automatically. Groups, MySQL and MongoDB do not support this mode.

Completed jobs can contain deferred rows. Use a new full export cycle to reconsider them. `rows_absent_uncertain` is a subset of `rows_absent` after replay of a pending intent; neither is added to confirmed deletions. Full archive verification remains enabled. Measured throughput, managed-database qualification, and the two-design-partner launch gate remain outstanding.

## Local acceptance evidence (23 October 2026)

- PostgreSQL 15: mixed matching/changed/ineligible/oversized/absent rows, A-to-B-to-A equivalence, lost source-commit acknowledgement with pending-ledger replay, and idempotent completed-job resume pass.
- PostgreSQL 15: concurrent candidate update is deferred, unrelated writes proceed, schema/trigger/FK changes remain fenced, lock timeout preserves the source, and the next full export revisits the deferred key.
- Least privilege: live mode refuses SELECT+DELETE alone, accepts column-level UPDATE, and rechecks revoked privileges. Legacy deletion still succeeds without UPDATE.
- SQLite: schema 19 history preservation, rollback of invalid history, stale response rejection, immutable batch identity, count bounds, replay uncertainty and exactly-once aggregate application pass.
- Cloud: strict legacy/live payload validation and frontend TypeScript checks pass. Deploy the API compatibility change before live agent reports.

These are local acceptance checks, not the full qualification matrix below. Managed PostgreSQL versions, process-kill/network fault campaigns, throughput and reclaim measurements, and design-partner deployments remain open.

## 1. Contract and boundaries

Phase 1 supports ordinary PostgreSQL tables with the existing single smallint/integer/bigint primary key and supported column types. Preserve restrictions on partitions/inheritance, RLS, user triggers, rules, incoming foreign keys, imported archives, source identity and schema drift. Parent-child groups and other engines keep their existing contracts.

Introduce explicit `--source-stability live-values` / `SourceStability::LiveValues`. Persist this choice in the job safety contract and recurring schedule. Do not reinterpret existing immutable/quiescent jobs or schedules. Existing jobs continue to fail a whole deletion batch on mismatches. Live mode requires a newly created job; do not rewrite old manifests to enable it.

For a live job, delete a source row only when its current archived-column values match the verified archive and it still satisfies the frozen job predicate. An update followed by reversal and an identical-value delete/reinsert are intentionally eligible. No `xmin`, `ctid`, application version column, or history-of-mutation guarantee is used. This guarantees preservation of archived values, not preservation of a particular historical row incarnation.

“Untouched” means no changed application values are deleted or updated by ColdCTL. Candidate rows may be briefly locked; this is not a promise of zero blocking. Missing rows are absent at the locking read, not proven absent forever. A concurrently inserted replacement not observed by that read is not targeted and is reconsidered on a later cycle.

Completion means every archived input row has a durable outcome for this job. It does not mean the source table is empty or every archived key is currently absent. Deferred rows remain in the source and are reconsidered through a new export next cycle, never by silently reopening a completed job.

### Value comparison

Keep the existing `postgres_delete::same` semantics, versioned as part of the live capability:

| Type | Equivalence |
| --- | --- |
| NULL | Equal only to NULL; distinct from empty text and JSON null |
| Boolean | Normalize supported true/t and false/f representations |
| float4 / float8 | Compare parsed bits; distinguish signed zero; treat two NaNs as equivalent, as today |
| Other supported types | Exact canonical exported text, using the existing UTC, DateStyle and float-output settings |

This is deliberately conservative. Do not substitute general SQL equality, approximate numeric comparison, collation equivalence, or JSON structural equality. Preserve numeric precision. A legitimate current row that now exceeds the 64 KiB representation limit is deferred; invalid archived input, unknown types and decode failures abort the transaction. Comparison behavior changes require a new capability/version.

## 2. Connector transaction

Choose a bounded locking read followed by deletion of matching keys. This reuses the existing comparator and separates absent rows from mismatches without deleting mismatches first.

### Privilege tradeoff

PostgreSQL locking reads require SELECT plus UPDATE privilege on at least one column. Live mode therefore requires SELECT on archived/predicate columns, DELETE, schema USAGE, and UPDATE on one column (document a column-scoped grant on the primary key). ColdCTL performs no UPDATE. Verify this before export/schedule admission and again at DeleteOpen; never grant privileges automatically.

This is a deliberate expansion relative to the existing SELECT+DELETE path. Preserve the existing no-UPDATE privilege test for legacy mode. If design partners reject this privilege, use a separately reviewed conditional-DELETE design with proven equivalent comparison semantics; do not silently fall back to delete-then-compare.

PostgreSQL references: [locking-read privileges](https://www.postgresql.org/docs/15/sql-select.html) and [Read Committed concurrency behavior](https://www.postgresql.org/docs/15/transaction-iso.html).

### Sequence

1. Before opening a source transaction, verify the completed original archive and load the exact bounded batch. Preserve all authorization, manifest binding, source-protection, cancellation and job/policy-lock checks. R1a initially keeps full verification; R10b is independent.
2. Validate nonempty bounded input, unique valid keys, row shape and byte limits. Keep current maximum 1,000 rows and configured batch limit. No destination I/O or local checkpoint writes while source locks are held.
3. Begin READ COMMITTED. Keep the existing 2-second lock timeout and 10-second statement timeout as initial defaults. Add a 30-second whole-transaction deadline and server-side idle-in-transaction timeout; a timed-out/disconnected session must not retain locks. Test cleanup independently of client cancellation. Tune only from measured source impact.
4. Acquire the existing SHARE UPDATE EXCLUSIVE table guard. Recheck table/database identity, signature, supported schema, permissions and all unsafe-table restrictions under the guard. This permits ordinary DML but can conflict with maintenance; timeout/retry rather than bypass it.
5. Lock candidates by archived key only, including rows that are now ineligible. Quote validated identifiers and bind all values:

   ```sql
   SELECT key::bigint,
          <bounded canonical row representation>,
          (<frozen retention/equality predicate>) IS TRUE AS eligible
   FROM ONLY <schema.table>
   WHERE key = ANY($1::bigint[])
   ORDER BY key
   FOR UPDATE;
   ```

   Use the frozen cutoff and policy values, not a newly computed current-time cutoff. Do not use SKIP LOCKED. Lock waits and deadlocks are retryable failures, never evidence of absence. Read Committed returns the updated qualifying row after a conflicting updater commits; compare that returned version. Reject duplicate/unexpected returned keys. Ordering reduces deadlock risk but does not eliminate it when applications change keys.
6. Partition the input keys into absent (not returned), deferred (ineligible, changed, or oversized current representation), and matching. Retain locks on returned rows through commit. Changed rows receive no DELETE or UPDATE.
7. Delete only matching keys, with RETURNING of keys and bounded values. Check that the returned key set equals the matching key set, and compare returned values again as defense in depth. Any unexpected result aborts the entire transaction; it is not a new deferral. Skip the DELETE statement when the matching set is empty. Missing/deferred keys are never added to the DELETE key list, even if a new row appears later.
8. Commit, then return `{deleted, absent, deferred}`. A COMMIT/transport error has uncertain outcome; let the host replay through the pending-batch state machine. Errors before commit roll back the batch. A successful all-deferred batch still commits and advances the job.

### Why this is safe

The matching row versions are locked from comparison through deletion. Writers cannot replace their values in that interval. The table guard protects the schema and unsafe-table checks. Only matching locked keys are deleted; an absent key inserted after the read is not in that set. Identical replacement rows observed before locking can be deleted by the chosen value-equivalence contract. Different values are deferred. A source transaction remains atomic even though a whole job spans many transactions.

## 3. Export, protocol and recurring integration

Keep bounded per-statement export snapshots, the frozen cutoff, saved upper key and durable keyset cursor. Retain identity/schema checks on every batch and resume. New/backdated rows behind the cursor or above the upper key may wait until the next cycle; live mode makes no complete point-in-time membership claim. Every recurring cycle starts from the beginning of the eligible range. No CDC or global deduplication is added.

Use distinct negotiated operations rather than changing the strict v1 result shape:

- Capability: `postgres-live-values-v1`.
- `DeleteOpenLiveV1(DeleteSpec)` validates LiveValues, PostgreSQL, empty parent selection and no group-delete flag.
- `DeleteBatchLiveV1 { rows }` returns `LiveDeleteBatchResult { deleted, absent, deferred }`.
- Existing DeleteOpen/DeleteBatch and DeleteBatchResult remain unchanged for legacy PostgreSQL, MySQL, MongoDB and groups.
- Missing capability is a hard preflight failure, not a downgrade. Update signed package capability declarations and cross-version tests together.
- Add the live consistency value to the engine-neutral connector contract and its advertised feature checks; reject it in unsupported connectors. Audit every exhaustive SourceStability match and serialized plan consumer, including manifest readers, Gateway and restore. Unknown live contracts must fail closed in older binaries.

Freeze the mode at admission and again in the deletion checkpoint. A new connector may resume a live job only with the exact supported comparison contract. Completed-with-deferred recurring jobs permit the next scheduled cycle. Keep fixed-delay admission, frozen policy/source/destination configuration, explicit deletion authorization, failure holds, and one worker per state directory. Manual one-shot jobs report that a new export is needed to reconsider deferred rows.

## 4. Checkpoint migration: schema 19 to 20

Use the next available migration number at implementation time; 20 is available in the inspected checkout. Proposed file: `crates/coldctl-core/migrations/0020_live_deletion.sql`. Run through the existing initialize transaction, which already commits migration history and installation state atomically.

### Existing table additions

| Column on archive_deletions | Definition / meaning |
| --- | --- |
| rows_deferred | INTEGER NOT NULL DEFAULT 0 CHECK >= 0 |
| rows_absent_uncertain | INTEGER NOT NULL DEFAULT 0 CHECK >= 0; subset of rows_absent observed after an unresolved attempt |
| deletion_mode | TEXT NOT NULL DEFAULT 'legacy'; allowed legacy/live_values_v1 |
| attribution_baseline | TEXT NOT NULL DEFAULT 'legacy_untracked'; allowed legacy_untracked/tracked_v1 |

`rows_processed = rows_deleted + rows_absent + rows_deferred`; `0 <= rows_absent_uncertain <= rows_absent`. Use checked arithmetic and validate counts against exact batch size before storage. Enforce cross-column invariants with INSERT/UPDATE triggers in this additive migration and in Rust. Preserve `completed` as “all batches accounted for”; expose a derived `completed_with_deferred` display state without changing legacy scheduler status enums unnecessarily.

Do not backfill exact attribution or per-batch evidence for historical work. Existing rows retain their counters, mode and completion flags. Existing partially processed jobs continue legacy behavior. Fresh live checkpoints explicitly set tracked_v1; legacy checkpoints remain labelled legacy_untracked. No old manifest, archive file or job plan is modified by migration.

### New per-batch ledger

Create `archive_deletion_batches`, foreign-keyed to archive_deletions:

```text
PRIMARY KEY (job_id, sequence)
sequence               INTEGER > 0
object_sha256          TEXT NOT NULL
row_count              INTEGER > 0
state                  pending | applied
attempts               INTEGER >= 1
recovery_uncertain     BOOLEAN
deleted/absent/deferred INTEGER NULL while pending, nonnegative when applied
absent_uncertain       INTEGER NULL while pending, 0..absent when applied
created_at/updated_at  TEXT NOT NULL
```

Constrain pending counters to NULL; applied counters must sum to row_count. The parent manifest hash plus sequence and object hash binds the operation to the immutable input. Only one pending row per job (partial unique index). No raw values or keys are stored in this ledger; those remain in the verified batch object.

### State machine and crash windows

| Boundary | Durable state | Recovery |
| --- | --- | --- |
| Before intent commit | No new ledger row | No request may have been sent; start normally |
| After intent commit, before sending | pending | Conservatively uncertain; cannot prove a request was never sent |
| During source transaction / before source commit | pending | Ensure old connector/session is terminated; replay safely |
| Source commit or response lost | pending | Replay same immutable batch; previously deleted keys may now be absent |
| Response received, before local commit | pending | Same recovery rule; in-memory counts are not durable evidence |
| After local commit | applied and aggregate advanced together | Do not call connector again for this sequence |

Before each source mutation request, commit the intent ledger entry. On a fresh batch attempts=1 and recovery_uncertain=false. Reusing pending intent increments attempts and durably marks recovery_uncertain=true before dispatch. Keep uncertainty sticky across repeated failures. Do not clear pending intent on an ambiguous connector error. Never start a new attempt while an old connector can continue the same mutation; close/kill and await process-tree cleanup first, with database timeouts as a backstop.

After a successful response, one SQLite transaction verifies sequence/hash/mode, marks the ledger applied and advances aggregate counters with compare-and-swap on `batches`. A duplicate apply is idempotent only when the recorded evidence exactly matches; otherwise fail closed. A stale response must not advance twice. The last batch sets completed in the same transaction. Empty archives complete without a batch ledger entry.

For a recovered batch, absent_uncertain equals its observed absent count; fresh batches use zero. This is conservative uncertainty, not a claim that ColdCTL deleted those rows. Rows reinserted before replay may instead be deleted or deferred according to their current values. Counts describe each archived input key's final accounted outcome, not every physical deletion event across retries.

Expose “confirmed deletions”, “absent”, “absent on recovery; attribution unknown”, and “deferred”. The unknown number is a subset of absent, never added again to totals. Preserve the legacy caveat for migrated histories. Portal/cloud metrics must not present absent or uncertain rows as confirmed ColdCTL removals. Local ledger durability assumes the state directory is retained; rollback to an older state backup retains existing recovery caveats and is not exact-once accounting.

### Migration safety and compatibility

Validate existing counters and manifest bindings before upgrading; reject invalid histories transactionally rather than repairing silently. Add columns, triggers and ledger, run foreign_key_check, then record version 20 in the same initialization transaction. Update accepted migration prefixes, schema-sensitive tests and documentation. Stop workers and back up state before upgrade. Old binaries reject schema 20 through migration-version validation; no in-place downgrade. Restoring a pre-upgrade backup after live deletions is a recovery operation, not a safe rollback procedure.

Keep legacy `advance` behavior available with deferred=0; it must never apply a live ledger operation. Add live `prepare_batch` and `apply_batch` APIs. Full cross-store atomicity is explicitly not provided; no source-side receipt table or CREATE privilege is introduced.

## 5. Implementation map

| Area | Existing paths / intended changes |
| --- | --- |
| Transaction and equality | `crates/coldctl-connector-postgres/src/postgres_delete.rs`; add live session path; retain legacy path and tests |
| Export contract | `crates/coldctl-core/src/archive/planner.rs`, `archive/executor.rs`, PostgreSQL `postgres_archive.rs`; live mode and accurate warnings |
| Wire protocol and capabilities | `crates/coldctl-core/src/source/connector.rs`, `connector_contract.rs`, connector dispatch/hello and signed package metadata; separate live operations |
| Durable progress | `crates/coldctl-core/src/state/deletion.rs`, `state/migrations.rs`, new migration; intent/apply state machine |
| Host orchestration | `crates/coldctl-core/src/archive/deletion.rs`; count validation, mode negotiation, pending replay, shutdown and process cleanup |
| Scheduling | `crates/coldctl-core/src/state/recurring.rs`, `state/scheduler.rs`, `scheduler.rs`; live admission and completed-with-deferred progression; reject groups |
| CLI and reporting | `crates/coldctl/src/commands/archive/mod.rs`, `commands/scheduler.rs`, core `cloud_lifecycle.rs`; mode option and honest counters |
| Consumers and docs | cloud lifecycle DTO/schema/UI, Gateway manifest parsing, restore contract parsing, `DELETION.md`, `SCHEDULER.md`, generated CLI reference |

Cloud payload additions require checking the backend's allowlists/DTOs and compatibility, not merely adding JSON fields. No raw row data or key values enter cloud reports. Gateway continues showing archive versions; a merged latest-record view remains out of scope. Signed manifests, catalog automation and incremental object verification stay separate roadmap workstreams.

## 6. Acceptance-test plan

Keep existing legacy tests unchanged wherever their semantics remain valid; add live-mode tests rather than weakening rollback assertions. PostgreSQL 15 matches current CI; qualify the selected managed PostgreSQL deployment separately. Run connector-level concurrency tests and host-level recovery/recurring tests through the real connector executable.

### A. Transaction and value safety

| ID | Fixture / action | Required result |
| --- | --- | --- |
| A1 | One batch: matching, changed, absent, ineligible and recent unrelated rows | Exact deleted/absent/deferred counts; changed/ineligible/recent values survive; only matching keys deleted |
| A2 | All matching; all changed; all absent; empty archive | Correct completion, including all-deferred completion and no empty DeleteBatch request |
| A3 | Update then revert; delete/reinsert identical; replacement differs | First two eligible under value equivalence; differing replacement deferred |
| A4 | External updater holds a candidate row, then commits changed values | Wait, compare committed values, defer it, delete other matches |
| A5 | External updater rolls back | Original matching row is deleted |
| A6 | Writer attempts update after ColdCTL has locked and compared row | Writer cannot change that row before ColdCTL commits; no changed version is deleted |
| A7 | Missing key inserted after locking read; concurrent delete/reinsert; primary-key move | Unobserved replacement not targeted; observed differing version preserved; no double-counted keys |
| A8 | NULL/empty/JSON null, bool forms, signed zero, NaN, large exact numerics, timestamps, text/JSON differences | Comparator matches the frozen contract with no precision loss or collation shortcuts |
| A9 | Duplicate keys, malformed archive, oversized archived row, oversized current row | Invalid input fails before mutation; oversized current row deferred without truncation |
| A10 | Lock timeout, deadlock, statement timeout, transaction deadline | Whole batch rolls back; no applied ledger; pending replay remains possible |
| A11 | Concurrent unrelated writes and attempted DDL/trigger/FK creation | Unrelated DML succeeds within fixture budget; unsafe schema changes fenced or batch fails safely |
| A12 | Source/table identity or schema drift; RLS/triggers/rules/incoming FK; imported archive | Fail closed before deletion; retain all existing safety restrictions |
| A13 | SELECT+DELETE role without UPDATE; then add one column UPDATE grant | Legacy still works; live preflight fails clearly without grant and works with minimal grant; no CREATE required |
| A14 | Unexpected DELETE RETURNING key/value/count | Abort all source changes, no local checkpoint advancement |

### B. Recovery and accounting

Use deterministic failpoints at each boundary, including an after-COMMIT-before-response connector failpoint. Never infer commit from a timeout. Test hooks must be test-only and cannot bypass verification in production builds.

| ID | Injection | Required result |
| --- | --- | --- |
| B1 | Crash before intent commit | No source request; restart processes batch once |
| B2 | Crash after intent commit but before dispatch | Conservative uncertainty retained; successful replay counts each archived key once |
| B3 | Kill connector before source commit | Transaction rolls back and locks release; host replays pending batch |
| B4 | Kill after source commit, before response | Replay observes absent keys; absent_uncertain subset reported; never invent confirmed deletions |
| B5 | Host dies after response, before SQLite apply | Same uncertainty semantics; no aggregate double count |
| B6 | Kill during SQLite apply, then after successful apply | Atomic ledger/aggregate outcome; applied batch never resent |
| B7 | Repeated ambiguous attempts, plus app reinsertion between attempts | Counts remain bounded by archived input size; current differing values survive |
| B8 | Shutdown/pause at boundary, verification failure, unavailable/corrupt object | No unverified source mutation; checkpoint remains resumable |
| B9 | Manifest/object hash changes, stale sequence, invalid/overflow counts, duplicate apply | Reject invalid transitions; exact duplicate apply idempotent |
| B10 | Zero-row archive and one-/multi-batch limits | Completion and sequence invariants hold across restart |

For every successful apply assert: processed=deleted+absent+deferred, uncertain<=absent, aggregate increments equal applied ledger results, and completed implies all expected objects and archived input rows accounted for. For legacy migrated prefixes, compare against preserved baseline counters rather than inventing missing historical ledger entries.

### C. Migration and compatibility

- Upgrade every supported schema prefix 0..19; include empty state and active/completed/partial legacy deletion histories. Upgrade twice without changing data.
- Test constraints and foreign keys; interrupt initialization and verify all-or-nothing migration. Invalid historical counts cause a clean failure.
- Verify existing archive/restore/recurring/group behavior and legacy no-UPDATE deletion regression tests.
- New host + old connector rejects live mode before source mutation; old host + new connector still uses v1 behavior. MySQL/MongoDB/groups reject live admission.
- Old binary rejects schema 20. Signed connector manifests advertise the capability consistently with hello.
- New live plan round-trips through checkpoint, manifest, engine-neutral contract, Gateway reader and restore; older consumers fail clearly instead of misinterpreting it.
- CLI and Cloud distinguish completed-with-deferred and uncertain absence; no unknown-attribution rows inflate confirmed-deletion metrics.

### D. Recurring and export

- First cycle defers a changed row; second exports its new eligible value and deletes it. First archive remains immutable and identifiable.
- Permanently changing rows do not block other matches or future completed cycles; a lock-timeout failure still retries the same cycle.
- Backdated lower-key arrivals, keys above the frozen upper bound, eligibility transitions and key changes are reconsidered on later full scans.
- Restart export between batches with changed source rows. Previously published batches are not rewritten; verify there is no point-in-time claim.
- Stop/retry/source-protection behavior and fixed-delay scheduling remain intact. Failed/incomplete jobs never admit a new cycle; deferred-complete jobs do.

### Execution and release gates

Use barriers, dedicated connections and observed database lock state for concurrency synchronization, not arbitrary sleeps. Report connector failures immediately; allow separate startup time from lock-test deadlines. Failpoint tests must assert final database contents and ledger state, not only returned errors.

Run formatting, focused core/connector tests, schema-prefix tests, existing ignored PostgreSQL suite, then workspace tests and Clippy as required by CI. The existing CI PostgreSQL ignored-test job will run the added integration tests; add focused fault-injection coverage where process-kill harnesses need separate invocation. Do not claim the plan's tests have run before implementation.

Release requires all safety/migration/recovery gates passing, explicit preflight for the chosen privilege model, and at least two design-partner deployments meeting the roadmap launch gate. Each partner agrees numerical p95 latency, lock-wait, CPU/I/O, replica-lag and transaction-duration limits before testing. Record baseline and archival-load measurements; a correctness pass does not substitute for source-impact qualification.

## 7. Delivery slices

1. Add live contract/capability and comparison fixtures, including rejected modes and privilege preflight.
2. Implement bounded connector transaction with A-series tests; leave live scheduling disabled until durable host support lands.
3. Land schema migration, ledger APIs and B/C-series tests, then integrate live host orchestration.
4. Enable CLI/recurring live admission, reporting and D-series tests; validate consumer compatibility.
5. Qualify crash behavior and source impact on managed PostgreSQL; publish operator grants, semantics and recovery instructions.

The value-equivalence decision is closed. No additional product decision is needed to begin implementation; the documented UPDATE-column privilege is a deployment constraint that must be validated with design partners.
