//! Dedicated-target restore. Data and progress commit together; retries read the journal.
use crate::mongo::{Mongo, bad, collection_name, err, object_id};
use coldctl_core::{
    error::Error,
    format::bson as codec,
    source::{
        config::ResolvedConnection,
        connector::{RestoreProgress, RestoreSpec},
    },
};
use mongodb::{
    ClientSession, Collection,
    bson::{Document, RawDocumentBuf, doc},
    options::{ReadConcern, WriteConcern},
};
use sha2::{Digest, Sha256};

pub struct RestoreSession {
    source: Mongo,
    spec: RestoreSpec,
    session: ClientSession,
    journal: Collection<Document>,
    target: Collection<RawDocumentBuf>,
    identity: coldctl_core::archive::planner::SourceIdentity,
    saved: Option<RestoreProgress>,
    validated: i64,
    previous: Option<coldctl_core::archive::key::ArchiveKey>,
}
fn marker(spec: &RestoreSpec) -> Document {
    doc! {"$jsonSchema": {"bsonType": "object", "description": format!("coldctl-mongodb-v1:{}", spec.manifest_hash)}}
}
impl RestoreSession {
    pub async fn abort(&mut self) {
        // Await cleanup before the supervisor reports failure and kills this process.
        // Dropping ClientSession only schedules asynchronous abort work.
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.session.abort_transaction(),
        )
        .await;
    }
    pub async fn open(connection: ResolvedConnection, spec: RestoreSpec) -> Result<Self, Error> {
        collection_name(&spec.table)?;
        if spec.schema != connection.database
            || spec.plan.policy.config.schema == connection.database
            || spec
                .plan
                .safety
                .as_ref()
                .is_none_or(|s| s.source_identity.database_name == connection.database)
            || spec.plan.columns != codec::columns()
            || spec.plan.primary_key != "_id"
            || spec.plan.delete
            || spec
                .plan
                .connector_pin
                .as_ref()
                .is_none_or(|pin| pin.id != "mongodb")
            || spec.objects_created < 0
            || spec.rows_processed < 0
            || spec.manifest_hash.len() != 64
            || !spec.manifest_hash.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(bad());
        }
        let source = Mongo::connect(connection).await?;
        let journal_name = format!(
            "_coldctl_restore_{:x}",
            Sha256::digest(spec.table.as_bytes())
        );
        let journal = source.db.collection::<Document>(&journal_name);
        let target = source.db.collection::<RawDocumentBuf>(&spec.table);
        if !spec.validate_only {
            for (name, validator) in [
                (&spec.table, marker(&spec)),
                (&journal_name, doc! {"$jsonSchema": {"bsonType": "object"}}),
            ] {
                if let Err(error) = source
                    .db
                    .create_collection(name)
                    .validator(validator)
                    .write_concern(WriteConcern::majority())
                    .await
                {
                    if !matches!(error.kind.as_ref(), mongodb::error::ErrorKind::Command(e) if e.code == 48)
                    {
                        return Err(err(error));
                    }
                }
            }
        }
        let info = source.info(&spec.table).await?;
        if info
            .get_document("options")
            .map_err(|_| bad())?
            .get_document("validator")
            .map_err(|_| bad())?
            != &marker(&spec)
        {
            return Err(bad());
        }
        let identity = source.collection_identity(&spec.table).await?;
        let mut session = source.client.start_session().await.map_err(err)?;
        session
            .start_transaction()
            .read_concern(ReadConcern::snapshot())
            .write_concern(WriteConcern::majority())
            .await
            .map_err(err)?;
        let existing = journal
            .find_one(doc! {"_id": "progress"})
            .session(&mut session)
            .await
            .map_err(err)?;
        if existing.is_none() {
            if spec.validate_only
                || target
                    .count_documents(doc! {})
                    .session(&mut session)
                    .await
                    .map_err(err)?
                    != 0
            {
                return Err(bad());
            }
            journal.insert_one(doc! {"_id": "progress", "manifest": &spec.manifest_hash, "identity": &identity.schema_signature, "batches": 0i64, "rows": 0i64, "completed": false}).session(&mut session).await.map_err(err)?;
        }
        // An unknown outcome is surfaced; a new invocation reads the durable journal.
        session.commit_transaction().await.map_err(err)?;
        Ok(Self {
            source,
            spec,
            session,
            journal,
            target,
            identity,
            saved: None,
            validated: 0,
            previous: None,
        })
    }
    pub async fn begin(&mut self, verify_count: bool) -> Result<RestoreProgress, Error> {
        if self.saved.is_some() {
            return Err(bad());
        }
        if self.source.collection_identity(&self.spec.table).await? != self.identity {
            return Err(bad());
        }
        self.session
            .start_transaction()
            .read_concern(ReadConcern::snapshot())
            .write_concern(WriteConcern::majority())
            .await
            .map_err(err)?;
        let saved = self
            .journal
            .find_one(doc! {"_id": "progress"})
            .session(&mut self.session)
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        if saved.get_str("manifest") != Ok(self.spec.manifest_hash.as_str())
            || saved.get_str("identity") != Ok(self.identity.schema_signature.as_str())
        {
            return Err(bad());
        }
        let progress = RestoreProgress {
            batches: saved.get_i64("batches").map_err(|_| bad())?,
            rows: saved.get_i64("rows").map_err(|_| bad())?,
            completed: saved.get_bool("completed").map_err(|_| bad())?,
        };
        if progress.batches < 0
            || progress.rows < 0
            || progress.batches > self.spec.objects_created
            || progress.rows > self.spec.rows_processed
        {
            return Err(bad());
        }
        if verify_count
            && self
                .target
                .count_documents(doc! {})
                .session(&mut self.session)
                .await
                .map_err(err)?
                != progress.rows as u64
        {
            return Err(bad());
        }
        self.saved = Some(RestoreProgress {
            batches: progress.batches,
            rows: progress.rows,
            completed: progress.completed,
        });
        self.validated = 0;
        self.previous = None;
        Ok(progress)
    }
    pub async fn validate(&mut self, rows: &[Vec<Option<String>>]) -> Result<(), Error> {
        let maximum = self.saved.as_ref().ok_or_else(bad)?.rows;
        if rows.is_empty() || rows.len() > 100 {
            return Err(bad());
        }
        for row in rows {
            self.validated += 1;
            if self.validated > maximum {
                return Err(bad());
            }
            let key = codec::key(row.first().and_then(|v| v.as_deref()).ok_or_else(bad)?)?;
            if self.previous.is_some_and(|previous| !key.follows(previous)) {
                return Err(bad());
            }
            self.compare(row).await?;
            self.previous = Some(key);
        }
        // Prefix validation can span millions of documents. Keep each snapshot short;
        // the dedicated target must have no external writers throughout validation.
        self.session.commit_transaction().await.map_err(err)?;
        self.session
            .start_transaction()
            .read_concern(ReadConcern::snapshot())
            .write_concern(WriteConcern::majority())
            .await
            .map_err(err)?;
        let checkpoint = self
            .journal
            .find_one(doc! {"_id": "progress"})
            .session(&mut self.session)
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        let saved = self.saved.as_ref().ok_or_else(bad)?;
        if checkpoint.get_i64("batches") != Ok(saved.batches)
            || checkpoint.get_i64("rows") != Ok(saved.rows)
            || checkpoint.get_str("manifest") != Ok(self.spec.manifest_hash.as_str())
            || checkpoint.get_str("identity") != Ok(self.identity.schema_signature.as_str())
        {
            return Err(bad());
        }
        Ok(())
    }
    async fn compare(&mut self, row: &[Option<String>]) -> Result<(), Error> {
        let bytes = codec::decode(row, &self.spec.plan.policy.config.time_column)?;
        let key = codec::key(row[0].as_deref().ok_or_else(bad)?)?;
        // A size predicate bounds target reads even after external tampering.
        let filter = doc! {"_id": object_id(key)?, "$expr": {"$lte": [{"$bsonSize": "$$ROOT"}, codec::MAX_DOCUMENT_BYTES as i64]}};
        let actual = self
            .target
            .find_one(filter)
            .session(&mut self.session)
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        if actual.as_bytes() != bytes {
            return Err(bad());
        }
        Ok(())
    }
    pub async fn apply(
        &mut self,
        sequence: i64,
        rows: &[Vec<Option<String>>],
    ) -> Result<(), Error> {
        let saved = self.saved.as_ref().ok_or_else(bad)?;
        if self.spec.validate_only
            || saved.completed
            || sequence != saved.batches + 1
            || sequence > self.spec.objects_created
            || rows.is_empty()
            || rows.len() > 100
        {
            return Err(bad());
        }
        let new_rows = saved.rows.checked_add(rows.len() as i64).ok_or_else(bad)?;
        if new_rows > self.spec.rows_processed {
            return Err(bad());
        }
        let previous_batches = saved.batches;
        let mut documents = Vec::new();
        let mut previous = None;
        for row in rows {
            codec::validate_eligibility(
                row,
                &self.spec.plan.policy.config.time_column,
                &self.spec.plan.cutoff_utc,
            )?;
            let key = codec::key(row[0].as_deref().ok_or_else(bad)?)?;
            if previous.is_some_and(|p| !key.follows(p)) {
                return Err(bad());
            }
            previous = Some(key);
            documents.push(
                RawDocumentBuf::from_bytes(codec::decode(
                    row,
                    &self.spec.plan.policy.config.time_column,
                )?)
                .map_err(|_| bad())?,
            );
        }
        self.target
            .insert_many(documents)
            .session(&mut self.session)
            .await
            .map_err(err)?;
        // MongoDB can reorder _id: reject any normalization rather than claim byte-exact restore.
        for row in rows {
            self.compare(row).await?;
        }
        let result = self.journal.update_one(doc! {"_id": "progress", "batches": previous_batches, "manifest": &self.spec.manifest_hash}, doc! {"$set": {"batches": sequence, "rows": new_rows}}).session(&mut self.session).await.map_err(err)?;
        if result.matched_count != 1 {
            return Err(bad());
        }
        self.session.commit_transaction().await.map_err(err)?;
        self.saved = None;
        Ok(())
    }
    pub async fn finish(&mut self, completed: bool) -> Result<(), Error> {
        let saved = self.saved.as_ref().ok_or_else(bad)?;
        if completed
            && (saved.rows != self.spec.rows_processed
                || saved.batches != self.spec.objects_created
                || self.validated != saved.rows)
        {
            return Err(bad());
        }
        if completed && !self.spec.validate_only {
            self.journal
                .update_one(
                    doc! {"_id": "progress", "batches": saved.batches},
                    doc! {"$set": {"completed": true}},
                )
                .session(&mut self.session)
                .await
                .map_err(err)?;
        }
        self.session.commit_transaction().await.map_err(err)?;
        self.saved = None;
        Ok(())
    }
}
