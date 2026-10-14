//! Exact BSON comparison and deletion in one majority-committed transaction.
use crate::mongo::{Mongo, bad, err, object_id};
use coldctl_core::{
    error::Error,
    format::bson as codec,
    source::{
        config::ResolvedConnection,
        connector::{DeleteBatchResult, DeleteSpec},
    },
};
use mongodb::{
    ClientSession,
    bson::{Bson, Document, RawDocumentBuf, doc},
    options::{ReadConcern, WriteConcern},
};

pub struct DeleteSession {
    source: Mongo,
    spec: DeleteSpec,
    session: ClientSession,
}
impl DeleteSession {
    pub async fn open(connection: ResolvedConnection, spec: DeleteSpec) -> Result<Self, Error> {
        if spec.plan.safety.is_none()
            || spec
                .plan
                .connector_pin
                .as_ref()
                .is_none_or(|p| p.id != "mongodb")
        {
            return Err(bad());
        }
        let source = Mongo::connect(connection).await?;
        source.check_plan(&spec.plan)?;
        source.check_identity(&spec.plan).await?;
        if source.identity() != spec.source_identity {
            return Err(Error::Archive(
                "MongoDB source identity changed; refusing deletion",
            ));
        }
        let session = source.client.start_session().await.map_err(err)?;
        Ok(Self {
            source,
            spec,
            session,
        })
    }
    pub async fn abort(&mut self) {
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.session.abort_transaction(),
        )
        .await;
    }
    pub async fn batch(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        let result = self.batch_inner(rows).await;
        if result.is_err() {
            self.abort().await;
        }
        result
    }
    async fn batch_inner(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        let plan = &self.spec.plan;
        if rows.is_empty()
            || rows.len() > 100
            || rows.len() > plan.policy.config.batch_size as usize
        {
            return Err(bad());
        }
        let mut documents = Vec::new();
        let mut previous = None;
        for row in rows {
            codec::validate_eligibility(row, &plan.policy.config.time_column, &plan.cutoff_utc)?;
            let key = codec::key(row.first().and_then(|v| v.as_deref()).ok_or_else(bad)?)?;
            if previous.is_some_and(|p| !key.follows(p)) {
                return Err(bad());
            }
            previous = Some(key);
            documents.push((
                object_id(key)?,
                codec::decode(row, &plan.policy.config.time_column)?,
            ));
        }
        self.session
            .start_transaction()
            .read_concern(ReadConcern::snapshot())
            .write_concern(WriteConcern::majority())
            .await
            .map_err(err)?;
        let metadata = self
            .source
            .db
            .collection::<Document>(&plan.policy.config.table);
        let target = self
            .source
            .db
            .collection::<RawDocumentBuf>(&plan.policy.config.table);
        let mut result = DeleteBatchResult {
            deleted: 0,
            absent: 0,
        };
        for (index, (id, expected)) in documents.into_iter().enumerate() {
            // Bound fetches without mistaking oversized changed documents for absent ones.
            let size = metadata
                .find_one(doc! {"_id":id})
                .projection(doc! {"_id":1,"_coldctl_size":{"$bsonSize":"$$ROOT"}})
                .max_time(self.source.timeout)
                .session(&mut self.session)
                .await
                .map_err(err)?;
            if index == 0 {
                // The first read binds the transaction to the collection and
                // retains its lock. Check UUID/options AFTER binding, preventing
                // drop/recreate between open and transaction from passing.
                self.source.check_identity(plan).await?;
            }
            let Some(size) = size else {
                result.absent += 1;
                continue;
            };
            let bytes = match size.get("_coldctl_size") {
                Some(Bson::Int32(n)) => i64::from(*n),
                Some(Bson::Int64(n)) => *n,
                _ => return Err(bad()),
            };
            if bytes < 0 || bytes as usize > codec::MAX_DOCUMENT_BYTES {
                return Err(Error::Archive(
                    "MongoDB source document changed or exceeds archive byte limit",
                ));
            }
            let actual = target
                .find_one(doc! {"_id":id})
                .max_time(self.source.timeout)
                .session(&mut self.session)
                .await
                .map_err(err)?
                .ok_or_else(bad)?;
            if actual.as_bytes() != expected {
                return Err(Error::Archive(
                    "MongoDB document differs from archive; deletion batch rolled back",
                ));
            }
            // A change after the snapshot read causes a write conflict, aborting
            // this transaction rather than removing the modified document.
            let deleted = target
                .delete_one(doc! {"_id":id})
                .session(&mut self.session)
                .await
                .map_err(err)?;
            if deleted.deleted_count != 1 {
                return Err(bad());
            }
            result.deleted += 1;
        }
        self.session.commit_transaction().await.map_err(err)?;
        Ok(result)
    }
}
