use coldctl_core::{
    archive::{
        batch::DataBatch,
        controls::ExecutionControls,
        key::ArchiveKey,
        planner::{ArchivePlan, SourceIdentity},
    },
    error::Error,
    format::bson as codec,
    policy::model::Policy,
    source::{
        Column, ConnectionInfo, DataSource, Discovery, Index, Table,
        config::{ResolvedConnection, TlsMode},
    },
};
use futures_util::TryStreamExt;
use mongodb::{
    Client, Database,
    bson::{self, Bson, DateTime, Document, RawDocumentBuf, doc, oid::ObjectId},
    options::{ClientOptions, Credential, ServerAddress, Tls, TlsOptions},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, io::Write, path::PathBuf, time::Duration};

pub fn bad() -> Error {
    Error::Archive(
        "MongoDB schema, topology, identity or archive contract is unsupported or changed",
    )
}
pub fn err(error: mongodb::error::Error) -> Error {
    use mongodb::error::ErrorKind;
    match error.kind.as_ref() {
        ErrorKind::Authentication { .. } => Error::Archive("MongoDB authentication failed"),
        ErrorKind::Command(e) if e.code == 13 => Error::Archive("MongoDB permission denied"),
        ErrorKind::Command(e) if e.code == 50 => Error::Archive("MongoDB operation timeout"),
        ErrorKind::Io(_)
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::InvalidTlsConfig { .. } => Error::Archive("MongoDB connection or TLS failed"),
        _ => {
            Error::Archive("MongoDB operation failed; database diagnostics and values are redacted")
        }
    }
}
pub fn object_id(key: ArchiveKey) -> Result<ObjectId, Error> {
    match key {
        ArchiveKey::ObjectId(k) => Ok(ObjectId::from_bytes(k.object_id)),
        _ => Err(bad()),
    }
}
fn primary_replica(hello: &Document) -> Result<(), Error> {
    if hello.get_bool("isWritablePrimary") != Ok(true)
        || hello.get_str("setName").is_err()
        || hello.get_str("msg") == Ok("isdbgrid")
    {
        return Err(bad());
    }
    Ok(())
}
pub fn collection_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 120
        || name.starts_with("system.")
        || name.starts_with("_coldctl_")
        || name.contains(['$', '\0'])
    {
        return Err(bad());
    }
    Ok(())
}
pub struct Mongo {
    pub client: Client,
    pub db: Database,
    pub deployment: String,
    endpoint: String,
    pub timeout: Duration,
    plan: Option<ArchivePlan>,
    _ca: Option<tempfile::NamedTempFile>,
}
impl Mongo {
    pub async fn connect(connection: ResolvedConnection) -> Result<Self, Error> {
        let mut options = ClientOptions::default();
        options.hosts = vec![ServerAddress::Tcp {
            host: connection.host.clone(),
            port: Some(connection.port),
        }];
        options.direct_connection = Some(true);
        options.app_name = Some("coldctl-mongodb".into());
        options.connect_timeout = Some(Duration::from_secs(10));
        options.server_selection_timeout = Some(Duration::from_secs(10));
        options.max_pool_size = Some(2);
        options.retry_reads = Some(false);
        options.retry_writes = Some(false);
        options.credential = Some(
            Credential::builder()
                .username(connection.user)
                .password(connection.password)
                .source(connection.database.clone())
                .build(),
        );
        let mut ca = None;
        options.tls = Some(if connection.tls == TlsMode::Require {
            let mut tls = TlsOptions::default();
            if let Some(pem) = connection.ca_pem {
                if pem.len() > 65536 {
                    return Err(bad());
                }
                let mut file = tempfile::NamedTempFile::new().map_err(|_| bad())?;
                file.write_all(pem.as_bytes()).map_err(|_| bad())?;
                tls.ca_file_path = Some(file.path().to_owned());
                ca = Some(file);
            }
            Tls::Enabled(tls)
        } else {
            if connection.ca_pem.is_some() {
                return Err(bad());
            }
            Tls::Disabled
        });
        let client = Client::with_options(options).map_err(err)?;
        let admin = client.database("admin");
        let hello = admin.run_command(doc! {"hello": 1}).await.map_err(err)?;
        primary_replica(&hello)?;
        let build = admin
            .run_command(doc! {"buildInfo": 1})
            .await
            .map_err(err)?;
        if !build
            .get_str("version")
            .map_err(|_| bad())?
            .starts_with("8.0.")
        {
            return Err(bad());
        }
        let shard = admin
            .run_command(doc! {"shardingState": 1})
            .await
            .map_err(err)?;
        if shard.get_bool("enabled") != Ok(false) {
            return Err(bad());
        }
        let config = admin
            .run_command(doc! {"replSetGetConfig": 1})
            .await
            .map_err(err)?;
        let config = config.get_document("config").map_err(|_| bad())?;
        if config.get_bool("configsvr").unwrap_or(false) {
            return Err(bad());
        }
        let deployment = config
            .get_document("settings")
            .map_err(|_| bad())?
            .get_object_id("replicaSetId")
            .map_err(|_| bad())?
            .to_hex();
        let db = client.database(&connection.database);
        Ok(Self {
            client,
            db,
            deployment,
            endpoint: format!(
                "mongodb:{}:{}:{}",
                connection.host, connection.port, connection.database
            ),
            timeout: Duration::from_secs(30),
            plan: None,
            _ca: ca,
        })
    }
    pub fn identity(&self) -> &str {
        &self.endpoint
    }
    pub async fn configure_timeouts(&mut self, controls: &ExecutionControls) -> Result<(), Error> {
        controls.validate()?;
        self.timeout = Duration::from_millis(controls.query_timeout_ms.into());
        Ok(())
    }
    pub async fn info(&self, name: &str) -> Result<Document, Error> {
        collection_name(name)?;
        let response = self.db.run_command(doc! {"listCollections": 1, "filter": {"name": name}, "cursor": {"batchSize": 2}, "maxTimeMS": self.timeout.as_millis() as i64}).await.map_err(err)?;
        let batch = response
            .get_document("cursor")
            .map_err(|_| bad())?
            .get_array("firstBatch")
            .map_err(|_| bad())?;
        if batch.len() != 1 {
            return Err(bad());
        }
        let info = batch[0].as_document().ok_or_else(bad)?.clone();
        if info.get_str("type") != Ok("collection") {
            return Err(bad());
        }
        let options = info.get_document("options").map_err(|_| bad())?;
        if options.get_bool("capped").unwrap_or(false)
            || options.contains_key("timeseries")
            || options.contains_key("clusteredIndex")
            || options.contains_key("encryptedFields")
        {
            return Err(bad());
        }
        Ok(info)
    }
    pub async fn collection_identity(&self, name: &str) -> Result<SourceIdentity, Error> {
        let info = self.info(name).await?;
        let uuid = info
            .get_document("info")
            .map_err(|_| bad())?
            .get("uuid")
            .ok_or_else(bad)?;
        let bytes = bson::to_vec(&doc! {"deployment": &self.deployment, "database": self.db.name(), "collection": name, "uuid": uuid, "options": info.get_document("options").map_err(|_| bad())?}).map_err(|_| bad())?;
        Ok(SourceIdentity {
            database_oid: 0,
            database_name: self.db.name().into(),
            schema_signature: format!("{:x}", Sha256::digest(bytes)),
        })
    }
    pub async fn source_identity(&self, _: u32) -> Result<SourceIdentity, Error> {
        self.collection_identity(&self.plan.as_ref().ok_or_else(bad)?.policy.config.table)
            .await
    }
    pub async fn validate_types(&self, plan: &ArchivePlan) -> Result<(), Error> {
        self.check_plan(plan)?;
        let field = format!("${}", plan.policy.config.time_column);
        // $expr/$type checks exact scalar types: query $type alone also matches array elements.
        let invalid = doc! {"$expr": {"$or": [
            {"$ne": [{"$type": "$_id"}, "objectId"]},
            {"$ne": [{"$type": field}, "date"]}
        ]}};
        if self
            .db
            .collection::<Document>(&plan.policy.config.table)
            .find_one(invalid)
            .projection(doc! {"_id": 0, "invalid": {"$literal": true}})
            .max_time(self.timeout)
            .await
            .map_err(err)?
            .is_some()
        {
            return Err(bad());
        }
        let mut indexes = self
            .db
            .collection::<Document>(&plan.policy.config.table)
            .list_indexes()
            .await
            .map_err(err)?;
        while let Some(index) = indexes.try_next().await.map_err(err)? {
            if index
                .options
                .is_some_and(|options| options.expire_after.is_some())
            {
                return Err(bad());
            }
        }
        let mut sizes = self
            .db
            .collection::<Document>(&plan.policy.config.table)
            .aggregate(vec![
                doc! {"$project": {"_id": 0, "n": {"$bsonSize": "$$ROOT"}}},
                doc! {"$match": {"n": {"$gt": codec::MAX_DOCUMENT_BYTES as i64}}},
                doc! {"$limit": 1},
            ])
            .max_time(self.timeout)
            .await
            .map_err(err)?;
        if sizes.try_next().await.map_err(err)?.is_some() {
            return Err(Error::Archive(
                "BSON document exceeds the 30 KiB archive byte limit",
            ));
        }
        Ok(())
    }
    fn check_plan(&self, plan: &ArchivePlan) -> Result<(), Error> {
        plan.policy.config.validate()?;
        collection_name(&plan.policy.config.table)?;
        let field = &plan.policy.config.time_column;
        if plan.delete
            || plan.policy.config.schema != self.db.name()
            || field == "_id"
            || field.contains(['.', '$', '\0'])
            || plan.policy.config.equals_column.is_some()
            || plan.columns != codec::columns()
            || plan.primary_key != "_id"
        {
            return Err(bad());
        }
        DateTime::parse_rfc3339_str(&plan.cutoff_utc).map_err(|_| bad())?;
        Ok(())
    }
    pub async fn plan(
        &mut self,
        policy: Policy,
        destination_path: PathBuf,
    ) -> Result<ArchivePlan, Error> {
        let cutoff = DateTime::from_millis(
            DateTime::now()
                .timestamp_millis()
                .checked_sub(i64::from(policy.config.older_than_days) * 86400000)
                .ok_or_else(bad)?,
        )
        .try_to_rfc3339_string()
        .map_err(|_| bad())?;
        let plan = ArchivePlan {connector_pin: None, policy, destination_path, cutoff_utc: cutoff, primary_key: "_id".into(), table_oid: 0, columns: codec::columns(), estimated_rows: None, delete: false, safety: None, warnings: vec![
            "MongoDB 8.0 primary replica set; ordinary nonsharded collections, ObjectId keys and top-level Date retention only. Equality filters are unsupported.".into(),
            "Preflight and resume scan the entire collection to validate key/Date types and BSON sizes. Each operation has a source query deadline; budget this load before large runs.".into(),
            "Documents are preserved as raw BSON; 30 KiB document limit. Discovery samples one document and cannot establish collection-wide types.".into(),
            "Freeze source contents and eligibility, including backdated inserts, for the entire archive and any resume. ObjectId order is not retention time.".into(),
        ]};
        self.collection_identity(&plan.policy.config.table).await?;
        self.validate_types(&plan).await?;
        self.plan = Some(plan.clone());
        Ok(plan)
    }
    pub async fn validate_resume(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        self.validate_types(plan).await?;
        self.check_identity(plan).await?;
        self.plan = Some(plan.clone());
        Ok(())
    }
    async fn check_identity(&self, plan: &ArchivePlan) -> Result<(), Error> {
        if plan.safety.as_ref().is_some_and(|s| {
            !matches!(
                s.source_stability,
                coldctl_core::archive::planner::SourceStability::QuiescentCopy
            )
        }) {
            return Err(Error::Archive(
                "MongoDB requires source-stability quiescent-copy throughout archive and resume",
            ));
        }
        let identity = self.collection_identity(&plan.policy.config.table).await?;
        if plan
            .safety
            .as_ref()
            .is_some_and(|s| s.source_identity != identity)
        {
            return Err(bad());
        }
        Ok(())
    }
    fn filter(&self, plan: &ArchivePlan) -> Result<Document, Error> {
        self.check_plan(plan)?;
        let cutoff = DateTime::parse_rfc3339_str(&plan.cutoff_utc).map_err(|_| bad())?;
        Ok(doc! {&plan.policy.config.time_column: {"$lt": cutoff}})
    }
    pub async fn upper_key(&mut self, plan: &ArchivePlan) -> Result<Option<ArchiveKey>, Error> {
        self.check_identity(plan).await?;
        let row = self
            .db
            .collection::<Document>(&plan.policy.config.table)
            .find_one(self.filter(plan)?)
            .sort(doc! {"_id": -1})
            .projection(doc! {"_id": 1})
            .max_time(self.timeout)
            .await
            .map_err(err)?;
        row.map(|r| {
            r.get_object_id("_id")
                .map(|id| ArchiveKey::object_id(id.bytes()))
                .map_err(|_| bad())
        })
        .transpose()
    }
    pub async fn read_batch(
        &mut self,
        plan: &ArchivePlan,
        last: Option<ArchiveKey>,
        upper: ArchiveKey,
    ) -> Result<Option<DataBatch>, Error> {
        self.check_identity(plan).await?;
        let mut filter = self.filter(plan)?;
        let mut range = doc! {"$lte": object_id(upper)?};
        if let Some(last) = last {
            range.insert("$gt", object_id(last)?);
        }
        filter.insert("_id", range);
        let collection = self.db.collection::<Document>(&plan.policy.config.table);
        // Bound keys and sizes before fetching any source payload; at most 100 documents/batch.
        let mut keys = collection
            .aggregate(vec![
                doc! {"$match": filter},
                doc! {"$sort": {"_id": 1}},
                doc! {"$limit": i64::from(plan.policy.config.batch_size.min(100))},
                doc! {"$project": {"_id": 1, "n": {"$bsonSize": "$$ROOT"}}},
            ])
            .max_time(self.timeout)
            .await
            .map_err(err)?;
        let mut ids = Vec::new();
        while let Some(row) = keys.try_next().await.map_err(err)? {
            if row.get_i32("n").map_err(|_| bad())? as usize > codec::MAX_DOCUMENT_BYTES {
                return Err(Error::Archive(
                    "BSON document exceeds the 30 KiB archive byte limit",
                ));
            }
            ids.push(row.get_object_id("_id").map_err(|_| bad())?);
        }
        if ids.is_empty() {
            return Ok(None);
        }
        // A second size condition protects the wire if an operator violates the frozen-source contract.
        let filter = doc! {"_id": {"$in": &ids}, "$expr": {"$lte": [{"$bsonSize": "$$ROOT"}, codec::MAX_DOCUMENT_BYTES as i64]}};
        let mut cursor = self
            .db
            .collection::<RawDocumentBuf>(&plan.policy.config.table)
            .find(filter)
            .sort(doc! {"_id": 1})
            .batch_size(100)
            .max_time(self.timeout)
            .await
            .map_err(err)?;
        let mut rows = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(err)? {
            let row = codec::encode(document.as_bytes(), &plan.policy.config.time_column)?;
            codec::validate_eligibility(&row, &plan.policy.config.time_column, &plan.cutoff_utc)?;
            if codec::key(row[0].as_deref().ok_or_else(bad)?)?
                != ArchiveKey::object_id(ids.get(rows.len()).ok_or_else(bad)?.bytes())
            {
                return Err(bad());
            }
            rows.push(row);
        }
        if rows.len() != ids.len() {
            return Err(bad());
        }
        self.check_identity(plan).await?;
        Ok(Some(DataBatch {
            rows,
            last_key: ArchiveKey::object_id(ids.last().ok_or_else(bad)?.bytes()),
        }))
    }
    async fn discover(&self) -> Result<Discovery, Error> {
        let mut collections = self.db.list_collections().await.map_err(err)?;
        let mut tables = Vec::new();
        while let Some(spec) = collections.try_next().await.map_err(err)? {
            if spec.name.starts_with("system.") || spec.name.starts_with("_coldctl_") {
                continue;
            }
            if tables.len() >= 1000 {
                return Err(bad());
            }
            let info = self.info(&spec.name).await?;
            let _ = info;
            let collection = self.db.collection::<Document>(&spec.name);
            let mut sampled = collection.aggregate(vec![doc! {"$limit": 1}, doc! {"$project": {"_id": 0, "fields": {"$map": {"input": {"$objectToArray": "$$ROOT"}, "as": "f", "in": {"name": "$$f.k", "type": {"$type": "$$f.v"}}}}}}]).max_time(self.timeout).await.map_err(err)?;
            let mut columns = Vec::new();
            if let Some(row) = sampled.try_next().await.map_err(err)? {
                for field in row.get_array("fields").map_err(|_| bad())? {
                    let field = field.as_document().ok_or_else(bad)?;
                    let kind = field.get_str("type").map_err(|_| bad())?;
                    columns.push(Column {
                        name: field.get_str("name").map_err(|_| bad())?.into(),
                        data_type: format!("bson:{kind} (sampled)"),
                        nullable: true,
                        archive_time_candidate: kind == "date",
                    });
                }
            }
            let mut indexes = Vec::new();
            let mut cursor = collection.list_indexes().await.map_err(err)?;
            while let Some(index) = cursor.try_next().await.map_err(err)? {
                let opts = index.options.unwrap_or_default();
                indexes.push(Index {
                    name: opts.name.unwrap_or_default(),
                    method: "btree".into(),
                    unique: opts.unique.unwrap_or(false),
                    primary: index.keys == doc! {"_id": 1},
                    valid: !opts.hidden.unwrap_or(false),
                    columns: index.keys.keys().cloned().collect(),
                    included_columns: vec![],
                    has_expressions: index
                        .keys
                        .values()
                        .any(|v| !matches!(v, Bson::Int32(1 | -1))),
                    partial: opts.partial_filter_expression.is_some(),
                });
            }
            tables.push(Table {
                schema: self.db.name().into(),
                name: spec.name,
                partitioned: false,
                estimated_rows: None,
                columns,
                primary_key: vec!["_id".into()],
                indexes,
            });
        }
        Ok(Discovery {
            schemas: vec![self.db.name().into()],
            tables,
        })
    }
}
pub struct MongoSource(ResolvedConnection);
impl MongoSource {
    pub fn new(connection: ResolvedConnection) -> Self {
        Self(connection)
    }
}
impl DataSource for MongoSource {
    async fn test_connection(&self) -> Result<ConnectionInfo, Error> {
        let source = Mongo::connect(self.0.clone()).await?;
        source.db.run_command(doc! {"ping": 1}).await.map_err(err)?;
        Ok(ConnectionInfo {
            database: self.0.database.clone(),
            user: self.0.user.clone(),
            server_version: "MongoDB 8.0 replica set".into(),
        })
    }
    async fn discover(&self) -> Result<Discovery, Error> {
        Mongo::connect(self.0.clone()).await?.discover().await
    }
    async fn analyze(&self) -> Result<coldctl_core::analysis::Analysis, Error> {
        let mut report = coldctl_core::analysis::assess(self.discover().await?, BTreeMap::new());
        report.method = "MongoDB metadata and one document's field types per collection; sampled Date candidates are not a schema guarantee. Archive preflight performs full type/size validation.".into();
        report.timestamp_ranges = "Not queried".into();
        Ok(report)
    }
}

#[cfg(test)]
mod topology_tests {
    use super::*;
    #[test]
    fn rejects_standalone_secondary_and_router() {
        assert!(primary_replica(&doc! {"isWritablePrimary": true, "setName": "rs"}).is_ok());
        for hello in [
            doc! {"isWritablePrimary": true},
            doc! {"isWritablePrimary": false, "setName": "rs"},
            doc! {"isWritablePrimary": true, "setName": "rs", "msg": "isdbgrid"},
        ] {
            assert!(primary_replica(&hello).is_err());
        }
    }
}
