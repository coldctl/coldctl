//! S3 adapter. Bounded atomic object writes; resumability is at archive-batch boundaries.
use super::object::{key_valid, network};
use crate::error::Error;
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub prefix: String,
    pub endpoint: Option<String>,
    pub access_key_env: String,
    pub secret_key_env: String,
    pub session_token_env: Option<String>,
    pub allow_http: bool,
}
fn bad() -> Error {
    Error::DestinationConfiguration("invalid S3 configuration")
}
impl S3Config {
    pub fn validate(&self) -> Result<(), Error> {
        if !(3..=63).contains(&self.bucket.len())
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.".contains(&b))
            || self.region.is_empty()
            || !self
                .region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || (!self.prefix.is_empty() && !key_valid(&self.prefix))
        {
            return Err(bad());
        }
        for name in [&self.access_key_env, &self.secret_key_env]
            .into_iter()
            .chain(self.session_token_env.iter())
        {
            if name.is_empty()
                || name.len() > 128
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return Err(bad());
            }
        }
        if let Some(endpoint) = &self.endpoint {
            let url = url::Url::parse(endpoint).map_err(|_| bad())?;
            let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.path() != "/"
                || url.host_str().is_none()
                || !(url.scheme() == "https"
                    || (url.scheme() == "http" && self.allow_http && loopback))
            {
                return Err(bad());
            }
        } else if self.allow_http {
            return Err(bad());
        }
        Ok(())
    }
    pub(super) fn build(&self) -> Result<object_store::aws::AmazonS3, Error> {
        self.validate()?;
        let credential = |name: &str| {
            std::env::var(name).ok().filter(|v| !v.is_empty()).ok_or(
                Error::DestinationConfiguration(
                    "S3 credential environment variable missing or empty",
                ),
            )
        };
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_access_key_id(credential(&self.access_key_env)?)
            .with_secret_access_key(credential(&self.secret_key_env)?)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(self.allow_http)
                    .with_timeout(Duration::from_secs(120))
                    .with_connect_timeout(Duration::from_secs(10)),
            )
            .with_retry(object_store::RetryConfig {
                max_retries: 3,
                retry_timeout: Duration::from_secs(60),
                ..Default::default()
            });
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(name) = &self.session_token_env {
            builder = builder.with_token(credential(name)?);
        }
        builder.build().map_err(network)
    }
    pub(super) fn key(&self, key: &str) -> Result<ObjectPath, Error> {
        if !key_valid(key) {
            return Err(bad());
        }
        ObjectPath::parse(if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        })
        .map_err(|_| bad())
    }
}

pub type S3Destination = super::object::ObjectDestination;

#[cfg(test)]
mod tests {
    use super::super::object::runtime;
    use super::*;
    use crate::destination::ArchiveDestination;
    use object_store::ObjectStore;
    fn config() -> S3Config {
        S3Config {
            bucket: "coldctl-test".into(),
            region: "us-east-1".into(),
            prefix: format!("tests/{}", uuid::Uuid::new_v4()),
            endpoint: Some("http://127.0.0.1:19000".into()),
            access_key_env: "AWS_ACCESS_KEY_ID".into(),
            secret_key_env: "AWS_SECRET_ACCESS_KEY".into(),
            session_token_env: None,
            allow_http: true,
        }
    }
    #[test]
    fn endpoints_keys_and_secret_references_are_validated_without_credentials() {
        let original = config();
        original.validate().unwrap();
        for endpoint in [
            "http://example.com",
            "https://user:secret@example.com",
            "https://example.com/?token=secret",
            "https://example.com/path",
            "file:///tmp",
        ] {
            let mut value = original.clone();
            value.endpoint = Some(endpoint.into());
            assert!(value.validate().is_err());
        }
        for key in [
            "",
            "../outside",
            "a/../b",
            "/absolute",
            "a//b",
            "a\\b",
            "a%2fb",
        ] {
            assert!(original.key(key).is_err());
        }
        let mut value = original.clone();
        value.allow_http = false;
        assert!(value.validate().is_err());
        value.endpoint = None;
        value.validate().unwrap();
    }
    #[test]
    #[ignore = "requires COLDCTL_TEST_S3_ENDPOINT, fixture AWS credentials and coldctl-test bucket"]
    fn s3_emulator_create_only_tampering_missing_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.endpoint = Some(std::env::var("COLDCTL_TEST_S3_ENDPOINT").unwrap());
        let sink = S3Destination::new(config.clone(), dir.path().to_owned()).unwrap();
        sink.test_access().unwrap();
        let source = dir.path().join("input");
        std::fs::write(&source, b"original").unwrap();
        let object = sink.put_file("job/batch.parquet", &source).unwrap();
        assert!(sink.exists(&object.key).unwrap());
        assert!(!sink.exists("missing").unwrap());
        std::fs::write(&source, b"replacement").unwrap();
        assert!(sink.put_file(&object.key, &source).is_err());
        sink.verify(&object).unwrap(); // Conditional write preserved original bytes.
        let mut missing = object.clone();
        missing.key = "missing".into();
        assert!(sink.verify(&missing).is_err());
        // An external actor can change objects unless bucket policy prohibits it. Detect this.
        runtime().unwrap().block_on(async {
            config
                .build()
                .unwrap()
                .put(
                    &config.key(&object.key).unwrap(),
                    bytes::Bytes::from_static(b"tampered").into(),
                )
                .await
                .unwrap();
        });
        assert!(sink.verify(&object).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1); // Read-back temp files are gone.
    }
}
