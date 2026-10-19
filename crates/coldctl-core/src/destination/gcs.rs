//! GCS credentials remain local. Custom endpoints are anonymous loopback emulators only.
use anyhow::{Result, anyhow, ensure};
use object_store::{gcp::GoogleCloudStorageBuilder, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use std::{
    io::{Read, Write},
    path::PathBuf,
    time::Duration,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GcsConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    /// Environment variable containing an absolute ADC JSON file path, never its contents.
    #[serde(default)]
    pub credentials_env: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub allow_http: bool,
}
impl GcsConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (3..=222).contains(&self.bucket.len())
                && self.bucket.split('.').all(|part| !part.is_empty()
                    && part.len() <= 63
                    && part
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && part
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric))
                && self
                    .bucket
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_.".contains(&b))
                && self.bucket.parse::<std::net::Ipv4Addr>().is_err(),
            "invalid GCS bucket"
        );
        ensure!(
            self.prefix.is_empty() || (self.prefix.len() <= 500 && key_valid(&self.prefix)),
            "invalid GCS prefix"
        );
        if let Some(name) = &self.credentials_env {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid GCS credential environment reference"
            );
        }
        if let Some(endpoint) = &self.endpoint {
            let url =
                url::Url::parse(endpoint).map_err(|_| anyhow!("invalid GCS emulator endpoint"))?;
            ensure!(
                self.allow_http
                    && self.credentials_env.is_none()
                    && matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
                    && url.scheme() == "http"
                    && url.path() == "/"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "GCS custom endpoints require anonymous HTTP loopback emulation"
            );
        } else {
            ensure!(
                !self.allow_http,
                "GCS HTTP requires an explicit loopback emulator"
            );
        }
        Ok(())
    }
    pub fn key(&self, key: &str) -> Result<ObjectPath> {
        ensure!(key_valid(key), "invalid GCS object key");
        let full = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        ensure!(full.len() <= 1024, "GCS object key too long");
        ObjectPath::parse(full).map_err(|_| anyhow!("invalid GCS object key"))
    }
    pub fn build(&self, timeout_seconds: u64) -> Result<object_store::gcp::GoogleCloudStorage> {
        self.validate()?;
        let mut builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(&self.bucket)
            .with_client_options(
                object_store::ClientOptions::new()
                    .with_allow_http(self.allow_http)
                    .with_timeout(Duration::from_secs(timeout_seconds))
                    .with_connect_timeout(Duration::from_secs(10)),
            )
            .with_retry(object_store::RetryConfig {
                max_retries: 3,
                retry_timeout: Duration::from_secs(30),
                ..Default::default()
            });
        let mut emulator_adc = None;
        if let Some(endpoint) = &self.endpoint {
            // SDK 0.12 also reads ADC even with explicit emulator credentials.
            // Supply a synthetic local ADC file so no ambient credential is read.
            let mut file = tempfile::NamedTempFile::new()
                .map_err(|_| anyhow!("cannot create emulator credential fixture"))?;
            file.write_all(br#"{"type":"authorized_user","client_id":"emulator","client_secret":"emulator","refresh_token":"emulator"}"#)
                .map_err(|_| anyhow!("cannot write emulator credential fixture"))?;
            builder = builder.with_application_credentials(
                file.path()
                    .to_str()
                    .ok_or_else(|| anyhow!("emulator fixture path must be Unicode"))?,
            );
            emulator_adc = Some(file);
            // Works with both pinned SDK versions. Never resolve real credentials
            // for an emulator, and never accept endpoint overrides from a key file.
            builder = builder
                .with_service_account_key(
                    serde_json::json!({
                        "private_key":"", "private_key_id":"emulator", "client_email":"", "disable_oauth":true,
                        "gcs_base_url":endpoint
                    })
                    .to_string(),
                )
                .with_skip_signature(true);
        } else if let Some(path) = self.credentials_path()? {
            validate_credentials(&path)?;
            builder = builder.with_application_credentials(
                path.to_str()
                    .ok_or_else(|| anyhow!("GCS credential path must be Unicode"))?,
            );
        }
        let result = builder.build().map_err(|_| anyhow!("GCS client initialization failed; check supported ADC credentials (details redacted)"));
        drop(emulator_adc);
        result
    }
    fn credentials_path(&self) -> Result<Option<PathBuf>> {
        let name = self
            .credentials_env
            .as_deref()
            .unwrap_or("GOOGLE_APPLICATION_CREDENTIALS");
        match std::env::var_os(name) {
            Some(value) if !value.is_empty() => {
                let path = PathBuf::from(value);
                ensure!(
                    path.is_absolute(),
                    "GCS credentials environment variable must contain an absolute JSON file path"
                );
                Ok(Some(path))
            }
            Some(_) => Err(anyhow!("GCS credential environment variable is empty")),
            None if self.credentials_env.is_some() => {
                Err(anyhow!("GCS credential environment variable is missing"))
            }
            None => {
                // Mirror the SDK's well-known ADC file; otherwise use attached
                // instance credentials. Explicit missing files must never fall back.
                let root = std::env::var_os(if cfg!(windows) { "APPDATA" } else { "HOME" });
                let path = root.map(PathBuf::from).map(|p| {
                    p.join(if cfg!(windows) {
                        "gcloud/application_default_credentials.json"
                    } else {
                        ".config/gcloud/application_default_credentials.json"
                    })
                });
                Ok(path.filter(|p| p.exists()))
            }
        }
    }
}
fn key_valid(key: &str) -> bool {
    !key.is_empty()
        && key
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-_.".contains(&b))
}
fn validate_credentials(path: &std::path::Path) -> Result<()> {
    let file =
        std::fs::File::open(path).map_err(|_| anyhow!("GCS credential file cannot be read"))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow!("GCS credential file cannot be read"))?;
    ensure!(
        bytes.len() <= 1024 * 1024,
        "GCS credential file exceeds 1 MiB"
    );
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow!("invalid GCS credential JSON (details redacted)"))?;
    ensure!(
        matches!(
            value.get("type").and_then(|v| v.as_str()),
            Some("service_account" | "authorized_user")
        ),
        "GCS supports service_account or authorized_user ADC files; external_account federation is not supported by the pinned SDK"
    );
    ensure!(
        value.get("gcs_base_url").is_none() && value.get("disable_oauth").is_none(),
        "GCS credential files cannot override storage endpoints or disable authentication"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    pub fn config() -> GcsConfig {
        GcsConfig {
            bucket: "coldctl-test".into(),
            prefix: "test".into(),
            credentials_env: None,
            endpoint: None,
            allow_http: false,
        }
    }
    #[test]
    fn gcs_configuration_rejects_unsafe_names_paths_and_endpoints() {
        let c = config();
        c.validate().unwrap();
        for bucket in ["a", "Uppercase", "127.0.0.1", "a..b", "-abc", "abc-", "a/b"] {
            let mut bad = c.clone();
            bad.bucket = bucket.into();
            assert!(bad.validate().is_err());
        }
        for key in ["", "../x", "a//b", "/x", "a\\b", "a%2fb"] {
            assert!(c.key(key).is_err());
        }
        for endpoint in [
            "https://example.com",
            "http://example.com",
            "http://user:password@localhost",
            "http://localhost/path",
            "http://localhost/?token=secret",
        ] {
            let mut bad = c.clone();
            bad.endpoint = Some(endpoint.into());
            bad.allow_http = true;
            assert!(bad.validate().is_err());
        }
        let mut emulator = c;
        emulator.endpoint = Some("http://127.0.0.1:4443".into());
        emulator.allow_http = true;
        emulator.validate().unwrap();
        emulator.build(120).unwrap();
        emulator.credentials_env = Some("GOOGLE_APPLICATION_CREDENTIALS".into());
        assert!(emulator.validate().is_err());
    }
    #[test]
    fn gcs_credentials_reject_overrides_and_unsupported_types_without_disclosure() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("credentials.json");
        for value in [
            serde_json::json!({"type":"external_account","secret":"never-disclose"}),
            serde_json::json!({"type":"service_account","gcs_base_url":"http://attacker","private_key":"never-disclose"}),
            serde_json::json!({"type":"service_account","disable_oauth":true}),
        ] {
            std::fs::write(&path, value.to_string()).unwrap();
            let error = validate_credentials(&path).unwrap_err().to_string();
            assert!(!error.contains("never-disclose"));
        }
        std::fs::write(
            &path,
            r#"{"type":"authorized_user","client_secret":"never-disclose"}"#,
        )
        .unwrap();
        validate_credentials(&path).unwrap();
    }
    #[test]
    #[ignore = "requires loopback GCS emulator and coldctl-test bucket"]
    fn gcs_emulator_create_only_tampering_missing_and_cleanup() {
        use crate::destination::{ArchiveDestination, object::ObjectDestination};
        use object_store::ObjectStoreExt;
        let dir = tempfile::tempdir().unwrap();
        let mut c = config();
        c.endpoint = Some(std::env::var("COLDCTL_TEST_GCS_ENDPOINT").unwrap());
        c.allow_http = true;
        c.prefix = format!("sink-tests/{}", uuid::Uuid::new_v4());
        let sink = ObjectDestination::new(c.clone(), dir.path().to_owned()).unwrap();
        assert!(sink.test_access().unwrap().probe_removed);
        let source = dir.path().join("input");
        std::fs::write(&source, b"original").unwrap();
        let object = sink.put_file("job/batch.parquet", &source).unwrap();
        assert!(sink.exists(&object.key).unwrap());
        assert!(!sink.exists("missing").unwrap());
        assert!(sink.put_file(&object.key, &source).is_err());
        sink.verify(&object).unwrap();
        let mut bytes = Vec::new();
        sink.read_verified(&object)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        assert_eq!(bytes, b"original");
        super::super::object::runtime().unwrap().block_on(async {
            c.build(120)
                .unwrap()
                .put(
                    &c.key(&object.key).unwrap(),
                    bytes::Bytes::from_static(b"tampered").into(),
                )
                .await
                .unwrap();
        });
        assert!(sink.verify(&object).is_err());
        let mut missing = object;
        missing.key = "missing".into();
        assert!(sink.verify(&missing).is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
