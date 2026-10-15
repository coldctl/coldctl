//! Azure Blob configuration. Azurite uses an explicit account-path endpoint.
use super::object::{key_valid, network};
use crate::error::Error;
use object_store::{azure::MicrosoftAzureBuilder, path::Path as ObjectPath};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    pub prefix: String,
    pub endpoint: Option<String>,
    pub account_key_env: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub managed_identity: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_identity_client_id: Option<String>,
    pub allow_http: bool,
}
fn bad() -> Error {
    Error::DestinationConfiguration("invalid Azure Blob configuration")
}
impl AzureConfig {
    pub fn validate(&self) -> Result<(), Error> {
        if !(3..=24).contains(&self.account.len())
            || !self
                .account
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
            || !(3..=63).contains(&self.container.len())
            || !self
                .container
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || self.container.starts_with('-')
            || self.container.ends_with('-')
            || self.container.contains("--")
            || (!self.prefix.is_empty() && !key_valid(&self.prefix))
            || self.account_key_env.is_empty()
            || self.account_key_env.len() > 128
            || !self
                .account_key_env
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(bad());
        }
        if self.managed_identity && (self.endpoint.is_some() || self.allow_http) {
            return Err(bad());
        }
        if let Some(id) = &self.managed_identity_client_id {
            if !self.managed_identity || uuid::Uuid::parse_str(id).is_err() {
                return Err(bad());
            }
        }
        if let Some(endpoint) = &self.endpoint {
            let url = url::Url::parse(endpoint).map_err(|_| bad())?;
            let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
            let account_path = format!("/{}", self.account);
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || url.host_str().is_none()
                || !(url.path() == "/" || (loopback && url.path() == account_path))
                || !(url.scheme() == "https"
                    || (url.scheme() == "http" && self.allow_http && loopback))
                || (self.allow_http && !loopback)
            {
                return Err(bad());
            }
        } else if self.allow_http {
            return Err(bad());
        }
        Ok(())
    }
    pub(super) fn build(&self) -> Result<object_store::azure::MicrosoftAzure, Error> {
        self.builder()?.build().map_err(network)
    }
    pub(super) fn builder(&self) -> Result<MicrosoftAzureBuilder, Error> {
        self.validate()?;
        // Do not use SDK emulator mode: it reads an ambient endpoint override. The
        // explicit endpoint below is validated and frozen in the archive plan.
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&self.account)
            .with_container_name(&self.container)
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
            builder = builder.with_endpoint(endpoint.clone());
        }
        if self.managed_identity {
            if let Some(id) = &self.managed_identity_client_id {
                builder = builder.with_client_id(id);
            }
        } else {
            let key = std::env::var(&self.account_key_env)
                .ok()
                .filter(|v| !v.is_empty())
                .ok_or(Error::DestinationConfiguration(
                    "Azure account-key environment variable missing or empty",
                ))?;
            builder = builder.with_access_key(key);
        }
        Ok(builder)
    }
    pub(super) fn key(&self, key: &str) -> Result<ObjectPath, Error> {
        if !key_valid(key) {
            return Err(bad());
        }
        let key = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        if key.len() > 1024 {
            return Err(bad());
        }
        ObjectPath::parse(key).map_err(|_| bad())
    }
}
pub type AzureDestination = super::object::ObjectDestination;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::ArchiveDestination;
    use object_store::ObjectStore;
    fn config() -> AzureConfig {
        AzureConfig {
            managed_identity: false,
            managed_identity_client_id: None,
            account: "coldctltest".into(),
            container: "coldctl-test".into(),
            prefix: format!("tests/{}", uuid::Uuid::new_v4()),
            endpoint: Some("http://127.0.0.1:11000/coldctltest".into()),
            account_key_env: "AZURE_STORAGE_ACCOUNT_KEY".into(),
            allow_http: true,
        }
    }
    #[test]
    fn cloud_auth_is_opt_in_and_cannot_send_credentials_to_custom_endpoints() {
        let original = config();
        let serialized = serde_json::to_value(&original).unwrap();
        assert!(serialized.get("managed_identity").is_none());
        let mut value: AzureConfig = serde_json::from_value(serialized).unwrap();
        assert!(!value.managed_identity);
        value.managed_identity = true;
        assert!(value.validate().is_err());
        value.endpoint = None;
        value.allow_http = false;
        value.validate().unwrap();
        // Constructing a cloud client must not resolve the static credential references.
        value.builder().unwrap();
        let roundtrip: AzureConfig =
            serde_json::from_value(serde_json::to_value(&value).unwrap()).unwrap();
        assert!(roundtrip.managed_identity);
        value.endpoint = Some("https://attacker.example".into());
        assert!(value.validate().is_err());
    }
    #[test]
    fn azure_configuration_is_validated_without_credentials() {
        let original = config();
        original.validate().unwrap();
        for endpoint in [
            "http://example.com",
            "https://user:secret@example.com",
            "https://example.com/?sig=secret",
            "https://example.com/path",
            "http://127.0.0.1:11000/wrongaccount",
            "file:///tmp",
        ] {
            let mut value = original.clone();
            value.endpoint = Some(endpoint.into());
            assert!(value.validate().is_err());
        }
        for container in ["ab", "-abc", "abc-", "a--b", "Uppercase", "a/b"] {
            let mut value = original.clone();
            value.container = container.into();
            assert!(value.validate().is_err());
        }
        for key in ["", "../outside", "/absolute", "a//b", "a\\b", "a%2fb"] {
            assert!(original.key(key).is_err());
        }
        let mut production = original;
        production.endpoint = None;
        production.allow_http = false;
        production.validate().unwrap();
    }
    #[test]
    #[ignore = "requires Azurite, COLDCTL_TEST_AZURE_ENDPOINT and fixture AZURE_STORAGE_ACCOUNT_KEY"]
    fn azure_emulator_create_only_tampering_missing_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        config.endpoint = Some(std::env::var("COLDCTL_TEST_AZURE_ENDPOINT").unwrap());
        let sink = AzureDestination::new(config.clone(), dir.path().to_owned()).unwrap();
        sink.test_access().unwrap();
        let source = dir.path().join("input");
        std::fs::write(&source, b"original").unwrap();
        let object = sink.put_file("job/batch.parquet", &source).unwrap();
        assert!(sink.exists(&object.key).unwrap());
        assert!(!sink.exists("missing").unwrap());
        std::fs::write(&source, b"replacement").unwrap();
        assert!(sink.put_file(&object.key, &source).is_err());
        sink.verify(&object).unwrap();
        let mut missing = object.clone();
        missing.key = "missing".into();
        assert!(sink.verify(&missing).is_err());
        super::super::object::runtime().unwrap().block_on(async {
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
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}
