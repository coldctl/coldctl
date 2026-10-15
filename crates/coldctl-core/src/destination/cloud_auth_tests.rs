//! Exercise the actual SDK metadata providers without network access or cloud secrets.
use http_body_util::BodyExt;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Clone, Default)]
struct Metadata(Arc<AtomicUsize>);
impl HttpConnector for Metadata {
    fn connect(&self, _: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(self.clone()))
    }
}
#[async_trait::async_trait]
impl HttpService for Metadata {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        assert_eq!(req.uri().host(), Some("169.254.169.254"));
        let body = match req.uri().path() {
            "/latest/api/token" => {
                assert_eq!(req.method(), "PUT");
                "imds-v2-token".to_owned()
            }
            "/latest/meta-data/iam/security-credentials/" => {
                assert_eq!(req.headers()["x-aws-ec2-metadata-token"], "imds-v2-token");
                "fixture-role".to_owned()
            }
            "/latest/meta-data/iam/security-credentials/fixture-role" => {
                assert_eq!(req.headers()["x-aws-ec2-metadata-token"], "imds-v2-token");
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                serde_json::json!({"AccessKeyId":format!("fixture-{n}"),"SecretAccessKey":"fixture-secret","Token":"fixture-session","Expiration":"2000-01-01T00:00:00Z"}).to_string()
            }
            "/metadata/identity/oauth2/token" => {
                assert_eq!(req.headers()["metadata"], "true");
                assert!(
                    req.uri()
                        .query()
                        .unwrap()
                        .contains("client_id=00000000-0000-0000-0000-000000000001")
                );
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                serde_json::json!({"access_token":format!("fixture-{n}"),"expires_on":"1"})
                    .to_string()
            }
            _ => panic!("unexpected metadata request"),
        };
        let body =
            http_body_util::Full::new(bytes::Bytes::from(body)).map_err(|never| match never {});
        Ok(http::Response::new(HttpResponseBody::new(body)))
    }
}

#[tokio::test]
async fn azure_managed_identity_refreshes_and_selects_user_identity() {
    let config: super::azure::AzureConfig = serde_json::from_value(serde_json::json!({
        "account":"fixtureaccount", "container":"archives", "prefix":"", "endpoint":null,
        "account_key_env":"COLDCTL_TEST_UNUSED_SECRET", "allow_http":false,
        "managed_identity":true, "managed_identity_client_id":"00000000-0000-0000-0000-000000000001"
    }))
    .unwrap();
    let metadata = Metadata::default();
    let store = config
        .builder()
        .unwrap()
        .with_http_connector(metadata.clone())
        .build()
        .unwrap();
    for _ in 0..2 {
        store.credentials().get_credential().await.unwrap();
    }
    assert_eq!(metadata.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn aws_role_uses_imdsv2_and_refreshes_expired_credentials() {
    // A configured web identity intentionally takes precedence over EC2 IMDS.
    // Run this fixture in a process without workload identity environment variables.
    assert!(std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_none());
    let config: super::s3::S3Config = serde_json::from_value(serde_json::json!({
        "bucket":"fixture-bucket", "region":"us-east-1", "prefix":"", "endpoint":null,
        "access_key_env":"COLDCTL_TEST_UNUSED_ACCESS", "secret_key_env":"COLDCTL_TEST_UNUSED_SECRET",
        "session_token_env":null, "allow_http":false, "role_auth":true
    })).unwrap();
    let metadata = Metadata::default();
    let store = config
        .builder()
        .unwrap()
        .with_http_connector(metadata.clone())
        .build()
        .unwrap();
    let first = store.credentials().get_credential().await.unwrap();
    let second = store.credentials().get_credential().await.unwrap();
    assert_ne!(first.key_id, second.key_id);
    assert_eq!(metadata.0.load(Ordering::SeqCst), 2);
}
