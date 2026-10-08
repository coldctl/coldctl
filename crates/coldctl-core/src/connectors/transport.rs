use super::{
    Result, fail,
    model::{MAX_CATALOG, MAX_FILE},
};
use async_trait::async_trait;
use futures_util::StreamExt;
use tough::{FilesystemTransport, Transport, TransportError, TransportErrorKind, TransportStream};
use url::Url;

#[derive(Debug, Clone)]
pub struct BoundedTransport {
    client: reqwest::Client,
    metadata: Url,
    targets: Url,
}
impl BoundedTransport {
    pub fn new(metadata: Url, targets: Url) -> Result<Self> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(120))
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|_| fail("cannot initialize registry transport"))?;
        Ok(Self {
            client,
            metadata,
            targets,
        })
    }
}
fn error() -> TransportError {
    TransportError::new(TransportErrorKind::Other, "connector-registry")
}
#[async_trait]
impl Transport for BoundedTransport {
    async fn fetch(&self, url: Url) -> std::result::Result<TransportStream, TransportError> {
        // Only flat names below the configured bases; no delegated path traversal.
        let cap = if url
            .as_str()
            .strip_prefix(self.metadata.as_str())
            .is_some_and(super::model::filename)
        {
            MAX_CATALOG
        } else if url
            .as_str()
            .strip_prefix(self.targets.as_str())
            .is_some_and(super::model::filename)
        {
            MAX_FILE
        } else {
            return Err(error());
        };
        let stream: TransportStream = match url.scheme() {
            "file" => {
                let path = url.to_file_path().map_err(|_| error())?;
                crate::fs_security::check(&path).map_err(|_| error())?;
                if let Ok(meta) = std::fs::metadata(&path) {
                    if !meta.is_file() || meta.len() > cap {
                        return Err(error());
                    }
                }
                FilesystemTransport.fetch(url).await?
            }
            "https" => {
                let response = self.client.get(url).send().await.map_err(|_| error())?;
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Err(TransportError::new(
                        TransportErrorKind::FileNotFound,
                        "connector-registry",
                    ));
                }
                if !response.status().is_success()
                    || response.content_length().is_some_and(|n| n > cap)
                {
                    return Err(error());
                }
                Box::pin(response.bytes_stream().map(|v| v.map_err(|_| error())))
            }
            _ => return Err(error()),
        };
        let mut received = 0u64;
        Ok(Box::pin(stream.map(move |chunk| {
            let chunk = chunk?;
            received = received.saturating_add(chunk.len() as u64);
            if received > cap {
                Err(error())
            } else {
                Ok(chunk)
            }
        })))
    }
}
