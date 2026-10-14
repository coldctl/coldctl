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
// GitHub Release assets are flat and redirect to GitHub's asset CDN. Keep other
// registries' no-redirect policy, and never forward credentials to redirected URLs.
fn release_asset(url: &Url) -> Option<Url> {
    if url.scheme() != "https" || url.host_str() != Some("github.com") || url.port().is_some() {
        return None;
    }
    let parts: Vec<_> = url.path_segments()?.collect();
    let (kind, name) = match parts.as_slice() {
        [_, _, "releases", "latest", "download", kind, name] => (*kind, *name),
        [_, _, "releases", "download", _, kind, name] => (*kind, *name),
        _ => return None,
    };
    if !["metadata", "targets"].contains(&kind) || !super::model::filename(name) {
        return None;
    }
    let mut mapped = url.clone();
    let prefix = parts[..parts.len() - 2].join("/");
    mapped.set_path(&format!("/{prefix}/{kind}--{name}"));
    Some(mapped)
}
fn github_redirect(url: &Url) -> bool {
    url.scheme() == "https"
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && matches!(
            url.host_str(),
            Some(
                "github.com"
                    | "release-assets.githubusercontent.com"
                    | "objects.githubusercontent.com"
            )
        )
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
                let mapped = release_asset(&url);
                let github = mapped.is_some();
                let mut next = mapped.unwrap_or(url);
                let mut redirects = 0;
                let response = loop {
                    let response = self
                        .client
                        .get(next.clone())
                        .send()
                        .await
                        .map_err(|_| error())?;
                    if !response.status().is_redirection() {
                        break response;
                    }
                    if !github || redirects >= 3 {
                        return Err(error());
                    }
                    let location = response
                        .headers()
                        .get(reqwest::header::LOCATION)
                        .ok_or_else(error)?
                        .to_str()
                        .map_err(|_| error())?;
                    next = next.join(location).map_err(|_| error())?;
                    if !github_redirect(&next) {
                        return Err(error());
                    }
                    redirects += 1;
                };
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn release_mapping_and_redirects_are_narrow() {
        let url = Url::parse(
            "https://github.com/coldctl/coldctl/releases/latest/download/metadata/1.root.json",
        )
        .unwrap();
        assert_eq!(
            release_asset(&url).unwrap().path(),
            "/coldctl/coldctl/releases/latest/download/metadata--1.root.json"
        );
        let url = Url::parse(
            "https://github.com/coldctl/coldctl/releases/download/v1.0.0/targets/hash-connector",
        )
        .unwrap();
        assert_eq!(
            release_asset(&url).unwrap().path(),
            "/coldctl/coldctl/releases/download/v1.0.0/targets--hash-connector"
        );
        for value in [
            "https://example.com/metadata/a",
            "https://github.com/o/r/tree/main/metadata/a",
            "https://github.com/o/r/releases/latest/download/targets/nested/a",
        ] {
            assert!(release_asset(&Url::parse(value).unwrap()).is_none());
        }
        assert!(github_redirect(
            &Url::parse("https://release-assets.githubusercontent.com/path?token=example").unwrap()
        ));
        for value in [
            "http://github.com/a",
            "https://github.com.evil.example/a",
            "https://example.com/a",
            "https://user:secret@github.com/a",
            "https://github.com:8443/a",
        ] {
            assert!(!github_redirect(&Url::parse(value).unwrap()));
        }
    }
}
