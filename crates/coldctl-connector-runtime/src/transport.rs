//! Bounded, correlated control envelopes followed by digest-checked binary chunks.
use crate::{Error, Result};
use coldctl_connector_protocol::wire::{FrameHeader, FrameKind, HEADER_BYTES, MAX_CHUNK_BYTES};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    bytes: usize,
    chunks: usize,
    sha256: String,
}

async fn frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    id: u64,
    kind: FrameKind,
    bytes: &[u8],
) -> Result<()> {
    let header = FrameHeader {
        kind,
        request_id: id,
        payload_bytes: bytes.len().try_into().map_err(|_| Error::Protocol)?,
    }
    .encode()
    .map_err(|_| Error::Protocol)?;
    writer
        .write_all(&header)
        .await
        .map_err(|_| Error::Unavailable)?;
    writer
        .write_all(bytes)
        .await
        .map_err(|_| Error::Unavailable)
}
async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    id: u64,
    kind: FrameKind,
) -> Result<Vec<u8>> {
    let mut raw = [0; HEADER_BYTES];
    reader
        .read_exact(&mut raw)
        .await
        .map_err(|_| Error::Unavailable)?;
    let header = FrameHeader::decode(&raw).map_err(|_| Error::Protocol)?;
    if header.request_id != id || header.kind != kind {
        return Err(Error::Protocol);
    }
    let mut bytes = vec![0; header.payload_bytes as usize];
    reader
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Error::Unavailable)?;
    Ok(bytes)
}
pub async fn send<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    id: u64,
    value: &T,
) -> Result<()> {
    // A capped writer prevents unbounded serialization even for a faulty caller.
    struct Limited(Vec<u8>);
    impl std::io::Write for Limited {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.0.len().saturating_add(bytes.len()) > MAX_MESSAGE {
                return Err(std::io::Error::other("message limit"));
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Limited(Vec::new());
    serde_json::to_writer(&mut buffer, value).map_err(|_| Error::Protocol)?;
    let bytes = buffer.0;
    let metadata = Metadata {
        bytes: bytes.len(),
        chunks: bytes.len().div_ceil(MAX_CHUNK_BYTES as usize),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    frame(
        writer,
        id,
        FrameKind::Control,
        &serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?,
    )
    .await?;
    for chunk in bytes.chunks(MAX_CHUNK_BYTES as usize) {
        frame(writer, id, FrameKind::Data, chunk).await?;
    }
    writer.flush().await.map_err(|_| Error::Unavailable)
}
pub async fn receive<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
    id: u64,
) -> Result<T> {
    let control = read_frame(reader, id, FrameKind::Control).await?;
    let metadata: Metadata = serde_json::from_slice(&control).map_err(|_| Error::Protocol)?;
    if metadata.bytes == 0
        || metadata.bytes > MAX_MESSAGE
        || metadata.chunks != metadata.bytes.div_ceil(MAX_CHUNK_BYTES as usize)
        || metadata.sha256.len() != 64
    {
        return Err(Error::Protocol);
    }
    let mut bytes = Vec::with_capacity(metadata.bytes);
    for _ in 0..metadata.chunks {
        let part = read_frame(reader, id, FrameKind::Data).await?;
        if part.len() != (metadata.bytes - bytes.len()).min(MAX_CHUNK_BYTES as usize) {
            return Err(Error::Protocol);
        }
        bytes.extend_from_slice(&part);
    }
    if format!("{:x}", Sha256::digest(&bytes)) != metadata.sha256 {
        return Err(Error::Protocol);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)
}
