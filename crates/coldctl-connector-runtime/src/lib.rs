//! One in-flight operation per supervised child. No implicit retries.
mod process_tree;
pub mod transport;
use coldctl_connector_protocol::{
    capability::{CURRENT, Hello, negotiate},
    model::ConnectorPin,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStdin, ChildStdout, Command},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Protocol,
    Unavailable,
    Timeout,
    OutcomeUnknown,
    Configuration,
    Authentication,
    Permission,
    Schema,
    OversizedRow,
    UnsupportedColumn,
    RowSecurity,
    Busy,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Protocol=>"connector protocol violation; session closed",
            Self::Unavailable=>"connector unavailable or exited; check installed executable and database connectivity",
            Self::Timeout=>"connector operation timed out; session closed",
            Self::OutcomeUnknown=>"connector write outcome unknown; reconcile the target journal before retrying",
            Self::Configuration=>"connector rejected configuration or operation; check source and target configuration",
            Self::Authentication=>"connector authentication rejected; check credential reference",
            Self::Permission=>"connector permission denied; check database privileges",
            Self::Schema=>"connector identity or schema changed; refusing operation",
            Self::OversizedRow=>"row exceeds the connector archive byte limit; check the documented engine limits",
            Self::UnsupportedColumn=>"unsupported column type in archive source",
            Self::RowSecurity=>"row-level security is unsupported for archival",
            Self::Busy=>"connector target is busy",
        })
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Call<T> {
    pub session: String,
    pub deadline_ms: u32,
    pub body: T,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply<T> {
    pub session: String,
    pub result: Result<T>,
}

pub fn digest(path: &Path) -> Result<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).map_err(|_| Error::Unavailable)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = file.read(&mut bytes).map_err(|_| Error::Unavailable)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
pub struct Client {
    child: Option<Child>,
    input: ChildStdin,
    output: ChildStdout,
    _tree: process_tree::Tree,
    stderr: tokio::task::JoinHandle<()>,
    id: u64,
    session: String,
    pub pin: ConnectorPin,
    pub hello: Hello,
    _lease: Option<std::fs::File>,
    poisoned: bool,
}
impl Client {
    pub async fn launch(
        path: PathBuf,
        expected_id: &str,
        session: String,
        features: BTreeSet<String>,
    ) -> Result<Self> {
        if !path.is_absolute() || !path.is_file() {
            return Err(Error::Configuration);
        }
        let hash = digest(&path)?;
        let mut command = Command::new(&path);
        command
            .env_clear()
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // Windows TLS/system libraries need the system root, never arbitrary caller secrets.
        for key in ["SystemRoot", "WINDIR"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        // Resolve the user's writable temporary directory in the host. With a cleared
        // Windows environment, GetTempPath otherwise falls back to the system directory.
        // Pass only the resolved path, never the caller's full environment.
        let temporary = std::env::temp_dir();
        for key in ["TEMP", "TMP", "TMPDIR"] {
            command.env(key, &temporary);
        }
        process_tree::configure(&mut command);
        let mut child = command.spawn().map_err(|_| Error::Unavailable)?;
        let tree = process_tree::Tree::attach(&child)?;
        let input = child.stdin.take().ok_or(Error::Unavailable)?;
        let mut output = child.stdout.take().ok_or(Error::Unavailable)?;
        let mut errors = child.stderr.take().ok_or(Error::Unavailable)?;
        let stderr = tokio::spawn(async move {
            let mut bytes = [0u8; 4096];
            while let Ok(n) = errors.read(&mut bytes).await {
                if n == 0 {
                    break;
                }
            }
        });
        let hello: Hello =
            match tokio::time::timeout(Duration::from_secs(10), transport::receive(&mut output, 0))
                .await
            {
                Ok(Ok(hello)) => hello,
                _ => {
                    let _ = child.kill().await;
                    stderr.abort();
                    return Err(Error::Protocol);
                }
            };
        if negotiate(&hello, &features, &features).is_err()
            || hello.connector.id != expected_id
            || hello.connector.sha256 != hash
            || digest(&path)? != hash
            || hello.protocol.major != CURRENT.major
        {
            let _ = child.kill().await;
            stderr.abort();
            return Err(Error::Protocol);
        }
        Ok(Self {
            child: Some(child),
            input,
            output,
            _tree: tree,
            stderr,
            id: 0,
            session,
            pin: hello.connector.clone(),
            hello,
            _lease: None,
            poisoned: false,
        })
    }
    pub fn retain_package_lease(&mut self, lease: std::fs::File) {
        self._lease = Some(lease);
    }
    pub async fn call<T: Serialize, R: DeserializeOwned>(
        &mut self,
        body: T,
        deadline_ms: u32,
        write: bool,
    ) -> Result<R> {
        if self.poisoned || deadline_ms == 0 || deadline_ms > 300_000 {
            return Err(Error::Protocol);
        }
        self.id = self.id.checked_add(1).ok_or(Error::Protocol)?;
        // Mark unusable before awaiting. Cancelling this future cannot reuse half a frame.
        self.poisoned = true;
        let request = Call {
            session: self.session.clone(),
            deadline_ms,
            body,
        };
        let mut cancellation = process_tree::Cancellation {
            tree: &self._tree,
            armed: true,
        };
        let outcome = tokio::time::timeout(Duration::from_millis(deadline_ms.into()), async {
            transport::send(&mut self.input, self.id, &request).await?;
            let reply: Reply<R> = transport::receive(&mut self.output, self.id).await?;
            if reply.session != self.session {
                return Err(Error::Protocol);
            }
            reply.result
        })
        .await
        .unwrap_or(Err(Error::Timeout));
        cancellation.armed = false;
        drop(cancellation);
        if outcome.is_ok() {
            self.poisoned = false;
            return outcome;
        }
        self.terminate().await;
        match outcome {
            Err(Error::Unavailable | Error::Timeout | Error::Protocol) if write => {
                Err(Error::OutcomeUnknown)
            }
            other => other,
        }
    }
    pub async fn terminate(&mut self) {
        self.poisoned = true;
        self._tree.terminate();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
        }
        self.stderr.abort();
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.stderr.abort();
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}
