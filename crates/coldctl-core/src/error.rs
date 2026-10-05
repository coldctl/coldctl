use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Archive/policy error: {0}")]
    Archive(&'static str),
    #[error("Archive job {job_id} failed: {reason}. Inspect it with `coldctl jobs show {job_id}`")]
    JobFailed {
        job_id: String,
        reason: String,
        failure: Failure,
    },
    #[error("Invalid destination configuration: {0}")]
    DestinationConfiguration(&'static str),
    #[error("A destination with this name already exists")]
    DestinationExists,
    #[error("Destination not found; use `coldctl destination list` to see configured destinations")]
    DestinationNotFound,
    #[error("Destination {operation} failed at {path}: {source}")]
    DestinationAccess {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Coldctl is not initialized; run `coldctl init` first")]
    NotInitialized,
    #[error("Invalid source configuration: {0}")]
    SourceConfiguration(&'static str),
    #[error("A source with this name already exists")]
    SourceExists,
    #[error("Source not found; use `coldctl source list` to see configured sources")]
    SourceNotFound,
    #[error("PostgreSQL {operation} failed: {reason}")]
    Postgres {
        operation: &'static str,
        reason: &'static str,
    },
    #[error("Unable to resolve the application data directory; supply --data-dir")]
    DataDirectoryUnavailable,
    #[error("Unable to access {path}: {source}")]
    Filesystem {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Unable to use SQLite state at {path}: {source}")]
    Database {
        path: PathBuf,
        #[source]
        source: rusqlite::Error,
    },
    #[error("Invalid local state: {0}. State has been preserved")]
    InvalidState(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    Configuration,
    Authentication,
    Permission,
    Connectivity,
    Timeout,
    Schema,
    OversizedRow,
    Corruption,
    Storage,
    State,
    Unknown,
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Failure {
    pub category: FailureCategory,
    /// True means a manual retry may succeed once the transient cause clears.
    /// This never authorizes retrying against a changed source.
    pub retryable: bool,
    pub message: String,
    pub next_step: String,
}
impl Error {
    /// Classify only our own static messages and typed errors. Never retain driver
    /// errors, SQL, row contents, connection strings, paths, or SQLite values.
    pub fn failure(&self) -> Failure {
        use FailureCategory::*;
        let (category, retryable, message) = match self {
            Self::JobFailed { failure, .. } => return failure.clone(),
            Self::Postgres { reason, .. } => {
                let category = if reason.contains("authentication") {
                    Authentication
                } else if reason.contains("permission denied") {
                    Permission
                } else if reason.contains("source schema") {
                    Schema
                } else if reason.contains("timed out") {
                    Timeout
                } else if reason.contains("cannot accept connections")
                    || reason.contains("connection closed")
                    || reason.contains("connection or TLS")
                {
                    Connectivity
                } else {
                    Configuration
                };
                let retryable = matches!(category, Timeout)
                    || reason.contains("cannot accept connections")
                    || reason.contains("connection closed");
                (category, retryable, *reason)
            }
            Self::Archive(message) => {
                let category =
                    if message.contains("timed out") || message.contains("operation deadline") {
                        Timeout
                    } else if message.contains("schema USAGE and table SELECT")
                        || message.contains("permission denied")
                    {
                        Permission
                    } else if message.contains("64 KiB archive limit") {
                        OversizedRow
                    } else if message.contains("schema changed")
                        || message.contains("identity changed")
                        || message.contains("column definition changed")
                        || message.contains("endpoint or user changed")
                    {
                        Schema
                    } else if message.contains("verification failed")
                        || message.contains("failed verification")
                        || message.contains("checksum")
                        || message.contains("does not match")
                        || message.contains("differs from")
                        || message.contains("inconsistent")
                        || message.contains("missing batch checkpoint")
                        || message.contains("missing manifest checkpoint")
                        || message.contains("mismatch")
                        || message.contains("do not match")
                        || message.contains("invalid batch checkpoint")
                        || message.contains("out of order or bounds")
                    {
                        Corruption
                    } else if message.contains("cannot stage")
                        || message.contains("cannot publish")
                        || message.contains("free space")
                        || message.contains("destination object")
                    {
                        Storage
                    } else {
                        Configuration
                    };
                let retryable = matches!(category, Timeout);
                (category, retryable, *message)
            }
            Self::DestinationAccess { source, .. } | Self::Filesystem { source, .. } => {
                if source.kind() == std::io::ErrorKind::PermissionDenied {
                    (Permission, false, "filesystem access denied")
                } else {
                    (
                        Storage,
                        false,
                        "filesystem operation failed; diagnostic details withheld",
                    )
                }
            }
            Self::Database { .. } | Self::InvalidState(_) => {
                (State, false, "local state is unavailable or inconsistent")
            }
            Self::SourceConfiguration(message) | Self::DestinationConfiguration(message) => {
                (Configuration, false, *message)
            }
            Self::NotInitialized => (
                Configuration,
                false,
                "Coldctl is not initialized; run `coldctl init` first",
            ),
            Self::SourceNotFound => (
                Configuration,
                false,
                "Source not found; use `coldctl source list`",
            ),
            Self::DestinationNotFound => (
                Configuration,
                false,
                "Destination not found; use `coldctl destination list`",
            ),
            Self::SourceExists => (
                Configuration,
                false,
                "A source with this name already exists",
            ),
            Self::DestinationExists => (
                Configuration,
                false,
                "A destination with this name already exists",
            ),
            Self::DataDirectoryUnavailable => (
                Configuration,
                false,
                "Unable to resolve the application data directory; supply --data-dir",
            ),
        };
        let next_step = match category {
            Timeout => {
                "Wait for database load or locks to clear; check timeout settings, then manually resume the same job."
            }
            Connectivity => {
                "Check server availability, host, port, network and TLS trust. After fixing the cause, manually resume the same job."
            }
            Authentication => {
                "Set the referenced credential environment variable and check the database user, then retry."
            }
            Permission => {
                "Fix database CONNECT/USAGE/SELECT or local filesystem permissions, then retry."
            }
            Schema => {
                "Do not resume against a changed source. Preserve this archive, inspect schema/source identity, and create a new job only after reviewing source stability."
            }
            OversizedRow => {
                "The row exceeds the supported limit; smaller batches cannot fix it. Review the export scope or use another export method. No value was truncated."
            }
            Corruption => {
                "Preserve the archive and state. Restore missing or damaged files from a trusted backup and verify; do not delete checkpoints or blindly retry."
            }
            Storage => {
                "Check disk space, destination availability and filesystem permissions, then manually resume the same job."
            }
            State => {
                "Preserve state.db and archive files. Check state directory access and run coldctl status; restore trusted state backup if necessary."
            }
            Configuration | Unknown => {
                "Review source, destination, policy and command options; fix the reported requirement before retrying."
            }
        };
        Failure {
            category,
            retryable,
            message: message.into(),
            next_step: next_step.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transient_failures_and_permanent_requirements_have_distinct_guidance() {
        for (error, category, retryable) in [
            (
                Error::Archive("batch exceeded the operation deadline"),
                FailureCategory::Timeout,
                true,
            ),
            (
                Error::Postgres {
                    operation: "connect",
                    reason: "connection closed or unavailable; check network and server availability before retrying",
                },
                FailureCategory::Connectivity,
                true,
            ),
            (
                Error::Postgres {
                    operation: "connect",
                    reason: "connection or TLS failure; check host, port, network access, and the server certificate trust/hostname",
                },
                FailureCategory::Connectivity,
                false,
            ),
            (
                Error::Postgres {
                    operation: "connect",
                    reason: "authentication rejected; check the user and credential environment variable",
                },
                FailureCategory::Authentication,
                false,
            ),
            (
                Error::Postgres {
                    operation: "read",
                    reason: "permission denied; check CONNECT, schema USAGE, and table SELECT privileges",
                },
                FailureCategory::Permission,
                false,
            ),
            (
                Error::Archive("source database or column identity changed; refusing resume"),
                FailureCategory::Schema,
                false,
            ),
            (
                Error::Archive("row exceeds the 64 KiB archive limit; no truncation was performed"),
                FailureCategory::OversizedRow,
                false,
            ),
            (
                Error::Archive("archive object size/checksum verification failed"),
                FailureCategory::Corruption,
                false,
            ),
        ] {
            let failure = error.failure();
            assert_eq!(failure.category, category);
            assert_eq!(failure.retryable, retryable);
            assert!(!failure.next_step.is_empty());
        }
    }

    #[test]
    fn dynamic_state_errors_are_not_included_in_diagnostics() {
        let error = Error::InvalidState("secret row value or password".into());
        let json = serde_json::to_string(&error.failure()).unwrap();
        assert!(!json.contains("secret row value or password"));
        assert!(json.contains("state"));
    }
}
