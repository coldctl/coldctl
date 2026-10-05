use crate::error::Error;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};

/// Persisted configuration contains only public fields or environment variable names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceConnection {
    Postgres {
        host: String,
        port: u16,
        database: String,
        user: String,
        tls: TlsMode,
        password_env: Option<String>,
    },
    UrlEnv {
        variable: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    Require,
    Disable,
}

// Intentionally not Debug or Serialize: resolved credentials must never enter logs/state.
pub(super) struct ResolvedConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub tls: TlsMode,
    pub password: Option<String>,
}

pub fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::SourceConfiguration(
            "name must contain 1–64 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

fn validate_env(variable: &str) -> Result<(), Error> {
    if variable.is_empty()
        || variable.len() > 128
        || !variable
            .bytes()
            .enumerate()
            .all(|(i, b)| b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit()))
    {
        return Err(Error::SourceConfiguration(
            "environment variable names must start with a letter or underscore and contain only ASCII letters, digits, or underscores",
        ));
    }
    Ok(())
}

fn validate_field(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(Error::SourceConfiguration(
            "host, database, and user must be nonempty and contain no control characters",
        ));
    }
    Ok(())
}

fn decode(value: &str) -> Result<String, Error> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|s| s.into_owned())
        .map_err(|_| Error::SourceConfiguration("URL fields must be valid UTF-8"))
}

fn parse_url(value: &str, allow_password: bool) -> Result<ResolvedConnection, Error> {
    let url = url::Url::parse(value).map_err(|_| {
        Error::SourceConfiguration("expected a PostgreSQL URL with host, user, and database")
    })?;
    if !matches!(url.scheme(), "postgres" | "postgresql") || url.fragment().is_some() {
        return Err(Error::SourceConfiguration(
            "use postgres:// or postgresql:// without a URL fragment",
        ));
    }
    if !allow_password && url.password().is_some() {
        return Err(Error::SourceConfiguration(
            "passwords in --url are not persisted; use --password-env with a password-free URL, or --url-env for a full credential URL",
        ));
    }
    let host = match url.host() {
        Some(url::Host::Domain(host)) => host.to_owned(),
        Some(url::Host::Ipv4(host)) => host.to_string(),
        Some(url::Host::Ipv6(host)) => host.to_string(),
        None => return Err(Error::SourceConfiguration("URL must include a TCP host")),
    };
    let user = decode(url.username())?;
    let database = decode(url.path().strip_prefix('/').unwrap_or(url.path()))?;
    for field in [&host, &user, &database] {
        validate_field(field)?;
    }
    if host.contains(',') || database.contains('/') {
        return Err(Error::SourceConfiguration(
            "use one TCP host and one database per source",
        ));
    }
    let mut tls = TlsMode::Require;
    let mut seen_sslmode = false;
    for (key, value) in url.query_pairs() {
        if key != "sslmode" || seen_sslmode {
            return Err(Error::SourceConfiguration(
                "only one sslmode URL parameter is supported; use require or disable",
            ));
        }
        seen_sslmode = true;
        tls = match value.as_ref() {
            "require" => TlsMode::Require,
            "disable" => TlsMode::Disable,
            _ => {
                return Err(Error::SourceConfiguration(
                    "sslmode must be require or disable; TLS downgrade is not automatic",
                ));
            }
        };
    }
    let port = url.port().unwrap_or(5432);
    if port == 0 {
        return Err(Error::SourceConfiguration(
            "port must be between 1 and 65535",
        ));
    }
    Ok(ResolvedConnection {
        host,
        port,
        database,
        user,
        tls,
        password: url.password().map(decode).transpose()?,
    })
}

impl SourceConnection {
    pub fn from_url(url: &str, password_env: Option<String>) -> Result<Self, Error> {
        let parsed = parse_url(url, false)?;
        let connection = Self::Postgres {
            host: parsed.host,
            port: parsed.port,
            database: parsed.database,
            user: parsed.user,
            tls: parsed.tls,
            password_env,
        };
        connection.validate()?;
        Ok(connection)
    }

    pub fn from_url_env(variable: String) -> Result<Self, Error> {
        validate_env(&variable)?;
        Ok(Self::UrlEnv { variable })
    }

    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::UrlEnv { variable } => validate_env(variable),
            Self::Postgres {
                host,
                port,
                database,
                user,
                password_env,
                ..
            } => {
                for field in [host, database, user] {
                    validate_field(field)?;
                }
                if *port == 0 {
                    return Err(Error::SourceConfiguration(
                        "port must be between 1 and 65535",
                    ));
                }
                if let Some(variable) = password_env {
                    validate_env(variable)?;
                }
                Ok(())
            }
        }
    }

    pub(super) fn resolve(&self) -> Result<ResolvedConnection, Error> {
        self.validate()?;
        let environment = |variable: &str| {
            std::env::var(variable)
            .ok().filter(|s| !s.is_empty())
            .ok_or(Error::SourceConfiguration("credential environment variable is missing, empty, or not Unicode; set the variable referenced by `source show`"))
        };
        match self {
            Self::UrlEnv { variable } => parse_url(&environment(variable)?, true),
            Self::Postgres {
                host,
                port,
                database,
                user,
                tls,
                password_env,
            } => Ok(ResolvedConnection {
                host: host.clone(),
                port: *port,
                database: database.clone(),
                user: user.clone(),
                tls: *tls,
                password: password_env.as_deref().map(environment).transpose()?,
            }),
        }
    }
}
