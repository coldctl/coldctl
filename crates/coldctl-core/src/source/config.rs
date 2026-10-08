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
    Mysql {
        host: String,
        port: u16,
        database: String,
        user: String,
        tls: TlsMode,
        password_env: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_env: Option<String>,
    },
    MysqlUrlEnv {
        variable: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_env: Option<String>,
    },
    Mongodb {
        host: String,
        port: u16,
        database: String,
        user: String,
        tls: TlsMode,
        password_env: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_env: Option<String>,
    },
    MongodbUrlEnv {
        variable: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_env: Option<String>,
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

// Private pipe payload only: never Debug, logs, arguments, or durable state.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub tls: TlsMode,
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_pem: Option<String>,
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

fn parse_url(value: &str, allow_password: bool, engine: &str) -> Result<ResolvedConnection, Error> {
    let url = url::Url::parse(value).map_err(|_| {
        Error::SourceConfiguration("expected a database URL with host, user, and database")
    })?;
    if !(if matches!(engine, "mysql" | "mongodb") {
        url.scheme() == engine
    } else {
        matches!(url.scheme(), "postgres" | "postgresql")
    }) || url.fragment().is_some()
    {
        return Err(Error::SourceConfiguration(
            "use the selected database scheme without a URL fragment",
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
    let port = url.port().unwrap_or(match engine {
        "mysql" => 3306,
        "mongodb" => 27017,
        _ => 5432,
    });
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
        ca_pem: None,
        password: url.password().map(decode).transpose()?,
    })
}

impl SourceConnection {
    pub fn from_url(url: &str, password_env: Option<String>) -> Result<Self, Error> {
        let engine = if url.starts_with("mongodb://") {
            "mongodb"
        } else if url.starts_with("mysql://") {
            "mysql"
        } else {
            "postgres"
        };
        let parsed = parse_url(url, false, engine)?;
        if engine == "mongodb" {
            let connection = Self::Mongodb {
                host: parsed.host,
                port: parsed.port,
                database: parsed.database,
                user: parsed.user,
                tls: parsed.tls,
                password_env,
                ca_env: None,
            };
            connection.validate()?;
            return Ok(connection);
        }
        if engine == "mysql" {
            let connection = Self::Mysql {
                host: parsed.host,
                port: parsed.port,
                database: parsed.database,
                user: parsed.user,
                tls: parsed.tls,
                password_env,
                ca_env: None,
            };
            connection.validate()?;
            return Ok(connection);
        }
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

    pub fn with_ca_env(mut self, ca: Option<String>) -> Result<Self, Error> {
        if let Some(value) = &ca {
            validate_env(value)?;
        }
        match &mut self {
            Self::Mongodb { ca_env, .. }
            | Self::MongodbUrlEnv { ca_env, .. }
            | Self::Mysql { ca_env, .. }
            | Self::MysqlUrlEnv { ca_env, .. } => *ca_env = ca,
            _ if ca.is_some() => {
                return Err(Error::SourceConfiguration(
                    "--tls-ca-env currently applies to MySQL and MongoDB",
                ));
            }
            _ => {}
        }
        Ok(self)
    }
    pub fn engine(&self) -> &'static str {
        match self {
            Self::Mysql { .. } | Self::MysqlUrlEnv { .. } => "mysql",
            Self::Mongodb { .. } | Self::MongodbUrlEnv { .. } => "mongodb",
            _ => "postgres",
        }
    }
    pub fn from_mysql_url_env(variable: String) -> Result<Self, Error> {
        validate_env(&variable)?;
        Ok(Self::MysqlUrlEnv {
            variable,
            ca_env: None,
        })
    }

    pub fn from_mongodb_url_env(variable: String) -> Result<Self, Error> {
        validate_env(&variable)?;
        Ok(Self::MongodbUrlEnv {
            variable,
            ca_env: None,
        })
    }

    pub fn from_url_env(variable: String) -> Result<Self, Error> {
        validate_env(&variable)?;
        Ok(Self::UrlEnv { variable })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if let Self::Mongodb {
            ca_env: Some(v), ..
        }
        | Self::MongodbUrlEnv {
            ca_env: Some(v), ..
        }
        | Self::Mysql {
            ca_env: Some(v), ..
        }
        | Self::MysqlUrlEnv {
            ca_env: Some(v), ..
        } = self
        {
            validate_env(v)?;
        }
        match self {
            Self::MongodbUrlEnv { variable, .. }
            | Self::UrlEnv { variable }
            | Self::MysqlUrlEnv { variable, .. } => validate_env(variable),
            Self::Mongodb {
                host,
                port,
                database,
                user,
                password_env,
                tls: _,
                ca_env: _,
            }
            | Self::Mysql {
                host,
                port,
                database,
                user,
                password_env,
                ..
            }
            | Self::Postgres {
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

    pub fn resolve(&self) -> Result<ResolvedConnection, Error> {
        self.validate()?;
        let environment = |variable: &str| {
            std::env::var(variable)
            .ok().filter(|s| !s.is_empty())
            .ok_or(Error::SourceConfiguration("credential environment variable is missing, empty, or not Unicode; set the variable referenced by `source show`"))
        };
        let mut resolved = match self {
            Self::MongodbUrlEnv { variable, .. }
            | Self::UrlEnv { variable }
            | Self::MysqlUrlEnv { variable, .. } => {
                parse_url(&environment(variable)?, true, self.engine())
            }
            Self::Mongodb {
                host,
                port,
                database,
                user,
                password_env,
                tls,
                ..
            }
            | Self::Mysql {
                host,
                port,
                database,
                user,
                password_env,
                tls,
                ..
            }
            | Self::Postgres {
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
                ca_pem: None,
                password: password_env.as_deref().map(environment).transpose()?,
            }),
        }?;
        if let Self::Mongodb {
            ca_env: Some(variable),
            ..
        }
        | Self::MongodbUrlEnv {
            ca_env: Some(variable),
            ..
        }
        | Self::Mysql {
            ca_env: Some(variable),
            ..
        }
        | Self::MysqlUrlEnv {
            ca_env: Some(variable),
            ..
        } = self
        {
            if resolved.tls != TlsMode::Require {
                return Err(Error::SourceConfiguration("custom CA requires TLS"));
            }
            let pem = environment(variable)?;
            if pem.len() > 65536 {
                return Err(Error::SourceConfiguration("CA bundle exceeds 64 KiB"));
            }
            resolved.ca_pem = Some(pem);
        }
        Ok(resolved)
    }
}
