use super::{Output, text};
use coldctl_core::{paths::StatePaths, source::SourceConnection, state::sources};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = sources::show(paths, name)?;
    match output {
        Output::Json => super::json(&serde_json::to_value(source)?)?,
        Output::Table => {
            println!(
                "Name        {}\nID          {}\nType        {}\nCreated     {}",
                source.name,
                text(&source.id),
                source.source_type,
                text(&source.created_at)
            );
            if let SourceConnection::Mongodb {
                ca_env: Some(v), ..
            }
            | SourceConnection::MongodbUrlEnv {
                ca_env: Some(v), ..
            }
            | SourceConnection::Mysql {
                ca_env: Some(v), ..
            }
            | SourceConnection::MysqlUrlEnv {
                ca_env: Some(v), ..
            } = &source.connection
            {
                println!("TLS CA env  {v}");
            }
            match source.connection {
                SourceConnection::MongodbUrlEnv { variable, .. }
                | SourceConnection::UrlEnv { variable }
                | SourceConnection::MysqlUrlEnv { variable, .. } => {
                    println!("URL env     {variable} (value hidden; resolved only when connecting)")
                }
                SourceConnection::Mongodb {
                    host,
                    port,
                    database,
                    user,
                    tls,
                    password_env,
                    ..
                }
                | SourceConnection::Mysql {
                    host,
                    port,
                    database,
                    user,
                    tls,
                    password_env,
                    ..
                }
                | SourceConnection::Postgres {
                    host,
                    port,
                    database,
                    user,
                    tls,
                    password_env,
                } => {
                    println!(
                        "Host        {}\nPort        {port}\nDatabase    {}\nUser        {}\nTLS         {tls:?}",
                        text(&host),
                        text(&database),
                        text(&user)
                    );
                    println!("Password env {}", password_env.as_deref().unwrap_or("none"));
                }
            }
        }
    }
    Ok(())
}
