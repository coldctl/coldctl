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
            match source.connection {
                SourceConnection::UrlEnv { variable } => {
                    println!("URL env     {variable} (value hidden; resolved only when connecting)")
                }
                SourceConnection::Postgres {
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
