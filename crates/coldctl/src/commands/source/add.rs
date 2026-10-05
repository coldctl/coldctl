use super::Output;
use clap::{ArgGroup, Args as ClapArgs};
use coldctl_core::{paths::StatePaths, source::SourceConnection, state::sources};

#[derive(ClapArgs)]
#[command(group(ArgGroup::new("connection").required(true).args(["url", "url_env"])))]
pub struct Args {
    /// Unique local source name (ASCII letters, digits, hyphens, underscores).
    #[arg(long)]
    name: String,
    /// Password-free postgres://user@host:5432/database URL; supports sslmode=require|disable.
    #[arg(long)]
    url: Option<String>,
    /// Name of an environment variable holding the full URL; its value is never stored.
    #[arg(long, conflicts_with = "password_env")]
    url_env: Option<String>,
    /// Name of an environment variable holding the password, resolved only when connecting.
    #[arg(long, requires = "url")]
    password_env: Option<String>,
}

pub fn run(
    args: Args,
    paths: &StatePaths,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let connection = match (args.url, args.url_env) {
        (Some(url), None) => SourceConnection::from_url(&url, args.password_env)?,
        (None, Some(variable)) => SourceConnection::from_url_env(variable)?,
        _ => return Err("provide exactly one of --url and --url-env".into()),
    };
    let source = sources::add(paths, &args.name, connection)?;
    match output {
        Output::Json => super::json(&serde_json::to_value(source)?)?,
        Output::Table => crate::output::success(&format!(
            "Source '{}' configured. Run `coldctl source test {}` to check access.",
            source.name, source.name
        )),
    }
    Ok(())
}
