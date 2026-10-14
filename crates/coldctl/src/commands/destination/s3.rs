use crate::output::Output;
use clap::Args as ClapArgs;
use coldctl_core::{destination::s3::S3Config, paths::StatePaths, state::destinations};
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long)]
    name: String,
    #[arg(long)]
    bucket: String,
    #[arg(long)]
    region: String,
    #[arg(long, default_value = "coldctl")]
    prefix: String,
    #[arg(long)]
    endpoint: Option<String>,
    /// Durable local staging directory; only pending batches remain here.
    #[arg(long)]
    staging_path: Option<PathBuf>,
    #[arg(long, default_value = "AWS_ACCESS_KEY_ID")]
    access_key_env: String,
    #[arg(long, default_value = "AWS_SECRET_ACCESS_KEY")]
    secret_key_env: String,
    #[arg(long)]
    session_token_env: Option<String>,
    /// Permit HTTP only for an explicitly configured loopback emulator.
    #[arg(long)]
    allow_http: bool,
}
pub fn run(
    args: Args,
    paths: &StatePaths,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let staging = args
        .staging_path
        .unwrap_or_else(|| paths.data_dir.join("staging").join(&args.name));
    let destination = destinations::add_s3(
        paths,
        &args.name,
        &staging,
        S3Config {
            bucket: args.bucket,
            region: args.region,
            prefix: args.prefix,
            endpoint: args.endpoint,
            access_key_env: args.access_key_env,
            secret_key_env: args.secret_key_env,
            session_token_env: args.session_token_env,
            allow_http: args.allow_http,
        },
    )?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destination)?),
        Output::Table => crate::output::success(&format!(
            "S3 destination '{}' configured. Run destination test to check access.",
            destination.name
        )),
    }
    Ok(())
}
