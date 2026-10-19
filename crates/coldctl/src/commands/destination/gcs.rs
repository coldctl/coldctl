use crate::output::Output;
use clap::Args as ClapArgs;
use coldctl_core::{destination::gcs::GcsConfig, paths::StatePaths, state::destinations};
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long)]
    name: String,
    #[arg(long)]
    bucket: String,
    #[arg(long, default_value = "coldctl")]
    prefix: String,
    /// Variable containing an absolute ADC JSON file path. Defaults to Google's ADC chain.
    #[arg(long)]
    credentials_env: Option<String>,
    /// Anonymous loopback GCS emulator only; never a production endpoint override.
    #[arg(long, requires = "allow_http", conflicts_with = "credentials_env")]
    endpoint: Option<String>,
    #[arg(long, requires = "endpoint")]
    allow_http: bool,
    /// Durable local staging directory for pending batches.
    #[arg(long)]
    staging_path: Option<PathBuf>,
}
pub fn run(
    args: Args,
    paths: &StatePaths,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let staging = args
        .staging_path
        .unwrap_or_else(|| paths.data_dir.join("staging").join(&args.name));
    let destination = destinations::add_remote(
        paths,
        &args.name,
        &staging,
        GcsConfig {
            bucket: args.bucket,
            prefix: args.prefix,
            credentials_env: args.credentials_env,
            endpoint: args.endpoint,
            allow_http: args.allow_http,
        }
        .into(),
    )?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destination)?),
        Output::Table => crate::output::success(&format!(
            "GCS destination '{}' configured. Run destination test to check access.",
            destination.name
        )),
    }
    Ok(())
}
