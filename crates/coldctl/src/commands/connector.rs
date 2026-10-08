use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::{connectors, paths::StatePaths};
use std::path::PathBuf;
#[derive(ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Pin a registry's independently verified TUF root. Never trusts keys from a downloaded bundle.
    Registry {
        #[arg(long)]
        url: String,
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        root_sha256: String,
    },
    /// Verify current signed metadata and retain revocations; no executable is launched.
    Refresh {
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// List installed connectors, or refresh and list available signed packages.
    List {
        #[arg(long)]
        available: bool,
        #[arg(long, requires = "available")]
        file: Option<PathBuf>,
    },
    /// Install an exact signed version. --file names an offline repository directory.
    Install {
        id: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        no_activate: bool,
    },
    /// Select an installed version for new sessions. Pinned jobs keep their exact version.
    Use {
        id: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        allow_downgrade: bool,
    },
    /// Inspect installed versions, integrity, active selection and resumable-job references.
    Inspect {
        id: String,
        #[arg(long)]
        version: Option<String>,
    },
    /// Check local package integrity and revocation state without executing or downloading code.
    Doctor,
    /// Remove an installed version unless running sessions or resumable jobs need it.
    Remove {
        id: String,
        #[arg(long)]
        version: String,
    },
    /// Export one connector and signed metadata into a new offline repository directory.
    Bundle {
        id: String,
        #[arg(long)]
        version: String,
        #[arg(long)]
        out: PathBuf,
    },
}
pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Registry {
            url,
            root,
            root_sha256,
        } => {
            connectors::configure_registry(paths, &url, &root, &root_sha256)?;
            println!(
                "Registry root pinned. Run `coldctl connector refresh` to verify the catalog."
            );
        }
        Command::Refresh { file } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&connectors::refresh(paths, file.as_deref()).await?)?
            );
        }
        Command::List {
            available: true,
            file,
        } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&connectors::refresh(paths, file.as_deref()).await?)?
            );
        }
        Command::List { .. } => println!(
            "{}",
            serde_json::to_string_pretty(&connectors::list(paths)?)?
        ),
        Command::Install {
            id,
            version,
            file,
            no_activate,
        } => {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &connectors::install(paths, &id, &version, file.as_deref(), !no_activate)
                        .await?
                )?
            );
        }
        Command::Use {
            id,
            version,
            allow_downgrade,
        } => {
            connectors::activate(paths, &id, &version, allow_downgrade)?;
            println!("Default connector selected. Existing job pins remain unchanged.");
        }
        Command::Inspect { id, version } => {
            let packages = connectors::list(paths)?
                .into_iter()
                .filter(|p| {
                    p.package.pin().id == id
                        && version
                            .as_ref()
                            .is_none_or(|v| v == &p.package.pin().version)
                })
                .collect::<Vec<_>>();
            if packages.is_empty() {
                return Err("requested connector is not installed".into());
            }
            println!("{}", serde_json::to_string_pretty(&packages)?);
        }
        Command::Doctor => {
            let packages = connectors::list(paths)?;
            println!("{}", serde_json::to_string_pretty(&packages)?);
            if packages.iter().any(|p| !p.healthy || p.revoked) {
                return Err("connector integrity or revocation check failed".into());
            }
        }
        Command::Remove { id, version } => {
            connectors::remove(paths, &id, &version)?;
            println!("Connector removed. Source configurations and archives are preserved.");
        }
        Command::Bundle { id, version, out } => {
            connectors::bundle(paths, &id, &version, &out).await?;
            println!(
                "Signed offline bundle created. Distribute its trust-root fingerprint independently."
            );
        }
    }
    Ok(())
}
