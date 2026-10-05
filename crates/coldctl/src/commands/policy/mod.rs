use crate::output::{Output, text};
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::{paths::StatePaths, policy::model::PolicyConfig, state::archive as store};

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long,global=true,value_enum,default_value_t=Output::Table)]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Save a retention policy. Validation against PostgreSQL is a separate read-only command.
    Create(Create),
    List,
    Show {
        name: String,
    },
    /// Validate source/table/types and show an estimated archive plan without writing files.
    Validate {
        name: String,
    },
    /// Remove policy configuration; existing job history and archive files are preserved.
    Remove {
        name: String,
    },
}
#[derive(ClapArgs)]
pub struct Create {
    #[arg(long)]
    name: String,
    #[arg(long)]
    source: String,
    #[arg(long)]
    destination: String,
    #[arg(long, default_value = "public")]
    schema: String,
    #[arg(long)]
    table: String,
    #[arg(long)]
    time_column: String,
    #[arg(long)]
    older_than_days: i32,
    #[arg(long, default_value_t = 500)]
    batch_size: i32,
    #[arg(long, requires = "equals_value")]
    equals_column: Option<String>,
    #[arg(long, requires = "equals_column")]
    equals_value: Option<String>,
}
pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Create(a) => {
            let p = store::policy_create(
                paths,
                &a.name,
                &a.source,
                &a.destination,
                PolicyConfig {
                    schema: a.schema,
                    table: a.table,
                    time_column: a.time_column,
                    older_than_days: a.older_than_days,
                    batch_size: a.batch_size,
                    equals_column: a.equals_column,
                    equals_value: a.equals_value,
                },
            )?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&p)?);
            } else {
                crate::output::success(&format!(
                    "Policy '{}' saved. Run `coldctl policy validate {}`.",
                    p.name, p.name
                ));
            }
        }
        Command::List => {
            let policies = store::policy_list(paths)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&policies)?);
            } else if policies.is_empty() {
                println!("No policies configured.");
            } else {
                for p in policies {
                    println!(
                        "{}  {}  {}.{} -> {}",
                        text(&p.name),
                        text(&p.source),
                        text(&p.config.schema),
                        text(&p.config.table),
                        text(&p.destination)
                    );
                }
            }
        }
        Command::Show { name } => {
            let p = store::policy_show(paths, &name)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&p)?);
            } else {
                println!(
                    "Policy       {}\nSource       {}\nTable        {}.{}\nTime column  {}\nOlder than   {} days\nDestination  {}\nBatch rows   {}\nDelete       NO",
                    text(&p.name),
                    text(&p.source),
                    text(&p.config.schema),
                    text(&p.config.table),
                    text(&p.config.time_column),
                    p.config.older_than_days,
                    text(&p.destination),
                    p.config.batch_size
                );
                if let (Some(column), Some(value)) =
                    (&p.config.equals_column, &p.config.equals_value)
                {
                    println!("Equality     {} = {}", text(column), text(value));
                }
            }
        }
        Command::Validate { name } => super::archive::print_plan(
            &coldctl_core::archive::planner::plan(paths, &name).await?,
            args.output,
        )?,
        Command::Remove { name } => {
            store::policy_remove(paths, &name)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::json!({"removed":name}));
            } else {
                crate::output::success("Policy removed; job history and archives preserved.");
            }
        }
    }
    Ok(())
}
