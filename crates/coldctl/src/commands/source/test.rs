use super::{Output, text};
use coldctl_core::{
    paths::StatePaths,
    source::{DataSource, postgres::PostgresSource},
    state::sources,
};

pub async fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = sources::show(paths, name)?;
    let info = PostgresSource::new(source.connection)
        .test_connection()
        .await?;
    match output {
        Output::Json => super::json(&serde_json::to_value(info)?)?,
        Output::Table => {
            crate::output::success(&format!("Connected to source '{name}'."));
            println!(
                "Database  {}\nUser      {}\nPostgreSQL {}",
                text(&info.database),
                text(&info.user),
                text(&info.server_version)
            );
        }
    }
    Ok(())
}
