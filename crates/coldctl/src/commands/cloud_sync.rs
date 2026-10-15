use coldctl_core::{
    cloud_sync::{Store, lock},
    paths::StatePaths,
};
use std::time::Duration;

async fn attempt(
    paths: &StatePaths,
    url: &str,
    token_env: &str,
    local: bool,
) -> Result<(), String> {
    let token = std::env::var(token_env)
        .map_err(|_| "Cloud token environment variable is missing".to_string())?;
    if token.trim() != token
        || !token.starts_with("coldctl_")
        || token.len() > 256
        || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err("Invalid Cloud token format".into());
    }
    // Error strings from collection may contain local paths; persist only a fixed category.
    let pages = coldctl_cloud::paged::collect(paths, env!("CARGO_PKG_VERSION")).map_err(|_| {
        "Cannot collect bounded Cloud metadata pages; inspect local state and report limits"
            .to_string()
    })?;
    coldctl_cloud::paged::sync(url, &token, local, pages)
        .await
        .map_err(|e| e.to_string())
}

pub async fn run(
    paths: &StatePaths,
    url: &str,
    token_env: &str,
    local: bool,
    watch: bool,
    interval: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    coldctl_core::state::status(paths)?.ok_or("Run coldctl init first")?;
    let mut endpoint = coldctl_cloud::client::endpoint(url, local)?;
    endpoint.set_path("/");
    let _lock = lock(paths)?;
    let store = Store::open(paths)?;
    store.configure(endpoint.as_str())?;
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);
    if watch {
        println!(
            "Background Cloud sync enabled; press Ctrl+C to stop. Only lifecycle metadata is uploaded."
        );
    }
    loop {
        if watch {
            let delay = store.status()?.next_attempt.unwrap_or(0) - coldctl_core::monitoring::now();
            if delay > 0 {
                tokio::select! {
                    _ = &mut shutdown => { store.stopped()?; return Ok(()); }
                    _ = tokio::time::sleep(Duration::from_secs(delay.min(86400) as u64)) => {}
                }
            }
        }
        store.begin()?;
        // Cancellation never records success; a later run sends a fresh snapshot.
        let result = tokio::select! {
            _ = &mut shutdown => { store.stopped()?; return Ok(()); }
            result = attempt(paths,url,token_env,local) => result,
        };
        store.finish(result.as_ref().err().map(String::as_str), interval, watch)?;
        match result {
            Ok(()) => println!(
                "Lifecycle metadata synchronized. Database contents and credentials stayed local."
            ),
            Err(e) if !watch => return Err(e.into()),
            Err(e) => eprintln!("Cloud sync: {e}; retry scheduled (see agent sync-status)."),
        }
        if !watch {
            return Ok(());
        }
        // Register Ctrl+C before the next attempt, including a signal received during upload.
        tokio::select! {
            _ = &mut shutdown => { store.stopped()?; return Ok(()); }
            _ = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
    }
}
