use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use clap::{Args, Subcommand};
use coldctl_core::{
    monitoring::{AlertConfig, Monitor, now},
    monitoring_agent,
    paths::StatePaths,
};
use std::{
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::Duration,
};
use subtle::ConstantTimeEq;

#[derive(Args)]
pub struct MonitorArgs {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Serve Prometheus metrics and durable failure history independently of the worker.
    Serve {
        #[arg(long, default_value = "127.0.0.1:9464")]
        listen: SocketAddr,
        /// Bearer token environment reference; required for non-loopback listeners.
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long, default_value_t=10, value_parser=clap::value_parser!(u64).range(1..=300))]
        poll_seconds: u64,
        /// Environment variable containing the HTTPS alert receiver URL.
        #[arg(long)]
        webhook_url_env: Option<String>,
        #[arg(long, requires = "webhook_url_env")]
        webhook_token_env: Option<String>,
        /// Allow only numeric loopback HTTP for webhook test fixtures.
        #[arg(long)]
        allow_loopback_http: bool,
    },
    /// Show up to 100 persisted incident transitions and alert delivery results as JSON.
    Status,
}
struct App {
    monitor: Monitor,
    token: Option<String>,
    sample: RwLock<(String, i64, bool)>,
    stale_after: i64,
}
fn authorized(app: &App, headers: &HeaderMap) -> bool {
    match &app.token {
        None => true,
        Some(token) => headers
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .is_some_and(|s| bool::from(s.as_bytes().ct_eq(token.as_bytes()))),
    }
}
async fn metrics(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let sample = app.sample.read().unwrap();
    let fresh = sample.2 && now() - sample.1 <= app.stale_after;
    let Ok(durable) = app.monitor.metrics() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    ([(header::CONTENT_TYPE,"text/plain; version=0.0.4; charset=utf-8"),(header::CACHE_CONTROL,"no-store")],format!("# TYPE coldctl_agent_monitor_up gauge\ncoldctl_agent_monitor_up {}\n# TYPE coldctl_agent_last_sample_timestamp_seconds gauge\ncoldctl_agent_last_sample_timestamp_seconds {}\n{}{}",i32::from(fresh),sample.1,sample.0,durable)).into_response()
}
async fn failures(State(app): State<Arc<App>>, headers: HeaderMap) -> Response {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match app.monitor.history() {
        Ok(v) => ([(header::CACHE_CONTROL, "no-store")], axum::Json(v)).into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
async fn ready(State(app): State<Arc<App>>, headers: HeaderMap) -> StatusCode {
    if !authorized(&app, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let sample = app.sample.read().unwrap();
    if sample.2 && now() - sample.1 <= app.stale_after {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
pub async fn run(args: MonitorArgs, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    inner(args, paths).await.map_err(|e| e.into())
}
async fn inner(args: MonitorArgs, paths: &StatePaths) -> anyhow::Result<()> {
    anyhow::ensure!(
        coldctl_core::state::status(paths)?.is_some(),
        "run coldctl init before monitoring"
    );
    let database = paths.data_dir.join("monitoring.sqlite");
    if matches!(args.command, Command::Status) {
        anyhow::ensure!(
            database.exists(),
            "monitoring history is not initialized; run agent monitor serve first"
        );
        println!(
            "{}",
            serde_json::to_string_pretty(
                &Monitor::open(&database, "agent", AlertConfig::default())?.history()?
            )?
        );
        return Ok(());
    }
    let Command::Serve {
        listen,
        token_env,
        poll_seconds,
        webhook_url_env,
        webhook_token_env,
        allow_loopback_http,
    } = args.command
    else {
        unreachable!()
    };
    let token = token_env
        .map(|n| {
            std::env::var(n)
                .ok()
                .filter(|s| s.len() >= 32 && s.len() <= 4096 && !s.contains(['\r', '\n']))
                .ok_or_else(|| anyhow::anyhow!("monitor bearer token must contain 32..4096 bytes"))
        })
        .transpose()?;
    anyhow::ensure!(
        listen.ip().is_loopback() || token.is_some(),
        "non-loopback monitoring listeners require --token-env and TLS at ingress"
    );
    let monitor = Monitor::open(
        &database,
        "agent",
        AlertConfig {
            webhook_url_env,
            webhook_token_env,
            allow_loopback_http,
        },
    )?;
    monitor.alerts.check()?;
    // One monitor process per state directory; all monitoring writes use their own DB.
    coldctl_core::monitoring::validate_path(&paths.data_dir.join("monitoring.lock"))?;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(paths.data_dir.join("monitoring.lock"))?;
    coldctl_core::monitoring_agent::lock(&lock)?;
    let initial = monitoring_agent::sample(paths, &monitor)?;
    let app = Arc::new(App {
        monitor,
        token,
        sample: RwLock::new((initial, now(), true)),
        stale_after: (poll_seconds * 3).max(10) as i64,
    });
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let copy = app.clone();
    let paths = paths.clone();
    let sampler = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(poll_seconds)).await;
            let paths = paths.clone();
            let monitor = copy.monitor.clone();
            let result =
                tokio::task::spawn_blocking(move || monitoring_agent::sample(&paths, &monitor))
                    .await;
            let ok = matches!(&result, Ok(Ok(_)));
            let recorded = copy.monitor.observe("state_read", "service", !ok).is_ok();
            let mut latest = copy.sample.write().unwrap();
            match result {
                Ok(Ok(text)) => *latest = (text, now(), recorded),
                _ => latest.2 = false,
            }
        }
    });
    let dispatcher = app.monitor.clone();
    let deliveries = tokio::spawn(async move {
        loop {
            if dispatcher.deliver_one().await.is_err() {
                eprintln!("monitoring alert state unavailable; pending delivery will retry");
            }
            if dispatcher.prune().is_err() {
                eprintln!("monitoring retention cleanup failed");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    println!("Monitoring listening on {listen}; /metrics, /failures, /ready");
    let result = axum::serve(
        listener,
        Router::new()
            .route("/metrics", get(metrics))
            .route("/failures", get(failures))
            .route("/ready", get(ready))
            .with_state(app),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    sampler.abort();
    deliveries.abort();
    drop(lock);
    result?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn endpoints_require_token_and_stale_samples_are_not_ready() {
        let temp = tempfile::tempdir().unwrap();
        let app = Arc::new(App {
            monitor: Monitor::open(
                &temp.path().join("monitor.sqlite"),
                "agent",
                Default::default(),
            )
            .unwrap(),
            token: Some("fixture-token-at-least-32-characters".into()),
            sample: RwLock::new((String::new(), now() - 60, true)),
            stale_after: 10,
        });
        assert_eq!(
            metrics(State(app.clone()), HeaderMap::new()).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            failures(State(app.clone()), HeaderMap::new())
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            "Bearer fixture-token-at-least-32-characters"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            ready(State(app.clone()), headers.clone()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let response = metrics(State(app.clone()), headers.clone()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 100_000)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&body).contains("coldctl_agent_monitor_up 0"));
        *app.sample.write().unwrap() = (String::new(), now(), true);
        assert_eq!(ready(State(app), headers).await, StatusCode::OK);
    }
}
