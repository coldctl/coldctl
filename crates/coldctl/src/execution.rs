use clap::Args;
use coldctl_core::archive::controls::{ExecutionControls, Shutdown};

#[derive(Args, Default)]
pub struct ExecutionArgs {
    /// Rows per invocation; 0 removes a saved limit.
    #[arg(long)]
    pub max_rows: Option<u64>,
    /// Seconds per invocation, checked at safe boundaries; 0 removes a saved limit.
    #[arg(long)]
    pub max_duration_seconds: Option<u64>,
    #[arg(long)]
    pub batch_delay_ms: Option<u64>,
    /// Keep this many bytes free before each destination write.
    #[arg(long)]
    pub min_free_bytes: Option<u64>,
    #[arg(long)]
    pub query_timeout_ms: Option<u32>,
    #[arg(long)]
    pub lock_timeout_ms: Option<u32>,
}
impl ExecutionArgs {
    pub fn apply(
        &self,
        mut value: ExecutionControls,
    ) -> Result<ExecutionControls, coldctl_core::error::Error> {
        if let Some(n) = self.max_rows {
            value.max_rows = (n != 0).then_some(n);
        }
        if let Some(n) = self.max_duration_seconds {
            value.max_duration_seconds = (n != 0).then_some(n);
        }
        if let Some(n) = self.batch_delay_ms {
            value.batch_delay_ms = n;
        }
        if let Some(n) = self.min_free_bytes {
            value.minimum_free_bytes = n;
        }
        if let Some(n) = self.query_timeout_ms {
            value.query_timeout_ms = n;
        }
        if let Some(n) = self.lock_timeout_ms {
            value.lock_timeout_ms = n;
        }
        value.validate()?;
        Ok(value)
    }
}
pub struct Signals {
    pub shutdown: Shutdown,
    task: tokio::task::JoinHandle<()>,
}
impl Signals {
    pub fn install() -> Result<Self, std::io::Error> {
        let shutdown = Shutdown::default();
        let request = shutdown.clone();
        #[cfg(unix)]
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        #[cfg(unix)]
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
        #[cfg(windows)]
        let mut interrupt = tokio::signal::windows::ctrl_c()?;
        #[cfg(windows)]
        let mut terminate = tokio::signal::windows::ctrl_break()?;
        let task = tokio::spawn(async move {
            tokio::select! { _=interrupt.recv()=>{}, _=terminate.recv()=>{} }
            request.request();
            eprintln!("Shutdown requested; stopping at the next safe batch boundary.");
        });
        Ok(Self { shutdown, task })
    }
}
impl Drop for Signals {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overrides_preserve_unspecified_controls_and_zero_clears_budgets() {
        let saved = ExecutionControls {
            max_rows: Some(12),
            max_duration_seconds: Some(30),
            batch_delay_ms: 123,
            ..Default::default()
        };
        let args = ExecutionArgs {
            max_rows: Some(0),
            ..Default::default()
        };
        let updated = args.apply(saved).unwrap();
        assert_eq!(updated.max_rows, None);
        assert_eq!(updated.max_duration_seconds, Some(30));
        assert_eq!(updated.batch_delay_ms, 123);
    }
    #[test]
    fn invalid_controls_are_rejected_before_execution() {
        assert!(
            ExecutionArgs {
                min_free_bytes: Some(0),
                ..Default::default()
            }
            .apply(Default::default())
            .is_err()
        );
        assert!(
            ExecutionArgs {
                query_timeout_ms: Some(1),
                lock_timeout_ms: Some(2),
                ..Default::default()
            }
            .apply(Default::default())
            .is_err()
        );
        assert!(
            ExecutionArgs {
                batch_delay_ms: Some(3_600_001),
                ..Default::default()
            }
            .apply(Default::default())
            .is_err()
        );
    }
}
