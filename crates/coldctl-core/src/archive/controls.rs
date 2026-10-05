use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExecutionControls {
    pub max_rows: Option<u64>,
    pub max_duration_seconds: Option<u64>,
    pub batch_delay_ms: u64,
    pub minimum_free_bytes: u64,
    pub query_timeout_ms: u32,
    pub lock_timeout_ms: u32,
}
impl Default for ExecutionControls {
    fn default() -> Self {
        Self {
            max_rows: None,
            max_duration_seconds: None,
            batch_delay_ms: 0,
            minimum_free_bytes: 1_073_741_824,
            query_timeout_ms: 10_000,
            lock_timeout_ms: 2_000,
        }
    }
}
impl ExecutionControls {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_rows == Some(0)
            || self.max_rows.is_some_and(|v| v > i64::MAX as u64)
            || self.max_duration_seconds == Some(0)
            || self.max_duration_seconds.is_some_and(|v| v > 31_536_000)
            || self.batch_delay_ms > 3_600_000
            || self.minimum_free_bytes == 0
            || !(1..=300_000).contains(&self.query_timeout_ms)
            || !(1..=300_000).contains(&self.lock_timeout_ms)
            || self.lock_timeout_ms > self.query_timeout_ms
        {
            return Err(Error::Archive(
                "invalid execution controls: positive limits, duration <= 1 year, delay <= 1 hour, and 1 <= lock timeout <= query timeout <= 300000 ms required",
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Default)]
pub struct Shutdown(Arc<AtomicBool>);
impl Shutdown {
    pub fn request(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn requested(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
