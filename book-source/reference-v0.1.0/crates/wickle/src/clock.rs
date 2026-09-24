use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{ContractError, ErrorCode, Id, PortFuture};

/// One wall-clock and monotonic reading from a Host-injected clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockReading {
    /// Current UTC milliseconds for admission and cross-process recovery.
    pub utc_ms: i64,
    /// Milliseconds on this clock instance's monotonic time axis.
    pub monotonic_ms: u64,
}

/// Time source and timer, injectable independently of storage and execution.
pub trait Clock: Send + Sync {
    /// Read local UTC and monotonic time without external I/O. Monotonic values
    /// must never regress; concurrent calls must produce consistent readings.
    fn now(&self) -> Result<ClockReading, ContractError>;
    /// Wait until an absolute point on this clock's monotonic time axis.
    fn sleep_until<'a>(&'a self, monotonic_ms: u64) -> PortFuture<'a, ()>;
}

/// UTC system time and a Tokio-compatible monotonic clock.
pub struct SystemClock {
    origin: tokio::time::Instant,
}

impl SystemClock {
    /// Create a clock without creating an asynchronous runtime.
    pub fn new() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Result<ClockReading, ContractError> {
        let utc_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
            .ok_or_else(|| ContractError::new(ErrorCode::ClockUnavailable, "clock.utc"))?;
        let monotonic_ms = u64::try_from(self.origin.elapsed().as_millis())
            .map_err(|_| ContractError::new(ErrorCode::ClockUnavailable, "clock.monotonic"))?;
        Ok(ClockReading {
            utc_ms,
            monotonic_ms,
        })
    }

    fn sleep_until<'a>(&'a self, monotonic_ms: u64) -> PortFuture<'a, ()> {
        Box::pin(async move {
            if tokio::runtime::Handle::try_current().is_err() {
                return Err(ContractError::new(
                    ErrorCode::RuntimeUnavailable,
                    "clock.timer",
                ));
            }
            let deadline = self
                .origin
                .checked_add(Duration::from_millis(monotonic_ms))
                .ok_or_else(|| ContractError::new(ErrorCode::ClockUnavailable, "clock.timer"))?;
            tokio::time::sleep_until(deadline).await;
            Ok(())
        })
    }
}

/// Source of new internal run, attempt, or event identities.
/// It never supplies missing business foreign keys or tool system-input values.
pub trait IdSource: Send + Sync {
    /// Generate an opaque identifier; the store still rejects identity collisions.
    fn next_id(&self) -> Result<Id, ContractError>;
}

/// Internal identifiers made from 128 bits of operating-system randomness.
#[derive(Debug, Clone, Copy, Default)]
pub struct RandomIdSource;

impl IdSource for RandomIdSource {
    fn next_id(&self) -> Result<Id, ContractError> {
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes)
            .map_err(|_| ContractError::new(ErrorCode::IdGenerationFailed, "identifier"))?;
        Id::new(format!("{:032x}", u128::from_be_bytes(bytes)))
    }
}
