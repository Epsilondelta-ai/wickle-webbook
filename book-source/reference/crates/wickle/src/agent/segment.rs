use super::*;

/// A read-only time anchor for preparing an atomic segment admission. No lease
/// or execution budget is acquired until the prepared transition is accepted.
pub(super) struct PreparationClock {
    clock: Arc<dyn Clock>,
    anchor: ClockReading,
    elapsed: u64,
    started_at_ms: i64,
    last_monotonic_ms: u64,
}
impl PreparationClock {
    pub(super) fn new(
        clock: Arc<dyn Clock>,
        snapshot: &RunSnapshot,
    ) -> Result<Self, ContractError> {
        let anchor = clock.now()?;
        let downtime = anchor
            .utc_ms
            .checked_sub(snapshot.timing.last_observed_at_ms)
            .filter(|value| *value >= 0)
            .ok_or_else(|| fail(ErrorCode::ClockRegression, "clock.resume"))?
            as u64;
        let elapsed = snapshot
            .usage
            .elapsed_ms
            .checked_add(downtime)
            .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        let last_monotonic_ms = anchor.monotonic_ms;
        Ok(Self {
            clock,
            anchor,
            elapsed,
            started_at_ms: snapshot.timing.started_at_ms,
            last_monotonic_ms,
        })
    }
    pub(super) fn now(&mut self) -> Result<(u64, i64), ContractError> {
        let reading = self.clock.now()?;
        if reading.monotonic_ms < self.last_monotonic_ms {
            return Err(fail(ErrorCode::ClockRegression, "clock.monotonic"));
        }
        self.last_monotonic_ms = reading.monotonic_ms;
        let elapsed = self
            .elapsed
            .checked_add(reading.monotonic_ms - self.anchor.monotonic_ms)
            .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        let now = i64::try_from(elapsed)
            .ok()
            .and_then(|value| self.started_at_ms.checked_add(value))
            .ok_or_else(|| fail(ErrorCode::ClockUnavailable, "clock.elapsed"))?;
        Ok((elapsed, now))
    }
}

impl Agent {
    pub(super) async fn execution_context(
        &self,
        run_id: &Id,
        caller: &ExecutionContext,
    ) -> Result<ExecutionContext, ContractError> {
        let bindings = &self.inner.bindings;
        let history = self
            .resume_read(
                caller,
                bindings.state.read_execution(&bindings.scope, run_id),
            )
            .await?;
        let mut data = caller.data.clone();
        data.principal_ref = history.execution_principal_ref;
        data.capability_grant_ref = history
            .execution_grant_ref
            .ok_or_else(|| fail(ErrorCode::ComponentUnavailable, "execution.legacy_grant"))?;
        data.system_inputs = None;
        Ok(ExecutionContext::new(data, caller.cancellation.clone()))
    }
}
