use crate::{
    ArtifactRef, BudgetUsage, ContractError, ExecutionContext, Guarded, Id, PolicyAction,
    PolicyGate, PolicyRequest, RunEvent, RunEventPayload, RunPhase, RunSnapshot, RunStatus,
};
use serde::Serialize;
use std::num::NonZeroU64;
use tokio::time::Instant;

/// Minimal run metadata. Protected records and outcome internals are not included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunView {
    /// Run identity.
    pub run_id: Id,
    /// Session identity.
    pub session_id: Id,
    /// Current status.
    pub status: RunStatus,
    /// Current phase.
    pub phase: RunPhase,
    /// Snapshot revision.
    pub revision: u64,
    /// Saved usage, not inferred provider consumption.
    pub usage: BudgetUsage,
}

/// Authorized artifact metadata without an embedded storage location or payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ArtifactView {
    /// Artifact identity.
    pub artifact_id: Id,
    /// Stored media type.
    pub media_type: Id,
    /// Original byte size.
    pub size_bytes: u64,
    /// Store-defined content hash.
    pub content_hash: Id,
}

/// Event metadata without protected record references from its payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventView {
    /// Event identity.
    pub event_id: Id,
    /// Owning run.
    pub run_id: Id,
    /// Owning session.
    pub session_id: Id,
    /// Durable sequence.
    pub seq: NonZeroU64,
    /// UTC milliseconds since Unix epoch.
    pub timestamp_ms: i64,
    /// Durable event kind.
    pub event_type: &'static str,
}

impl PolicyGate {
    /// Authorize using the stored run scope and select public fields explicitly.
    pub async fn run_view(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRun {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(RunView {
                run_id: snapshot.run_id.clone(),
                session_id: snapshot.request.session_id.clone(),
                status: snapshot.status,
                phase: snapshot.phase,
                revision: snapshot.revision,
                usage: snapshot.usage.clone(),
            })
        })
        .await
    }

    /// Read a full protected checkpoint only through the distinct details action.
    pub async fn run_details(
        &self,
        snapshot: &RunSnapshot,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<RunSnapshot>, ContractError> {
        let request = PolicyRequest {
            owner_scope: snapshot.scope.clone(),
            resource_id: snapshot.run_id.clone(),
            action: PolicyAction::ReadRunDetails {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(snapshot.clone())
        })
        .await
    }

    /// Authorize against authoritative artifact metadata before selecting its view.
    pub async fn artifact_view(
        &self,
        artifact: &ArtifactRef,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<ArtifactView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: artifact.scope.clone(),
            resource_id: artifact.artifact_id.clone(),
            action: PolicyAction::ReadArtifact {},
        };
        self.guard(&request, context, deadline, None, || async {
            Ok(ArtifactView {
                artifact_id: artifact.artifact_id.clone(),
                media_type: artifact.media_type.clone(),
                size_bytes: artifact.size_bytes,
                content_hash: artifact.content_hash.clone(),
            })
        })
        .await
    }

    /// Authorize a stored event without publishing its protected payload references.
    pub async fn event_view(
        &self,
        event: &RunEvent,
        context: &ExecutionContext,
        deadline: Option<Instant>,
    ) -> Result<Guarded<EventView>, ContractError> {
        let request = PolicyRequest {
            owner_scope: event.scope.clone(),
            resource_id: event.run_id.clone(),
            action: PolicyAction::ReadEvents {},
        };
        self.guard(&request, context, deadline, None, || async {
            let event_type = match &event.payload {
                RunEventPayload::RunStarted { .. } => "run.started",
                RunEventPayload::ContextRewritten { .. } => "context.rewritten",
                RunEventPayload::ToolPlanned { .. } => "tool.planned",
                RunEventPayload::ToolSettled { .. } => "tool.settled",
                RunEventPayload::ToolReconciled { .. } => "tool.reconciled",
                RunEventPayload::ToolUnresolved { .. } => "tool.unresolved",
                RunEventPayload::VerificationCompleted { .. } => "verification.completed",
                RunEventPayload::RunWaiting { .. } => "run.waiting",
                RunEventPayload::RunResumed { .. } => "run.resumed",
                RunEventPayload::RunRecovered { .. } => "run.recovered",
                RunEventPayload::RunFinished { .. } => "run.finished",
                RunEventPayload::ModelRouteSelected { .. } => "model.route_selected",
            };
            Ok(EventView {
                event_id: event.event_id.clone(),
                run_id: event.run_id.clone(),
                session_id: event.session_id.clone(),
                seq: event.seq,
                timestamp_ms: event.timestamp_ms,
                event_type,
            })
        })
        .await
    }
}
