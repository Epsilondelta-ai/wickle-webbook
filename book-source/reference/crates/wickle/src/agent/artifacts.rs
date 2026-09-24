use super::*;

pub(super) fn produced(snapshot: &RunSnapshot) -> Vec<ArtifactRef> {
    let mut references = vec![];
    for entry in &snapshot.tool_ledger {
        if let ToolCallState::Settled { result } = &entry.state {
            for content in &result.content {
                if let InputContent::Artifact { reference } = content {
                    if !references.contains(reference) {
                        references.push(reference.clone());
                    }
                }
            }
        }
    }
    references
}
/// Select only typed Tool artifacts actually present in this model projection.
pub(super) fn selected(
    request: &ModelRequest,
    saved: &StoredRun,
    extra: &[ArtifactRef],
) -> Result<Vec<ArtifactRef>, ContractError> {
    crate::prepared_step::projected_artifacts(
        request,
        &saved.messages,
        extra,
        &saved.snapshot.scope,
    )
}
