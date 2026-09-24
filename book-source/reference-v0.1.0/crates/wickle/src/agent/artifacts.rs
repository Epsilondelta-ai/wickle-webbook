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
    let mut known = vec![];
    for message in &saved.messages {
        for block in &message.content {
            let result = match block {
                ContentBlock::ToolResult { result }
                | ContentBlock::ToolResultCorrection { result, .. } => result,
                _ => continue,
            };
            for item in &result.content {
                if let InputContent::Artifact { reference } = item {
                    known.push(reference);
                }
            }
        }
    }
    known.extend(extra.iter());
    let mut selected: Vec<ArtifactRef> = vec![];
    for message in &request.messages {
        for content in &message.content {
            let ModelContent::ToolResult { content, .. } = content else {
                continue;
            };
            for item in content
                .get("content")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                if item.get("type").and_then(serde_json::Value::as_str) != Some("artifact") {
                    continue;
                }
                let reference=known.iter().find(|reference|item==&serde_json::json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,"size_bytes":reference.size_bytes,"content_hash":reference.content_hash})).ok_or_else(||fail(ErrorCode::InvalidArtifact,"agent.artifact_projection"))?;
                if reference.scope != saved.snapshot.scope {
                    return Err(fail(ErrorCode::AccessDenied, "agent.artifact_scope"));
                }
                if !selected.contains(*reference) {
                    selected.push((*reference).clone());
                }
            }
        }
    }
    Ok(selected)
}
