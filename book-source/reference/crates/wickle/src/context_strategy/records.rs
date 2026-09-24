use super::*;
use std::collections::BTreeSet;

pub(crate) struct Group {
    pub ids: Vec<Id>,
    pub tool_round: bool,
    pub protected: bool,
}
fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}
/// Complete groups are identified from core call IDs, not provider-local aliases.
pub(crate) fn groups(messages: &[Message]) -> Result<Vec<Group>, ContractError> {
    let corrected: BTreeSet<_> = messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResultCorrection {
                previous_message_id,
                ..
            } => Some(previous_message_id.clone()),
            _ => None,
        })
        .collect();
    let mut groups = vec![];
    let mut pending = BTreeSet::new();
    let mut current = vec![];
    let mut unsafe_effect = false;
    for message in messages.iter().filter(|message| visible(message)) {
        let calls: Vec<_> = message
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::ToolCall { call } = block {
                    Some(call)
                } else {
                    None
                }
            })
            .collect();
        let results: Vec<_> = message
            .content
            .iter()
            .filter_map(|block| {
                if let ContentBlock::ToolResult { result } = block {
                    Some(result)
                } else {
                    None
                }
            })
            .collect();
        if !calls.is_empty() {
            if !pending.is_empty() || message.role != MessageRole::Assistant {
                return Err(context_error(
                    ErrorCode::InvalidContext,
                    "context.incomplete_group",
                ));
            }
            current = vec![message.message_id.clone()];
            unsafe_effect = false;
            for call in calls {
                if !pending.insert(call.call_id.clone()) {
                    return Err(context_error(
                        ErrorCode::InvalidContext,
                        "context.duplicate_call",
                    ));
                }
            }
        } else if !results.is_empty() {
            if message.role != MessageRole::Tool || pending.is_empty() {
                return Err(context_error(
                    ErrorCode::InvalidContext,
                    "context.unpaired_result",
                ));
            }
            current.push(message.message_id.clone());
            for result in results {
                if !pending.remove(&result.call_id) {
                    return Err(context_error(
                        ErrorCode::InvalidContext,
                        "context.unpaired_result",
                    ));
                }
                unsafe_effect |= result.status == ToolResultStatus::Unknown
                    || result.effect == ToolEffect::Unknown
                    || corrected.contains(&message.message_id);
            }
            if pending.is_empty() {
                groups.push(Group {
                    ids: std::mem::take(&mut current),
                    tool_round: true,
                    protected: unsafe_effect,
                });
            }
        } else if message.role == MessageRole::Assistant {
            if !pending.is_empty() {
                return Err(context_error(
                    ErrorCode::InvalidContext,
                    "context.incomplete_group",
                ));
            }
            groups.push(Group {
                ids: vec![message.message_id.clone()],
                tool_round: false,
                protected: false,
            });
        } else if !pending.is_empty() && message.role == MessageRole::User {
            return Err(context_error(
                ErrorCode::InvalidContext,
                "context.interrupted_group",
            ));
        }
    }
    if !pending.is_empty() {
        groups.push(Group {
            ids: current,
            tool_round: true,
            protected: true,
        });
    }
    if let Some(last) = groups.iter_mut().rfind(|group| group.tool_round) {
        last.protected = true;
    }
    Ok(groups)
}
pub(crate) fn safe_message(message: &Message, scope: &Scope) -> Result<Value, ContractError> {
    let mut content = vec![];
    for block in &message.content {
        match block {
            ContentBlock::Content {content:item}=>content.push(crate::context_projection::safe_value(item,scope)?),
            ContentBlock::ToolCall {call}=>content.push(serde_json::json!({"type":"tool_call","name":call.tool_name,"arguments":call.model_inputs})),
            ContentBlock::ToolResult {result}=>content.push(serde_json::json!({"type":"tool_result","status":result.status,"effect":result.effect,"content":result.content.iter().map(|item|crate::context_projection::safe_value(item,scope)).collect::<Result<Vec<_>,_>>()?,"error":result.error.as_ref().map(|error|&error.code)})),
            _=>{},
        }
    }
    Ok(serde_json::json!({"message_id":message.message_id,"role":message.role,"content":content}))
}
pub(crate) fn segments(
    messages: &[Message],
    scope: &Scope,
) -> Result<Vec<ContextSegment>, ContractError> {
    groups(messages)?
        .into_iter()
        .filter(|group| !group.protected)
        .map(|group| {
            let content = messages
                .iter()
                .filter(|message| group.ids.contains(&message.message_id))
                .map(|message| safe_message(message, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ContextSegment {
                message_ids: group.ids,
                content: Value::Array(content),
            })
        })
        .collect()
}
pub(crate) fn validate_selection(messages: &[Message], ids: &[Id]) -> Result<(), ContractError> {
    let selected: BTreeSet<_> = ids.iter().collect();
    if selected.len() != ids.len() {
        return Err(context_error(
            ErrorCode::InvalidContextSelection,
            "context.duplicates",
        ));
    }
    let mut found = 0;
    for group in groups(messages)? {
        let count = group.ids.iter().filter(|id| selected.contains(id)).count();
        if count > 0 && (group.protected || count != group.ids.len()) {
            return Err(context_error(
                ErrorCode::InvalidContextSelection,
                "context.protected_or_partial",
            ));
        }
        found += count;
    }
    if found != selected.len() {
        return Err(context_error(
            ErrorCode::InvalidContextSelection,
            "context.unknown_message",
        ));
    }
    Ok(())
}
pub(crate) fn original_content<'a>(
    messages: &'a [Message],
    preview: &ContextPreview,
) -> Result<&'a InputContent, ContractError> {
    messages
        .iter()
        .find(|message| message.message_id == preview.message_id)
        .and_then(|message| {
            message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult { result } if result.call_id == preview.tool_call_id => {
                    result.content.get(preview.content_index)
                }
                _ => None,
            })
        })
        .ok_or_else(|| context_error(ErrorCode::InvalidContext, "context.preview_source"))
}
pub(crate) fn payload(content: &InputContent) -> Option<(Vec<u8>, &'static str)> {
    match content {
        InputContent::Text { text } => Some((text.as_bytes().to_vec(), "text/plain")),
        InputContent::Json { value } => Some((
            crate::serialization::canonical_json_bytes(value),
            "application/json",
        )),
        _ => None,
    }
}
pub(crate) fn covered_digest(messages: &[Message], ids: &[Id]) -> JsonDigest {
    crate::serialization::data_digest(
        &messages
            .iter()
            .filter(|message| ids.contains(&message.message_id))
            .collect::<Vec<_>>(),
    )
}
pub(crate) fn anchors(
    messages: &[Message],
    ids: &[Id],
    previews: &[ContextPreview],
) -> Vec<InputContent> {
    let mut anchors = vec![];
    for message in messages
        .iter()
        .filter(|message| ids.contains(&message.message_id))
    {
        for block in &message.content {
            let content: Vec<_> = match block {
                ContentBlock::Content { content } => vec![content],
                ContentBlock::ToolResult { result } => result.content.iter().collect(),
                _ => vec![],
            };
            for item in content {
                if matches!(
                    item,
                    InputContent::Artifact { .. } | InputContent::Evidence { .. }
                ) && !anchors.contains(item)
                {
                    anchors.push(item.clone());
                }
            }
        }
    }
    for preview in previews
        .iter()
        .filter(|preview| ids.contains(&preview.message_id))
    {
        let item = InputContent::Artifact {
            reference: preview.preview.reference.clone(),
        };
        if !anchors.contains(&item) {
            anchors.push(item);
        }
    }
    anchors
}
pub(crate) fn apply(
    messages: &[Message],
    revision: Option<&ContextRevision>,
) -> Result<Vec<Message>, ContractError> {
    let Some(revision) = revision else {
        return Ok(messages.to_vec());
    };
    transform(messages, &revision.covered_message_ids, &revision.previews)
}
pub(crate) fn transform(
    messages: &[Message],
    covered: &[Id],
    previews: &[ContextPreview],
) -> Result<Vec<Message>, ContractError> {
    let mut result = vec![];
    for original in messages
        .iter()
        .filter(|message| !covered.contains(&message.message_id))
    {
        let mut message = original.clone();
        for block in &mut message.content {
            if let ContentBlock::ToolResult { result } = block {
                let mut content = vec![];
                for (index, item) in result.content.iter().enumerate() {
                    if let Some(preview) = previews.iter().find(|preview| {
                        preview.message_id == message.message_id
                            && preview.tool_call_id == result.call_id
                            && preview.content_index == index
                    }) {
                        content.push(InputContent::Artifact {
                            reference: preview.preview.reference.clone(),
                        });
                        if let Some(text) = &preview.preview.text {
                            content.push(InputContent::Json {value:serde_json::json!({"kind":"artifact_preview","text":text,"truncated":preview.preview.truncated})});
                        }
                    } else {
                        content.push(item.clone());
                    }
                }
                result.content = content;
            }
        }
        result.push(message);
    }
    Ok(result)
}
impl ContextRevision {
    /// Original message identities represented by the cumulative summary.
    pub fn covered_message_ids(&self) -> &[Id] {
        &self.covered_message_ids
    }
    /// Exact source batches represented by the covered conversation.
    pub fn source_lineage(&self) -> &[ContextLineage] {
        &self.source_lineage
    }
    /// Stored complete summary, never substituted for System instructions.
    pub fn summary(&self) -> Option<&str> {
        self.summary.as_deref()
    }
    /// Immutable artifact previews selected for the model view.
    pub fn previews(&self) -> &[ContextPreview] {
        &self.previews
    }
    /// Context item for the summary and its mechanically preserved typed references.
    pub(crate) fn item(&self, plan: &ContextPlan) -> Result<Option<ContextItem>, ContractError> {
        let Some(summary) = &self.summary else {
            return Ok(None);
        };
        let mut content = vec![InputContent::Json {
            value: serde_json::json!({"kind":"conversation_summary","source_snapshot_sequence":self.through_sequence,"summary":summary}),
        }];
        content.extend(self.anchors.clone());
        Ok(Some(ContextItem::new(
            Id::new(format!(
                "summary-{}",
                canonical_digest(&serde_json::json!([
                    self.session_id,
                    self.covered_digest,
                    summary
                ]))
            ))?,
            ContextOrigin::Compaction,
            plan.strategy.strategy.clone(),
            self.scope.clone(),
            content,
            ContextLifetime::Session {
                session_id: self.session_id.clone(),
            },
            ContextPriority::Required,
        )))
    }
    /// Restore a protected revision against its exact plan and original session transcript.
    pub fn restore(
        record: &ProtectedRecord,
        plan: &ContextPlan,
        scope: &Scope,
        session_id: &Id,
        messages: &[Message],
    ) -> Result<Self, ContractError> {
        let revision: Self = serde_json::from_value(record.value().clone())
            .map_err(|_| context_error(ErrorCode::InvalidSnapshot, "context.revision"))?;
        if record.reference().digest != crate::serialization::data_digest(&revision)
            || !matches!(
                revision.schema_version.as_str(),
                "wickle.context-revision.v1" | "wickle.context-revision.v2"
            )
            || &revision.scope != scope
            || &revision.session_id != session_id
            || revision.plan_ref.digest != plan.digest()
            || plan.scope != revision.scope
            || revision.after_bytes >= revision.before_bytes
            || revision.after_tokens > revision.before_tokens
            || revision.before_bytes > plan.limits.max_prepared_bytes as u64
        {
            return Err(context_error(
                ErrorCode::InvalidSnapshot,
                "context.revision_identity",
            ));
        }
        let history: Vec<_> = messages
            .iter()
            .filter(|message| message.sequence.get() <= revision.through_sequence)
            .cloned()
            .collect();
        if history.last().map(|message| message.sequence.get()) != Some(revision.through_sequence) {
            return Err(context_error(
                ErrorCode::InvalidSnapshot,
                "context.through_sequence",
            ));
        }
        validate_selection(&history, &revision.covered_message_ids)?;
        if covered_digest(&history, &revision.covered_message_ids) != revision.covered_digest
            || anchors(&history, &revision.covered_message_ids, &revision.previews)
                != revision.anchors
        {
            return Err(context_error(
                ErrorCode::InvalidSnapshot,
                "context.covered_data",
            ));
        }
        if revision.summary.as_ref().is_some_and(|summary| {
            summary.trim().is_empty() || summary.len() > plan.limits.max_summary_bytes
        }) || !revision.covered_message_ids.is_empty() && revision.summary.is_none()
        {
            return Err(context_error(ErrorCode::InvalidSnapshot, "context.summary"));
        }
        validate_previews(&history, &revision.previews, scope)?;
        Ok(revision)
    }
}

pub(crate) fn validate_previews(
    history: &[Message],
    previews: &[ContextPreview],
    scope: &Scope,
) -> Result<(), ContractError> {
    let mut seen = BTreeSet::new();
    for preview in previews {
        let original = original_content(history, preview)?;
        let (bytes, media) = payload(original)
            .ok_or_else(|| context_error(ErrorCode::InvalidSnapshot, "context.preview_kind"))?;
        let text = preview
            .preview
            .text
            .as_ref()
            .ok_or_else(|| context_error(ErrorCode::InvalidSnapshot, "context.preview_text"))?;
        if !seen.insert((
            preview.message_id.clone(),
            preview.tool_call_id.clone(),
            preview.content_index,
        )) || preview.original_digest != crate::serialization::data_digest(original)
            || &preview.preview.reference.scope != scope
            || preview.preview.reference.media_type.as_str() != media
            || preview.preview.reference.size_bytes != bytes.len() as u64
            || preview.preview.reference.content_hash != crate::artifacts::content_hash(&bytes)?
            || !bytes.starts_with(text.as_bytes())
            || preview.preview.truncated != (text.len() < bytes.len())
        {
            return Err(context_error(ErrorCode::InvalidSnapshot, "context.preview"));
        }
    }
    Ok(())
}
