//! Read-only composition reports derived from stored evidence.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, num::NonZeroU64};

/// Identify persisted preparation without supplying runtime objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum StepRef {
    /// Exact logical input revision and purpose.
    Logical {
        /// Logical model step.
        model_step_id: Id,
        /// Agent, verification, or compaction.
        purpose: ModelPurpose,
        /// Frozen projection revision.
        projection_revision: NonZeroU64,
    },
    /// A preparation record belonging to this Run.
    Prepared {
        /// Record ID, resolved against the Run's saved reference list.
        record_id: Id,
    },
}
/// Display options, never execution or credential-disclosure permission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionOptions {
    /// Request source fragment content; each item still needs current permission.
    #[serde(default)]
    pub include_context_content: bool,
}
/// Exact saved content set covered by the final diagnostic authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectionFragmentRef {
    /// Scoped producer and native fragment identity.
    pub identity: FragmentIdentity,
    /// Exact core observation.
    pub core_revision: NonZeroU64,
    /// Original content identity.
    pub content_digest: JsonDigest,
}
/// Whether persisted evidence was available, without recreating it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionStatus {
    /// Selected preparation and its required records were available.
    Found,
    /// Some referenced evidence is absent or explicitly expired.
    Partial,
    /// No matching preparation or record exists.
    NotFound,
    /// The store explicitly classified the selected record as expired.
    Expired,
}
/// Why one report field cannot be populated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnresolvedInspectionField {
    /// Safe field/category name, not a storage error payload.
    pub field: String,
    /// Missing record identity when known.
    pub record_id: Option<Id>,
    /// `not_found`, `expired`, or `not_recorded`.
    pub reason: String,
}
/// Facts a stored record supports; none asserts business success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionEvidence {
    /// A model input was prepared and saved.
    Prepared,
    /// A physical call budget was reserved, not proof of transmission.
    DispatchReserved,
    /// A response record was observed and saved.
    ResponseObserved,
    /// No saved response proves whether transmission occurred.
    TransmissionUnknown,
}
/// Public report, with protected runtime inputs excluded by construction.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompositionReport {
    /// Stable report encoding.
    pub schema_version: &'static str,
    /// Authorized Run identity.
    pub run_id: Id,
    /// Original selector.
    pub step: StepRef,
    /// Lookup/retention result.
    pub status: InspectionStatus,
    /// Available saved preparation, never a newly generated projection.
    pub composition: Option<StepComposition>,
    /// Explicit gaps; unknown values are not guessed.
    pub unresolved: Vec<UnresolvedInspectionField>,
}
/// Composition of one exact prepared input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StepComposition {
    /// Selected preparation record identity.
    pub prepared_record_id: Id,
    /// Logical model step.
    pub model_step_id: Id,
    /// Invocation purpose.
    pub purpose: ModelPurpose,
    /// Exact frozen revision.
    pub projection_revision: NonZeroU64,
    /// Original unredacted execution fingerprint; never recomputed from this DTO.
    pub fingerprint: JsonDigest,
    /// Pinned assembler identity.
    pub assembler: VersionedRef,
    /// Saved reason for this projection revision.
    pub change_reason: Id,
    /// Preparation evidence, independent of physical transmission.
    pub evidence: Vec<InspectionEvidence>,
    /// Safe model and option metadata.
    pub model: Option<ModelComposition>,
    /// Model-owned schemas; hidden execution contracts are excluded.
    pub tools: Vec<ToolComposition>,
    /// Fragments in their original saved order.
    pub fragments: Vec<FragmentComposition>,
    /// Context selection evidence, distinct from display redaction.
    pub selection: Option<SelectionComposition>,
    /// Physical reservations and observed response facts.
    pub attempts: Vec<AttemptComposition>,
    /// Saved input estimate, not actual provider usage.
    pub estimated_input_tokens: Option<u64>,
    /// Estimator identity, if it was recorded; older preparations have none.
    pub estimator: Option<VersionedRef>,
    /// Immutable Run limits.
    pub run_limits: RunLimits,
    /// Current saved Run status; inspection does not decide an outcome.
    pub recorded_run_status: RunStatus,
    /// Snapshot revision at which this Run status was read.
    pub recorded_run_revision: u64,
    /// Saved Run outcome metadata, without business output or inferred success.
    pub recorded_run_outcome: Option<OutcomeComposition>,
    /// Paths hidden only for display, not removed from the saved projection.
    pub redacted_paths: Vec<String>,
}
/// Authoritative Run outcome metadata associated with the inspected snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutcomeComposition {
    /// Saved status, not an inspector verdict.
    pub status: RunStatus,
    /// Exact settlement revision.
    pub checkpoint_revision: u64,
    /// Present only when the saved Run succeeded; turn-ended is not verified business success.
    pub completion_basis: Option<CompletionBasis>,
}
/// Model metadata with connection references and target configuration omitted.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelComposition {
    /// Selected provider.
    pub provider: Id,
    /// Requested model/alias.
    pub requested_model: Id,
    /// Resolved model name.
    pub model_id: Id,
    /// Opaque model release.
    pub model_version: Id,
    /// Whether the target is pinned or mutable.
    pub version_semantics: VersionSemantics,
    /// Adapter implementation identity.
    pub adapter: VersionedRef,
    /// Operation and API version.
    pub api_contract: ApiContract,
    /// Capability metadata revision.
    pub capability_revision: Id,
    /// Original route identity, without exposing its protected fields.
    pub route_digest: JsonDigest,
    /// Requested/effective inference options and origins, with sensitive fields redacted.
    pub configuration: Option<ModelConfiguration>,
}
/// Safe schema and compiler evidence for an advertised Tool.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ToolComposition {
    /// Exact catalog Tool version.
    pub tool: VersionedRef,
    /// Model-facing canonical name.
    pub canonical_name: Id,
    /// Canonical model-only schema, with display redaction.
    pub canonical_schema: Value,
    /// Original canonical schema identity.
    pub canonical_schema_digest: JsonDigest,
    /// Saved provider name, when its compilation record is available.
    pub provider_name: Option<Id>,
    /// Provider model-only schema, with display redaction.
    pub provider_schema: Option<Value>,
    /// Pinned compiler identity.
    pub compiler: Option<VersionedRef>,
    /// Saved compilation identity.
    pub compiled_digest: Option<JsonDigest>,
    /// Identity of the saved argument decoding plan; not execution arguments.
    pub decode_plan_digest: Option<JsonDigest>,
    /// Constraint explanation metadata; text can contain schema annotations and is omitted.
    pub constraints: Vec<ConstraintComposition>,
    /// Saved native/context/core enforcement classifications.
    pub enforcement: Vec<ToolConstraintEnforcement>,
}
/// Source and identity of trusted compiler-generated constraint text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConstraintComposition {
    /// Fragment identity.
    pub fragment_id: Id,
    /// Original explanation digest.
    pub digest: JsonDigest,
    /// Compiler that generated the text.
    pub source: VersionedRef,
}
/// Display disclosure, independent of inclusion in model input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContextDisclosure {
    /// Explicitly authorized content, bounded to a finite report size.
    Included {
        /// Source context only; never model messages or system execution inputs.
        content: Vec<InputContent>,
    },
    /// Content withheld from the display.
    Redacted {
        /// `not_requested`, `access_denied`, `approval_required`, or `display_limit`.
        reason: String,
    },
    /// Selection/tombstone metadata has no source text to disclose.
    NotApplicable,
}
/// Stable observation metadata; source revision can legitimately be unknown.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FragmentComposition {
    /// Producer identity; scope and Host execution values are not copied here.
    pub producer_id: Id,
    /// Native fragment identity.
    pub fragment_id: Id,
    /// Core-issued revision.
    pub core_revision: NonZeroU64,
    /// Original observation digest.
    pub content_digest: JsonDigest,
    /// Opaque external revision, if known.
    pub source_revision: Option<Id>,
    /// Origin classification.
    pub origin: ContextOrigin,
    /// Zero-based order in the saved preparation.
    pub order: usize,
    /// `included`, `excluded`, `tombstone`, `selection`, or `unresolved`.
    pub selection: String,
    /// Saved reason, or `not_recorded` when the preparation lacks the detail.
    pub selection_reason: String,
    /// Separately authorized display data.
    pub disclosure: ContextDisclosure,
}
/// Selection lists refer to saved inputs, not redaction decisions.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SelectionComposition {
    /// Transcript boundary used for preparation.
    pub through_sequence: u64,
    /// Selected original messages, without contents or opaque continuations.
    pub included_messages: Vec<Id>,
    /// Dropped messages; historical records do not record individual reasons.
    pub excluded_messages: Vec<Id>,
    /// Selected context item identities.
    pub included_context: Vec<Id>,
    /// Dropped context identities.
    pub excluded_context: Vec<Id>,
}
/// Reservation/response evidence for a physical model attempt.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AttemptComposition {
    /// Physical attempt identity.
    pub attempt_id: Id,
    /// Saved ledger state.
    pub state: ModelAttemptState,
    /// A response or local collector-failure record was saved; not proof of remote response.
    pub result_recorded: bool,
    /// Reservation alone always leaves transmission unconfirmed.
    pub evidence: Vec<InspectionEvidence>,
    /// Model identity actually reported, never filled from the route.
    pub reported_model_id: Option<Id>,
    /// Version actually reported.
    pub reported_model_version: Option<Id>,
    /// Actual reported usage, if observed.
    pub usage: Option<ModelUsage>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SavedToolInspection {
    pub tool: VersionedRef,
    pub canonical_name: Id,
    pub canonical_schema_digest: JsonDigest,
    pub compiler: VersionedRef,
    pub target: ProviderToolTarget,
    pub wire_tool: ModelTool,
    pub decode_plan_digest: JsonDigest,
    pub digest: JsonDigest,
    pub fragments: Vec<ToolConstraintFragment>,
    pub enforcement: Vec<ToolConstraintEnforcement>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredInspection {
    pub reference: RecordRef,
    pub root: PreparedStepRecord,
    pub route: Option<ResolvedModelRoute>,
    pub configuration: Option<ModelConfiguration>,
    pub tools: Vec<(PinnedPromptTool, Option<SavedToolInspection>)>,
    pub projection: Option<PreparedModelProjection>,
    pub attempts: Vec<(ModelInvocationRecord, bool)>,
    pub disclosure: BTreeMap<String, ContextDisclosure>,
    pub limits: RunLimits,
    pub status: RunStatus,
    pub revision: u64,
    pub outcome: Option<RunOutcome>,
    pub unresolved: Vec<UnresolvedInspectionField>,
}

/// Pure conversion: no ports, clocks, policy callbacks or mutable runtime objects.
pub(crate) fn compose(step: StepRef, saved: StoredInspection) -> CompositionReport {
    let mut redacted_paths = vec![];
    let model = saved.route.map(|route| {
        let configuration = saved.configuration.map(|mut config| {
            for (name, options) in [
                ("requested", &mut config.requested),
                ("effective", &mut config.effective),
            ] {
                for (key, value) in options.iter_mut() {
                    let path = format!("model.configuration.{name}.{key}");
                    if sensitive(key) {
                        *value = Value::Null;
                        redacted_paths.push(path);
                    } else {
                        scrub(value, &path, &mut redacted_paths);
                    }
                }
            }
            config
        });
        ModelComposition {
            route_digest: route.digest(),
            provider: route.provider,
            requested_model: route.requested_model,
            model_id: route.model_id,
            model_version: route.model_version,
            version_semantics: route.version_semantics,
            adapter: route.adapter,
            api_contract: route.api_contract,
            capability_revision: route.capability_revision,
            configuration,
        }
    });
    let tools = saved
        .tools
        .into_iter()
        .enumerate()
        .map(|(index, (manifest, compiled))| {
            let mut canonical_schema = manifest.model_tool.model_input_schema;
            scrub(
                &mut canonical_schema,
                &format!("tools.{index}.canonical_schema"),
                &mut redacted_paths,
            );
            let mut report = ToolComposition {
                tool: manifest.tool,
                canonical_name: manifest.model_tool.name,
                canonical_schema,
                canonical_schema_digest: manifest.model_schema_digest,
                provider_name: None,
                provider_schema: None,
                compiler: None,
                compiled_digest: None,
                decode_plan_digest: None,
                constraints: vec![],
                enforcement: vec![],
            };
            if let Some(compiled) = compiled {
                let mut schema = compiled.wire_tool.model_input_schema;
                scrub(
                    &mut schema,
                    &format!("tools.{index}.provider_schema"),
                    &mut redacted_paths,
                );
                report.provider_name = Some(compiled.wire_tool.name);
                report.provider_schema = Some(schema);
                report.constraints = compiled
                    .fragments
                    .into_iter()
                    .map(|fragment| ConstraintComposition {
                        fragment_id: fragment.id,
                        digest: fragment.digest,
                        source: compiled.compiler.clone(),
                    })
                    .collect();
                report.compiler = Some(compiled.compiler);
                report.compiled_digest = Some(compiled.digest);
                report.decode_plan_digest = Some(compiled.decode_plan_digest);
                report.enforcement = compiled.enforcement;
            }
            report
        })
        .collect();
    let fragments = saved
        .projection
        .as_ref()
        .map(|projection| {
            projection
                .provenance
                .fragments
                .iter()
                .enumerate()
                .map(|(order, fragment)| {
                    let (selection, reason) = match &fragment.value {
                        FragmentValue::Item { item }
                            if projection
                                .provenance
                                .selected_context_ids
                                .contains(&item.item_id) =>
                        {
                            ("included".into(), "selected".into())
                        }
                        FragmentValue::Item { item }
                            if projection
                                .provenance
                                .dropped_context_ids
                                .contains(&item.item_id) =>
                        {
                            ("excluded".into(), "not_recorded".into())
                        }
                        FragmentValue::Item { .. } => ("unresolved".into(), "not_recorded".into()),
                        FragmentValue::Tombstone { reason } => {
                            ("tombstone".into(), reason.to_string())
                        }
                        FragmentValue::Selection { status, .. } => {
                            ("selection".into(), status.to_string())
                        }
                    };
                    FragmentComposition {
                        producer_id: fragment.identity.producer_id.clone(),
                        fragment_id: fragment.identity.fragment_id.clone(),
                        core_revision: fragment.core_revision,
                        content_digest: fragment.content_digest.clone(),
                        source_revision: fragment.source_revision.clone(),
                        origin: fragment.origin,
                        order,
                        selection,
                        selection_reason: reason,
                        disclosure: saved
                            .disclosure
                            .get(&disclosure_key(fragment))
                            .cloned()
                            .unwrap_or_else(|| ContextDisclosure::Redacted {
                                reason: "not_requested".into(),
                            }),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let selection = saved
        .projection
        .as_ref()
        .map(|projection| SelectionComposition {
            through_sequence: projection.provenance.through_sequence,
            included_messages: projection.provenance.selected_message_ids.clone(),
            excluded_messages: projection.provenance.dropped_message_ids.clone(),
            included_context: projection.provenance.selected_context_ids.clone(),
            excluded_context: projection.provenance.dropped_context_ids.clone(),
        });
    let attempts = saved
        .attempts
        .into_iter()
        .map(|(attempt, response_observed)| AttemptComposition {
            evidence: vec![
                InspectionEvidence::DispatchReserved,
                if response_observed {
                    InspectionEvidence::ResponseObserved
                } else {
                    InspectionEvidence::TransmissionUnknown
                },
            ],
            result_recorded: attempt.response_ref.is_some(),
            attempt_id: attempt.attempt_id,
            state: attempt.state,
            reported_model_id: attempt.reported_model_id,
            reported_model_version: attempt.reported_model_version,
            usage: attempt.usage,
        })
        .collect();
    let status = if saved
        .unresolved
        .iter()
        .any(|field| field.reason != "not_recorded")
    {
        InspectionStatus::Partial
    } else {
        InspectionStatus::Found
    };
    CompositionReport {
        schema_version: "wickle.composition-report.v1",
        run_id: saved.root.run_id.clone(),
        step,
        status,
        composition: Some(StepComposition {
            prepared_record_id: saved.reference.record_id,
            model_step_id: saved.root.model_step_id,
            purpose: saved.root.purpose,
            projection_revision: saved.root.projection_revision,
            fingerprint: saved.root.projection_fingerprint,
            assembler: saved.root.assembler,
            change_reason: saved.root.change_reason,
            evidence: vec![InspectionEvidence::Prepared],
            model,
            tools,
            fragments,
            selection,
            attempts,
            estimated_input_tokens: saved
                .projection
                .as_ref()
                .map(|projection| projection.input_tokens),
            estimator: None,
            run_limits: saved.limits,
            recorded_run_status: saved.status,
            recorded_run_revision: saved.revision,
            recorded_run_outcome: saved.outcome.map(|outcome| OutcomeComposition {
                status: outcome.result.status(),
                checkpoint_revision: outcome.checkpoint_revision,
                completion_basis: match outcome.result {
                    OutcomeResult::Succeeded { completion_basis } => Some(completion_basis),
                    _ => None,
                },
            }),
            redacted_paths,
        }),
        unresolved: saved.unresolved,
    }
}
fn sensitive(key: &str) -> bool {
    let normalized: String = key
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    [
        "password",
        "secret",
        "apikey",
        "credential",
        "authorization",
        "accesstoken",
        "refreshtoken",
        "connection",
        "endpoint",
        "executionargs",
        "systeminputs",
        "opaque",
    ]
    .iter()
    .any(|word| normalized.contains(word))
}
fn scrub(value: &mut Value, path: &str, redacted: &mut Vec<String>) {
    match value {
        Value::Object(values) => {
            for (key, value) in values {
                let child = format!("{path}.{key}");
                if sensitive(key)
                    || matches!(
                        key.as_str(),
                        "description" | "examples" | "default" | "$comment"
                    )
                {
                    *value = Value::Null;
                    redacted.push(child);
                } else {
                    scrub(value, &child, redacted);
                }
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                scrub(value, &format!("{path}.{index}"), redacted);
            }
        }
        _ => {}
    }
}

pub(crate) fn disclosure_key(fragment: &ContextFragment) -> String {
    format!(
        "{}:{}:{}",
        fragment.identity.key(),
        fragment.core_revision,
        fragment.content_digest
    )
}
