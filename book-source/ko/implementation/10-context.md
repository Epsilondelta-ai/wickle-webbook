# 10장 전체 Rust 구현과 테스트

[강의로](../10-context.md) · [전체 변경 패치](../solutions/10-context.patch)

기준 `e8119800dc861ed6b612083ec4f8dac67e16cfac`. 아래는 이 단계에서 추가·변경된 Rust 파일의 완성본이다. 생략 기호나 TODO 골격이 아니다. 변경 위치는 패치에서, 파일 전체 문맥은 여기에서 확인한다. manifest·lockfile·삭제·이름 변경은 패치를 따른다.

## `crates/wickle/src/context_projection.rs`

```rust
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Value, json};

use crate::{
    AgentProfile, CompiledTool, ComponentKind, ContentBlock, ContractError, ErrorCode, Id,
    InputContent, Instructions, JsonDigest, Message, MessageOrigin, MessageRole, ModelContent,
    ModelMessage, ModelOutput, ModelPurpose, ModelRequest, ModelResponseLimits, ModelRole,
    ModelTool, OpaqueContinuation, RecordRef, ResolvedComponent, ResolvedModelRoute,
    ResolvedProfile, RunRequest, Scope, ToolBindingRef, ToolResultStatus, VersionedRef, Visibility,
    parse_json, serialization::data_digest,
};

/// Version of the session prefix and byte-bounded projection contract.
pub const CONTEXT_ASSEMBLER_VERSION: &str = "wickle.context-assembler.v1";

/// Instruction data already resolved and authorized by the Host; no loader is invoked here.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionAssetContent {
    /// Exact instruction asset selected by the profile.
    pub asset: VersionedRef,
    /// Complete text to pin; it is never silently truncated.
    pub text: String,
}

impl fmt::Debug for InstructionAssetContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstructionAssetContent")
            .field("asset", &self.asset)
            .finish_non_exhaustive()
    }
}

/// A trusted assembly's mapping from a selected profile reference to its compiled tool.
/// The later adapter factory must attest that an export actually supplies this descriptor.
#[derive(Debug, Clone)]
pub struct PromptToolBinding {
    /// Exact selected catalog reference or adapter export, including alias/configuration.
    pub selection: ToolBindingRef,
    /// Validated immutable input split; its full schema is not copied into the prefix.
    pub compiled: CompiledTool,
}

/// Initial skill listing metadata, deliberately separate from skill body loading.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    /// Exact selected skill identity and version.
    pub skill: VersionedRef,
    /// Short public listing name.
    pub name: String,
    /// Public purpose description, not an automatically executed instruction body.
    pub description: String,
    /// Trusted catalog manifest identity pinned with this listing.
    pub manifest_digest: JsonDigest,
}

impl fmt::Debug for SkillManifest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillManifest")
            .field("skill", &self.skill)
            .field("manifest_digest", &self.manifest_digest)
            .finish_non_exhaustive()
    }
}

/// Model-facing part of a tool pinned into the session prefix.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PinnedPromptTool {
    /// Exact profile selection, retaining alias and binding identity.
    pub selection: ToolBindingRef,
    /// Exact underlying tool descriptor identity.
    pub tool: VersionedRef,
    /// Compiler contract used for input projection.
    pub compiler_version: String,
    /// Full compiled input-contract digest, without its hidden schemas or values.
    pub compiled_digest: JsonDigest,
    /// Original descriptor identity used by stored core ToolCall records.
    pub descriptor_digest: JsonDigest,
    /// Identity of the derived model-input schema.
    pub model_schema_digest: JsonDigest,
    /// Only the model-visible tool schema and public description.
    pub model_tool: ModelTool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptData {
    assembler_version: String,
    scope: Scope,
    profile: AgentProfile,
    initial_resolution_digest: JsonDigest,
    pinned_components: Vec<ResolvedComponent>,
    host_instructions: Vec<String>,
    profile_asset: Option<InstructionAssetContent>,
    tools: Vec<PinnedPromptTool>,
    skills: Vec<SkillManifest>,
}

/// Owned session prefix. It can be serialized for protected storage but cannot be
/// deserialized without verifying a trusted expected digest, scope and profile.
/// Its digest equals the digest of the serialized value stored by ProtectedRecord.
#[derive(Clone)]
pub struct PromptSnapshot {
    data: PromptData,
    digest: JsonDigest,
}

impl Serialize for PromptSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.data.serialize(serializer)
    }
}
impl fmt::Debug for PromptSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PromptSnapshot")
            .field("digest", &self.digest)
            .field("tool_count", &self.data.tools.len())
            .field("skill_count", &self.data.skills.len())
            .finish_non_exhaustive()
    }
}

impl PromptSnapshot {
    /// Pin already-authorized assets in profile order. This does not create adapter
    /// factories, fetch instructions, or load skill bodies. Profile text cannot
    /// delete or replace the independently owned Host message. Actual instruction
    /// adherence within a provider's system channel still requires evaluation;
    /// execution permissions are enforced separately by PolicyGate.
    pub fn create(
        profile: &ResolvedProfile,
        host_instructions: Vec<String>,
        profile_asset: Option<InstructionAssetContent>,
        mut tools: Vec<PromptToolBinding>,
        mut skills: Vec<SkillManifest>,
    ) -> Result<Self, ContractError> {
        if tools.len() != profile.profile().tools.len()
            || skills.len() != profile.profile().skills.len()
        {
            return Err(invalid("prompt.selections"));
        }
        let mut pinned_tools = Vec::new();
        for selection in &profile.profile().tools {
            let index = tools
                .iter()
                .position(|binding| &binding.selection == selection)
                .ok_or_else(|| invalid("prompt.tools"))?;
            let binding = tools.remove(index);
            let mut model_tool = binding.compiled.to_model_tool();
            if let ToolBindingRef::Export(export) = selection {
                if let Some(alias) = &export.alias {
                    model_tool.name = alias.clone();
                }
            }
            pinned_tools.push(PinnedPromptTool {
                selection: selection.clone(),
                tool: binding.compiled.descriptor().tool.clone(),
                compiler_version: binding.compiled.compiler_version().into(),
                compiled_digest: binding.compiled.digest().clone(),
                descriptor_digest: binding.compiled.descriptor_digest().clone(),
                model_schema_digest: binding.compiled.model_schema_digest().clone(),
                model_tool,
            });
        }
        let mut pinned_skills = Vec::new();
        for selection in &profile.profile().skills {
            let index = skills
                .iter()
                .position(|manifest| {
                    manifest.skill.id == selection.skill_id
                        && manifest.skill.version == selection.version
                })
                .ok_or_else(|| invalid("prompt.skills"))?;
            pinned_skills.push(skills.remove(index));
        }
        let data = PromptData {
            assembler_version: CONTEXT_ASSEMBLER_VERSION.into(),
            scope: profile.scope().clone(),
            profile: profile.profile().clone(),
            initial_resolution_digest: profile.resolution_digest().clone(),
            pinned_components: non_model_components(profile),
            host_instructions,
            profile_asset,
            tools: pinned_tools,
            skills: pinned_skills,
        };
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_data()?;
        Ok(snapshot)
    }

    /// Canonical identity of the exact protected serialized prefix.
    pub fn digest(&self) -> JsonDigest {
        self.digest.clone()
    }
    /// Read pinned public tool metadata and identities, without hidden input schemas.
    pub fn tools(&self) -> &[PinnedPromptTool] {
        &self.data.tools
    }
    /// Read the original selected skill listings, without fetching newer versions.
    pub fn skills(&self) -> &[SkillManifest] {
        &self.data.skills
    }
    /// Read the authenticated scope in which this prefix was pinned.
    pub fn scope(&self) -> &Scope {
        &self.data.scope
    }

    /// Restore a protected record using its trusted digest and the current run's
    /// resolved profile. A new run may resolve a different model binding only.
    /// Resume must continue to use the original run's profile and selected route;
    /// this method is not an authorization to replace either during a run.
    pub fn restore(
        input: &str,
        expected_digest: &JsonDigest,
        profile: &ResolvedProfile,
        scope: &Scope,
    ) -> Result<Self, ContractError> {
        let data: PromptData =
            serde_json::from_value(parse_json(input).map_err(|_| invalid("prompt"))?)
                .map_err(|_| invalid("prompt"))?;
        let snapshot = Self {
            digest: data_digest(&data),
            data,
        };
        snapshot.validate_for(profile, scope, expected_digest)?;
        Ok(snapshot)
    }

    /// Require the stored prefix identity, scope, profile, and all non-model assets.
    pub fn validate_for(
        &self,
        profile: &ResolvedProfile,
        scope: &Scope,
        expected_digest: &JsonDigest,
    ) -> Result<(), ContractError> {
        if &self.digest != expected_digest
            || &self.data.scope != scope
            || profile.scope() != scope
            || self.data.profile.digest() != *profile.profile_digest()
            || self.data.pinned_components != non_model_components(profile)
        {
            return Err(mismatch("prompt"));
        }
        self.validate_data()
    }

    fn validate_data(&self) -> Result<(), ContractError> {
        if self.data.assembler_version != CONTEXT_ASSEMBLER_VERSION
            || data_digest(&self.data) != self.digest
        {
            return Err(mismatch("prompt.version"));
        }
        match (&self.data.profile.instructions, &self.data.profile_asset) {
            (Instructions::Text(_), None) => {}
            (Instructions::Asset(reference), Some(asset)) if reference.asset_ref == asset.asset => {
            }
            _ => return Err(mismatch("prompt.instructions")),
        }
        if self.data.tools.len() != self.data.profile.tools.len()
            || self.data.skills.len() != self.data.profile.skills.len()
        {
            return Err(mismatch("prompt.selections"));
        }
        let mut names = BTreeSet::new();
        for (selection, tool) in self.data.profile.tools.iter().zip(&self.data.tools) {
            if selection != &tool.selection
                || !names.insert(&tool.model_tool.name)
                || crate::canonical_digest(&tool.model_tool.model_input_schema)
                    != tool.model_schema_digest
            {
                return Err(mismatch("prompt.tools"));
            }
            match selection {
                ToolBindingRef::Catalog(reference) => {
                    if tool.tool.id != reference.tool_id || tool.tool.version != reference.version {
                        return Err(mismatch("prompt.tools"));
                    }
                }
                ToolBindingRef::Export(export) => {
                    let adapter = self
                        .data
                        .profile
                        .adapters
                        .as_ref()
                        .and_then(|adapters| {
                            adapters
                                .iter()
                                .find(|adapter| adapter.binding_id == export.adapter_binding)
                        })
                        .ok_or_else(|| mismatch("prompt.export"))?;
                    if !self.data.pinned_components.iter().any(|component| {
                        component.reference.kind == ComponentKind::Adapter
                            && component.reference.id == adapter.adapter_id
                            && component.reference.version.as_ref() == Some(&adapter.version)
                    }) || export
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias != &tool.model_tool.name)
                    {
                        return Err(mismatch("prompt.export"));
                    }
                }
            }
        }
        for (selected, manifest) in self.data.profile.skills.iter().zip(&self.data.skills) {
            if selected.skill_id != manifest.skill.id || selected.version != manifest.skill.version
            {
                return Err(mismatch("prompt.skills"));
            }
        }
        Ok(())
    }

    fn prefix(&self) -> Vec<ModelMessage> {
        let profile_text = match &self.data.profile.instructions {
            Instructions::Text(instructions) => instructions.text.clone(),
            Instructions::Asset(_) => self
                .data
                .profile_asset
                .as_ref()
                .expect("validated instruction asset")
                .text
                .clone(),
        };
        let mut messages = vec![
            ModelMessage {
                role: ModelRole::System,
                content: self
                    .data
                    .host_instructions
                    .iter()
                    .map(|text| ModelContent::Text { text: text.clone() })
                    .collect(),
            },
            ModelMessage {
                role: ModelRole::System,
                content: vec![ModelContent::Text { text: profile_text }],
            },
        ];
        if !self.data.skills.is_empty() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: vec![ModelContent::Json {
                    value: json!({"kind":"available_skills", "skills":self.data.skills}),
                }],
            });
        }
        messages
    }
}

fn non_model_components(profile: &ResolvedProfile) -> Vec<ResolvedComponent> {
    profile
        .components()
        .iter()
        .filter(|component| component.reference.kind != ComponentKind::ModelBinding)
        .cloned()
        .collect()
}

/// Source classification of already-authorized context data. None grants system authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextOrigin {
    /// Additional user-provided context, distinct from the preserved original request.
    User,
    /// Data associated with a selected pinned skill; no loader runs here.
    Skill,
    /// Data associated with a selected tool.
    Tool,
    /// External retrieved data, not trusted instructions.
    Retrieval,
    /// Recalled memory, not a policy grant.
    Memory,
    /// Verification feedback, not a Host instruction replacement.
    Verification,
}

/// Scope of context lifetime. An item outside its lifetime is explicitly omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextLifetime {
    /// Context valid throughout one session.
    Session {
        /// Owning session.
        session_id: Id,
    },
    /// Context valid during one run.
    Run {
        /// Owning run.
        run_id: Id,
    },
    /// Context valid only for one logical model step.
    Step {
        /// Owning run.
        run_id: Id,
        /// Logical step, preserved across physical retries.
        model_step_id: Id,
    },
}

/// Selection importance, independent of source authority and provider role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextPriority {
    /// Fail if this active item cannot fit in full.
    Required,
    /// Include whole if remaining bounds permit it.
    Optional,
}

/// Data with explicit source, scope, integrity and lifetime. Constructing this DTO
/// does not authenticate provenance; callers must authorize sources before supply.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    /// Stable source item identity.
    pub item_id: Id,
    /// Claimed source classification, retained in a data envelope.
    pub origin: ContextOrigin,
    /// Exact source/asset identity and version.
    pub source_ref: VersionedRef,
    /// Authenticated source scope supplied by the Host.
    pub scope: Scope,
    /// Explicitly selected content, not a system map or raw protected record.
    pub content: Vec<InputContent>,
    /// Digest of all other fields, checked again at projection.
    pub digest: JsonDigest,
    /// Session/run/step applicability.
    pub lifetime: ContextLifetime,
    /// Required versus optional selection, without elevated instruction authority.
    pub priority_class: ContextPriority,
}

impl ContextItem {
    /// Own supplied data and compute its source/lifetime/content identity.
    pub fn new(
        item_id: Id,
        origin: ContextOrigin,
        source_ref: VersionedRef,
        scope: Scope,
        content: Vec<InputContent>,
        lifetime: ContextLifetime,
        priority_class: ContextPriority,
    ) -> Self {
        let digest = data_digest(&(
            &item_id,
            origin,
            &source_ref,
            &scope,
            &content,
            &lifetime,
            priority_class,
        ));
        Self {
            item_id,
            origin,
            source_ref,
            scope,
            content,
            digest,
            lifetime,
            priority_class,
        }
    }
    fn valid_digest(&self) -> bool {
        self.digest
            == data_digest(&(
                &self.item_id,
                self.origin,
                &self.source_ref,
                &self.scope,
                &self.content,
                &self.lifetime,
                self.priority_class,
            ))
    }
}
impl fmt::Debug for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextItem")
            .field("item_id", &self.item_id)
            .field("origin", &self.origin)
            .field("digest", &self.digest)
            .finish_non_exhaustive()
    }
}

/// Already-authorized typed provider replay data, not a generic JSON record loader.
#[derive(Debug, Clone)]
pub struct ScopedOpaque {
    /// Scope from which the protected record was read.
    pub scope: Scope,
    /// Exact reference whose digest covers the serialized OpaqueContinuation.
    pub reference: RecordRef,
    /// Provider that owns the record.
    pub provider: Id,
    /// Typed continuation with an exact route identity.
    pub continuation: OpaqueContinuation,
}

/// Finite projection size, separate from model token context capacity and usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionLimits {
    /// Maximum serialized final ModelRequest bytes, including schemas and metadata.
    pub max_bytes: usize,
    /// Maximum projected content blocks plus model tool definitions.
    pub max_items: usize,
}

/// Read-only projection inputs. Transcript must come from a trusted, scoped session
/// store; Message alone cannot authenticate its owner or prove history completeness.
pub struct ProjectionInput<'a> {
    /// Original resolved profile of this run; never re-resolve it during resume.
    pub profile: &'a ResolvedProfile,
    /// Authenticated execution scope.
    pub scope: &'a Scope,
    /// Current owning run.
    pub run_id: &'a Id,
    /// Logical step, identical to request_id before physical invocation allocation.
    pub model_step_id: &'a Id,
    /// Original persisted run request.
    pub current_request: &'a RunRequest,
    /// Exact stored user message containing that request, to prevent duplication.
    pub current_request_message_id: &'a Id,
    /// Owned-store history borrowed without mutation, including the current user message.
    pub transcript: &'a [Message],
    /// Already-authorized context items; no external source is queried here.
    pub context_items: &'a [ContextItem],
    /// Already-authorized opaque records with typed scope/provider/route metadata.
    pub opaque_records: &'a [ScopedOpaque],
    /// Trusted digest from the session's pinned prompt record.
    pub expected_prompt_digest: &'a JsonDigest,
    /// Logical step identity; ModelExchange later assigns a separate physical request ID.
    pub request_id: Id,
    /// Accounting purpose of this invocation.
    pub purpose: ModelPurpose,
    /// Already-selected immutable model route.
    pub route: ResolvedModelRoute,
    /// Already-resolved requested output mode.
    pub output: ModelOutput,
    /// Provider output-token request, not an estimate of input bytes.
    pub max_output_tokens: NonZeroU64,
    /// Provider request/response decoding bounds.
    pub response_limits: ModelResponseLimits,
    /// Byte/item projection bounds, not a tokenizer or model context-window check.
    pub limits: ProjectionLimits,
}

/// Separate model projection and explicit selection provenance. No original messages change.
#[derive(Debug)]
pub struct ContextProjection {
    /// Complete prepared model request.
    pub request: ModelRequest,
    /// Original message identities represented in the model request.
    pub selected_message_ids: Vec<Id>,
    /// Original message identities omitted by visibility or whole-run selection.
    pub dropped_message_ids: Vec<Id>,
    /// Active supplied context items included in full.
    pub selected_context_ids: Vec<Id>,
    /// Context items omitted by lifetime or optional-item bounds.
    pub dropped_context_ids: Vec<Id>,
    /// Identity of the unchanged session prefix.
    pub prompt_digest: JsonDigest,
}

/// Prefix reuse and conservative selection without retrieval, loading or compaction.
#[derive(Debug, Clone, Copy, Default)]
pub struct ContextAssembler;

struct RunGroup {
    run_id: Id,
    messages: Vec<(Id, ModelMessage)>,
    has_tool_round: bool,
    has_unknown: bool,
}

impl ContextAssembler {
    /// Construct an assembler without doing I/O.
    pub fn new() -> Self {
        Self
    }

    /// Preserve the fixed prefix and all model-visible current-run messages. Older
    /// complete runs and optional items are added newest first without splitting
    /// tool rounds. The latest visible tool round and runs with unknown tool results
    /// are mandatory. Any unfinished round or oversized required input fails.
    /// Byte bounds do not claim to estimate or enforce provider token context size.
    pub fn project(
        &self,
        snapshot: &PromptSnapshot,
        input: ProjectionInput<'_>,
    ) -> Result<ContextProjection, ContractError> {
        snapshot.validate_for(input.profile, input.scope, input.expected_prompt_digest)?;
        if input.request_id != *input.model_step_id
            || input.limits.max_bytes == 0
            || input.limits.max_items == 0
        {
            return Err(invalid("projection.identity_or_limits"));
        }
        validate_current_request(&input)?;
        let groups = project_transcript(snapshot, &input)?;
        let current = groups
            .iter()
            .position(|group| &group.run_id == input.run_id)
            .ok_or_else(|| invalid("projection.current_run"))?;
        if current + 1 != groups.len() {
            return Err(invalid("projection.incomplete_round"));
        }
        let mut selected_groups = BTreeSet::from([current]);
        if let Some(index) = groups.iter().rposition(|group| group.has_tool_round) {
            selected_groups.insert(index);
        }
        selected_groups.extend(
            groups
                .iter()
                .enumerate()
                .filter(|(_, group)| group.has_unknown)
                .map(|(index, _)| index),
        );
        let mut context = Vec::new();
        let mut active = Vec::new();
        let mut seen_context = BTreeSet::new();
        for item in input.context_items {
            if !seen_context.insert(&item.item_id) || !item.valid_digest() {
                return Err(invalid("context_item.digest"));
            }
            if &item.scope != input.scope {
                return Err(mismatch("context_item.scope"));
            }
            if item.origin == ContextOrigin::Skill
                && !snapshot
                    .data
                    .skills
                    .iter()
                    .any(|manifest| manifest.skill == item.source_ref)
            {
                return Err(mismatch("context_item.skill"));
            }
            if item.origin == ContextOrigin::Tool
                && !snapshot
                    .data
                    .tools
                    .iter()
                    .any(|tool| tool.tool == item.source_ref)
            {
                return Err(mismatch("context_item.tool"));
            }
            let applicable = match &item.lifetime {
                ContextLifetime::Session { session_id } => {
                    session_id == &input.current_request.session_id
                }
                ContextLifetime::Run { run_id } => run_id == input.run_id,
                ContextLifetime::Step {
                    run_id,
                    model_step_id,
                } => run_id == input.run_id && model_step_id == input.model_step_id,
            };
            let message = if applicable {
                Some(ModelMessage {
                    role: ModelRole::User,
                    content: vec![ModelContent::Json {
                        value: json!({
                            "kind":"context_data", "item_id":item.item_id, "origin":item.origin,
                            "source_ref":item.source_ref,
                            "content":item.content.iter().map(|content| safe_value(content, input.scope)).collect::<Result<Vec<_>,_>>()?
                        }),
                    }],
                })
            } else {
                None
            };
            active.push(applicable);
            context.push(message);
        }
        let mut selected_context: BTreeSet<usize> = input
            .context_items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                active[*index] && item.priority_class == ContextPriority::Required
            })
            .map(|(index, _)| index)
            .collect();
        let make_request = |selected_groups: &BTreeSet<usize>,
                            selected_context: &BTreeSet<usize>| {
            let mut messages = snapshot.prefix();
            for index in selected_groups {
                messages.extend(
                    groups[*index]
                        .messages
                        .iter()
                        .map(|(_, message)| message.clone()),
                );
            }
            for index in selected_context {
                messages.push(context[*index].as_ref().expect("active context").clone());
            }
            ModelRequest {
                request_id: input.request_id.clone(),
                purpose: input.purpose,
                route: input.route.clone(),
                messages,
                tools: snapshot
                    .data
                    .tools
                    .iter()
                    .map(|tool| tool.model_tool.clone())
                    .collect(),
                output: input.output.clone(),
                max_output_tokens: input.max_output_tokens,
                limits: input.response_limits.clone(),
            }
        };
        if !fits(
            &make_request(&selected_groups, &selected_context),
            &input.limits,
        ) {
            return Err(budget());
        }
        for index in (0..current).rev() {
            if selected_groups.contains(&index) {
                continue;
            }
            selected_groups.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_groups.remove(&index);
            }
        }
        for index in (0..input.context_items.len()).rev() {
            if !active[index]
                || input.context_items[index].priority_class == ContextPriority::Required
            {
                continue;
            }
            selected_context.insert(index);
            if !fits(
                &make_request(&selected_groups, &selected_context),
                &input.limits,
            ) {
                selected_context.remove(&index);
            }
        }
        let request = make_request(&selected_groups, &selected_context);
        request
            .validate()
            .map_err(|_| invalid("projection.model_request"))?;
        let selected_message_ids: Vec<_> = selected_groups
            .iter()
            .flat_map(|index| groups[*index].messages.iter().map(|(id, _)| id.clone()))
            .collect();
        let selected_ids: BTreeSet<_> = selected_message_ids.iter().collect();
        Ok(ContextProjection {
            request,
            dropped_message_ids: input
                .transcript
                .iter()
                .filter(|message| !selected_ids.contains(&message.message_id))
                .map(|message| message.message_id.clone())
                .collect(),
            selected_message_ids,
            selected_context_ids: selected_context
                .iter()
                .map(|index| input.context_items[*index].item_id.clone())
                .collect(),
            dropped_context_ids: input
                .context_items
                .iter()
                .enumerate()
                .filter(|(index, _)| !selected_context.contains(index))
                .map(|(_, item)| item.item_id.clone())
                .collect(),
            prompt_digest: snapshot.digest(),
        })
    }
}

fn validate_current_request(input: &ProjectionInput<'_>) -> Result<(), ContractError> {
    let message = input
        .transcript
        .iter()
        .find(|message| &message.message_id == input.current_request_message_id)
        .ok_or_else(|| invalid("projection.current_request"))?;
    if &message.run_id != input.run_id
        || message.role != MessageRole::User
        || message.origin != MessageOrigin::User
        || !visible(message)
    {
        return Err(invalid("projection.current_request"));
    }
    let contents: Option<Vec<_>> = message
        .content
        .iter()
        .map(|block| match block {
            ContentBlock::Content { content } => Some(content),
            _ => None,
        })
        .collect();
    if contents.as_deref()
        != Some(
            input
                .current_request
                .input
                .iter()
                .collect::<Vec<_>>()
                .as_slice(),
        )
    {
        return Err(mismatch("projection.current_request"));
    }
    Ok(())
}

struct PendingCall {
    message_id: Id,
    provider_call_id: Id,
    visible: bool,
    known: bool,
}

fn project_transcript(
    snapshot: &PromptSnapshot,
    input: &ProjectionInput<'_>,
) -> Result<Vec<RunGroup>, ContractError> {
    let mut groups = Vec::new();
    let mut seen_messages = BTreeSet::new();
    let mut seen_runs = BTreeSet::new();
    let mut previous_sequence = 0;
    let mut cursor = 0;
    while cursor < input.transcript.len() {
        let run_id = input.transcript[cursor].run_id.clone();
        if !seen_runs.insert(run_id.clone()) {
            return Err(invalid("transcript.run_order"));
        }
        let end = input.transcript[cursor..]
            .iter()
            .position(|message| message.run_id != run_id)
            .map_or(input.transcript.len(), |offset| cursor + offset);
        let mut projected = Vec::new();
        let mut has_tool_round = false;
        let mut has_unknown = false;
        let mut pending: BTreeMap<Id, PendingCall> = BTreeMap::new();
        let mut seen_calls = BTreeSet::new();
        for message in &input.transcript[cursor..end] {
            if message.sequence.get() <= previous_sequence
                || !seen_messages.insert(&message.message_id)
            {
                return Err(invalid("transcript.order"));
            }
            previous_sequence = message.sequence.get();
            let is_visible = visible(message);
            if is_visible {
                match message.role {
                    MessageRole::System => return Err(invalid("transcript.system_role")),
                    MessageRole::Assistant if message.origin != MessageOrigin::Model => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::Tool if message.origin != MessageOrigin::Tool => {
                        return Err(invalid("transcript.origin"));
                    }
                    MessageRole::User
                        if matches!(
                            message.origin,
                            MessageOrigin::Host
                                | MessageOrigin::Profile
                                | MessageOrigin::Model
                                | MessageOrigin::Tool
                        ) =>
                    {
                        return Err(invalid("transcript.origin"));
                    }
                    _ => {}
                }
                if !pending.is_empty() && message.role != MessageRole::Tool {
                    return Err(invalid("transcript.incomplete_round"));
                }
            }
            let mut content = Vec::new();
            for block in &message.content {
                match block {
                    ContentBlock::ToolCall { call } => {
                        has_tool_round |= is_visible;
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || !seen_calls.insert(&call.call_id)
                        {
                            return Err(invalid("transcript.tool_call"));
                        }
                        let tool = snapshot
                            .data
                            .tools
                            .iter()
                            .find(|tool| tool.model_tool.name == call.tool_name);
                        if tool.is_some_and(|tool| tool.descriptor_digest != call.descriptor_digest)
                        {
                            return Err(mismatch("transcript.descriptor"));
                        }
                        pending.insert(
                            call.call_id.clone(),
                            PendingCall {
                                message_id: message.message_id.clone(),
                                provider_call_id: call.provider_call_id.clone(),
                                visible: is_visible,
                                known: tool.is_some(),
                            },
                        );
                        if is_visible {
                            content.push(ModelContent::ToolCall {
                                provider_call_id: call.provider_call_id.clone(),
                                name: call.tool_name.clone(),
                                arguments: call.model_inputs.clone(),
                            });
                        }
                    }
                    ContentBlock::ToolResult { result } => {
                        if message.role != MessageRole::Tool
                            || message.origin != MessageOrigin::Tool
                        {
                            return Err(invalid("transcript.tool_result"));
                        }
                        let call = pending
                            .remove(&result.call_id)
                            .ok_or_else(|| invalid("transcript.tool_result"))?;
                        if call.message_id != result.call_message_id
                            || call.visible != is_visible
                            || (!call.known && result.status == ToolResultStatus::Succeeded)
                        {
                            return Err(invalid("transcript.tool_pair"));
                        }
                        if result.status == ToolResultStatus::Unknown {
                            if !is_visible {
                                return Err(invalid("transcript.hidden_unknown_effect"));
                            }
                            has_unknown = true;
                        }
                        if is_visible {
                            let values = result
                                .content
                                .iter()
                                .map(|item| safe_value(item, input.scope))
                                .collect::<Result<Vec<_>, _>>()?;
                            let mut value = json!({"status":result.status,"content":values});
                            if let Some(failure) = &result.error {
                                value["error"] = json!({"code":failure.code});
                            }
                            content.push(ModelContent::ToolResult {
                                provider_call_id: call.provider_call_id,
                                content: value,
                            });
                        }
                    }
                    ContentBlock::Content { content: item } if is_visible => {
                        if message.role == MessageRole::Tool {
                            return Err(invalid("transcript.tool_result"));
                        }
                        content.push(safe_content(item, input.scope)?);
                    }
                    ContentBlock::ProviderOpaque {
                        provider,
                        route_digest,
                        data_ref,
                    } if is_visible => {
                        if message.role != MessageRole::Assistant
                            || message.origin != MessageOrigin::Model
                            || provider != &input.route.provider
                            || route_digest != &input.route.digest()
                        {
                            return Err(mismatch("transcript.opaque_route"));
                        }
                        let records: Vec<_> = input
                            .opaque_records
                            .iter()
                            .filter(|record| &record.reference == data_ref)
                            .collect();
                        if records.len() != 1 {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        let record = records[0];
                        if &record.scope != input.scope
                            || &record.provider != provider
                            || record.continuation.route_digest() != route_digest
                            || data_digest(&record.continuation) != data_ref.digest
                        {
                            return Err(mismatch("transcript.opaque_record"));
                        }
                        content.push(ModelContent::Opaque {
                            continuation: record.continuation.clone(),
                        });
                    }
                    _ => {}
                }
            }
            if is_visible && !content.is_empty() {
                let role = match message.role {
                    MessageRole::User => ModelRole::User,
                    MessageRole::Assistant => ModelRole::Assistant,
                    MessageRole::Tool => ModelRole::Tool,
                    MessageRole::System => unreachable!("visible System rejected"),
                };
                if message.role == MessageRole::User && message.origin != MessageOrigin::User {
                    let values = content
                        .iter()
                        .map(|content| {
                            serde_json::to_value(content).expect("model content serialization")
                        })
                        .collect::<Vec<_>>();
                    content = vec![ModelContent::Json {
                        value: json!({"kind":"transcript_data", "origin":message.origin,
                        "source_message_id":message.message_id, "content":values}),
                    }];
                }
                projected.push((message.message_id.clone(), ModelMessage { role, content }));
            }
        }
        if !pending.is_empty() {
            return Err(invalid("transcript.incomplete_round"));
        }
        groups.push(RunGroup {
            run_id,
            messages: projected,
            has_tool_round,
            has_unknown,
        });
        cursor = end;
    }
    Ok(groups)
}

fn visible(message: &Message) -> bool {
    matches!(
        message.visibility,
        Visibility::Model | Visibility::UserAndModel
    )
}

fn safe_content(content: &InputContent, scope: &Scope) -> Result<ModelContent, ContractError> {
    match content {
        InputContent::Text { text } => Ok(ModelContent::Text { text: text.clone() }),
        InputContent::Json { value } => Ok(ModelContent::Json {
            value: value.clone(),
        }),
        _ => Ok(ModelContent::Json {
            value: safe_value(content, scope)?,
        }),
    }
}

fn safe_value(content: &InputContent, scope: &Scope) -> Result<Value, ContractError> {
    Ok(match content {
        InputContent::Text { text } => json!({"type":"text","text":text}),
        InputContent::Json { value } => json!({"type":"json","value":value}),
        InputContent::Artifact { reference } => {
            if &reference.scope != scope {
                return Err(mismatch("context.artifact_scope"));
            }
            json!({"type":"artifact","artifact_id":reference.artifact_id,"media_type":reference.media_type,
                "size_bytes":reference.size_bytes,"content_hash":reference.content_hash})
        }
        InputContent::Evidence { reference } => {
            let mut value = json!({"type":"evidence","source_id":reference.source_id,"version":reference.version,
                "location":reference.location,"content_hash":reference.content_hash});
            if let Some(quote) = &reference.quote {
                value["quote"] = json!(quote);
            }
            value
        }
    })
}

struct ByteCounter {
    written: usize,
    limit: usize,
}
impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.written = self
            .written
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("projection limit exceeded"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn fits(request: &ModelRequest, limits: &ProjectionLimits) -> bool {
    let count = request
        .messages
        .iter()
        .try_fold(request.tools.len(), |count, message| {
            count.checked_add(message.content.len())
        });
    if count.is_none_or(|count| count > limits.max_items) {
        return false;
    }
    serde_json::to_writer(
        &mut ByteCounter {
            written: 0,
            limit: limits.max_bytes.min(request.limits.max_input_bytes),
        },
        request,
    )
    .is_ok()
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
fn mismatch(path: &str) -> ContractError {
    ContractError::new(ErrorCode::ContextMismatch, path)
}
fn budget() -> ContractError {
    ContractError::new(
        ErrorCode::ContextBudgetExceeded,
        "projection.required_input",
    )
}
```

## `crates/wickle/src/error.rs`

```rust
use serde::{Deserialize, Serialize};

/// Stable categories for contract and profile validation failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorCode {
    /// Current policy or exact owner scope denies access.
    AccessDenied,
    /// The trusted policy failed or panicked; no permission was granted.
    PolicyUnavailable,
    /// The call's finite deadline elapsed.
    DeadlineExceeded,
    /// The current operation was cancelled.
    Cancelled,
    /// The Host has not supplied the required asynchronous runtime.
    RuntimeUnavailable,
    /// A configured call, repair, or recovery budget has no remaining capacity.
    BudgetExceeded,
    /// A required time reading or timer could not be obtained.
    ClockUnavailable,
    /// A monotonic reading regressed or a resumed UTC clock predates saved progress.
    ClockRegression,
    /// The Host identifier source could not generate an internal execution identifier.
    IdGenerationFailed,
    /// Input is not unambiguous, finite JSON.
    InvalidJson,
    /// Input does not match a data contract.
    InvalidContract,
    /// Tool exposure, binding metadata, or a registered input schema is inconsistent.
    InvalidToolInputContract,
    /// The compiler cannot safely project this input schema or reference form.
    UnsupportedInputProjection,
    /// Model-owned or assembled tool arguments do not satisfy their input contract.
    InvalidArguments,
    /// A supplied system value does not satisfy its registered input contract.
    SystemInputInvalid,
    /// Context identity, provenance structure, or call/result protocol is invalid.
    InvalidContext,
    /// Context scope, pinned assets, or protected-record identity does not match.
    ContextMismatch,
    /// Required context cannot fit the explicit byte or item bounds without truncation.
    ContextBudgetExceeded,
    /// The document format is not supported.
    UnsupportedSchemaVersion,
    /// A reference or binding is missing or inconsistent.
    InvalidReference,
    /// A required component or exact version is unavailable.
    ComponentUnavailable,
    /// A component uses an unsupported metadata contract.
    UnsupportedContractVersion,
    /// Selected components do not supply a required capability.
    CapabilityUnsupported,
    /// A configuration does not satisfy its registered schema.
    InvalidConfiguration,
    /// A registered schema is invalid or requires unsupported resolution.
    InvalidSchema,
    /// A profile differs from the profile pinned to an existing execution.
    ProfileMismatch,
    /// Stored data violates checkpoint invariants.
    InvalidSnapshot,
    /// The requested run, session, or protected record is absent in this exact scope.
    StateNotFound,
    /// An existing request identity was reused with different logical input.
    RequestConflict,
    /// The session already has a running or waiting run.
    SessionBusy,
    /// A proposed run identifier already belongs to another request in this scope.
    RunConflict,
    /// The compare-and-swap revision no longer matches saved state.
    RevisionConflict,
    /// Another unexpired execution lease already owns the run.
    LeaseBusy,
    /// The execution lease expired or no longer matches its owner and generation.
    LeaseLost,
    /// A candidate change violates immutable data or state-transition rules.
    InvalidTransition,
    /// An event has a duplicate identity, invalid sequence, or inconsistent references.
    InvalidEvent,
    /// A message has a duplicate identity, invalid sequence, or wrong owning run.
    InvalidMessage,
    /// Immutable record content or a requested reference digest conflicts.
    RecordConflict,
    /// Authoritative storage is unavailable; no successful commit is implied.
    PersistenceUnavailable,
}

/// A validation error that does not retain submitted values or credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{code:?} at {path}")]
pub struct ContractError {
    /// Machine-readable failure category.
    pub code: ErrorCode,
    /// Contract field or reference location, without submitted values.
    pub path: String,
}

impl ContractError {
    /// Construct an error using a safe contract location.
    pub fn new(code: ErrorCode, path: impl Into<String>) -> Self {
        Self {
            code,
            path: path.into(),
        }
    }
}
```

## `crates/wickle/src/lib.rs`

```rust
//! Wickle, an agent engine for Rust applications.
//!
//! Profiles, scoped metadata resolution, and versioned execution data contracts.
//! Model calls use scoped ports and persisted attempt accounting. Tool dispatch
//! and the agent driver are not implemented yet.
//!
//! Runtime objects stay in Host code. Only documented data contracts are
//! serialized; successful decoding does not authenticate a caller.
//!
//! Internal modules are not extension points; use the root exports.
//! ```compile_fail
//! use wickle::serialization::canonical_digest;
//! ```

mod budget;
mod clock;
mod context;
mod context_projection;
mod error;
mod message;
mod model;
mod model_execution;
mod model_protocol;
mod policy;
mod profile;
mod resolution;
mod run;
mod serialization;
mod state;
mod tool_schema;
mod views;

pub use budget::{AttemptReservation, ReservationKind, RunBudget, RunTiming};
pub use clock::{Clock, ClockReading, IdSource, RandomIdSource, SystemClock};
pub use context_projection::{
    CONTEXT_ASSEMBLER_VERSION, ContextAssembler, ContextItem, ContextLifetime, ContextOrigin,
    ContextPriority, ContextProjection, InstructionAssetContent, PinnedPromptTool, ProjectionInput,
    ProjectionLimits, PromptSnapshot, PromptToolBinding, ScopedOpaque, SkillManifest,
};
pub use model_protocol::{
    ModelCallContext, ModelContent, ModelEvent, ModelFinish, ModelMessage, ModelOutput, ModelPort,
    ModelPortBinding, ModelProtocolError, ModelProtocolErrorCode, ModelRequest, ModelResponse,
    ModelResponseLimits, ModelResponseMetadata, ModelRole, ModelTool, OpaqueContinuation,
    ProposedToolCall, ToolCallValidation, collect_model_response,
};
pub use policy::{
    ApprovalChallenge, Guarded, PolicyAction, PolicyContext, PolicyDecision, PolicyGate,
    PolicyPort, PolicyRequest, ToolPolicyInput,
};
pub use state::{
    AdmissionInput, AdmissionResult, CommitInput, EventPage, MAX_EVENT_PAGE_SIZE, MemoryStateStore,
    ProtectedRecord, RunLease, StateStore, StateStoreCapabilities, StoredRun,
};
pub use tool_schema::{
    CompiledTool, SchemaCompiler, SystemInputDefinition, SystemInputRegistry, SystemInputSource,
    TOOL_SCHEMA_COMPILER_VERSION, ToolConcurrency, ToolDescriptor, ToolRetryPolicy, ToolSideEffect,
};
pub use views::{ArtifactView, EventView, RunView};

pub use context::{
    ExecutionContext, ExecutionContextData, PortFuture, PortStream, Scope, SystemInputs,
};
pub use error::{ContractError, ErrorCode};
pub use message::{
    ArtifactRef, ContentBlock, EvidenceRef, Failure, InputContent, Message, MessageOrigin,
    MessageRole, RecordRef, ToolCall, ToolResult, ToolResultStatus, Visibility,
};
pub use model::{
    ApiContract, ModelAttemptState, ModelFailureKind, ModelInvocationRecord, ModelPurpose,
    ModelUsage, ResolvedModelRoute, RouteRequest, UsageMeasurement, VersionPolicy,
    VersionSemantics,
};
pub use model_execution::{
    ModelExchange, ModelExchangeOutcome, ModelRetryPolicy, StoredModelResponse,
};
pub use profile::{
    AdapterBindingRef, AgentProfile, CatalogHookRef, CatalogSourceRef, CatalogToolRef,
    CompletionPolicy, ConnectorBindingRef, ContextPolicy, ContextSourceBinding, ContextSourceRef,
    ContextTrigger, ExportRef, HookPosition, HookRef, InstructionAsset, InstructionText,
    Instructions, OutputContract, PROFILE_SCHEMA_VERSION, ProfileSchemaVersion, RunLimits,
    SkillRef, ToolBindingRef, VersionedRef,
};
pub use resolution::{
    ComponentKind, ComponentMetadata, ComponentRef, ExportKind, ExportMetadata, ProfileResolver,
    ProfileValidator, ResolvedComponent, ResolvedProfile,
};
pub use run::{
    ApprovalTarget, BudgetKind, BudgetUsage, CompletionBasis, EphemeralEvent, InputRequest,
    OutcomeResult, RUN_EVENT_SCHEMA_VERSION, RUN_SNAPSHOT_SCHEMA_VERSION, ResumeAction,
    ResumeCommand, RunEvent, RunEventPayload, RunEventSchemaVersion, RunOutcome, RunPhase,
    RunRequest, RunSnapshot, RunSnapshotSchemaVersion, RunStatus, RunTrigger, SessionSchemaVersion,
    SessionSnapshot, SourceExecutionState, SystemInputSnapshotRef, ToolCallState, ToolLedgerEntry,
    VerificationSummary, VerificationVerdict, WaitState, WaitTarget, admission_digest,
};
pub use serialization::{
    Id, JsonDigest, JsonObject, canonical_digest, canonical_digest_json, parse_json,
};
```

## `crates/wickle/tests/context_projection.rs`

```rust
//! Pinned prompts, provenance, protected-input boundaries, and atomic context selection.

use serde_json::{Value, json};
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).unwrap()
}
fn versioned(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
fn object(value: Value) -> JsonObject {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
fn record(value: &str) -> RecordRef {
    RecordRef {
        record_id: id(value),
        revision: 1,
        digest: canonical_digest(&json!(value)),
    }
}
fn compiled_tool(name: &str) -> CompiledTool {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string","format":"uuid"}),
        source: SystemInputSource::Run {},
    }])
    .unwrap();
    SchemaCompiler::new().compile(ToolDescriptor {
        tool:versioned(name), name:id(name), description:format!("{name} records"),
        input_schema:json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","format":"uuid"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters:vec!["query".into()], system_bindings:None,
        output_schema:json!({"type":"string"}), side_effect:ToolSideEffect::ReadOnly,
        concurrency:ToolConcurrency::Serial, retry:ToolRetryPolicy::Never, reconcile:false,
        max_output_bytes:4096.try_into().unwrap(),
    }, &registry).unwrap()
}

struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: Some(reference.version.clone().unwrap_or_else(|| id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(
                    &json!({"id":reference.id,"version":reference.version}),
                ),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| reference.id.clone()),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}

fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: versioned("model"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("model"),
        model_id: id("model"),
        model_version: id("release"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("provider"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("v1"),
        },
        adapter: versioned("adapter"),
        capability_revision: id("capabilities"),
        connection_ref: versioned("connection"),
    }
}

struct Fixture {
    scope: Scope,
    profile: ResolvedProfile,
    prompt: PromptSnapshot,
    prompt_digest: JsonDigest,
    tools: Vec<CompiledTool>,
    skill: SkillManifest,
    request: RunRequest,
    run_id: Id,
    step_id: Id,
    request_message_id: Id,
}

impl Fixture {
    async fn new() -> Self {
        let scope = Scope {
            tenant_id: id("tenant"),
            workspace_id: id("workspace"),
            user_id: None,
        };
        let profile=AgentProfile::from_json(r#"{
            "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1.0.0",
            "name":"Assistant","description":"Context fixture","instructions":{"text":"Profile instructions"},
            "model_binding":"model","tools":[{"tool_id":"search","version":"1"},{"tool_id":"read","version":"1"}],
            "skills":[{"skill_id":"analysis","version":"1"}],"connectors":[],
            "context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
            "limits":{"max_model_calls":4,"max_tool_attempts":4,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
        }"#).unwrap();
        let profile = ProfileValidator::new(&Catalog)
            .validate(&profile, &scope)
            .await
            .unwrap();
        let tools = vec![compiled_tool("search"), compiled_tool("read")];
        let skill = SkillManifest {
            skill: versioned("analysis"),
            name: "Analysis".into(),
            description: "Analyze evidence".into(),
            manifest_digest: canonical_digest(&json!("analysis manifest")),
        };
        let bindings = profile
            .profile()
            .tools
            .iter()
            .cloned()
            .zip(tools.iter().cloned())
            .map(|(selection, compiled)| PromptToolBinding {
                selection,
                compiled,
            })
            .collect();
        let prompt = PromptSnapshot::create(
            &profile,
            vec!["Host rule A".into(), "Host rule B".into()],
            None,
            bindings,
            vec![skill.clone()],
        )
        .unwrap();
        let prompt_digest = prompt.digest();
        Self {
            scope,
            profile,
            prompt,
            prompt_digest,
            tools,
            skill,
            request: RunRequest {
                request_id: id("user-request"),
                session_id: id("session"),
                input: vec![InputContent::Text {
                    text: "Current requested work".into(),
                }],
                trigger: RunTrigger::User {},
                output_contract: None,
            },
            run_id: id("current-run"),
            step_id: id("step"),
            request_message_id: id("current-message"),
        }
    }
    fn current_message(&self, sequence: u64) -> Message {
        Message {
            message_id: self.request_message_id.clone(),
            run_id: self.run_id.clone(),
            sequence: sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: self
                .request
                .input
                .iter()
                .cloned()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }
    }
    fn input<'a>(
        &'a self,
        transcript: &'a [Message],
        items: &'a [ContextItem],
        opaque: &'a [ScopedOpaque],
    ) -> ProjectionInput<'a> {
        ProjectionInput {
            profile: &self.profile,
            scope: &self.scope,
            run_id: &self.run_id,
            model_step_id: &self.step_id,
            current_request: &self.request,
            current_request_message_id: &self.request_message_id,
            transcript,
            context_items: items,
            opaque_records: opaque,
            expected_prompt_digest: &self.prompt_digest,
            request_id: self.step_id.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 100_000,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 32,
                max_tool_calls: 4,
            },
            limits: ProjectionLimits {
                max_bytes: 100_000,
                max_items: 100,
            },
        }
    }
    fn item(&self, name: &str, origin: ContextOrigin, priority: ContextPriority) -> ContextItem {
        ContextItem::new(
            id(name),
            origin,
            versioned(name),
            self.scope.clone(),
            vec![InputContent::Text {
                text: format!("data for {name}"),
            }],
            ContextLifetime::Run {
                run_id: self.run_id.clone(),
            },
            priority,
        )
    }
}

fn message(
    run: &str,
    sequence: u64,
    role: MessageRole,
    origin: MessageOrigin,
    content: Vec<ContentBlock>,
) -> Message {
    Message {
        message_id: id(&format!("message-{sequence}")),
        run_id: id(run),
        sequence: sequence.try_into().unwrap(),
        role,
        origin,
        content,
        visibility: Visibility::UserAndModel,
    }
}
fn text(value: &str) -> ContentBlock {
    ContentBlock::Content {
        content: InputContent::Text { text: value.into() },
    }
}
fn round(run: &str, sequence: u64, label: &str, body: &str, tool: &CompiledTool) -> Vec<Message> {
    let call_message = id(&format!("message-{sequence}"));
    let call = ToolCall {
        call_id: id(label),
        model_request_id: id(&format!("request-{label}")),
        provider_call_id: id(&format!("provider-{label}")),
        tool_name: id("search"),
        model_inputs: object(json!({"query":label})),
        descriptor_digest: tool.descriptor_digest().clone(),
        bound_input_ref: Some(record("bound-private-input")),
    };
    let result = ToolResult {
        call_id: call.call_id.clone(),
        call_message_id: call_message,
        status: ToolResultStatus::Failed,
        content: vec![InputContent::Text { text: body.into() }],
        effect_receipt_ref: Some(record("private-effect-receipt")),
        error: Some(Failure {
            code: id("unavailable"),
            diagnostic_ref: Some(record("private-diagnostic")),
        }),
    };
    vec![
        message(
            run,
            sequence,
            MessageRole::Assistant,
            MessageOrigin::Model,
            vec![ContentBlock::ToolCall { call }],
        ),
        message(
            run,
            sequence + 1,
            MessageRole::Tool,
            MessageOrigin::Tool,
            vec![ContentBlock::ToolResult { result }],
        ),
    ]
}

#[tokio::test]
async fn host_profile_and_skill_prefixes_are_pinned_and_tools_use_only_compiled_model_schemas() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.prompt_digest, fixture.prompt_digest);
    assert_eq!(
        projected.request.messages[0],
        ModelMessage {
            role: ModelRole::System,
            content: vec![
                ModelContent::Text {
                    text: "Host rule A".into()
                },
                ModelContent::Text {
                    text: "Host rule B".into()
                }
            ]
        }
    );
    assert_eq!(
        projected.request.messages[1],
        ModelMessage {
            role: ModelRole::System,
            content: vec![ModelContent::Text {
                text: "Profile instructions".into()
            }]
        }
    );
    assert_eq!(
        projected.request.tools,
        fixture
            .tools
            .iter()
            .map(CompiledTool::to_model_tool)
            .collect::<Vec<_>>()
    );
    for tool in &projected.request.tools {
        assert!(
            tool.model_input_schema["properties"]
                .get("workspace_id")
                .is_none()
        );
    }
    assert_eq!(
        projected.selected_message_ids,
        vec![fixture.request_message_id.clone()]
    );
    projected.request.validate().unwrap();
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(&fixture.prompt).unwrap(),
        &fixture.prompt_digest,
        &fixture.profile,
        &fixture.scope,
    )
    .unwrap();
    let repeated = ContextAssembler::new()
        .project(&restored, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(projected.request, repeated.request);
}

#[tokio::test]
async fn current_request_is_exactly_the_persisted_message_and_is_never_silently_replaced() {
    let fixture = Fixture::new().await;
    let current = fixture.current_message(1);
    for transcript in [
        vec![],
        vec![Message {
            content: vec![text("A different request")],
            ..current.clone()
        }],
        vec![Message {
            visibility: Visibility::Internal,
            ..current.clone()
        }],
        vec![Message {
            run_id: id("other-run"),
            ..current.clone()
        }],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let transcript = vec![current];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert_eq!(
        projected.request.messages.last().unwrap(),
        &ModelMessage {
            role: ModelRole::User,
            content: vec![ModelContent::Text {
                text: "Current requested work".into()
            }]
        }
    );
    assert_eq!(
        projected
            .selected_message_ids
            .iter()
            .filter(|message_id| *message_id == &fixture.request_message_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn tool_projection_keeps_model_arguments_and_public_observations_without_execution_records() {
    let fixture = Fixture::new().await;
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(round(
        "current-run",
        2,
        "call",
        "public observation",
        &fixture.tools[0],
    ));
    let mut internal = message(
        "current-run",
        4,
        MessageRole::User,
        MessageOrigin::Recovery,
        vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"system_inputs":{"workspace_id":"11111111-1111-4111-8111-111111111111"},"execution_args":{"query":"call","workspace_id":"11111111-1111-4111-8111-111111111111"},"raw_diagnostic":"private"}),
            },
        }],
    );
    internal.visibility = Visibility::Internal;
    transcript.push(internal);
    let original = transcript.clone();
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let contents: Vec<_> = projected
        .request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .collect();
    let call = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolCall {
                provider_call_id,
                name,
                arguments,
            } => Some((provider_call_id, name, arguments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        call,
        (
            &id("provider-call"),
            &id("search"),
            &object(json!({"query":"call"}))
        )
    );
    let result = contents
        .iter()
        .find_map(|content| match content {
            ModelContent::ToolResult {
                provider_call_id,
                content,
            } => Some((provider_call_id, content)),
            _ => None,
        })
        .unwrap();
    assert_eq!(result.0, &id("provider-call"));
    assert_eq!(
        result.1,
        &json!({"status":"failed","content":[{"type":"text","text":"public observation"}],"error":{"code":"unavailable"}})
    );
    assert_eq!(projected.request.messages.len(), 6);
    assert_eq!(
        projected.selected_message_ids,
        vec![
            fixture.request_message_id.clone(),
            id("message-2"),
            id("message-3")
        ]
    );
    assert_eq!(transcript, original);
    projected.request.validate().unwrap();
}

#[tokio::test]
async fn data_context_never_adds_system_authority_or_replaces_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let items = vec![
        fixture.item(
            "retrieval",
            ContextOrigin::Retrieval,
            ContextPriority::Required,
        ),
        fixture.item("memory", ContextOrigin::Memory, ContextPriority::Required),
        fixture.item(
            "verification",
            ContextOrigin::Verification,
            ContextPriority::Required,
        ),
    ];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids.len(), 3);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let mut forged = serde_json::to_value(&items[0]).unwrap();
    forged["origin"] = json!("host");
    assert!(serde_json::from_value::<ContextItem>(forged).is_err());
}

#[tokio::test]
async fn context_items_validate_scope_and_digest_and_exclude_other_run_or_step_lifetimes() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let original = fixture.item(
        "source",
        ContextOrigin::Retrieval,
        ContextPriority::Required,
    );
    let mut wrong_scope = original.clone();
    wrong_scope.scope.tenant_id = id("other-tenant");
    let mut wrong_run = original.clone();
    wrong_run.lifetime = ContextLifetime::Run {
        run_id: id("other-run"),
    };
    let mut wrong_step = original.clone();
    wrong_step.lifetime = ContextLifetime::Step {
        run_id: fixture.run_id.clone(),
        model_step_id: id("other-step"),
    };
    let mut changed_content = original;
    changed_content.content = vec![InputContent::Text {
        text: "changed data".into(),
    }];
    let with_valid_digest = |item: ContextItem| {
        ContextItem::new(
            item.item_id,
            item.origin,
            item.source_ref,
            item.scope,
            item.content,
            item.lifetime,
            item.priority_class,
        )
    };
    for item in [with_valid_digest(wrong_scope), changed_content] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[item], &[]))
                .is_err()
        );
    }
    for item in [with_valid_digest(wrong_run), with_valid_digest(wrong_step)] {
        let items = vec![item];
        let projected = ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
            .unwrap();
        assert!(projected.selected_context_ids.is_empty());
        assert_eq!(projected.dropped_context_ids, vec![id("source")]);
    }
}

#[tokio::test]
async fn a_current_tool_round_cannot_be_incomplete_or_have_an_unpaired_result() {
    let fixture = Fixture::new().await;
    let complete = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    for transcript in [
        vec![fixture.current_message(1), complete[0].clone()],
        vec![fixture.current_message(1), complete[1].clone()],
        vec![
            fixture.current_message(1),
            complete[0].clone(),
            message(
                "current-run",
                3,
                MessageRole::User,
                MessageOrigin::User,
                vec![text("interleaved")],
            ),
            Message {
                sequence: 4.try_into().unwrap(),
                ..complete[1].clone()
            },
        ],
    ] {
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_transcript_system_roles_cannot_extend_or_replace_the_pinned_prefix() {
    let fixture = Fixture::new().await;
    for origin in [
        MessageOrigin::Host,
        MessageOrigin::Profile,
        MessageOrigin::Retrieval,
        MessageOrigin::Memory,
    ] {
        let transcript = vec![
            fixture.current_message(1),
            message(
                "current-run",
                2,
                MessageRole::System,
                origin,
                vec![text("untrusted transcript instructions")],
            ),
        ];
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
}

#[tokio::test]
async fn visibility_filtering_and_message_references_cannot_separate_a_tool_call_from_its_result() {
    let fixture = Fixture::new().await;
    for hidden in [0, 1] {
        let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
        messages[hidden].visibility = Visibility::Internal;
        let mut transcript = vec![fixture.current_message(1)];
        transcript.extend(messages);
        assert!(
            ContextAssembler::new()
                .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
                .is_err()
        );
    }
    let mut messages = round("current-run", 2, "call", "observation", &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut messages[1].content[0] else {
        unreachable!()
    };
    result.call_message_id = id("some-other-call-message");
    let mut transcript = vec![fixture.current_message(1)];
    transcript.extend(messages);
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
            .is_err()
    );
}

#[tokio::test]
async fn bounded_selection_discards_an_older_run_as_a_whole_and_retains_the_latest_complete_round()
{
    let fixture = Fixture::new().await;
    let large = "old ".repeat(512);
    let mut transcript = vec![message(
        "old-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Old work")],
    )];
    transcript.extend(round("old-run", 2, "old-call", &large, &fixture.tools[0]));
    transcript.push(message(
        "old-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Old answer")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let original = transcript.clone();
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let full_bytes = serde_json::to_vec(&full.request).unwrap().len();
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = full_bytes - large.len() / 2;
    let byte_limit = bounded.limits.max_bytes;
    let selected = ContextAssembler::new()
        .project(&fixture.prompt, bounded)
        .unwrap();
    assert_eq!(
        selected.selected_message_ids,
        vec![
            id("message-5"),
            id("message-6"),
            id("message-7"),
            id("message-8"),
            fixture.request_message_id.clone()
        ]
    );
    assert_eq!(
        selected.dropped_message_ids,
        vec![
            id("message-1"),
            id("message-2"),
            id("message-3"),
            id("message-4")
        ]
    );
    assert!(serde_json::to_vec(&selected.request).unwrap().len() <= byte_limit);
    selected.request.validate().unwrap();
    assert_eq!(transcript, original);
}

#[tokio::test]
async fn required_current_work_and_prefix_report_budget_exhaustion_instead_of_truncation() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let count = baseline
        .request
        .messages
        .iter()
        .map(|message| message.content.len())
        .sum::<usize>()
        + baseline.request.tools.len();
    let mut item_limited = fixture.input(&current, &[], &[]);
    item_limited.limits.max_items = count - 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, item_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut input_limited = fixture.input(&current, &[], &[]);
    input_limited.response_limits.max_input_bytes = 1;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, input_limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
    let mut transcript = current;
    transcript.extend(round(
        "current-run",
        2,
        "current-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    let mut bounded = fixture.input(&transcript, &[], &[]);
    bounded.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, bounded)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn optional_context_can_be_removed_but_required_context_cannot_be_silently_dropped() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let mut optional = fixture.item(
        "optional",
        ContextOrigin::Retrieval,
        ContextPriority::Optional,
    );
    optional = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        vec![InputContent::Text {
            text: "reference data ".repeat(1000),
        }],
        optional.lifetime,
        optional.priority_class,
    );
    let items = vec![optional.clone()];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let mut limited = fixture.input(&transcript, &items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, limited)
        .unwrap();
    assert!(projected.selected_context_ids.is_empty());
    assert_eq!(projected.dropped_context_ids, vec![id("optional")]);
    let required = ContextItem::new(
        optional.item_id,
        optional.origin,
        optional.source_ref,
        optional.scope,
        optional.content,
        optional.lifetime,
        ContextPriority::Required,
    );
    let required_items = vec![required];
    let mut limited = fixture.input(&transcript, &required_items, &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 128;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn pinned_snapshot_rejects_other_scope_profile_and_recomputed_replacement_contents() {
    let fixture = Fixture::new().await;
    let mut foreign = fixture.scope.clone();
    foreign.tenant_id = id("other-tenant");
    assert!(
        fixture
            .prompt
            .validate_for(&fixture.profile, &foreign, &fixture.prompt_digest)
            .is_err()
    );
    let mut changed_profile = fixture.profile.profile().clone();
    changed_profile.version = id("2.0.0");
    let changed_profile = ProfileValidator::new(&Catalog)
        .validate(&changed_profile, &fixture.scope)
        .await
        .unwrap();
    assert!(
        fixture
            .prompt
            .validate_for(&changed_profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let replacement = PromptSnapshot::create(
        &fixture.profile,
        vec!["Replacement Host policy".into()],
        None,
        bindings,
        vec![fixture.skill.clone()],
    )
    .unwrap();
    assert!(
        replacement
            .validate_for(&fixture.profile, &fixture.scope, &fixture.prompt_digest)
            .is_err()
    );
    assert!(
        PromptSnapshot::restore(
            &serde_json::to_string(&replacement).unwrap(),
            &fixture.prompt_digest,
            &fixture.profile,
            &fixture.scope
        )
        .is_err()
    );
}

#[tokio::test]
async fn prompt_creation_rejects_unselected_tools_and_changed_skill_versions() {
    let fixture = Fixture::new().await;
    let mut extra = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect::<Vec<_>>();
    extra.push(PromptToolBinding {
        selection: ToolBindingRef::Catalog(CatalogToolRef {
            tool_id: id("unselected"),
            version: id("1"),
            bindings: None,
            config: None,
        }),
        compiled: compiled_tool("unselected"),
    });
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            extra,
            vec![fixture.skill.clone()]
        )
        .is_err()
    );
    let bindings = fixture
        .profile
        .profile()
        .tools
        .iter()
        .cloned()
        .zip(fixture.tools.iter().cloned())
        .map(|(selection, compiled)| PromptToolBinding {
            selection,
            compiled,
        })
        .collect();
    let mut changed = fixture.skill.clone();
    changed.skill.version = id("2");
    assert!(
        PromptSnapshot::create(
            &fixture.profile,
            vec!["Host rule".into()],
            None,
            bindings,
            vec![changed]
        )
        .is_err()
    );
}

#[tokio::test]
async fn opaque_replay_requires_matching_protected_record_scope_provider_and_route() {
    let fixture = Fixture::new().await;
    let continuation =
        OpaqueContinuation::new(&route(), json!({"signature":"provider continuation"}));
    let reference = RecordRef {
        record_id: id("opaque"),
        revision: 1,
        digest: canonical_digest(&serde_json::to_value(&continuation).unwrap()),
    };
    let mut transcript = vec![fixture.current_message(1)];
    transcript.push(message(
        "current-run",
        2,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![ContentBlock::ProviderOpaque {
            provider: id("provider"),
            route_digest: route().digest(),
            data_ref: reference.clone(),
        }],
    ));
    let records = vec![ScopedOpaque {
        scope: fixture.scope.clone(),
        reference: reference.clone(),
        provider: id("provider"),
        continuation: continuation.clone(),
    }];
    let accepted = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &records))
        .unwrap();
    assert!(accepted.request.messages.iter().flat_map(|message|&message.content).any(|content|matches!(content,ModelContent::Opaque{continuation:found} if found==&continuation)));
    let mut changed = fixture.input(&transcript, &[], &records);
    changed.route.connection_ref.version = id("different-connection");
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, changed)
            .is_err()
    );
    let mut wrong_scope = records.clone();
    wrong_scope[0].scope.tenant_id = id("other-tenant");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_scope)
            )
            .is_err()
    );
    let mut wrong_provider = records.clone();
    wrong_provider[0].provider = id("other-provider");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_provider)
            )
            .is_err()
    );
    let mut wrong_record = records;
    wrong_record[0].reference = record("different-record");
    assert!(
        ContextAssembler::new()
            .project(
                &fixture.prompt,
                fixture.input(&transcript, &[], &wrong_record)
            )
            .is_err()
    );
}

#[tokio::test]
async fn the_latest_tool_round_from_an_earlier_run_is_required_context() {
    let fixture = Fixture::new().await;
    let current = vec![fixture.current_message(5)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&current, &[], &[]))
        .unwrap();
    let mut transcript = vec![message(
        "previous-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Previous work")],
    )];
    transcript.extend(round(
        "previous-run",
        2,
        "previous-call",
        &"data ".repeat(400),
        &fixture.tools[0],
    ));
    transcript.push(message(
        "previous-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Previous answer")],
    ));
    transcript.extend(current);
    let unbounded = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(unbounded.selected_message_ids.contains(&id("message-2")));
    assert!(unbounded.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&baseline.request).unwrap().len() + 64;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn an_older_unknown_effect_is_not_dropped_when_a_newer_round_exists() {
    let fixture = Fixture::new().await;
    let large = "unknown effect ".repeat(128);
    let mut transcript = vec![message(
        "unknown-run",
        1,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Earlier work")],
    )];
    let mut uncertain = round("unknown-run", 2, "unknown-call", &large, &fixture.tools[0]);
    let ContentBlock::ToolResult { result } = &mut uncertain[1].content[0] else {
        unreachable!()
    };
    result.status = ToolResultStatus::Unknown;
    transcript.extend(uncertain);
    transcript.push(message(
        "unknown-run",
        4,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Effect not confirmed")],
    ));
    transcript.push(message(
        "recent-run",
        5,
        MessageRole::User,
        MessageOrigin::User,
        vec![text("Recent work")],
    ));
    transcript.extend(round(
        "recent-run",
        6,
        "recent-call",
        "Recent observation",
        &fixture.tools[0],
    ));
    transcript.push(message(
        "recent-run",
        8,
        MessageRole::Assistant,
        MessageOrigin::Model,
        vec![text("Recent answer")],
    ));
    transcript.push(fixture.current_message(9));
    let full = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    assert!(full.selected_message_ids.contains(&id("message-3")));
    let mut limited = fixture.input(&transcript, &[], &[]);
    limited.limits.max_bytes = serde_json::to_vec(&full.request).unwrap().len() - large.len() / 2;
    assert_eq!(
        ContextAssembler::new()
            .project(&fixture.prompt, limited)
            .unwrap_err()
            .code,
        ErrorCode::ContextBudgetExceeded
    );
}

#[tokio::test]
async fn loaded_skill_context_uses_its_pinned_version_without_gaining_system_authority() {
    let fixture = Fixture::new().await;
    let transcript = vec![fixture.current_message(1)];
    let baseline = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &[], &[]))
        .unwrap();
    let loaded = ContextItem::new(
        id("loaded-skill"),
        ContextOrigin::Skill,
        fixture.skill.skill.clone(),
        fixture.scope.clone(),
        vec![InputContent::Text {
            text: "Loaded task instructions".into(),
        }],
        ContextLifetime::Run {
            run_id: fixture.run_id.clone(),
        },
        ContextPriority::Required,
    );
    let items = vec![loaded.clone()];
    let projected = ContextAssembler::new()
        .project(&fixture.prompt, fixture.input(&transcript, &items, &[]))
        .unwrap();
    assert_eq!(projected.selected_context_ids, vec![id("loaded-skill")]);
    let system = |request: ModelRequest| {
        request
            .messages
            .into_iter()
            .filter(|message| message.role == ModelRole::System)
            .collect::<Vec<_>>()
    };
    assert_eq!(system(projected.request), system(baseline.request));
    let changed = ContextItem::new(
        loaded.item_id,
        loaded.origin,
        VersionedRef {
            id: loaded.source_ref.id,
            version: id("2"),
        },
        loaded.scope,
        loaded.content,
        loaded.lifetime,
        loaded.priority_class,
    );
    assert!(
        ContextAssembler::new()
            .project(&fixture.prompt, fixture.input(&transcript, &[changed], &[]))
            .is_err()
    );
}
```

## `tests/support/context_consumer.rs`

```rust
use serde_json::json;
use std::collections::BTreeSet;
use wickle::*;

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifier")
}
fn reference(value: &str) -> VersionedRef {
    VersionedRef {
        id: id(value),
        version: id("1"),
    }
}
struct Catalog;
impl ProfileResolver for Catalog {
    fn resolve<'a>(
        &'a self,
        reference: &'a ComponentRef,
        _: &'a Scope,
    ) -> PortFuture<'a, ComponentMetadata> {
        Box::pin(async move {
            Ok(ComponentMetadata {
                reference: ComponentRef {
                    version: reference.version.clone().or_else(|| Some(id("1"))),
                    ..reference.clone()
                },
                contract_version: 1,
                manifest_digest: canonical_digest(&json!(reference.id)),
                config_schema: json!({"type":"object","additionalProperties":false}),
                dependencies: vec![],
                capabilities: BTreeSet::new(),
                required_capabilities: BTreeSet::new(),
                required_connections: BTreeSet::new(),
                model_name: (reference.kind == ComponentKind::Tool).then(|| id("search")),
                hook_position: None,
                exports: vec![],
            })
        })
    }
}
fn compiled_tool() -> Result<CompiledTool, ContractError> {
    let registry = SystemInputRegistry::new(vec![SystemInputDefinition {
        key: id("workspace_id"),
        version: id("1"),
        value_schema: json!({"type":"string"}),
        source: SystemInputSource::Run {},
    }])?;
    SchemaCompiler::new().compile(ToolDescriptor {
        tool: reference("search"), name: id("search"), description: "Search available evidence".into(),
        input_schema: json!({"type":"object","properties":{"query":{"type":"string"},"workspace_id":{"type":"string","description":"Internal workspace key"}},"required":["query","workspace_id"],"additionalProperties":false}),
        agent_parameters: vec!["query".into()], system_bindings: None, output_schema: json!({"type":"array"}), side_effect: ToolSideEffect::ReadOnly, concurrency: ToolConcurrency::Serial, retry: ToolRetryPolicy::Never, reconcile: false, max_output_bytes: 4096.try_into().unwrap(),
    }, &registry)
}
fn route() -> ResolvedModelRoute {
    ResolvedModelRoute {
        binding: reference("primary"),
        catalog_revision: id("catalog"),
        routing_policy_revision: id("policy"),
        requested_model: id("example"),
        model_id: id("example"),
        model_version: id("1"),
        version_semantics: VersionSemantics::Pinned,
        provider: id("example"),
        target: JsonObject::new(),
        deployment_revision: None,
        api_contract: ApiContract {
            operation: id("messages"),
            version: id("1"),
        },
        adapter: reference("example"),
        capability_revision: id("capabilities"),
        connection_ref: reference("connection"),
    }
}
fn request(run: &str, text: &str) -> RunRequest {
    RunRequest {
        request_id: id(&format!("request-{run}")),
        session_id: id("session"),
        input: vec![InputContent::Text { text: text.into() }],
        trigger: RunTrigger::User {},
        output_contract: None,
    }
}
fn admission(
    profile: &ResolvedProfile,
    prompt: &ProtectedRecord,
    run: &str,
    first_sequence: u64,
    started_at_ms: i64,
    text: &str,
) -> AdmissionInput {
    let request = request(run, text);
    let request_record = ProtectedRecord::new(
        id(&format!("input-{run}")),
        1,
        serde_json::to_value(&request).unwrap(),
    );
    let scope = profile.scope().clone();
    let snapshot = RunSnapshot {
        schema_version: RunSnapshotSchemaVersion::V1,
        run_id: id(run),
        request_digest: admission_digest(&request, profile, None),
        request: request.clone(),
        scope: scope.clone(),
        profile: profile.clone(),
        status: RunStatus::Running,
        phase: RunPhase::Admission,
        model_step_id: None,
        limits: profile.profile().limits.clone(),
        usage: BudgetUsage::default(),
        timing: RunTiming::new(started_at_ms, 10000).unwrap(),
        reservations: vec![],
        model_ledger: vec![],
        tool_ledger: vec![],
        system_inputs: None,
        wait: None,
        outcome: None,
        assembly_ref: None,
        context_batches: vec![],
        source_states: vec![],
        revision: 0,
        last_event_seq: 1,
    };
    AdmissionInput {
        snapshot,
        prompt_snapshot: prompt.reference().clone(),
        require_durable: false,
        messages: vec![Message {
            message_id: id(&format!("user-{run}")),
            run_id: id(run),
            sequence: first_sequence.try_into().unwrap(),
            role: MessageRole::User,
            content: request
                .input
                .into_iter()
                .map(|content| ContentBlock::Content { content })
                .collect(),
            origin: MessageOrigin::User,
            visibility: Visibility::UserAndModel,
        }],
        events: vec![RunEvent {
            schema_version: RunEventSchemaVersion::V1,
            event_id: id(&format!("start-{run}")),
            scope,
            run_id: id(run),
            session_id: id("session"),
            seq: 1.try_into().unwrap(),
            timestamp_ms: started_at_ms,
            payload: RunEventPayload::RunStarted {
                request_ref: request_record.reference().clone(),
                profile_digest: profile.profile_digest().clone(),
            },
        }],
        records: vec![request_record, prompt.clone()],
    }
}
fn project(
    prompt: &PromptSnapshot,
    stored: &StoredRun,
) -> Result<ContextProjection, ContractError> {
    let step = id(&format!("step-{}", stored.snapshot.run_id));
    ContextAssembler::new().project(
        prompt,
        ProjectionInput {
            profile: &stored.snapshot.profile,
            scope: &stored.snapshot.scope,
            run_id: &stored.snapshot.run_id,
            model_step_id: &step,
            current_request: &stored.snapshot.request,
            current_request_message_id: &id(&format!("user-{}", stored.snapshot.run_id)),
            transcript: &stored.messages,
            context_items: &[],
            opaque_records: &[],
            expected_prompt_digest: &stored.session.prompt_snapshot.digest,
            request_id: step.clone(),
            purpose: ModelPurpose::Agent,
            route: route(),
            output: ModelOutput::Text {},
            max_output_tokens: 128.try_into().unwrap(),
            response_limits: ModelResponseLimits {
                max_input_bytes: 32_768,
                max_response_bytes: 4096,
                max_delta_bytes: 1024,
                max_events: 16,
                max_tool_calls: 1,
            },
            limits: ProjectionLimits {
                max_bytes: 32_768,
                max_items: 30,
            },
        },
    )
}

fn projected_user_occurrences(projection: &ContextProjection, text: &str) -> usize {
    projection
        .request
        .messages
        .iter()
        .filter(|message| message.role == ModelRole::User)
        .flat_map(|message| &message.content)
        .filter(|content| matches!(content, ModelContent::Text { text: value } if value == text))
        .count()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope {
        tenant_id: id("tenant"),
        workspace_id: id("workspace"),
        user_id: None,
    };
    let profile = AgentProfile::from_json(
        r#"{
      "schema_version":"wickle.agent-profile.v1","agent_id":"assistant","version":"1",
      "name":"Assistant","description":"Context example","instructions":{"text":"Summarize available evidence"},
      "model_binding":"primary","tools":[{"tool_id":"search","version":"1"}],"skills":[],"connectors":[],"context_policy":{"strategy":"bounded"},"output_contract":{"type":"text"},
      "limits":{"max_model_calls":4,"max_tool_attempts":2,"max_repair_attempts":0,"max_recovery_attempts":0,"max_elapsed_ms":10000}
    }"#,
    )?;
    let profile = ProfileValidator::new(&Catalog)
        .validate(&profile, &scope)
        .await?;
    let tool = compiled_tool()?;
    let prompt = PromptSnapshot::create(
        &profile,
        vec!["Only report actions supported by supplied observations.".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool.clone(),
        }],
        vec![],
    )?;
    let prompt_record = ProtectedRecord::new(id("prompt"), 1, serde_json::to_value(&prompt)?);
    assert_eq!(prompt.digest(), prompt_record.reference().digest);
    let store = MemoryStateStore::new();
    let mut first_input = admission(
        &profile,
        &prompt_record,
        "first",
        1,
        1000,
        "Review the available evidence",
    );
    first_input.messages.push(Message {
        message_id: id("private-state"),
        run_id: id("first"),
        sequence: 2.try_into()?,
        role: MessageRole::System,
        content: vec![ContentBlock::Content {
            content: InputContent::Json {
                value: json!({"workspace_id":"host-only-database-value"}),
            },
        }],
        origin: MessageOrigin::Host,
        visibility: Visibility::Internal,
    });
    let first = store.admit(&scope, first_input).await?.state;
    let first_projection = project(&prompt, &first)?;
    assert_eq!(first_projection.request.messages[0].role, ModelRole::System);
    assert_eq!(
        projected_user_occurrences(&first_projection, "Review the available evidence"),
        1
    );
    assert!(
        first_projection.request.tools[0].model_input_schema["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert!(
        !serde_json::to_string(&first_projection.request)?.contains("host-only-database-value")
    );
    assert_eq!(
        first_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-first"))
            .count(),
        1
    );

    // Finish this demonstration run without claiming that a model or tool executed.
    let lease = store
        .acquire_lease(&scope, &id("first"), &id("worker"), 1000, 1000)
        .await?;
    let mut snapshot = first.snapshot.clone();
    snapshot.revision = 1;
    snapshot.last_event_seq = 2;
    snapshot.status = RunStatus::Cancelled;
    snapshot.phase = RunPhase::Finish;
    snapshot.usage.elapsed_ms = 1;
    snapshot.timing.last_observed_at_ms = 1001;
    let outcome = RunOutcome {
        result: OutcomeResult::Cancelled {
            reason: "Demonstration complete".into(),
        },
        output: vec![],
        artifacts: vec![],
        usage: snapshot.usage.clone(),
        checkpoint_revision: 1,
        verification: None,
        unresolved_effects: vec![],
    };
    let outcome_record =
        ProtectedRecord::new(id("outcome-first"), 1, serde_json::to_value(&outcome)?);
    snapshot.outcome = Some(outcome);
    store
        .commit(
            &scope,
            &id("first"),
            CommitInput {
                expected_revision: 0,
                lease,
                now_ms: 1001,
                snapshot,
                messages: vec![],
                records: vec![outcome_record.clone()],
                events: vec![RunEvent {
                    schema_version: RunEventSchemaVersion::V1,
                    event_id: id("finish-first"),
                    scope: scope.clone(),
                    run_id: id("first"),
                    session_id: id("session"),
                    seq: 2.try_into()?,
                    timestamp_ms: 1001,
                    payload: RunEventPayload::RunFinished {
                        outcome_ref: outcome_record.reference().clone(),
                    },
                }],
            },
        )
        .await?;

    let changed = PromptSnapshot::create(
        &profile,
        vec!["Changed operating policy".into()],
        None,
        vec![PromptToolBinding {
            selection: profile.profile().tools[0].clone(),
            compiled: tool,
        }],
        vec![],
    )?;
    let changed_record =
        ProtectedRecord::new(id("changed-prompt"), 1, serde_json::to_value(&changed)?);
    assert!(
        store
            .admit(
                &scope,
                admission(&profile, &changed_record, "wrong", 3, 1002, "Continue")
            )
            .await
            .is_err()
    );
    let second = store
        .admit(
            &scope,
            admission(
                &profile,
                &prompt_record,
                "second",
                3,
                1002,
                "Now give a concise summary",
            ),
        )
        .await?
        .state;
    let saved_prompt = store
        .read_record(&scope, &second.session.prompt_snapshot)
        .await?;
    let restored = PromptSnapshot::restore(
        &serde_json::to_string(saved_prompt.value())?,
        &second.session.prompt_snapshot.digest,
        &second.snapshot.profile,
        &scope,
    )?;
    let second_projection = project(&restored, &second)?;
    assert_eq!(
        projected_user_occurrences(&second_projection, "Now give a concise summary"),
        1
    );
    assert_eq!(
        first_projection.prompt_digest,
        second_projection.prompt_digest
    );
    assert_eq!(
        first_projection.request.tools,
        second_projection.request.tools
    );
    assert_eq!(
        second_projection
            .selected_message_ids
            .iter()
            .filter(|message| **message == id("user-second"))
            .count(),
        1
    );
    assert!(
        !serde_json::to_string(&second_projection.request)?.contains("host-only-database-value")
    );
    println!(
        "context consumer: two stored runs share the pinned prompt/tool schema; changed prompt refused; current request appears once; internal execution data excluded"
    );
    Ok(())
}
```
