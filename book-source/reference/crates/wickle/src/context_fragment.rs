//! Core-issued context observations, independent of opaque external revisions.
use crate::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, num::NonZeroU64};

/// Pinned assembly algorithm for newly recorded context fragments.
pub const CONTEXT_FRAGMENT_ASSEMBLER: &str = "wickle.context-fragments.v1";
/// Namespace owner. Step fragments retain their Run owner and a separate step lifetime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FragmentOwner {
    /// One immutable-profile conversation.
    Session {
        /// Session identity.
        session_id: Id,
    },
    /// One execution, including its logical model steps.
    Run {
        /// Run identity.
        run_id: Id,
    },
}
/// Stable identity; observation time, batch and external revision are not identity keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FragmentIdentity {
    /// Exact authenticated namespace.
    pub scope: Scope,
    /// Session or Run ownership.
    pub owner: FragmentOwner,
    /// Core-qualified producer selection, including trigger where applicable.
    pub producer_id: Id,
    /// Producer-local fragment identity.
    pub fragment_id: Id,
}
impl FragmentIdentity {
    /// Stable lookup key, independent of the external revision's spelling or ordering.
    pub fn key(&self) -> JsonDigest {
        crate::serialization::data_digest(self)
    }
}
/// Protected fragment content or an explicit selection withdrawal.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FragmentValue {
    /// Model-visible data; validation and current source authorization remain required.
    Item {
        /// Frozen namespaced item.
        item: ContextItem,
    },
    /// Removal/empty/unavailable selection, never a fallback to older content.
    Tombstone {
        /// Safe core-issued classification.
        reason: Id,
    },
    /// Complete source-slot selection, including successful empty observations.
    Selection {
        /// Ready, empty or unavailable.
        status: Id,
        /// Native source item identities in the committed provider order.
        item_ids: Vec<Id>,
    },
}
/// One committed observation. The store validates increasing revisions under its CAS.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextFragment {
    /// Stable producer/owner/item identity.
    pub identity: FragmentIdentity,
    /// First observation is 1; every new activation or withdrawal advances it.
    pub core_revision: NonZeroU64,
    /// Content/selection, provenance and assembler identity, excluding observation metadata.
    pub content_digest: JsonDigest,
    /// Opaque external version at this fragment's scope; None means unknown.
    pub source_revision: Option<Id>,
    /// Exact assembly algorithm, not an implicit latest version.
    pub assembler_version: Id,
    /// Core source batch responsible for this observation.
    pub batch_id: Id,
    /// Data classification; external providers cannot promote it to instructions.
    pub origin: ContextOrigin,
    /// Frozen data or withdrawal.
    pub value: FragmentValue,
}
impl fmt::Debug for ContextFragment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextFragment")
            .field("identity", &self.identity)
            .field("core_revision", &self.core_revision)
            .field("content_digest", &self.content_digest)
            .finish_non_exhaustive()
    }
}
impl ContextFragment {
    pub(crate) fn issue(
        identity: FragmentIdentity,
        previous: Option<&Self>,
        batch_id: Id,
        origin: ContextOrigin,
        source_revision: Option<Id>,
        value: FragmentValue,
    ) -> Result<Self, ContractError> {
        if previous.is_some_and(|prior| prior.identity != identity) {
            return Err(invalid("context.fragment_identity"));
        }
        let core_revision = previous
            .map_or(Some(1), |prior| prior.core_revision.get().checked_add(1))
            .and_then(NonZeroU64::new)
            .ok_or_else(|| invalid("context.fragment_revision"))?;
        let mut fragment = Self {
            identity,
            core_revision,
            content_digest: crate::canonical_digest(&serde_json::Value::Null),
            source_revision,
            assembler_version: Id::new(CONTEXT_FRAGMENT_ASSEMBLER)?,
            batch_id,
            origin,
            value,
        };
        fragment.content_digest = fragment.calculate_digest();
        fragment.validate()?;
        Ok(fragment)
    }
    fn calculate_digest(&self) -> JsonDigest {
        let content = match &self.value {
            // The transient namespaced item ID and current model step are observation
            // metadata. The stable identity supplies scope and lifetime owner.
            FragmentValue::Item { item } => {
                serde_json::json!({"type":"item","source_ref":item.source_ref,"content":item.content,"priority":item.priority_class,"lifetime_kind": match item.lifetime { ContextLifetime::Session {..} => "session", ContextLifetime::Run {..} => "run", ContextLifetime::Step {..} => "step" }})
            }
            value => serde_json::to_value(value).expect("fragment metadata serialization"),
        };
        crate::serialization::data_digest(&(
            &self.identity,
            self.origin,
            &self.source_revision,
            &self.assembler_version,
            content,
        ))
    }
    /// Check intrinsic identity and integrity. This does not establish access permission.
    pub fn validate(&self) -> Result<(), ContractError> {
        if self.assembler_version.as_str() != CONTEXT_FRAGMENT_ASSEMBLER
            || self.content_digest != self.calculate_digest()
        {
            return Err(invalid("context.fragment_digest"));
        }
        if let FragmentValue::Item { item } = &self.value {
            let owner = match &item.lifetime {
                ContextLifetime::Session { session_id } => FragmentOwner::Session {
                    session_id: session_id.clone(),
                },
                ContextLifetime::Run { run_id } | ContextLifetime::Step { run_id, .. } => {
                    FragmentOwner::Run {
                        run_id: run_id.clone(),
                    }
                }
            };
            if item.scope != self.identity.scope
                || owner != self.identity.owner
                || item.origin != self.origin
                || !item.valid_digest()
            {
                return Err(invalid("context.fragment_item"));
            }
        }
        Ok(())
    }
}
/// Select newest core observations while retaining their committed input order.
/// Tombstones remove content, and conflicting copies of one revision are errors.
/// Callers must authorize the corresponding source records before projection.
pub fn select_context_fragments(
    fragments: &[ContextFragment],
    scope: &Scope,
) -> Result<Vec<ContextItem>, ContractError> {
    let mut selected: BTreeMap<String, (usize, &ContextFragment)> = BTreeMap::new();
    for (index, fragment) in fragments.iter().enumerate() {
        fragment.validate()?;
        if &fragment.identity.scope != scope {
            return Err(ContractError::new(
                ErrorCode::AccessDenied,
                "context.fragment_scope",
            ));
        }
        let key = fragment.identity.key().to_string();
        if let Some((_, prior)) = selected.get(&key) {
            if fragment.core_revision == prior.core_revision && fragment != *prior {
                return Err(invalid("context.fragment_conflict"));
            }
            if fragment.core_revision <= prior.core_revision {
                continue;
            }
        }
        selected.insert(key, (index, fragment));
    }
    let mut ordered: Vec<_> = selected.into_values().collect();
    ordered.sort_by_key(|(index, _)| *index);
    Ok(ordered
        .into_iter()
        .filter_map(|(_, fragment)| {
            if let FragmentValue::Item { item } = &fragment.value {
                Some(item.clone())
            } else {
                None
            }
        })
        .collect())
}
fn invalid(path: &str) -> ContractError {
    ContractError::new(ErrorCode::InvalidContext, path)
}
