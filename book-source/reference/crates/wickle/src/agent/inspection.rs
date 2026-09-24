use super::*;
use crate::inspection::{StoredInspection, compose};
use serde::de::DeserializeOwned;

impl Agent {
    /// Inspect saved composition under current permission. This performs only
    /// policy checks and storage reads; it never prepares or executes a step.
    pub async fn inspect_step(
        &self,
        run_id: &Id,
        step: StepRef,
        context: &ExecutionContext,
        options: InspectionOptions,
    ) -> Result<Guarded<CompositionReport>, ContractError> {
        self.check_scope(context)?;
        let timeout = Duration::from_millis(self.inner.bindings.settings.start_timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut request = PolicyRequest {
            owner_scope: self.inner.bindings.scope.clone(),
            resource_id: run_id.clone(),
            action: PolicyAction::InspectStep {
                step: step.clone(),
                options,
                context_fragments: vec![],
            },
        };
        if let Guarded::ApprovalRequired(challenge) = self
            .inner
            .bindings
            .policy
            .guard(&request, context, Some(deadline), None, || async { Ok(()) })
            .await?
        {
            return Ok(Guarded::ApprovalRequired(challenge));
        }
        let (report, context_fragments) = caller_read(
            context,
            Some(timeout),
            crate::future::boxed(|| self.inspect_records(run_id, step, context, options, deadline)),
        )
        .await?;
        if let PolicyAction::InspectStep {
            context_fragments: included,
            ..
        } = &mut request.action
        {
            *included = context_fragments;
        }
        self.inner
            .bindings
            .policy
            .guard(&request, context, Some(deadline), None, || async {
                Ok(report)
            })
            .await
    }

    async fn inspect_records(
        &self,
        run_id: &Id,
        step: StepRef,
        context: &ExecutionContext,
        options: InspectionOptions,
        deadline: tokio::time::Instant,
    ) -> Result<(CompositionReport, Vec<InspectionFragmentRef>), ContractError> {
        let bindings = &self.inner.bindings;
        let saved = match bindings.state.load(&bindings.scope, run_id).await {
            Ok(saved) => saved,
            Err(error) if missing_reason(error.code).is_some() => {
                return Ok((unavailable(run_id, step, error.code, "run", None), vec![]));
            }
            Err(error) => return Err(error),
        };
        if saved.snapshot.scope != bindings.scope || saved.snapshot.run_id != *run_id {
            return Err(fail(ErrorCode::InvalidSnapshot, "inspection.run"));
        }
        let mut lookup_gaps = vec![];
        let mut selected = None;
        for reference in &saved.snapshot.prepared_steps {
            if matches!(&step, StepRef::Prepared { record_id } if *record_id != reference.record_id)
            {
                continue;
            }
            let root: Option<PreparedStepRecord> = self
                .inspection_record(reference, "preparation", &mut lookup_gaps)
                .await?;
            let Some(root) = root else { continue };
            if root.scope != bindings.scope
                || root.run_id != *run_id
                || root.profile_digest != *saved.snapshot.profile.profile_digest()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.preparation"));
            }
            let matched = match &step {
                StepRef::Prepared { record_id } => *record_id == reference.record_id,
                StepRef::Logical {
                    model_step_id,
                    purpose,
                    projection_revision,
                } => {
                    root.model_step_id == *model_step_id
                        && root.purpose == *purpose
                        && root.projection_revision == *projection_revision
                }
            };
            if matched {
                selected = Some((reference.clone(), root));
                break;
            }
        }
        let Some((reference, root)) = selected else {
            let status = if lookup_gaps.is_empty() {
                InspectionStatus::NotFound
            } else if matches!(step, StepRef::Prepared { .. })
                && lookup_gaps.iter().any(|gap| gap.reason == "expired")
            {
                InspectionStatus::Expired
            } else if matches!(step, StepRef::Prepared { .. }) {
                InspectionStatus::NotFound
            } else {
                InspectionStatus::Partial
            };
            return Ok((
                CompositionReport {
                    schema_version: "wickle.composition-report.v1",
                    run_id: run_id.clone(),
                    step,
                    status,
                    composition: None,
                    unresolved: lookup_gaps,
                },
                vec![],
            ));
        };
        let mut unresolved = vec![UnresolvedInspectionField {
            field: "estimator".into(),
            record_id: None,
            reason: "not_recorded".into(),
        }];
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigurationRecord {
            route: ResolvedModelRoute,
            configuration: ModelConfiguration,
        }
        let config: Option<ConfigurationRecord> = self
            .inspection_record(
                &root.model_configuration,
                "model_configuration",
                &mut unresolved,
            )
            .await?;
        let projection: Option<PreparedModelProjection> = self
            .inspection_record(&root.context_projection, "projection", &mut unresolved)
            .await?;
        if projection.as_ref().is_some_and(|projection| {
            projection.scope != bindings.scope
                || projection.run_id != *run_id
                || projection.request.request_id != root.model_step_id
                || projection.request.purpose != root.purpose
                || projection.fingerprint() != root.projection_fingerprint
        }) {
            return Err(fail(ErrorCode::InvalidSnapshot, "inspection.projection"));
        }
        if let (Some(config), Some(projection)) = (&config, &projection) {
            if config.route != projection.request.route
                || config.configuration.effective != projection.request.options
                || config.configuration.max_output_tokens != projection.request.max_output_tokens
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.configuration"));
            }
        }
        let route = config
            .as_ref()
            .map(|record| record.route.clone())
            .or_else(|| {
                projection
                    .as_ref()
                    .map(|projection| projection.request.route.clone())
            });
        let tool_set: Option<ResolvedToolSet> = self
            .inspection_record(&root.tool_set, "tool_set", &mut unresolved)
            .await?;
        let mut tools = vec![];
        if let Some(tool_set) = tool_set {
            if tool_set.scope != bindings.scope
                || tool_set.run_id != *run_id
                || tool_set.entries.len() != root.compiled_tools.len()
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.tool_set"));
            }
            for (entry, reference) in tool_set.entries.into_iter().zip(&root.compiled_tools) {
                let value = self
                    .inspection_record::<serde_json::Value>(
                        reference,
                        "tool_compilation",
                        &mut unresolved,
                    )
                    .await?;
                let compiled = value.map(CompiledToolContract::inspection).transpose()?;
                if let Some(compiled) = &compiled {
                    if compiled.tool != entry.manifest.tool
                        || compiled.canonical_name != entry.manifest.model_tool.name
                        || compiled.canonical_schema_digest != entry.manifest.model_schema_digest
                        || route.as_ref().is_some_and(|route| {
                            compiled.target.provider != route.provider
                                || compiled.target.api_contract != route.api_contract
                                || compiled.target.capability_revision != route.capability_revision
                        })
                    {
                        return Err(fail(
                            ErrorCode::InvalidSnapshot,
                            "inspection.tool_compilation",
                        ));
                    }
                }
                tools.push((entry.manifest, compiled));
            }
        }
        let mut attempts = vec![];
        for invocation in &saved.snapshot.model_ledger {
            if invocation.prepared_step_ref.as_ref() != Some(&reference) {
                continue;
            }
            if invocation.model_step_id != root.model_step_id
                || invocation.purpose != root.purpose
                || invocation.run_id != *run_id
            {
                return Err(fail(ErrorCode::InvalidSnapshot, "inspection.invocation"));
            }
            let mut observed = matches!(invocation.state, ModelAttemptState::Completed {})
                || invocation.provider_request_id.is_some()
                || invocation.reported_model_id.is_some()
                || invocation.reported_model_version.is_some()
                || invocation
                    .usage
                    .as_ref()
                    .is_some_and(|usage| usage.measurement == UsageMeasurement::Reported);
            if let Some(response_ref) = &invocation.response_ref {
                let response: Option<StoredModelResponse> = self
                    .inspection_record(response_ref, "response", &mut unresolved)
                    .await?;
                if let Some(response) = response {
                    if response.request_id != invocation.attempt_id
                        || response.route_digest != invocation.route.digest()
                    {
                        return Err(fail(ErrorCode::InvalidSnapshot, "inspection.response"));
                    }
                    observed |= match response.outcome {
                        ModelExchangeOutcome::Completed { .. } => true,
                        ModelExchangeOutcome::Failed { failure } => {
                            !failure.partial_text().is_empty()
                                || failure.metadata.provider_request_id.is_some()
                                || failure.metadata.reported_model_id.is_some()
                                || failure.metadata.reported_model_version.is_some()
                                || failure.metadata.usage.as_ref().is_some_and(|usage| {
                                    usage.measurement == UsageMeasurement::Reported
                                })
                        }
                    };
                }
            }
            attempts.push((invocation.clone(), observed));
        }
        let mut disclosure = BTreeMap::new();
        let mut context_fragments = vec![];
        let mut displayed_bytes = 0usize;
        if let Some(projection) = &projection {
            for fragment in &projection.provenance.fragments {
                if fragment.identity.scope != bindings.scope {
                    return Err(fail(
                        ErrorCode::InvalidSnapshot,
                        "inspection.fragment_scope",
                    ));
                }
                let display = match &fragment.value {
                    FragmentValue::Item { item } if options.include_context_content => {
                        let request = PolicyRequest {
                            owner_scope: bindings.scope.clone(),
                            resource_id: run_id.clone(),
                            action: PolicyAction::InspectContextFragment {
                                identity: Box::new(fragment.identity.clone()),
                                core_revision: fragment.core_revision,
                                content_digest: fragment.content_digest.clone(),
                            },
                        };
                        match bindings
                            .policy
                            .check(&request, context, Some(deadline), None)
                            .await?
                        {
                            PolicyDecision::Allow {} => {
                                let bytes = serde_json::to_vec(&item.content)
                                    .map_err(|_| {
                                        fail(ErrorCode::InvalidSnapshot, "inspection.fragment")
                                    })?
                                    .len();
                                if bytes > 65_536usize.saturating_sub(displayed_bytes) {
                                    ContextDisclosure::Redacted {
                                        reason: "display_limit".into(),
                                    }
                                } else {
                                    displayed_bytes += bytes;
                                    ContextDisclosure::Included {
                                        content: item.content.clone(),
                                    }
                                }
                            }
                            PolicyDecision::Deny { .. } => ContextDisclosure::Redacted {
                                reason: "access_denied".into(),
                            },
                            PolicyDecision::RequireApproval { .. } => ContextDisclosure::Redacted {
                                reason: "approval_required".into(),
                            },
                        }
                    }
                    FragmentValue::Item { .. } => ContextDisclosure::Redacted {
                        reason: "not_requested".into(),
                    },
                    _ => ContextDisclosure::NotApplicable,
                };
                if matches!(display, ContextDisclosure::Included { .. }) {
                    context_fragments.push(InspectionFragmentRef {
                        identity: fragment.identity.clone(),
                        core_revision: fragment.core_revision,
                        content_digest: fragment.content_digest.clone(),
                    });
                }
                disclosure.insert(crate::inspection::disclosure_key(fragment), display);
            }
        }
        Ok((
            compose(
                step,
                StoredInspection {
                    reference,
                    root,
                    route,
                    configuration: config.map(|record| record.configuration),
                    tools,
                    projection,
                    attempts,
                    disclosure,
                    limits: saved.snapshot.limits,
                    status: saved.snapshot.status,
                    revision: saved.snapshot.revision,
                    outcome: saved.snapshot.outcome,
                    unresolved,
                },
            ),
            context_fragments,
        ))
    }

    async fn inspection_record<T: DeserializeOwned>(
        &self,
        reference: &RecordRef,
        field: &str,
        unresolved: &mut Vec<UnresolvedInspectionField>,
    ) -> Result<Option<T>, ContractError> {
        match self
            .inner
            .bindings
            .state
            .read_record(&self.inner.bindings.scope, reference)
            .await
        {
            Ok(record) => {
                if record.reference() != reference {
                    return Err(fail(
                        ErrorCode::InvalidSnapshot,
                        "inspection.record_identity",
                    ));
                }
                serde_json::from_value(record.value().clone())
                    .map(Some)
                    .map_err(|_| fail(ErrorCode::InvalidSnapshot, "inspection.record"))
            }
            Err(error) if missing_reason(error.code).is_some() => {
                unresolved.push(UnresolvedInspectionField {
                    field: field.into(),
                    record_id: Some(reference.record_id.clone()),
                    reason: missing_reason(error.code)
                        .expect("matched missing code")
                        .into(),
                });
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
fn missing_reason(code: ErrorCode) -> Option<&'static str> {
    match code {
        ErrorCode::StateNotFound => Some("not_found"),
        ErrorCode::RecordExpired => Some("expired"),
        _ => None,
    }
}
fn unavailable(
    run_id: &Id,
    step: StepRef,
    code: ErrorCode,
    field: &str,
    record_id: Option<Id>,
) -> CompositionReport {
    CompositionReport {
        schema_version: "wickle.composition-report.v1",
        run_id: run_id.clone(),
        step,
        status: if code == ErrorCode::RecordExpired {
            InspectionStatus::Expired
        } else {
            InspectionStatus::NotFound
        },
        composition: None,
        unresolved: vec![UnresolvedInspectionField {
            field: field.into(),
            record_id,
            reason: missing_reason(code).unwrap_or("not_found").into(),
        }],
    }
}
