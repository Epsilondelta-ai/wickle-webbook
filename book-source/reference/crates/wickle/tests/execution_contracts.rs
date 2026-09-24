//! Execution-record invariants at the persistence boundary.
use serde_json::json;
use wickle::*;
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn profile() -> VersionedRef {
    VersionedRef {
        id: id("agent"),
        version: id("1"),
    }
}
#[test]
fn submitted_snapshot_preserves_exact_numbers_and_normalizes_only_empty_overrides() {
    let a = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{},"input":184467440737095516160001}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    let b = RequestSnapshot::capture(
        profile(),
        r#"{"input":184467440737095516160001}"#,
        Some("{}"),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_eq!(a.digest(), b.digest());
    assert!(!a.system_inputs_provided());
    assert!(b.system_inputs_provided());
    let restored: RequestSnapshot =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    restored.validate(JsonTextLimits::default()).unwrap();
    assert_eq!(
        restored.request_json(),
        r#"{"input":184467440737095516160001,"model_options":{}}"#
    );
    let other = RequestSnapshot::capture(
        profile(),
        r#"{"input":184467440737095516160002}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_ne!(a.digest(), other.digest());
    assert!(
        RequestSnapshot::capture(
            profile(),
            r#"{"model_options":null}"#,
            None,
            JsonTextLimits::default()
        )
        .is_err()
    );
    assert!(
        RequestSnapshot::capture(profile(), "{}", Some("null"), JsonTextLimits::default()).is_err()
    );
}
#[test]
fn snapshots_reject_tampering_and_debug_omits_protected_values() {
    let a = RequestSnapshot::capture(
        profile(),
        "{}",
        Some(r#"{"workspace_id":"private-workspace-uuid"}"#),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert!(!format!("{a:?}").contains("private-workspace-uuid"));
    let mut changed = serde_json::to_value(a).unwrap();
    changed["system_inputs_json"] = json!("{}");
    let b: RequestSnapshot = serde_json::from_value(changed.clone()).unwrap();
    assert!(b.validate(JsonTextLimits::default()).is_err());
    changed["schema_version"] = json!("future");
    assert!(serde_json::from_value::<RequestSnapshot>(changed).is_err());
}
#[test]
fn interrupted_segments_require_matching_recoverable_evidence() {
    let mut segment = ExecutionSegment {
        last_event_seq: None,
        source_snapshot_ref: None,
        schema_version: ExecutionRecordVersion::V1,
        execution_principal_ref: id("original-user"),
        run_id: id("run"),
        segment_id: id("segment"),
        accepted_revision: 4,
        app_state: None,
        outcome: Some(SegmentOutcome::Interrupted {
            interruption: InterruptionRecord {
                segment_id: id("segment"),
                cause: InterruptionCause::HostShutdown,
                checkpoint_revision: 5,
                recoverable: true,
                unresolved_effects: vec![],
            },
        }),
    };
    segment.validate().unwrap();
    let saved = serde_json::to_string(&segment).unwrap();
    let replay: ExecutionSegment = serde_json::from_str(&saved).unwrap();
    replay.validate().unwrap();
    assert_eq!(segment, replay);
    if let Some(SegmentOutcome::Interrupted { interruption }) = &mut segment.outcome {
        interruption.cause = InterruptionCause::UserCancel;
    }
    assert!(segment.validate().is_err());
    if let Some(SegmentOutcome::Interrupted { interruption }) = &mut segment.outcome {
        interruption.cause = InterruptionCause::HostShutdown;
        interruption.segment_id = id("other");
    }
    assert!(segment.validate().is_err());
    assert!(!RunStatus::Interrupted.is_terminal());
}
#[test]
fn output_cap_preserves_omission_and_rejects_null_zero_or_fraction() {
    let base = json!({"request_id":"r","session_id":"s","input":[],"trigger":{"kind":"user"}});
    let original = RunRequest::from_json(&base.to_string()).unwrap();
    assert!(original.max_output_tokens.is_none());
    assert!(
        serde_json::to_value(original)
            .unwrap()
            .get("max_output_tokens")
            .is_none()
    );
    for cap in [json!(null), json!(0), json!(-1), json!(1.5)] {
        let mut input = base.clone();
        input["max_output_tokens"] = cap;
        assert!(RunRequest::from_json(&input.to_string()).is_err());
    }
    let mut input = base;
    input["max_output_tokens"] = json!(42);
    assert_eq!(
        RunRequest::from_json(&input.to_string())
            .unwrap()
            .max_output_tokens
            .unwrap()
            .get(),
        42
    );
}

#[test]
fn replay_uses_stored_canonicalization_instead_of_candidate_digest() {
    let original = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{"temperature":1e3}}"#,
        None,
        JsonTextLimits::default(),
    )
    .unwrap();
    let envelope = format!(
        "{{\"profile_ref\":{},\"request\":{},\"system_inputs\":{{}}}}",
        serde_json::to_string(&profile()).unwrap(),
        original.request_json()
    );
    let mut stored = serde_json::to_value(&original).unwrap();
    stored["canonicalization"] = json!("sorted-json-v1");
    stored["digest"] = json!(
        versioned_digest_json(
            &envelope,
            CanonicalizationVersion::SortedJsonV1,
            JsonTextLimits::default()
        )
        .unwrap()
    );
    let stored: RequestSnapshot = serde_json::from_value(stored).unwrap();
    let candidate = RequestSnapshot::capture(
        profile(),
        r#"{"model_options":{"temperature":1000.0}}"#,
        Some("{}"),
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_ne!(stored.digest(), candidate.digest());
    assert!(
        stored
            .matches_submission(&candidate, JsonTextLimits::default())
            .unwrap()
    );
    assert!(
        !original
            .matches_submission(&candidate, JsonTextLimits::default())
            .unwrap()
    );
}
