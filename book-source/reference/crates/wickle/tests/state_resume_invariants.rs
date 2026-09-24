//! Replay and transaction checks use actual waiting Agent executions as their seed.

#[path = "support/agent.rs"]
#[allow(dead_code)]
mod agent_support;
#[path = "support/agent_resume.rs"]
#[allow(dead_code)]
mod support;

use futures_util::stream;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use support::*;
use wickle::*;

fn restore(image: &Value) -> Result<MemoryStateStore, ContractError> {
    StateStoreCheckpoint::from_json(&image.to_string(), &scope(), &canonical_digest(image))
        .map(MemoryStateStore::from_checkpoint)
}

fn replace_reference(value: &mut Value, old: &Value, new: &Value) {
    if value == old {
        *value = new.clone();
        return;
    }
    match value {
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| replace_reference(item, old, new)),
        Value::Object(fields) => fields
            .values_mut()
            .for_each(|item| replace_reference(item, old, new)),
        _ => {}
    }
}

fn replace_record(image: &mut Value, reference: &Value, value: Value) {
    let record = image["records"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|record| &record["reference"] == reference)
        .unwrap();
    record["value"] = value.clone();
    let mut updated = reference.clone();
    updated["digest"] = serde_json::to_value(canonical_digest(&value)).unwrap();
    replace_reference(image, reference, &updated);
}

struct TwoQuestions(AtomicUsize);
impl ModelPort for TwoQuestions {
    fn binding(&self) -> ModelPortBinding {
        ModelPortBinding {
            provider: id("fixture"),
            adapter: reference("adapter"),
            connection_ref: reference("connection"),
        }
    }
    fn generate<'a>(
        &'a self,
        _: &'a ModelRequest,
        _: &'a ModelCallContext,
    ) -> PortStream<'a, ModelEvent> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        let (event, finish) = if call < 2 {
            (
                ModelEvent::ToolArgumentsDelta {
                    index: 0,
                    provider_call_id: Some(format!("question-{call}")),
                    name: Some("target".into()),
                    delta: json!({"query":"source"}).to_string(),
                },
                ModelFinish::ToolCalls,
            )
        } else {
            (
                ModelEvent::TextDelta {
                    text: "Both source choices are saved.".into(),
                },
                ModelFinish::Stop,
            )
        };
        Box::pin(stream::iter(vec![
            Ok(event),
            Ok(ModelEvent::ResponseCompleted {
                finish,
                metadata: ModelResponseMetadata::default(),
                continuation: vec![],
            }),
        ]))
    }
}

#[tokio::test]
async fn restored_resume_receipts_have_distinct_events_and_the_exact_preceding_wait() {
    let fixture = Fixture::new(Mode::Input);
    let mut bindings = fixture.bindings();
    let model = Arc::new(TwoQuestions(AtomicUsize::new(0)));
    bindings.model_exchange = Arc::new(
        ModelExchange::new(model.clone(), bindings.policy.clone())
            .with_route_inspector(
                fixture.base.inspector.clone(),
                std::time::Duration::from_secs(1),
            )
            .unwrap(),
    );
    let agent = create_agent(fixture.profile.clone(), bindings).unwrap();
    let mut handle = fixture.started(&agent).await;
    for (number, selection) in ["annual", "quarterly"].into_iter().enumerate() {
        assert_eq!(
            fixture.outcome(&handle).await.result.status(),
            RunStatus::Waiting
        );
        let saved = fixture.saved(&handle).await;
        let command = ResumeCommand {
            run_id: handle.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id(&format!("answer-{number}")),
            action: ResumeAction::Input {
                wait_id: saved.snapshot.wait.unwrap().wait_id,
                answer: json!({"selection":selection}),
            },
        };
        handle = completed(agent.resume(command, context()).await.unwrap());
    }
    assert_eq!(
        fixture.outcome(&handle).await.result.status(),
        RunStatus::Succeeded
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 3);
    assert_eq!(fixture.tools[1].calls.load(Ordering::SeqCst), 2);
    let image =
        serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    let restored = restore(&image).unwrap();
    assert_eq!(
        restored.load(&scope(), handle.run_id()).await.unwrap(),
        fixture.saved(&handle).await
    );
    for fault in 0..4 {
        let mut corrupt = image.clone();
        match fault {
            0 => {
                let events = corrupt["runs"][0]["events"].as_array_mut().unwrap();
                let indices: Vec<_> = events
                    .iter()
                    .enumerate()
                    .filter_map(|(i, event)| {
                        (event["payload"]["type"] == "run.resumed").then_some(i)
                    })
                    .collect();
                assert_eq!(indices.len(), 2);
                events[indices[1]]["payload"]["command_ref"] =
                    events[indices[0]]["payload"]["command_ref"].clone();
            }
            1 => {
                corrupt["runs"][0]["snapshot"]["resume_receipts"][0]["previous_last_event_seq"] =
                    json!(1)
            }
            2 => {
                let receipt = &mut corrupt["runs"][0]["snapshot"]["resume_receipts"][0];
                receipt["command"]["action"]["answer"] = json!({"selection":"quarterly"});
                let command = receipt["command"].clone();
                let reference = receipt["command_ref"].clone();
                replace_record(&mut corrupt, &reference, command);
            }
            3 => corrupt["runs"][0]["snapshot"]["resume_receipts"][0]["expired"] = json!(true),
            _ => unreachable!(),
        }
        assert!(
            restore(&corrupt).is_err(),
            "resume history fault {fault} was accepted"
        );
    }
}

#[tokio::test]
async fn an_external_correction_cannot_outlive_its_authorized_settlement_history() {
    let fixture = Fixture::new(Mode::External);
    let agent = fixture.agent();
    let original = fixture.started(&agent).await;
    assert_eq!(
        fixture.outcome(&original).await.result.status(),
        RunStatus::Waiting
    );
    let before = fixture.saved(&original).await;
    let command = ResumeCommand {
        run_id: original.run_id().clone(),
        expected_revision: before.snapshot.revision,
        command_id: id("external-proof"),
        action: ResumeAction::External {
            wait_id: before.snapshot.wait.unwrap().wait_id,
            receipt_ref: fixture.store.proof.reference().clone(),
        },
    };
    let resumed = completed(agent.resume(command, context()).await.unwrap());
    assert_eq!(
        fixture.outcome(&resumed).await.result.status(),
        RunStatus::Succeeded
    );
    let image =
        serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
    restore(&image).unwrap();
    let mut corrupt = image.clone();
    let messages = corrupt["sessions"][0]["messages"].as_array_mut().unwrap();
    let before = messages.len();
    messages.retain(|message| message["content"][0]["type"] != "tool_result_correction");
    assert_eq!(messages.len(), before - 1);
    for (index, message) in messages.iter_mut().enumerate() {
        message["sequence"] = json!(index + 1);
    }
    let count = messages.len();
    corrupt["sessions"][0]["snapshot"]["transcript_revision"] = json!(count);
    assert!(
        restore(&corrupt).is_err(),
        "settled effect restored without its correction"
    );
}

async fn replay_acceptance(before: &Value, after: &Value) -> (MemoryStateStore, CommitInput) {
    let store = restore(before).unwrap();
    let run_id: Id =
        serde_json::from_value(before["runs"][0]["snapshot"]["run_id"].clone()).unwrap();
    let saved = store.load(&scope(), &run_id).await.unwrap();
    let final_snapshot = RunSnapshot::from_json(&after["runs"][0]["snapshot"].to_string()).unwrap();
    let receipt = final_snapshot.resume_receipts[0].clone();
    let all_events: Vec<RunEvent> =
        serde_json::from_value(after["runs"][0]["events"].clone()).unwrap();
    let resumed = all_events
        .iter()
        .find(|event| {
            matches!(&event.payload,
        RunEventPayload::RunResumed { command_ref } if command_ref == &receipt.command_ref)
        })
        .unwrap();
    let events: Vec<_> = all_events
        .iter()
        .filter(|event| event.seq.get() > saved.snapshot.last_event_seq && event.seq <= resumed.seq)
        .cloned()
        .collect();
    assert_eq!(
        events.len(),
        2,
        "one settlement and one acceptance are expected"
    );
    let now_ms = resumed.timestamp_ms;
    let raw_lease = &before["runs"][0]["lease"];
    let lease = if raw_lease.is_null() {
        store
            .acquire_lease(&scope(), &run_id, &id("transaction-review"), now_ms, 60_000)
            .await
            .unwrap()
    } else {
        RunLease {
            scope: scope(),
            run_id: run_id.clone(),
            owner: serde_json::from_value(raw_lease["owner"].clone()).unwrap(),
            fencing_token: raw_lease["fencing_token"].as_u64().unwrap(),
            expires_at_ms: raw_lease["expires_at_ms"].as_i64().unwrap(),
        }
    };
    let mut snapshot = saved.snapshot.clone();
    snapshot.revision = receipt.accepted_revision;
    snapshot.status = RunStatus::Running;
    snapshot.phase = RunPhase::Tool;
    snapshot.wait = None;
    snapshot.outcome = None;
    snapshot.last_event_seq = resumed.seq.get();
    snapshot.usage.elapsed_ms = (now_ms - snapshot.timing.started_at_ms) as u64;
    snapshot.timing.last_observed_at_ms = now_ms;
    snapshot.resume_receipts.push(receipt);
    snapshot.tool_ledger[1].state = final_snapshot.tool_ledger[1].state.clone();
    let all_messages: Vec<Message> =
        serde_json::from_value(after["sessions"][0]["messages"].clone()).unwrap();
    let messages = all_messages
        .into_iter()
        .filter(|message| message.sequence.get() == saved.session.transcript_revision + 1)
        .collect();
    let records = after["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| {
            let reference: RecordRef = serde_json::from_value(record["reference"].clone()).unwrap();
            ProtectedRecord::new(
                reference.record_id,
                reference.revision,
                record["value"].clone(),
            )
        })
        .collect();
    (
        store,
        CommitInput {
            control_commands: vec![],
            expected_revision: saved.snapshot.revision,
            lease,
            now_ms,
            snapshot,
            messages,
            events,
            records,
        },
    )
}

#[tokio::test]
async fn accepting_a_command_requires_its_target_settlement_in_the_same_transaction() {
    for (case, mode) in [Mode::Input, Mode::Approval, Mode::External]
        .into_iter()
        .enumerate()
    {
        let fixture = Fixture::new(mode);
        let agent = fixture.agent();
        let original = fixture.started(&agent).await;
        assert_eq!(
            fixture.outcome(&original).await.result.status(),
            RunStatus::Waiting
        );
        let saved = fixture.saved(&original).await;
        let before =
            serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
        let wait = saved.snapshot.wait.clone().unwrap();
        let action = match wait.target {
            WaitTarget::Input { .. } => ResumeAction::Input {
                wait_id: wait.wait_id,
                answer: json!({"selection":"annual"}),
            },
            WaitTarget::Approval { target } => ResumeAction::Deny {
                wait_id: wait.wait_id,
                target,
                reason: "Declined by reviewer".into(),
            },
            WaitTarget::External { .. } => ResumeAction::External {
                wait_id: wait.wait_id,
                receipt_ref: fixture.store.proof.reference().clone(),
            },
        };
        let command = ResumeCommand {
            run_id: original.run_id().clone(),
            expected_revision: saved.snapshot.revision,
            command_id: id("acceptance"),
            action,
        };
        let resumed = completed(agent.resume(command, context()).await.unwrap());
        assert_eq!(
            fixture.outcome(&resumed).await.result.status(),
            RunStatus::Succeeded
        );
        let after =
            serde_json::to_value(fixture.base.store.export_checkpoint(&scope()).unwrap()).unwrap();
        let (positive_store, positive) = replay_acceptance(&before, &after).await;
        positive_store
            .commit(&scope(), original.run_id(), positive)
            .await
            .unwrap();
        let (delayed_store, mut delayed) = replay_acceptance(&before, &after).await;
        delayed.now_ms += 5;
        delayed_store
            .commit(&scope(), original.run_id(), delayed)
            .await
            .unwrap();

        for fault in 0..2 {
            let (store, mut candidate) = replay_acceptance(&before, &after).await;
            if fault == 0 {
                candidate.events.remove(0);
                candidate.events[0].seq = (saved.snapshot.last_event_seq + 1).try_into().unwrap();
                candidate.snapshot.last_event_seq -= 1;
                candidate.messages.clear();
            } else if let ResumeAction::Input { answer, .. } =
                &mut candidate.snapshot.resume_receipts[0].command.action
            {
                *answer = json!({"selection":"quarterly"});
                let receipt = &mut candidate.snapshot.resume_receipts[0];
                let record = candidate
                    .records
                    .iter_mut()
                    .find(|record| record.reference() == &receipt.command_ref)
                    .unwrap();
                *record = ProtectedRecord::new(
                    record.reference().record_id.clone(),
                    record.reference().revision,
                    serde_json::to_value(&receipt.command).unwrap(),
                );
                receipt.command_ref = record.reference().clone();
                let RunEventPayload::RunResumed { command_ref } = &mut candidate.events[1].payload
                else {
                    unreachable!()
                };
                *command_ref = receipt.command_ref.clone();
            } else {
                // Keep the old unresolved/pending target but supply the accepted
                // command and a perfectly well-formed settlement event for it.
                candidate.snapshot.tool_ledger[1].state =
                    saved.snapshot.tool_ledger[1].state.clone();
            }
            let initial = store.load(&scope(), original.run_id()).await.unwrap();
            assert!(
                store
                    .commit(&scope(), original.run_id(), candidate)
                    .await
                    .is_err(),
                "accepted malformed transaction {case}/{fault}"
            );
            assert_eq!(
                store.load(&scope(), original.run_id()).await.unwrap(),
                initial
            );
        }
        let (expired_store, mut stale) = replay_acceptance(&before, &after).await;
        let deadline = stale.snapshot.timing.deadline_at_ms;
        stale.lease = expired_store
            .renew_lease(
                &scope(),
                original.run_id(),
                &stale.lease,
                stale.now_ms,
                (deadline - stale.now_ms + 1_000) as u64,
            )
            .await
            .unwrap();
        stale.now_ms = deadline;
        let initial = expired_store
            .load(&scope(), original.run_id())
            .await
            .unwrap();
        assert_eq!(
            expired_store
                .commit(&scope(), original.run_id(), stale)
                .await
                .unwrap_err()
                .code,
            ErrorCode::DeadlineExceeded,
            "queue delay must not accept an expired decision"
        );
        assert_eq!(
            expired_store
                .load(&scope(), original.run_id())
                .await
                .unwrap(),
            initial
        );
    }
}
