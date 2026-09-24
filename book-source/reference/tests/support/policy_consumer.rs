use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use wickle::{
    ExecutionContext, ExecutionContextData, Guarded, Id, PolicyAction, PolicyContext,
    PolicyDecision, PolicyGate, PolicyPort, PolicyRequest, PortFuture, Scope, ToolPolicyInput,
    VersionedRef, canonical_digest,
};

fn id(value: &str) -> Id {
    Id::new(value).expect("example identifiers")
}
fn scope(tenant: &str) -> Scope {
    Scope {
        tenant_id: id(tenant),
        workspace_id: id("workspace"),
        user_id: None,
    }
}

struct HostPolicy {
    owners: BTreeMap<String, Scope>,
}

impl PolicyPort for HostPolicy {
    fn authorize<'a>(
        &'a self,
        request: &'a PolicyRequest,
        context: PolicyContext<'a>,
    ) -> PortFuture<'a, PolicyDecision> {
        Box::pin(async move {
            if context.capability_grant_ref.as_str() == "revoked" {
                return Ok(PolicyDecision::Deny {
                    reason: id("revoked"),
                });
            }
            if let PolicyAction::ExecuteTool { input } = &request.action {
                let record = input
                    .execution_args()
                    .get("record_id")
                    .and_then(|v| v.as_str());
                if record.and_then(|key| self.owners.get(key)) != Some(context.scope) {
                    return Ok(PolicyDecision::Deny {
                        reason: id("target_unavailable"),
                    });
                }
            }
            Ok(if context.capability_grant_ref.as_str() == "review" {
                PolicyDecision::RequireApproval {
                    reason: id("review_needed"),
                }
            } else {
                PolicyDecision::Allow {}
            })
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "allow".into());
    let owner = scope("tenant");
    let context = ExecutionContext::new(
        ExecutionContextData {
            scope: if mode == "foreign-scope" {
                scope("other")
            } else {
                owner.clone()
            },
            principal_ref: id("reviewer"),
            capability_grant_ref: id(match mode.as_str() {
                "deny" => "revoked",
                "approval" => "review",
                _ => "current",
            }),
            trace_context: None,
            system_inputs: None,
        },
        Default::default(),
    );
    let valid_id = "123e4567-e89b-12d3-a456-426614174000";
    let record_id = if mode == "missing-target" {
        "00000000-0000-4000-8000-000000000001"
    } else {
        valid_id
    };
    let arguments = BTreeMap::from([("record_id".into(), json!(record_id))]);
    let request = PolicyRequest {
        owner_scope: owner.clone(),
        resource_id: id("records.read"),
        action: PolicyAction::ExecuteTool {
            input: ToolPolicyInput::new(
                id("call"),
                VersionedRef {
                    id: id("records.read"),
                    version: id("1.0.0"),
                },
                canonical_digest(&json!("descriptor")),
                canonical_digest(&json!(arguments)),
                arguments,
            ),
        },
    };
    let gate = PolicyGate::new(
        Arc::new(HostPolicy {
            owners: BTreeMap::from([(valid_id.to_owned(), owner)]),
        }),
        Duration::from_secs(1),
    )?;
    let reads = AtomicUsize::new(0);
    let outcome = gate
        .guard(&request, &context, None, None, || async {
            reads.fetch_add(1, Ordering::SeqCst);
            Ok("authorized record contents")
        })
        .await;
    match outcome {
        Ok(Guarded::Completed(value)) => println!("allowed: {value}"),
        Ok(Guarded::ApprovalRequired(challenge)) => {
            println!("approval required for {}", challenge.request_digest)
        }
        Err(error) => println!("rejected: {:?}", error.code),
    }
    println!("record reads: {}", reads.load(Ordering::SeqCst));
    Ok(())
}
