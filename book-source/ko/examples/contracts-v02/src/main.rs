use serde_json::json;
use wickle::{Id, JsonObject, JsonTextLimits, RequestSnapshot, VersionedRef, merge_model_options};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let profile = VersionedRef {
        id: Id::new("agent")?,
        version: Id::new("1")?,
    };
    let limits = JsonTextLimits::default();
    let omitted = RequestSnapshot::capture(
        profile.clone(),
        r#"{"input":184467440737095516160001}"#,
        None,
        limits,
    )?;
    let explicit_empty = RequestSnapshot::capture(
        profile.clone(),
        r#"{"model_options":{},"input":184467440737095516160001}"#,
        Some("{}"),
        limits,
    )?;
    assert_eq!(omitted.digest(), explicit_empty.digest());
    assert!(!omitted.system_inputs_provided());
    assert!(explicit_empty.system_inputs_provided());
    let different_number = RequestSnapshot::capture(
        profile.clone(),
        r#"{"input":184467440737095516160002}"#,
        None,
        limits,
    )?;
    assert_ne!(omitted.digest(), different_number.digest());
    assert!(
        RequestSnapshot::capture(profile.clone(), r#"{"model_options":null}"#, None, limits,)
            .is_err()
    );
    assert!(RequestSnapshot::capture(profile, "{}", Some("null"), limits).is_err());
    let saved = serde_json::to_string(&omitted)?;
    let restored: RequestSnapshot = serde_json::from_str(&saved)?;
    restored.validate(limits)?;
    assert_eq!(
        restored.request_json(),
        r#"{"input":184467440737095516160001}"#
    );

    let binding = JsonObject::from([("a".into(), json!({"x":1,"y":2}))]);
    let run = JsonObject::from([("a".into(), json!({"x":3}))]);
    let effective = merge_model_options(&binding, &run);
    assert_eq!(effective["a"], json!({"x":3}));
    assert_eq!(binding["a"], json!({"x":1,"y":2}));
    println!("v0.2 contract lab: snapshot identity and shallow option precedence passed");
    Ok(())
}
