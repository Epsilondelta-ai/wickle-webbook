use wickle::{Id, JsonTextLimits, RequestSnapshot, VersionedRef};
fn main() {
    let profile=VersionedRef{id:Id::new("report-agent").unwrap(),version:Id::new("1").unwrap()};
    let submitted=RequestSnapshot::capture(profile.clone(),r#"{"model_options":{},"input":184467440737095516160001}"#,None,JsonTextLimits::default()).unwrap();
    let persisted=serde_json::to_string(&submitted).unwrap();
    let restored:RequestSnapshot=serde_json::from_str(&persisted).unwrap();
    restored.validate(JsonTextLimits::default()).unwrap();
    let replay=RequestSnapshot::capture(profile,r#"{"input":184467440737095516160001}"#,Some("{}"),JsonTextLimits::default()).unwrap();
    assert_eq!(restored.digest(),replay.digest());
    assert!(restored.request_json().contains("184467440737095516160001"));
    let mut tampered:serde_json::Value=serde_json::from_str(&persisted).unwrap();
    tampered["request_json"]=serde_json::json!("{}");
    let changed:RequestSnapshot=serde_json::from_value(tampered).unwrap();
    assert!(changed.validate(JsonTextLimits::default()).is_err());
    println!("execution contract consumer: exact submitted values survive persistence; omission/empty overrides compare equally; changed payload rejected");
}
