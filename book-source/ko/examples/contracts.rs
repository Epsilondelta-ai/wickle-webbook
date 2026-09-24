#[derive(Debug, PartialEq)]
struct RequestId(String);

impl RequestId {
    fn new(value: &str) -> Result<Self, &'static str> {
        if value.trim().is_empty() {
            return Err("empty identifier");
        }
        Ok(Self(value.to_owned()))
    }
}

#[derive(Debug, PartialEq)]
enum Effect {
    NotApplied,
    Applied { receipt: String },
    Unknown,
}

fn retry_is_safe(effect: &Effect) -> bool {
    matches!(effect, Effect::NotApplied)
}

fn main() -> Result<(), &'static str> {
    let id = RequestId::new("request-1")?;
    let effect = Effect::Unknown;
    assert!(!retry_is_safe(&effect));
    assert!(retry_is_safe(&Effect::NotApplied));
    assert!(!retry_is_safe(&Effect::Applied { receipt: "saved".into() }));
    assert_eq!(RequestId::new("  "), Err("empty identifier"));
    println!("{}: unknown is not safe to retry", id.0);
    Ok(())
}
