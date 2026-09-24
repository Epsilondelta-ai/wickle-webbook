use wickle::{CanonicalizationVersion, JsonTextLimits, canonical_digest_json, canonicalize_json_text, versioned_digest_json};
fn main() {
    let limits = JsonTextLimits::default();
    let request = r#"{"amount":184467440737095516160001,"scale":1E+003}"#;
    assert_eq!(canonicalize_json_text(request, limits).unwrap(), request.as_bytes());
    let digest = versioned_digest_json(request, CanonicalizationVersion::WickleCanonicalJsonV1, limits).unwrap();
    let changed = request.replace("160001", "160002");
    assert_ne!(digest, versioned_digest_json(&changed, CanonicalizationVersion::WickleCanonicalJsonV1, limits).unwrap());
    assert!(canonicalize_json_text(r#"{"amount":1,"amount":2}"#, limits).is_err());
    assert_eq!(versioned_digest_json("1e3", CanonicalizationVersion::SortedJsonV1, limits).unwrap(), canonical_digest_json("1e3").unwrap());
    println!("canonical consumer: exact large numbers, exponent spelling, duplicate rejection and legacy digest selection passed");
}
