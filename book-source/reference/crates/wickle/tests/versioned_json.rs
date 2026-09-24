//! Behavior checks for versioned JSON text contracts.
use serde_json::json;
use wickle::{
    CanonicalizationVersion as Version, CompletionPolicy, JsonDigest, JsonTextLimits,
    canonical_digest_json, canonicalize_json_text, versioned_digest_json,
};

fn canonical(input: &str) -> Vec<u8> {
    canonicalize_json_text(input, JsonTextLimits::default()).unwrap()
}
#[test]
fn large_numbers_and_exponents_are_preserved_without_float_rounding() {
    let input = r#"{"z":1E+003,"a":[184467440737095516160001,-0,1.00,1e9999]}"#;
    assert_eq!(
        canonical(input),
        br#"{"a":[184467440737095516160001,-0,1.00,1e9999],"z":1E+003}"#
    );
    for (a, b) in [
        ("1", "1.0"),
        ("1e3", "1E+003"),
        ("0", "-0"),
        ("184467440737095516160001", "184467440737095516160002"),
    ] {
        assert_ne!(
            versioned_digest_json(a, Version::WickleCanonicalJsonV1, JsonTextLimits::default())
                .unwrap(),
            versioned_digest_json(b, Version::WickleCanonicalJsonV1, JsonTextLimits::default())
                .unwrap()
        );
    }
}
#[test]
fn string_escaping_key_order_and_array_order_have_stable_meaning() {
    assert_eq!(
        canonical(r#"{"z":[2,1],"\u0061":"\u0062"}"#),
        br#"{"a":"b","z":[2,1]}"#
    );
    assert_ne!(canonical(r#"[2,1]"#), canonical(r#"[1,2]"#));
    assert_ne!(canonical(r#""é""#), canonical(r#""e\u0301""#));
    let digest = versioned_digest_json(
        "{}",
        Version::WickleCanonicalJsonV1,
        JsonTextLimits::default(),
    )
    .unwrap();
    assert_eq!(
        digest.as_str(),
        "wickle-canonical-json-v1:sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    );
    assert_eq!(
        JsonDigest::try_from(digest.as_str().to_owned()).unwrap(),
        digest
    );
}
#[test]
fn ambiguous_invalid_or_oversized_text_cannot_be_hashed() {
    for input in [
        r#"{"a":1,"\u0061":2}"#,
        r#"{"nested":{"a":1,"a":2}}"#,
        r#"[1,]"#,
        "01",
        "+1",
        "NaN",
        "Infinity",
        "{}{}",
        r#""\ud800""#,
    ] {
        assert!(
            canonicalize_json_text(input, JsonTextLimits::default()).is_err(),
            "{input}"
        );
    }
    assert!(
        canonicalize_json_text(
            "null",
            JsonTextLimits {
                max_bytes: 3,
                max_depth: 1
            }
        )
        .is_err()
    );
    assert!(
        canonicalize_json_text(
            "[[0]]",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 1
            }
        )
        .is_err()
    );
    assert!(
        canonicalize_json_text(
            "[0]",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 1
            }
        )
        .is_ok()
    );
    assert!(
        canonicalize_json_text(
            r#""[{\"}]""#,
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 0
            }
        )
        .is_ok()
    );
    assert!(
        canonicalize_json_text(
            "0",
            JsonTextLimits {
                max_bytes: 100,
                max_depth: 129
            }
        )
        .is_err()
    );
}
#[test]
fn saved_legacy_rules_are_not_silently_replaced() {
    for input in [
        "{}",
        r#"{"z":1,"a":{"x":[3,1],"b":2}}"#,
        "1.0",
        "-0.0",
        "1e3",
    ] {
        assert_eq!(
            versioned_digest_json(input, Version::SortedJsonV1, JsonTextLimits::default()).unwrap(),
            canonical_digest_json(input).unwrap()
        );
    }
    assert_eq!(
        serde_json::to_string(&Version::WickleCanonicalJsonV1).unwrap(),
        "\"wickle-canonical-json-v1\""
    );
    assert!(serde_json::from_str::<Version>("\"unknown\"").is_err());
}
#[test]
fn completion_policy_accepts_only_the_two_tagged_contracts() {
    for input in [
        json!({"mode":"turn_end"}),
        json!({"mode":"verified","verifier_ref":{"id":"report","version":"1"}}),
    ] {
        let policy: CompletionPolicy = serde_json::from_value(input.clone()).unwrap();
        assert_eq!(serde_json::to_value(policy).unwrap(), input);
    }
    for input in [
        json!("turn_end"),
        json!(null),
        json!({}),
        json!({"mode":"unknown"}),
        json!({"mode":"verified"}),
        json!({"mode":"turn_end","verifier_ref":{"id":"a","version":"1"}}),
        json!({"mode":"verified","verifier_ref":null}),
        json!({"mode":"verified","verifier_ref":{"id":"","version":"1"}}),
        json!({"mode":"verified","verifier_ref":{"id":"a","version":""}}),
        json!({"mode":"verified","verifier_ref":{"id":"a"}}),
        json!({"mode":"verified","verifier_ref":{"id":"a","version":"1","extra":true}}),
        json!({"mode":"turn_end","extra":true}),
    ] {
        assert!(
            serde_json::from_value::<CompletionPolicy>(input.clone()).is_err(),
            "{input}"
        );
    }
}
