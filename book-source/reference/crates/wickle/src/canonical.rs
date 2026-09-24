//! Versioned canonicalization at the JSON text boundary. Never round-trip new
//! numeric tokens through floating point or reinterpret an old digest version.
use std::{collections::BTreeMap, fmt};

use crate::{ContractError, ErrorCode, JsonDigest, canonical_digest_json};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

/// Rules recorded alongside a submitted request, independent of runtime version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CanonicalizationVersion {
    /// Historical Value-based encoding. Existing stored comparisons keep this rule.
    SortedJsonV1,
    /// Object ordering and fixed string escaping with original number lexemes.
    WickleCanonicalJsonV1,
}
use serde::Serialize;

/// Resource bounds applied before allocating the canonical representation.
#[derive(Debug, Clone, Copy)]
pub struct JsonTextLimits {
    /// Maximum input UTF-8 bytes.
    pub max_bytes: usize,
    /// Maximum nested object/array depth (a scalar has depth zero).
    pub max_depth: usize,
}
impl Default for JsonTextLimits {
    fn default() -> Self {
        Self {
            max_bytes: 1024 * 1024,
            max_depth: 128,
        }
    }
}
fn invalid() -> ContractError {
    ContractError::new(ErrorCode::InvalidJson, "$")
}

/// Canonicalize strict JSON, preserving every number token exactly.
///
/// Duplicate keys (including equivalent escaped spellings), trailing JSON,
/// invalid syntax and resource-limit violations are errors. Limits may be lowered;
/// depth is capped at 128 to bound stack use even with untrusted configuration.
/// Output string escaping is serde_json's JSON encoder; Unicode is not normalized.
pub fn canonicalize_json_text(
    input: &str,
    limits: JsonTextLimits,
) -> Result<Vec<u8>, ContractError> {
    if input.len() > limits.max_bytes || limits.max_depth > 128 {
        return Err(invalid());
    }
    // Bound nesting before RawValue scans the document. Quoted braces are data.
    let (mut depth, mut quoted, mut escape) = (0usize, false, false);
    for b in input.bytes() {
        if quoted {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                quoted = false;
            }
        } else {
            match b {
                b'"' => quoted = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > limits.max_depth {
                        return Err(invalid());
                    }
                }
                b'}' | b']' => {
                    depth = depth.checked_sub(1).ok_or_else(invalid)?;
                }
                _ => {}
            }
        }
    }
    let raw: &RawValue = serde_json::from_str(input).map_err(|_| invalid())?;
    let mut out = Vec::with_capacity(input.len());
    encode(raw, &mut out)?;
    Ok(out)
}

struct Object<'a>(BTreeMap<String, &'a RawValue>);
impl<'de> Deserialize<'de> for Object<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object<'de>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an object with unique keys")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut result = BTreeMap::new();
                while let Some(key) = map.next_key::<String>()? {
                    if result.contains_key(&key) {
                        return Err(de::Error::custom("duplicate object key"));
                    }
                    result.insert(key, map.next_value::<&'de RawValue>()?);
                }
                Ok(Object(result))
            }
        }
        d.deserialize_map(ObjectVisitor)
    }
}
fn encode(raw: &RawValue, out: &mut Vec<u8>) -> Result<(), ContractError> {
    let text = raw.get();
    match text.as_bytes()[0] {
        b'{' => {
            let object: Object<'_> = serde_json::from_str(text).map_err(|_| invalid())?;
            out.push(b'{');
            for (i, (key, value)) in object.0.into_iter().enumerate() {
                if i != 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, &key).map_err(|_| invalid())?;
                out.push(b':');
                encode(value, out)?;
            }
            out.push(b'}');
        }
        b'[' => {
            let array: Vec<&RawValue> = serde_json::from_str(text).map_err(|_| invalid())?;
            out.push(b'[');
            for (i, value) in array.into_iter().enumerate() {
                if i != 0 {
                    out.push(b',');
                }
                encode(value, out)?;
            }
            out.push(b']');
        }
        b'"' => {
            let value: String = serde_json::from_str(text).map_err(|_| invalid())?;
            serde_json::to_writer(out, &value).map_err(|_| invalid())?;
        }
        _ => out.extend_from_slice(text.as_bytes()), // RawValue already validated JSON grammar.
    }
    Ok(())
}

/// Hash a submitted JSON document using its explicitly selected stored rules.
///
/// New rules preserve number lexemes; the legacy branch intentionally uses the
/// historical parser/encoder and keeps its original range limitations.
pub fn versioned_digest_json(
    input: &str,
    version: CanonicalizationVersion,
    limits: JsonTextLimits,
) -> Result<JsonDigest, ContractError> {
    let bytes = canonicalize_json_text(input, limits)?;
    match version {
        CanonicalizationVersion::SortedJsonV1 => canonical_digest_json(input),
        CanonicalizationVersion::WickleCanonicalJsonV1 => {
            let hex: String = Sha256::digest(bytes)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            JsonDigest::try_from(format!("wickle-canonical-json-v1:sha256:{hex}"))
        }
    }
}
