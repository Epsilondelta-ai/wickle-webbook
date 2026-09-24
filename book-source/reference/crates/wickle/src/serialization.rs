use std::{collections::BTreeMap, fmt};

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};

use crate::{ContractError, ErrorCode};

/// An opaque, nonblank identifier. No UUID, SemVer, or provider naming is assumed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Id(String);

impl Id {
    /// Validate an identifier without normalizing its spelling.
    pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ContractError::new(ErrorCode::InvalidContract, "identifier"));
        }
        Ok(Self(value))
    }

    /// Return the original identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Id {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<Id> for String {
    fn from(value: Id) -> Self {
        value.0
    }
}
impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A SHA-256 digest carrying its canonicalization version.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct JsonDigest(String);

impl JsonDigest {
    /// Return the encoding version, algorithm, and hexadecimal digest.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for JsonDigest {
    type Error = ContractError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let valid = value
            .strip_prefix("sorted-json-v1:sha256:")
            .or_else(|| value.strip_prefix("wickle-canonical-json-v1:sha256:"))
            .is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            });
        if !valid {
            return Err(ContractError::new(ErrorCode::InvalidContract, "digest"));
        }
        Ok(Self(value))
    }
}
impl From<JsonDigest> for String {
    fn from(value: JsonDigest) -> Self {
        value.0
    }
}
impl fmt::Display for JsonDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A JSON object containing data, not executable objects.
pub type JsonObject = BTreeMap<String, Value>;

/// Parse JSON, rejecting duplicate object keys and nonfinite numbers.
pub fn parse_json(input: &str) -> Result<Value, ContractError> {
    serde_json::from_str::<StrictJson>(input)
        .map(|value| value.0)
        .map_err(|_| ContractError::new(ErrorCode::InvalidJson, "$"))
}

struct StrictJson(Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct JsonVisitor;
        impl<'de> Visitor<'de> for JsonVisitor {
            type Value = StrictJson;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("unambiguous finite JSON")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Number::from_f64(v)
                    .map(|n| StrictJson(Value::Number(n)))
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictJson(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictJson(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element::<StrictJson>()? {
                    values.push(value.0);
                }
                Ok(StrictJson(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate object key"));
                    }
                    values.insert(key, map.next_value::<StrictJson>()?.0);
                }
                Ok(StrictJson(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(JsonVisitor)
    }
}

/// Hash JSON after sorting object keys recursively; retain array order and values.
///
/// This is not RFC 8785. Numbers retain serde_json's representation: `1` and
/// `1.0`, and `0` and `-0.0`, remain distinct. Use [`canonical_digest_json`] when
/// reading text so duplicate keys and nonfinite numbers are rejected first.
pub fn canonical_digest(value: &Value) -> JsonDigest {
    let bytes = canonical_json_bytes(value);
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    JsonDigest(format!("sorted-json-v1:sha256:{hex}"))
}

pub(crate) fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let keys: BTreeMap<_, _> = map.iter().collect();
                Value::Object(
                    keys.into_iter()
                        .map(|(k, v)| (k.clone(), ordered(v)))
                        .collect(),
                )
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_vec(&ordered(value)).expect("JSON values serialize into a byte vector")
}

/// Parse strict JSON and compute its versioned digest.
pub fn canonical_digest_json(input: &str) -> Result<JsonDigest, ContractError> {
    parse_json(input).map(|value| canonical_digest(&value))
}

pub(crate) fn optional<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(
    input: &str,
    version: Option<&str>,
) -> Result<T, ContractError> {
    let value = parse_json(input)?;
    if let Some(expected) = version {
        let actual = value
            .get("schema_version")
            .and_then(Value::as_str)
            .ok_or_else(|| ContractError::new(ErrorCode::InvalidContract, "schema_version"))?;
        if actual != expected {
            return Err(ContractError::new(
                ErrorCode::UnsupportedSchemaVersion,
                "schema_version",
            ));
        }
    }
    serde_json::from_value(value).map_err(|_| ContractError::new(ErrorCode::InvalidContract, "$"))
}

pub(crate) fn data_digest(value: &impl Serialize) -> JsonDigest {
    canonical_digest(&serde_json::to_value(value).expect("contract DTOs contain only JSON data"))
}
