//! Preserve signed call parts while validating every byte outside their arguments.
use crate::codec::invalid;
use serde_json::{Value, value::RawValue};
use std::collections::BTreeMap;
use wickle::{ContractError, parse_json, parse_provider_arguments};
type Object<'a> = BTreeMap<String, &'a RawValue>;
fn object(raw: &str) -> Result<Object<'_>, ContractError> {
    serde_json::from_str(raw).map_err(|_| invalid())
}

pub(crate) struct Part<'a> {
    pub value: Value,
    pub arguments: Option<&'a str>,
    pub invalid_arguments: bool,
    pub original: &'a str,
}
fn span(container: &str, value: &str) -> Result<(usize, usize), ContractError> {
    let start = (value.as_ptr() as usize)
        .checked_sub(container.as_ptr() as usize)
        .ok_or_else(invalid)?;
    let end = start.checked_add(value.len()).ok_or_else(invalid)?;
    if container.get(start..end) != Some(value) {
        return Err(invalid());
    }
    Ok((start, end))
}
fn mask(container: &str, values: &[&str]) -> Result<String, ContractError> {
    let mut spans = values
        .iter()
        .map(|value| span(container, value))
        .collect::<Result<Vec<_>, _>>()?;
    spans.sort_unstable();
    let mut output = String::new();
    let mut previous = 0;
    for (start, end) in spans {
        output.push_str(container.get(previous..start).ok_or_else(invalid)?);
        output.push_str("{}");
        previous = end;
    }
    output.push_str(container.get(previous..).ok_or_else(invalid)?);
    Ok(output)
}
pub(crate) fn part(raw: &str, max_arguments: usize) -> Result<Part<'_>, ContractError> {
    let fields = object(raw)?;
    let arguments = fields
        .get("functionCall")
        .map(|call| {
            Ok::<_, ContractError>(object(call.get())?.get("args").map(|value| value.get()))
        })
        .transpose()?
        .flatten();
    let invalid_arguments =
        arguments.is_some_and(|args| parse_provider_arguments(args, max_arguments).is_err());
    let value = if invalid_arguments {
        parse_json(&mask(
            raw,
            &[arguments.expect("present invalid arguments")],
        )?)?
    } else {
        parse_json(raw)?
    };
    Ok(Part {
        value,
        arguments,
        invalid_arguments,
        original: raw,
    })
}
pub(crate) fn event(
    raw: &str,
    max_arguments: usize,
) -> Result<(Value, Vec<Part<'_>>), ContractError> {
    let root = object(raw)?;
    let Some(candidates) = root.get("candidates") else {
        return Ok((parse_json(raw)?, vec![]));
    };
    let candidates: Vec<&RawValue> =
        serde_json::from_str(candidates.get()).map_err(|_| invalid())?;
    if candidates.len() != 1 {
        return Ok((parse_json(raw)?, vec![]));
    }
    let candidate = object(candidates[0].get())?;
    let Some(content) = candidate.get("content") else {
        return Ok((parse_json(raw)?, vec![]));
    };
    let content = object(content.get())?;
    let parts = content.get("parts").ok_or_else(invalid)?;
    let parts: Vec<&RawValue> = serde_json::from_str(parts.get()).map_err(|_| invalid())?;
    let parts = parts
        .into_iter()
        .map(|raw| part(raw.get(), max_arguments))
        .collect::<Result<Vec<_>, _>>()?;
    let invalid: Vec<_> = parts
        .iter()
        .filter(|part| part.invalid_arguments)
        .filter_map(|part| part.arguments)
        .collect();
    let value = if invalid.is_empty() {
        parse_json(raw)?
    } else {
        parse_json(&mask(raw, &invalid)?)?
    };
    Ok((value, parts))
}
