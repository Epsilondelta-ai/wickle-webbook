use crate::error;
use wickle::{ContractError, ErrorCode};

/// A complete SSE data record.
pub struct Event {
    /// Optional SSE event name.
    pub name: Option<String>,
    /// UTF-8 data with multiline fields joined by newlines.
    pub data: String,
}

/// Incremental SSE framing; JSON and UTF-8 may be split across network chunks.
pub struct Decoder {
    line: Vec<u8>,
    data: Vec<u8>,
    name: Option<String>,
    skip_lf: bool,
    frame_bytes: usize,
    bytes: usize,
    frames: usize,
    max_bytes: usize,
    max_frame: usize,
    max_frames: usize,
}
impl Decoder {
    /// Set finite limits on total bytes, one event, and event count.
    pub fn new(max_bytes: usize, max_frame: usize, max_frames: usize) -> Self {
        Self {
            line: vec![],
            data: vec![],
            name: None,
            skip_lf: false,
            frame_bytes: 0,
            bytes: 0,
            frames: 0,
            max_bytes,
            max_frame,
            max_frames,
        }
    }
    /// Frame arbitrary network chunks without assuming UTF-8 or line boundaries.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, ContractError> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|count| *count <= self.max_bytes)
            .ok_or_else(limit)?;
        let mut events = vec![];
        for &byte in bytes {
            if self.skip_lf {
                self.skip_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            self.frame_bytes = self
                .frame_bytes
                .checked_add(1)
                .filter(|count| *count <= self.max_frame)
                .ok_or_else(limit)?;
            match byte {
                b'\r' => {
                    self.line(&mut events)?;
                    self.skip_lf = true;
                }
                b'\n' => self.line(&mut events)?,
                byte => self.line.push(byte),
            }
        }
        Ok(events)
    }
    fn line(&mut self, events: &mut Vec<Event>) -> Result<(), ContractError> {
        if self.line.is_empty() {
            self.frame_bytes = 0;
            if !self.data.is_empty() {
                self.frames = self
                    .frames
                    .checked_add(1)
                    .filter(|count| *count <= self.max_frames)
                    .ok_or_else(limit)?;
                self.data.pop();
                let data =
                    String::from_utf8(std::mem::take(&mut self.data)).map_err(|_| invalid())?;
                events.push(Event {
                    name: self.name.take(),
                    data,
                });
            } else {
                self.name = None;
            }
            return Ok(());
        }
        let line = std::str::from_utf8(&self.line).map_err(|_| invalid())?;
        let (field, mut value) = line.split_once(':').unwrap_or((line, ""));
        if let Some(rest) = value.strip_prefix(' ') {
            value = rest;
        }
        match field {
            "data" => {
                self.data.extend_from_slice(value.as_bytes());
                self.data.push(b'\n');
            }
            "event" => {
                self.name = Some(value.to_owned());
            }
            _ => {} // SSE comments, id, retry, and extension fields do not reconnect.
        }
        self.line.clear();
        Ok(())
    }
    /// Reject a truncated final record.
    pub fn finish(&self) -> Result<(), ContractError> {
        if !self.line.is_empty() || !self.data.is_empty() || self.name.is_some() {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> ContractError {
    error(ErrorCode::InvalidContract, "sse")
}
fn limit() -> ContractError {
    error(ErrorCode::ContextBudgetExceeded, "sse_limit")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_split_utf8_and_all_line_endings_preserve_event_data() {
        for ending in ["\n", "\r\n", "\r"] {
            let body = format!(
                ": heartbeat{ending}event: response.created{ending}data: {{\"text\":{ending}data: \"안녕\"}}{ending}{ending}"
            );
            let mut decoder = Decoder::new(4096, 1024, 2);
            let mut events = vec![];
            for byte in body.as_bytes() {
                events.extend(decoder.push(&[*byte]).unwrap());
            }
            decoder.finish().unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].name.as_deref(), Some("response.created"));
            assert_eq!(events[0].data, "{\"text\":\n\"안녕\"}");
        }
    }
    #[test]
    fn incomplete_frames_and_normalized_event_bounds_never_dispatch_a_partial_record() {
        let mut decoder = Decoder::new(1024, 128, 1);
        assert!(decoder.push(b"data: {\"x\":1}\n").unwrap().is_empty());
        assert!(decoder.finish().is_err());
        let mut decoder = Decoder::new(1024, 8, 2);
        assert!(decoder.push(b"data: too large\n\n").is_err());
        let mut decoder = Decoder::new(1024, 128, 1);
        assert_eq!(decoder.push(b"data: first\n\n").unwrap().len(), 1);
        assert!(decoder.push(b"data: second\n\n").is_err());
        let mut decoder = Decoder::new(4, 128, 2);
        assert!(decoder.push(b"12345").is_err());
    }
}
