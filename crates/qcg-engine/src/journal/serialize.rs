use serde::Serialize;
use std::io::Write;

use super::types::{JournalError, JournalLimits, JournalStats};
use super::writer::validate_limits;

pub fn serialize_bounded<T: Serialize>(
    value: &T,
    limit: Option<usize>,
    resource: &'static str,
) -> Result<Vec<u8>, JournalError> {
    if limit == Some(0) {
        return Err(JournalError::InvalidLimit { resource });
    }
    let Some(limit) = limit else {
        return serde_json::to_vec(value).map_err(JournalError::Json);
    };
    let mut writer = BoundedBytesWriter::new(limit);
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.bytes),
        Err(_error) if writer.exceeded => Err(JournalError::LimitExceeded {
            resource,
            actual: limit.checked_add(1).ok_or(JournalError::InvalidEvent(
                "event size limit overflowed".into(),
            ))?,
            limit,
        }),
        Err(error) => Err(JournalError::Json(error)),
    }
}

struct BoundedBytesWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl BoundedBytesWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(64 * 1024)),
            limit,
            exceeded: false,
        }
    }
}

impl Write for BoundedBytesWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(next) = self.bytes.len().checked_add(bytes.len()) else {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "bounded JSON serialization overflowed",
            ));
        };
        if next > self.limit {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "bounded JSON serialization exceeded limit",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn append_serialized_json_line<W: Write>(
    writer: &mut W,
    mut bytes: Vec<u8>,
    stats: &mut JournalStats,
    limits: JournalLimits,
) -> Result<(), JournalError> {
    validate_limits(limits)?;
    if let Some(limit) = limits.max_event_count
        && stats.events >= limit
    {
        return Err(JournalError::EventCountExceeded {
            actual: stats
                .events
                .checked_add(1)
                .ok_or(JournalError::InvalidEvent(
                    "journal event count overflowed".into(),
                ))?,
            limit,
        });
    }
    if let Some(limit) = limits.max_event_bytes
        && bytes.len() > limit
    {
        return Err(JournalError::LimitExceeded {
            resource: "event",
            actual: bytes.len(),
            limit,
        });
    }
    // A `usize` length overflow is a corrupt caller, not a limit
    // question: report it without inventing a bound.
    let line_len = bytes
        .len()
        .checked_add(1)
        .ok_or(JournalError::InvalidEvent(
            "journal event length overflowed usize".into(),
        ))?;
    let line_bytes = line_len;
    let total = stats
        .bytes
        .checked_add(line_bytes)
        .ok_or(JournalError::InvalidEvent(
            "journal byte total overflowed usize".into(),
        ))?;
    if let Some(limit) = limits.max_total_bytes
        && total > limit
    {
        return Err(JournalError::LimitExceeded {
            resource: "total journal",
            actual: total,
            limit,
        });
    }
    bytes.push(b'\n');
    writer.write_all(&bytes)?;
    stats.bytes = total;
    // Counter overflow fails closed instead of wrapping the stats (E13).
    stats.events = stats
        .events
        .checked_add(1)
        .ok_or(JournalError::InvalidEvent(
            "journal event count overflowed".into(),
        ))?;
    Ok(())
}
