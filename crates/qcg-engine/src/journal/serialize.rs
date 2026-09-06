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
            actual: limit.saturating_add(1),
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
    if limits
        .max_event_count
        .is_some_and(|limit| stats.events >= limit)
    {
        return Err(JournalError::EventCountExceeded {
            actual: stats.events.saturating_add(1),
            limit: limits.max_event_count.unwrap_or(usize::MAX),
        });
    }
    if limits
        .max_event_bytes
        .is_some_and(|limit| bytes.len() > limit)
    {
        return Err(JournalError::LimitExceeded {
            resource: "event",
            actual: bytes.len(),
            limit: limits.max_event_bytes.unwrap_or(usize::MAX),
        });
    }
    let line_bytes = bytes
        .len()
        .checked_add(1)
        .ok_or(JournalError::LimitExceeded {
            resource: "total journal",
            actual: usize::MAX,
            limit: limits.max_total_bytes.unwrap_or(usize::MAX),
        })?;
    let total = stats
        .bytes
        .checked_add(line_bytes)
        .ok_or(JournalError::LimitExceeded {
            resource: "total journal",
            actual: usize::MAX,
            limit: limits.max_total_bytes.unwrap_or(usize::MAX),
        })?;
    if limits.max_total_bytes.is_some_and(|limit| total > limit) {
        return Err(JournalError::LimitExceeded {
            resource: "total journal",
            actual: total,
            limit: limits.max_total_bytes.unwrap_or(usize::MAX),
        });
    }
    bytes.push(b'\n');
    writer.write_all(&bytes)?;
    stats.bytes = total;
    stats.events = stats.events.saturating_add(1);
    Ok(())
}
