use base64::{DecodeError, Engine as _, engine::general_purpose::STANDARD as BASE64};

use super::file_value::FileValueError;

fn too_large(actual_bytes: usize, limit_bytes: usize) -> FileValueError {
    FileValueError::TooLarge {
        actual_bytes,
        limit_bytes,
    }
}

pub(crate) fn validate_decoded_size_optional_limit(
    bytes: usize,
    max_bytes: Option<usize>,
) -> Result<(), FileValueError> {
    if let Some(limit) = max_bytes.filter(|&limit| bytes > limit) {
        return Err(too_large(bytes, limit));
    }
    Ok(())
}

pub(crate) fn validate_base64_input_size_optional(
    input: &str,
    max_bytes: Option<usize>,
) -> Result<(), FileValueError> {
    if let Some(limit) = max_bytes
        && input.len() > limit.div_ceil(3).saturating_mul(4)
    {
        return Err(too_large(input.len().saturating_mul(3) / 4, limit));
    }
    Ok(())
}

pub(crate) fn encode_base64(bytes: &[u8]) -> String {
    BASE64.encode(bytes)
}

/// The single base64 decoder for every wire and file surface.
///
/// Canonical RFC 4648 with the standard alphabet and required padding, and
/// nothing else: an unpadded value, interior whitespace, or non-zero trailing
/// bits are rejected instead of being normalized, so one encoding of the same
/// bytes is the only accepted spelling. `steps` and the design schema
/// enforce the same rule.
pub fn decode_canonical_base64(input: &str) -> Result<Vec<u8>, String> {
    if !input.len().is_multiple_of(4) {
        return Err("base64 length must be a multiple of four with required padding".to_string());
    }
    let mut output = Vec::with_capacity(input.len() / 4 * 3);
    // Decode in wire order: canonical terminal padding outranks any later invalid byte.
    for (index, chunk) in input.as_bytes().as_chunks::<4>().0.iter().enumerate() {
        BASE64
            .decode_vec(chunk, &mut output)
            .map_err(|error| match error {
                DecodeError::InvalidByte(2, b'=') | DecodeError::InvalidLastSymbol { .. } => {
                    "base64 padding is not canonical"
                }
                _ => "base64 contains a non-alphabet character",
            })?;
        if chunk[3] == b'=' && (index + 1) * 4 != input.len() {
            return Err("base64 data appeared after terminal padding".to_string());
        }
    }
    Ok(output)
}

pub(crate) fn decode_base64(input: &str) -> Result<Vec<u8>, FileValueError> {
    decode_canonical_base64(input).map_err(FileValueError::InvalidBase64)
}
