use super::file_value::FileValueError;

pub(crate) fn validate_decoded_size_optional_limit(
    bytes: usize,
    max_bytes: Option<usize>,
) -> Result<(), FileValueError> {
    if let Some(limit) = max_bytes
        && bytes > limit
    {
        return Err(FileValueError::TooLarge {
            actual_bytes: bytes,
            limit_bytes: limit,
        });
    }
    Ok(())
}

pub(crate) fn validate_base64_input_size_optional(
    input: &str,
    max_bytes: Option<usize>,
) -> Result<(), FileValueError> {
    let Some(limit) = max_bytes else {
        return Ok(());
    };
    validate_base64_input_size(input, limit)
}

fn validate_base64_input_size(input: &str, max_bytes: usize) -> Result<(), FileValueError> {
    let max_encoded = max_bytes.div_ceil(3).saturating_mul(4);
    if input.len() > max_encoded {
        return Err(FileValueError::TooLarge {
            actual_bytes: input.len().saturating_mul(3) / 4,
            limit_bytes: max_bytes,
        });
    }
    Ok(())
}

pub(crate) fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        output.push(TABLE[(first >> 2) as usize] as char);
        let second = chunk.get(1).copied();
        output.push(
            TABLE[(((first & 0x03) << 4) | second.map_or(0, |value| value >> 4)) as usize] as char,
        );
        match second {
            Some(second) => {
                let third = chunk.get(2).copied();
                output.push(
                    TABLE[(((second & 0x0f) << 2) | third.map_or(0, |value| value >> 6)) as usize]
                        as char,
                );
                output.push(third.map_or('=', |value| TABLE[(value & 0x3f) as usize] as char));
            }
            None => {
                output.push('=');
                output.push('=');
            }
        }
    }
    output
}

pub(crate) fn decode_base64(input: &str) -> Result<Vec<u8>, FileValueError> {
    if input.len() % 4 == 1 {
        return Err(FileValueError::InvalidBase64(
            "length must not leave a single trailing sextet".into(),
        ));
    }
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(input.len().div_ceil(4) * 3);
    let mut index = 0;
    while index < bytes.len() {
        let remaining = bytes.len() - index;
        let chunk_len = remaining.min(4);
        if chunk_len < 4 {
            let first = base64_value(bytes[index])?;
            let second = base64_value(bytes[index + 1])?;
            output.push((first << 2) | (second >> 4));
            if chunk_len == 3 {
                let third = base64_value(bytes[index + 2])?;
                output.push((second << 4) | (third >> 2));
            }
            break;
        }
        let first = base64_value(bytes[index])?;
        let second = base64_value(bytes[index + 1])?;
        output.push((first << 2) | (second >> 4));
        if bytes[index + 2] == b'=' {
            if bytes[index + 3] != b'=' || index + 4 != bytes.len() {
                return Err(FileValueError::InvalidBase64("invalid padding".into()));
            }
            break;
        }
        let third = base64_value(bytes[index + 2])?;
        output.push((second << 4) | (third >> 2));
        if bytes[index + 3] == b'=' {
            if index + 4 != bytes.len() {
                return Err(FileValueError::InvalidBase64("invalid padding".into()));
            }
            break;
        }
        let fourth = base64_value(bytes[index + 3])?;
        output.push((third << 6) | fourth);
        index += 4;
    }
    Ok(output)
}

fn base64_value(byte: u8) -> Result<u8, FileValueError> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(FileValueError::InvalidBase64(format!(
            "invalid character `{}`",
            char::from(byte)
        ))),
    }
}
