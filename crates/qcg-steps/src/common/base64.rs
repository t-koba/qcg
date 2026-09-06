use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::files::{atomic_replace, atomic_temp_path};
use super::unix_mode::apply_unix_mode;

pub(crate) fn strict_base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let decoded = BASE64.decode(encoded).map_err(|error| error.to_string())?;
    if BASE64.encode(&decoded) != encoded {
        return Err("value is not canonical padded base64".into());
    }
    Ok(decoded)
}

pub(crate) async fn decode_base64_file_atomic(
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    unix_mode: Option<u32>,
) -> Result<usize, String> {
    let temporary = atomic_temp_path(target_path);
    let result = async {
        let mut source = tokio::fs::File::open(source_path)
            .await
            .map_err(|error| error.to_string())?;
        let mut target = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| error.to_string())?;
        let mut input_bytes = 0_usize;
        let mut output_bytes = 0_usize;
        let mut quartet = [0_u8; 4];
        let mut quartet_len = 0_usize;
        let mut finished = false;
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            input_bytes = input_bytes
                .checked_add(read)
                .ok_or_else(|| "base64 input byte count overflowed".to_owned())?;
            if input_limit.is_some_and(|limit| input_bytes > limit) {
                return Err(format!(
                    "base64 source exceeds {} bytes",
                    input_limit.unwrap_or(usize::MAX)
                ));
            }
            for byte in &chunk[..read] {
                if finished {
                    return Err("base64 data appeared after terminal padding".into());
                }
                quartet[quartet_len] = *byte;
                quartet_len += 1;
                if quartet_len == quartet.len() {
                    let (decoded, decoded_len, terminal) = decode_base64_quartet(&quartet)?;
                    target
                        .write_all(&decoded[..decoded_len])
                        .await
                        .map_err(|error| error.to_string())?;
                    output_bytes = output_bytes
                        .checked_add(decoded_len)
                        .ok_or_else(|| "base64 output byte count overflowed".to_owned())?;
                    if input_limit.is_some_and(|limit| output_bytes > limit) {
                        return Err(format!(
                            "base64 decoded output exceeds {} bytes",
                            input_limit.unwrap_or(usize::MAX)
                        ));
                    }
                    quartet_len = 0;
                    finished = terminal;
                }
            }
        }
        if quartet_len != 0 {
            return Err("base64 input length must be a multiple of four".into());
        }
        target.sync_all().await.map_err(|error| error.to_string())?;
        drop(target);
        drop(source);
        apply_unix_mode(&temporary, unix_mode)?;
        atomic_replace(&temporary, target_path)
            .await
            .map_err(|error| error.to_string())?;
        Ok(output_bytes)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}

pub(crate) fn decode_base64_quartet(quartet: &[u8; 4]) -> Result<([u8; 3], usize, bool), String> {
    let first = base64_value(quartet[0])?;
    let second = base64_value(quartet[1])?;
    if quartet[2] == b'=' {
        if quartet[3] != b'=' || second & 0x0f != 0 {
            return Err("non-canonical base64 padding".into());
        }
        return Ok(([first << 2 | second >> 4, 0, 0], 1, true));
    }
    let third = base64_value(quartet[2])?;
    if quartet[3] == b'=' {
        if third & 0x03 != 0 {
            return Err("non-canonical base64 padding".into());
        }
        return Ok((
            [first << 2 | second >> 4, second << 4 | third >> 2, 0],
            2,
            true,
        ));
    }
    let fourth = base64_value(quartet[3])?;
    Ok((
        [
            first << 2 | second >> 4,
            second << 4 | third >> 2,
            third << 6 | fourth,
        ],
        3,
        false,
    ))
}

pub(crate) fn base64_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err("base64 contains a non-alphabet character".into()),
    }
}

pub(crate) async fn encode_base64_file_atomic(
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
) -> Result<usize, String> {
    let temporary = atomic_temp_path(target_path);
    let result = async {
        let mut source = tokio::fs::File::open(source_path)
            .await
            .map_err(|error| error.to_string())?;
        let mut target = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .await
            .map_err(|error| error.to_string())?;
        let mut input_bytes = 0_usize;
        let mut carry = Vec::with_capacity(2);
        let mut chunk = [0_u8; 64 * 1024];
        loop {
            let read = source
                .read(&mut chunk)
                .await
                .map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            input_bytes = input_bytes
                .checked_add(read)
                .ok_or_else(|| "base64 input byte count overflowed".to_owned())?;
            if input_limit.is_some_and(|limit| input_bytes > limit) {
                return Err(format!(
                    "base64 source exceeds {} bytes",
                    input_limit.unwrap_or(usize::MAX)
                ));
            }
            let mut input = chunk[..read].to_vec();
            if !carry.is_empty() {
                let mut combined = std::mem::take(&mut carry);
                combined.append(&mut input);
                input = combined;
            }
            let complete = input.len() / 3 * 3;
            if complete != 0 {
                let encoded = BASE64.encode(&input[..complete]);
                target
                    .write_all(encoded.as_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            carry.extend_from_slice(&input[complete..]);
        }
        if !carry.is_empty() {
            let encoded = BASE64.encode(&carry);
            target
                .write_all(encoded.as_bytes())
                .await
                .map_err(|error| error.to_string())?;
        }
        target.sync_all().await.map_err(|error| error.to_string())?;
        drop(target);
        drop(source);
        atomic_replace(&temporary, target_path)
            .await
            .map_err(|error| error.to_string())?;
        Ok(input_bytes)
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}
