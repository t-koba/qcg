use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

pub(crate) fn strict_base64_decode(encoded: &str) -> Result<Vec<u8>, String> {
    let decoded = BASE64.decode(encoded).map_err(|error| error.to_string())?;
    if BASE64.encode(&decoded) != encoded {
        return Err("value is not canonical padded base64".into());
    }
    Ok(decoded)
}

/// Decodes a workspace file to a target through the gateway: the source is
/// opened handle-relative and the target replaced handle-relative, so a
/// parent swapped after resolution cannot redirect either side (E13). The
/// decoder streams quadruplets straight into the staged output file, so a
/// large source is never buffered whole in memory.
pub(crate) async fn decode_base64_file_atomic(
    fs: &qcg_engine::FsGateway,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
    unix_mode: Option<u32>,
) -> Result<usize, String> {
    let mut source = fs
        .open_read_resolved(source_path)
        .map_err(|error| error.to_string())?;
    let output_bytes = fs
        .write_file_atomic_stream(
            target_path,
            unix_mode.map(|mode| mode & 0o777),
            move |output| decode_base64_stream(&mut source, output, input_limit),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(output_bytes)
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

/// Streaming base64 chunk length, a multiple of three so only the final
/// chunk can carry padding.
const BASE64_STREAM_CHUNK: usize = 3 * 16 * 1024;

fn invalid_data(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

/// Decodes padded base64 from any reader into any writer. Reads are
/// accumulated until a full quartet is available, so a short read (pipes,
/// sockets, line-buffered sources) can never split or misalign a quartet.
fn decode_base64_stream(
    source: &mut impl std::io::Read,
    output: &mut impl std::io::Write,
    input_limit: Option<usize>,
) -> std::io::Result<usize> {
    let mut quartet = [0_u8; 4];
    let mut finished = false;
    let mut encoded_bytes = 0_usize;
    let mut output_bytes = 0_usize;
    loop {
        let mut filled = 0_usize;
        while filled < quartet.len() {
            let read = source.read(&mut quartet[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
            encoded_bytes = encoded_bytes.saturating_add(read);
            if let Some(limit) = input_limit
                && encoded_bytes > limit
            {
                return Err(invalid_data(format!("base64 source exceeds {limit} bytes")));
            }
        }
        if filled == 0 {
            break;
        }
        if filled != quartet.len() {
            return Err(invalid_data(
                "base64 input length must be a multiple of four".into(),
            ));
        }
        if finished {
            return Err(invalid_data(
                "base64 data appeared after terminal padding".into(),
            ));
        }
        let (decoded, decoded_len, terminal) =
            decode_base64_quartet(&quartet).map_err(invalid_data)?;
        output.write_all(&decoded[..decoded_len])?;
        output_bytes = output_bytes
            .checked_add(decoded_len)
            .ok_or_else(|| std::io::Error::other("base64 output byte count overflowed"))?;
        if let Some(limit) = input_limit
            && output_bytes > limit
        {
            return Err(invalid_data(format!(
                "base64 decoded output exceeds {limit} bytes"
            )));
        }
        finished = terminal;
    }
    Ok(output_bytes)
}

/// Encodes bytes from any reader into any writer. Input is accumulated into
/// a three-byte-aligned buffer so short reads never introduce padding in
/// the middle of the stream; only the final flush can pad.
fn encode_base64_stream(
    source: &mut impl std::io::Read,
    output: &mut impl std::io::Write,
    input_limit: Option<usize>,
) -> std::io::Result<usize> {
    let mut pending: Vec<u8> = Vec::with_capacity(BASE64_STREAM_CHUNK);
    let mut buffer = vec![0_u8; BASE64_STREAM_CHUNK];
    let mut total = 0_usize;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read)
            .ok_or_else(|| std::io::Error::other("base64 source size overflowed"))?;
        if let Some(limit) = input_limit
            && total > limit
        {
            return Err(invalid_data(format!("base64 source exceeds {limit} bytes")));
        }
        pending.extend_from_slice(&buffer[..read]);
        while pending.len() >= BASE64_STREAM_CHUNK {
            let encoded = BASE64.encode(&pending[..BASE64_STREAM_CHUNK]);
            output.write_all(encoded.as_bytes())?;
            pending.drain(..BASE64_STREAM_CHUNK);
        }
    }
    if !pending.is_empty() {
        let encoded = BASE64.encode(&pending);
        output.write_all(encoded.as_bytes())?;
    }
    Ok(total)
}

/// Encodes a workspace file to a target through the gateway, with the same
/// handle-relative source and target guarantees as the decoder (E13). The
/// encoder streams three-byte-aligned chunks into the staged output, so
/// neither side is buffered whole and no interior padding can appear.
pub(crate) async fn encode_base64_file_atomic(
    fs: &qcg_engine::FsGateway,
    source_path: &camino::Utf8Path,
    target_path: &camino::Utf8Path,
    input_limit: Option<usize>,
) -> Result<usize, String> {
    let mut source = fs
        .open_read_resolved(source_path)
        .map_err(|error| error.to_string())?;
    let source_bytes = fs
        .write_file_atomic_stream(target_path, None, move |output| {
            encode_base64_stream(&mut source, output, input_limit)
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(source_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read, Write};

    /// A reader that returns at most one byte per call, simulating pipe or
    /// socket short reads.
    struct OneByteReader<R>(R);

    impl<R: Read> Read for OneByteReader<R> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if buffer.is_empty() {
                return Ok(0);
            }
            self.0.read(&mut buffer[..1])
        }
    }

    fn sample_data() -> Vec<u8> {
        (0..=255_u8)
            .cycle()
            .take(3 * BASE64_STREAM_CHUNK + 7)
            .collect()
    }

    #[test]
    fn short_read_encoding_matches_a_single_pass() {
        // The encoder must buffer across short reads: encoding each read
        // independently would inject padding mid-stream.
        let data = sample_data();
        let mut streamed = Vec::new();
        let total =
            encode_base64_stream(&mut OneByteReader(Cursor::new(&data)), &mut streamed, None)
                .expect("encoding should succeed");
        assert_eq!(total, data.len());
        assert_eq!(
            String::from_utf8(streamed).expect("utf8"),
            BASE64.encode(&data)
        );
    }

    #[test]
    fn short_read_decoding_round_trips() {
        // The decoder must assemble full quartets across short reads.
        let data = sample_data();
        let encoded = BASE64.encode(&data);
        let mut decoded = Vec::new();
        let total = decode_base64_stream(
            &mut OneByteReader(Cursor::new(encoded.as_bytes())),
            &mut decoded,
            None,
        )
        .expect("decoding should succeed");
        assert_eq!(total, data.len());
        assert_eq!(decoded, data);
    }

    #[test]
    fn streaming_limits_apply_without_buffering_everything() {
        let data = sample_data();
        let mut sink = Vec::new();
        let error =
            encode_base64_stream(&mut OneByteReader(Cursor::new(&data)), &mut sink, Some(10))
                .expect_err("the source limit must fail");
        assert!(error.to_string().contains("base64 source exceeds 10 bytes"));
        let mut decoded = Vec::new();
        let error = decode_base64_stream(
            &mut OneByteReader(Cursor::new(b"AAAAA".as_slice())),
            &mut decoded,
            None,
        )
        .expect_err("a trailing partial quartet must fail");
        assert!(error.to_string().contains("multiple of four"));
    }

    #[test]
    fn writer_short_writes_are_retried_by_write_all() {
        // A writer may accept fewer bytes than offered; write_all must loop.
        struct OneByteWriter(Vec<u8>, usize);
        impl Write for OneByteWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let take = bytes.len().min(1);
                self.0.extend_from_slice(&bytes[..take]);
                self.1 += take;
                Ok(take)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let data = sample_data();
        let mut writer = OneByteWriter(Vec::new(), 0);
        let total = encode_base64_stream(&mut Cursor::new(&data), &mut writer, None)
            .expect("encoding should succeed");
        assert_eq!(total, data.len());
        assert_eq!(writer.0, BASE64.encode(&data).into_bytes());
    }
}
