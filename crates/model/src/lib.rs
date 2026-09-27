mod types;

pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn file_value_normalizes_text_and_base64_content() {
        let text = FileValue::from_text("note.txt", "hello").expect("text file should decode");
        assert_eq!(text.decode().expect("text should decode"), b"hello");
        assert_eq!(
            serde_json::to_value(&text).expect("text file should encode"),
            json!({"name": "note.txt", "text": "hello"})
        );

        let bytes = FileValue::from_bytes("bytes.bin", &[0, 255]).expect("bytes should encode");
        assert_eq!(bytes.content_base64.as_deref(), Some("AP8="));
        assert_eq!(bytes.decode().expect("base64 should decode"), [0, 255]);
    }

    #[test]
    fn base64_policy_is_canonical_padded_everywhere() {
        // One spelling per byte string: padded RFC 4648 only, so a value cannot
        // mean two things on two surfaces. Unpadded and non-canonical inputs are
        // refused rather than normalized.
        for rejected in ["aGk", "aGk=aGk=", "aGk=x", "aGl=", "YWJj="] {
            assert!(
                crate::types::encoding::decode_canonical_base64(rejected).is_err(),
                "`{rejected}` must not be accepted"
            );
            assert!(
                serde_json::from_value::<FileValue>(json!({
                    "name": "note.txt",
                    "content_base64": rejected,
                }))
                .is_err(),
                "`{rejected}` must not be accepted as a FileValue"
            );
        }
        assert_eq!(
            crate::types::encoding::decode_canonical_base64("AP8=").expect("canonical decodes"),
            [0, 255]
        );
    }

    #[test]
    fn file_value_requires_exclusive_content_and_safe_name() {
        for value in [
            json!({"name": "note.txt"}),
            json!({"name": "note.txt", "text": "a", "content_base64": "Yg=="}),
            json!({"name": "../note.txt", "text": "a"}),
            json!({"name": "note.txt", "text": "a", "unexpected": true}),
            json!({"name": "note.txt", "content_base64": "not base64"}),
        ] {
            assert!(
                serde_json::from_value::<FileValue>(value).is_err(),
                "invalid file value should be rejected"
            );
        }
    }

    #[test]
    fn file_value_enforces_decoded_size_limit() {
        let at_limit = FileValue {
            name: "limit.bin".into(),
            text: Some("a".repeat(MAX_FILE_INPUT_BYTES)),
            content_base64: None,
        };
        at_limit
            .validate()
            .expect("exactly the limit should be accepted");

        let over_limit = FileValue {
            name: "limit.bin".into(),
            text: Some("a".repeat(MAX_FILE_INPUT_BYTES + 1)),
            content_base64: None,
        };
        assert!(matches!(
            over_limit.validate(),
            Err(FileValueError::TooLarge {
                actual_bytes,
                limit_bytes,
            }) if actual_bytes == MAX_FILE_INPUT_BYTES + 1 && limit_bytes == MAX_FILE_INPUT_BYTES
        ));
    }
}
