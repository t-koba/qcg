//! Credential-name heuristics shared by secret handling and redaction.

/// Returns whether a configuration name conventionally denotes credential material.
///
/// Token boundaries avoid false positives such as `AUTHORITY` and `TOKENIZER`, while
/// also recognizing compact names such as `APIKEY` and numbered secret slots.
pub fn credential_like_name(name: &str) -> bool {
    name.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .any(|token| {
            [
                "APIKEY",
                "APITOKEN",
                "AUTH",
                "AUTHORIZATION",
                "BEARER",
                "COOKIE",
                "CREDENTIAL",
                "CREDENTIALS",
                "KEY",
                "PASSWORD",
                "PASSWD",
                "SECRET",
                "TOKEN",
            ]
            .iter()
            .any(|marker| {
                token == *marker
                    || token.strip_prefix(marker).is_some_and(|suffix| {
                        !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit())
                    })
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_token_boundaries() {
        for yes in ["API_KEY", "apiToken", "SECRET2", "x-auth-y", "PASSWORD"] {
            assert!(credential_like_name(yes), "{yes}");
        }
        for no in ["AUTHORITY", "TOKENIZER", "MONKEY", ""] {
            assert!(!credential_like_name(no), "{no}");
        }
    }
}
