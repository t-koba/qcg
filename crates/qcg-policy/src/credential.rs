//! Credential-name heuristics shared by secret handling and redaction.

/// Redacts every query value in a URL while keeping parameter names
/// visible. Userinfo (`user:pass@`) is stripped and fragments are redacted
/// like queries, so `#token=s3cret` cannot smuggle a value either.
/// Declared sensitivity still drives digest salting elsewhere; this display
/// redaction is the fail-closed default so an undeclared `api_key` value
/// never reaches the journal in plaintext (E09-1). Malformed URLs fail
/// closed by stripping the query entirely rather than leaking a possible
/// credential.
pub fn redact_all_query_values(url: &str) -> String {
    // Split off the fragment first so `#` never smuggles a value.
    let (before_fragment, fragment) = match url.find('#') {
        Some(index) => (&url[..index], Some(&url[index + 1..])),
        None => (url, None),
    };
    let Some(query_start) = before_fragment.find('?') else {
        return format!(
            "{}{}",
            redact_matrix_params(&strip_url_userinfo(before_fragment)),
            redact_url_fragment(fragment)
        );
    };
    let (base, query) = before_fragment.split_at(query_start);
    let query = &query[1..];
    if query.is_empty() {
        return format!(
            "{}{}",
            redact_matrix_params(&strip_url_userinfo(base)),
            redact_url_fragment(fragment)
        );
    }
    let mut redacted = String::with_capacity(url.len());
    redacted.push_str(&redact_matrix_params(&strip_url_userinfo(base)));
    redacted.push('?');
    for (index, pair) in split_query_pairs(query).enumerate() {
        if index > 0 {
            // Preserve the original separator (`&` or `;`).
            let sep = query_separator_at(query, index);
            redacted.push(sep);
        }
        match pair.find('=') {
            Some(eq) => {
                redacted.push_str(&pair[..eq + 1]);
                redacted.push_str("[REDACTED]");
            }
            None => {
                // A bare key with no value carries nothing to leak.
                redacted.push_str(pair);
            }
        }
    }
    redacted.push_str(&redact_url_fragment(fragment));
    redacted
}

/// Splits a query string on both `&` and `;` separators, preserving
/// empty segments so the redacted shape stays stable.
fn split_query_pairs(query: &str) -> impl Iterator<Item = &str> {
    query.split(['&', ';'])
}

/// Returns the separator character preceding the `index`-th pair by
/// re-walking the query. Falls back to `&` when the shape is unexpected;
/// fail-closed direction keeps redaction, never plaintext.
fn query_separator_at(query: &str, index: usize) -> char {
    let mut seen = 0;
    for ch in query.chars() {
        if ch == '&' || ch == ';' {
            seen += 1;
            if seen == index {
                return ch;
            }
        }
    }
    '&'
}

/// Redacts `;` matrix parameters in the path (for example
/// `/path;jsessionid=s3cret`): every `;name=value` value becomes
/// `[REDACTED]` while names stay visible. Bare `;name` segments carry
/// nothing to leak. Query strings are handled by the caller; only the
/// path portion before `?` reaches here.
fn redact_matrix_params(url_without_query: &str) -> String {
    let mut out = String::with_capacity(url_without_query.len());
    let mut rest = url_without_query;
    // Preserve scheme://authority prefix verbatim (userinfo already
    // stripped by the caller); only redact `;` segments in path.
    loop {
        let Some(semi) = rest.find(';') else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..semi + 1]);
        rest = &rest[semi + 1..];
        // Segment ends at `/`, `?` (defensive), or end.
        let end = rest.find(['/', '?']).unwrap_or(rest.len());
        let segment = &rest[..end];
        match segment.find('=') {
            Some(eq) => {
                out.push_str(&segment[..eq + 1]);
                out.push_str("[REDACTED]");
            }
            None => {
                out.push_str(segment);
            }
        }
        rest = &rest[end..];
    }
    out
}

/// Strips `user:pass@` userinfo from the authority section, if present.
fn strip_url_userinfo(url: &str) -> String {
    let scheme_end = url.find("://").map(|index| index + 3).unwrap_or(0);
    let (scheme, rest) = url.split_at(scheme_end);
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, path) = rest.split_at(authority_end);
    match authority.rfind('@') {
        Some(at) => format!("{scheme}{}{}", &authority[at + 1..], path),
        None => url.to_string(),
    }
}

/// Redacts a URL fragment's values like query values, preserving only the
/// `#` marker itself. Both `&` and `;` separators are redacted so
/// `#a=1;b=2` cannot smuggle a value.
fn redact_url_fragment(fragment: Option<&str>) -> String {
    let Some(fragment) = fragment else {
        return String::new();
    };
    if fragment.is_empty() {
        return "#".to_string();
    }
    let mut redacted = String::from("#");
    let mut current = String::new();
    let flush = |current: &mut String, redacted: &mut String| {
        if current.is_empty() {
            return;
        }
        match current.find('=') {
            Some(eq) => {
                redacted.push_str(&current[..eq + 1]);
                redacted.push_str("[REDACTED]");
            }
            None => redacted.push_str("[REDACTED]"),
        }
        current.clear();
    };
    for ch in fragment.chars() {
        if ch == '&' || ch == ';' {
            flush(&mut current, &mut redacted);
            redacted.push(ch);
        } else {
            current.push(ch);
        }
    }
    flush(&mut current, &mut redacted);
    redacted
}

/// Strips credentials and query values from free-text error strings.
/// Truncation alone is not redaction (E09-2): every `http(s)://` token has
/// all of its query VALUES replaced (keys stay visible) via
/// [`redact_all_query_values`], so an error naming a URL never echoes a
/// secret. Non-URL text passes through unchanged.
pub fn redact_urls_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let rest = &text[index..];
        let http = rest.find("http://");
        let https = rest.find("https://");
        let start = match (http, https) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let Some(offset) = start else {
            out.push_str(rest);
            break;
        };
        out.push_str(&rest[..offset]);
        let token_start = index + offset;
        // A URL token ends at whitespace or a common wrapper character.
        let mut token_end = text.len();
        for (position, ch) in text[token_start..].char_indices() {
            if ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '<' | '>' | '|') {
                token_end = token_start + position;
                break;
            }
        }
        // Trailing sentence punctuation is not part of the URL.
        let mut token = &text[token_start..token_end];
        while token.ends_with(['.', ',', ';', ':', '!', ')', ']']) && token.len() > 8 {
            token = &token[..token.len() - 1];
            token_end -= 1;
        }
        out.push_str(&redact_all_query_values(token));
        index = token_end;
    }
    out
}

/// Redacts credential-like response/request headers for logging.
/// Values under an authorization-like header name become `[REDACTED]`;
/// all other headers pass through unchanged (E09-3). The redacted map is
/// what step outputs and journaled events carry; the live request still
/// sends the original values.
pub fn redact_header_values(
    headers: &std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    headers
        .iter()
        .map(|(key, value)| {
            if credential_like_name(key) || key.eq_ignore_ascii_case("set-cookie") {
                (key.clone(), "[REDACTED]".to_string())
            } else {
                (key.clone(), value.clone())
            }
        })
        .collect()
}

/// Redacts credential assignments in free text: a credential-like key
/// followed by `=` or `:` has its value replaced with `[REDACTED]`.
/// Shapes covered include `api_key=secret`, `"password": "hunter2"`,
/// and `Authorization: Bearer xyz`. URL-shaped secrets are handled by
/// [`redact_urls_in_text`] (run it first); this covers the non-URL
/// remainder such as echoed headers or config dumps in error strings.
/// Keys stay visible so the message remains diagnosable; unknown shapes
/// pass through rather than corrupting the text. Fail-closed direction:
/// a value that merely looks like a secret is masked.
/// Error-display sites (not journaled tool results) use this after
/// [`redact_urls_in_text`] (E09-2).
pub fn redact_credential_assignments_in_text(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        let rest = &text[index..];
        // Next `=` or `:` candidate; `://` (URL schemes) is never an
        // assignment separator.
        let mut separator = None;
        for (offset, ch) in rest.char_indices() {
            if ch == ':' && rest[offset..].starts_with("://") {
                continue;
            }
            if ch == '=' || ch == ':' {
                // Skip `::` (paths, IPv6, JSON noise): not an assignment.
                if ch == ':'
                    && (rest[offset..].starts_with("::")
                        || offset > 0 && rest[..offset].ends_with(':'))
                {
                    continue;
                }
                separator = Some((offset, ch));
                break;
            }
        }
        let Some((offset, _)) = separator else {
            out.push_str(rest);
            break;
        };
        // Key token immediately before the separator, allowing whitespace
        // and one layer of quotes: `"api_key" :`.
        let mut key_end = offset;
        while key_end > 0 && rest[..key_end].ends_with([' ', '\t']) {
            key_end -= 1;
        }
        let mut key_start = key_end;
        if key_end > 0 && matches!(rest[..key_end].chars().last(), Some('"' | '\'' | '`')) {
            let quote = rest[..key_end].chars().last().expect("quoted key");
            key_end -= quote.len_utf8();
            key_start = key_end;
            while key_start > 0 {
                let ch = rest[..key_start].chars().last().expect("key start");
                if ch == quote {
                    // The opening quote ends at key_start: content starts
                    // here, without consuming the quote itself.
                    break;
                }
                key_start -= ch.len_utf8();
            }
            let key = &rest[key_start..key_end];
            // Emit through the separator, then decide on the value below.
            if !credential_like_name(key) {
                out.push_str(&rest[..offset + 1]);
                index += offset + 1;
                continue;
            }
            out.push_str(&rest[..offset + 1]);
            index += offset + 1;
        } else {
            while key_start > 0 {
                let ch = rest[..key_start].chars().last().expect("key start");
                if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')) {
                    break;
                }
                key_start -= ch.len_utf8();
            }
            let key = &rest[key_start..key_end];
            if key.is_empty() || !credential_like_name(key) {
                out.push_str(&rest[..offset + 1]);
                index += offset + 1;
                continue;
            }
            out.push_str(&rest[..offset + 1]);
            index += offset + 1;
        }
        // Mask the value: skip whitespace, keep one layer of quotes,
        // stop at whitespace or a structural delimiter.
        let remaining = &text[index..];
        let mut skipped = 0;
        for ch in remaining.chars() {
            if ch == ' ' || ch == '\t' {
                skipped += ch.len_utf8();
            } else {
                break;
            }
        }
        out.push_str(&remaining[..skipped]);
        index += skipped;
        let remaining = &text[index..];
        let mut value_end = remaining.len();
        if let Some(quote) = remaining
            .chars()
            .next()
            .filter(|c| matches!(c, '"' | '\'' | '`'))
        {
            out.push(quote);
            index += quote.len_utf8();
            let remaining = &text[index..];
            for (position, ch) in remaining.char_indices() {
                if ch == quote {
                    value_end = position;
                    break;
                }
            }
            out.push_str("[REDACTED]");
            if value_end < remaining.len() {
                out.push(quote);
                index += value_end + quote.len_utf8();
            } else {
                index = text.len();
            }
            continue;
        }
        for (position, ch) in remaining.char_indices() {
            if ch.is_whitespace()
                || matches!(
                    ch,
                    '"' | '\'' | '`' | ',' | ';' | '}' | ']' | ')' | '<' | '>' | '|'
                )
            {
                value_end = position;
                break;
            }
        }
        out.push_str("[REDACTED]");
        index += value_end;
        // An `Authorization: Bearer <token>` style value carries the
        // secret in the token after the scheme word: extend the mask
        // through the gap plus one more token so the secret itself never
        // survives, while later keys stay diagnosable (E09-2).
        let masked_value = &remaining[..value_end.min(remaining.len())];
        let scheme_shaped = masked_value.eq_ignore_ascii_case("bearer")
            || masked_value.eq_ignore_ascii_case("basic")
            || masked_value.eq_ignore_ascii_case("digest");
        if scheme_shaped {
            let tail = &text[index..];
            let mut consumed = 0;
            let mut in_token = false;
            for (position, ch) in tail.char_indices() {
                if ch == ' ' || ch == '\t' {
                    if in_token {
                        break;
                    }
                    consumed = position + ch.len_utf8();
                    continue;
                }
                if ch.is_whitespace()
                    || matches!(
                        ch,
                        '"' | '\'' | '`' | ',' | ';' | '}' | ']' | ')' | '<' | '>' | '|'
                    )
                {
                    break;
                }
                in_token = true;
                consumed = position + ch.len_utf8();
            }
            index += consumed;
        }
    }
    out
}

/// Canonicalizes a full HTTP URL with sorted query pairs for digest
/// binding (E09a). Different queries bind different digests; the same pairs
/// in different orders bind identically. Implemented lexically (no new
/// dependencies): split at `?`/`#`, sort `&`/`;`-separated pairs. This is
/// the single shared canonicalizer used by both the plain HTTP step and
/// the agent HTTP tool so the two paths produce byte-identical canonical
/// URLs and cannot fork approvals (E09).
pub fn canonical_http_url(url: &str) -> String {
    let (before_fragment, fragment) = match url.find('#') {
        Some(index) => (&url[..index], Some(&url[index..])),
        None => (url, None),
    };
    let Some(query_start) = before_fragment.find('?') else {
        return url.to_string();
    };
    let (base, query) = before_fragment.split_at(query_start);
    let query = &query[1..];
    if query.is_empty() {
        return url.to_string();
    }
    let mut pairs: Vec<&str> = query.split(['&', ';']).collect();
    pairs.sort_unstable();
    let mut canonical = String::with_capacity(url.len());
    canonical.push_str(base);
    canonical.push('?');
    canonical.push_str(&pairs.join("&"));
    if let Some(fragment) = fragment {
        canonical.push_str(fragment);
    }
    canonical
}

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

    #[test]
    fn credential_assignments_in_text_mask_values_not_keys() {
        // E09-2: error strings echoing headers or config dumps must not
        // leak secret values; keys stay visible for diagnosability.
        for (input, leaked, kept) in [
            ("api_key=s3cret", "s3cret", "api_key="),
            ("Authorization: Bearer xyz", "xyz", "Authorization:"),
            (r#""password": "hunter2""#, "hunter2", "password"),
            ("token = abc123,", "abc123", "token ="),
        ] {
            let redacted = redact_credential_assignments_in_text(input);
            assert!(!redacted.contains(leaked), "no value leaks: {redacted}");
            assert!(redacted.contains(kept), "keys stay visible: {redacted}");
        }
        // URLs are another helper's job (`redact_urls_in_text` runs
        // first): a plain query value without a credential-like key must
        // pass through here untouched.
        assert_eq!(
            redact_credential_assignments_in_text("https://example.test/?q=1"),
            "https://example.test/?q=1"
        );
        // Non-credential shapes pass through untouched.
        assert_eq!(
            redact_credential_assignments_in_text("status=ok, count=3"),
            "status=ok, count=3"
        );
        assert_eq!(
            redact_credential_assignments_in_text("authority=example"),
            "authority=example"
        );
    }

    #[test]
    fn all_query_values_are_redacted_but_keys_stay_visible() {
        let redacted = redact_all_query_values("https://example.test/search?q=x&api_key=s3cret");
        assert!(redacted.contains("q="), "keys stay visible: {redacted}");
        assert!(
            redacted.contains("api_key="),
            "keys stay visible: {redacted}"
        );
        assert!(!redacted.contains("s3cret"), "no value leaks: {redacted}");
        assert!(!redacted.contains("q=x"), "no value leaks: {redacted}");
        assert!(
            redacted.contains("[REDACTED]"),
            "placeholder marks redaction: {redacted}"
        );
        assert_eq!(
            redact_all_query_values("https://example.test/plain"),
            "https://example.test/plain"
        );
    }

    #[test]
    fn userinfo_and_fragments_are_redacted() {
        // E09: userinfo never survives and fragments cannot smuggle values.
        let redacted = redact_all_query_values("https://user:s3cret@example.test/path");
        assert!(!redacted.contains("s3cret"), "{redacted}");
        assert!(!redacted.contains("user@"), "{redacted}");
        assert!(redacted.contains("example.test/path"), "{redacted}");
        let redacted = redact_all_query_values("https://example.test/cb#token=s3cret");
        assert!(!redacted.contains("s3cret"), "{redacted}");
        let redacted = redact_all_query_values("https://example.test/plain");
        assert_eq!(redacted, "https://example.test/plain");
    }

    #[test]
    fn matrix_and_semicolon_query_values_are_redacted() {
        // E09j: `;` matrix parameters and `;` query separators must not
        // smuggle values past `?`-only redaction.
        let redacted = redact_all_query_values(
            "https://example.test/path;jsessionid=s3cret?q=x&api_key=s3cret",
        );
        assert!(!redacted.contains("s3cret"), "no value leaks: {redacted}");
        assert!(
            redacted.contains("jsessionid="),
            "matrix names stay visible: {redacted}"
        );
        assert!(
            redacted.contains("q="),
            "query names stay visible: {redacted}"
        );
        let redacted = redact_all_query_values("https://example.test/search?q=x;api_key=s3cret");
        assert!(!redacted.contains("s3cret"), "no value leaks: {redacted}");
        assert!(
            redacted.contains("api_key="),
            "semicolon-separated names stay visible: {redacted}"
        );
        assert!(
            redacted.contains(';'),
            "original separator shape is preserved: {redacted}"
        );
        let redacted = redact_all_query_values("https://example.test/cb#token=s3cret;other=x");
        assert!(!redacted.contains("s3cret"), "{redacted}");
        assert!(!redacted.contains("other=x"), "{redacted}");
    }

    #[test]
    fn error_text_redaction_keeps_keys_but_drops_values() {
        let redacted = redact_urls_in_text(
            "tool `fetch` url `https://example.test/search?q=x&api_key=s3cret` is outside declared hosts",
        );
        assert!(
            !redacted.contains("s3cret"),
            "secret must not echo: {redacted}"
        );
        assert!(
            redacted.contains("api_key="),
            "keys stay visible: {redacted}"
        );
        assert!(
            redacted.contains("[REDACTED]"),
            "placeholder marks redaction: {redacted}"
        );
        assert_eq!(redact_urls_in_text("no url here"), "no url here");
    }

    #[test]
    fn debug_helpers_cover_url_header_body_and_args_sentinels() {
        // E09h support: the redacting Debug wrappers for args-carrying
        // types (`RedactedToolCall`/`RedactedMessage` in `qcg-llm-steps`)
        // delegate to these helpers. This pins each helper against sentinel
        // secrets for URL query values, credential headers, and credential
        // assignments (body/args/content) so a wrapper regression fails here
        // with the exact helper at fault.
        let url = redact_all_query_values(
            "https://example.test/search?q=SENTINEL_URL&api_key=SENTINEL_KEY",
        );
        assert!(!url.contains("SENTINEL_URL"), "{url}");
        assert!(!url.contains("SENTINEL_KEY"), "{url}");
        assert!(url.contains("q=") && url.contains("api_key="), "{url}");
        let headers = redact_header_values(&std::collections::BTreeMap::from([
            (
                "Authorization".to_string(),
                "Bearer SENTINEL_HEADER".to_string(),
            ),
            ("X-Tenant".to_string(), "alpha".to_string()),
        ]));
        assert_eq!(headers["Authorization"], "[REDACTED]");
        assert_eq!(headers["X-Tenant"], "alpha");
        assert!(
            !serde_json::to_string(&headers)
                .unwrap()
                .contains("SENTINEL_HEADER")
        );
        let body =
            redact_credential_assignments_in_text("token=SENTINEL_BODY api_key=SENTINEL_ARG");
        assert!(!body.contains("SENTINEL_BODY"), "{body}");
        assert!(!body.contains("SENTINEL_ARG"), "{body}");
        assert!(body.contains("token="), "{body}");
        let text =
            redact_urls_in_text("see https://example.test/?token=SENTINEL_TEXT_URL for details");
        assert!(!text.contains("SENTINEL_TEXT_URL"), "{text}");
    }

    #[test]
    fn canonical_http_url_sorts_pairs_and_preserves_shape() {
        // E09: single shared canonicalizer. Different queries bind
        // different canonical URLs; same pairs in different orders bind
        // identically so approvals cannot fork.
        let first = canonical_http_url("https://example.test/search?q=x&api_key=s3cret");
        let second = canonical_http_url("https://example.test/search?api_key=s3cret&q=x");
        assert_eq!(first, second, "order must not fork the canonical URL");
        let other = canonical_http_url("https://example.test/search?q=y&api_key=s3cret");
        assert_ne!(first, other, "query change must alter the canonical URL");
        assert_eq!(
            canonical_http_url("https://example.test/plain"),
            "https://example.test/plain"
        );
    }
}
