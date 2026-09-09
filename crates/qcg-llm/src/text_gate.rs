//! Pre-publication gate for provider credentials in streamed text.
//!
//! The gateway layer holds back unverified text for generator secrets, but
//! that never inspects the provider's own credential. This gate enforces
//! the provider side independently: streamed text deltas pass through it,
//! and only prefixes proven free of the configured credential are released
//! for publication (B04). Detection runs on the actually-published text
//! stream (per channel), never on metadata-mixed concatenations whose
//! interleaving can hide a split secret.
//!
//! Guarantee: a credential occurrence is reported no later than the arrival
//! of the chunk completing it, and none of its bytes publish before that
//! decision. The gate retains only a bounded unverified suffix plus a
//! bounded boundary window, so long streams stay flat in memory. All splits
//! respect UTF-8 character boundaries (B05).

/// Credential holdback gate over one emitted text stream.
#[derive(Debug, Default)]
pub(crate) struct SensitiveTextGate {
    /// Configured credential bytes. Empty disables the gate (passthrough).
    key: String,
    /// Trailing bytes withheld from publication: `key.len() - 1`, so any
    /// occurrence completing later still has an unpublished byte.
    holdback: usize,
    /// Last emitted bytes (up to `holdback`) retained so occurrences
    /// spanning a push boundary are still detected.
    tail: String,
    /// Emitted but unpublished suffix.
    unverified: String,
}

/// Greatest index at or below `index` that is a UTF-8 character boundary.
/// (`str::floor_char_boundary` needs a newer MSRV than this crate allows.)
fn floor_char_boundary(text: &str, index: usize) -> usize {
    let bytes = text.as_bytes();
    let mut index = index.min(bytes.len());
    while index > 0 && index < bytes.len() && (bytes[index] >> 6) == 0b10 {
        index -= 1;
    }
    index
}

impl SensitiveTextGate {
    pub(crate) fn new(key: Option<&str>) -> Self {
        match key.filter(|key| !key.is_empty()) {
            Some(key) => Self {
                holdback: key.len().saturating_sub(1),
                key: key.to_string(),
                tail: String::new(),
                unverified: String::new(),
            },
            None => Self::default(),
        }
    }

    /// Pushes newly emitted text. Returns the newly cleared publishable
    /// prefix, or `Err(())` when the credential completes anywhere in the
    /// emitted stream (its bytes are/partly withheld, so the caller must
    /// abort instead of publishing).
    pub(crate) fn push(&mut self, text: &str) -> Result<String, ()> {
        if self.key.is_empty() {
            return Ok(text.to_string());
        }
        // Every occurrence ending in this push lies within the retained
        // tail plus the new text; earlier-ending occurrences were checked
        // on their own pushes. Either way a hit aborts before publishing.
        self.tail.push_str(text);
        if self.tail.contains(&self.key) {
            return Err(());
        }
        // Retain only the trailing window for the next boundary check.
        if self.tail.len() > self.holdback {
            let cut = floor_char_boundary(&self.tail, self.tail.len() - self.holdback);
            self.tail.drain(..cut);
        }
        self.unverified.push_str(text);
        // Publish everything except the trailing holdback window, split on
        // a character boundary so multi-byte text never panics.
        let publishable = floor_char_boundary(
            &self.unverified,
            self.unverified.len().saturating_sub(self.holdback),
        );
        Ok(self.unverified.drain(..publishable).collect())
    }

    /// Flushes the withheld suffix after the stream ends. Every occurrence
    /// ending anywhere in the emitted stream was already checked at push
    /// time (each push scans the retained tail plus the new text), so the
    /// withheld suffix alone is re-scanned defensively and returned as-is.
    /// The retained tail is NOT concatenated: it overlaps the suffix over
    /// the same emitted bytes, and scanning the junction would fabricate
    /// strings that were never emitted (C07).
    pub(crate) fn finish(self) -> Result<String, ()> {
        if self.key.is_empty() {
            return Ok(String::new());
        }
        if self.unverified.contains(&self.key) {
            return Err(());
        }
        Ok(self.unverified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARY: &str = "qcg-canary-credential-12345";

    fn gate() -> SensitiveTextGate {
        SensitiveTextGate::new(Some(CANARY))
    }

    #[test]
    fn split_at_every_position_is_refused() {
        let bytes = CANARY.as_bytes();
        for split in 1..bytes.len() {
            // ASCII canary: every byte index is a char boundary.
            let (head, tail) = CANARY.split_at(split);
            let mut gate = gate();
            let published = gate.push(head).expect("prefix should clear");
            assert!(
                !published.contains(CANARY),
                "split at {split} published the credential early"
            );
            assert!(
                gate.push(tail).is_err(),
                "split at {split} must be refused on completion"
            );
        }
    }

    #[test]
    fn clean_stream_publishes_with_bounded_holdback() {
        let mut gate = gate();
        let mut published = gate.push("hello, ").expect("clean text should clear");
        published.push_str(&gate.push("world").expect("clean text should clear"));
        // At most key.len() - 1 bytes stay back until the stream ends.
        assert!(published.len() < "hello, world".len());
        let tail = gate.finish().expect("clean stream should flush");
        published.push_str(&tail);
        assert_eq!(published, "hello, world");
    }

    #[test]
    fn multibyte_text_never_panics_and_survives() {
        // B05: long multi-byte input forces the holdback split onto every
        // char boundary as it slides; all splits must stay on boundaries
        // and no byte may be lost.
        let text = "あ".repeat(4000);
        let mut gate = SensitiveTextGate::new(Some("sk-secret-credential-12345"));
        let mut published = String::new();
        let chars: Vec<char> = text.chars().collect();
        for chunk in chars.chunks(7) {
            let piece: String = chunk.iter().collect();
            published.push_str(&gate.push(&piece).expect("clean text should clear"));
        }
        published.push_str(&gate.finish().expect("clean stream should flush"));
        assert_eq!(published, text);
    }

    #[test]
    fn emoji_and_mixed_text_round_trip() {
        let text = "ok ✅ done 🎉 mixed ascii";
        let mut gate = gate();
        let mut published = gate.push(text).expect("clean text should clear");
        published.push_str(&gate.finish().expect("clean stream should flush"));
        assert_eq!(published, text);
    }

    #[test]
    fn single_char_credential_checks_before_publish() {
        let mut gate = SensitiveTextGate::new(Some("x"));
        assert!(gate.push("axb").is_err());
        let mut gate = SensitiveTextGate::new(Some("x"));
        assert_eq!(gate.push("abc").expect("clean text clears"), "abc");
    }

    #[test]
    fn empty_credential_disables_the_gate() {
        let mut gate = SensitiveTextGate::new(None);
        assert_eq!(gate.push(CANARY).expect("passthrough"), CANARY);
        assert_eq!(gate.finish().expect("passthrough"), "");
        let mut gate = SensitiveTextGate::new(Some(""));
        assert_eq!(gate.push(CANARY).expect("passthrough"), CANARY);
    }

    #[test]
    fn credential_inside_larger_text_is_refused() {
        let mut gate = gate();
        assert!(gate.push(&format!("prefix {CANARY} suffix")).is_err());
    }

    #[test]
    fn overlapping_suffix_is_not_double_counted() {
        // C07: tail and unverified overlap over the same emitted bytes;
        // finish must not concatenate them. key "aba" never occurs in
        // the emitted "ba", so the stream must flush cleanly.
        let mut gate = SensitiveTextGate::new(Some("aba"));
        let published = gate.push("ba").expect("clean text should clear");
        assert_eq!(published, "");
        assert_eq!(gate.finish().expect("overlapping suffix must flush"), "ba");
    }

    #[test]
    fn clean_bodies_round_trip_at_every_split() {
        // C07 counterpart to the refusal property: credential-free bodies
        // round-trip byte-complete at every split position, and no
        // published prefix ever completes the key.
        let cases = [
            ("aba", "ba"),
            ("aba", "abba"),
            ("aba", "aabbaa"),
            (CANARY, "hello, world"),
            (CANARY, "qcg-canary-credential-1234"),
            ("sk-secret-credential-12345", "日本語の応答テスト"),
        ];
        for (key, body) in cases {
            assert!(
                !body.contains(key),
                "fixture body must be credential-free: {body}"
            );
            for split in 0..=body.len() {
                if !body.is_char_boundary(split) {
                    continue;
                }
                let (head, tail) = body.split_at(split);
                let mut gate = SensitiveTextGate::new(Some(key));
                let mut published = gate
                    .push(head)
                    .unwrap_or_else(|()| panic!("clean head must clear for {key:?}/{body:?}"));
                assert!(
                    !published.contains(key),
                    "published prefix must never complete the key"
                );
                published.push_str(
                    &gate
                        .push(tail)
                        .unwrap_or_else(|()| panic!("clean tail must clear for {key:?}/{body:?}")),
                );
                assert!(
                    !published.contains(key),
                    "published prefix must never complete the key"
                );
                published.push_str(
                    &gate
                        .finish()
                        .unwrap_or_else(|()| panic!("clean body must flush for {key:?}/{body:?}")),
                );
                assert_eq!(
                    published, body,
                    "clean body must round-trip at split {split}"
                );
            }
        }
    }
}
