//! Hash-anchored line patch mechanism.
//!
//! Pure transformation core: no filesystem, no contract, no policy.
//! Callers resolve policy first and pass plain values down.
//! All anchor checks run against a single snapshot; any stale anchor
//! rejects the whole batch without modification.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

/// Hex characters kept from the line digest in each anchor.
///
/// Fixed at 8 (32 bits) by mechanism design, never tunable per contract:
/// a single-anchor false accept is 2^-32, and a maximal 128-edit batch
/// stays near 3e-8, both negligible. The full-file `base_sha256` check is
/// the second factor, so lengthening anchors would only spend tokens
/// (8 more characters per anchor, roughly 4k tokens per 2000-line read)
/// without reducing any load-bearing risk. Shorter anchors (for example
/// the external 2-character style at 8 bits, near 40% false accept per
/// maximal batch) cannot meet the fail-closed bar and are rejected here.
pub const ANCHOR_HASH_LEN: usize = 8;

/// One addressable line with its anchor hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchoredLine {
    pub line_no: usize,
    pub hash: String,
    pub text: String,
}

impl AnchoredLine {
    pub fn anchor(&self) -> String {
        format!("{}:{}", self.line_no, self.hash)
    }
}

/// Single patch operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOp {
    Replace,
    Append,
    Prepend,
}

impl PatchOp {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "replace" => Some(Self::Replace),
            "append" => Some(Self::Append),
            "prepend" => Some(Self::Prepend),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Append => "append",
            Self::Prepend => "prepend",
        }
    }
}

/// One validated edit against a snapshot anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchEdit {
    pub op: PatchOp,
    pub anchor_line: usize,
    pub anchor_hash: String,
    pub lines: Vec<String>,
}

/// Stale anchor remap hint returned without re-reading the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnchorRemap {
    pub stale_anchor: String,
    pub current_anchor: String,
}

/// Successful patch outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchOutcome {
    pub new_text: String,
    pub new_base_sha256: String,
    pub applied: usize,
}

/// Typed mechanism errors. Policy layers match on variants,
/// never on message strings.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AnchoredPatchError {
    #[error("patch has no edits")]
    EmptyEdits,
    #[error("anchor `{anchor}` is invalid")]
    AnchorInvalid { anchor: String },
    #[error("anchor `{anchor}` targets a missing line")]
    AnchorNotFound { anchor: String },
    #[error("file base mismatch: expected `{expected}`, actual `{actual}`")]
    BaseMismatch { expected: String, actual: String },
    #[error("anchor `{anchor}` is stale")]
    AnchorStale {
        anchor: String,
        remaps: Vec<AnchorRemap>,
        current_base: String,
    },
    #[error("edits overlap on line {line_no}: only one replace per line")]
    OverlappingEdits { line_no: usize },
    #[error("too many edits: {count} > {limit}")]
    TooManyEdits { count: usize, limit: usize },
    #[error("patch payload too large: {bytes} > {limit} bytes")]
    PatchTooLarge { bytes: usize, limit: usize },
    #[error("result too large: {bytes} > {limit} bytes")]
    ResultTooLarge { bytes: usize, limit: usize },
}

/// Compute the anchor hash for one line.
/// Mixes the 1-indexed line number so identical text at
/// different positions never shares an anchor.
pub fn line_hash(line_no: usize, text: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(line_no.to_string().as_bytes());
    digest.update(b":");
    digest.update(text.as_bytes());
    hex::encode(digest.finalize())[..ANCHOR_HASH_LEN].to_string()
}

/// Build the `LINE:HASH` anchor for one line.
pub fn anchor_for(line_no: usize, text: &str) -> String {
    format!("{line_no}:{}", line_hash(line_no, text))
}

/// Parse a `LINE:HASH` anchor.
pub fn parse_anchor(anchor: &str) -> Option<(usize, String)> {
    let (line, hash) = anchor.split_once(':')?;
    let line_no: usize = line.parse().ok()?;
    if line_no == 0 || hash.len() != ANCHOR_HASH_LEN {
        return None;
    }
    if !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some((line_no, hash.to_ascii_lowercase()))
}

/// Split text into lines without terminators.
/// Splits on `\n` and strips one trailing `\r` per line.
pub fn split_lines(text: &str) -> Vec<String> {
    text.split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect()
}

/// Hash the full file bytes with SHA-256 hex.
pub fn base_sha256(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// Annotate a snapshot with anchors.
pub fn annotate(text: &str) -> Vec<AnchoredLine> {
    split_lines(text)
        .into_iter()
        .enumerate()
        .map(|(index, line_text)| {
            let line_no = index + 1;
            AnchoredLine {
                hash: line_hash(line_no, &line_text),
                line_no,
                text: line_text,
            }
        })
        .collect()
}

/// Read a window of anchored lines plus the file base hash.
/// `offset` and `limit` are 1-indexed line counts; `offset` defaults
/// to 1 when 0. `limit` of 0 means no lines, which callers reject
/// at the policy layer.
pub fn read_window(text: &str, offset: usize, limit: usize) -> (String, Vec<AnchoredLine>) {
    let base = base_sha256(text);
    let start = offset.max(1);
    let lines = annotate(text)
        .into_iter()
        .skip(start - 1)
        .take(limit)
        .collect();
    (base, lines)
}

/// Parse and validate raw edits against policy-free structural rules.
/// Hash freshness is checked in [`apply_anchored_patch`] against
/// the snapshot, not here.
pub fn parse_edits(raw: &[RawPatchEdit]) -> Result<Vec<PatchEdit>, AnchoredPatchError> {
    if raw.is_empty() {
        return Err(AnchoredPatchError::EmptyEdits);
    }
    let mut edits = Vec::with_capacity(raw.len());
    for item in raw {
        let op = PatchOp::parse(&item.op).ok_or_else(|| AnchoredPatchError::AnchorInvalid {
            anchor: item.anchor.clone(),
        })?;
        let (line_no, hash) =
            parse_anchor(&item.anchor).ok_or_else(|| AnchoredPatchError::AnchorInvalid {
                anchor: item.anchor.clone(),
            })?;
        edits.push(PatchEdit {
            op,
            anchor_line: line_no,
            anchor_hash: hash,
            lines: item.lines.clone(),
        });
    }
    Ok(edits)
}

/// Raw serializable edit used at API boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPatchEdit {
    pub op: String,
    pub anchor: String,
    pub lines: Vec<String>,
}

/// Apply validated edits to a snapshot.
/// `expected_base` binds the snapshot when set; `None` checks anchors only.
/// Returns the new text with remaps on stale anchors.
pub fn apply_anchored_patch(
    base_text: &str,
    expected_base: Option<&str>,
    edits: &[PatchEdit],
    limits: PatchLimits,
) -> Result<PatchOutcome, AnchoredPatchError> {
    if edits.is_empty() {
        return Err(AnchoredPatchError::EmptyEdits);
    }
    if edits.len() > limits.max_edits {
        return Err(AnchoredPatchError::TooManyEdits {
            count: edits.len(),
            limit: limits.max_edits,
        });
    }
    let payload_bytes: usize = edits
        .iter()
        .map(|edit| edit.lines.iter().map(|line| line.len()).sum::<usize>())
        .sum();
    if payload_bytes > limits.max_patch_bytes {
        return Err(AnchoredPatchError::PatchTooLarge {
            bytes: payload_bytes,
            limit: limits.max_patch_bytes,
        });
    }
    let actual_base = base_sha256(base_text);
    if let Some(expected) = expected_base
        && expected.to_ascii_lowercase() != actual_base
    {
        return Err(AnchoredPatchError::BaseMismatch {
            expected: expected.to_string(),
            actual: actual_base,
        });
    }
    let snapshot = annotate(base_text);
    let by_line: BTreeMap<usize, &AnchoredLine> =
        snapshot.iter().map(|line| (line.line_no, line)).collect();
    let mut replace_seen = BTreeSet::new();
    for edit in edits {
        let Some(current) = by_line.get(&edit.anchor_line) else {
            return Err(AnchoredPatchError::AnchorNotFound {
                anchor: format!("{}:{}", edit.anchor_line, edit.anchor_hash),
            });
        };
        if current.hash != edit.anchor_hash {
            let remaps = vec![AnchorRemap {
                stale_anchor: format!("{}:{}", edit.anchor_line, edit.anchor_hash),
                current_anchor: current.anchor(),
            }];
            return Err(AnchoredPatchError::AnchorStale {
                anchor: format!("{}:{}", edit.anchor_line, edit.anchor_hash),
                remaps,
                current_base: actual_base,
            });
        }
        if edit.op == PatchOp::Replace && !replace_seen.insert(edit.anchor_line) {
            return Err(AnchoredPatchError::OverlappingEdits {
                line_no: edit.anchor_line,
            });
        }
    }
    let mut lines: Vec<String> = snapshot.into_iter().map(|line| line.text).collect();
    // Bottom-up application keeps earlier line numbers stable.
    // Same-line order is deterministic: prepend, replace, append.
    let mut ordered: Vec<&PatchEdit> = edits.iter().collect();
    ordered.sort_by(|left, right| {
        right
            .anchor_line
            .cmp(&left.anchor_line)
            .then_with(|| op_order(left.op).cmp(&op_order(right.op)))
    });
    for edit in ordered {
        let index = edit.anchor_line - 1;
        match edit.op {
            PatchOp::Replace => {
                if edit.lines.is_empty() {
                    lines.remove(index);
                } else if edit.lines.len() == 1 {
                    lines[index] = edit.lines[0].clone();
                } else {
                    lines.splice(index..=index, edit.lines.clone());
                }
            }
            PatchOp::Append => {
                for (offset, line) in edit.lines.iter().enumerate() {
                    lines.insert(index + 1 + offset, line.clone());
                }
            }
            PatchOp::Prepend => {
                for (offset, line) in edit.lines.iter().enumerate() {
                    lines.insert(index + offset, line.clone());
                }
            }
        }
    }
    let new_text = join_lines(base_text, &lines);
    if new_text.len() > limits.max_result_bytes {
        return Err(AnchoredPatchError::ResultTooLarge {
            bytes: new_text.len(),
            limit: limits.max_result_bytes,
        });
    }
    let new_base_sha256 = base_sha256(&new_text);
    Ok(PatchOutcome {
        new_text,
        new_base_sha256,
        applied: edits.len(),
    })
}

/// Rejoins applied lines with the snapshot's terminator. Uniform CRLF
/// snapshots stay CRLF so a patch never rewrites untouched line endings;
/// LF and mixed-ending snapshots join with LF. Anchors always hash the
/// stripped form, so matching is unaffected either way.
fn join_lines(base_text: &str, lines: &[String]) -> String {
    if uniform_crlf(base_text) {
        lines.join("\r\n")
    } else {
        lines.join("\n")
    }
}

/// Detects uniform CRLF snapshots: every line feed pairs with a carriage
/// return and vice versa. Anything else is LF or mixed and normalizes.
fn uniform_crlf(text: &str) -> bool {
    let bytes = text.as_bytes();
    if !bytes.contains(&b'\r') || !bytes.contains(&b'\n') {
        return false;
    }
    bytes.iter().enumerate().all(|(index, &byte)| match byte {
        b'\n' => index > 0 && bytes[index - 1] == b'\r',
        b'\r' => bytes.get(index + 1) == Some(&b'\n'),
        _ => true,
    })
}

fn op_order(op: PatchOp) -> u8 {
    match op {
        PatchOp::Append => 0,
        PatchOp::Replace => 1,
        PatchOp::Prepend => 2,
    }
}

/// Plain numeric bounds passed from the policy layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PatchLimits {
    pub max_edits: usize,
    pub max_patch_bytes: usize,
    pub max_result_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_text_at_different_lines_has_different_hashes() {
        let text = "}\nlet x = 1;\n}";
        let annotated = annotate(text);
        assert_eq!(annotated.len(), 3);
        assert_ne!(annotated[0].hash, annotated[2].hash);
    }

    #[test]
    fn replace_single_line_and_report_new_base() {
        let base = "line one\nline two\nline three";
        let anchor = anchor_for(2, "line two");
        let (_, hash) = parse_anchor(&anchor).expect("anchor should parse");
        let edits = vec![PatchEdit {
            op: PatchOp::Replace,
            anchor_line: 2,
            anchor_hash: hash,
            lines: vec!["line 2 edited".into()],
        }];
        let outcome = apply_anchored_patch(
            base,
            Some(&base_sha256(base)),
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect("patch should apply");
        assert_eq!(outcome.new_text, "line one\nline 2 edited\nline three");
        assert_eq!(outcome.applied, 1);
    }

    #[test]
    fn uniform_crlf_snapshot_stays_crlf() {
        // A patch must not rewrite untouched line endings: uniform CRLF
        // rejoins with CRLF while anchors still hash the stripped form.
        let base = "line one\r\nline two\r\nline three";
        let anchor = anchor_for(2, "line two");
        let (_, hash) = parse_anchor(&anchor).expect("anchor should parse");
        let edits = vec![PatchEdit {
            op: PatchOp::Replace,
            anchor_line: 2,
            anchor_hash: hash,
            lines: vec!["line 2 edited".into()],
        }];
        let outcome = apply_anchored_patch(
            base,
            Some(&base_sha256(base)),
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect("patch should apply");
        assert_eq!(outcome.new_text, "line one\r\nline 2 edited\r\nline three");
        assert!(!uniform_crlf("line one\nline two"));
        assert!(uniform_crlf("line one\r\nline two\r\n"));
    }

    #[test]
    fn stale_anchor_rejects_batch_with_remap() {
        let edited = "alpha\nBETA";
        let stale = anchor_for(2, "beta");
        let (_, hash) = parse_anchor(&stale).expect("anchor should parse");
        let edits = vec![PatchEdit {
            op: PatchOp::Replace,
            anchor_line: 2,
            anchor_hash: hash,
            lines: vec!["gamma".into()],
        }];
        let error = apply_anchored_patch(
            edited,
            None,
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect_err("stale anchor must fail");
        match error {
            AnchoredPatchError::AnchorStale { remaps, .. } => {
                assert_eq!(remaps.len(), 1);
                assert_eq!(remaps[0].current_anchor, anchor_for(2, "BETA"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn base_mismatch_fails_before_anchor_checks() {
        let base = "one\ntwo";
        let edits = vec![PatchEdit {
            op: PatchOp::Replace,
            anchor_line: 1,
            anchor_hash: line_hash(1, "one"),
            lines: vec!["1".into()],
        }];
        let error = apply_anchored_patch(
            base,
            Some("deadbeef"),
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect_err("base mismatch must fail");
        assert!(matches!(error, AnchoredPatchError::BaseMismatch { .. }));
    }

    #[test]
    fn duplicate_replace_on_same_line_is_rejected() {
        let base = "one\ntwo";
        let hash = line_hash(1, "one");
        let edits = vec![
            PatchEdit {
                op: PatchOp::Replace,
                anchor_line: 1,
                anchor_hash: hash.clone(),
                lines: vec!["a".into()],
            },
            PatchEdit {
                op: PatchOp::Replace,
                anchor_line: 1,
                anchor_hash: hash,
                lines: vec!["b".into()],
            },
        ];
        let error = apply_anchored_patch(
            base,
            None,
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect_err("overlap must fail");
        assert!(matches!(error, AnchoredPatchError::OverlappingEdits { .. }));
    }

    #[test]
    fn bottom_up_multi_edit_keeps_line_numbers_stable() {
        let base = "a\nb\nc\nd";
        let edits = vec![
            PatchEdit {
                op: PatchOp::Replace,
                anchor_line: 2,
                anchor_hash: line_hash(2, "b"),
                lines: vec!["B".into()],
            },
            PatchEdit {
                op: PatchOp::Replace,
                anchor_line: 4,
                anchor_hash: line_hash(4, "d"),
                lines: vec!["D".into()],
            },
        ];
        let outcome = apply_anchored_patch(
            base,
            None,
            &edits,
            PatchLimits {
                max_edits: 8,
                max_patch_bytes: 1024,
                max_result_bytes: 1024,
            },
        )
        .expect("multi edit should apply");
        assert_eq!(outcome.new_text, "a\nB\nc\nD");
    }
}
