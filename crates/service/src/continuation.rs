//! Source-relative evidence binding for the offline failed-run continuation.
//! Redacted refusals and the projection manifest live here; no caller text,
//! path, value or I/O detail can escape through `ContinuationRefusal`.
use crate::{ForkFileObservation, PackageLimits, PreparedForkPlan};
use camino::{Utf8Path, Utf8PathBuf};
use engine::{ContinuationRefusal as PlanRefusal, ResetPlanCandidate};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationRefusal {
    Unavailable,
    Source,
    NotFailed,
    InFlight,
    Planning(PlanRefusal),
    Manifest,
    Admission,
}

impl ContinuationRefusal {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Unavailable => "continuation service is unavailable",
            Self::Source => "source run is unavailable or has no pinned generator package",
            Self::NotFailed => "source run is not terminally failed",
            Self::InFlight => "source run is executing or has in-flight work",
            Self::Planning(refusal) => refusal.reason(),
            Self::Manifest => "source tree cannot be bound to a safe projection manifest",
            Self::Admission => "child continuation admission was refused",
        }
    }
}

impl std::fmt::Display for ContinuationRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.reason())
    }
}

impl std::error::Error for ContinuationRefusal {}

impl From<PlanRefusal> for ContinuationRefusal {
    fn from(refusal: PlanRefusal) -> Self {
        Self::Planning(refusal)
    }
}

/// Bounded, no-follow observation of the projection inputs plus every file
/// the live plan observed. Workspace bytes are not projection inputs; the
/// plan's own file observations are checked here again under the caller's
/// source lease, so a changed file cannot silently enter a child manifest.
pub(crate) fn prepare_plan(
    source_dir: &Utf8Path,
    candidate: &ResetPlanCandidate,
) -> Result<PreparedForkPlan, ContinuationRefusal> {
    let limits = PackageLimits::default();
    let max_bytes = limits.max_bytes.ok_or(ContinuationRefusal::Manifest)?;
    let max_entries = policy::retention::directory_scan_limit_from_env()
        .map_err(|_| ContinuationRefusal::Manifest)?;
    let mut files = Vec::new();
    let mut total = 0u64;
    for root in [
        source_dir.join("meta"),
        source_dir.join("generator-package"),
    ] {
        if root.exists() {
            observe_tree(
                source_dir,
                &root,
                &mut files,
                &mut total,
                max_bytes,
                max_entries,
            )?;
        }
    }
    for (relative, expected) in candidate
        .required_file_observations()
        .ok_or(ContinuationRefusal::Manifest)?
    {
        let relative_name = policy::portable_relative_path(relative);
        let relative = Utf8Path::new(&relative_name);
        let path = source_dir.join(relative);
        match expected {
            Some(expected) => {
                let (sha256, length) = files::hash_file_sha256(&path, Some(max_bytes))
                    .map_err(|_| ContinuationRefusal::Manifest)?;
                if sha256 != *expected {
                    return Err(ContinuationRefusal::Manifest);
                }
                total = total
                    .checked_add(length)
                    .filter(|total| *total <= max_bytes)
                    .ok_or(ContinuationRefusal::Manifest)?;
                if !files
                    .iter()
                    .any(|file: &ForkFileObservation| file.path == relative)
                {
                    if files.len() >= max_entries {
                        return Err(ContinuationRefusal::Manifest);
                    }
                    files.push(ForkFileObservation {
                        path: relative.to_owned(),
                        length,
                        sha256,
                    });
                }
            }
            None => {
                if std::fs::symlink_metadata(&path).is_ok() {
                    return Err(ContinuationRefusal::Manifest);
                }
            }
        }
    }
    PreparedForkPlan::from_reset_candidate(candidate, files)
        .map_err(|_| ContinuationRefusal::Manifest)
}

fn observe_tree(
    root: &Utf8Path,
    dir: &Utf8Path,
    files: &mut Vec<ForkFileObservation>,
    total: &mut u64,
    max_bytes: u64,
    max_entries: usize,
) -> Result<(), ContinuationRefusal> {
    let entries = std::fs::read_dir(dir).map_err(|_| ContinuationRefusal::Manifest)?;
    for entry in entries {
        let entry = entry.map_err(|_| ContinuationRefusal::Manifest)?;
        let path =
            Utf8PathBuf::from_path_buf(entry.path()).map_err(|_| ContinuationRefusal::Manifest)?;
        let kind = entry
            .file_type()
            .map_err(|_| ContinuationRefusal::Manifest)?;
        if kind.is_dir() {
            observe_tree(root, &path, files, total, max_bytes, max_entries)?;
        } else if kind.is_file() {
            if files.len() >= max_entries {
                return Err(ContinuationRefusal::Manifest);
            }
            let (sha256, length) = files::hash_file_sha256(&path, Some(max_bytes))
                .map_err(|_| ContinuationRefusal::Manifest)?;
            *total = total
                .checked_add(length)
                .filter(|total| *total <= max_bytes)
                .ok_or(ContinuationRefusal::Manifest)?;
            files.push(ForkFileObservation {
                path: Utf8PathBuf::from(policy::portable_relative_path(
                    path.strip_prefix(root)
                        .map_err(|_| ContinuationRefusal::Manifest)?,
                )),
                length,
                sha256,
            });
        } else {
            return Err(ContinuationRefusal::Manifest);
        }
    }
    Ok(())
}

/// Stable, redacted evidence used to confirm the live recheck inside the
/// child admission gate produced exactly the plan that was bound.
pub(crate) fn plan_fingerprint(
    candidate: &ResetPlanCandidate,
) -> Result<Value, ContinuationRefusal> {
    serde_json::to_value(candidate).map_err(|_| ContinuationRefusal::Manifest)
}
