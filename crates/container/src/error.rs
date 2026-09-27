#[derive(Debug, thiserror::Error)]
pub enum ContainerError {
    #[error("container client `{binary}` was not found on PATH")]
    BinaryMissing { binary: String },
    #[error("container image `{image}` is not valid for {backend}: {reason}")]
    InvalidImage {
        image: String,
        backend: String,
        reason: String,
    },
    #[error("container {stage} failed for instance `{instance}`: {detail}")]
    StageFailed {
        stage: &'static str,
        instance: String,
        detail: String,
    },
    /// The daemon confirmed the instance itself is absent. Returned only
    /// when the teardown process spawned, exited non-zero, and named the
    /// instance in an absence report. Spawn failures, timeouts, and
    /// connection errors are never this variant (C05).
    #[error("container {stage} found instance `{instance}` already absent")]
    InstanceAbsent {
        stage: &'static str,
        instance: String,
    },
    /// The daemon refused creation because the instance name already
    /// exists. With a full UUID name this means the name belongs to
    /// another provisioning attempt, so ownership must not be adopted and
    /// no stop/delete may be sent to it (D01).
    #[error("container {stage} found instance `{instance}` already exists")]
    InstanceExists {
        stage: &'static str,
        instance: String,
    },
    #[error(
        "container {stage} timed out after {timeout_secs}s for instance `{instance}`; daemon-side state is unknown and the instance may still exist"
    )]
    StageTimedOut {
        stage: &'static str,
        instance: String,
        timeout_secs: u64,
    },
    #[error("container operation was canceled")]
    Canceled,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
