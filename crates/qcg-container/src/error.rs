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
    #[error("container {stage} timed out for instance `{instance}`")]
    StageTimedOut {
        stage: &'static str,
        instance: String,
    },
    #[error("container operation was canceled")]
    Canceled,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
