#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{detail}")]
    NotFound { detail: String },
    #[error("{reason}")]
    Invalid {
        field: Option<String>,
        reason: String,
    },
    #[error("payload is too large: {actual_bytes} > {limit_bytes} bytes")]
    TooLarge {
        limit_bytes: usize,
        actual_bytes: usize,
    },
    #[error("{detail}")]
    Conflict { detail: String },
    #[error("{detail}")]
    Unsupported { detail: String },
    #[error("{detail}")]
    Unavailable { detail: String },
    #[error("{detail}")]
    Internal { detail: String },
}

impl ApiError {
    pub fn invalid(reason: impl Into<String>) -> Self {
        Self::Invalid {
            field: None,
            reason: reason.into(),
        }
    }

    pub fn invalid_field(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field: Some(field.into()),
            reason: reason.into(),
        }
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Self::NotFound {
            detail: detail.into(),
        }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal {
            detail: detail.into(),
        }
    }

    pub fn unavailable(detail: impl Into<String>) -> Self {
        Self::Unavailable {
            detail: detail.into(),
        }
    }
}
