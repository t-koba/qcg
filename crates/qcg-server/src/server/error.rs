use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use qcg_api::{ApiError, ProblemDetails, ProblemFieldError};

#[derive(Debug)]
pub(crate) struct ApiHttpError {
    pub(crate) problem: Box<ProblemDetails>,
}

impl ApiHttpError {
    pub(crate) fn bad_request(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "Invalid request",
            "invalid_request",
            message,
            "",
            Vec::new(),
        )
    }

    pub(crate) fn bad_request_field(field: impl Into<String>, reason: impl Into<String>) -> Self {
        let reason = reason.into();
        Self::new(
            StatusCode::BAD_REQUEST,
            "Invalid request",
            "invalid_request",
            reason.clone(),
            "",
            vec![ProblemFieldError {
                field: field.into(),
                reason,
            }],
        )
    }

    pub(crate) fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error",
            "internal_error",
            error.to_string(),
            "",
            Vec::new(),
        )
    }

    pub(crate) fn unauthorized(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "Unauthorized",
            "unauthorized",
            detail,
            "",
            Vec::new(),
        )
    }

    pub(crate) fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "Forbidden",
            "forbidden",
            detail,
            "",
            Vec::new(),
        )
    }

    pub(crate) fn service_unavailable(detail: impl Into<String>) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service unavailable",
            "service_unavailable",
            detail,
            "",
            Vec::new(),
        )
    }

    pub(crate) fn from_api(error: ApiError) -> Self {
        match error {
            ApiError::NotFound { detail } => Self::new(
                StatusCode::NOT_FOUND,
                "Resource not found",
                "not_found",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Invalid { field, reason } => {
                let errors: Vec<ProblemFieldError> = field
                    .map(|field| ProblemFieldError {
                        field,
                        reason: reason.clone(),
                    })
                    .into_iter()
                    .collect();
                let status = if errors.is_empty() {
                    StatusCode::BAD_REQUEST
                } else {
                    StatusCode::UNPROCESSABLE_ENTITY
                };
                Self::new(
                    status,
                    "Invalid request",
                    if status == StatusCode::UNPROCESSABLE_ENTITY {
                        "validation_failed"
                    } else {
                        "invalid_request"
                    },
                    reason,
                    "",
                    errors,
                )
            }
            ApiError::TooLarge {
                limit_bytes,
                actual_bytes,
            } => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Payload too large",
                "payload_too_large",
                format!("payload is {actual_bytes} bytes; limit is {limit_bytes} bytes"),
                "",
                Vec::new(),
            ),
            ApiError::Conflict { detail } => Self::new(
                StatusCode::CONFLICT,
                "Resource conflict",
                "conflict",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Unsupported { detail } => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "Unsupported operation",
                "unsupported",
                detail,
                "",
                Vec::new(),
            ),
            ApiError::Unavailable { detail } => Self::service_unavailable(detail),
            ApiError::Internal { detail } => Self::internal(detail),
        }
    }

    pub(crate) fn new(
        status: StatusCode,
        title: impl Into<String>,
        code: impl Into<String>,
        detail: impl Into<String>,
        instance: impl Into<String>,
        errors: Vec<ProblemFieldError>,
    ) -> Self {
        let code = code.into();
        Self {
            problem: Box::new(ProblemDetails {
                problem_type: format!("https://qcg.dev/problems/{code}"),
                title: title.into(),
                status: status.as_u16(),
                detail: detail.into(),
                instance: instance.into(),
                code,
                errors,
            }),
        }
    }
}

impl axum::response::IntoResponse for ApiHttpError {
    fn into_response(self) -> axum::response::Response {
        let status =
            StatusCode::from_u16(self.problem.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut response = (status, Json(self.problem)).into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        response
    }
}
