use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use http::{HeaderName, HeaderValue, header};
use reqwest::StatusCode;
use rmcp::model::{ClientJsonRpcMessage, ErrorData, RequestId, ServerJsonRpcMessage};
use rmcp::transport::streamable_http_client::{
    AuthRequiredError, InsufficientScopeError, SseError, StreamableHttpClient, StreamableHttpError,
    StreamableHttpPostResponse,
};
use sse_stream::{Sse, SseStream};
use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Poll, ready};

const JSON_MIME_TYPE: &str = "application/json";
const EVENT_STREAM_MIME_TYPE: &str = "text/event-stream";
const HEADER_SESSION_ID: &str = "mcp-session-id";

#[derive(Clone, Debug)]
pub(crate) struct BoundedHttpClient {
    inner: reqwest::Client,
    max_response_bytes: usize,
}

impl BoundedHttpClient {
    pub(crate) fn new(
        timeout_seconds: u64,
        max_response_bytes: usize,
    ) -> Result<Self, reqwest::Error> {
        Ok(Self {
            inner: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout_seconds))
                .redirect(reqwest::redirect::Policy::none())
                .pool_max_idle_per_host(0)
                .build()?,
            max_response_bytes,
        })
    }
}

impl StreamableHttpClient for BoundedHttpClient {
    type Error = reqwest::Error;

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session_id,
            last_event_id,
            auth_header,
            custom_headers,
            self.max_response_bytes,
        )
        .await
    }

    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session_id: Option<Arc<str>>,
        last_event_id: Option<String>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        <reqwest::Client as StreamableHttpClient>::get_stream_with_max_sse_event_size(
            &self.inner,
            uri,
            session_id,
            last_event_id,
            auth_header,
            custom_headers,
            max_sse_event_size.min(self.max_response_bytes),
        )
        .await
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        <reqwest::Client as StreamableHttpClient>::delete_session(
            &self.inner,
            uri,
            session_id,
            auth_header,
            custom_headers,
        )
        .await
    }

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session_id,
            auth_header,
            custom_headers,
            self.max_response_bytes,
        )
        .await
    }

    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        max_sse_event_size: usize,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let request_id = match &message {
            ClientJsonRpcMessage::Request(request) => Some(request.id.clone()),
            _ => None,
        };
        let mut request = self
            .inner
            .post(uri.as_ref())
            .header(
                header::ACCEPT,
                [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", "),
            )
            .json(&message);
        if let Some(auth_header) = auth_header {
            request = request.bearer_auth(auth_header);
        }
        request = apply_custom_headers(request, custom_headers)?;
        let had_session = session_id.is_some();
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let response = request.send().await.map_err(StreamableHttpError::Client)?;
        if let Some(error) = authorization_error(&response)? {
            return Err(error);
        }
        let status = response.status();
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == StatusCode::NOT_FOUND && had_session {
            return Err(StreamableHttpError::SessionExpired);
        }
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
        let content_length = response.content_length();
        let session_id = response
            .headers()
            .get(HEADER_SESSION_ID)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if content_length.is_some_and(|length| length > self.max_response_bytes as u64) {
            return response_too_large_message(request_id, session_id, self.max_response_bytes);
        }
        if status.is_success()
            && content_length == Some(0)
            && matches!(
                message,
                ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_)
            )
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !status.is_success() {
            if content_type_matches(content_type.as_deref(), JSON_MIME_TYPE) {
                let body = match read_bounded_body(response, self.max_response_bytes).await {
                    Ok(body) => body,
                    Err(BodyReadError::TooLarge) => {
                        return response_too_large_message(
                            request_id,
                            session_id,
                            self.max_response_bytes,
                        );
                    }
                    Err(BodyReadError::Client(error)) => {
                        return Err(StreamableHttpError::Client(error));
                    }
                };
                if let Ok(message @ ServerJsonRpcMessage::Error(_)) =
                    serde_json::from_slice::<ServerJsonRpcMessage>(&body)
                {
                    return Ok(StreamableHttpPostResponse::Json(message, session_id));
                }
            }
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!("HTTP {status}"),
            )));
        }
        if content_type_matches(content_type.as_deref(), EVENT_STREAM_MIME_TYPE) {
            return Ok(StreamableHttpPostResponse::Sse(
                bounded_sse_stream(
                    response.bytes_stream(),
                    max_sse_event_size.min(self.max_response_bytes),
                ),
                session_id,
            ));
        }
        if content_type_matches(content_type.as_deref(), JSON_MIME_TYPE) {
            let body = match read_bounded_body(response, self.max_response_bytes).await {
                Ok(body) => body,
                Err(BodyReadError::TooLarge) => {
                    return response_too_large_message(
                        request_id,
                        session_id,
                        self.max_response_bytes,
                    );
                }
                Err(BodyReadError::Client(error)) => {
                    return Err(StreamableHttpError::Client(error));
                }
            };
            return decode_json_response(&body, session_id);
        }
        Err(StreamableHttpError::UnexpectedContentType(content_type))
    }
}

fn decode_json_response(
    body: &[u8],
    session_id: Option<String>,
) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>> {
    let message = serde_json::from_slice::<ServerJsonRpcMessage>(body).map_err(|error| {
        StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
            "HTTP response contained invalid JSON-RPC: {error}"
        )))
    })?;
    Ok(StreamableHttpPostResponse::Json(message, session_id))
}

fn apply_custom_headers(
    mut request: reqwest::RequestBuilder,
    headers: HashMap<HeaderName, HeaderValue>,
) -> Result<reqwest::RequestBuilder, StreamableHttpError<reqwest::Error>> {
    for (name, value) in headers {
        if crate::reserved_transport_header(name.as_str()) {
            return Err(StreamableHttpError::ReservedHeaderConflict(
                name.to_string(),
            ));
        }
        request = request.header(name, value);
    }
    Ok(request)
}

fn authorization_error(
    response: &reqwest::Response,
) -> Result<Option<StreamableHttpError<reqwest::Error>>, StreamableHttpError<reqwest::Error>> {
    let Some(challenge) = response.headers().get(header::WWW_AUTHENTICATE) else {
        return Ok(None);
    };
    let challenge = challenge
        .to_str()
        .map_err(|_| {
            StreamableHttpError::UnexpectedServerResponse(
                "invalid www-authenticate header value".into(),
            )
        })?
        .to_owned();
    match response.status() {
        StatusCode::UNAUTHORIZED => Ok(Some(StreamableHttpError::AuthRequired(
            AuthRequiredError::new(challenge),
        ))),
        StatusCode::FORBIDDEN => Ok(Some(StreamableHttpError::InsufficientScope(
            InsufficientScopeError::new(challenge.clone(), extract_scope(&challenge)),
        ))),
        _ => Ok(None),
    }
}

fn extract_scope(challenge: &str) -> Option<String> {
    challenge.split(',').find_map(|part| {
        let value = part.trim().strip_prefix("scope=")?.trim();
        Some(value.trim_matches('"').to_owned())
    })
}

fn content_type_matches(content_type: Option<&str>, expected: &str) -> bool {
    content_type
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
}

async fn read_bounded_body(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, BodyReadError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(BodyReadError::Client)?;
        if chunk.len() > limit.saturating_sub(body.len()) {
            return Err(BodyReadError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

enum BodyReadError {
    Client(reqwest::Error),
    TooLarge,
}

fn response_too_large_message(
    request_id: Option<RequestId>,
    session_id: Option<String>,
    limit: usize,
) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>> {
    let Some(request_id) = request_id else {
        return Err(response_too_large(limit));
    };
    Ok(StreamableHttpPostResponse::Json(
        ServerJsonRpcMessage::error(
            ErrorData::internal_error(format!("HTTP response exceeded {limit} bytes"), None),
            Some(request_id),
        ),
        session_id,
    ))
}

fn response_too_large(limit: usize) -> StreamableHttpError<reqwest::Error> {
    StreamableHttpError::UnexpectedServerResponse(Cow::Owned(format!(
        "HTTP response exceeded {limit} bytes"
    )))
}

#[derive(Debug, thiserror::Error)]
enum BoundedSseStreamError {
    #[error("SSE source failed")]
    Source,
    #[error("SSE event exceeded {limit} bytes")]
    EventTooLarge { limit: usize },
}

#[derive(Debug)]
struct SseEventSizeLimiter {
    limit: usize,
    retained_size: usize,
    line_size: usize,
    line_is_comment: bool,
    previous_was_cr: bool,
}

impl SseEventSizeLimiter {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            retained_size: 0,
            line_size: 0,
            line_is_comment: false,
            previous_was_cr: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for &byte in chunk {
            if self.previous_was_cr {
                self.previous_was_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            match byte {
                b'\r' => {
                    self.finish_line()?;
                    self.previous_was_cr = true;
                }
                b'\n' => self.finish_line()?,
                _ => {
                    if self.line_size == 0 {
                        self.line_is_comment = byte == b':';
                    }
                    self.line_size = self.line_size.saturating_add(1);
                    self.check_limit()?;
                }
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), ()> {
        if self.line_size == 0 {
            self.retained_size = 0;
        } else if !self.line_is_comment {
            self.retained_size = self
                .retained_size
                .saturating_add(self.line_size)
                .saturating_add(1);
        }
        self.line_size = 0;
        self.line_is_comment = false;
        self.check_limit()
    }

    fn check_limit(&self) -> Result<(), ()> {
        (self.retained_size.saturating_add(self.line_size) <= self.limit)
            .then_some(())
            .ok_or(())
    }
}

pin_project_lite::pin_project! {
    struct BoundedSseByteStream<S> {
        #[pin]
        inner: S,
        limiter: SseEventSizeLimiter,
        failed: bool,
    }
}

impl<S, E> Stream for BoundedSseByteStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, BoundedSseStreamError>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }
        match ready!(this.inner.as_mut().poll_next(context)) {
            Some(Ok(chunk)) if this.limiter.observe(&chunk).is_ok() => Poll::Ready(Some(Ok(chunk))),
            Some(Ok(_)) => {
                *this.failed = true;
                Poll::Ready(Some(Err(BoundedSseStreamError::EventTooLarge {
                    limit: this.limiter.limit,
                })))
            }
            Some(Err(_)) => {
                *this.failed = true;
                Poll::Ready(Some(Err(BoundedSseStreamError::Source)))
            }
            None => Poll::Ready(None),
        }
    }
}

fn bounded_sse_stream<S, E>(stream: S, limit: usize) -> BoxStream<'static, Result<Sse, SseError>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    SseStream::from_bytes_stream(BoundedSseByteStream {
        inner: stream,
        limiter: SseEventSizeLimiter::new(limit),
        failed: false,
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_success_json_is_a_protocol_error() {
        let error = decode_json_response(br#"{"jsonrpc":"2.0","result":}"#, None)
            .expect_err("malformed JSON-RPC must not be treated as an accepted notification");

        assert!(
            error.to_string().contains("invalid JSON-RPC"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn content_type_matching_uses_the_complete_media_type() {
        assert!(content_type_matches(
            Some("application/json; charset=utf-8"),
            JSON_MIME_TYPE
        ));
        assert!(content_type_matches(
            Some("Application/JSON"),
            JSON_MIME_TYPE
        ));
        assert!(!content_type_matches(
            Some("application/jsonp"),
            JSON_MIME_TYPE
        ));
    }
}
