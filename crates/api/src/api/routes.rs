use super::openapi_types::{
    ApiHeader, ApiParameter, ApiResponse, ApiRoute, ParameterSchema, ResponseBody, ResponseSchema,
};

/// Statuses every route can answer regardless of its own handler, because the
/// bearer-auth middleware and the rate limiter run in front of all of them.
/// Declared once here so the document cannot describe a route as unable to
/// return 401 or 429 while the server does exactly that.
pub const ERR_AUTH: &[u16] = &[401];
pub const ERR_RATE_LIMITED: &[u16] = &[429];

const ERR_INTERNAL: &[u16] = &[500];
const ERR_RESOURCE: &[u16] = &[400, 404, 500];
const ERR_INVALID: &[u16] = &[400, 500];
const ERR_MCP_MUTATION: &[u16] = &[400, 403];
const ERR_MCP_CALLBACK: &[u16] = &[400, 403, 500];
const ERR_START_RUN: &[u16] = &[400, 409, 413, 422, 500, 503];
const ERR_INTERACTION: &[u16] = &[400, 404, 409, 422, 500, 503];
const ERR_MUTATION: &[u16] = &[404, 409, 500];

const NO_HEADERS: &[ApiHeader] = &[];
const NO_QUERY_PARAMETERS: &[ApiParameter] = &[];
/// Pagination bounds for `GET /api/runs`, shared by the OpenAPI document and
/// the server handler so the documented and enforced ranges cannot diverge.
/// Path that bypasses the rate limiter so a saturated limiter can never make
/// the process look unhealthy. The document and the middleware share this
/// constant instead of each naming the path.
pub const RATE_LIMIT_EXEMPT_PATH: &str = "/healthz";

/// Every status the shared middleware can produce for `path`, independent of
/// the route handler: bearer authentication applies to all routes and rate
/// limiting applies to every route except the exempt one.
pub fn middleware_error_statuses(path: &str) -> Vec<u16> {
    let mut statuses = ERR_AUTH.to_vec();
    if path != RATE_LIMIT_EXEMPT_PATH {
        statuses.extend_from_slice(ERR_RATE_LIMITED);
    }
    statuses
}

pub const RUN_LIST_LIMIT_MIN: usize = 1;
pub const RUN_LIST_LIMIT_MAX: usize = 200;
pub const RUN_LIST_LIMIT_DEFAULT: usize = 50;
const RUN_LIST_QUERY_PARAMETERS: &[ApiParameter] = &[
    ApiParameter {
        name: "limit",
        required: false,
        schema: ParameterSchema::Integer {
            minimum: Some(RUN_LIST_LIMIT_MIN as u64),
            maximum: Some(RUN_LIST_LIMIT_MAX as u64),
            default: Some(RUN_LIST_LIMIT_DEFAULT as u64),
        },
    },
    ApiParameter {
        name: "cursor",
        required: false,
        schema: ParameterSchema::String,
    },
    ApiParameter {
        name: "state",
        required: false,
        schema: ParameterSchema::Ref("RunStatus"),
    },
    ApiParameter {
        name: "generator_id",
        required: false,
        schema: ParameterSchema::String,
    },
    ApiParameter {
        name: "since",
        required: false,
        schema: ParameterSchema::DateTime,
    },
    ApiParameter {
        name: "order",
        required: false,
        schema: ParameterSchema::Ref("RunListOrder"),
    },
];
const OAUTH_CALLBACK_QUERY_PARAMETERS: &[ApiParameter] = &[
    ApiParameter {
        name: "code",
        required: false,
        schema: ParameterSchema::String,
    },
    ApiParameter {
        name: "state",
        required: true,
        schema: ParameterSchema::String,
    },
    ApiParameter {
        name: "iss",
        required: false,
        schema: ParameterSchema::String,
    },
    ApiParameter {
        name: "error",
        required: false,
        schema: ParameterSchema::String,
    },
];
const NO_ADDITIONAL_RESPONSES: &[ApiResponse] = &[];
const IDEMPOTENCY_HEADERS: &[ApiHeader] = &[ApiHeader {
    name: "Idempotency-Key",
    description: "Retries with the same key and request body return the original run.",
    required: false,
}];
const LAST_EVENT_ID_HEADERS: &[ApiHeader] = &[ApiHeader {
    name: "Last-Event-ID",
    description: "Resume the event stream after this event sequence number.",
    required: false,
}];
const LOCATION_HEADERS: &[ApiHeader] = &[ApiHeader {
    name: "Location",
    description: "URL of the newly created run.",
    required: true,
}];
const ETAG_HEADERS: &[ApiHeader] = &[ApiHeader {
    name: "ETag",
    description: "Weak validator for conditional GET requests.",
    required: true,
}];
const IF_NONE_MATCH_HEADERS: &[ApiHeader] = &[ApiHeader {
    name: "If-None-Match",
    description: "Return 304 when this validator matches the current representation.",
    required: false,
}];
const NOT_MODIFIED_RESPONSES: &[ApiResponse] = &[ApiResponse {
    status: 304,
    description: "Not modified",
    body: ResponseBody::Empty,
    headers: ETAG_HEADERS,
}];

const ARTIFACT_REQUEST_HEADERS: &[ApiHeader] = &[
    ApiHeader {
        name: "If-None-Match",
        description: "Return 304 when the artifact digest matches.",
        required: false,
    },
    ApiHeader {
        name: "Range",
        description: "One bytes=start-end, bytes=start- or bytes=-suffix range.",
        required: false,
    },
    ApiHeader {
        name: "If-Range",
        description: "Apply Range only when this strong ETag matches.",
        required: false,
    },
];
const ARTIFACT_RESPONSE_HEADERS: &[ApiHeader] = &[
    ApiHeader {
        name: "ETag",
        description: "Strong validator naming the verified artifact SHA-256.",
        required: true,
    },
    ApiHeader {
        name: "Accept-Ranges",
        description: "Supported range unit: bytes.",
        required: true,
    },
    ApiHeader {
        name: "Content-Disposition",
        description: "Attachment filename.",
        required: true,
    },
];
const ARTIFACT_RESPONSES: &[ApiResponse] = &[
    ApiResponse {
        status: 304,
        description: "Not modified",
        body: ResponseBody::Empty,
        headers: ETAG_HEADERS,
    },
    ApiResponse {
        status: 206,
        description: "Requested byte range",
        body: ResponseBody::Binary("application/octet-stream"),
        headers: ARTIFACT_RESPONSE_HEADERS,
    },
    ApiResponse {
        status: 416,
        description: "Unsatisfiable byte range; Content-Range gives the artifact length",
        body: ResponseBody::Empty,
        headers: NO_HEADERS,
    },
];
const ERR_ARTIFACT: &[u16] = &[400, 404, 413, 416, 500];

pub const API_ROUTES: &[ApiRoute] = &[
    ApiRoute {
        method: "get",
        path: "/healthz",
        summary: "Health check",
        response: ApiResponse {
            status: 200,
            description: "Server is healthy",
            body: ResponseBody::Json(None),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "get",
        path: "/metrics",
        summary: "Prometheus metrics",
        response: ApiResponse {
            status: 200,
            description: "Prometheus text exposition",
            body: ResponseBody::Text("text/plain; version=0.0.4"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "get",
        path: "/api/openapi.json",
        summary: "OpenAPI document",
        response: ApiResponse {
            status: 200,
            description: "OpenAPI document",
            body: ResponseBody::Json(None),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "get",
        path: "/api/llm/catalog",
        summary: "List selectable LLM providers and models",
        response: ApiResponse {
            status: 200,
            description: "Selectable provider, model, and reasoning effort metadata",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("LlmCatalogResponse"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: &[ApiParameter {
            name: "refresh",
            required: false,
            schema: ParameterSchema::Boolean,
        }],
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "get",
        path: "/api/generators",
        summary: "List generators",
        response: ApiResponse {
            status: 200,
            description: "Available generators",
            body: ResponseBody::Json(Some(ResponseSchema::ArrayRef("GeneratorSummary"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "get",
        path: "/api/generators/{id}",
        summary: "Describe a generator",
        response: ApiResponse {
            status: 200,
            description: "Generator detail",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("GeneratorDetail"))),
            headers: ETAG_HEADERS,
        },
        additional_responses: NOT_MODIFIED_RESPONSES,
        request_schema: None,
        request_headers: IF_NONE_MATCH_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/generators/{id}/assets/{path}",
        summary: "Read a declared generator asset",
        response: ApiResponse {
            status: 200,
            description: "Generator asset bytes",
            body: ResponseBody::Binary("application/octet-stream"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/mcp/servers",
        summary: "List configured MCP servers and authorization status",
        response: ApiResponse {
            status: 200,
            description: "Configured MCP servers",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("McpServerList"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERNAL,
    },
    ApiRoute {
        method: "post",
        path: "/api/mcp/servers/{id}/authorization",
        summary: "Start MCP OAuth authorization",
        response: ApiResponse {
            status: 200,
            description: "Authorization URL",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("McpAuthorizationStart"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_MCP_MUTATION,
    },
    ApiRoute {
        method: "delete",
        path: "/api/mcp/servers/{id}/authorization",
        summary: "Clear stored MCP OAuth authorization",
        response: ApiResponse {
            status: 204,
            description: "Authorization cleared",
            body: ResponseBody::Empty,
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_MCP_MUTATION,
    },
    ApiRoute {
        method: "delete",
        path: "/api/mcp/servers/{id}/authorization/pending",
        summary: "Cancel a pending MCP OAuth authorization",
        response: ApiResponse {
            status: 204,
            description: "Pending authorization canceled",
            body: ResponseBody::Empty,
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_MCP_MUTATION,
    },
    ApiRoute {
        method: "get",
        path: "/api/mcp/oauth/callback",
        summary: "Complete an MCP OAuth authorization callback",
        response: ApiResponse {
            status: 200,
            description: "Authorization completed",
            body: ResponseBody::Text("text/html"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: OAUTH_CALLBACK_QUERY_PARAMETERS,
        errors: ERR_MCP_CALLBACK,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs",
        summary: "List runs",
        response: ApiResponse {
            status: 200,
            description: "Known runs",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunListResponse"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: RUN_LIST_QUERY_PARAMETERS,
        errors: ERR_INVALID,
    },
    ApiRoute {
        method: "post",
        path: "/api/runs",
        summary: "Start a run",
        response: ApiResponse {
            status: 201,
            description: "Started run",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: LOCATION_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: Some("StartRun"),
        request_headers: IDEMPOTENCY_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_START_RUN,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}",
        summary: "Run snapshot",
        response: ApiResponse {
            status: 200,
            description: "Run snapshot",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: ETAG_HEADERS,
        },
        additional_responses: NOT_MODIFIED_RESPONSES,
        request_schema: None,
        request_headers: IF_NONE_MATCH_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "post",
        path: "/api/runs/{id}/fork",
        summary: "Fork a run from a durable checkpoint",
        response: ApiResponse {
            status: 201,
            description: "Forked run",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: LOCATION_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: Some("ForkRun"),
        request_headers: IDEMPOTENCY_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_START_RUN,
    },
    ApiRoute {
        method: "put",
        path: "/api/runs/{id}/questions/{qid}",
        summary: "Answer a pending run question",
        response: ApiResponse {
            status: 200,
            description: "Updated run snapshot",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: Some("AnswerPayload"),
        request_headers: IDEMPOTENCY_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERACTION,
    },
    ApiRoute {
        method: "put",
        path: "/api/runs/{id}/confirmations/{cid}",
        summary: "Confirm or deny a pending side effect",
        response: ApiResponse {
            status: 200,
            description: "Updated run snapshot",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: Some("ConfirmDecision"),
        request_headers: IDEMPOTENCY_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_INTERACTION,
    },
    ApiRoute {
        method: "post",
        path: "/api/runs/{id}:cancel",
        summary: "Cancel a run",
        response: ApiResponse {
            status: 200,
            description: "Settled run snapshot",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunSnapshot"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: IDEMPOTENCY_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_MUTATION,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/events",
        summary: "Subscribe to run events",
        response: ApiResponse {
            status: 200,
            description: "Server-sent event stream",
            body: ResponseBody::Text("text/event-stream"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: LAST_EVENT_ID_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/artifacts",
        summary: "Run output manifest",
        response: ApiResponse {
            status: 200,
            description: "Output manifest",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("OutputManifest"))),
            headers: ETAG_HEADERS,
        },
        additional_responses: NOT_MODIFIED_RESPONSES,
        request_schema: None,
        request_headers: IF_NONE_MATCH_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/artifacts/{path}",
        summary: "Read an artifact",
        response: ApiResponse {
            status: 200,
            description: "Artifact bytes",
            body: ResponseBody::Binary("application/octet-stream"),
            headers: ARTIFACT_RESPONSE_HEADERS,
        },
        additional_responses: ARTIFACT_RESPONSES,
        request_schema: None,
        request_headers: ARTIFACT_REQUEST_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_ARTIFACT,
    },
    ApiRoute {
        method: "delete",
        path: "/api/runs/{id}",
        summary: "Delete a terminal run",
        response: ApiResponse {
            status: 204,
            description: "Run deleted",
            body: ResponseBody::Empty,
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_MUTATION,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/artifacts.zip",
        summary: "Download all artifacts as zip",
        response: ApiResponse {
            status: 200,
            description: "Artifact zip",
            body: ResponseBody::Binary("application/zip"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/bundle",
        summary: "Download a self-contained run bundle as zip",
        response: ApiResponse {
            status: 200,
            description: "Run bundle zip",
            body: ResponseBody::Binary("application/zip"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/journal",
        summary: "Read run journal",
        response: ApiResponse {
            status: 200,
            description: "Run journal JSONL",
            body: ResponseBody::Text("application/x-ndjson"),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
    ApiRoute {
        method: "get",
        path: "/api/runs/{id}/metrics",
        summary: "Read run cost metrics",
        response: ApiResponse {
            status: 200,
            description: "Run cost metrics with USD estimate",
            body: ResponseBody::Json(Some(ResponseSchema::Ref("RunCostMetrics"))),
            headers: NO_HEADERS,
        },
        additional_responses: NO_ADDITIONAL_RESPONSES,
        request_schema: None,
        request_headers: NO_HEADERS,
        query_parameters: NO_QUERY_PARAMETERS,
        errors: ERR_RESOURCE,
    },
];
