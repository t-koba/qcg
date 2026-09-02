use qcg_types::{
    AssetSpec, ConfirmSpec, FormSpec, GeneratorMeta, InputSpec, OutputManifest,
    RUN_EVENT_DATA_SCHEMAS, RunEvent,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GeneratorSummary {
    pub id: String,
    pub name: String,
    pub version: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GeneratorDetail {
    pub generator: GeneratorMeta,
    pub inputs: InputSpec,
    pub assets: AssetSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StartRun {
    pub generator_id: String,
    pub inputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkStatePatch {
    #[serde(default)]
    pub inputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub step_outputs: BTreeMap<String, Value>,
    #[serde(default)]
    pub step_statuses: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForkRun {
    pub at_seq: u64,
    #[serde(default)]
    pub state_patch: ForkStatePatch,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Waiting,
    Confirming,
    Succeeded,
    Failed,
    Canceled,
    Interrupted,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Canceled | Self::Interrupted
        )
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Confirming => "confirming",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
            Self::Interrupted => "interrupted",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunSnapshot {
    pub run_id: String,
    pub state: RunStatus,
    #[serde(default)]
    pub seq: u64,
    #[serde(default)]
    pub contract_sha256: Option<String>,
    pub artifacts: Option<OutputManifest>,
    pub question: Option<FormSpec>,
    pub confirm: Option<ConfirmSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunListItem {
    pub run_id: String,
    pub state: RunStatus,
    pub generator_id: String,
    pub started_at: String,
    #[serde(default)]
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RunListResponse {
    pub items: Vec<RunListItem>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct RunListQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<RunStatus>,
    pub generator_id: Option<String>,
    pub since: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AnswerPayload {
    pub values: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConfirmDecision {
    pub decision: ConfirmationDecision,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpServerSummary {
    pub id: String,
    pub transport: String,
    pub auth: String,
    pub authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpServerList {
    pub items: Vec<McpServerSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct McpAuthorizationStart {
    pub authorization_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConfirmationDecision {
    Approve,
    Deny,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProblemFieldError {
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
    pub code: String,
    #[serde(default)]
    pub errors: Vec<ProblemFieldError>,
}

pub fn openapi_components() -> Value {
    let mut schemas = serde_json::Map::new();
    insert_schema::<GeneratorSummary>(&mut schemas, "GeneratorSummary");
    insert_schema::<GeneratorDetail>(&mut schemas, "GeneratorDetail");
    insert_schema::<StartRun>(&mut schemas, "StartRun");
    insert_schema::<ForkStatePatch>(&mut schemas, "ForkStatePatch");
    insert_schema::<ForkRun>(&mut schemas, "ForkRun");
    insert_schema::<RunSnapshot>(&mut schemas, "RunSnapshot");
    insert_schema::<RunListItem>(&mut schemas, "RunListItem");
    insert_schema::<RunListResponse>(&mut schemas, "RunListResponse");
    insert_schema::<qcg_types::FileValue>(&mut schemas, "FileValue");
    insert_schema::<RunEvent>(&mut schemas, "RunEvent");
    insert_schema::<AnswerPayload>(&mut schemas, "AnswerPayload");
    insert_schema::<ConfirmDecision>(&mut schemas, "ConfirmDecision");
    insert_schema::<McpServerSummary>(&mut schemas, "McpServerSummary");
    insert_schema::<McpServerList>(&mut schemas, "McpServerList");
    insert_schema::<McpAuthorizationStart>(&mut schemas, "McpAuthorizationStart");
    insert_schema::<ProblemDetails>(&mut schemas, "ProblemDetails");
    insert_schema::<OutputManifest>(&mut schemas, "OutputManifest");
    json!({
        "schemas": schemas,
        "securitySchemes": {
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer"
            }
        }
    })
}

pub fn openapi_document(version: &str) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": { "title": "qcg", "version": version },
        "security": [{}, { "bearerAuth": [] }],
        "paths": openapi_paths(),
        "components": openapi_components()
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResponseSchema {
    Ref(&'static str),
    ArrayRef(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseBody {
    Json(Option<ResponseSchema>),
    Binary(&'static str),
    Text(&'static str),
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiHeader {
    pub name: &'static str,
    pub description: &'static str,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParameterSchema {
    String,
    DateTime,
    Integer {
        minimum: Option<u64>,
        maximum: Option<u64>,
        default: Option<u64>,
    },
    Ref(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiParameter {
    pub name: &'static str,
    pub required: bool,
    pub schema: ParameterSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiResponse {
    pub status: u16,
    pub description: &'static str,
    pub body: ResponseBody,
    pub headers: &'static [ApiHeader],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiRoute {
    pub method: &'static str,
    pub path: &'static str,
    pub summary: &'static str,
    pub response: ApiResponse,
    pub additional_responses: &'static [ApiResponse],
    pub request_schema: Option<&'static str>,
    pub request_headers: &'static [ApiHeader],
    pub query_parameters: &'static [ApiParameter],
    pub errors: &'static [u16],
}

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
const RUN_LIST_QUERY_PARAMETERS: &[ApiParameter] = &[
    ApiParameter {
        name: "limit",
        required: false,
        schema: ParameterSchema::Integer {
            minimum: Some(1),
            maximum: Some(200),
            default: Some(50),
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
        request_headers: NO_HEADERS,
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
        request_headers: NO_HEADERS,
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
        request_headers: NO_HEADERS,
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
        request_headers: NO_HEADERS,
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
];

pub fn openapi_route_paths() -> Vec<&'static str> {
    let mut paths = API_ROUTES
        .iter()
        .map(|route| route.path)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
}

fn openapi_paths() -> Value {
    let mut paths = serde_json::Map::new();
    for route in API_ROUTES {
        let entry = paths
            .entry(route.path.to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
        let object = entry.as_object_mut().expect("path item must be object");
        object.insert(route.method.into(), route_operation(route));
    }
    Value::Object(paths)
}

fn route_operation(route: &ApiRoute) -> Value {
    let mut operation = serde_json::Map::new();
    operation.insert("summary".into(), Value::String(route.summary.into()));
    let params = path_params_for(route.path);
    let mut params = params;
    params.extend(route.query_parameters.iter().map(|parameter| {
        parameter_json(
            parameter.name,
            "query",
            parameter.required,
            parameter.schema,
            None,
        )
    }));
    params.extend(route.request_headers.iter().map(parameter_json_header));
    if !params.is_empty() {
        operation.insert("parameters".into(), Value::Array(params));
    }
    if let Some(schema) = route.request_schema {
        operation.insert(
            "requestBody".into(),
            json_body(&format!("#/components/schemas/{schema}")),
        );
    }
    let mut responses = serde_json::Map::new();
    responses.insert(route.response.status.to_string(), response(&route.response));
    for additional in route.additional_responses {
        responses.insert(additional.status.to_string(), response(additional));
    }
    for status in route.errors {
        responses.insert(
            status.to_string(),
            json!({
                "description": error_description(*status),
                "content": {
                    "application/problem+json": {
                        "schema": { "$ref": "#/components/schemas/ProblemDetails" }
                    }
                }
            }),
        );
    }
    operation.insert("responses".into(), Value::Object(responses));
    Value::Object(operation)
}

fn error_description(status: u16) -> &'static str {
    match status {
        400 => "Invalid request",
        404 => "Resource not found",
        409 => "Resource conflict",
        413 => "Payload too large",
        422 => "Validation failed",
        500 => "Internal server error",
        503 => "Service unavailable",
        _ => "Request failed",
    }
}

fn parameter_json(
    name: &str,
    location: &str,
    required: bool,
    schema: ParameterSchema,
    description: Option<&str>,
) -> Value {
    let mut value = json!({
        "name": name,
        "in": location,
        "required": required,
        "schema": parameter_schema(schema),
    });
    if let Some(description) = description {
        value["description"] = Value::String(description.into());
    }
    value
}

fn parameter_json_header(header: &ApiHeader) -> Value {
    parameter_json(
        header.name,
        "header",
        header.required,
        ParameterSchema::String,
        Some(header.description),
    )
}

fn parameter_schema(schema: ParameterSchema) -> Value {
    match schema {
        ParameterSchema::String => json!({ "type": "string" }),
        ParameterSchema::DateTime => json!({ "type": "string", "format": "date-time" }),
        ParameterSchema::Integer {
            minimum,
            maximum,
            default,
        } => {
            let mut schema = json!({ "type": "integer" });
            if let Some(minimum) = minimum {
                schema["minimum"] = json!(minimum);
            }
            if let Some(maximum) = maximum {
                schema["maximum"] = json!(maximum);
            }
            if let Some(default) = default {
                schema["default"] = json!(default);
            }
            schema
        }
        ParameterSchema::Ref(name) => json!({ "$ref": format!("#/components/schemas/{name}") }),
    }
}

fn response(spec: &ApiResponse) -> Value {
    let mut response = json!({ "description": spec.description });
    let body = match spec.body {
        ResponseBody::Json(schema) => {
            let schema = schema.map(response_schema);
            Some(("application/json", schema))
        }
        ResponseBody::Binary(media_type) => Some((
            media_type,
            Some(json!({ "type": "string", "format": "binary" })),
        )),
        ResponseBody::Text(media_type) => Some((media_type, Some(json!({ "type": "string" })))),
        ResponseBody::Empty => None,
    };
    if let Some((media_type, schema)) = body {
        let media = schema.map_or_else(|| json!({}), |schema| json!({ "schema": schema }));
        response["content"] = json!({ media_type: media });
    }
    if !spec.headers.is_empty() {
        let mut headers = serde_json::Map::new();
        for header in spec.headers {
            headers.insert(
                header.name.into(),
                json!({
                    "description": header.description,
                    "schema": { "type": "string" }
                }),
            );
        }
        response["headers"] = Value::Object(headers);
    }
    response
}

fn response_schema(schema: ResponseSchema) -> Value {
    match schema {
        ResponseSchema::Ref(name) => json!({ "$ref": format!("#/components/schemas/{name}") }),
        ResponseSchema::ArrayRef(name) => {
            json!({ "type": "array", "items": { "$ref": format!("#/components/schemas/{name}") } })
        }
    }
}

pub fn run_event_reference_markdown() -> String {
    let components = openapi_components();
    let mut markdown = String::from(
        "## RunEvent Reference\n\nGenerated from the OpenAPI `RunEvent` schema. Every event uses the required envelope fields `seq`, `ts`, `run_id`, `trace_id`, `span_id`, `kind`, and `data`; `path` is present for node-scoped events. Trace and span IDs use W3C-compatible hexadecimal widths. Unknown `kind` values are preserved with opaque `data`.\n\n| Event | Required `data` fields |\n|---|---|\n",
    );
    for (event, schema_name) in RUN_EVENT_DATA_SCHEMAS {
        let required = components["schemas"][schema_name]
            .get("required")
            .and_then(Value::as_array)
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|field| format!("`{field}`"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let required = if required.is_empty() {
            "none".to_string()
        } else {
            required.join(", ")
        };
        markdown.push_str("| `");
        markdown.push_str(event);
        markdown.push_str("` | ");
        markdown.push_str(&required);
        markdown.push_str(" |\n");
    }
    markdown
}

fn path_params_for(path: &str) -> Vec<Value> {
    let mut params = Vec::new();
    let mut remaining = path;
    while let Some(start) = remaining.find('{') {
        let after_start = &remaining[start + 1..];
        let Some(end) = after_start.find('}') else {
            break;
        };
        let name = &after_start[..end];
        if !name.is_empty() {
            params.push(parameter_json(
                name,
                "path",
                true,
                ParameterSchema::String,
                None,
            ));
        }
        remaining = &after_start[end + 1..];
    }
    params
}

fn json_body(schema_ref: &str) -> Value {
    json!({
        "required": true,
        "content": {
            "application/json": {
                "schema": { "$ref": schema_ref }
            }
        }
    })
}

fn schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("schema must serialize")
}

fn insert_schema<T: JsonSchema>(schemas: &mut serde_json::Map<String, Value>, name: &str) {
    let mut value = schema::<T>();
    promote_defs(&mut value, schemas);
    rewrite_local_defs(&mut value);
    schemas.insert(name.into(), value);
}

fn promote_defs(value: &mut Value, schemas: &mut serde_json::Map<String, Value>) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    let Some(defs) = object.remove("$defs") else {
        return;
    };
    let Some(defs) = defs.as_object() else {
        return;
    };
    for (name, mut schema) in defs.clone() {
        promote_defs(&mut schema, schemas);
        rewrite_local_defs(&mut schema);
        schemas.entry(name).or_insert(schema);
    }
}

fn rewrite_local_defs(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object
                .get("$ref")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                && let Some(name) = reference.strip_prefix("#/$defs/")
            {
                object.insert(
                    "$ref".into(),
                    Value::String(format!("#/components/schemas/{name}")),
                );
            }
            for value in object.values_mut() {
                rewrite_local_defs(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                rewrite_local_defs(value);
            }
        }
        _ => {}
    }
}

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
