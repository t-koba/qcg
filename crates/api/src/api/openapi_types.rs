use model::OutputManifest;
use serde_json::{Value, json};

use super::dto::{
    AnswerPayload, ConfirmDecision, ForkRun, ForkStatePatch, GeneratorDetail, GeneratorSummary,
    LlmCatalogCapabilities, LlmCatalogModel, LlmCatalogProvider, LlmCatalogResponse,
    LlmCatalogSource, McpAuthorizationStart, McpServerList, McpServerSummary, ProblemDetails,
    RunCostMetrics, RunListItem, RunListOrder, RunListResponse, RunSnapshot, StartRun,
};
use super::openapi_doc::{insert_schema, openapi_paths};
use crate::events::RunEvent;

/// Publishes the run-event kind vocabulary and relaxes request-body defaults.
///
/// `required` is the only source of requiredness in the document. A generator
/// infers "required" from a `default` annotation, which would force every SDK
/// caller to send `answers`, `priority`, and the rest of a request the server
/// already defaults, so the annotation is dropped for request bodies only.
fn normalize_request_schemas(schemas: &mut serde_json::Map<String, Value>) {
    for route in crate::api::routes::API_ROUTES {
        let Some(name) = route.request_schema else {
            continue;
        };
        let Some(Value::Object(schema)) = schemas.get_mut(name) else {
            continue;
        };
        let required = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<String>>()
            })
            .unwrap_or_default();
        if let Some(Value::Object(properties)) = schema.get_mut("properties") {
            for (property, definition) in properties.iter_mut() {
                if !required.iter().any(|entry| entry == property)
                    && let Value::Object(definition) = definition
                {
                    definition.remove("default");
                }
            }
        }
    }
}

/// Publishes `RunEvent.kind` as the closed set the events endpoint can deliver
/// so a generated client narrows an event by its kind instead of parsing `data`
/// blind. The Rust wire type stays open: `RunEvent.kind` remains a `String` and
/// an unknown kind from a newer server still decodes into
/// `RunEventData::Unknown`, so an older client keeps the raw payload.
fn narrow_event_kind(schemas: &mut serde_json::Map<String, Value>) {
    let Some(Value::Object(event)) = schemas.get_mut("RunEvent") else {
        return;
    };
    let Some(Value::Object(kind)) = event
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .and_then(|properties| properties.get_mut("kind"))
    else {
        return;
    };
    kind.insert("type".into(), Value::String("string".into()));
    kind.insert(
        "enum".into(),
        Value::Array(
            crate::events::all_run_event_kinds()
                .into_iter()
                .map(|kind| Value::from(kind.to_string()))
                .collect(),
        ),
    );
}

/// Schemas of the document. Exposed as a map for callers that build a
/// sub-document; the document itself nests it under `components.schemas`.
pub fn openapi_components() -> Value {
    let mut schemas = serde_json::Map::new();
    insert_schema::<GeneratorSummary>(&mut schemas, "GeneratorSummary");
    insert_schema::<GeneratorDetail>(&mut schemas, "GeneratorDetail");
    insert_schema::<LlmCatalogSource>(&mut schemas, "LlmCatalogSource");
    insert_schema::<LlmCatalogCapabilities>(&mut schemas, "LlmCatalogCapabilities");
    insert_schema::<LlmCatalogModel>(&mut schemas, "LlmCatalogModel");
    insert_schema::<LlmCatalogProvider>(&mut schemas, "LlmCatalogProvider");
    insert_schema::<LlmCatalogResponse>(&mut schemas, "LlmCatalogResponse");
    insert_schema::<StartRun>(&mut schemas, "StartRun");
    insert_schema::<ForkStatePatch>(&mut schemas, "ForkStatePatch");
    insert_schema::<ForkRun>(&mut schemas, "ForkRun");
    insert_schema::<RunSnapshot>(&mut schemas, "RunSnapshot");
    insert_schema::<RunCostMetrics>(&mut schemas, "RunCostMetrics");
    insert_schema::<RunListItem>(&mut schemas, "RunListItem");
    insert_schema::<RunListResponse>(&mut schemas, "RunListResponse");
    insert_schema::<RunListOrder>(&mut schemas, "RunListOrder");
    insert_schema::<model::FileValue>(&mut schemas, "FileValue");
    insert_schema::<RunEvent>(&mut schemas, "RunEvent");
    insert_schema::<AnswerPayload>(&mut schemas, "AnswerPayload");
    insert_schema::<ConfirmDecision>(&mut schemas, "ConfirmDecision");
    insert_schema::<McpServerSummary>(&mut schemas, "McpServerSummary");
    insert_schema::<McpServerList>(&mut schemas, "McpServerList");
    insert_schema::<McpAuthorizationStart>(&mut schemas, "McpAuthorizationStart");
    insert_schema::<ProblemDetails>(&mut schemas, "ProblemDetails");
    insert_schema::<OutputManifest>(&mut schemas, "OutputManifest");
    normalize_request_schemas(&mut schemas);
    narrow_event_kind(&mut schemas);
    json!({ "schemas": schemas })
}

pub fn openapi_document(version: &str) -> Value {
    let mut components = openapi_components();
    if let Value::Object(map) = &mut components {
        map.insert(
            "securitySchemes".into(),
            json!({ "bearerAuth": { "type": "http", "scheme": "bearer" } }),
        );
        map.insert(
            "responses".into(),
            crate::api::openapi_doc::shared_error_responses(),
        );
    }
    json!({
        "openapi": "3.1.0",
        "info": { "title": "api", "version": version },
        "security": [{}, { "bearerAuth": [] }],
        "paths": openapi_paths(),
        "components": components
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
    Boolean,
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
