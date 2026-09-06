use qcg_types::OutputManifest;
use serde_json::{Value, json};

use super::dto::{
    AnswerPayload, ConfirmDecision, ForkRun, ForkStatePatch, GeneratorDetail, GeneratorSummary,
    McpAuthorizationStart, McpServerList, McpServerSummary, ProblemDetails, RunCostMetrics,
    RunListItem, RunListResponse, RunSnapshot, StartRun,
};
use super::openapi_doc::{insert_schema, openapi_paths};
use crate::events::RunEvent;

pub fn openapi_components() -> Value {
    let mut schemas = serde_json::Map::new();
    insert_schema::<GeneratorSummary>(&mut schemas, "GeneratorSummary");
    insert_schema::<GeneratorDetail>(&mut schemas, "GeneratorDetail");
    insert_schema::<StartRun>(&mut schemas, "StartRun");
    insert_schema::<ForkStatePatch>(&mut schemas, "ForkStatePatch");
    insert_schema::<ForkRun>(&mut schemas, "ForkRun");
    insert_schema::<RunSnapshot>(&mut schemas, "RunSnapshot");
    insert_schema::<RunCostMetrics>(&mut schemas, "RunCostMetrics");
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
