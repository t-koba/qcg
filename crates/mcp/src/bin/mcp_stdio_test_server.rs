use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler, ServiceExt as _};
use serde_json::{Map, Value, json};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
struct TestServer {
    session_id: String,
    calls: Arc<AtomicU64>,
}

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let input_schema = json!({
            "type": "object",
            "properties": { "value": { "type": "string" } },
            "required": ["value"],
            "additionalProperties": false
        });
        let output_schema = json!({
            "type": "object",
            "properties": {
                "sessionId": { "type": "string" },
                "callCount": { "type": "integer" },
                "value": { "type": "string" }
            },
            "required": ["sessionId", "callCount", "value"],
            "additionalProperties": false
        });
        Ok(ListToolsResult {
            tools: vec![
                Tool::new(
                    "echo",
                    "Return the value and this process session identity.",
                    object(input_schema)?,
                )
                .with_raw_output_schema(Arc::new(object(output_schema)?)),
            ],
            ..Default::default()
        })
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        if request.name != "echo" {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        let value = request
            .arguments
            .as_ref()
            .and_then(|arguments| arguments.get("value"))
            .and_then(Value::as_str)
            .ok_or_else(|| ErrorData::invalid_params("value must be a string", None))?;
        let call_count = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(CallToolResult::structured(json!({
            "sessionId": self.session_id,
            "callCount": call_count,
            "value": value,
        }))
        .into())
    }
}

fn object(value: Value) -> Result<Map<String, Value>, ErrorData> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| ErrorData::internal_error("test schema must be an object", None))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session_id = parse_session_id()?;
    let service = TestServer {
        session_id,
        calls: Arc::new(AtomicU64::new(0)),
    }
    .serve(rmcp::transport::stdio())
    .await?;
    service.waiting().await?;
    Ok(())
}

fn parse_session_id() -> Result<String, Box<dyn Error>> {
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if argument == "--session-id" {
            let value = arguments.next().ok_or("--session-id requires a value")?;
            if value.is_empty() {
                return Err("--session-id must not be empty".into());
            }
            return Ok(value);
        }
    }
    Err("missing --session-id".into())
}
