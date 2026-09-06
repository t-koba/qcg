use schemars::JsonSchema;
use serde_json::{Value, json};

use super::openapi_types::{
    ApiHeader, ApiResponse, ApiRoute, ParameterSchema, ResponseBody, ResponseSchema,
    openapi_components,
};
use super::routes::API_ROUTES;
use crate::events::RUN_EVENT_DATA_SCHEMAS;

pub fn openapi_route_paths() -> Vec<&'static str> {
    let mut paths = API_ROUTES
        .iter()
        .map(|route| route.path)
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
}

pub(crate) fn openapi_paths() -> Value {
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

pub(crate) fn response(spec: &ApiResponse) -> Value {
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

pub(crate) fn schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("schema must serialize")
}

pub(crate) fn insert_schema<T: JsonSchema>(
    schemas: &mut serde_json::Map<String, Value>,
    name: &str,
) {
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
