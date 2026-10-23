use serde_json::Value;

#[test]
fn schema_component_index_matches_snapshot() {
    let components = api::openapi_components();
    let mut names = components
        .pointer("/schemas")
        .and_then(Value::as_object)
        .expect("openapi components must contain schemas")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    let actual = serde_json::to_string_pretty(&names).expect("snapshot should serialize");
    let expected = include_str!("schema_components.snapshot.json").trim();
    assert_eq!(actual, expected);
}

#[test]
fn openapi_path_index_matches_snapshot() {
    let document = api::openapi_document("test");
    let mut paths = document
        .pointer("/paths")
        .and_then(Value::as_object)
        .expect("openapi document must contain paths")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    paths.sort();
    let actual = serde_json::to_string_pretty(&paths).expect("snapshot should serialize");
    let expected = include_str!("openapi_paths.snapshot.json").trim();
    assert_eq!(actual, expected);
}

#[test]
fn every_route_documents_exact_declared_errors() {
    let document = api::openapi_document("test");
    for route in api::API_ROUTES {
        let pointer = format!(
            "/paths/{}/{}/responses",
            route.path.replace('~', "~0").replace('/', "~1"),
            route.method
        );
        let responses = document
            .pointer(&pointer)
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("responses must exist for {} {}", route.method, route.path));
        let mut actual = responses
            .keys()
            .filter_map(|status| status.parse::<u16>().ok())
            .filter(|status| *status >= 400)
            .collect::<Vec<_>>();
        actual.sort_unstable();
        // The handler's own failures plus the statuses every route inherits
        // from the bearer-auth middleware and the rate limiter, taken from the
        // same function the document is generated from.
        let mut expected = route.errors.to_vec();
        expected.extend(api::middleware_error_statuses(route.path));
        expected.sort_unstable();
        assert_eq!(actual, expected, "{} {}", route.method, route.path);
    }
}

#[test]
fn middleware_failures_are_documented_once_and_referenced() {
    // The server answers 401 and 429 in front of every route, so the document
    // must say so; each response body is defined once under `components` and
    // referenced per route, and the 429 exemption matches the server.
    let document = api::openapi_document("test");
    let shared = &document["components"]["responses"];
    assert!(shared["Unauthorized"]["headers"]["WWW-Authenticate"].is_object());
    assert!(shared["TooManyRequests"]["headers"]["Retry-After"].is_object());
    for (path, item) in document["paths"].as_object().expect("paths") {
        for (method, operation) in item.as_object().expect("path item") {
            if !operation.is_object() {
                continue;
            }
            let responses = &operation["responses"];
            let reference = |status: &str, name: &str| {
                responses[status]["$ref"] == Value::String(format!("#/components/responses/{name}"))
            };
            assert!(
                reference("401", "Unauthorized"),
                "{method} {path} must reference Unauthorized"
            );
            assert_eq!(
                responses.get("429").is_some(),
                path != api::RATE_LIMIT_EXEMPT_PATH,
                "{method} {path} rate limiting must match the server exemption"
            );
            if path != api::RATE_LIMIT_EXEMPT_PATH {
                assert!(
                    reference("429", "TooManyRequests"),
                    "{method} {path} must reference TooManyRequests"
                );
            }
        }
    }
}

#[test]
fn run_event_kind_is_published_as_the_full_stream_vocabulary() {
    // A client narrows an event by its kind, so the document must list every
    // kind the events endpoint can deliver: the public registry, the internal
    // journal records, and the transport markers.
    let document = api::openapi_document("test");
    let kind = &document["components"]["schemas"]["RunEvent"]["properties"]["kind"];
    let published: Vec<&str> = kind["enum"]
        .as_array()
        .expect("RunEvent.kind must be an enum")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let expected = api::all_run_event_kinds();
    assert_eq!(
        published.len(),
        expected.len(),
        "every kind must be published"
    );
    for name in &expected {
        assert!(
            published.contains(name),
            "`{name}` reaches clients but is missing from the document"
        );
    }
    // The vocabulary is owned here, so the engine fold gate and the document
    // cannot drift, and a transport marker is not a journal record.
    assert!(api::INTERNAL_RUN_EVENT_KINDS.contains(&"step_interrupted"));
    assert!(!api::is_known_run_event_kind("step_interrupted"));
    assert!(api::TRANSPORT_RUN_EVENT_KINDS.contains(&api::STREAM_ERROR_KIND));
    assert!(!api::INTERNAL_RUN_EVENT_KINDS.contains(&api::STREAM_ERROR_KIND));
}

#[test]
fn request_body_defaults_stay_optional_for_generated_clients() {
    // A generator infers "required" from a `default` annotation, which would
    // force every SDK caller to send `answers`, `priority`, and the rest. The
    // document therefore carries `required` as the only source of requiredness
    // for request bodies, while response schemas keep their defaults.
    let components = api::openapi_components();
    for route in api::API_ROUTES {
        let Some(name) = route.request_schema else {
            continue;
        };
        let schema = &components["schemas"][name];
        let required = schema["required"]
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for (property, definition) in schema["properties"].as_object().expect("properties") {
            assert!(
                required.iter().any(|entry| entry == property)
                    || definition.get("default").is_none(),
                "request schema `{name}` must not mark optional property `{property}` required through a default"
            );
        }
    }
    assert!(
        components["schemas"]["RunSnapshot"]["properties"]["queue_position"]
            .get("default")
            .is_some(),
        "response schemas must keep their defaults"
    );
}

#[test]
fn start_request_rejects_unknown_controls_but_keeps_free_form_maps_open() {
    let error = serde_json::from_value::<api::StartRun>(serde_json::json!({
        "generator_id": "example",
        "inputs": {},
        "prioritty": "must-not-be-echoed"
    }))
    .expect_err("unknown top-level controls must fail before admission");
    let message = error.to_string();
    assert!(message.contains("prioritty"));
    assert!(!message.contains("must-not-be-echoed"));

    let request: api::StartRun = serde_json::from_value(serde_json::json!({
        "generator_id": "example",
        "inputs": {"future_input": {"nested": true}},
        "answers": {"future_question": {"answer": 1}},
        "confirmations": {"future_confirmation": true},
        "labels": {"owner": "team"}
    }))
    .expect("declared free-form maps keep their established semantics");
    assert_eq!(request.inputs["future_input"]["nested"], true);
    assert_eq!(request.answers["future_question"]["answer"], 1);

    let schema = &api::openapi_components()["schemas"]["StartRun"];
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(
        schema["properties"]["inputs"]["additionalProperties"], true,
        "input names and JSON payloads remain free-form"
    );
    assert_eq!(
        schema["properties"]["answers"]["additionalProperties"], true,
        "answer keys and JSON payloads remain free-form"
    );
}

#[test]
fn openapi_documents_runtime_http_metadata() {
    let document = api::openapi_document("test");
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths must be an object");

    let cancel = &paths["/api/runs/{id}:cancel"]["post"];
    let cancel_params = cancel["parameters"]
        .as_array()
        .expect("cancel must document its path parameter");
    assert_eq!(cancel_params.len(), 2);
    assert_eq!(cancel_params[0]["name"], "id");
    assert_eq!(cancel_params[0]["in"], "path");
    assert_eq!(cancel_params[0]["required"], true);
    assert!(cancel_params.iter().any(|parameter| {
        parameter["name"] == "Idempotency-Key"
            && parameter["in"] == "header"
            && parameter["required"] == false
    }));

    let start = &paths["/api/runs"]["post"];
    let start_params = start["parameters"]
        .as_array()
        .expect("start run must document request headers");
    assert!(start_params.iter().any(|parameter| {
        parameter["name"] == "Idempotency-Key"
            && parameter["in"] == "header"
            && parameter["required"] == false
    }));
    assert_eq!(
        start["responses"]["201"]["headers"]["Location"]["schema"]["type"],
        "string"
    );

    let events = &paths["/api/runs/{id}/events"]["get"];
    let event_params = events["parameters"]
        .as_array()
        .expect("events must document path and request headers");
    assert!(event_params.iter().any(|parameter| {
        parameter["name"] == "Last-Event-ID"
            && parameter["in"] == "header"
            && parameter["required"] == false
    }));
    assert_eq!(
        events["responses"]["200"]["content"]["text/event-stream"]["schema"]["type"],
        "string"
    );

    for path in [
        "/api/generators/{id}",
        "/api/runs/{id}",
        "/api/runs/{id}/artifacts",
    ] {
        assert_eq!(
            paths[path]["get"]["responses"]["304"]["description"],
            "Not modified"
        );
        assert_eq!(
            paths[path]["get"]["responses"]["200"]["headers"]["ETag"]["schema"]["type"],
            "string"
        );
        assert!(
            paths[path]["get"]["parameters"]
                .as_array()
                .expect("conditional GET must document request headers")
                .iter()
                .any(|parameter| {
                    parameter["name"] == "If-None-Match"
                        && parameter["in"] == "header"
                        && parameter["required"] == false
                })
        );
    }

    assert_eq!(
        paths["/api/runs/{id}/artifacts.zip"]["get"]["responses"]["200"]["content"]["application/zip"]
            ["schema"]["format"],
        "binary"
    );
    assert_eq!(
        paths["/api/runs/{id}/journal"]["get"]["responses"]["200"]["content"]["application/x-ndjson"]
            ["schema"]["type"],
        "string"
    );
    assert_eq!(
        paths["/api/runs/{id}/artifacts/{path}"]["get"]["responses"]["200"]["content"]["application/octet-stream"]
            ["schema"]["format"],
        "binary"
    );
}

#[test]
fn openapi_file_value_requires_exclusive_content_and_safe_name() {
    let file_value = api::openapi_components()["schemas"]["FileValue"].clone();
    let one_of = file_value["oneOf"]
        .as_array()
        .expect("FileValue must declare exclusive content variants");
    assert_eq!(one_of.len(), 2);
    assert_eq!(file_value["required"], serde_json::json!(["name"]));
    assert!(file_value["properties"]["name"]["pattern"].is_string());
    assert_eq!(file_value["properties"]["text"]["type"], "string");
    assert_eq!(file_value["properties"]["content_base64"]["type"], "string");
}

#[test]
fn run_snapshot_step_summaries_schema_is_typed_and_required() {
    let components = api::openapi_components();
    let snapshot = &components["schemas"]["RunSnapshot"];
    assert!(
        snapshot["required"]
            .as_array()
            .unwrap()
            .contains(&Value::String("step_summaries".into()))
    );
    assert_eq!(
        snapshot["properties"]["step_summaries"]["additionalProperties"]["$ref"],
        "#/components/schemas/StepSummary"
    );
    let status = &components["schemas"]["StepSummaryStatus"]["enum"];
    assert_eq!(status, &serde_json::json!(["success", "failed", "skipped"]));
    assert_eq!(
        components["schemas"]["StepSummary"]["properties"]["reason"]["anyOf"][1]["type"],
        "null"
    );
}

#[test]
fn elapsed_event_schema_is_required_and_nullable_and_wire_values_validate() {
    let schema = &api::openapi_components()["schemas"]["ElapsedAccountedEventData"];
    let required = schema["required"].as_array().expect("required properties");
    assert!(required.iter().any(|name| name == "active_started_at"));
    let active_start = &schema["properties"]["active_started_at"];
    let serialized_schema = active_start.to_string();
    assert!(
        serialized_schema.contains("null"),
        "active_started_at must admit explicit null: {active_start}"
    );
    assert!(
        serialized_schema.contains("string"),
        "active_started_at must admit an RFC3339 string: {active_start}"
    );

    for value in [
        serde_json::json!({"active_nanoseconds": 5, "active_started_at": null}),
        serde_json::json!({"active_nanoseconds": 5, "active_started_at": "2026-10-15T12:34:56.123Z"}),
    ] {
        serde_json::from_value::<api::ElapsedAccountedEventData>(value)
            .expect("null and RFC3339 timestamp are valid anchors");
    }
    for value in [
        serde_json::json!({"active_nanoseconds": 5}),
        serde_json::json!({"active_nanoseconds": 5, "active_started_at": "not-a-timestamp"}),
    ] {
        assert!(
            serde_json::from_value::<api::ElapsedAccountedEventData>(value).is_err(),
            "missing or malformed active anchor must be rejected"
        );
    }
}

#[test]
fn schema_two_run_identity_requires_elapsed_mode_in_openapi() {
    let schema = &api::openapi_components()["schemas"]["RunStartedEventData"];
    let required = schema["required"].as_array().expect("required properties");
    assert!(required.iter().any(|name| name == "elapsed_budget_mode"));
}

#[test]
fn generator_detail_and_start_document_the_package_identity_precondition() {
    let document = api::openapi_document("test");
    let detail = &document["components"]["schemas"]["GeneratorDetail"];
    assert_eq!(detail["properties"]["package_sha256"]["type"], "string");
    assert!(
        detail["required"]
            .as_array()
            .is_some_and(|required| { required.iter().any(|field| field == "package_sha256") })
    );

    let operation = &document["paths"]["/api/runs"]["post"];
    let package_header = operation["parameters"]
        .as_array()
        .expect("start request headers must be documented")
        .iter()
        .find(|parameter| parameter["name"] == "X-QCG-Package-SHA256")
        .expect("the package identity header must be documented");
    assert_eq!(package_header["in"], "header");
    assert!(package_header["required"].as_bool().unwrap_or(false));
    assert!(
        package_header["description"]
            .as_str()
            .is_some_and(|description| { description.contains("loaded GeneratorDetail") })
    );
}

#[test]
fn run_identity_parallel_compatibility_is_optional_and_roundtrips() {
    let old: api::RunStartedEventData = serde_json::from_value(serde_json::json!({
        "generator": "g@1",
        "generator_path": "g",
        "contract_sha256": "abc",
        "inputs": {},
        "schema_version": 2,
        "elapsed_budget_mode": "wall_clock"
    }))
    .expect("old identity without compat fields must parse");
    assert!(old.parallel_compatibility.is_none());
    assert!(old.parallel_compatibility_origin.is_none());
    let new: api::RunStartedEventData = serde_json::from_value(serde_json::json!({
        "generator": "g@1",
        "generator_path": "g",
        "contract_sha256": "abc",
        "inputs": {},
        "schema_version": 2,
        "elapsed_budget_mode": "wall_clock",
        "parallel_compatibility": "fail_fast",
        "parallel_compatibility_origin": "deployment_policy"
    }))
    .expect("new identity must parse");
    assert_eq!(
        serde_json::to_value(new.parallel_compatibility).unwrap(),
        serde_json::json!("fail_fast")
    );
    assert_eq!(
        new.parallel_compatibility_origin.as_deref(),
        Some("deployment_policy")
    );
    let schema = &api::openapi_components()["schemas"]["RunStartedEventData"];
    let required = schema["required"].as_array().expect("required");
    assert!(!required.iter().any(|name| name == "parallel_compatibility"));
    assert!(
        !required
            .iter()
            .any(|name| name == "parallel_compatibility_origin")
    );
}

#[test]
fn failure_exhaustion_is_optional_typed_and_amounts_are_unsigned() {
    let components = api::openapi_components();
    let schemas = &components["schemas"];
    let detail = &schemas["FailureDetail"];
    assert!(
        !detail["required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("exhaustion"))
    );
    assert_eq!(
        detail["properties"]["exhaustion"]["anyOf"][0]["$ref"],
        "#/components/schemas/BudgetExhaustion"
    );
    let facts = &schemas["BudgetExhaustion"];
    assert_eq!(facts["additionalProperties"], false);
    for amount in ["used", "allowed"] {
        assert_eq!(facts["properties"][amount]["type"], "integer");
        assert_eq!(facts["properties"][amount]["minimum"], 0);
        assert!(
            facts["required"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(amount))
        );
    }
    assert!(
        facts["required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("resource"))
    );
}
