use serde_json::Value;

#[test]
fn schema_component_index_matches_snapshot() {
    let components = qcg_api::openapi_components();
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
    let document = qcg_api::openapi_document("test");
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
    let document = qcg_api::openapi_document("test");
    for route in qcg_api::API_ROUTES {
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
        assert_eq!(actual, route.errors, "{} {}", route.method, route.path);
    }
}

#[test]
fn openapi_documents_runtime_http_metadata() {
    let document = qcg_api::openapi_document("test");
    let paths = document
        .get("paths")
        .and_then(Value::as_object)
        .expect("openapi paths must be an object");

    let cancel = &paths["/api/runs/{id}:cancel"]["post"];
    let cancel_params = cancel["parameters"]
        .as_array()
        .expect("cancel must document its path parameter");
    assert_eq!(cancel_params.len(), 1);
    assert_eq!(cancel_params[0]["name"], "id");
    assert_eq!(cancel_params[0]["in"], "path");
    assert_eq!(cancel_params[0]["required"], true);

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
    let file_value = qcg_api::openapi_components()["schemas"]["FileValue"].clone();
    let one_of = file_value["oneOf"]
        .as_array()
        .expect("FileValue must declare exclusive content variants");
    assert_eq!(one_of.len(), 2);
    assert_eq!(file_value["required"], serde_json::json!(["name"]));
    assert!(file_value["properties"]["name"]["pattern"].is_string());
    assert_eq!(file_value["properties"]["text"]["type"], "string");
    assert_eq!(file_value["properties"]["content_base64"]["type"], "string");
}
