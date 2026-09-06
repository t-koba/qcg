mod check_contract;
mod check_format;
mod check_schema;

pub(crate) use check_contract::CheckContractStep;
pub(crate) use check_format::CheckFormatStep;
pub(crate) use check_schema::CheckSchemaStep;

#[cfg(test)]
mod tests {
    use super::super::common::{decode_base64_file_atomic, encode_base64_file_atomic};
    use super::*;
    use crate::common::test_helpers::*;
    use crate::{CopyStep, RenderStep, deterministic_registry};
    use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
    use camino::Utf8PathBuf;
    use qcg_contract::Contract;
    use qcg_engine::{StepExecutor, validate_json_schema_findings};
    use qcg_policy::MAX_JSON_SCHEMA_BYTES;
    use serde_json::json;

    #[test]
    fn package_backed_steps_validate_package_paths_before_execution() {
        let (contract, root) = test_contract("path");
        std::fs::create_dir_all(root.join("directory")).expect("package directory should exist");
        write_package_file(&root, "templates/good.j2", "Hello {{ inputs.name }}");
        write_package_file(&root, "source.txt", "source");
        write_package_file(&root, "schema.json", r#"{"type":"string"}"#);

        let render = package_node(
            "render",
            "render",
            "template = \"templates/good.j2\"\noutput_file = \"rendered.txt\"",
        );
        RenderStep
            .validate(&render, &contract)
            .expect("render template file should validate");
        let render_directory = package_node(
            "render-directory",
            "render",
            "template = \"directory\"\noutput_file = \"rendered.txt\"",
        );
        let error = RenderStep
            .validate(&render_directory, &contract)
            .expect_err("render directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let copy = package_node(
            "copy",
            "copy",
            "source = \"source.txt\"\ntarget = \"copied.txt\"",
        );
        CopyStep
            .validate(&copy, &contract)
            .expect("copy source file should validate");
        let copy_directory = package_node(
            "copy-directory",
            "copy",
            "source = \"directory\"\ntarget = \"copied\"",
        );
        let error = CopyStep
            .validate(&copy_directory, &contract)
            .expect_err("copy directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let check_schema = package_node(
            "check-schema",
            "check.schema",
            "source = \"value.json\"\nschema = \"schema.json\"",
        );
        CheckSchemaStep
            .validate(&check_schema, &contract)
            .expect("valid schema package file should validate");
        let check_schema_directory = package_node(
            "check-schema-directory",
            "check.schema",
            "source = \"value.json\"\nschema = \"directory\"",
        );
        let error = CheckSchemaStep
            .validate(&check_schema_directory, &contract)
            .expect_err("schema directory must fail validation");
        assert!(error.to_string().contains("must be a file"), "{error}");

        let missing = package_node(
            "missing",
            "copy",
            "source = \"missing.txt\"\ntarget = \"copied.txt\"",
        );
        let error = CopyStep
            .validate(&missing, &contract)
            .expect_err("missing package source must fail validation");
        assert!(
            error.to_string().contains("copy source package path"),
            "{error}"
        );
        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[test]
    fn check_schema_validation_parses_and_compiles_the_complete_schema() {
        let (contract, root) = test_contract("schema");
        write_package_file(&root, "value.json", "{}");
        let node = package_node(
            "check-schema",
            "check.schema",
            "source = \"value.json\"\nschema = \"schema.json\"",
        );

        write_package_file(&root, "schema.json", "not-json");
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("malformed schema JSON must fail validation");
        assert!(error.to_string().contains("not valid JSON"), "{error}");

        write_package_file(&root, "schema.json", r##"{"$ref":"#/missing"}"##);
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("unresolvable schema reference must fail validation");
        assert!(
            error.to_string().contains("invalid or unsafe JSON Schema"),
            "{error}"
        );

        std::fs::write(
            root.join("schema.json"),
            vec![b' '; MAX_JSON_SCHEMA_BYTES + 1],
        )
        .expect("oversized schema should be written");
        let error = CheckSchemaStep
            .validate(&node, &contract)
            .expect_err("oversized schema must fail validation");
        assert!(error.to_string().contains("byte limit"), "{error}");

        write_package_file(
            &root,
            "schema.json",
            r#"{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}"#,
        );
        CheckSchemaStep
            .validate(&node, &contract)
            .expect("complete valid schema should compile during validation");
        std::fs::remove_dir_all(root).expect("temporary package should be removed");
    }

    #[test]
    fn schema_validator_reports_missing_required_property() {
        let schema = json!({
            "type": "object",
            "required": ["name"],
            "properties": { "name": { "type": "string" } }
        });
        let value = json!({});
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn schema_validator_recurses_into_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "count": { "type": "integer" }
            }
        });
        let value = json!({ "count": "three" });
        let findings = validate_json_schema_findings(&schema, &value, "$");
        assert_eq!(findings[0].location.as_deref(), Some("$.count"));
    }

    #[test]
    fn contract_validation_reports_misspelled_param_with_line_number() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-param-line-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("generator directory should be created");
        std::fs::write(
            root.join("qcg.toml"),
            r#"[generator]
id = "typo"
version = "0.1.0"
qcg_version = "^0.1"

[[flow]]
id = "emit"
type = "write"
[flow.params]
tempalte = "x"
output_file = "out.txt"
"#,
        )
        .expect("manifest should be written");
        let contract = Contract::load(&root).expect("common contract fields should load");
        let error = deterministic_registry()
            .validate_contract(&contract)
            .expect_err("misspelled params must fail validation");
        assert!(error.to_string().contains("line 10"));
        assert!(error.to_string().contains("tempalte"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn base64_file_transforms_round_trip_and_preserve_target_on_failure() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-base64-file-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary directory should be created");
        let source = root.join("source.bin");
        let encoded = root.join("encoded.txt");
        let decoded = root.join("decoded.bin");
        let original = b"binary\0payload\xff";
        std::fs::write(&source, original).expect("source should be written");
        let source_bytes = encode_base64_file_atomic(&source, &encoded, Some(1024))
            .await
            .expect("base64 encoding should succeed");
        assert_eq!(source_bytes, original.len());
        let decoded_bytes = decode_base64_file_atomic(&encoded, &decoded, Some(1024), None)
            .await
            .expect("base64 decoding should succeed");
        assert_eq!(decoded_bytes, original.len());
        assert_eq!(
            std::fs::read(&decoded).expect("decoded file should be readable"),
            original
        );

        std::fs::write(&encoded, "not canonical*").expect("invalid source should be written");
        std::fs::write(&decoded, b"previous output").expect("existing target should be written");
        let error = decode_base64_file_atomic(&encoded, &decoded, Some(1024), None)
            .await
            .expect_err("invalid base64 must fail");
        assert!(error.contains("base64"));
        assert_eq!(
            std::fs::read(&decoded).expect("existing target should remain readable"),
            b"previous output"
        );
        assert!(
            std::fs::read_dir(&root)
                .expect("temporary directory should be readable")
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains("qcg-part-")),
            "failed transforms must not leave partial files"
        );

        let inplace = root.join("inplace");
        std::fs::write(&inplace, BASE64.encode(original))
            .expect("in-place source should be written");
        decode_base64_file_atomic(&inplace, &inplace, Some(1024), None)
            .await
            .expect("in-place base64 decode should succeed");
        assert_eq!(
            std::fs::read(&inplace).expect("in-place output should be readable"),
            original
        );
        encode_base64_file_atomic(&inplace, &inplace, Some(1024))
            .await
            .expect("in-place base64 encode should succeed");
        assert_eq!(
            std::fs::read_to_string(&inplace).expect("in-place encoded output should be readable"),
            BASE64.encode(original)
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn base64_decode_applies_requested_unix_mode_after_commit() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-base64-mode-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("temporary directory should be created");
        let source = root.join("source.txt");
        let target = root.join("script");
        std::fs::write(&source, "c2V0IC1l").expect("source should be written");
        decode_base64_file_atomic(&source, &target, Some(1024), Some(0o750))
            .await
            .expect("base64 decoding should succeed");
        assert_eq!(
            std::fs::metadata(&target)
                .expect("target metadata should be available")
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
