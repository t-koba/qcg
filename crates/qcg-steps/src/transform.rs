use async_trait::async_trait;
use qcg_contract::{Contract, NodeDef};
use qcg_engine::{StepContext, StepError, StepExecutor, StepOutcome, StepTraits};
use qcg_policy::{params_schema, string_array_schema, string_schema};
use serde::Deserialize;
use serde_json::{Value, json};

use super::common::{
    bounded_transform_text, decode_base64_file_atomic, encode_base64_file_atomic,
    merge_json_objects, parse_unix_mode, require, strip_null_values, validate_unix_mode_template,
    write_transform_output, write_zip_atomic,
};
pub(crate) struct TransformStep;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransformParams {
    transform: String,
    source: String,
    target: String,
    /// Second input file for `json_merge`; `source` wins on conflicting keys.
    #[serde(default)]
    with: Option<String>,
    #[serde(default)]
    secrets: Vec<String>,
    #[serde(default)]
    unix_mode: Option<String>,
    #[serde(default)]
    remove_source: bool,
}

#[async_trait]
impl StepExecutor for TransformStep {
    fn type_id(&self) -> &'static str {
        "transform"
    }

    fn params_schema(&self) -> Option<Value> {
        Some(params_schema(
            &["transform", "source", "target"],
            json!({
                "transform": {
                    "type": "string",
                    "enum": [
                        "inject_secrets",
                        "json_pretty",
                        "json_compact",
                        "toml_to_json",
                        "json_to_toml",
                        "json_merge",
                        "base64_decode",
                        "base64_encode",
                        "zip"
                    ]
                },
                "source": string_schema(),
                "target": string_schema(),
                "with": string_schema(),
                "secrets": string_array_schema(),
                "unix_mode": { "type": "string", "pattern": "^0[6-7][0-7]{2}$" },
                "remove_source": { "type": "boolean" },
            }),
        ))
    }

    fn traits(&self) -> StepTraits {
        StepTraits::parallel()
    }

    fn validate(&self, node: &NodeDef, contract: &Contract) -> Result<(), StepError> {
        let params = transform_params(node)?;
        if params.transform == "json_merge" && params.with.is_none() {
            return Err(StepError::failed(
                &node.id,
                "json_merge requires a `with` input file",
            ));
        }
        validate_unix_mode_template(node, params.unix_mode.as_deref())?;
        if params.unix_mode.is_some() && params.transform != "base64_decode" {
            return Err(StepError::failed(
                &node.id,
                "transform unix_mode is only valid for base64_decode",
            ));
        }
        if params.transform == "inject_secrets" {
            if params.secrets.is_empty() {
                return Err(StepError::failed(
                    &node.id,
                    "inject_secrets requires a non-empty secrets declaration",
                ));
            }
            for secret in &params.secrets {
                if !contract.manifest.secrets.contains_key(secret) {
                    return Err(StepError::failed(
                        &node.id,
                        format!("inject_secrets references unknown secret `{secret}`"),
                    ));
                }
            }
        } else if !params.secrets.is_empty() {
            return Err(StepError::failed(
                &node.id,
                "secrets is only valid for inject_secrets",
            ));
        }
        if params.remove_source && params.transform != "base64_decode" {
            return Err(StepError::failed(
                &node.id,
                "transform remove_source is only valid for base64_decode",
            ));
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut StepContext<'_>,
        node: &NodeDef,
    ) -> Result<StepOutcome, StepError> {
        let params = transform_params(node)?;
        let transform = params.transform.as_str();
        let source = ctx.render_inline(node, &params.source)?;
        let target = ctx.render_inline(node, &params.target)?;
        let rendered_unix_mode = params
            .unix_mode
            .as_deref()
            .map(|mode| ctx.render_inline(node, mode))
            .transpose()?;
        let source_path = ctx.run.fs.resolve_read(&source).map_err(|error| {
            StepError::failed(
                &node.id,
                format!("source path is not in workspace: {error}"),
            )
        })?;
        let target_path = ctx
            .run
            .fs
            .resolve_write(&target)
            .map_err(|error| StepError::failed(&node.id, error.to_string()))?;
        let transform_limit = ctx.run.contract.manifest.runtime.file_input_limit_bytes;
        let output_limit = ctx.run.contract.manifest.runtime.output_file_limit_bytes;
        let mut value_output = None;
        match transform {
            "inject_secrets" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let injected = ctx
                    .run
                    .secrets
                    .inject_declared_placeholders(&text, &params.secrets)
                    .map_err(|error| StepError::failed(&node.id, error))?;
                write_transform_output(&node.id, &target_path, injected.as_bytes(), output_limit)
                    .await?;
            }
            "json_pretty" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: Value = serde_json::from_str(&text)?;
                let rendered = serde_json::to_string_pretty(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "json_compact" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: Value = serde_json::from_str(&text)?;
                let rendered = serde_json::to_string(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "toml_to_json" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let value: toml::Value = toml::from_str(&text).map_err(|error| {
                    StepError::failed(&node.id, format!("invalid TOML: {error}"))
                })?;
                let value = serde_json::to_value(value)?;
                let rendered = serde_json::to_string_pretty(&value)? + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(value);
            }
            "json_merge" => {
                let with_path = ctx
                    .run
                    .fs
                    .resolve_read(
                        ctx.render_inline(node, params.with.as_deref().expect("validated"))?
                            .as_str(),
                    )
                    .map_err(|error| {
                        StepError::failed(
                            &node.id,
                            format!("merge base is not in workspace: {error}"),
                        )
                    })?;
                let base_text = bounded_transform_text(&with_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let overlay_text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let overlay: Value = serde_json::from_str(&overlay_text)?;
                let mut base: Value = serde_json::from_str(&base_text)?;
                merge_json_objects(&mut base, &overlay);
                let rendered = serde_json::to_string_pretty(&base)?;
                let rendered = rendered + "\n";
                write_transform_output(&node.id, &target_path, rendered.as_bytes(), output_limit)
                    .await?;
                value_output = Some(base);
            }
            "json_to_toml" => {
                let text = bounded_transform_text(&source_path, transform_limit)
                    .await
                    .map_err(|error| StepError::failed(&node.id, error))?;
                let mut value: Value = serde_json::from_str(&text)?;
                strip_null_values(&mut value);
                let value: toml::Value = toml::Value::try_from(value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to convert JSON to TOML: {error}"))
                })?;
                let text = toml::to_string_pretty(&value).map_err(|error| {
                    StepError::failed(&node.id, format!("failed to encode TOML: {error}"))
                })?;
                write_transform_output(&node.id, &target_path, text.as_bytes(), output_limit)
                    .await?;
            }
            "base64_decode" => {
                let unix_mode = parse_unix_mode(node, rendered_unix_mode.as_deref())?;
                if params.remove_source && source_path == target_path {
                    return Err(StepError::failed(
                        &node.id,
                        "base64_decode remove_source cannot target the source file",
                    ));
                }
                let decoded_bytes = decode_base64_file_atomic(
                    &source_path,
                    &target_path,
                    transform_limit,
                    unix_mode,
                )
                .await
                .map_err(|error| StepError::failed(&node.id, error))?;
                if params.remove_source {
                    tokio::fs::remove_file(&source_path).await?;
                }
                value_output = Some(json!({ "bytes": decoded_bytes, "encoding": "binary" }));
            }
            "base64_encode" => {
                let source_bytes =
                    encode_base64_file_atomic(&source_path, &target_path, transform_limit)
                        .await
                        .map_err(|error| StepError::failed(&node.id, error))?;
                value_output = Some(json!({ "bytes": source_bytes, "encoding": "base64" }));
            }
            "zip" => {
                write_zip_atomic(
                    &node.id,
                    &source_path,
                    &target_path,
                    transform_limit,
                    ctx.run.contract.manifest.runtime.file_count_limit,
                )
                .await?
            }
            other => {
                return Err(StepError::failed(
                    &node.id,
                    format!("unsupported transform `{other}`"),
                ));
            }
        }
        Ok(StepOutcome::Success {
            output: Some(json!({ "file": target, "transform": transform, "value": value_output })),
            files: vec![target_path],
        })
    }
}

fn transform_params(node: &NodeDef) -> Result<TransformParams, StepError> {
    let params: TransformParams = node.deserialize_params().map_err(|error| {
        StepError::failed(&node.id, format!("invalid transform params: {error}"))
    })?;
    require(node, Some(&params.transform), "transform")?;
    require(node, Some(&params.source), "source")?;
    require(node, Some(&params.target), "target")?;
    if !matches!(
        params.transform.as_str(),
        "inject_secrets"
            | "json_pretty"
            | "json_compact"
            | "toml_to_json"
            | "json_to_toml"
            | "json_merge"
            | "base64_decode"
            | "base64_encode"
            | "zip"
    ) {
        return Err(StepError::failed(
            &node.id,
            format!("unsupported transform `{}`", params.transform),
        ));
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use crate::common::write_zip;
    use camino::Utf8PathBuf;

    #[test]
    fn zip_transform_preserves_directory_entries_metadata_and_contents() {
        let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .expect("temporary directory path must be UTF-8")
            .join(format!("qcg-zip-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let source = root.join("source");
        std::fs::create_dir_all(source.join("empty")).expect("empty directory should be created");
        std::fs::create_dir_all(source.join("nested")).expect("nested directory should be created");
        std::fs::write(source.join("nested/file.txt"), "archive content")
            .expect("source file should be written");
        let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200);
        std::fs::File::options()
            .write(true)
            .open(source.join("nested/file.txt"))
            .expect("source file should open")
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .expect("source modification time should be set");
        let target = root.join("result.zip");

        write_zip("archive", &source, &target, Some(1024 * 1024), Some(100))
            .expect("zip should be written");
        let file = std::fs::File::open(&target).expect("zip should open");
        let mut archive = zip::ZipArchive::new(file).expect("zip should parse");
        assert!(
            archive
                .by_name("empty/")
                .expect("empty directory entry")
                .is_dir()
        );
        assert!(
            archive
                .by_name("nested/")
                .expect("nested directory entry")
                .is_dir()
        );
        let mut entry = archive.by_name("nested/file.txt").expect("file entry");
        let archived_modified = entry.last_modified().expect("file timestamp");
        assert_eq!(archived_modified.year(), 2024);
        assert_eq!(archived_modified.month(), 1);
        assert_eq!(archived_modified.day(), 1);
        let mut content = String::new();
        std::io::Read::read_to_string(&mut entry, &mut content)
            .expect("file entry should be readable");
        assert_eq!(content, "archive content");
        std::fs::remove_dir_all(root).expect("temporary directory should be removed");
    }
}
