use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OutputManifest {
    pub artifacts: Vec<OutputArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct OutputArtifact {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub label: String,
    pub required: bool,
    #[serde(default)]
    pub mime: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub preview: ArtifactPreview,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPreview {
    #[default]
    Auto,
    Text,
    Json,
    Markdown,
    Image,
    Html,
    Pdf,
    Audio,
    Video,
    None,
}
