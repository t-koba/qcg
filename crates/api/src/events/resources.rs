use schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceEventData {
    pub name: String,
    #[serde(rename = "type")]
    pub resource_type: String,
    pub source: ResourceSource,
    #[serde(default)]
    pub snapshot: Option<String>,
    pub sha256: String,
    pub bytes: usize,
    #[serde(default)]
    pub files: Vec<ResourceFileEventData>,
    pub cache: ResourceEventCacheStatus,
    #[serde(default)]
    pub pin_sha256: Option<String>,
    pub trust: String,
    pub llm_visible: bool,
    /// Lenient specification violations recorded while inspecting the
    /// resource (for example a skill name that differs from its directory).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceSource {
    Path {
        path: String,
    },
    Url {
        url: String,
        final_url: String,
    },
    Command {
        command: Vec<String>,
    },
    /// Snapshot copied from another run's declared artifact.
    RunRef {
        run_id: String,
        artifact: String,
    },
}

#[derive(Debug, Clone, Serialize, serde::Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceFileEventData {
    pub path: String,
    pub sha256: String,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, serde::Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceEventCacheStatus {
    NotApplicable,
    Local,
    Hit,
    Miss,
}
