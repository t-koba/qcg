use qcg_types::NodePath;
use schemars::JsonSchema;
use serde::Serialize;
use serde::{Deserialize, Deserializer, de::Error as DeError};
use serde_json::Value;

use super::completion::{LaggedAction, LaggedEventData};
use super::core::RunEventData;

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RunEvent {
    pub seq: u64,
    pub ts: String,
    pub run_id: String,
    pub trace_id: String,
    pub span_id: String,
    #[serde(default)]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub path: Option<NodePath>,
    pub kind: String,
    pub data: RunEventData,
}

impl<'de> Deserialize<'de> for RunEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawRunEvent {
            seq: u64,
            ts: String,
            run_id: String,
            trace_id: String,
            span_id: String,
            #[serde(default)]
            parent_span_id: Option<String>,
            #[serde(default)]
            path: Option<NodePath>,
            kind: String,
            data: Value,
        }

        let raw = RawRunEvent::deserialize(deserializer)?;
        let data = RunEventData::parse(&raw.kind, raw.data).map_err(D::Error::custom)?;
        Ok(Self {
            seq: raw.seq,
            ts: raw.ts,
            run_id: raw.run_id,
            trace_id: raw.trace_id,
            span_id: raw.span_id,
            parent_span_id: raw.parent_span_id,
            path: raw.path,
            kind: raw.kind,
            data,
        })
    }
}

impl RunEvent {
    pub fn from_flat(value: &Value) -> Result<Self, String> {
        let object = value
            .as_object()
            .ok_or_else(|| "run event must be an object".to_string())?;
        let seq = object
            .get("seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| "run event seq is required".to_string())?;
        let ts = object
            .get("ts")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event ts is required".to_string())?
            .to_string();
        let kind = object
            .get("t")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event kind is required".to_string())?
            .to_string();
        let run_id = object
            .get("run_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event run_id is required".to_string())?
            .to_string();
        let trace_id = object
            .get("trace_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event trace_id is required".to_string())?
            .to_string();
        let span_id = object
            .get("span_id")
            .and_then(Value::as_str)
            .ok_or_else(|| "run event span_id is required".to_string())?
            .to_string();
        let path = object
            .get("node")
            .and_then(Value::as_str)
            .map(NodePath::root);
        let mut data = object.clone();
        for common in [
            "seq",
            "ts",
            "t",
            "run_id",
            "trace_id",
            "span_id",
            "parent_span_id",
            "node",
        ] {
            data.remove(common);
        }
        let data = RunEventData::parse(&kind, Value::Object(data))?;
        Ok(Self {
            seq,
            ts,
            run_id,
            trace_id,
            span_id,
            parent_span_id: object
                .get("parent_span_id")
                .and_then(Value::as_str)
                .map(str::to_string),
            path,
            kind,
            data,
        })
    }

    pub fn lagged(run_id: impl Into<String>, seq: u64) -> Self {
        let run_id = run_id.into();
        Self {
            seq,
            ts: chrono::Utc::now().to_rfc3339(),
            trace_id: trace_id_for_run(&run_id),
            span_id: span_id_for_seq(seq),
            parent_span_id: None,
            run_id,
            path: None,
            kind: "lagged".into(),
            data: RunEventData::Lagged(LaggedEventData {
                action: LaggedAction::ResyncSnapshot,
            }),
        }
    }
}

pub fn trace_id_for_run(run_id: &str) -> String {
    let compact = run_id
        .bytes()
        .filter(|byte| byte.is_ascii_hexdigit())
        .map(|byte| (byte as char).to_ascii_lowercase())
        .collect::<String>();
    if compact.len() == 32 && compact.bytes().any(|byte| byte != b'0') {
        return compact;
    }
    let mut state = [0xcbf29ce484222325_u64, 0x84222325cbf29ce4_u64];
    for (index, byte) in run_id.bytes().enumerate() {
        let slot = index & 1;
        state[slot] ^= u64::from(byte);
        state[slot] = state[slot].wrapping_mul(0x100000001b3);
    }
    format!("{:016x}{:016x}", state[0], state[1])
}

pub fn span_id_for_seq(seq: u64) -> String {
    format!("{:016x}", seq.max(1))
}

pub fn span_id_for_scope(run_id: &str, scope: &str) -> String {
    let mut state = 0xcbf29ce484222325_u64;
    for byte in run_id.bytes().chain(*b":").chain(scope.bytes()) {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x100000001b3);
    }
    if state == 0 {
        state = 1;
    }
    format!("{state:016x}")
}
