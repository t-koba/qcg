use crate::{ResultExt, StepError};
use qcg_contract::NodeDef;
use qcg_llm::{ChatContent, ChatContentPart, ChatRequest, ChatResponse};
use serde_json::Value;

use super::types::LlmGateway;

impl<'a> LlmGateway<'a> {
    pub fn scan_text(&self, node: &NodeDef, text: &str) -> Result<(), StepError> {
        self.assert_absent(node, text)
    }

    pub(crate) fn scan_request(
        &self,
        node: &NodeDef,
        request: &ChatRequest,
    ) -> Result<(), StepError> {
        if let Some(system) = &request.system {
            self.assert_absent(node, system)?;
        }
        for message in &request.messages {
            self.assert_absent(node, &message.content)?;
            for part in &message.parts {
                match part {
                    ChatContentPart::Text { text } => self.assert_absent(node, text)?,
                    ChatContentPart::InputImage { media_type, .. }
                    | ChatContentPart::InputAudio { media_type, .. }
                    | ChatContentPart::InputFile { media_type, .. } => {
                        self.assert_absent(node, media_type)?;
                    }
                }
                if let ChatContentPart::InputFile { filename, .. } = part {
                    self.assert_absent(node, filename)?;
                }
            }
            if let Some(id) = &message.tool_call_id {
                self.assert_absent(node, id)?;
            }
            for call in &message.tool_calls {
                self.assert_absent(node, &call.id)?;
                self.assert_absent(node, &call.name)?;
                self.assert_value_absent(node, &call.args)?;
            }
            if let Some(state) = &message.provider_state {
                self.assert_value_absent(node, &Value::Array(state.clone()))?;
            }
        }
        Ok(())
    }

    pub(crate) fn scan_response(
        &self,
        node: &NodeDef,
        response: &ChatResponse,
    ) -> Result<(), StepError> {
        for content in &response.content {
            match content {
                ChatContent::Text(text) => self.assert_absent(node, text)?,
                ChatContent::ToolCall { id, name, args } => {
                    self.assert_absent(node, id)?;
                    self.assert_absent(node, name)?;
                    self.assert_value_absent(node, args)?;
                }
            }
        }
        if let Some(state) = &response.provider_state {
            self.assert_value_absent(node, state)?;
        }
        Ok(())
    }

    fn assert_value_absent(&self, node: &NodeDef, value: &Value) -> Result<(), StepError> {
        match value {
            Value::String(value) => self.assert_absent(node, value),
            Value::Array(values) => {
                for value in values {
                    self.assert_value_absent(node, value)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                for (key, value) in values {
                    self.assert_absent(node, key)?;
                    self.assert_value_absent(node, value)?;
                }
                Ok(())
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => Ok(()),
        }
    }

    fn assert_absent(&self, node: &NodeDef, text: &str) -> Result<(), StepError> {
        self.secrets.assert_absent(text).step_err(&node.id)
    }
}
