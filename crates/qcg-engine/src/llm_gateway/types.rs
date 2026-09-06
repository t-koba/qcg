use crate::{JournalWriter, SecretStore};
use qcg_contract::ModelRef;
use qcg_llm::LlmProvider;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) const LLM_STREAM_CHANNEL_CAPACITY: usize = 64;

pub struct LlmGateway<'a> {
    pub(crate) provider: Arc<dyn LlmProvider>,
    pub(crate) secrets: &'a SecretStore,
    pub(crate) journal: &'a JournalWriter,
    pub(crate) cancellation: CancellationToken,
    pub(crate) budget: qcg_policy::LlmCostBudget,
    pub(crate) pricing: Vec<ModelRef>,
}
