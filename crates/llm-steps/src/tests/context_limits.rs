use super::*;
use async_trait::async_trait;
use contract::ContextOverflowPolicy;
use engine::{Engine, NodeOutcome, Progress, RunOptions, RunState};
use llm::{Capabilities, ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider};
use model::{BudgetExhaustion, FailureCode, NodePath};
use std::sync::atomic::{AtomicUsize, Ordering};

struct UnusedProvider(AtomicUsize);

struct TempRoot(std::path::PathBuf);

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[async_trait]
impl LlmProvider for UnusedProvider {
    fn id(&self) -> &str {
        "fake"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ChatResponse {
            content: vec![ChatContent::Text("should not run".into())],
            usage: Default::default(),
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

#[tokio::test]
async fn local_prompt_context_limits_reach_terminal_failures_without_dispatch() {
    for (unit, input, byte_limit, token_limit, overflow, used, allowed, prefix) in [
        (
            "explicit byte cap",
            "123456789012345678901234567890",
            20,
            None,
            ContextOverflowPolicy::Error,
            Some(30),
            20,
            "LLM context byte limit exceeded",
        ),
        (
            "token-derived byte cap",
            "123456789012345678901234567890",
            100,
            Some(5),
            ContextOverflowPolicy::Error,
            Some(30),
            20,
            "LLM context byte limit exceeded",
        ),
        (
            "request error policy",
            "x",
            22,
            None,
            ContextOverflowPolicy::Error,
            None,
            0,
            "LLM request context byte limit exceeded",
        ),
        (
            "request bounded compaction",
            "123456789012345678901234567890",
            22,
            None,
            ContextOverflowPolicy::TruncateTail,
            None,
            0,
            "LLM request context byte limit exceeded and non-tool context cannot be compacted safely",
        ),
    ] {
        let used = used.unwrap_or_else(|| {
            let content = if unit == "request error policy" {
                input
            } else {
                "\n[CONTEXT_TRUNCATED]\n"
            };
            serde_json::to_vec(&[llm::ChatMessage::text("user", content)])
                .unwrap()
                .len()
        });
        let root = std::env::temp_dir().join(format!(
            "qcg-context-limits-{}-{}",
            std::process::id(),
            uuid_suffix()
        ));
        let _temp_root = TempRoot(root.clone());
        let _ = contract_with_llm_generate(&root, "fake");
        let manifest_path = root.join(contract::MANIFEST_FILE);
        let manifest = std::fs::read_to_string(&manifest_path).unwrap().replace(
            "[[flow]]",
            "[[inputs.stages]]\nid = \"basic\"\n[[inputs.stages.fields]]\nid = \"x\"\ntype = \"string\"\nrequired = true\n[[flow]]",
        );
        std::fs::write(manifest_path, manifest).unwrap();
        let mut contract =
            Contract::load(camino::Utf8PathBuf::from_path_buf(root.clone()).unwrap()).unwrap();
        let llm = contract.manifest.llm.as_mut().unwrap();
        llm.max_context_bytes = Some(byte_limit);
        llm.max_context_tokens = token_limit;
        llm.context_overflow = overflow;
        std::fs::write(root.join("prompt.j2"), "{{ inputs.x }}").unwrap();
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let provider = Arc::new(UnusedProvider(AtomicUsize::new(0)));
        let options = RunOptions {
            elapsed_budget_mode: Default::default(),
            output_dir: camino::Utf8PathBuf::from_path_buf(workspace).unwrap(),
            json_events: false,
            event_sender: None,
            interactive: false,
            answers: BTreeMap::new(),
            confirmations: BTreeMap::new(),
            max_total_steps: 100,
            max_parallel_steps: 1,
            parallel_compatibility: Default::default(),
            llm_provider: Some(provider.clone()),
            llm_seed_override: None,
            run_refs: BTreeMap::new(),
            cancellation: Default::default(),
            shutdown: None,
            scheduler_yield: Default::default(),
        };
        let mut registry = StepRegistry::new();
        register_llm_steps(&mut registry, Arc::new(LlmRuntime::builtins()));
        let progress = Engine::new(registry)
            .advance_with_id(
                "context-limit-test".into(),
                camino::Utf8PathBuf::from_path_buf(root.join("meta")).unwrap(),
                contract,
                BTreeMap::from([("x".into(), Value::String(input.into()))]),
                options,
            )
            .await;
        let Progress::Failed(failure) = progress else {
            panic!("{unit}: {progress:?}")
        };
        assert_eq!(failure.code, FailureCode::ExecutionFailed, "{failure:?}");
        assert!(failure.exhaustion.is_some(), "{failure:?}");
        assert_eq!(
            failure.exhaustion,
            Some(BudgetExhaustion {
                resource: "context_bytes".into(),
                used: used as u64,
                allowed: allowed as u64,
                used_subsec_nanoseconds: None,
            })
        );
        assert_eq!(
            failure.message,
            format!("step `gen` failed: {prefix}: {used} > {allowed}"),
            "{unit}"
        );
        assert_eq!(provider.0.load(Ordering::SeqCst), 0);
        let journal = camino::Utf8PathBuf::from_path_buf(root.join("meta/journal.jsonl")).unwrap();
        let state = RunState::fold_journal(&journal).unwrap();
        assert_eq!(state.budget.llm_calls, 0);
        let NodeOutcome::Failed { reason } = &state.nodes[&NodePath::root("gen")] else {
            panic!("expected failed generation")
        };
        assert_eq!(reason, &failure.detail);
        let events: Vec<Value> = std::fs::read_to_string(root.join("meta/journal.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let finished = events
            .iter()
            .find(|event| event["t"] == "step_finished")
            .unwrap();
        assert_eq!(
            finished["reason"],
            serde_json::to_value(&failure.detail).unwrap()
        );
        let terminal = events
            .iter()
            .find(|event| event["t"] == "run_finished")
            .unwrap();
        assert!(
            terminal["failures"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry["failure"] == serde_json::to_value(&failure.detail).unwrap()
                })
        );
        api::RunEvent::from_flat(finished).unwrap();
        api::RunEvent::from_flat(terminal).unwrap();
    }
}

#[test]
fn request_and_transcript_compaction_facts_use_the_serialized_message_domain() {
    let node = agent_node();
    for scope in ["request", "agent_transcript"] {
        let mut messages = vec![llm::ChatMessage::text("user", "excess content")];
        let original_bytes = serde_json::to_vec(&messages).unwrap().len() as u64;
        let error = crate::context::compact_agent_messages(
            &node,
            &mut messages,
            5,
            scope,
            &ContextOverflowPolicy::Error,
        )
        .unwrap_err();
        assert_context_facts(
            error,
            original_bytes,
            5,
            &format!("LLM {scope} context byte limit exceeded: {original_bytes} > 5"),
        );

        let mut messages = vec![llm::ChatMessage::text("user", "excess content")];
        let error = crate::context::compact_agent_messages(
            &node,
            &mut messages,
            0,
            scope,
            &ContextOverflowPolicy::TruncateTail,
        )
        .unwrap_err();
        let final_bytes =
            serde_json::to_vec(&[llm::ChatMessage::text("user", "\n[CONTEXT_TRUNCATED]\n")])
                .unwrap()
                .len() as u64;
        assert_context_facts(
            error,
            final_bytes,
            0,
            &format!(
                "LLM {scope} context byte limit exceeded and non-tool context cannot be compacted safely: {final_bytes} > 0"
            ),
        );
    }
}

fn assert_context_facts(error: engine::StepError, used: u64, allowed: u64, message: &str) {
    let engine::StepError::LocalResourceExhausted {
        resource,
        used: actual_used,
        allowed: actual_allowed,
        message: actual_message,
        ..
    } = error
    else {
        panic!("expected typed context exhaustion, got {error:?}")
    };
    assert_eq!(resource, "context_bytes");
    assert_eq!(actual_used, used);
    assert_eq!(actual_allowed, allowed);
    assert_eq!(actual_message, message);
}
