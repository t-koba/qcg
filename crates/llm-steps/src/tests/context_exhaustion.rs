use super::*;
use async_trait::async_trait;
use engine::{LlmGateway, StepContext, StepExecutor};
use llm::{
    Capabilities, ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider, TokenUsage,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct ExhaustedProvider {
    calls: AtomicUsize,
    tools: bool,
    delegated: bool,
}
#[async_trait]
impl LlmProvider for ExhaustedProvider {
    fn id(&self) -> &str {
        "fake"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, _: ChatRequest) -> Result<ChatResponse, LlmError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.delegated && index == 0 {
            return Ok(ChatResponse {
                content: vec![ChatContent::ToolCall {
                    id: "delegate".into(),
                    name: "researcher".into(),
                    args: json!({}),
                }],
                usage: TokenUsage::default(),
                stop: StopReason::ToolUse,
                provider_state: None,
                web_search: None,
            });
        }
        let content = if self.tools {
            vec![ChatContent::ToolCall {
                id: "t1".into(),
                name: "write".into(),
                args: json!({"path":"out.txt","content":"must not be written"}),
            }]
        } else {
            vec![ChatContent::Text("partial answer".into())]
        };
        Ok(ChatResponse {
            content,
            usage: TokenUsage {
                input: 10,
                output: 3,
                ..Default::default()
            },
            stop: StopReason::ModelContextWindowExceeded,
            provider_state: None,
            web_search: None,
        })
    }
}

#[tokio::test]
async fn provider_context_exhaustion_never_completes_outputs_dispatches_tools_or_retries_generation()
 {
    for kind in [
        "llm.agent",
        "llm.generate",
        "llm.fill",
        "llm.choose",
        "delegated",
    ] {
        for stream in [false, true] {
            let root = std::env::temp_dir().join(format!(
                "qcg-provider-context-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(root.join("workspace")).unwrap();
            std::fs::create_dir_all(root.join("meta")).unwrap();
            std::fs::write(root.join(contract::MANIFEST_FILE), "[generator]\nid = \"t\"\nname = \"t\"\nversion = \"0.1.0\"\n[llm]\nmodel = { provider = \"fake\", model = \"test\" }\nmax_tokens = 8\n[[flow]]\nid = \"agent\"\ntype = \"llm.generate\"\n[flow.params]\nprompt = \"prompt.txt\"\n").unwrap();
            std::fs::write(root.join("prompt.txt"), "request").unwrap();
            let contract =
                Contract::load(camino::Utf8PathBuf::from_path_buf(root.clone()).unwrap()).unwrap();
            let run = engine::RunContext::for_tests(
                "context-test".into(),
                camino::Utf8PathBuf::from_path_buf(root.join("workspace")).unwrap(),
                camino::Utf8PathBuf::from_path_buf(root.join("meta")).unwrap(),
                contract,
                BTreeMap::new(),
                BTreeMap::new(),
            )
            .unwrap();
            let journal = engine::JournalWriter::create(
                &camino::Utf8PathBuf::from_path_buf(root.join("journal.jsonl")).unwrap(),
                "context-test",
                false,
                None,
            )
            .unwrap();
            let provider = Arc::new(ExhaustedProvider {
                calls: AtomicUsize::new(0),
                tools: kind == "llm.agent" || kind == "delegated",
                delegated: kind == "delegated",
            });
            let gateway = LlmGateway::new(
                provider.clone(),
                &run.secrets,
                &journal,
                run.cancellation.clone(),
                policy::LlmCostBudget {
                    max_tokens: None,
                    max_cost_microusd: None,
                    require_pricing: false,
                },
                vec![],
            );
            let mut vars = contract::ValueBag::with_inputs(BTreeMap::new());
            let mut ctx = StepContext {
                run: &run,
                journal: &journal,
                vars: &mut vars,
                llm: Some(gateway),
            };
            let mut node = agent_node();
            node.kind = StepType::literal(if kind == "delegated" {
                "llm.agent"
            } else {
                kind
            });
            node.params = serde_json::from_value(
                json!({"prompt":"prompt.txt","max_iterations":3,"max_tokens_total":1000,
                "request":{"stream":stream},"output_file":"out.txt","options":["partial answer"],
                "tools":[{"kind":"fs.write","name":"write","path_prefix":""},
                {"kind":"agent","name":"researcher","instructions":"Research","tools":["write"],
                 "max_calls":3,"max_iterations":3,"max_tokens_total":1000,"max_tool_calls_total":3}]}),
            )
            .unwrap();
            let runtime = Arc::new(LlmRuntime::builtins());
            let executor: Box<dyn StepExecutor> = match kind {
                "llm.agent" | "delegated" => Box::new(LlmAgentStep { runtime }),
                "llm.generate" => Box::new(crate::generate::LlmGenerateStep { runtime }),
                "llm.fill" => Box::new(crate::generate::LlmFillStep { runtime }),
                _ => Box::new(crate::choose::LlmChooseStep { runtime }),
            };
            let error = executor.execute(&mut ctx, &node).await.unwrap_err();
            assert!(
                matches!(error, StepError::ProviderContextExhausted { .. }),
                "{kind}: {error:?}"
            );
            assert_eq!(
                provider.calls.load(Ordering::SeqCst),
                if kind == "delegated" { 2 } else { 1 },
                "{kind}"
            );
            assert_eq!(
                journal.state().budget.llm_calls,
                if kind == "delegated" { 2 } else { 1 }
            );
            assert_eq!(
                (
                    journal.state().budget.tokens_input,
                    journal.state().budget.tokens_output
                ),
                (10, 3)
            );
            assert!(!root.join("workspace/out.txt").exists());
            let text = std::fs::read_to_string(root.join("journal.jsonl")).unwrap();
            assert!(
                !text.lines().any(|line| {
                    let v: Value = serde_json::from_str(line).unwrap();
                    v["t"] == "tool_call_started" && v["name"] == "write"
                }),
                "partial tool must not dispatch"
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
