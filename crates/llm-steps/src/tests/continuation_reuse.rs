use super::*;
use async_trait::async_trait;
use engine::{Engine, Progress, ReuseSource, RunOptions, RunState};
use llm::{
    Capabilities, ChatContent, ChatRequest, ChatResponse, LlmError, LlmProvider, TokenUsage,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Answers with its prompt, so equal prompts yield equal responses; while
/// `down`, any prompt naming STEP-C fails like an outage.
struct Echo {
    calls: AtomicUsize,
    down: AtomicBool,
}

#[async_trait]
impl LlmProvider for Echo {
    fn id(&self) -> &str {
        "fake"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    async fn complete(&self, request: ChatRequest) -> Result<ChatResponse, LlmError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let prompt = request
            .messages
            .last()
            .map(|m| m.content.clone())
            .unwrap_or_default();
        if self.down.load(Ordering::SeqCst) && prompt.contains("STEP-C") {
            return Err(LlmError::new("provider outage"));
        }
        Ok(ChatResponse {
            content: vec![ChatContent::Text(format!("R[{prompt}]"))],
            usage: TokenUsage::default(),
            stop: StopReason::EndTurn,
            provider_state: None,
            web_search: None,
        })
    }
}

fn echo(down: bool) -> Arc<Echo> {
    Arc::new(Echo {
        calls: AtomicUsize::new(0),
        down: AtomicBool::new(down),
    })
}

struct Dir(Utf8PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const HEADER: &str = r#"
[generator]
id = "reuse-test"
name = "Reuse test"
version = "0.1.0"
[llm]
model = { provider = "fake", model = "test" }
max_tokens = 8
[audit]
level = "standard"
[budget]
max_tokens = 1000000
[permissions]
fs_write = ["workspace"]
fs_read = ["workspace"]
network = []
commands = []
containers = { enabled = false, images = [], on_missing = "error" }
side_effects = "none"
[[inputs.stages]]
id = "main"
[[inputs.stages.fields]]
id = "topic"
type = "string"
required = false
[[inputs.stages.fields]]
id = "selected"
type = "string"
required = false
[[inputs.stages.fields]]
id = "use_opt"
type = "boolean"
required = false
"#;

/// A generator whose steps are `(id, extra node keys, prompt)`; each step
/// needs its predecessor and writes `<id>.out`.
fn generator(name: &str, steps: &[(&str, &str, &str)]) -> Dir {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir().join(format!(
        "qcg-reuse-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )))
    .unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let mut manifest = HEADER.to_string();
    for (index, (id, extra, prompt)) in steps.iter().enumerate() {
        let needs = if index == 0 {
            "[]".to_string()
        } else {
            format!("[\"{}\"]", steps[index - 1].0)
        };
        manifest.push_str(&format!(
            "[[flow]]\nid = \"{id}\"\ntype = \"llm.generate\"\nneeds = {needs}\non_deps = \"none_failed\"\n{extra}\n[flow.params]\nprompt = \"{id}.txt\"\noutput_file = \"{id}.out\"\n"
        ));
        std::fs::write(root.join(format!("{id}.txt")), prompt).unwrap();
    }
    std::fs::write(root.join(contract::MANIFEST_FILE), manifest).unwrap();
    Dir(root)
}

fn chain() -> Dir {
    generator(
        "chain",
        &[
            ("a", "", "STEP-A {{ inputs.topic }}"),
            ("b", "", "STEP-B {{ steps.a.output.text }}"),
            ("c", "", "STEP-C {{ steps.b.output.text }}"),
            ("d", "", "STEP-D {{ steps.c.output.text }}"),
            ("e", "", "STEP-E {{ steps.d.output.text }}"),
        ],
    )
}

fn inputs(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

async fn run(
    dir: &Dir,
    tag: &str,
    inputs: BTreeMap<String, Value>,
    provider: &Arc<Echo>,
    reuse: Option<Arc<ReuseSource>>,
) -> Progress {
    let mut registry = StepRegistry::new();
    crate::register_llm_steps(&mut registry, Arc::new(LlmRuntime::builtins()));
    let run_dir = dir.0.join(tag);
    let options = RunOptions {
        elapsed_budget_mode: Default::default(),
        output_dir: run_dir.join("workspace"),
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
        reuse,
        cancellation: Default::default(),
        shutdown: None,
        scheduler_yield: Default::default(),
    };
    Engine::new(registry)
        .advance_with_id(
            tag.into(),
            run_dir.join("meta"),
            Contract::load(dir.0.clone()).unwrap(),
            inputs,
            options,
        )
        .await
}

fn events(dir: &Dir, tag: &str) -> Vec<Value> {
    std::fs::read_to_string(dir.0.join(tag).join("meta/journal.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn source(dir: &Dir, tag: &str) -> Arc<ReuseSource> {
    source_from(dir, tag, &events(dir, tag))
}

fn source_from(dir: &Dir, tag: &str, events: &[Value]) -> Arc<ReuseSource> {
    Arc::new(ReuseSource::from_events(
        tag,
        &dir.0.join(tag).join("meta"),
        events,
    ))
}

fn outputs(dir: &Dir, tag: &str, steps: &[&str]) -> Vec<String> {
    steps
        .iter()
        .map(|id| {
            std::fs::read_to_string(dir.0.join(tag).join(format!("workspace/{id}.out"))).unwrap()
        })
        .collect()
}

fn reused(dir: &Dir, tag: &str) -> Vec<String> {
    events(dir, tag)
        .iter()
        .filter(|event| event["t"] == "llm_memo" && event["reused_from"].is_object())
        .map(|event| event["node"].as_str().unwrap().to_string())
        .collect()
}

const STEPS: [&str; 5] = ["a", "b", "c", "d", "e"];

#[tokio::test]
async fn continuation_repays_only_the_failed_step_and_what_follows() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(true);
    let failed = run(&dir, "src", topic.clone(), &provider, None).await;
    assert!(matches!(failed, Progress::Failed(_)), "{failed:?}");
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        3,
        "a, b and the failed c"
    );

    provider.down.store(false, Ordering::SeqCst);
    provider.calls.store(0, Ordering::SeqCst);
    let done = run(
        &dir,
        "child",
        topic.clone(),
        &provider,
        Some(source(&dir, "src")),
    )
    .await;
    assert!(matches!(done, Progress::Done(_)), "{done:?}");
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        3,
        "only c, d and e reach the provider"
    );
    assert_eq!(reused(&dir, "child"), ["a", "b"]);
    let memo = events(&dir, "child")
        .into_iter()
        .find(|e| e["reused_from"].is_object())
        .unwrap();
    assert_eq!(memo["reused_from"]["run_id"], "src");
    assert!(memo["reused_from"]["seq"].as_u64().unwrap() > 0);
    let state = RunState::fold_journal(&dir.0.join("child/meta/journal.jsonl")).unwrap();
    assert_eq!(
        state.budget.llm_calls, 3,
        "reused responses are not provider calls"
    );

    run(&dir, "fresh", topic, &provider, None).await;
    assert_eq!(
        outputs(&dir, "child", &STEPS),
        outputs(&dir, "fresh", &STEPS)
    );
}

#[tokio::test]
async fn unchanged_continuation_of_a_finished_run_makes_no_provider_call() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    provider.calls.store(0, Ordering::SeqCst);
    let done = run(&dir, "child", topic, &provider, Some(source(&dir, "src"))).await;
    assert!(matches!(done, Progress::Done(_)), "{done:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    assert_eq!(reused(&dir, "child"), STEPS);
    // A continuation can itself be continued.
    run(
        &dir,
        "grandchild",
        inputs(&[("topic", json!("x"))]),
        &provider,
        Some(source(&dir, "child")),
    )
    .await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dynamic_step_selection_that_changes_reruns_only_the_dependent_steps() {
    let dir = generator(
        "dynamic",
        &[
            ("a", "", "STEP-A one"),
            ("b", "", "STEP-B two"),
            ("pick", "", "PICK {{ steps[inputs.selected].output.text }}"),
            ("last", "", "LAST {{ steps.pick.output.text }}"),
        ],
    );
    let provider = echo(false);
    run(
        &dir,
        "src",
        inputs(&[("selected", json!("a"))]),
        &provider,
        None,
    )
    .await;
    provider.calls.store(0, Ordering::SeqCst);
    let done = run(
        &dir,
        "child",
        inputs(&[("selected", json!("b"))]),
        &provider,
        Some(source(&dir, "src")),
    )
    .await;
    assert!(matches!(done, Progress::Done(_)), "{done:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2, "pick and last");
    assert_eq!(reused(&dir, "child"), ["a", "b"]);
    assert!(outputs(&dir, "child", &["pick"])[0].contains("STEP-B two"));
}

#[tokio::test]
async fn a_read_that_was_missing_in_the_source_and_is_present_now_reruns_its_reader() {
    let dir = generator(
        "missing",
        &[
            ("first", "", "STEP-A first"),
            ("opt", "when = \"inputs.use_opt\"", "STEP-B opt"),
            (
                "use",
                "",
                "USE {% if steps.opt %}{{ steps.opt.output.text }}{% else %}none{% endif %}",
            ),
        ],
    );
    let provider = echo(false);
    run(
        &dir,
        "src",
        inputs(&[("use_opt", json!(false))]),
        &provider,
        None,
    )
    .await;
    provider.calls.store(0, Ordering::SeqCst);
    let done = run(
        &dir,
        "child",
        inputs(&[("use_opt", json!(true))]),
        &provider,
        Some(source(&dir, "src")),
    )
    .await;
    assert!(matches!(done, Progress::Done(_)), "{done:?}");
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        2,
        "opt now runs and use reads it"
    );
    assert_eq!(reused(&dir, "child"), ["first"]);
    assert!(outputs(&dir, "child", &["use"])[0].contains("STEP-B opt"));
}

#[tokio::test]
async fn a_changed_step_definition_reruns_that_step_and_its_readers_only() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    std::fs::write(
        dir.0.join("c.txt"),
        "STEP-C changed {{ steps.b.output.text }}",
    )
    .unwrap();
    provider.calls.store(0, Ordering::SeqCst);
    run(&dir, "child", topic, &provider, Some(source(&dir, "src"))).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 3, "c, d and e");
    assert_eq!(reused(&dir, "child"), ["a", "b"]);
}

#[tokio::test]
async fn a_changed_request_parameter_is_a_different_request() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    let manifest = dir.0.join(contract::MANIFEST_FILE);
    let text = std::fs::read_to_string(&manifest).unwrap();
    std::fs::write(&manifest, text.replace("max_tokens = 8", "max_tokens = 9")).unwrap();
    provider.calls.store(0, Ordering::SeqCst);
    run(&dir, "child", topic, &provider, Some(source(&dir, "src"))).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
    assert!(reused(&dir, "child").is_empty());
}

#[tokio::test]
async fn a_never_policy_repeats_the_effect_while_the_rest_is_reused() {
    let dir = generator(
        "never",
        &[
            ("a", "", "STEP-A {{ inputs.topic }}"),
            ("b", "reuse = \"never\"", "STEP-B {{ steps.a.output.text }}"),
            ("c", "", "STEP-C {{ steps.b.output.text }}"),
        ],
    );
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    provider.calls.store(0, Ordering::SeqCst);
    run(&dir, "child", topic, &provider, Some(source(&dir, "src"))).await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1, "only b");
    assert_eq!(reused(&dir, "child"), ["a", "c"]);
}

#[tokio::test]
async fn missing_or_altered_recorded_responses_fall_back_to_the_provider() {
    for tamper in [false, true] {
        let dir = chain();
        let topic = inputs(&[("topic", json!("x"))]);
        let provider = echo(false);
        run(&dir, "src", topic.clone(), &provider, None).await;
        let memo = events(&dir, "src")
            .into_iter()
            .find(|e| e["t"] == "llm_memo" && e["node"] == "a")
            .unwrap();
        let sidecar = dir.0.join(format!(
            "src/meta/llm-memo/{}.json",
            memo["memo_ref"].as_str().unwrap()
        ));
        if tamper {
            std::fs::write(&sidecar, b"{\"forged\":true}").unwrap();
        } else {
            std::fs::remove_file(&sidecar).unwrap();
        }
        provider.calls.store(0, Ordering::SeqCst);
        let done = run(&dir, "child", topic, &provider, Some(source(&dir, "src"))).await;
        assert!(matches!(done, Progress::Done(_)), "{done:?}");
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1, "only a is repaid");
        assert_eq!(reused(&dir, "child"), ["b", "c", "d", "e"]);
    }
}

#[tokio::test]
async fn a_journal_without_records_reuses_nothing_and_still_completes() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    let legacy: Vec<Value> = events(&dir, "src")
        .into_iter()
        .filter(|e| e["t"] != "llm_memo")
        .collect();
    provider.calls.store(0, Ordering::SeqCst);
    let done = run(
        &dir,
        "child",
        topic,
        &provider,
        Some(source_from(&dir, "src", &legacy)),
    )
    .await;
    assert!(matches!(done, Progress::Done(_)), "{done:?}");
    assert_eq!(provider.calls.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn responses_of_steps_that_did_not_succeed_are_not_served() {
    let dir = chain();
    let topic = inputs(&[("topic", json!("x"))]);
    let provider = echo(false);
    run(&dir, "src", topic.clone(), &provider, None).await;
    let unfinished: Vec<Value> = events(&dir, "src")
        .into_iter()
        .filter(|e| !(e["t"] == "step_finished" && e["node"] == "b"))
        .collect();
    provider.calls.store(0, Ordering::SeqCst);
    run(
        &dir,
        "child",
        topic,
        &provider,
        Some(source_from(&dir, "src", &unfinished)),
    )
    .await;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1, "only b is repaid");
}
