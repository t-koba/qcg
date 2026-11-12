use super::super::reuse::request_digest;
use super::*;

#[test]
fn digest_is_stable_and_follows_every_part_of_the_request() {
    let node = test_node();
    let base = test_request();
    let digest = |node: &NodeDef, request: &ChatRequest| request_digest(node, request).unwrap();
    assert_eq!(digest(&node, &base), digest(&node, &base.clone()));

    let mut other = test_node();
    other.id = "elsewhere".into();
    assert_ne!(digest(&node, &base), digest(&other, &base), "node identity");
    for change in [
        |r: &mut ChatRequest| r.model.push('x'),
        |r: &mut ChatRequest| r.provider.push('x'),
        |r: &mut ChatRequest| r.max_tokens += 1,
        |r: &mut ChatRequest| r.temperature = Some(0.5),
        |r: &mut ChatRequest| r.system = Some("other".into()),
        |r: &mut ChatRequest| r.messages[0].content.push('x'),
        |r: &mut ChatRequest| r.response_schema = Some(serde_json::json!({"type": "object"})),
    ] {
        let mut changed = base.clone();
        change(&mut changed);
        assert_ne!(digest(&node, &base), digest(&node, &changed));
    }
}
