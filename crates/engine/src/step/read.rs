//! Stable MiniJinja Object adapters for JSON collections. Scalars stay native:
//! their lookup depends on the full scalar (including subsequent string indexes).
use contract::{ReadIdentity, ReadKind, ReadRecorder};
use minijinja::value::{Enumerator, Object, ObjectRepr, Value, ValueKind};
use std::sync::Arc;

#[derive(Debug)]
struct Tracked {
    value: Value,
    path: Vec<String>,
    reads: ReadRecorder,
}

pub(super) fn context(value: Value, reads: ReadRecorder) -> Value {
    wrap(value, Vec::new(), reads)
}

fn wrap(value: Value, path: Vec<String>, reads: ReadRecorder) -> Value {
    if matches!(value.kind(), ValueKind::Map | ValueKind::Seq) {
        Value::from_object(Tracked { value, path, reads })
    } else {
        value
    }
}

fn identity(value: &Value) -> ReadIdentity {
    if value.is_undefined() {
        return ReadIdentity::Missing;
    }
    if matches!(value.kind(), ValueKind::Map | ValueKind::Seq) {
        return ReadIdentity::Present;
    }
    match serde_json::to_value(value) {
        Ok(value) => ReadIdentity::of(Some(&value)),
        Err(_) => ReadIdentity::Unsupported,
    }
}

fn collection_identity(value: &Value) -> ReadIdentity {
    match serde_json::to_value(value) {
        Ok(value) => ReadIdentity::of(Some(&value)),
        Err(_) => ReadIdentity::Unsupported,
    }
}

impl Tracked {
    fn unsupported_lookup(&self) {
        self.reads
            .record(&self.path, ReadKind::Lookup, ReadIdentity::Unsupported);
        self.collection();
    }

    fn collection(&self) {
        self.reads.record(
            &self.path,
            ReadKind::Collection,
            collection_identity(&self.value),
        );
    }
}

impl Object for Tracked {
    fn repr(self: &Arc<Self>) -> ObjectRepr {
        self.collection();
        self.value.as_object().expect("JSON collection").repr()
    }

    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let segment = key.as_str().map(str::to_owned).or_else(|| {
            matches!(self.value.kind(), ValueKind::Seq)
                .then(|| key.as_i64().map(|index| index.to_string()))
                .flatten()
        });
        let Some(segment) = segment else {
            // Preserve native lookup behavior for non-path keys, but never
            // claim that such an access was completely traced.
            self.unsupported_lookup();
            let value = self.value.get_item(key).ok()?;
            return (!value.is_undefined()).then_some(value);
        };
        let value = match self.value.get_item(key) {
            Ok(value) => value,
            Err(_) => {
                self.unsupported_lookup();
                return None;
            }
        };
        let mut path = self.path.clone();
        path.push(segment);
        let retained = self.reads.record(&path, ReadKind::Lookup, identity(&value));
        if value.is_undefined() {
            None
        } else if retained {
            Some(wrap(value, path, self.reads.clone()))
        } else {
            // Once overflowed, do not retain more adapter paths either.
            Some(value)
        }
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        self.collection();
        // Map enumeration may yield raw pairs; the full-subtree dependency
        // above covers those reads even if a filter bypasses get_value.
        self.value.as_object().expect("JSON collection").enumerate()
    }

    fn enumerator_len(self: &Arc<Self>) -> Option<usize> {
        self.collection();
        self.value.len()
    }

    fn is_true(self: &Arc<Self>) -> bool {
        self.collection();
        self.value.is_true()
    }

    fn render(self: &Arc<Self>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.collection();
        self.value.as_object().expect("JSON collection").render(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalWriter, StepOutcome, TemplateService};
    use contract::{ReadOwner, RuntimeLimits, ValueBag};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn render_bag(
        source: &str,
        bag: &ValueBag,
        limits: &RuntimeLimits,
    ) -> Result<String, minijinja::Error> {
        TemplateService.render_bag_scoped(ReadOwner::Node("test".into()), source, bag, limits)
    }

    fn bag() -> ValueBag {
        let mut bag = ValueBag::with_inputs(BTreeMap::from([
            ("key".into(), json!("ready")),
            ("target".into(), json!("future")),
            ("field".into(), json!("a.b")),
            ("index".into(), json!(1)),
            ("rows".into(), json!([{"a.b":"one"}, {"a.b":"two"}])),
            ("map".into(), json!({"x":{"name":"X"}, "y":{"name":"Y"}})),
        ]));
        bag.set_step_output("ready", json!({"rows":[{"name":"ok"}]}));
        bag.set_step_status("pending", "running");
        bag
    }

    fn has(bag: &ValueBag, path: &[&str], kind: ReadKind, missing: bool) -> bool {
        bag.read_trace().reads.iter().any(|r| {
            r.path == path && r.kind == kind && (r.identity == ReadIdentity::Missing) == missing
        })
    }

    #[test]
    fn template_reads_dynamic_missing_forward_and_nested_paths() {
        let mut bag = bag();
        let limits = RuntimeLimits::default();
        let source = "{{ steps[inputs.key].output.rows[0].name }}:{{ inputs.rows[inputs.index][inputs.field] }}:{{ steps[inputs.target]|default('later') }}:{{ steps.pending.output|default('none') }}{% if false %}{{ inputs.unread }}{% endif %}";
        let rendered = render_bag(source, &bag, &limits).unwrap();
        assert_eq!(rendered, "ok:two:later:none");
        for path in [
            vec!["inputs", "key"],
            vec!["inputs", "target"],
            vec!["inputs", "index"],
            vec!["inputs", "field"],
            vec!["inputs", "rows", "1", "a.b"],
            vec!["steps", "ready", "output", "rows", "0", "name"],
        ] {
            assert!(has(&bag, &path, ReadKind::Lookup, false), "{path:?}");
        }
        assert!(has(&bag, &["steps", "future"], ReadKind::Lookup, true));
        assert!(has(
            &bag,
            &["steps", "pending", "output"],
            ReadKind::Lookup,
            true
        ));
        assert!(
            !bag.read_trace()
                .reads
                .iter()
                .any(|r| r.path.iter().any(|p| p == "unread"))
        );
        // A missing ancestor remains native undefined: do not synthesize a
        // proxy that changes the existing chained-undefined error behavior.
        let source = "{{ steps[inputs.target].output.name }}";
        let native = TemplateService
            .render_inline(source, bag.template_snapshot(), &limits)
            .unwrap_err();
        let traced = render_bag(source, &bag, &limits).unwrap_err();
        assert_eq!(native.kind(), traced.kind());
        assert_eq!(native.to_string(), traced.to_string());
        bag.set_step_output("future", json!({"name":"arrived"}));
        assert_eq!(render_bag(source, &bag, &limits).unwrap(), "arrived");
        assert!(has(
            &bag,
            &["steps", "future", "output", "name"],
            ReadKind::Lookup,
            false
        ));
        assert_eq!(
            render_bag(
                "{{ inputs.rows[99]|default('none') }}:{{ inputs.map.missing|default('none') }}",
                &bag,
                &limits,
            )
            .unwrap(),
            "none:none"
        );
        assert!(has(&bag, &["inputs", "rows", "99"], ReadKind::Lookup, true));
        assert!(has(
            &bag,
            &["inputs", "map", "missing"],
            ReadKind::Lookup,
            true
        ));
        assert!(!bag.read_trace().incomplete);
    }

    #[test]
    fn template_reads_enumeration_is_conservative_and_native_behavior_is_preserved() {
        let limits = RuntimeLimits::default();
        for source in [
            "{% for k in inputs.map %}{{ k }}{% endfor %}",
            "{% for row in inputs.rows %}{{ row['a.b'] }}{% endfor %}",
            "{{ inputs.map|items|list }}",
            "{{ inputs.rows|map(attribute='a.b')|list }}",
            "{{ inputs.rows[-1]['a.b'] }}",
            "{{ inputs.rows[0:2]|reverse|list }}",
            "{{ inputs.rows[99]|default('absent') }}",
            "{{ inputs.rows['bad']|default('absent') }}",
            "{{ inputs.map == inputs.map }}:{{ inputs.rows == inputs.rows }}",
            "{{ inputs.rows|length }}:{% if inputs.map %}yes{% endif %}",
            "{{ inputs.map|tojson }}:{{ inputs.rows|pprint }}:{{ inputs.map }}",
            "{{ inputs.map.x.name[0] }}",
            "{{ inputs.map[true]|default('absent') }}",
            "{{ inputs.map[[1]]|default('absent') }}",
            "{{ inputs.map[{}]|default('absent') }}",
            "{{ inputs.map.items() }}",
            "{{ inputs.rows + [3] }}",
        ] {
            let bag = bag();
            let native = TemplateService.render_inline(source, bag.template_snapshot(), &limits);
            let traced = render_bag(source, &bag, &limits);
            match (native, traced) {
                (Ok(a), Ok(b)) => assert_eq!(a, b, "{source}"),
                (Err(a), Err(b)) => {
                    assert_eq!(a.kind(), b.kind(), "{source}");
                    assert_eq!(a.to_string(), b.to_string(), "{source}");
                }
                other => panic!("behavior changed for {source}: {other:?}"),
            }
        }
        for (source, path) in [
            (
                "{% for k in inputs.map %}{{ k }}{% endfor %}",
                vec!["inputs", "map"],
            ),
            (
                "{% for v in inputs.rows %}{{ v['a.b'] }}{% endfor %}",
                vec!["inputs", "rows"],
            ),
        ] {
            let bag = bag();
            render_bag(source, &bag, &limits).unwrap();
            assert!(has(&bag, &path, ReadKind::Collection, false));
            assert!(!bag.read_trace().incomplete);
        }
        let rows_source = "{% for _ in inputs.rows %}x{% endfor %}";
        let original_rows = bag();
        assert_eq!(
            render_bag(rows_source, &original_rows, &limits).unwrap(),
            "xx"
        );
        let original_rows_identity = original_rows
            .read_trace()
            .reads
            .iter()
            .find(|read| read.path == ["inputs", "rows"] && read.kind == ReadKind::Collection)
            .expect("enumeration records a subtree dependency")
            .identity
            .clone();
        assert_eq!(
            original_rows_identity,
            ReadIdentity::of(Some(&json!([{"a.b":"one"}, {"a.b":"two"}])))
        );
        let mut changed_rows = bag();
        changed_rows.patch_inputs(BTreeMap::from([(
            "rows".into(),
            json!([{"a.b":"changed"}, {"a.b":"two"}]),
        )]));
        assert_eq!(
            render_bag(rows_source, &changed_rows, &limits).unwrap(),
            "xx"
        );
        let changed_rows_identity = changed_rows
            .read_trace()
            .reads
            .iter()
            .find(|read| read.path == ["inputs", "rows"] && read.kind == ReadKind::Collection)
            .expect("enumeration records a subtree dependency")
            .identity
            .clone();
        assert_ne!(original_rows_identity, changed_rows_identity);

        let map_source = "{% for _ in inputs.map %}x{% endfor %}";
        let original_map = bag();
        assert_eq!(
            render_bag(map_source, &original_map, &limits).unwrap(),
            "xx"
        );
        let original_map_identity = original_map
            .read_trace()
            .reads
            .iter()
            .find(|read| read.path == ["inputs", "map"] && read.kind == ReadKind::Collection)
            .expect("map enumeration records a subtree dependency")
            .identity
            .clone();
        assert_eq!(
            original_map_identity,
            ReadIdentity::of(Some(&json!({"x":{"name":"X"}, "y":{"name":"Y"}})))
        );
        let mut changed_map = bag();
        changed_map.patch_inputs(BTreeMap::from([(
            "map".into(),
            json!({"x":{"name":"changed"}, "y":{"name":"Y"}}),
        )]));
        assert_eq!(render_bag(map_source, &changed_map, &limits).unwrap(), "xx");
        let changed_map_identity = changed_map
            .read_trace()
            .reads
            .iter()
            .find(|read| read.path == ["inputs", "map"] && read.kind == ReadKind::Collection)
            .expect("map enumeration records a subtree dependency")
            .identity
            .clone();
        assert_ne!(original_map_identity, changed_map_identity);
        let nested_bag = bag();
        render_bag("{{ inputs.map.x.name }}", &nested_bag, &limits).unwrap();
        let trace = nested_bag.read_trace();
        assert!(trace.reads.iter().any(|read| {
            read.path == ["inputs", "map", "x", "name"]
                && matches!(read.identity, ReadIdentity::Digest(_))
        }));
        assert!(
            !trace
                .reads
                .iter()
                .any(|read| { read.path == ["inputs"] && read.identity != ReadIdentity::Present })
        );
        assert!(
            !trace.reads.iter().any(|read| {
                read.path == ["inputs", "map"] && read.kind == ReadKind::Collection
            })
        );

        let compare_bag = bag();
        render_bag("{{ inputs.map == inputs.map }}", &compare_bag, &limits).unwrap();
        assert!(has(
            &compare_bag,
            &["inputs", "map"],
            ReadKind::Collection,
            false
        ));

        let bag = bag();
        render_bag("{{ inputs.map[true] }}", &bag, &limits).unwrap();
        assert!(bag.read_trace().unsupported && bag.read_trace().incomplete);
    }

    #[test]
    fn scoped_templates_preserve_owners_and_observed_value_identities() {
        let mut bag = bag();
        let limits = RuntimeLimits::default();
        let a = ReadOwner::Node("each[0]/render".into());
        let b = ReadOwner::Node("each[1]/render".into());
        assert_eq!(
            TemplateService
                .render_bag_scoped(a.clone(), "{{ inputs.key }}", &bag, &limits)
                .unwrap(),
            "ready"
        );
        bag.patch_inputs(BTreeMap::from([("key".into(), json!("changed"))]));
        assert_eq!(
            TemplateService
                .render_bag_scoped(b.clone(), "{{ inputs.key }}", &bag, &limits)
                .unwrap(),
            "changed"
        );
        bag.eval_value_scoped(b.clone(), "inputs.key").unwrap();
        let trace = bag.read_trace();
        let reads = trace
            .reads
            .iter()
            .filter(|r| r.path == ["inputs", "key"])
            .collect::<Vec<_>>();
        assert_eq!(reads.len(), 2); // expression/template identity agrees
        assert!(
            reads
                .iter()
                .any(|r| r.owner == a && r.identity == ReadIdentity::of(Some(&json!("ready"))))
        );
        assert!(
            reads
                .iter()
                .any(|r| r.owner == b && r.identity == ReadIdentity::of(Some(&json!("changed"))))
        );
        assert!(trace.complete_for(&a) && trace.complete_for(&b));
        assert!(!trace.owners.contains(&ReadOwner::Unscoped));
        // Ordinary detached JSON access is conservative, never a silent escape.
        bag.to_json();
        assert!(
            bag.read_trace()
                .reads
                .iter()
                .any(|r| r.path.is_empty() && r.kind == ReadKind::Collection)
        );
        assert!(!bag.read_trace().complete_for(&a));
    }

    #[test]
    fn invalid_keys_fail_closed_even_when_native_lookup_errors() {
        let bag = bag();
        let owner = ReadOwner::Node("render".into());
        for key in [
            Value::from(true),
            Value::from_serialize([1]),
            Value::from_serialize(json!({"k":1})),
        ] {
            let tracked = Arc::new(Tracked {
                value: Value::from_serialize(json!({"x":1})),
                path: vec!["inputs".into(), "map".into()],
                reads: bag.read_recorder().for_owner(owner.clone()),
            });
            let native = tracked.value.get_item(&key).ok();
            let actual = tracked.get_value(&key);
            assert_eq!(actual, native.filter(|v| !v.is_undefined()));
            assert!(bag.read_trace().unsupported);
            assert!(!bag.read_trace().complete_for(&owner));
            assert!(
                bag.read_trace()
                    .reads
                    .iter()
                    .any(|r| r.owner == owner && r.identity == ReadIdentity::Unsupported)
            );
        }
    }

    #[test]
    fn template_reads_are_bounded_isolated_and_not_in_results_or_journal() {
        let bag = bag();
        let limits = RuntimeLimits {
            journal_event_limit_bytes: Some(64),
            ..RuntimeLimits::default()
        };
        let source = "{{ inputs|tojson }}";
        let public = TemplateService
            .render_inline(source, bag.template_snapshot(), &limits)
            .unwrap();
        let output = render_bag(source, &bag, &limits).unwrap();
        assert_eq!(public, output);
        assert!(bag.read_trace().overflow && bag.read_trace().incomplete);
        assert!(bag.read_trace().retained_bytes <= 64);
        let child = ValueBag::default();
        assert!(child.read_trace().reads.is_empty());
        render_bag("{{ item }}", &child, &RuntimeLimits::default()).unwrap();
        assert!(has(&child, &["item"], ReadKind::Lookup, false));
        assert!(!has(&bag, &["item"], ReadKind::Lookup, false));
        let outcome = serde_json::to_value(StepOutcome::success(Some(json!(output)))).unwrap();
        assert_eq!(
            outcome,
            json!({"status":"success", "output":public, "files":[]})
        );
        let dir = std::env::temp_dir().join(format!("qcg-read-trace-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = camino::Utf8PathBuf::from_path_buf(dir.join("journal.jsonl")).unwrap();
        let journal = JournalWriter::create(&path, "read-trace", false, None).unwrap();
        let mut event = outcome;
        event["node"] = json!("render");
        journal.event("step_finished", &event).unwrap();
        drop(journal);
        let persisted: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(&path).unwrap().trim()).unwrap();
        assert_eq!(persisted["output"], public);
        assert!(!persisted.as_object().unwrap().keys().any(|key| {
            [
                "reads",
                "read_trace",
                "retained_bytes",
                "incomplete",
                "overflow",
                "unsupported",
            ]
            .contains(&key.as_str())
        }));
        assert!(!public.contains("ReadRecorder") && !public.contains("retained_bytes"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
