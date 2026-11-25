//! Operator discoverability of an unresolved container effect on a run that
//! is already terminal. The CLI must not hide the fail-closed diagnostic:
//! default `runs show` warns, and `--diagnose` lists it as a cause. Both are
//! derived from the already-folded journal; no second scan is introduced.

use camino::Utf8PathBuf;
use serde_json::{Value, json};

const MARKER: &str = "qcg has not proved that the effect stopped";

fn cli(runs: &Utf8PathBuf, args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_qcg"))
        .arg("runs")
        .args(args)
        .arg("--runs-dir")
        .arg(runs)
        .env("AUTO_GC", "false")
        .output()
        .unwrap()
}

/// A terminal run with one open (never-closed) container effect and the
/// state-free `effect_unresolved` diagnostic appended after the terminal event.
fn write_run(runs: &Utf8PathBuf, id: &str, include_diagnostic: bool) {
    let meta = runs.join(id).join("meta");
    std::fs::create_dir_all(&meta).unwrap();
    let journal =
        engine::JournalWriter::create(&meta.join("journal.jsonl"), id, false, None).unwrap();
    journal
        .event(
            "run_started",
            json!({
                "generator": "demo@1.0.0", "generator_path": "demo", "contract_sha256": "test",
                "inputs": {}, "resource_hashes": [], "schema_version": api::JOURNAL_SCHEMA_VERSION,
                "elapsed_budget_mode": "wall_clock", "retention_days": 10,
            }),
        )
        .unwrap();
    journal
        .event(
            "effect_intent",
            json!({"effect": "orphan-instance", "backend": {"unknown": true}}),
        )
        .unwrap();
    journal
        .event("run_finished", json!({"status": "success"}))
        .unwrap();
    if include_diagnostic {
        journal
            .event(
                "effect_unresolved",
                json!({"detail": format!("container effect `orphan-instance`: reconcile failed; {MARKER}; reconcile the external outcome manually")}),
            )
            .unwrap();
    }
    drop(journal);
}

#[test]
fn terminal_unresolved_effect_is_visible_in_show_and_diagnose() {
    let root = Utf8PathBuf::from_path_buf(std::env::temp_dir())
        .unwrap()
        .join(format!("runs-unresolved-{}", uuid::Uuid::now_v7()));
    let runs = root.join("runs");
    write_run(&runs, "unresolved", true);
    write_run(&runs, "resolved", false);

    // Default text view surfaces a short warning carrying the exact marker.
    let shown = cli(&runs, &["show", "unresolved"]);
    assert!(
        shown.status.success(),
        "{}",
        String::from_utf8_lossy(&shown.stderr)
    );
    let text = String::from_utf8(shown.stdout).unwrap();
    assert!(text.contains("warning:"), "{text}");
    assert!(text.contains(MARKER), "{text}");

    // A clean terminal run must not warn.
    let clean = cli(&runs, &["show", "resolved"]);
    assert!(
        clean.status.success(),
        "{}",
        String::from_utf8_lossy(&clean.stderr)
    );
    let clean_text = String::from_utf8(clean.stdout).unwrap();
    assert!(!clean_text.contains(MARKER), "{clean_text}");

    // JSON summary carries a folded flag (not a stored reason).
    let json_out = cli(&runs, &["show", "unresolved", "--json"]);
    assert!(
        json_out.status.success(),
        "{}",
        String::from_utf8_lossy(&json_out.stderr)
    );
    let summary: Value = serde_json::from_slice(&json_out.stdout).unwrap();
    assert_eq!(summary["unresolved_effect"], true, "{summary}");
    let clean_json_out = cli(&runs, &["show", "resolved", "--json"]);
    let clean_summary: Value = serde_json::from_slice(&clean_json_out.stdout).unwrap();
    assert_eq!(clean_summary["unresolved_effect"], false, "{clean_summary}");

    // --diagnose lists effect_unresolved as a cause with the marker in detail.
    let diagnosed = cli(&runs, &["show", "unresolved", "--json", "--diagnose"]);
    assert!(
        diagnosed.status.success(),
        "{}",
        String::from_utf8_lossy(&diagnosed.stderr)
    );
    let report: Value = serde_json::from_slice(&diagnosed.stdout).unwrap();
    let causes = report["diagnosis"]["causes"].as_array().unwrap();
    let effect_cause = causes
        .iter()
        .find(|cause| cause["kind"] == "effect_unresolved")
        .expect("effect_unresolved must be surfaced as a cause");
    assert!(
        effect_cause["message"]
            .as_str()
            .is_some_and(|message| message.contains(MARKER)),
        "{effect_cause}"
    );
    assert_eq!(report["summary"]["unresolved_effect"], true, "{report}");

    std::fs::remove_dir_all(&root).unwrap();
}
