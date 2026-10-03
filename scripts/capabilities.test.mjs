import { test } from "node:test";
import assert from "node:assert/strict";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { verify, unixGatedInFile, unixGatedTests } from "./check-capabilities.mjs";
test("only registered tests satisfy claims; empty inputs fail closed", () => {
  assert.equal(
    verify("| A | guarantee | `live_test` |", "module::live_test: test"),
    1,
  );
  assert.throws(() =>
    verify("| A | guarantee | `inert_function` |", "module::live_test: test"),
  );
  assert.throws(() => verify("", "module::live_test: test"));
  assert.throws(() => verify("| A | guarantee | `live_test` |", ""));
});
test("win32 skips unix-gated claims but still demands everything else", () => {
  const matrix =
    "| U | unix guarantee | `unix_only_test` |\n| P | portable guarantee | `portable_test` |";
  const gated = new Set(["unix_only_test"]);
  assert.equal(
    verify(matrix, "module::portable_test: test", "win32", gated),
    2,
  );
  assert.throws(() =>
    verify(matrix, "module::portable_test: test", "linux", gated),
  );
  assert.throws(() =>
    verify(matrix, "module::unix_only_test: test", "win32", gated),
  );
});
test("unix-gated scan finds test modules, not portable code", () => {
  const source = `
#[cfg(all(test, unix))]
mod unix_only {
    #[test]
    fn unix_test() {}
    fn helper() {}
    #[test]
    fn brace_in_string() {
        let s = format!("{} {toml = 1}");
        let raw = r#"{"a": 1}"#;
        // } stray brace in comment
        /* } block comment */ let _ = s.len() + raw.len();
    }
}
#[cfg(test)]
mod portable {
    #[test]
    fn portable_test() {}
}
#[cfg(unix)]
#[test]
fn direct_gated_test() {}
#[test]
fn plain_test() {}
`;
  assert.deepEqual(
    [...unixGatedInFile(source)].sort(),
    ["brace_in_string", "direct_gated_test", "unix_test"],
  );
});
test("live tree exposes every unix-only matrix claim to the gate", () => {
  const root = join(dirname(fileURLToPath(import.meta.url)), "..", "crates");
  const gated = unixGatedTests(root);
  for (const name of [
    "concurrent_same_file_patches_fail_closed_on_base_drift",
    "commit_time_recheck_refuses_a_foreign_writer",
    "g02_patch_vs_ordinary_write_serializes_without_loss",
    "g02_repair_style_verify_then_preserving_write_serializes",
    "g02_delete_recreate_keeps_presence_expectation",
    "g02_guarded_writer_does_not_self_deadlock_and_keeps_parallelism",
    "g06_patch_preserves_executable_bit_and_runs",
    "g06_data_mode_and_special_bits_are_sanitized",
    "g06_rejected_patch_leaves_content_and_mode_untouched",
  ])
    assert.ok(gated.has(name), `${name} must be unix-gated`);
});
