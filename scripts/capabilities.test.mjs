import { test } from "node:test";
import assert from "node:assert/strict";
import { verify } from "./check-capabilities.mjs";
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
