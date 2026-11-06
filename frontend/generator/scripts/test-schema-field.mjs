import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { createServer } from "vite";

// Vitest's SSR tests cannot run onMount; use the existing Playwright browser.
const server = await createServer({
  root: fileURLToPath(new URL("..", import.meta.url)),
  server: { host: "127.0.0.1", port: 0 },
});
let browser;
try {
  await server.listen();
  browser = await chromium.launch();
  const page = await browser.newPage();
  const errors = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto(`${server.resolvedUrls.local[0]}testdata/schema-field.html`);
  await page.waitForFunction(() => typeof window.renderSchemaField === "function");

  const emptyObject = { type: "object", properties: {}, additionalProperties: false };
  for (const combinator of ["oneOf", "anyOf"]) {
    const result = await page.evaluate(async ({ combinator, emptyObject }) => {
      return window.renderSchemaField({ [combinator]: [emptyObject] });
    }, { combinator, emptyObject });
    assert.equal(result.options, 1);
    assert.deepEqual(result.emitted, [{ path: [], value: {} }]);
  }

  // Defined values and schemas without a sole initializable branch stay untouched.
  for (const [schema, value] of [
    [{ oneOf: [emptyObject] }, {}],
    [{ oneOf: [{ type: ["object", "null"], default: {} }] }, null],
    [{ oneOf: [emptyObject] }, { keep: true }],
    [{ oneOf: [emptyObject, { type: "string" }] }, undefined],
    [{ oneOf: [{ type: "string" }] }, undefined],
    [emptyObject, undefined],
  ]) {
    const result = await page.evaluate(async ([schema, value]) => {
      return window.renderSchemaField(schema, value);
    }, [schema, value]);
    assert.deepEqual(result.emitted, []);
  }

  for (const [branch, expected] of [
    [{ type: ["object", "null"], default: null }, null],
    [{ type: "object", default: { preset: true } }, { preset: true }],
    [{ type: "string", default: "default" }, "default"],
    [{ const: false }, false],
    [{ const: null }, null],
  ]) {
    const result = await page.evaluate(async branch => {
      return window.renderSchemaField({ oneOf: [branch] });
    }, branch);
    assert.deepEqual(result.emitted, [{ path: [], value: expected }]);
  }
  assert.deepEqual(errors, []);
  console.log("SchemaField mount regression passed (no select changes)");
} finally {
  await browser?.close();
  await server.close();
}
