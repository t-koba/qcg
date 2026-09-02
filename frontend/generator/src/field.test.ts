import { describe, expect, it } from "vitest";
import type { InputField } from "./api/client";
import { encodeBase64, fieldKind, fieldSchema, isBlockingSchemaIssue, isSafeFileName, MAX_SCHEMA_NODES, normalizeList, removePath, resolveSchemaReferences, schemaKind, schemaNodeCount, setPath, validateField, validateInputField, validateSchemaValue, valueNodeCountExceeded } from "./field";

function field(overrides: Partial<InputField> = {}): InputField {
  return { id: "value", label: null, label_i18n: {}, type: "string", required: false, default: null, pattern: null, options: [], option_labels_i18n: {}, min_items: null, item_type: null, ...overrides };
}

describe("field contracts", () => {
  it("maps serialized field kinds without case guessing", () => {
    expect(fieldKind(field({ type: "natural_language" }))).toBe("natural_language");
    expect(fieldKind(field({ type: "json" }))).toBe("json");
    expect(fieldKind(field({ type: "vendor.custom" }))).toBe("custom");
    expect(fieldKind(field({ type: "geo.point", schema: { type: "object" } }))).toBe("json");
  });

  it("normalizes list input", () => {
    expect(normalizeList(" alpha\n\n beta ")).toEqual(["alpha", "beta"]);
  });

  it("leaves regular-expression constraints to the server and validates minimum items", () => {
    expect(validateField(field({ pattern: "^[a-z]+$" }), "ABC")).toBe("");
    expect(validateField(field({ min_items: 2 }), ["one"])).toContain("at least 2");
  });

  it("validates JSON field content", () => {
    expect(validateField(field({ type: "json" }), "{not json")).toContain("not valid JSON");
    expect(validateField(field({ type: "json" }), "{\"a\": 1}")).toBe("");
    expect(validateField(field({ type: "json", required: true }), "")).toContain("required");
  });

  it("keeps arbitrary custom fields schema-driven", () => {
    const custom = field({
      type: "vendor.settings",
      schema: {
        type: "object",
        required: ["name"],
        properties: { name: { type: "string", minLength: 2 }, retries: { type: "integer", minimum: 0 } },
      },
    });
    expect(fieldSchema(custom)).toMatchObject({ explicit: true, custom: true, schema: { type: "object" } });
    expect(validateSchemaValue(fieldSchema(custom).schema, { name: "", retries: -1 }).map((issue) => issue.path)).toEqual([["name"], ["retries"]]);
  });

  it("keeps an unconstrained schema on the JSON editor fallback", () => {
    expect(schemaKind({}, { nested: ["value"] }, "vendor.untyped")).toBe("json");
    expect(schemaKind({ type: ["string", "null"] })).toBe("union");
  });

  it("counts the complete schema tree for the renderer limit", () => {
    expect(schemaNodeCount({ type: "object", properties: { value: { type: "string" } } })).toBeGreaterThan(3);
  });

  it("bounds hostile schema scans before reference expansion", () => {
    const schema = {
      type: "object",
      properties: Object.fromEntries(Array.from({ length: MAX_SCHEMA_NODES * 4 }, (_, index) => [`field-${index}`, { type: "string" }])),
    };
    expect(schemaNodeCount(schema)).toBe(MAX_SCHEMA_NODES + 1);
    expect(resolveSchemaReferences(schema)).toBe(schema);
    expect(fieldSchema(field({ type: "vendor.large", schema })).schema).toBe(schema);
  });

  it("honors boolean subschemas, tuple tails, property names, and not", () => {
    const schema = {
      type: "object",
      properties: {
        tuple: { type: "array", prefixItems: [{ type: "string" }], items: false },
        key: { type: "string" },
      },
      propertyNames: { pattern: "^[a-z]+$" },
      not: { required: ["forbidden"] },
    };
    const issues = validateSchemaValue(schema, { "Bad-Key": "x", forbidden: true, tuple: ["ok", 1] });
    expect(issues.some((issue) => issue.keyword === "items")).toBe(true);
    expect(issues.some((issue) => issue.keyword === "pattern")).toBe(false);
    expect(issues.some((issue) => issue.keyword === "not")).toBe(true);
    expect(validateSchemaValue({ type: "array", prefixItems: [true], items: false }, ["ok"])).toEqual([]);
    expect(validateSchemaValue({ type: "array", contains: { const: "ok" }, minContains: 1 }, ["nope", "ok"])).toEqual([]);
    expect(validateSchemaValue({ type: "array", contains: { const: "ok" } }, ["nope"]).some((issue) => issue.keyword === "contains")).toBe(true);
  });

  it("preserves boolean root schemas without treating false as unconstrained", () => {
    const custom = field({ type: "vendor.always", schema: false });
    expect(fieldSchema(custom).explicit).toBe(true);
    expect(validateSchemaValue(fieldSchema(custom).schema, "value").some((issue) => issue.keyword === "falseSchema")).toBe(true);
    expect(fieldSchema(field({ schema: true })).schema.type).toBe("string");
  });

  it("treats an explicit null as present for required fields", () => {
    expect(validateInputField(field({ type: "json", required: true }), null)).toEqual([]);
  });

  it("resolves local references without evaluating package code", () => {
    const schema = {
      $defs: { port: { type: "integer", minimum: 1 } },
      type: "object",
      properties: { port: { $ref: "#/$defs/port" } },
    };
    const resolved = resolveSchemaReferences(schema);
    expect((resolved.properties as Record<string, Record<string, unknown>>).port.minimum).toBe(1);
    expect(validateSchemaValue(resolved, { port: 0 }).some((issue) => issue.keyword === "minimum")).toBe(true);
  });

  it("keeps the bounded validation notice non-blocking for large valid values", () => {
    const issues = validateSchemaValue({ type: "array", items: { type: "integer" } }, Array.from({ length: 300 }, (_, index) => index));
    expect(issues.some((issue) => issue.keyword === "limit")).toBe(true);
    expect(issues.filter(isBlockingSchemaIssue)).toEqual([]);
  });

  it("bounds value traversal before unique and contains checks", () => {
    const value = Array.from({ length: 300 }, (_, index) => ({ index, nested: [index] }));
    expect(valueNodeCountExceeded(value)).toBe(true);
    expect(valueNodeCountExceeded(Object.fromEntries(Array.from({ length: MAX_SCHEMA_NODES * 4 }, (_, index) => [`key-${index}`, index])))).toBe(true);
    const issues = validateSchemaValue({ type: "array", uniqueItems: true, contains: { type: "object" } }, value);
    expect(issues).toEqual([{ path: [], keyword: "limit", message: "schema validation limit exceeded" }]);
  });

  it("does not execute or block on untrusted regular expressions", () => {
    expect(validateSchemaValue({ type: "string", pattern: "[unterminated" }, "value")).toEqual([]);
    expect(validateSchemaValue({
      type: "object",
      patternProperties: { "^(a+)+$": { type: "number" } },
      additionalProperties: false,
    }, { "a-key": "value" })).toEqual([]);
    expect(isBlockingSchemaIssue({ path: [], keyword: "server", message: "server validation" })).toBe(false);
  });

  it("updates nested object and array paths immutably", () => {
    const original = { service: { ports: [8080, 8081] } };
    const changed = setPath(original, ["service", "ports", 1], 9090);
    expect(changed).toEqual({ service: { ports: [8080, 9090] } });
    expect(original).toEqual({ service: { ports: [8080, 8081] } });
    expect(removePath(changed, ["service", "ports", 0])).toEqual({ service: { ports: [9090] } });
  });

  it("validates oneOf and format constraints without executing schema content", () => {
    const schema = {
      oneOf: [
        { type: "object", required: ["kind"], properties: { kind: { const: "local" }, path: { type: "string" } } },
        { type: "object", required: ["kind"], properties: { kind: { const: "remote" }, url: { type: "string", format: "uri" } } },
      ],
    };
    expect(validateSchemaValue(schema, { kind: "remote", url: "not-a-url" }).some((issue) => issue.keyword === "format")).toBe(true);
    expect(validateSchemaValue(schema, { kind: "other" })[0]?.keyword).toBe("oneOf");
  });

  it("requires absolute uri and url values while allowing uri references", () => {
    const uriSchema = { type: "string", format: "uri" };
    const urlSchema = { type: "string", format: "url" };
    const referenceSchema = { type: "string", format: "uri-reference" };
    expect(validateSchemaValue(uriSchema, "/relative").some((issue) => issue.keyword === "format")).toBe(true);
    expect(validateSchemaValue(urlSchema, "//host/path").some((issue) => issue.keyword === "format")).toBe(true);
    expect(validateSchemaValue(uriSchema, "https://example.test/path")).toEqual([]);
    expect(validateSchemaValue(referenceSchema, "./relative")).toEqual([]);
  });

  it("shares file safety rules with nested and top-level controls", () => {
    expect(isSafeFileName("config.json")).toBe(true);
    expect(isSafeFileName("../config.json")).toBe(false);
    expect(isSafeFileName("config\\secret.json")).toBe(false);
    expect(isSafeFileName("C:config.json")).toBe(false);
    expect(encodeBase64(new Uint8Array([0, 1, 2, 255]))).toBe("AAEC/w==");
  });

  it("validates canonical file values as FileValue objects", () => {
    const fileField = field({ type: "file", required: true });
    expect(validateInputField(fileField, { name: "config.json", content_base64: "eA==" })).toEqual([]);
    expect(validateInputField(fileField, { name: "../config.json", content_base64: "eA==" }).some((issue) => issue.keyword === "pattern")).toBe(false);
    expect(validateInputField(fileField, { name: "config.json" }).some((issue) => issue.keyword === "oneOf")).toBe(true);
  });
});
