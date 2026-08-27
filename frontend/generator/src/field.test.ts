import { describe, expect, it } from "vitest";
import type { InputField } from "./api/client";
import { fieldKind, normalizeList, validateField } from "./field";

function field(overrides: Partial<InputField> = {}): InputField {
  return { id: "value", type: "string", required: false, default: null, pattern: null, options: [], min_items: null, item_type: null, ...overrides };
}

describe("field contracts", () => {
  it("maps serialized field kinds without case guessing", () => {
    expect(fieldKind(field({ type: "natural_language" }))).toBe("natural_language");
    expect(fieldKind(field({ type: "json" }))).toBe("json");
    expect(fieldKind(field({ type: "vendor.custom" }))).toBe("custom");
  });

  it("normalizes list input", () => {
    expect(normalizeList(" alpha\n\n beta ")).toEqual(["alpha", "beta"]);
  });

  it("validates pattern and minimum items", () => {
    expect(validateField(field({ pattern: "^[a-z]+$" }), "ABC")).toContain("does not match");
    expect(validateField(field({ min_items: 2 }), ["one"])).toContain("at least 2");
  });

  it("validates JSON field content", () => {
    expect(validateField(field({ type: "json" }), "{not json")).toContain("not valid JSON");
    expect(validateField(field({ type: "json" }), "{\"a\": 1}")).toBe("");
    expect(validateField(field({ type: "json", required: true }), "")).toContain("required");
  });
});
