import { render } from "svelte/server";
import { describe, expect, it } from "vitest";
import SchemaField from "./SchemaField.svelte";

describe("SchemaField", () => {
  it("renders nested object, array, and union controls from one custom schema", () => {
    const rendered = render(SchemaField, {
      props: {
        schema: {
          type: "object",
          required: ["name", "ports", "mode"],
          properties: {
            name: { type: "string", title: "Name" },
            ports: { type: "array", title: "Ports", items: { type: "integer" } },
            mode: { oneOf: [{ const: "local", title: "Local" }, { const: "remote", title: "Remote" }] },
          },
        },
        value: { name: "demo", ports: [8080], mode: "local" },
        idPrefix: "schema-test",
        ui: { preview: true },
        language: "en",
        root: true,
        explicitSchema: true,
        onValue: () => undefined,
        onFile: async () => undefined,
      },
    });

    expect(rendered.body).toContain("Name");
    expect(rendered.body).toContain("Add item");
    expect(rendered.body).toContain("Local");
    expect(rendered.body).toContain("Input preview");
    expect(rendered.body.match(/id="schema-test-mode"/g)).toHaveLength(1);
  });

  it("reports missing required children before an object value exists", () => {
    const rendered = render(SchemaField, {
      props: {
        schema: {
          type: "object",
          required: ["name"],
          properties: { name: { type: "string", title: "Name" } },
        },
        value: undefined,
        idPrefix: "schema-empty",
        language: "en",
        required: true,
        root: true,
        explicitSchema: true,
        onValue: () => undefined,
        onFile: async () => undefined,
      },
    });

    expect(rendered.body).toContain("name: is required");
  });
});
