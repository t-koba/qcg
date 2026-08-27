import type { InputField } from "./api/client";

export type FieldKind = "string" | "text" | "number" | "boolean" | "select" | "multiselect" | "list" | "file" | "json" | "natural_language" | "custom";

export function fieldKind(field: InputField): FieldKind {
  if (typeof field.type !== "string") return "custom";
  const kinds: Record<string, FieldKind> = {
    string: "string",
    text: "text",
    number: "number",
    boolean: "boolean",
    select: "select",
    multiselect: "multiselect",
    list: "list",
    file: "file",
    json: "json",
    natural_language: "natural_language",
  };
  return kinds[field.type] || "custom";
}

export function normalizeList(value: string): string[] {
  return value.split("\n").map((item) => item.trim()).filter(Boolean);
}

export function parseJsonValue(text: string): unknown {
  return JSON.parse(text);
}

export function validateField(field: InputField, value: unknown): string {
  if (field.required && (value === undefined || value === null || value === "" || (Array.isArray(value) && value.length === 0))) {
    return `${field.id} is required`;
  }
  if (field.pattern && typeof value === "string" && !new RegExp(field.pattern).test(value)) {
    return `${field.id} does not match ${field.pattern}`;
  }
  if (field.type === "json" && typeof value === "string") {
    try {
      JSON.parse(value);
    } catch {
      return `${field.id} is not valid JSON`;
    }
  }
  if (field.min_items !== null && Array.isArray(value) && value.length < field.min_items) {
    return `${field.id} requires at least ${field.min_items} item(s)`;
  }
  return "";
}
