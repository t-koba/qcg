import type { InputField } from "./api/client";
import { localizedText } from "./format";

export type FieldKind = "string" | "text" | "number" | "boolean" | "select" | "multiselect" | "list" | "file" | "json" | "natural_language" | "custom";
export type JsonObject = Record<string, unknown>;
export type JsonSchema = JsonObject;
export type PathSegment = string | number;
export type SchemaIssue = {
  path: PathSegment[];
  keyword: string;
  message: string;
};

export function isBlockingSchemaIssue(issue: SchemaIssue): boolean {
  return issue.keyword !== "limit" && issue.keyword !== "server";
}

export const MAX_SCHEMA_DEPTH = 12;
export const MAX_SCHEMA_NODES = 256;
export const MAX_FILE_INPUT_BYTES = 16 * 1024 * 1024;
const FALSE_SCHEMA_MARKER = "__qcg_false_schema";

const BUILTIN_KINDS = new Set([
  "string", "text", "number", "boolean", "select", "multiselect", "list", "file", "json", "natural_language",
]);
const INPUT_TYPES = new Set([
  "color", "date", "datetime-local", "email", "month", "password", "range", "search", "tel", "text", "time", "url", "week",
]);
const SCHEMA_CHILD_OBJECT_KEYS = new Set(["properties", "patternProperties", "dependentSchemas", "$defs", "definitions"]);
const SCHEMA_CHILD_ARRAY_KEYS = new Set(["prefixItems", "oneOf", "anyOf", "allOf"]);
const SCHEMA_CHILD_SINGLE_KEYS = new Set([
  "additionalProperties", "contains", "contentSchema", "else", "if", "items", "not", "propertyNames", "then", "unevaluatedItems", "unevaluatedProperties",
]);

export function isSafeFileName(name: string): boolean {
  return name.length > 0
    && name !== "."
    && name !== ".."
    && !(name.length >= 2 && /^[A-Za-z]:/.test(name))
    && !name.includes("/")
    && !name.includes("\\")
    && !name.includes("\0");
}

export function validateFileInput(file: File | undefined): void {
  if (!file) return;
  if (file.size > MAX_FILE_INPUT_BYTES) {
    throw new Error(`file input exceeds the ${MAX_FILE_INPUT_BYTES} byte limit`);
  }
  if (!isSafeFileName(file.name)) {
    throw new Error(`file name must be one safe path component: ${file.name}`);
  }
}

export function encodeBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

export function asJsonObject(value: unknown): JsonObject | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as JsonObject : undefined;
}

/** Normalize a JSON Schema boolean into the object form used by the renderer. */
export function normalizeSchema(value: unknown): JsonSchema {
  if (value === false) return { [FALSE_SCHEMA_MARKER]: true };
  return asJsonObject(value) || {};
}

export function asStringMap(value: unknown): Record<string, string> {
  const object = asJsonObject(value);
  if (!object) return {};
  return Object.fromEntries(Object.entries(object).filter((entry): entry is [string, string] => typeof entry[1] === "string"));
}

export function asUi(field: Pick<InputField, "ui"> | JsonObject | undefined): JsonObject {
  if (!field) return {};
  return asJsonObject("ui" in field ? field.ui : field) || {};
}

export function fieldKind(field: InputField): FieldKind {
  const ui = asUi(field);
  if (typeof ui.widget === "string" && [
    "string", "text", "number", "boolean", "natural_language", "json", "select", "multiselect", "list", "file",
  ].includes(ui.widget)) {
    return ui.widget as FieldKind;
  }
  const schema = asJsonObject(field.schema);
  if (Array.isArray(schema?.enum)) return schema.type === "array" ? "multiselect" : "select";
  if (typeof field.type === "string" && !BUILTIN_KINDS.has(field.type)) {
    if (schema?.type === "object" || schema?.type === "array") return "json";
    if (schema?.type === "number" || schema?.type === "integer") return "number";
    if (schema?.type === "boolean") return "boolean";
  }
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

export type FieldSchema = {
  schema: JsonSchema;
  explicit: boolean;
  custom: boolean;
};

/** Build the renderer schema without executing any package-provided code. */
export function fieldSchema(field: InputField): FieldSchema {
  const explicitSchema = asJsonObject(field.schema);
  const kind = fieldKind(field);
  const custom = typeof field.type !== "string" || !BUILTIN_KINDS.has(field.type);
  const finish = (schema: JsonSchema, explicit: boolean): FieldSchema => ({
    schema: applyCanonicalFieldConstraints(field, schema),
    explicit,
    custom,
  });
  if (field.schema === true) {
    return finish(custom ? {} : inferredSchemaForKind(kind, field), true);
  }
  if (field.schema === false) {
    return finish(custom ? { [FALSE_SCHEMA_MARKER]: true } : { ...inferredSchemaForKind(kind, field), [FALSE_SCHEMA_MARKER]: true }, true);
  }
  if (explicitSchema) {
    const schema = schemaNodeCount(explicitSchema) > MAX_SCHEMA_NODES
      ? explicitSchema
      : resolveSchemaReferences({ ...explicitSchema });
    if (schema.type === undefined && !custom) {
      const inferred = inferredSchemaForKind(kind, field);
      return finish({ ...inferred, ...schema }, true);
    }
    return finish(schema, true);
  }
  return finish(inferredSchemaForKind(kind, field), false);
}

/** Apply canonical field constraints that are not represented by a JSON Schema keyword at the root. */
function applyCanonicalFieldConstraints(field: InputField, schema: JsonSchema): JsonSchema {
  if (field.type !== "file" || !field.pattern) return schema;
  const properties = asJsonObject(schema.properties);
  const name = asJsonObject(properties?.name);
  if (!properties || !name || name.pattern === field.pattern) return schema;
  const allOf = Array.isArray(name.allOf) ? name.allOf : [];
  return {
    ...schema,
    properties: {
      ...properties,
      name: { ...name, allOf: [...allOf, { pattern: field.pattern }] },
    },
  };
}

function inferredSchemaForKind(kind: FieldKind, field: InputField): JsonSchema {
  switch (kind) {
    case "number": return { type: "number" };
    case "boolean": return { type: "boolean" };
    case "select": return { type: "string", enum: field.options };
    case "multiselect": return { type: "array", items: { type: "string", enum: field.options } };
    case "list": return { type: "array", items: { type: field.item_type === "number" ? "number" : "string" } };
    case "file": return {
      type: "object",
      additionalProperties: false,
      required: ["name"],
      properties: {
        name: { type: "string", minLength: 1, pattern: "^(?!\\.\\.?$)(?![A-Za-z]:)[^/\\\\\\u0000]+$" },
        text: { type: "string" },
        content_base64: { type: "string" },
      },
      oneOf: [
        { required: ["text"], not: { required: ["content_base64"] } },
        { required: ["content_base64"], not: { required: ["text"] } },
      ],
    };
    case "json": return {};
    case "string":
    case "text":
    case "natural_language": return { type: "string" };
    case "custom": return {};
  }
}

export function schemaTypes(schema: JsonSchema, _value?: unknown): string[] {
  const type = schema.type;
  if (Array.isArray(type)) return type.filter((entry): entry is string => typeof entry === "string");
  if (typeof type === "string") return [type];
  if (Array.isArray(schema.oneOf) || Array.isArray(schema.anyOf)) return [];
  if (Array.isArray(schema.allOf)) {
    const types = schema.allOf.flatMap((branch) => {
      const object = asJsonObject(branch);
      return object ? schemaTypes(object) : [];
    });
    if (types.includes("object")) return ["object"];
    if (types.includes("array")) return ["array"];
    if (types.length > 0) return [types[0]];
  }
  if (asJsonObject(schema.properties) || asJsonObject(schema.patternProperties) || schema.required !== undefined || schema.additionalProperties !== undefined || schema.propertyNames !== undefined || schema.minProperties !== undefined || schema.maxProperties !== undefined || schema.dependentRequired !== undefined || schema.dependentSchemas !== undefined) return ["object"];
  if (schema.items !== undefined || schema.prefixItems !== undefined || schema.contains !== undefined || schema.minItems !== undefined || schema.maxItems !== undefined || schema.uniqueItems !== undefined || schema.minContains !== undefined || schema.maxContains !== undefined) return ["array"];
  return [];
}

export function schemaKind(schema: JsonSchema, value?: unknown, canonicalType?: string): "object" | "array" | "boolean" | "number" | "integer" | "string" | "null" | "enum" | "union" | "json" {
  if (Array.isArray(schema.oneOf) || Array.isArray(schema.anyOf)) return "union";
  if (Array.isArray(schema.enum) || schema.const !== undefined) return "enum";
  if (canonicalType === "file") return "string";
  const types = schemaTypes(schema, value);
  if (types.length > 1) return "union";
  const nonNull = types.find((type) => type !== "null");
  if (nonNull === "object" || nonNull === "array" || nonNull === "boolean" || nonNull === "number" || nonNull === "integer" || nonNull === "string") {
    return nonNull;
  }
  if (types.includes("null")) return "null";
  return "json";
}

export function schemaBranches(schema: JsonSchema): JsonSchema[] {
  const combinator = Array.isArray(schema.oneOf) ? schema.oneOf : Array.isArray(schema.anyOf) ? schema.anyOf : undefined;
  if (combinator) return combinator.map(normalizeSchema);
  if (Array.isArray(schema.type) && schema.type.length > 1) {
    return schema.type
      .filter((type): type is string => typeof type === "string")
      .map((type) => ({ ...schema, type }));
  }
  return [];
}

export function optionValues(schema: JsonSchema, field?: InputField): unknown[] {
  if (Array.isArray(schema.enum)) return schema.enum;
  if (schema.const !== undefined) return [schema.const];
  if (field && field.options.length > 0) return field.options;
  return [];
}

export function optionValueKey(value: unknown): string {
  if (typeof value === "string") return `s:${value}`;
  try {
    return `j:${JSON.stringify(value)}`;
  } catch {
    return `t:${String(value)}`;
  }
}

export function optionLabel(
  value: unknown,
  ui: JsonObject,
  language: string,
  field?: InputField,
): string {
  const raw = value === null ? "null" : typeof value === "string" ? value : stringifyValue(value);
  const fieldLabels = field?.option_labels_i18n || {};
  const fieldTranslations = Object.fromEntries(Object.entries(fieldLabels).flatMap(([locale, labels]) => {
    const label = labels?.[String(value)];
    return label ? [[locale, label]] : [];
  }));
  if (Object.keys(fieldTranslations).length > 0) return localizedText(raw, fieldTranslations, language);
  const labels = asJsonObject(ui.enum_labels_i18n || ui.option_labels_i18n);
  if (labels) {
    const translations = Object.fromEntries(Object.entries(labels).flatMap(([locale, entries]) => {
      const label = asJsonObject(entries)?.[String(value)];
      return typeof label === "string" ? [[locale, label]] : [];
    }));
    if (Object.keys(translations).length > 0) return localizedText(raw, translations, language);
  }
  const direct = asStringMap(ui.enum_labels);
  return direct[String(value)] || raw;
}

export function schemaText(
  schema: JsonSchema,
  ui: JsonObject,
  key: "title" | "description" | "placeholder",
  fallback: string,
  language: string,
): string {
  const direct = typeof ui[key] === "string" ? ui[key] as string : typeof schema[key] === "string" ? schema[key] as string : fallback;
  const translations = asStringMap(ui[`${key}_i18n`]);
  const schemaTranslations = asStringMap(schema[`${key}_i18n`]);
  return localizedText(direct, { ...schemaTranslations, ...translations }, language);
}

export function schemaBoolean(ui: JsonObject, key: string, fallback = false): boolean {
  return typeof ui[key] === "boolean" ? ui[key] as boolean : fallback;
}

export function schemaNumber(ui: JsonObject, schema: JsonObject, uiKey: string, schemaKey: string): number | undefined {
  const value = ui[uiKey] ?? schema[schemaKey];
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export function schemaInputType(schema: JsonSchema, ui: JsonObject, canonicalType?: string): string {
  if (canonicalType === "file" || ui.widget === "file" || schema.contentEncoding === "binary" || schema.contentEncoding === "base64") return "file";
  const format = typeof ui.format === "string" ? ui.format : schema.format;
  if (typeof ui.input_type === "string" && INPUT_TYPES.has(ui.input_type) && !(format === "uri-reference" && ui.input_type === "url")) return ui.input_type;
  if (format === "email" || format === "uri" || format === "url" || format === "date" || format === "date-time" || format === "time" || format === "month" || format === "week") {
    if (format === "url") return "url";
    if (format === "uri") return "text";
    return format === "date-time" ? "datetime-local" : format;
  }
  return "text";
}

export function schemaOrder(schema: JsonSchema, ui: JsonObject): string[] {
  const order = ui.order ?? ui.property_order ?? schema.propertyOrder;
  if (Array.isArray(order)) return order.filter((value): value is string => typeof value === "string");
  return [];
}

export function schemaDefault(schema: JsonSchema): unknown {
  if (schema.default !== undefined) return schema.default;
  if (schema.const !== undefined) return schema.const;
  return undefined;
}

/** Count JSON values up to the renderer budget so a hostile schema cannot force a full scan. */
export function schemaNodeCount(schema: JsonSchema, max = MAX_SCHEMA_NODES): number {
  const stack: unknown[] = [schema];
  const seen = new Set<object>();
  let count = 0;
  while (stack.length > 0) {
    const value = stack.pop();
    count += 1;
    if (count > max) return count;
    if (value === null || typeof value !== "object") continue;
    if (seen.has(value)) continue;
    seen.add(value);
    if (Array.isArray(value)) {
      if (value.length > max) return max + 1;
      for (let index = value.length - 1; index >= 0; index -= 1) stack.push(value[index]);
      continue;
    }
    const object = value as Record<string, unknown>;
    let properties = 0;
    for (const key in object) {
      if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
      properties += 1;
      if (properties > max) return max + 1;
      stack.push(object[key]);
    }
  }
  return count;
}

/**
 * Check the value budget before applying any potentially expensive keyword.
 * This is deliberately iterative so untrusted JSON cannot overflow the call stack.
 */
export function valueNodeCountExceeded(value: unknown, max = MAX_SCHEMA_NODES): boolean {
  const stack: unknown[] = [value];
  const seen = new Set<object>();
  let count = 0;
  while (stack.length > 0) {
    const current = stack.pop();
    count += 1;
    if (count > max) return true;
    if (current === null || typeof current !== "object") continue;
    if (seen.has(current)) continue;
    seen.add(current);
    if (Array.isArray(current)) {
      if (current.length > max) return true;
      for (let index = current.length - 1; index >= 0; index -= 1) stack.push(current[index]);
    } else {
      const object = current as Record<string, unknown>;
      let properties = 0;
      for (const key in object) {
        if (!Object.prototype.hasOwnProperty.call(object, key)) continue;
        properties += 1;
        if (properties > max) return true;
        stack.push(object[key]);
      }
    }
  }
  return false;
}

/** Resolve local JSON Schema references for display while leaving external refs untouched. */
export function resolveSchemaReferences(schema: JsonSchema): JsonSchema {
  if (schemaNodeCount(schema) > MAX_SCHEMA_NODES) return schema;
  const budget = { count: 0 };

  function resolve(value: unknown, refs: readonly string[], depth: number): unknown {
    budget.count += 1;
    if (budget.count > MAX_SCHEMA_NODES || depth > MAX_SCHEMA_DEPTH) return value;
    if (Array.isArray(value)) return value.map((entry) => resolve(entry, refs, depth + 1));
    const object = asJsonObject(value);
    if (!object) return value;
    let resolved: JsonObject = object;
    const reference = object.$ref;
    if (typeof reference === "string" && reference.startsWith("#/") && reference.length <= 4096 && !refs.includes(reference)) {
      const target = schemaPointer(schema, reference);
      const targetObject = asJsonObject(target);
      if (targetObject) {
        resolved = { ...resolve(targetObject, [...refs, reference], depth + 1) as JsonObject, ...object };
        delete resolved.$ref;
      }
    }
    const result = { ...resolved };
    for (const key of SCHEMA_CHILD_OBJECT_KEYS) {
      const entries = asJsonObject(result[key]);
      if (entries) result[key] = Object.fromEntries(Object.entries(entries).map(([name, child]) => [name, resolve(child, refs, depth + 1)]));
    }
    for (const key of SCHEMA_CHILD_ARRAY_KEYS) {
      if (Array.isArray(result[key])) result[key] = result[key].map((child) => resolve(child, refs, depth + 1));
    }
    for (const key of SCHEMA_CHILD_SINGLE_KEYS) {
      if (key in result) result[key] = resolve(result[key], refs, depth + 1);
    }
    return result;
  }

  return asJsonObject(resolve(schema, [], 0)) || {};
}

function schemaPointer(schema: JsonSchema, reference: string): unknown {
  let current: unknown = schema;
  const segments = reference.slice(2).split("/");
  if (segments.length > MAX_SCHEMA_DEPTH) return undefined;
  for (const segment of segments) {
    const key = segment.replaceAll("~1", "/").replaceAll("~0", "~");
    if (Array.isArray(current) && /^\d+$/.test(key)) current = current[Number(key)];
    else if (asJsonObject(current)) current = asJsonObject(current)?.[key];
    else return undefined;
  }
  return current;
}

export function readPath(value: unknown, path: readonly PathSegment[]): unknown {
  let current = value;
  for (const segment of path) {
    if (Array.isArray(current) && typeof segment === "number") current = current[segment];
    else if (asJsonObject(current) && typeof segment === "string") current = asJsonObject(current)?.[segment];
    else return undefined;
  }
  return current;
}

export function setPath(value: unknown, path: readonly PathSegment[], next: unknown): unknown {
  if (path.length === 0) return next;
  const [head, ...tail] = path;
  const source = Array.isArray(value) ? value.slice() : asJsonObject(value) ? { ...asJsonObject(value) } : typeof head === "number" ? [] : {};
  if (Array.isArray(source) && typeof head === "number") source[head] = setPath(source[head], tail, next);
  else if (!Array.isArray(source) && typeof head === "string") source[head] = setPath(source[head], tail, next);
  return source;
}

export function removePath(value: unknown, path: readonly PathSegment[]): unknown {
  if (path.length === 0) return undefined;
  const [head, ...tail] = path;
  if (Array.isArray(value) && typeof head === "number") {
    const copy = value.slice();
    if (tail.length === 0) copy.splice(head, 1);
    else copy[head] = removePath(copy[head], tail);
    return copy;
  }
  const object = asJsonObject(value);
  if (!object || typeof head !== "string") return value;
  const copy = { ...object };
  if (tail.length === 0) delete copy[head];
  else copy[head] = removePath(copy[head], tail);
  return copy;
}

export function pathString(path: readonly PathSegment[]): string {
  return path.reduce<string>((result, segment) => typeof segment === "number" ? `${result}[${segment}]` : result ? `${result}.${segment}` : String(segment), "");
}

export function stringifyValue(value: unknown): string {
  if (value === undefined || value === null) return "";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) || "";
  } catch {
    return String(value);
  }
}

export function normalizeList(value: string): string[] {
  return value.split("\n").map((item) => item.trim()).filter(Boolean);
}

export function parseJsonValue(text: string): unknown {
  return JSON.parse(text);
}

export function validateInputField(field: InputField, value: unknown): SchemaIssue[] {
  const fieldData = fieldSchema(field);
  const schema = { ...fieldData.schema };
  if (field.pattern && schema.pattern === undefined) schema.pattern = field.pattern;
  if (field.min_items !== null && schema.minItems === undefined) schema.minItems = field.min_items;
  const issues = validateSchemaValue(schema, value).filter((issue) => !(field.min_items !== null && Array.isArray(value) && issue.keyword === "type"));
  if (field.required && isEmptyValue(value)) {
    issues.unshift({ path: [], keyword: "required", message: "is required" });
  }
  if (field.type === "json" && typeof value === "string") {
    try {
      JSON.parse(value);
    } catch {
      issues.push({ path: [], keyword: "json", message: "is not valid JSON" });
    }
  }
  if (field.min_items !== null && Array.isArray(value) && value.length < field.min_items) {
    issues.push({ path: [], keyword: "minItems", message: `requires at least ${field.min_items} item(s)` });
  }
  return dedupeIssues(issues);
}

export function validateField(field: InputField, value: unknown): string {
  const issue = validateInputField(field, value)[0];
  return issue ? `${field.id} ${issue.message}` : "";
}

type ValidationBudget = { count: number; valueChecked?: boolean };

export function validateSchemaValue(schema: JsonSchema, value: unknown, path: PathSegment[] = [], depth = 0, nodes: ValidationBudget = { count: 0 }): SchemaIssue[] {
  if (value === undefined) return [];
  if (depth === 0 && !nodes.valueChecked) {
    nodes.valueChecked = true;
    if (valueNodeCountExceeded(value)) return [{ path, keyword: "limit", message: "schema validation limit exceeded" }];
  }
  if (depth > MAX_SCHEMA_DEPTH || nodes.count++ >= MAX_SCHEMA_NODES) return [{ path, keyword: "limit", message: "schema validation limit exceeded" }];
  if (schema[FALSE_SCHEMA_MARKER] === true) return [{ path, keyword: "falseSchema", message: "must not be present" }];
  const issues: SchemaIssue[] = [];
  const types = schemaTypes(schema, value);
  if (types.length > 0 && !types.some((type) => type === "null" && value === null || type !== "null" && matchesType(type, value))) {
    issues.push({ path, keyword: "type", message: `must be ${types.join(" or ")}` });
    return issues;
  }
  if (Array.isArray(schema.enum) && !schema.enum.some((candidate) => deepEqual(candidate, value))) issues.push({ path, keyword: "enum", message: "must be one of the available options" });
  if (schema.const !== undefined && !deepEqual(schema.const, value)) issues.push({ path, keyword: "const", message: `must equal ${stringifyValue(schema.const)}` });

  if (typeof value === "string") {
    const length = [...value].length;
    if (typeof schema.minLength === "number" && length < schema.minLength) issues.push({ path, keyword: "minLength", message: `must contain at least ${schema.minLength} characters` });
    if (typeof schema.maxLength === "number" && length > schema.maxLength) issues.push({ path, keyword: "maxLength", message: `must contain at most ${schema.maxLength} characters` });
    if (typeof schema.format === "string" && !matchesFormat(schema.format, value)) issues.push({ path, keyword: "format", message: `must be a valid ${schema.format}` });
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    if (typeof schema.minimum === "number" && value < schema.minimum) issues.push({ path, keyword: "minimum", message: `must be at least ${schema.minimum}` });
    if (typeof schema.maximum === "number" && value > schema.maximum) issues.push({ path, keyword: "maximum", message: `must be at most ${schema.maximum}` });
    if (typeof schema.exclusiveMinimum === "number" && value <= schema.exclusiveMinimum) issues.push({ path, keyword: "exclusiveMinimum", message: `must be greater than ${schema.exclusiveMinimum}` });
    if (typeof schema.exclusiveMaximum === "number" && value >= schema.exclusiveMaximum) issues.push({ path, keyword: "exclusiveMaximum", message: `must be less than ${schema.exclusiveMaximum}` });
    if (typeof schema.multipleOf === "number" && schema.multipleOf > 0 && !isMultipleOf(value, schema.multipleOf)) issues.push({ path, keyword: "multipleOf", message: `must be a multiple of ${schema.multipleOf}` });
  }
  if (Array.isArray(value)) {
    if (typeof schema.minItems === "number" && value.length < schema.minItems) issues.push({ path, keyword: "minItems", message: `requires at least ${schema.minItems} item(s)` });
    if (typeof schema.maxItems === "number" && value.length > schema.maxItems) issues.push({ path, keyword: "maxItems", message: `allows at most ${schema.maxItems} item(s)` });
    if (schema.uniqueItems === true) {
      const seenItems = new Set<string>();
      let duplicate = false;
      let comparable = true;
      for (const item of value) {
        const key = stableValueKey(item);
        if (key === undefined) {
          comparable = false;
          break;
        }
        if (seenItems.has(key)) {
          duplicate = true;
          break;
        }
        seenItems.add(key);
      }
      if (comparable && duplicate) issues.push({ path, keyword: "uniqueItems", message: "must contain unique items" });
    }
    const prefixItems = Array.isArray(schema.prefixItems) ? schema.prefixItems : [];
    for (const [index, item] of value.entries()) {
      const itemPath = [...path, index];
      if (index < prefixItems.length) {
        issues.push(...validateSubschema(prefixItems[index], item, itemPath, depth + 1, nodes));
      } else if (schema.items === false) {
        issues.push({ path: itemPath, keyword: "items", message: "must not contain additional items" });
      } else if (schema.items !== undefined) {
        issues.push(...validateSubschema(schema.items, item, itemPath, depth + 1, nodes));
      }
    }
    if (schema.contains !== undefined) {
      const matches = value.reduce((count, item, index) => count + (validateSubschema(schema.contains, item, [...path, index], depth + 1, nodes).length === 0 ? 1 : 0), 0);
      const minimum = typeof schema.minContains === "number" ? schema.minContains : 1;
      const maximum = typeof schema.maxContains === "number" ? schema.maxContains : Number.POSITIVE_INFINITY;
      if (matches < minimum || matches > maximum) {
        issues.push({ path, keyword: "contains", message: `must contain between ${minimum} and ${Number.isFinite(maximum) ? maximum : "unlimited"} matching item(s)` });
      }
    }
  }
  const object = asJsonObject(value);
  if (object) {
    const properties = asJsonObject(schema.properties) || {};
    const required = Array.isArray(schema.required) ? schema.required.filter((key): key is string => typeof key === "string") : [];
    for (const key of required) {
      if (object[key] === undefined) issues.push({ path: [...path, key], keyword: "required", message: "is required" });
    }
    for (const [key, propertySchemaValue] of Object.entries(properties)) {
      if (object[key] === undefined) continue;
      issues.push(...validateSubschema(propertySchemaValue, object[key], [...path, key], depth + 1, nodes));
    }
    for (const [key, item] of Object.entries(object)) {
      if (key in properties) continue;
      const patternSchemas = asJsonObject(schema.patternProperties) || {};
      // Pattern constraints are evaluated by the backend. Never execute generator-provided
      // regular expressions in the browser or reject a value based on an incomplete match.
      if (Object.keys(patternSchemas).length > 0) continue;
      if (schema.additionalProperties === false) issues.push({ path: [...path, key], keyword: "additionalProperties", message: "is not an allowed property" });
      else if (schema.additionalProperties !== undefined) {
        issues.push(...validateSubschema(schema.additionalProperties, item, [...path, key], depth + 1, nodes));
      }
    }
    if (schema.propertyNames !== undefined) {
      for (const key of Object.keys(object)) {
        issues.push(...validateSubschema(schema.propertyNames, key, [...path, key], depth + 1, nodes));
      }
    }
    const dependentRequired = asJsonObject(schema.dependentRequired) || {};
    for (const [key, dependencies] of Object.entries(dependentRequired)) {
      if (!(key in object) || !Array.isArray(dependencies)) continue;
      for (const dependency of dependencies) {
        if (typeof dependency === "string" && object[dependency] === undefined) {
          issues.push({ path: [...path, dependency], keyword: "dependentRequired", message: `is required when ${key} is present` });
        }
      }
    }
    const dependentSchemas = asJsonObject(schema.dependentSchemas) || {};
    for (const [key, dependency] of Object.entries(dependentSchemas)) {
      if (key in object) issues.push(...validateSubschema(dependency, value, path, depth + 1, nodes));
    }
    if (typeof schema.minProperties === "number" && Object.keys(object).length < schema.minProperties) issues.push({ path, keyword: "minProperties", message: `requires at least ${schema.minProperties} propert${schema.minProperties === 1 ? "y" : "ies"}` });
    if (typeof schema.maxProperties === "number" && Object.keys(object).length > schema.maxProperties) issues.push({ path, keyword: "maxProperties", message: `allows at most ${schema.maxProperties} properties` });
  }
  if (Array.isArray(schema.oneOf)) issues.push(...validateUnion(schema.oneOf, value, path, depth, nodes, true));
  else if (Array.isArray(schema.anyOf)) issues.push(...validateUnion(schema.anyOf, value, path, depth, nodes, false));
  if (Array.isArray(schema.allOf)) {
    for (const branch of schema.allOf) {
      issues.push(...validateSubschema(branch, value, path, depth + 1, nodes));
    }
  }
  if (schema.if !== undefined) {
    const condition = validateSubschema(schema.if, value, path, depth + 1, nodes).length === 0;
    const branch = condition ? schema.then : schema.else;
    if (branch !== undefined) issues.push(...validateSubschema(branch, value, path, depth + 1, nodes));
  }
  if (schema.not !== undefined && validateSubschema(schema.not, value, path, depth + 1, nodes).length === 0) {
    issues.push({ path, keyword: "not", message: "must not satisfy this schema" });
  }
  return dedupeIssues(issues);
}

function validateUnion(branches: unknown[], value: unknown, path: PathSegment[], depth: number, nodes: ValidationBudget, exclusive: boolean): SchemaIssue[] {
  const results = branches.map((branch) => {
    return validateSubschema(branch, value, path, depth + 1, nodes);
  });
  const matches = results.filter((result) => result.length === 0).length;
  if ((exclusive && matches !== 1) || (!exclusive && matches === 0)) {
    const unionIssue = { path, keyword: exclusive ? "oneOf" : "anyOf", message: exclusive ? "must match exactly one option" : "must match at least one option" };
    if (matches === 0) {
      return [unionIssue, ...results.flat()];
    }
    return [unionIssue];
  }
  return [];
}

function validateSubschema(subschema: unknown, value: unknown, path: PathSegment[], depth: number, nodes: ValidationBudget): SchemaIssue[] {
  if (subschema === true) return [];
  if (subschema === false) return [{ path, keyword: "falseSchema", message: "must not be present" }];
  const schema = asJsonObject(subschema);
  return schema
    ? validateSchemaValue(schema, value, path, depth, nodes)
    : [{ path, keyword: "schema", message: "contains an invalid schema" }];
}

function matchesType(type: string, value: unknown): boolean {
  switch (type) {
    case "object": return asJsonObject(value) !== undefined;
    case "array": return Array.isArray(value);
    case "string": return typeof value === "string";
    case "number": return typeof value === "number" && Number.isFinite(value);
    case "integer": return typeof value === "number" && Number.isInteger(value);
    case "boolean": return typeof value === "boolean";
    case "null": return value === null;
    default: return true;
  }
}

function isEmptyValue(value: unknown): boolean {
  return value === undefined || value === "" || (Array.isArray(value) && value.length === 0);
}

function matchesFormat(format: string, value: string): boolean {
  if (format === "email") return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
  if (format === "uri" || format === "url") {
    try { return Boolean(new URL(value)); } catch { return false; }
  }
  if (format === "uri-reference") {
    try { return Boolean(new URL(value, "https://qcg.invalid")); } catch { return false; }
  }
  if (format === "date") return /^\d{4}-\d{2}-\d{2}$/.test(value) && !Number.isNaN(Date.parse(`${value}T00:00:00Z`));
  if (format === "date-time") return !Number.isNaN(Date.parse(value));
  if (format === "time") return /^\d{2}:\d{2}(:\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?$/.test(value);
  return true;
}

function isMultipleOf(value: number, divisor: number): boolean {
  const quotient = value / divisor;
  return Math.abs(quotient - Math.round(quotient)) < Number.EPSILON * Math.max(1, Math.abs(quotient)) * 10;
}

function deepEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (typeof left !== typeof right || left === null || right === null) return false;
  if (Array.isArray(left) && Array.isArray(right)) return left.length === right.length && left.every((item, index) => deepEqual(item, right[index]));
  const leftObject = asJsonObject(left);
  const rightObject = asJsonObject(right);
  if (leftObject && rightObject) {
    const leftKeys = Object.keys(leftObject);
    const rightKeys = Object.keys(rightObject);
    return leftKeys.length === rightKeys.length && leftKeys.every((key) => key in rightObject && deepEqual(leftObject[key], rightObject[key]));
  }
  return false;
}

function stableValueKey(value: unknown): string | undefined {
  const MAX_KEY_LENGTH = 64 * 1024;
  type Task = { value: unknown } | { token: string };
  const tasks: Task[] = [{ value }];
  const seen = new Set<object>();
  const output: string[] = [];
  let outputLength = 0;

  while (tasks.length > 0) {
    const task = tasks.pop();
    if (!task) continue;
    if ("token" in task) {
      outputLength += task.token.length;
      if (outputLength > MAX_KEY_LENGTH) return undefined;
      output.push(task.token);
      continue;
    }

    const current = task.value;
    if (current === null) {
      output.push("null");
      outputLength += 4;
    } else if (Array.isArray(current)) {
      if (seen.has(current)) {
        output.push("[Circular]");
        outputLength += 10;
      } else {
        seen.add(current);
        tasks.push({ token: "]" });
        for (let index = current.length - 1; index >= 0; index -= 1) {
          tasks.push({ value: current[index] });
          if (index > 0) tasks.push({ token: "," });
        }
        tasks.push({ token: "[" });
      }
    } else {
      const object = asJsonObject(current);
      if (object) {
        if (seen.has(object)) {
          output.push("{Circular}");
          outputLength += 9;
        } else {
          seen.add(object);
          const keys = Object.keys(object).sort();
          tasks.push({ token: "}" });
          for (let index = keys.length - 1; index >= 0; index -= 1) {
            const key = keys[index];
            if (key.length > MAX_KEY_LENGTH) return undefined;
            tasks.push({ value: object[key] });
            tasks.push({ token: ":" });
            tasks.push({ token: JSON.stringify(key) });
            if (index > 0) tasks.push({ token: "," });
          }
          tasks.push({ token: "{" });
        }
      } else {
        let token: string;
        if (typeof current === "string") {
          if (current.length > MAX_KEY_LENGTH) return undefined;
          token = JSON.stringify(current);
        } else if (typeof current === "number" && Number.isNaN(current)) {
          token = "number:NaN";
        } else {
          token = `${typeof current}:${String(current)}`;
        }
        outputLength += token.length;
        if (outputLength > MAX_KEY_LENGTH) return undefined;
        output.push(token);
      }
    }
    if (outputLength > MAX_KEY_LENGTH) return undefined;
  }
  return output.join("");
}

function dedupeIssues(issues: SchemaIssue[]): SchemaIssue[] {
  const seen = new Set<string>();
  return issues.filter((issue) => {
    const key = `${pathString(issue.path)}\0${issue.keyword}\0${issue.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}
