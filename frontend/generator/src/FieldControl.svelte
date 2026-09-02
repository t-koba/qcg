<script lang="ts">
  import type { InputField } from "./api/client";
  import SchemaField from "./SchemaField.svelte";
  import { encodeBase64, fieldSchema, removePath, setPath, validateFileInput, type JsonObject, type PathSegment } from "./field";
  import { humanizeIdentifier, localizedText } from "./format";

  let {
    field,
    value = undefined,
    valueKey = field.id,
    language = "en",
    onValue,
    onFile,
  }: {
    field: InputField;
    value?: unknown;
    valueKey?: string;
    requiredLabel?: string;
    language?: string;
    onValue: (id: string, value: unknown) => void;
    onFile: (id: string, file: File | undefined) => Promise<void>;
  } = $props();

  let schemaData = $derived(fieldSchema(field));
  let inputId = $derived(`field-${valueKey.replace(/[^A-Za-z0-9_-]/g, "-")}`);
  let ui = $derived((field.ui || {}) as JsonObject);
  let rendererUi = $derived({
    ...ui,
    title: localizedText(field.label || humanizeIdentifier(field.id), field.label_i18n, language),
    description: localizedText(field.description || "", field.description_i18n, language),
    placeholder: localizedText(field.placeholder || "", field.placeholder_i18n, language),
  });
  let rendererSchema = $derived({
    ...schemaData.schema,
    ...(field.pattern && schemaData.schema.pattern === undefined ? { pattern: field.pattern } : {}),
    ...(field.min_items !== null && schemaData.schema.minItems === undefined ? { minItems: field.min_items } : {}),
  });

  function updateValue(path: PathSegment[], candidate: unknown): void {
    if (path.length === 0) {
      onValue(valueKey, candidate);
      return;
    }
    onValue(valueKey, candidate === undefined ? removePath(value, path) : setPath(value, path, candidate));
  }

  async function updateFile(path: PathSegment[], file: File | undefined): Promise<void> {
    if (path.length === 0) {
      await onFile(valueKey, file);
      return;
    }
    if (!file) {
      updateValue(path, undefined);
      return;
    }
    validateFileInput(file);
    const bytes = new Uint8Array(await file.arrayBuffer());
    updateValue(path, { name: file.name, content_base64: encodeBase64(bytes) });
  }
</script>

<div class="field">
  <SchemaField
    schema={rendererSchema}
    value={value}
    idPrefix={inputId}
    canonicalType={field.type}
    {field}
    ui={rendererUi}
    {language}
    required={field.required}
    root
    explicitSchema={schemaData.explicit}
    onValue={updateValue}
    onFile={updateFile}
  />
</div>
