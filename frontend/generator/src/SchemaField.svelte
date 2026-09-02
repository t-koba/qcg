<script lang="ts">
  import type { InputField } from "./api/client";
  import SchemaErrorList from "./SchemaErrorList.svelte";
  import SchemaField from "./SchemaField.svelte";
  import {
    MAX_SCHEMA_DEPTH,
    MAX_SCHEMA_NODES,
    asJsonObject,
    normalizeSchema,
    optionLabel,
    optionValueKey,
    optionValues,
    pathString,
    schemaBoolean,
    schemaBranches,
    schemaDefault,
    schemaInputType,
    schemaKind,
    schemaNumber,
    schemaNodeCount,
    schemaOrder,
    schemaText,
    stringifyValue,
    isBlockingSchemaIssue,
    validateFileInput,
    valueNodeCountExceeded,
    type JsonObject,
    type JsonSchema,
    type PathSegment,
    type SchemaIssue,
    validateSchemaValue,
  } from "./field";
  import { humanizeIdentifier } from "./format";

  type Props = {
    schema: JsonSchema;
    value?: unknown;
    path?: PathSegment[];
    idPrefix: string;
    canonicalType?: string;
    field?: InputField;
    ui?: JsonObject;
    language?: string;
    required?: boolean;
    root?: boolean;
    explicitSchema?: boolean;
    nodeCount?: number;
    valueLimitReached?: boolean;
    issues?: SchemaIssue[];
    onValue: (path: PathSegment[], value: unknown) => void;
    onFile: (path: PathSegment[], file: File | undefined) => Promise<void>;
  };

  let {
    schema,
    value = undefined,
    path = [],
    idPrefix,
    canonicalType,
    field,
    ui = {},
    language = "en",
    required = false,
    root = false,
    explicitSchema = false,
    nodeCount = 0,
    valueLimitReached,
    issues,
    onValue,
    onFile,
  }: Props = $props();

  const copy = $derived(language.toLowerCase().startsWith("ja") ? {
    addItem: "項目を追加",
    removeItem: "項目を削除",
    addProperty: "プロパティを追加",
    removeProperty: "プロパティを削除",
    propertyName: "プロパティ名",
    propertyNamePlaceholder: "例: options",
    selectOption: "選択してください",
    option: "選択肢",
    preview: "入力値のプレビュー",
    jsonValue: "JSON 値",
    jsonPlaceholder: "有効な JSON を入力してください",
    invalidJson: "有効な JSON を入力してください。",
    schemaLimit: "Schema が大きすぎるため、この部分は JSON エディターに切り替えました。",
    schemaDepth: "Schema の深さ制限に達したため、この部分は JSON エディターに切り替えました。",
    valueLimit: "入力値が大きいため、この部分は JSON エディターに切り替えました。",
    validationLimit: "入力値の検証が上限に達しました。最終検証はサーバーで行われます。",
    serverValidation: "パターン制約はサーバーで検証されます。",
    required: "必須です",
    value: "値",
    setNull: "null を設定",
    nullValue: "null",
    tupleItem: "タプル項目",
  } : {
    addItem: "Add item",
    removeItem: "Remove item",
    addProperty: "Add property",
    removeProperty: "Remove property",
    propertyName: "Property name",
    propertyNamePlaceholder: "e.g. options",
    selectOption: "Select an option",
    option: "Option",
    preview: "Input preview",
    jsonValue: "JSON value",
    jsonPlaceholder: "Enter valid JSON",
    invalidJson: "Enter valid JSON.",
    schemaLimit: "This schema is too large, so this part uses a JSON editor.",
    schemaDepth: "The schema depth limit was reached, so this part uses a JSON editor.",
    valueLimit: "This value is too large, so this part uses a JSON editor.",
    validationLimit: "Client-side validation reached its bound. The server will perform final validation.",
    serverValidation: "Pattern constraints are validated by the server.",
    required: "is required",
    value: "Value",
    setNull: "Set null",
    nullValue: "null",
    tupleItem: "Tuple item",
  });

  let kind = $derived(schemaKind(schema, value, canonicalType));
  let validationValue = $derived(
    value === undefined && kind === "object"
      ? {}
      : value === undefined && kind === "array"
        ? []
        : value,
  );
  let allIssues = $derived(issues || [
    ...validateSchemaValue(schema, validationValue),
    ...(required && isEmptyValue(value) ? [{ path, keyword: "required", message: copy.required }] : []),
  ]);
  let localIssues = $derived(allIssues.filter((issue) => samePath(issue.path, path) && isBlockingSchemaIssue(issue)));
  let validationLimit = $derived(allIssues.some((issue) => issue.keyword === "limit"));
  let inputId = $derived(`${idPrefix}-${path.length === 0 ? "value" : path.map(idSegment).join("-")}`);
  let descriptionId = $derived(`${inputId}-description`);
  let hintId = $derived(`${inputId}-hint`);
  let errorId = $derived(`${inputId}-errors`);
  let fileErrorId = $derived(`${inputId}-file-error`);
  let previewId = $derived(`${inputId}-preview`);
  let label = $derived(schemaText(schema, ui, "title", path.length > 0 ? (typeof path[path.length - 1] === "number" ? `${copy.value} ${Number(path[path.length - 1]) + 1}` : humanizeIdentifier(String(path[path.length - 1]))) : copy.value, language));
  let description = $derived(schemaText(schema, ui, "description", "", language));
  let placeholder = $derived(schemaText(schema, ui, "placeholder", "", language));
  let readOnly = $derived(schemaBoolean(ui, "readonly") || schemaBoolean(ui, "read_only") || schema.readOnly === true);
  let disabled = $derived(schemaBoolean(ui, "disabled"));
  let preview = $derived(ui.preview === true || typeof ui.preview === "string" || schema.preview === true);
  let previewOpen = $derived(schemaBoolean(ui, "preview_open"));
  let proxy = $state<HTMLInputElement>();
  let rawError = $state("");
  let rawDraft = $state<string | null>(null);
  let rawDraftSource = $state("");
  let fileError = $state("");
  let newProperty = $state("");
  let selectedBranch = $state(0);
  let branchPathKey = $state("__initial__");

  $effect(() => {
    const currentPath = pathString(path);
    if (branchPathKey !== currentPath) {
      branchPathKey = currentPath;
      selectedBranch = inferBranch(schema, value);
    }
  });

  $effect(() => {
    if (proxy) proxy.setCustomValidity([...allIssues.filter(isBlockingSchemaIssue), ...(rawError ? [{ message: rawError }] : [])].map((issue) => issue.message).join(" "));
  });

  $effect(() => {
    const source = `${pathString(path)}\0${canonicalJsonValue(value)}`;
    if (rawDraftSource !== source) {
      rawDraftSource = source;
      rawDraft = null;
      rawError = "";
      fileError = "";
    }
  });

  let objectProperties = $derived(asJsonObject(schema.properties) || {});
  let orderedProperties = $derived(orderProperties(objectProperties, schema, ui));
  let objectValue = $derived(asJsonObject(value) || {});
  let extraProperties = $derived(Object.keys(objectValue).filter((key) => !(key in objectProperties)));
  let arrayValue = $derived(Array.isArray(value) ? value : []);
  let arrayItems = $derived(Array.isArray(schema.prefixItems) ? schema.prefixItems : []);
  let additionalSchema = $derived(asJsonObject(schema.additionalProperties));
  let canAddProperty = $derived(schema.additionalProperties !== false);
  let canAddItem = $derived(!(schema.items === false && arrayValue.length >= arrayItems.length) && (schema.maxItems === undefined || arrayValue.length < (numeric(schema.maxItems, Number.POSITIVE_INFINITY) ?? Number.POSITIVE_INFINITY)));
  let canRemoveItem = $derived(arrayValue.length > (numeric(schema.minItems, 0) ?? 0));
  let branches = $derived(schemaBranches(schema));
  let activeBranch = $derived(branches[selectedBranch] || branches[0] || {});
  let options = $derived(optionValues(schema, field));
  let inputType = $derived(schemaInputType(schema, ui, canonicalType));
  let constraintHint = $derived(buildConstraintHint(schema, language));
  let describedBy = $derived([description ? descriptionId : "", constraintHint ? hintId : "", localIssues.length > 0 || rawError ? errorId : "", fileError ? fileErrorId : ""].filter(Boolean).join(" ") || undefined);
  let valueSizeLimit = $derived(valueLimitReached ?? valueNodeCountExceeded(value));
  let serverPatternNotice = $derived(hasServerPatternConstraint(schema));
  let atLimit = $derived(schemaNodeCount(schema) > MAX_SCHEMA_NODES || nodeCount >= MAX_SCHEMA_NODES || path.length >= MAX_SCHEMA_DEPTH);
  let usesLegacyList = $derived(canonicalType === "list" && !explicitSchema && ui.widget === undefined);
  let usesLegacyJson = $derived(canonicalType === "json" && !explicitSchema && ui.widget === undefined);
  let usesLegacyMultiselect = $derived(canonicalType === "multiselect" && !explicitSchema && ui.widget === undefined);
  let usesLegacySelect = $derived(canonicalType === "select" && !explicitSchema && ui.widget === undefined);
  let needsJsonFallback = $derived(atLimit || valueSizeLimit || kind === "json" || usesLegacyJson);

  function emit(candidate: unknown): void {
    onValue(path, candidate);
  }

  function updateChild(childPath: PathSegment[], candidate: unknown): void {
    onValue(childPath, candidate);
  }

  function updateRawJson(element: HTMLTextAreaElement): void {
    rawDraft = element.value;
    if (!element.value.trim()) {
      rawError = required ? copy.invalidJson : "";
      element.setCustomValidity(rawError);
      emit(undefined);
      return;
    }
    try {
      rawError = "";
      element.setCustomValidity("");
      emit(JSON.parse(element.value));
    } catch {
      rawError = copy.invalidJson;
      element.setCustomValidity(rawError);
    }
  }

  function updateLegacyList(element: HTMLTextAreaElement): void {
    emit(element.value.split("\n").map((item) => item.trim()).filter(Boolean));
  }

  function updateNumber(element: HTMLInputElement): void {
    if (!element.value.trim()) {
      emit(undefined);
      return;
    }
    const number = Number(element.value);
    emit(Number.isFinite(number) ? number : undefined);
  }

  function updateEnum(element: HTMLSelectElement): void {
    const option = optionForKey(element.value);
    emit(option);
  }

  function updateUnion(index: number): void {
    selectedBranch = index;
    const next = schemaDefault(branches[index] || {});
    emit(next);
  }

  function addArrayItem(): void {
    if (!canAddItem) return;
    const itemSchema = itemSchemaForIndex(arrayValue.length);
    const next = schemaDefault(itemSchema) ?? defaultForSchema(itemSchema);
    emit([...arrayValue, next]);
  }

  function removeArrayItem(index: number): void {
    if (!canRemoveItem) return;
    emit(arrayValue.filter((_, candidateIndex) => candidateIndex !== index));
  }

  function addObjectProperty(): void {
    const key = newProperty.trim();
    if (!key || key in objectValue || key in objectProperties) return;
    const next = schemaDefault(additionalSchema || {}) ?? defaultForSchema(additionalSchema || {});
    updateChild([...path, key], next);
    newProperty = "";
  }

  function removeObjectProperty(key: string): void {
    onValue([...path, key], undefined);
  }

  function itemSchemaForIndex(index: number): JsonSchema {
    return normalizeSchema(arrayItems[index] ?? schema.items);
  }

  async function fileChanged(element: HTMLInputElement): Promise<void> {
    const file = element.files?.[0];
    fileError = "";
    element.setCustomValidity("");
    try {
      validateFileInput(file);
      await onFile(path, file);
    } catch (error) {
      fileError = error instanceof Error ? error.message : String(error);
      element.setCustomValidity(fileError);
    }
  }

  function branchLabel(branch: JsonSchema, index: number): string {
    const typeLabel = typeof branch.type === "string" ? humanizeIdentifier(branch.type) : `${copy.option} ${index + 1}`;
    return schemaText(branch, branchUi(index, branch), "title", typeLabel, language);
  }

  function optionText(option: unknown): string {
    return optionLabel(option, ui, language, field);
  }

  function currentOptionKey(candidate: unknown): string {
    return usesLegacySelect || usesLegacyMultiselect ? String(candidate) : optionValueKey(candidate);
  }

  function optionForKey(key: string): unknown {
    return options.find((candidate) => currentOptionKey(candidate) === key);
  }

  function pathForIssue(issue: SchemaIssue): string {
    const relative = issue.path.length >= path.length && path.every((segment, index) => segment === issue.path[index])
      ? issue.path.slice(path.length)
      : issue.path;
    return relative.length > 0 ? pathString(relative) : label;
  }

  function previewText(): string {
    if (typeof value === "string" && (kind === "string" || kind === "null")) return value;
    return stringifyValue(value) || "—";
  }

  function jsonEditorValue(): string {
    return rawDraft ?? canonicalJsonValue(value);
  }

  function canonicalJsonValue(candidate: unknown): string {
    if (usesLegacyJson || candidate === undefined) return stringifyValue(candidate);
    try {
      return JSON.stringify(candidate, null, 2) || "";
    } catch {
      return stringifyValue(candidate);
    }
  }

  function numeric(value: unknown, fallback: number | undefined): number | undefined {
    return typeof value === "number" && Number.isFinite(value) ? value : fallback;
  }

  function isEmptyValue(value: unknown): boolean {
    return value === undefined || value === "" || (Array.isArray(value) && value.length === 0);
  }

  function idSegment(value: PathSegment): string {
    return String(value).replace(/[^A-Za-z0-9_-]/g, "-");
  }

  function samePath(left: readonly PathSegment[], right: readonly PathSegment[]): boolean {
    return left.length === right.length && left.every((segment, index) => segment === right[index]);
  }

  function deepOptionEqual(left: unknown, right: unknown): boolean {
    if (Object.is(left, right)) return true;
    if (typeof left !== typeof right || left === null || right === null) return false;
    if (Array.isArray(left) && Array.isArray(right)) return left.length === right.length && left.every((item, index) => deepOptionEqual(item, right[index]));
    const leftObject = asJsonObject(left);
    const rightObject = asJsonObject(right);
    if (!leftObject || !rightObject) return false;
    const keys = Object.keys(leftObject);
    return keys.length === Object.keys(rightObject).length && keys.every((key) => key in rightObject && deepOptionEqual(leftObject[key], rightObject[key]));
  }

  function orderProperties(properties: JsonObject, schema: JsonSchema, ui: JsonObject): string[] {
    const keys = Object.keys(properties);
    const order = schemaOrder(schema, ui);
    return [...order.filter((key) => key in properties), ...keys.filter((key) => !order.includes(key))];
  }

  function propertyUi(key: string, propertySchema: JsonSchema): JsonObject {
    const schemaUi = asJsonObject(propertySchema.ui) || {};
    const configured = asJsonObject(asJsonObject(ui.properties)?.[key]) || {};
    return { ...schemaUi, ...configured };
  }

  function itemUi(itemSchema: JsonSchema): JsonObject {
    const schemaUi = asJsonObject(itemSchema.ui) || {};
    const configured = asJsonObject(ui.items) || {};
    return { ...schemaUi, ...configured };
  }

  function branchUi(index: number, branch: JsonSchema): JsonObject {
    const schemaUi = asJsonObject(branch.ui) || {};
    const configured = Array.isArray(ui.oneOf)
      ? asJsonObject(ui.oneOf[index]) || {}
      : asJsonObject(asJsonObject(ui.oneOf)?.[String(index)]) || {};
    return { ...schemaUi, ...configured };
  }

  function requiredProperty(schema: JsonSchema, key: string): boolean {
    return Array.isArray(schema.required) && schema.required.includes(key);
  }

  function defaultForSchema(schema: JsonSchema): unknown {
    const kind = schemaKind(schema);
    if (kind === "object") return {};
    if (kind === "array") return [];
    if (kind === "boolean") return false;
    if (kind === "number" || kind === "integer") return 0;
    if (kind === "enum") return optionValues(schema)[0];
    if (kind === "string") return "";
    if (kind === "null") return null;
    return undefined;
  }

  function inferBranch(schema: JsonSchema, value: unknown): number {
    const branches = schemaBranches(schema);
    if (branches.length === 0) return 0;
    const valid = branches.findIndex((branch) => validateSchemaValue(branch, value).length === 0);
    return valid >= 0 ? valid : 0;
  }

  function buildConstraintHint(schema: JsonSchema, language: string): string {
    const parts: string[] = [];
    if (typeof schema.minLength === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.minLength} 文字以上` : `at least ${schema.minLength} characters`);
    if (typeof schema.maxLength === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.maxLength} 文字以下` : `at most ${schema.maxLength} characters`);
    if (typeof schema.minimum === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.minimum} 以上` : `minimum ${schema.minimum}`);
    if (typeof schema.maximum === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.maximum} 以下` : `maximum ${schema.maximum}`);
    if (typeof schema.minItems === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.minItems} 項目以上` : `at least ${schema.minItems} item(s)`);
    if (typeof schema.maxItems === "number") parts.push(language.toLowerCase().startsWith("ja") ? `${schema.maxItems} 項目以下` : `at most ${schema.maxItems} item(s)`);
    return parts.join(" · ");
  }

  function hasServerPatternConstraint(schema: JsonSchema): boolean {
    return typeof schema.pattern === "string" || Object.keys(asJsonObject(schema.patternProperties) || {}).length > 0;
  }

</script>

{#if !schemaBoolean(ui, "hidden")}
{#if serverPatternNotice}<p class="schema-limit schema-server-validation" role="status">{copy.serverValidation}</p>{/if}
{#if needsJsonFallback}
  <div class="schema-node schema-json-fallback">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    {#if atLimit}<p class="schema-limit" role="status">{path.length >= MAX_SCHEMA_DEPTH ? copy.schemaDepth : copy.schemaLimit}</p>{:else if valueSizeLimit}<p class="schema-limit" role="status">{copy.valueLimit}</p>{/if}
    <textarea
      id={inputId}
      name={path.length === 0 ? field?.id : pathString(path)}
      class="schema-json-input"
      rows={Math.max(4, Math.min(24, numeric(ui.rows, 8) ?? 8))}
      placeholder={placeholder || copy.jsonPlaceholder}
      spellcheck="false"
      readonly={readOnly}
      disabled={disabled}
      aria-describedby={describedBy}
      aria-invalid={localIssues.length > 0 || Boolean(rawError)}
      value={jsonEditorValue()}
      oninput={(event) => updateRawJson(event.currentTarget)}
    ></textarea>
    {#if rawError}<p id={errorId} class="schema-error" role="alert">{rawError}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={`${errorId}-list`} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if kind === "union" && inputType !== "file"}
  <fieldset class="schema-node schema-union" aria-describedby={describedBy}>
    <legend>{label}{#if required}<span class="schema-required">*</span>{/if}</legend>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <select id={inputId} value={String(selectedBranch)} disabled={disabled || readOnly} aria-label={label} aria-describedby={describedBy} onchange={(event) => updateUnion(Number(event.currentTarget.value))}>
      {#each branches as branch, index}<option value={index}>{branchLabel(branch, index)}</option>{/each}
    </select>
    {#if branches.length > 0}
      <SchemaField
        schema={activeBranch}
        value={value}
        path={path}
        idPrefix={`${idPrefix}-branch-${selectedBranch}`}
        language={language}
        nodeCount={nodeCount + 1}
        valueLimitReached={valueSizeLimit}
        issues={allIssues}
        onValue={onValue}
        {onFile}
      />
    {/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </fieldset>
{:else if kind === "object" && inputType !== "file"}
  <fieldset class="schema-node schema-object" aria-describedby={describedBy}>
    <legend>{label}{#if required}<span class="schema-required">*</span>{/if}</legend>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <div class="schema-object-fields">
      {#each orderedProperties as key (key)}
        {@const propertySchema = normalizeSchema(objectProperties[key])}
        <SchemaField
          schema={propertySchema}
          value={objectValue[key]}
          path={[...path, key]}
          {idPrefix}
          canonicalType={undefined}
          language={language}
          ui={propertyUi(key, propertySchema)}
          required={requiredProperty(schema, key)}
          nodeCount={nodeCount + 1}
          valueLimitReached={valueSizeLimit}
          issues={allIssues}
          onValue={updateChild}
          {onFile}
        />
      {/each}
      {#each extraProperties as key (key)}
        {@const propertyValue = objectValue[key]}
        <div class="schema-extra-property">
          <div class="schema-extra-heading"><span>{key}</span><button type="button" class="schema-remove" disabled={disabled || readOnly} aria-label={`${copy.removeProperty}: ${key}`} onclick={() => removeObjectProperty(key)}>×</button></div>
          <SchemaField
            schema={additionalSchema || {}}
            value={propertyValue}
            path={[...path, key]}
            {idPrefix}
            language={language}
            ui={itemUi(additionalSchema || {})}
            nodeCount={nodeCount + 1}
            valueLimitReached={valueSizeLimit}
            issues={allIssues}
            onValue={updateChild}
            {onFile}
          />
        </div>
      {/each}
    </div>
    {#if canAddProperty && !readOnly && !disabled}
      <div class="schema-add-property">
        <input aria-label={copy.propertyName} placeholder={copy.propertyNamePlaceholder} value={newProperty} oninput={(event) => newProperty = event.currentTarget.value} onkeydown={(event) => { if (event.key === "Enter") { event.preventDefault(); addObjectProperty(); } }} />
        <button type="button" class="secondary-btn" onclick={addObjectProperty}>{copy.addProperty}</button>
      </div>
    {/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </fieldset>
{:else if kind === "array" && inputType !== "file" && !usesLegacyList && !usesLegacyMultiselect}
  <fieldset class="schema-node schema-array" aria-describedby={describedBy}>
    <legend>{label}{#if required}<span class="schema-required">*</span>{/if}</legend>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <div class="schema-array-items">
      {#each arrayValue as item, index (index)}
        <div class="schema-array-item">
          <div class="schema-array-heading"><span>{arrayItems[index] ? `${copy.tupleItem} ${index + 1}` : `${label} ${index + 1}`}</span><button type="button" class="schema-remove" disabled={disabled || readOnly || !canRemoveItem} aria-label={`${copy.removeItem}: ${index + 1}`} onclick={() => removeArrayItem(index)}>×</button></div>
          <SchemaField
            schema={itemSchemaForIndex(index)}
            value={item}
            path={[...path, index]}
            {idPrefix}
            language={language}
            ui={itemUi(itemSchemaForIndex(index))}
            nodeCount={nodeCount + 1}
            valueLimitReached={valueSizeLimit}
            issues={allIssues}
            onValue={updateChild}
            {onFile}
          />
        </div>
      {/each}
    </div>
    {#if !readOnly && !disabled}<button type="button" class="secondary-btn schema-add-item" disabled={!canAddItem} onclick={addArrayItem}>{copy.addItem}</button>{/if}
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </fieldset>
{:else if inputType !== "file" && (kind === "enum" || usesLegacySelect)}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    {#if ui.widget === "radio"}
      <div class="schema-radio-group" role="radiogroup" aria-describedby={describedBy}>
        {#each options as option, index}
          {@const optionId = `${inputId}-option-${index}`}
          <label for={optionId} class="schema-radio"><input id={optionId} type="radio" name={inputId} checked={deepOptionEqual(option, value)} disabled={disabled || readOnly} onchange={() => emit(option)} /><span>{optionText(option)}</span></label>
        {/each}
      </div>
    {:else}
      <select id={inputId} name={path.length === 0 ? field?.id : pathString(path)} required={required} disabled={disabled || readOnly} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} onchange={(event) => updateEnum(event.currentTarget)}>
        {#if !required}<option value="">{copy.selectOption}</option>{/if}
        {#each options as option}<option value={currentOptionKey(option)} selected={deepOptionEqual(option, value)}>{optionText(option)}</option>{/each}
      </select>
    {/if}
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if usesLegacyMultiselect}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <select id={inputId} name={field?.id} multiple size={Math.min(Math.max(options.length, 2), 8)} required={required} disabled={disabled || readOnly} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} onchange={(event) => emit([...event.currentTarget.selectedOptions].map((selected) => optionForKey(selected.value)).filter((option) => option !== undefined))}>
      {#each options as option}<option value={currentOptionKey(option)} selected={Array.isArray(value) && value.some((candidate) => deepOptionEqual(candidate, option))}>{optionText(option)}</option>{/each}
    </select>
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if kind === "boolean"}
  <div class="schema-node schema-scalar">
    <label class="boolean-control" for={inputId}><input id={inputId} name={path.length === 0 ? field?.id : pathString(path)} type="checkbox" checked={Boolean(value ?? schemaDefault(schema) ?? false)} disabled={disabled || readOnly} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} onchange={(event) => emit(event.currentTarget.checked)} /><span aria-hidden="true"></span><b>{label}{#if required}<span class="schema-required">*</span>{/if}</b></label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if kind === "null"}
  <div class="schema-node schema-scalar">
    <span class="schema-label">{label}{#if required}<span class="schema-required">*</span>{/if}</span>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    {#if value === null}<output aria-label={label}>null</output>{:else}<button type="button" class="secondary-btn" disabled={disabled || readOnly} onclick={() => emit(null)}>{copy.setNull}</button>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if inputType === "file"}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <input id={inputId} name={path.length === 0 ? field?.id : pathString(path)} type="file" required={required} disabled={disabled || readOnly} accept={typeof ui.accept === "string" ? ui.accept : undefined} aria-describedby={describedBy} aria-invalid={localIssues.length > 0 || Boolean(fileError)} onchange={(event) => void fileChanged(event.currentTarget)} />
    {#if value && typeof value === "object" && "name" in value}<p class="schema-file-name">{String((value as JsonObject).name)}</p>{/if}
    {#if fileError}<p id={fileErrorId} class="schema-error" role="alert">{fileError}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if kind === "number" || kind === "integer"}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <input id={inputId} name={path.length === 0 ? field?.id : pathString(path)} type="number" required={required} disabled={disabled || readOnly} min={schemaNumber(ui, schema, "min", "minimum")} max={schemaNumber(ui, schema, "max", "maximum")} step={schemaNumber(ui, schema, "step", "multipleOf") || (kind === "integer" ? 1 : undefined)} placeholder={placeholder || undefined} value={value === undefined || value === null ? "" : String(value)} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} oninput={(event) => updateNumber(event.currentTarget)} />
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else if usesLegacyList}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    <textarea id={inputId} name={field?.id} required={required} disabled={disabled || readOnly} rows={Math.max(2, Math.min(40, numeric(ui.rows, 4) ?? 4))} placeholder={placeholder || undefined} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} value={Array.isArray(value) ? value.map(String).join("\n") : ""} oninput={(event) => updateLegacyList(event.currentTarget)}></textarea>
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{:else}
  <div class="schema-node schema-scalar">
    <label for={inputId}>{label}{#if required}<span class="schema-required">*</span>{/if}</label>
    {#if description}<p id={descriptionId} class="schema-description">{description}</p>{/if}
    {#if inputType === "text" && (ui.widget === "textarea" || ui.widget === "code" || canonicalType === "text" || canonicalType === "natural_language")}
      <textarea id={inputId} name={path.length === 0 ? field?.id : pathString(path)} required={required} disabled={disabled || readOnly} rows={Math.max(2, Math.min(40, numeric(ui.rows, 5) ?? 5))} minlength={numeric(schema.minLength, undefined)} maxlength={numeric(schema.maxLength, undefined)} placeholder={placeholder || undefined} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} value={value === undefined || value === null ? "" : String(value)} oninput={(event) => emit(event.currentTarget.value)}></textarea>
    {:else}
      <input id={inputId} name={path.length === 0 ? field?.id : pathString(path)} type={inputType} required={required} disabled={disabled || readOnly} minlength={numeric(schema.minLength, undefined)} maxlength={numeric(schema.maxLength, undefined)} placeholder={placeholder || undefined} value={value === undefined || value === null ? "" : String(value)} aria-describedby={describedBy} aria-invalid={localIssues.length > 0} oninput={(event) => emit(event.currentTarget.value)} />
    {/if}
    {#if constraintHint}<p id={hintId} class="schema-hint">{constraintHint}</p>{/if}
    {#if localIssues.length > 0}<SchemaErrorList id={errorId} issues={localIssues} {pathForIssue} />{/if}
  </div>
{/if}

{#if root}
  <input class="schema-validity-proxy" bind:this={proxy} aria-label={label} tabindex="-1" value="" />
  {#if preview}
    <details class="schema-preview" id={previewId} open={previewOpen}>
      <summary>{typeof ui.preview === "string" && ui.preview !== "json" ? ui.preview : copy.preview}</summary>
      <pre>{previewText()}</pre>
    </details>
  {/if}
  {#if allIssues.length > 0}
    {#if validationLimit}<p class="schema-limit" role="status">{copy.validationLimit}</p>{/if}
    {#if allIssues.some(isBlockingSchemaIssue)}<SchemaErrorList id={`${errorId}-summary`} issues={allIssues.filter(isBlockingSchemaIssue)} {pathForIssue} />{/if}
  {/if}
{/if}

{/if}
