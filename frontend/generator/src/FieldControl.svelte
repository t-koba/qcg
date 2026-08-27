<script lang="ts">
  import type { InputField } from "./api/client";
  import { fieldKind } from "./field";

  let {
    field,
    value = undefined,
    valueKey = field.id,
    onValue,
    onFile,
  }: {
    field: InputField;
    value?: unknown;
    valueKey?: string;
    onValue: (id: string, value: unknown) => void;
    onFile: (id: string, file: File | undefined) => Promise<void>;
  } = $props();

  let kind = $derived(fieldKind(field));

  function emit(candidate: unknown): void {
    onValue(valueKey, candidate);
  }

  function stringValue(candidate: unknown): string {
    if (candidate === undefined || candidate === null) return "";
    return typeof candidate === "string" ? candidate : JSON.stringify(candidate, null, 2);
  }

  function listValue(candidate: unknown): string {
    return Array.isArray(candidate) ? candidate.map(String).join("\n") : "";
  }
</script>

<div class="field">
  <label for={`field-${valueKey}`}>{field.id}</label>

  {#if kind === "boolean"}
    <select id={`field-${valueKey}`} name={field.id} value={String(value ?? field.default ?? false)} onchange={(e) => emit((e.currentTarget as HTMLSelectElement).value === "true")}>
      <option value="false">false</option>
      <option value="true">true</option>
    </select>
  {:else if kind === "select"}
    <select id={`field-${valueKey}`} name={field.id} value={String(value ?? "")} onchange={(e) => emit((e.currentTarget as HTMLSelectElement).value)}>
      {#each field.options as option}
        <option value={option}>{option}</option>
      {/each}
    </select>
  {:else if kind === "multiselect"}
    <select id={`field-${valueKey}`} name={field.id} multiple size={Math.min(Math.max(field.options.length, 2), 8)} onchange={(e) => emit([...(e.currentTarget as HTMLSelectElement).selectedOptions].map((o) => o.value))}>
      {#each field.options as option}
        <option value={option} selected={Array.isArray(value) && (value as unknown[]).includes(option)}>{option}</option>
      {/each}
    </select>
  {:else if kind === "file"}
    <input id={`field-${valueKey}`} name={field.id} type="file" onchange={(e) => void onFile(valueKey, (e.currentTarget as HTMLInputElement).files?.[0])} />
  {:else if kind === "json"}
    <textarea id={`field-${valueKey}`} name={field.id} data-json placeholder="JSON" value={stringValue(value ?? field.default)} oninput={(e) => { try { emit(JSON.parse(e.currentTarget.value)); } catch { /* typing */ } }}></textarea>
  {:else if kind === "list"}
    <textarea id={`field-${valueKey}`} name={field.id} value={listValue(value)} oninput={(e) => emit(e.currentTarget.value.split("\n").map((s) => s.trim()).filter(Boolean))}></textarea>
  {:else if kind === "natural_language"}
    <textarea id={`field-${valueKey}`} name={field.id} value={stringValue(value ?? field.default)} oninput={(e) => emit(e.currentTarget.value)}></textarea>
  {:else if kind === "number"}
    <input id={`field-${valueKey}`} name={field.id} type="number" value={stringValue(value ?? field.default)} oninput={(e) => emit(Number(e.currentTarget.value))} />
  {:else}
    <input id={`field-${valueKey}`} name={field.id} type="text" value={stringValue(value ?? field.default)} oninput={(e) => emit(e.currentTarget.value)} />
  {/if}

  {#if field.pattern && (kind === "string" || kind === "number")}
    <div class="hint">Pattern: {field.pattern}</div>
  {/if}
  {#if field.min_items !== null && kind === "list"}
    <div class="hint">Min items: {field.min_items}</div>
  {/if}
  {#if field.options.length && kind === "select"}
    <div class="hint">Options: {field.options.join(", ")}</div>
  {/if}
</div>

<style>
  .field { display: grid; gap: 5px; }
  label { font-weight: 700; }
  input, select, textarea { border: 1px solid var(--line); border-radius: 6px; font: inherit; min-height: 36px; min-width: 0; padding: 8px 10px; width: 100%; }
  textarea { min-height: 100px; resize: vertical; }
  .hint { color: var(--muted); font-size: 12px; }
</style>
