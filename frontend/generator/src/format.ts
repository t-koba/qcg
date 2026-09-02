export function humanizeIdentifier(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function localizedText(
  fallback: string,
  translations: Record<string, string> | null | undefined,
  language: string,
): string {
  if (!translations) return fallback;
  const normalized = language.trim().toLowerCase().replace(/_/g, "-");
  const entries = Object.entries(translations);
  const exact = entries.find(([locale]) => locale.toLowerCase().replace(/_/g, "-") === normalized)?.[1];
  if (exact) return exact;
  const primary = normalized.split("-", 1)[0];
  return entries.find(([locale]) => locale.toLowerCase().split(/[-_]/, 1)[0] === primary)?.[1] || fallback;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(bytes < 10 * 1024 ? 1 : 0)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}
