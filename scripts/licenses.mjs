// Cargo's deprecated slash-separated licenses represent alternatives.
// https://doc.rust-lang.org/cargo/reference/manifest.html#the-license-and-license-file-fields
export function declaredLicense(value, cargo = false) {
  if (!value || value === "UNLICENSED") return "NOASSERTION";
  return cargo ? value.replace(/\s*\/\s*/g, " OR ") : value;
}
