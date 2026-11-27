import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const cargoHome = process.env.CARGO_HOME || join(homedir(), ".cargo");
const separator = "\x1f";
const inherited = process.env.CARGO_ENCODED_RUSTFLAGS
  ? process.env.CARGO_ENCODED_RUSTFLAGS.split(separator).filter(Boolean)
  : (process.env.RUSTFLAGS || "").trim().split(/\s+/).filter(Boolean);
const rustflags = [
  ...inherited,
  `--remap-path-prefix=${root}=.`,
  `--remap-path-prefix=${cargoHome}=/cargo`,
].join(separator);

// wasm-bindgen reads the release artifact directly; honor CARGO_TARGET_DIR
// the same way cargo does so custom target directories keep working.
const cargoTargetDir = process.env.CARGO_TARGET_DIR || join(root, "target");
// cargo resolves a relative CARGO_TARGET_DIR against its spawn cwd (root),
// while this script can be run from frontend/generator; anchor it to root for
// both the magic-header guard and wasm-bindgen.
const wasmFile = resolve(
  root,
  cargoTargetDir,
  "wasm32-unknown-unknown/release/expr_wasm.wasm",
);
const outDir = join(root, "frontend/generator/src/expr/pkg");

function runStatus(command, args, env) {
  const result = spawnSync(command, args, {
    cwd: root,
    env,
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  return result.status ?? 1;
}

function run(command, args, env) {
  const status = runStatus(command, args, env);
  if (status !== 0) process.exit(status);
}

const cargoEnv = { ...process.env, CARGO_ENCODED_RUSTFLAGS: rustflags, RUSTFLAGS: "" };
const wasmMagic = Buffer.from([0x00, 0x61, 0x73, 0x6d]);

function buildWasm() {
  run(
    "cargo",
    [
      "build",
      "-p",
      "expr-wasm",
      "--release",
      "--target",
      "wasm32-unknown-unknown",
      "--locked",
    ],
    cargoEnv,
  );
}

function bindgen() {
  return runStatus(
    "wasm-bindgen",
    ["--target", "web", "--out-dir", outDir, "--out-name", "expr_wasm", wasmFile],
    process.env,
  );
}

function hasWasmMagic() {
  try {
    return readFileSync(wasmFile).subarray(0, 4).equals(wasmMagic);
  } catch {
    return false;
  }
}

function rebuildArtifact() {
  // A cached or partially written artifact can look up to date to Cargo while
  // wasm-bindgen cannot parse it. Remove this package's outputs for that target
  // so the same build reproduces the module from source.
  console.error("invalid Wasm artifact after build; cleaning and rebuilding");
  run(
    "cargo",
    ["clean", "-p", "expr-wasm", "--target", "wasm32-unknown-unknown", "--release"],
    cargoEnv,
  );
  buildWasm();
}

buildWasm();
let rebuilt = false;
if (!hasWasmMagic()) {
  rebuildArtifact();
  rebuilt = true;
  if (!hasWasmMagic()) {
    console.error(`cargo did not produce a valid Wasm module: ${wasmFile}`);
    process.exit(1);
  }
}

let bindgenStatus = bindgen();
if (bindgenStatus !== 0) {
  // A truncated module keeps its magic bytes but still fails bindgen parsing.
  if (rebuilt) {
    console.error(`wasm-bindgen still failed after rebuilding: ${wasmFile}`);
    process.exit(bindgenStatus);
  }
  rebuildArtifact();
  bindgenStatus = bindgen();
  if (bindgenStatus !== 0) {
    console.error(`wasm-bindgen still failed after rebuilding: ${wasmFile}`);
    process.exit(bindgenStatus);
  }
}
