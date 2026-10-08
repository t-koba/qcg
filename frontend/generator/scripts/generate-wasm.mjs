import { spawnSync } from "node:child_process";
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
const wasmFile = join(
  cargoTargetDir,
  "wasm32-unknown-unknown/release/expr_wasm.wasm",
);
const outDir = join(root, "frontend/generator/src/expr/pkg");

function run(command, args, env) {
  const result = spawnSync(command, args, {
    cwd: root,
    env,
    stdio: "inherit",
  });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

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
  { ...process.env, CARGO_ENCODED_RUSTFLAGS: rustflags, RUSTFLAGS: "" },
);

run(
  "wasm-bindgen",
  ["--target", "web", "--out-dir", outDir, "--out-name", "expr_wasm", wasmFile],
  process.env,
);
