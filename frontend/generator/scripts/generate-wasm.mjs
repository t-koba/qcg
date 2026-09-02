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

const result = spawnSync(
  "wasm-pack",
  [
    "build",
    "crates/qcg-expr-wasm",
    "--target",
    "web",
    "--out-dir",
    "../../frontend/generator/src/expr/pkg",
    "--out-name",
    "qcg_expr_wasm",
  ],
  {
    cwd: root,
    env: { ...process.env, CARGO_ENCODED_RUSTFLAGS: rustflags, RUSTFLAGS: "" },
    stdio: "inherit",
  },
);

if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
