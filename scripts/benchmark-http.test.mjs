import { spawnSync } from "node:child_process";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import assert from "node:assert/strict";

const benchmark = fileURLToPath(new URL("./benchmark-http.mjs", import.meta.url));

test(
  "HTTP benchmark preserves early server failure and removes its temporary root",
  { skip: process.platform !== "linux" },
  () => {
    const root = mkdtempSync(join(tmpdir(), "qcg-http-benchmark-test-"));
    const temporary = join(root, "tmp");
    mkdirSync(temporary);
    try {
      const binary = join(root, "early-exit-server");
      writeFileSync(binary, "#!/bin/sh\necho 'fake server exited early'\nexit 17\n");
      chmodSync(binary, 0o755);
      const result = spawnSync(process.execPath, [benchmark], {
        env: {
          ...process.env,
          TMPDIR: temporary,
          QCG_HTTP_BINARY: binary,
          QCG_HTTP_BENCHMARK_REPORT: join(temporary, "report.json"),
        },
        encoding: "utf8",
        timeout: 5000,
      });

      assert.equal(
        result.error,
        undefined,
        `benchmark did not exit: ${result.error?.message}\n${result.stdout}\n${result.stderr}`,
      );
      assert.equal(result.status, 1, `${result.stdout}\n${result.stderr}`);
      assert.match(result.stderr, /server startup failed: fake server exited early/);
      assert.deepEqual(
        readdirSync(temporary),
        [],
        "benchmark leaked its temporary root",
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  },
);
