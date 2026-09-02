import { execFileSync, spawn } from "node:child_process";
import { mkdtemp, mkdir, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";

const qcgPort = Number(process.env.QCG_UI_SMOKE_PORT || 58018);
const uiPort = Number(process.env.QCG_UI_VITE_SMOKE_PORT || 58019);
const servedAssetsPort = Math.max(qcgPort, uiPort) + 1;
const qcgBinary = join("target", "debug", process.platform === "win32" ? "qcg.exe" : "qcg");
const viteCli = join(process.cwd(), "frontend", "generator", "node_modules", "vite", "bin", "vite.js");
const root = await mkdtemp(join(tmpdir(), "qcg-ui-smoke."));
let browser;
const logChunks = [];

async function main() {
  execFileSync("npm", ["run", "build"], { cwd: "frontend/generator", stdio: "inherit" });
  execFileSync("cargo", ["build", "-p", "qcg"], { stdio: "inherit" });
  browser = await chromium.launch();
  const modes = [
    { name: "vite-proxy", apiPort: qcgPort, frontendPort: uiPort, frontend: "vite" },
    { name: "serve-assets", apiPort: servedAssetsPort, frontend: "assets" },
  ];
  for (const mode of modes) {
    await runMode(mode);
  }
  console.log("ui playwright smoke ok");
}

async function runMode(mode) {
  const modeRoot = join(root, mode.name);
  const generatorsDir = await mergeGenerators(join(modeRoot, "generators"));
  const runsDir = join(modeRoot, "runs");
  const providersPath = join(modeRoot, "providers.toml");
  await writeFile(providersPath, "");
  const apiBase = `http://127.0.0.1:${mode.apiPort}`;
  const frontendOrigin = mode.frontend === "assets"
    ? `${apiBase}/api/generators/generator/assets/ui/index.html`
    : `http://127.0.0.1:${mode.frontendPort}`;
  const qcgArgs = [
    "--providers",
    providersPath,
    "serve",
    "--bind",
    "127.0.0.1",
    "--port",
    String(mode.apiPort),
    "--generators-dir",
    generatorsDir,
    "--runs-dir",
    runsDir,
  ];
  const qcg = spawn(qcgBinary, qcgArgs, { stdio: ["ignore", "pipe", "pipe"] });
  captureOutput(qcg, `${mode.name} qcg`);
  let frontend;
  let context;
  try {
    await waitForUrl(`${apiBase}/healthz`, 30000);
    if (mode.frontend === "vite") {
      frontend = spawn(
        process.execPath,
        [viteCli, "--host", "127.0.0.1", "--port", String(mode.frontendPort), "--strictPort"],
        {
          cwd: "frontend/generator",
          env: { ...process.env, QCG_API_TARGET: apiBase },
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
    }
    if (frontend) captureOutput(frontend, `${mode.name} frontend`);
    await waitForUrl(mode.frontend === "assets" ? frontendOrigin : `${frontendOrigin}/`, 30000);

    context = await browser.newContext();
    const page = await context.newPage();
    page.on("console", (message) => logChunks.push(Buffer.from(`${mode.name} browser console ${message.type()}: ${message.text()}\n`)));
    page.on("pageerror", (error) => {
      if (logChunks.length < 100) logChunks.push(Buffer.from(`${mode.name} browser pageerror: ${error.message}\n`));
    });
    await page.goto(frontendOrigin, { waitUntil: "domcontentloaded" });
    await page.getByRole("button", { name: /Hello Template/ }).waitFor({ timeout: 10000 });
    await assertMcpConnections(page);
    await assertSuccessfulRun(page);
    await assertCancelRun(page);
    await assertQuestionForm(page);
    await assertListMultiselectForm(page);
    await assertSchemaDrivenForm(page);
    await assertSkippedReason(page);
    await assertArtifactPreviewSandbox(page);
    await assertFileInput(page);
    await context.close();
    context = undefined;

    context = await browser.newContext({ locale: "ja-JP" });
    await assertLocalizedGeneratorQuestion(await context.newPage(), frontendOrigin);
    await context.close();
    context = undefined;
  } finally {
    if (context) await context.close();
    await stopProcess(frontend);
    await stopProcess(qcg);
  }
}

async function assertLocalizedGeneratorQuestion(page, frontendOrigin) {
  await page.goto(frontendOrigin, { waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: /^Generator Builds a qcg\b/ }).click();
  await page.getByRole("button", { name: /^生成を開始$/ }).click();
  await page.locator("#run-state.waiting").waitFor({ timeout: 15000 });
  await page
    .getByRole("heading", { name: "作成したいジェネレーターの目的を説明してください。" })
    .waitFor({ timeout: 5000 });
  await page.getByLabel("目的", { exact: true }).waitFor({ timeout: 5000 });
}

async function assertMcpConnections(page) {
  const panel = page.locator(".mcp-connections");
  await panel.waitFor({ timeout: 10000 });
  for (const id of ["exa-public", "parallel-public"]) {
    const server = panel.locator(".mcp-server-row").filter({ hasText: id });
    await server.waitFor({ timeout: 5000 });
    await server.locator("small").filter({ hasText: "streamable_http" }).waitFor({ timeout: 5000 });
    await server.locator("small").filter({ hasText: "none" }).waitFor({ timeout: 5000 });
  }
}

function captureOutput(child, label) {
  child?.stdout?.on("data", (chunk) => logChunks.push(Buffer.from(`${label}: ${chunk}`)));
  child?.stderr?.on("data", (chunk) => logChunks.push(Buffer.from(`${label}: ${chunk}`)));
}

async function assertSuccessfulRun(page) {
  await selectGenerator(page, /Hello Template/);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  await page.locator("#artifact-list .artifact").filter({ hasText: "README.md" }).waitFor({ timeout: 5000 });
  const zipLink = page.locator("#zip-link:not(.hidden)");
  await zipLink.waitFor({ timeout: 5000 });

  const artifactHref = await page.locator("#artifact-list a").first().getAttribute("href");
  if (!artifactHref) {
    throw new Error("artifact link was not rendered");
  }
  const artifact = await page.evaluate(async (href) => {
    const response = await fetch(href);
    if (!response.ok) {
      throw new Error(`artifact fetch failed: ${response.status}`);
    }
    return response.text();
  }, artifactHref);
  if (!artifact.includes("Hello from qcg")) {
    throw new Error(`artifact content was unexpected: ${artifact}`);
  }
  const zipHref = await zipLink.getAttribute("href");
  if (!zipHref) {
    throw new Error("zip link was not rendered");
  }
  const zipBytes = await page.evaluate(async (href) => {
    const response = await fetch(href);
    if (!response.ok) {
      throw new Error(`zip fetch failed: ${response.status}`);
    }
    return (await response.arrayBuffer()).byteLength;
  }, zipHref);
  if (zipBytes <= 0) {
    throw new Error("zip response was empty");
  }
}

async function assertCancelRun(page) {
  await selectGenerator(page, /Cancelable UI/);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.running").waitFor({ timeout: 15000 });
  await page.waitForTimeout(200);
  await page.getByRole("button", { name: /^Cancel run$/ }).click();
  await page.locator("#run-state.canceled").waitFor({ timeout: 5000 });
  await page.getByRole("button", { name: /^Start again$/ }).click();
  await page.getByRole("button", { name: /^Start generation$/ }).waitFor({ timeout: 5000 });
}

async function assertQuestionForm(page) {
  await selectGenerator(page, /^Ask User\b/);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.waiting").waitFor({ timeout: 15000 });
  const answer = page.locator("#question-panel [name='answer']");
  await answer.waitFor({ timeout: 5000 });
  const tagName = await answer.evaluate((node) => node.tagName.toLowerCase());
  if (tagName !== "select") {
    throw new Error(`question answer control should be select, got ${tagName}`);
  }
  await answer.selectOption("detailed");
  await page.getByRole("button", { name: /^Continue$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  await page.locator("#artifact-list .artifact").filter({ hasText: "answer.txt" }).waitFor({ timeout: 15000 });
}

async function assertListMultiselectForm(page) {
  await selectGenerator(page, /List Multiselect/);
  const sites = page.locator("[name='sites']");
  const features = page.locator("[name='features']");
  await sites.waitFor({ timeout: 5000 });
  await features.waitFor({ timeout: 5000 });
  const sitesTag = await sites.evaluate((node) => node.tagName.toLowerCase());
  const featuresTag = await features.evaluate((node) => node.tagName.toLowerCase());
  if (sitesTag !== "textarea") {
    throw new Error(`list input should render as textarea, got ${sitesTag}`);
  }
  if (featuresTag !== "select") {
    throw new Error(`multiselect input should render as select, got ${featuresTag}`);
  }
  const isMultiple = await features.evaluate((node) => node.hasAttribute("multiple"));
  if (!isMultiple) {
    throw new Error("multiselect input should have the multiple attribute");
  }
  await sites.fill("one.example\ntwo.example");
  await features.selectOption(["tls", "cache"]);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  const row = page.locator("#artifact-list .artifact").filter({ hasText: "summary.txt" });
  await row.waitFor({ timeout: 5000 });
  await row.getByRole("button", { name: "Preview" }).click();
  await page.locator("#artifact-preview").filter({ hasText: "sites=one.example,two.example" }).waitFor({ timeout: 5000 });
  await page.locator("#artifact-preview").filter({ hasText: "features=tls,cache" }).waitFor({ timeout: 5000 });
}

async function assertSchemaDrivenForm(page) {
  // The generator button also exposes its description to assistive technology.
  await selectGenerator(page, /^Schema-driven UI\b/);
  const preview = page.locator(".schema-preview");
  await preview.waitFor({ timeout: 5000 });

  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator(".schema-errors").filter({ hasText: "name" }).first().waitFor({ timeout: 5000 });
  if ((await page.locator("#run-state").count()) !== 0) {
    throw new Error("schema validation should prevent an invalid custom form submission");
  }

  const name = page.getByLabel(/^Name/);
  const port = page.locator(".schema-array input[type='number']").first();
  const branch = page.locator(".schema-union > select");
  await name.fill("demo");
  await page.getByRole("button", { name: "Add item" }).click();
  await port.fill("8080");
  await branch.selectOption("1");
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  await page.locator("#artifact-list .artifact").filter({ hasText: "settings.json" }).waitFor({ timeout: 5000 });
}

async function assertSkippedReason(page) {
  await selectGenerator(page, /Branching/);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  await page.locator(".run-details summary").click();
  await page
    .locator(".event-log li")
    .filter({ hasText: "write_b" })
    .filter({ hasText: "when expression evaluated false" })
    .waitFor({ timeout: 5000 });
}

async function assertArtifactPreviewSandbox(page) {
  await selectGenerator(page, /Artifact Preview Sandbox/);
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  const row = page.locator("#artifact-list .artifact").filter({ hasText: "preview.html" });
  await row.waitFor({ timeout: 5000 });
  const titleBeforePreview = await page.title();
  await row.getByRole("button", { name: "Preview" }).click();
  try {
    await page.locator("#artifact-preview").waitFor({ timeout: 5000 });
  } catch (error) {
    throw new Error(`artifact preview did not render; body:\n${await page.locator("body").innerText()}\nlog:\n${Buffer.concat(logChunks).toString()}\n${error}`);
  }
  const frame = page.locator("#artifact-preview iframe");
  await frame.waitFor({ timeout: 5000 });
  if ((await frame.count()) !== 1) {
    throw new Error(`HTML artifact preview should use a sandboxed iframe; preview text: ${await page.locator("#artifact-preview").innerText()}`);
  }
  const sandbox = await frame.getAttribute("sandbox");
  if (sandbox !== "") {
    throw new Error(`artifact preview iframe should have an empty sandbox policy, got ${sandbox}`);
  }
  await page.waitForTimeout(300);
  const title = await page.title();
  if (title !== titleBeforePreview) {
    throw new Error(`artifact preview script escaped sandbox and changed title to ${title}`);
  }

  const jsonRow = page.locator("#artifact-list .artifact").filter({ hasText: "result.json" });
  await jsonRow.getByRole("button", { name: "Preview" }).click();
  await page.locator("#artifact-preview .json-preview").filter({ hasText: '"safe":true' }).waitFor({ timeout: 5000 });

  const markdownRow = page.locator("#artifact-list .artifact").filter({ hasText: "README.md" });
  await markdownRow.getByRole("button", { name: "Preview" }).click();
  await page.locator("#artifact-preview .markdown-preview").filter({ hasText: "# Safe markdown" }).waitFor({ timeout: 5000 });

  const mismatchedRow = page.locator("#artifact-list .artifact").filter({ hasText: "mismatch.txt" });
  await mismatchedRow.getByRole("button", { name: "Preview" }).click();
  await page.locator("#artifact-preview .preview-error").filter({ hasText: "MIME" }).waitFor({ timeout: 5000 });
}

async function assertFileInput(page) {
  await selectGenerator(page, /File Input/);
  const input = page.locator("input[type='file'][name='config_file']");
  await input.waitFor({ timeout: 5000 });
  await input.setInputFiles({
    name: "config.json",
    mimeType: "application/json",
    buffer: Buffer.from('{"enabled":true}\n'),
  });
  await page.getByRole("button", { name: /^Start generation$/ }).click();
  await page.locator("#run-state.succeeded").waitFor({ timeout: 15000 });
  const row = page.locator("#artifact-list .artifact").filter({ hasText: "summary.md" });
  await row.waitFor({ timeout: 5000 });
  await row.getByRole("button", { name: "Preview" }).click();
  await page
    .locator("#artifact-preview")
    .filter({ hasText: "files/config_file/config.json" })
    .waitFor({ timeout: 5000 });
}

async function selectGenerator(page, name) {
  try {
    await page.getByRole("button", { name }).click();
  } catch (error) {
    const body = await page.locator("body").innerText().catch(() => "");
    throw new Error(`generator ${name} was not selectable; body:\n${body}\nlog:\n${Buffer.concat(logChunks).toString()}\n${error}`);
  }
  await page.getByRole("button", { name: /^Start generation$/ }).waitFor({ timeout: 5000 });
}

async function stopProcess(child) {
  if (!child || child.exitCode !== null) return;
  child.kill("SIGTERM");
  await new Promise((resolve) => {
    const timer = setTimeout(resolve, 2000);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
  if (child.exitCode === null) child.kill("SIGKILL");
}

async function mergeGenerators(target) {
  await mkdir(target, { recursive: true });
  for (const source of ["fixtures/generators", "generators"]) {
    for (const entry of await readdir(source)) {
      await rm(join(target, entry), { recursive: true, force: true });
      await symlink(join(process.cwd(), source, entry), join(target, entry));
    }
  }
  const cancelable = join(target, "ui-cancelable");
  await mkdir(cancelable, { recursive: true });
  await writeFile(join(cancelable, "qcg.toml"), `
[generator]
id = "ui-cancelable"
name = "Cancelable UI"
version = "0.1.0"
qcg_version = "^0.1"

[permissions]
side_effects = "allowed"
fs_write = ["workspace"]
commands = [{ bin = "sh", args = ["-c", "sleep 30"], purpose = "UI cancellation test", isolation = "trusted_host" }]

[[flow]]
id = "wait"
type = "command"
[flow.params]
command = ["sh", "-c", "sleep 30"]

[[flow]]
id = "must_not_run"
type = "write"
needs = ["wait"]
artifact = { label = "Unexpected", required = false }
[flow.params]
output_file = "must-not-exist.txt"
content = "unexpected"
`);
  return target;
}

async function waitForUrl(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // Retry until the server is ready.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`server did not become ready at ${url}; log:\n${Buffer.concat(logChunks).toString()}`);
}

try {
  await main();
} finally {
  if (browser) {
    await browser.close();
  }
  await rm(root, { recursive: true, force: true });
}
