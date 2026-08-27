import { execFileSync, spawn } from "node:child_process";
import { mkdtemp, mkdir, readdir, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright";

const qcgPort = Number(process.env.QCG_UI_SMOKE_PORT || 58018);
const uiPort = Number(process.env.QCG_UI_VITE_SMOKE_PORT || 58019);
const root = await mkdtemp(join(tmpdir(), "qcg-ui-smoke."));
let browser;
const logChunks = [];

async function main() {
  execFileSync("npm", ["run", "build"], { cwd: "frontend/generator", stdio: "inherit" });
  browser = await chromium.launch();
  const modes = [
    { name: "vite-proxy", apiPort: qcgPort, frontendPort: uiPort, frontend: "vite" },
    { name: "serve-assets", apiPort: qcgPort + 1, frontend: "assets" },
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
  const apiBase = `http://127.0.0.1:${mode.apiPort}`;
  const frontendOrigin = mode.frontend === "assets"
    ? `${apiBase}/api/generators/generator/assets/ui/index.html`
    : `http://127.0.0.1:${mode.frontendPort}`;
  const qcgArgs = [
    "run",
    "-p",
    "qcg",
    "--",
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
  const qcg = spawn("cargo", qcgArgs, { stdio: ["ignore", "pipe", "pipe"] });
  captureOutput(qcg, `${mode.name} qcg`);
  let frontend;
  let context;
  try {
    await waitForUrl(`${apiBase}/healthz`, 30000);
    if (mode.frontend === "vite") {
      frontend = spawn(
        "npm",
        ["run", "dev", "--", "--host", "127.0.0.1", "--port", String(mode.frontendPort), "--strictPort"],
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
    await assertSuccessfulRun(page);
    await assertQuestionForm(page);
    await assertListMultiselectForm(page);
    await assertSkippedReason(page);
    await assertArtifactPreviewSandbox(page);
    await assertFileInput(page);
    await context.close();
    context = undefined;
  } finally {
    if (context) await context.close();
    await stopProcess(frontend);
    await stopProcess(qcg);
  }
}

function captureOutput(child, label) {
  child?.stdout?.on("data", (chunk) => logChunks.push(Buffer.from(`${label}: ${chunk}`)));
  child?.stderr?.on("data", (chunk) => logChunks.push(Buffer.from(`${label}: ${chunk}`)));
}

async function assertSuccessfulRun(page) {
  await selectGenerator(page, /Hello Template/);
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
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

async function assertQuestionForm(page) {
  await selectGenerator(page, /^Ask User\b/);
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "waiting" }).waitFor({ timeout: 15000 });
  const answer = page.locator("#question-panel [name='answer']");
  await answer.waitFor({ timeout: 5000 });
  const tagName = await answer.evaluate((node) => node.tagName.toLowerCase());
  if (tagName !== "select") {
    throw new Error(`question answer control should be select, got ${tagName}`);
  }
  await answer.selectOption("detailed");
  await page.getByRole("button", { name: /^Answer$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
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
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
  const row = page.locator("#artifact-list .artifact").filter({ hasText: "summary.txt" });
  await row.waitFor({ timeout: 5000 });
  await row.getByRole("button", { name: "Preview" }).click();
  await page.locator("#artifact-preview").filter({ hasText: "sites=one.example,two.example" }).waitFor({ timeout: 5000 });
  await page.locator("#artifact-preview").filter({ hasText: "features=tls,cache" }).waitFor({ timeout: 5000 });
}

async function assertSkippedReason(page) {
  await selectGenerator(page, /Branching/);
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
  await page
    .locator("#node-progress .node.skipped")
    .filter({ hasText: "write_b" })
    .filter({ hasText: "when expression evaluated false" })
    .waitFor({ timeout: 5000 });
}

async function assertArtifactPreviewSandbox(page) {
  await selectGenerator(page, /Artifact Preview Sandbox/);
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
  const row = page.locator("#artifact-list .artifact").filter({ hasText: "preview.html" });
  await row.waitFor({ timeout: 5000 });
  await row.getByRole("button", { name: "Preview" }).click();
  try {
    await page.locator("#artifact-preview").waitFor({ timeout: 5000 });
  } catch (error) {
    throw new Error(`artifact preview did not render; body:\n${await page.locator("body").innerText()}\nlog:\n${Buffer.concat(logChunks).toString()}\n${error}`);
  }
  const frame = page.locator("#artifact-preview iframe");
  if ((await frame.count()) !== 1) {
    throw new Error(`HTML artifact preview should use a sandboxed iframe; preview text: ${await page.locator("#artifact-preview").innerText()}`);
  }
  const sandbox = await frame.getAttribute("sandbox");
  if (sandbox !== "") {
    throw new Error(`artifact preview iframe should have an empty sandbox policy, got ${sandbox}`);
  }
  await page.waitForTimeout(300);
  const title = await page.title();
  if (title !== "qcg") {
    throw new Error(`artifact preview script escaped sandbox and changed title to ${title}`);
  }
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
  await page.getByRole("button", { name: /^Run$/ }).click();
  await page.locator("#run-state").filter({ hasText: "succeeded" }).waitFor({ timeout: 15000 });
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
  await page.locator("#run-state").filter({ hasText: "idle" }).waitFor({ timeout: 5000 });
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
