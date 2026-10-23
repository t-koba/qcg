// HTTP failures must stay QcgError even when the response is not a Problem object.
import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";

const { QcgClient, QcgError } = await import(pathToFileURL(process.env.QCG_TS_CLIENT).href);
const statusText = "Unprocessable Content";
const cases = [
  ["detail wins", '{"detail":"specific","title":"general","extra":42}', "specific"],
  ["title only", '{"title":"general"}', "general"],
  ["empty detail", '{"detail":"","title":"general"}', "general"],
  ["plain text", "upstream failed", "upstream failed"],
  ["empty body", "", statusText],
  ["malformed JSON", '{"detail":', '{"detail":'],
  ["empty object", "{}", statusText],
  ["empty messages", '{"detail":"","title":""}', statusText],
  ["null", "null", statusText],
  ["string", '"primitive"', statusText],
  ["empty string", '""', statusText],
  ["number", "42", statusText],
  ["boolean", "true", statusText],
  ["array", '[{"detail":"nested"}]', statusText],
];
for (const detail of [null, 42, false, [], {}]) {
  cases.push([`wrong detail ${JSON.stringify(detail)}`, JSON.stringify({ detail, title: "general" }), "general"]);
}
for (const title of [null, 42, false, [], {}]) {
  cases.push([`wrong title ${JSON.stringify(title)}`, JSON.stringify({ detail: 42, title }), statusText]);
}

// All five response readers use the shared status-error path.
const readers = [
  ["json", (client) => client.health()],
  ["text", (client) => client.metrics()],
  ["bytes", (client) => client.downloadRunBundle("r1")],
  ["ndjson", (client) => client.readRunJournal("r1")],
  ["sse", async (client) => { for await (const _ of client.streamRunEvents("r1")) { /* drain */ } }],
];
let assertions = 0;
for (const [name, body, message] of cases) {
  let problem = {};
  if (body) {
    try { problem = JSON.parse(body); }
    catch { problem = { detail: body }; }
  }
  for (const [reader, read] of readers) {
    const client = new QcgClient({
      baseUrl: "http://sdk.test",
      fetch: async () => new Response(body, { status: 422, statusText }),
    });
    await assert.rejects(() => read(client), (error) => {
      assert.ok(error instanceof QcgError, `${reader}/${name}: expected QcgError, got ${error}`);
      assert.equal(error.name, "QcgError");
      assert.equal(error.status, 422);
      assert.equal(error.message, message, `${reader}/${name}: message`);
      assert.deepEqual(error.problem, problem, `${reader}/${name}: original payload`);
      return true;
    });
    assertions++;
  }
}
console.log(`TS HTTP problem errors: ${assertions} reader/case checks passed`);
