import { render } from "svelte/server";
import { expect, it } from "vitest";
import type { FormSpec } from "./api/client";
import QuestionPanel from "./components/QuestionPanel.svelte";
import { currentMessages } from "./messages";
import { RunStore } from "./run-store.svelte";

it("renders a stored null answer instead of the field default", () => {
  const store = new RunStore();
  store.question = {
    id: "question-1",
    title: "Question",
    fields: [{
      id: "nullable",
      type: "nullable",
      label: "Nullable answer",
      schema: { oneOf: [{ type: "null", title: "Nullable answer" }, { type: "string", title: "Text" }] },
      default: "fallback",
    }],
  } as unknown as FormSpec;
  store.values = { "question:nullable": null };

  const rendered = render(QuestionPanel, {
    props: { store, messages: currentMessages("en"), language: "en" },
  });

  expect(rendered.body).toContain('<output aria-label="Nullable answer">null</output>');
});
