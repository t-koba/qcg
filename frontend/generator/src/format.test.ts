import { describe, expect, it } from "vitest";
import { localizedText } from "./format";

describe("localizedText", () => {
  it("selects exact and primary-language translations", () => {
    const translations = { ja: "日本語", "en-US": "US English" };
    expect(localizedText("Fallback", translations, "ja-JP")).toBe("日本語");
    expect(localizedText("Fallback", translations, "en_US")).toBe("US English");
  });

  it("uses the fallback for an unavailable language", () => {
    expect(localizedText("Fallback", { ja: "日本語" }, "fr-FR")).toBe("Fallback");
  });
});
