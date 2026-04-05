import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { parseURL, buildURL, DEFAULTS } from "./url-state.js";

describe("DEFAULTS", () => {
  it("defines expected default values", () => {
    assert.equal(DEFAULTS.edge, "djTransition");
    assert.equal(DEFAULTS.depth, "2");
    assert.equal(DEFAULTS.limit, "10");
    assert.equal(DEFAULTS.month, "");
    assert.equal(DEFAULTS.dj, "");
  });
});

describe("parseURL", () => {
  it("returns defaults when search string is empty", () => {
    const state = parseURL("");
    assert.equal(state.artist, null);
    assert.equal(state.edge, "djTransition");
    assert.equal(state.depth, "2");
    assert.equal(state.limit, "10");
    assert.equal(state.month, "");
    assert.equal(state.dj, "");
  });

  it("parses artist name", () => {
    const state = parseURL("?artist=Autechre");
    assert.equal(state.artist, "Autechre");
  });

  it("decodes URL-encoded artist name", () => {
    const state = parseURL("?artist=Father%20John%20Misty");
    assert.equal(state.artist, "Father John Misty");
  });

  it("returns null artist when param is absent", () => {
    const state = parseURL("?edge=sharedStyle");
    assert.equal(state.artist, null);
  });

  it("parses all params together", () => {
    const state = parseURL("?artist=Stereolab&edge=sharedStyle&depth=1&limit=50");
    assert.equal(state.artist, "Stereolab");
    assert.equal(state.edge, "sharedStyle");
    assert.equal(state.depth, "1");
    assert.equal(state.limit, "50");
  });

  it("falls back to defaults for missing non-artist params", () => {
    const state = parseURL("?artist=Autechre");
    assert.equal(state.edge, "djTransition");
    assert.equal(state.depth, "2");
    assert.equal(state.limit, "10");
  });

  it("ignores unknown params", () => {
    const state = parseURL("?artist=Autechre&foo=bar");
    assert.equal(state.artist, "Autechre");
    assert.equal(Object.hasOwn(state, "foo"), false);
  });
});

describe("buildURL", () => {
  it("returns just artist param when all controls are defaults", () => {
    const url = buildURL("Autechre", { edge: "djTransition", depth: "2", limit: "10" });
    assert.equal(url, "?artist=Autechre");
  });

  it("URL-encodes artist names with spaces", () => {
    const url = buildURL("Father John Misty", { edge: "djTransition", depth: "2", limit: "10" });
    assert.equal(url, "?artist=Father+John+Misty");
  });

  it("includes non-default edge type", () => {
    const url = buildURL("Autechre", { edge: "sharedStyle", depth: "2", limit: "10" });
    assert.equal(url, "?artist=Autechre&edge=sharedStyle");
  });

  it("includes non-default depth", () => {
    const url = buildURL("Autechre", { edge: "djTransition", depth: "1", limit: "10" });
    assert.equal(url, "?artist=Autechre&depth=1");
  });

  it("includes non-default limit", () => {
    const url = buildURL("Autechre", { edge: "djTransition", depth: "2", limit: "50" });
    assert.equal(url, "?artist=Autechre&limit=50");
  });

  it("includes all non-default params", () => {
    const url = buildURL("Stereolab", { edge: "labelFamily", depth: "1", limit: "5" });
    assert.equal(url, "?artist=Stereolab&edge=labelFamily&depth=1&limit=5");
  });

  it("returns pathname when no artist and all defaults", () => {
    const url = buildURL(null, { edge: "djTransition", depth: "2", limit: "10" });
    assert.equal(url, "/");
  });

  it("returns only non-default params when no artist", () => {
    const url = buildURL(null, { edge: "sharedStyle", depth: "2", limit: "10" });
    assert.equal(url, "?edge=sharedStyle");
  });

  it("round-trips with parseURL", () => {
    const controls = { edge: "compilation", depth: "1", limit: "20" };
    const url = buildURL("Cat Power", controls);
    const parsed = parseURL(url);
    assert.equal(parsed.artist, "Cat Power");
    assert.equal(parsed.edge, "compilation");
    assert.equal(parsed.depth, "1");
    assert.equal(parsed.limit, "20");
  });

  it("round-trips defaults through parseURL", () => {
    const url = buildURL("Autechre", { ...DEFAULTS });
    const parsed = parseURL(url);
    assert.equal(parsed.artist, "Autechre");
    assert.equal(parsed.edge, "djTransition");
    assert.equal(parsed.depth, "2");
    assert.equal(parsed.limit, "10");
    assert.equal(parsed.month, "");
    assert.equal(parsed.dj, "");
  });

  it("includes non-default month", () => {
    const url = buildURL("Autechre", { ...DEFAULTS, month: "3" });
    assert.equal(url, "?artist=Autechre&month=3");
  });

  it("includes non-default dj", () => {
    const url = buildURL("Autechre", { ...DEFAULTS, dj: "42" });
    assert.equal(url, "?artist=Autechre&dj=42");
  });

  it("includes both month and dj", () => {
    const url = buildURL("Autechre", { ...DEFAULTS, month: "12", dj: "7" });
    assert.equal(url, "?artist=Autechre&month=12&dj=7");
  });

  it("round-trips facet params", () => {
    const controls = { ...DEFAULTS, month: "6", dj: "3" };
    const url = buildURL("Cat Power", controls);
    const parsed = parseURL(url);
    assert.equal(parsed.artist, "Cat Power");
    assert.equal(parsed.month, "6");
    assert.equal(parsed.dj, "3");
    assert.equal(parsed.edge, "djTransition");
  });
});

describe("parseURL facets", () => {
  it("parses month param", () => {
    const state = parseURL("?artist=Autechre&month=3");
    assert.equal(state.month, "3");
    assert.equal(state.dj, "");
  });

  it("parses dj param", () => {
    const state = parseURL("?artist=Autechre&dj=42");
    assert.equal(state.dj, "42");
    assert.equal(state.month, "");
  });

  it("parses both facet params", () => {
    const state = parseURL("?artist=Autechre&month=12&dj=7");
    assert.equal(state.month, "12");
    assert.equal(state.dj, "7");
  });
});
