import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { parseURL, buildURL, DEFAULTS } from "./url-state.js";

describe("DEFAULTS", () => {
  it("defines expected default values", () => {
    assert.equal(DEFAULTS.edge, "djTransition");
    assert.equal(DEFAULTS.depth, "2");
    assert.equal(DEFAULTS.limit, "10");
  });
});

describe("parseURL", () => {
  it("returns defaults when search string is empty", () => {
    const state = parseURL("");
    assert.equal(state.artist, null);
    assert.equal(state.edge, "djTransition");
    assert.equal(state.depth, "2");
    assert.equal(state.limit, "10");
  });

  it("parses artist ID as integer", () => {
    const state = parseURL("?artist=42");
    assert.equal(state.artist, 42);
  });

  it("returns null artist when param is absent", () => {
    const state = parseURL("?edge=sharedStyle");
    assert.equal(state.artist, null);
  });

  it("parses all params together", () => {
    const state = parseURL("?artist=7&edge=sharedStyle&depth=1&limit=50");
    assert.equal(state.artist, 7);
    assert.equal(state.edge, "sharedStyle");
    assert.equal(state.depth, "1");
    assert.equal(state.limit, "50");
  });

  it("falls back to defaults for missing non-artist params", () => {
    const state = parseURL("?artist=7");
    assert.equal(state.edge, "djTransition");
    assert.equal(state.depth, "2");
    assert.equal(state.limit, "10");
  });

  it("ignores unknown params", () => {
    const state = parseURL("?artist=7&foo=bar");
    assert.equal(state.artist, 7);
    assert.equal(state.edge, "djTransition");
    // no foo property
    assert.equal(Object.hasOwn(state, "foo"), false);
  });
});

describe("buildURL", () => {
  it("returns just artist param when all controls are defaults", () => {
    const url = buildURL(42, { edge: "djTransition", depth: "2", limit: "10" });
    assert.equal(url, "?artist=42");
  });

  it("includes non-default edge type", () => {
    const url = buildURL(42, { edge: "sharedStyle", depth: "2", limit: "10" });
    assert.equal(url, "?artist=42&edge=sharedStyle");
  });

  it("includes non-default depth", () => {
    const url = buildURL(42, { edge: "djTransition", depth: "1", limit: "10" });
    assert.equal(url, "?artist=42&depth=1");
  });

  it("includes non-default limit", () => {
    const url = buildURL(42, { edge: "djTransition", depth: "2", limit: "50" });
    assert.equal(url, "?artist=42&limit=50");
  });

  it("includes all non-default params", () => {
    const url = buildURL(7, { edge: "labelFamily", depth: "1", limit: "5" });
    assert.equal(url, "?artist=7&edge=labelFamily&depth=1&limit=5");
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
    const url = buildURL(99, controls);
    const parsed = parseURL(url);
    assert.equal(parsed.artist, 99);
    assert.equal(parsed.edge, "compilation");
    assert.equal(parsed.depth, "1");
    assert.equal(parsed.limit, "20");
  });

  it("round-trips defaults through parseURL", () => {
    const url = buildURL(42, { edge: "djTransition", depth: "2", limit: "10" });
    const parsed = parseURL(url);
    assert.equal(parsed.artist, 42);
    assert.equal(parsed.edge, "djTransition");
    assert.equal(parsed.depth, "2");
    assert.equal(parsed.limit, "10");
  });
});
