/** URL state management for the graph explorer. */

export const DEFAULTS = { edge: "djTransition", depth: "2", limit: "10", month: "", dj: "", heat: "0.5" };

/**
 * Parse URL search string into graph state.
 * @param {string} search - URL search string (e.g. "?artist=Autechre&edge=sharedStyle")
 */
export function parseURL(search) {
  const p = new URLSearchParams(search);
  return {
    artist: p.get("artist") || null,
    edge: p.get("edge") || DEFAULTS.edge,
    depth: p.get("depth") || DEFAULTS.depth,
    limit: p.get("limit") || DEFAULTS.limit,
    month: p.get("month") || DEFAULTS.month,
    dj: p.get("dj") || DEFAULTS.dj,
    heat: p.get("heat") || DEFAULTS.heat,
  };
}

/**
 * Build a URL query string from graph state. Only includes non-default values.
 * @param {string|null} artistName
 * @param {object} controls
 * @returns {string} URL path with query string, e.g. "?artist=Autechre&edge=sharedStyle"
 */
export function buildURL(artistName, controls) {
  const p = new URLSearchParams();
  if (artistName != null) p.set("artist", artistName);
  if (controls.edge !== DEFAULTS.edge) p.set("edge", controls.edge);
  if (controls.depth !== DEFAULTS.depth) p.set("depth", controls.depth);
  if (controls.limit !== DEFAULTS.limit) p.set("limit", controls.limit);
  if (controls.month && controls.month !== DEFAULTS.month) p.set("month", controls.month);
  if (controls.dj && controls.dj !== DEFAULTS.dj) p.set("dj", controls.dj);
  if (controls.heat && controls.heat !== DEFAULTS.heat) p.set("heat", controls.heat);
  const qs = p.toString();
  return qs ? `?${qs}` : "/";
}
