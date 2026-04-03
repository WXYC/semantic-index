/** URL state management for the graph explorer. */

export const DEFAULTS = { edge: "djTransition", depth: "2", limit: "10" };

/**
 * Parse URL search string into graph state.
 * @param {string} search - URL search string (e.g. "?artist=42&edge=sharedStyle")
 * @returns {{ artist: number|null, edge: string, depth: string, limit: string }}
 */
export function parseURL(search) {
  const p = new URLSearchParams(search);
  return {
    artist: p.has("artist") ? parseInt(p.get("artist"), 10) : null,
    edge: p.get("edge") || DEFAULTS.edge,
    depth: p.get("depth") || DEFAULTS.depth,
    limit: p.get("limit") || DEFAULTS.limit,
  };
}

/**
 * Build a URL query string from graph state. Only includes non-default values.
 * @param {number|null} artistId
 * @param {{ edge: string, depth: string, limit: string }} controls
 * @returns {string} URL path with query string, e.g. "?artist=42&edge=sharedStyle"
 */
export function buildURL(artistId, controls) {
  const p = new URLSearchParams();
  if (artistId != null) p.set("artist", String(artistId));
  if (controls.edge !== DEFAULTS.edge) p.set("edge", controls.edge);
  if (controls.depth !== DEFAULTS.depth) p.set("depth", controls.depth);
  if (controls.limit !== DEFAULTS.limit) p.set("limit", controls.limit);
  const qs = p.toString();
  return qs ? `?${qs}` : "/";
}
