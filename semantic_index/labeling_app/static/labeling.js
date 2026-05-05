// Single-page labeling UI. Tiny vanilla JS — no framework.
// State lives in module-locals; persistence is the server (POST per save).
//
// Keyboard:
//   j / Tab         next row
//   k / Shift+Tab   prev row
//   1 / 2 / 3       severity = severe / minor / not_wrong
//   Cmd+Enter       save current row
//
// Labeler identity is a name typed once; stored in localStorage so a page
// reload doesn't lose it. "Change labeler" wipes it and prompts again.

const LS_KEY = "wxyc-labeler";

let state = {
  labeler: null,
  rows: [],          // [{row_id, pair, cell_id, insufficient_signal, my_label}]
  currentId: null,   // selected row_id
  detail: null,      // { row, my_label }
  saving: false,
};

function $(sel) { return document.querySelector(sel); }
function $$(sel) { return [...document.querySelectorAll(sel)]; }

function setStatus(msg, isError) {
  const el = $("#save-status");
  el.textContent = msg;
  el.style.color = isError ? "#a33" : "#2a7a2a";
}

async function ensureLabeler(force) {
  if (!force) {
    const cached = localStorage.getItem(LS_KEY);
    if (cached) { state.labeler = cached; return; }
  }
  let name = (prompt("Your name (used to keep your labels separate from other labelers):") || "").trim();
  while (!name) {
    name = (prompt("Name is required:") || "").trim();
  }
  state.labeler = name;
  localStorage.setItem(LS_KEY, name);
}

function renderHeader() {
  $("#labeler-display").textContent = "labeler: " + state.labeler;
  $("#export-link").href = `/api/export.csv?labeler=${encodeURIComponent(state.labeler)}`;
}

async function loadRows() {
  const resp = await fetch(`/api/rows?labeler=${encodeURIComponent(state.labeler)}`);
  const data = await resp.json();
  state.rows = data.rows;
  renderRowList();
  renderProgress(data.labeled, data.total);
}

function renderRowList() {
  const list = $("#row-list");
  list.innerHTML = "";
  for (const r of state.rows) {
    const div = document.createElement("div");
    div.className = "row-link" + (r.my_label ? " labeled" : "") + (r.row_id === state.currentId ? " active" : "");
    div.dataset.rowId = r.row_id;
    div.innerHTML = `
      <span class="rid">${r.row_id}</span>
      <span class="cell">${r.cell_id || ""}</span>
      <span class="pair">${escapeHtml(r.pair)}</span>
    `;
    div.addEventListener("click", () => selectRow(r.row_id));
    list.appendChild(div);
  }
}

function renderProgress(labeled, total) {
  $("#progress").textContent = `${labeled} / ${total} labeled`;
}

async function selectRow(rowId) {
  state.currentId = rowId;
  $$("#row-list .row-link").forEach((el) => {
    el.classList.toggle("active", el.dataset.rowId === rowId);
  });
  const resp = await fetch(`/api/rows/${rowId}?labeler=${encodeURIComponent(state.labeler)}`);
  if (!resp.ok) {
    setStatus("could not load row", true);
    return;
  }
  state.detail = await resp.json();
  renderRowDetail();
  // Scroll the active item into view in the row list.
  $$("#row-list .row-link.active")[0]?.scrollIntoView({ block: "nearest" });
}

function renderRowDetail() {
  $("#row-empty").hidden = true;
  $("#row-card").hidden = false;
  const { row, my_label } = state.detail;
  $("#row-id").textContent = row.row_id;
  $("#row-cell").textContent = row.cell_id || "";
  $("#row-insufficient").hidden = !row.insufficient_signal;
  $("#row-pair").textContent = row.pair;
  $("#row-narrative").textContent = row.narrative || "(no narrative)";
  fillArtistDl($("#row-source-data"), row.source_data);
  fillArtistDl($("#row-target-data"), row.target_data);

  const ul = $("#row-neighbors");
  ul.innerHTML = "";
  for (const n of row.shared_neighbors || []) {
    const li = document.createElement("li");
    const name = document.createElement("span");
    name.textContent = n.name;
    const score = document.createElement("span");
    score.className = "score";
    score.textContent = (n.aa_score ?? 0).toFixed(2);
    score.title = "Adamic-Adar score: how strongly this neighbor links the two artists.";
    li.append(name, score);
    ul.appendChild(li);
  }
  if (!ul.children.length) {
    const li = document.createElement("li");
    li.className = "empty";
    li.textContent = "no shared neighbors recorded";
    ul.appendChild(li);
  }

  // Restore form state from saved label (or clear it).
  $$("#label-form input[name=severity]").forEach((r) => { r.checked = my_label && r.value === my_label.severity; });
  $$("#label-form input[name=failure_mode]").forEach((r) => { r.checked = my_label && r.value === my_label.failure_mode; });
  $("#label-form textarea[name=notes]").value = my_label?.notes || "";
  setStatus(my_label ? "saved earlier" : "", false);
  updateFailureModeAvailability();
}

// Map raw JSONL keys to plain-English labels. Order in this list is the
// display order; keys absent from the metadata are simply skipped.
const TOP_LEVEL_FIELDS = [
  ["total_plays", "WXYC plays"],
  ["genre", "WXYC genre"],
  ["styles", "Discogs styles"],
];
const AUDIO_FIELDS = [
  ["primary_genre", "Sounds like"],
  ["danceability", "Danceability"],
  ["top_moods", "Top moods"],
  ["key", "Musical key"],
  ["recording_count", "Recordings analyzed"],
];

function fillArtistDl(dl, meta) {
  dl.innerHTML = "";
  if (!meta) {
    const empty = document.createElement("dd");
    empty.className = "empty";
    empty.textContent = "no metadata";
    dl.appendChild(empty);
    return;
  }
  for (const [key, label] of TOP_LEVEL_FIELDS) {
    appendDlRow(dl, label, meta[key]);
  }
  if (meta.audio) {
    for (const [key, label] of AUDIO_FIELDS) {
      appendDlRow(dl, label, meta.audio[key]);
    }
  }
}

function appendDlRow(dl, label, value) {
  if (value == null) return;
  if (Array.isArray(value)) {
    if (!value.length) return;
    value = value.join(", ");
  }
  const dt = document.createElement("dt");
  dt.textContent = label;
  const dd = document.createElement("dd");
  dd.textContent = String(value);
  dl.append(dt, dd);
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);
}

function getSelectedSeverity() {
  return $$("#label-form input[name=severity]").find((r) => r.checked)?.value || "";
}

function getSelectedFailureMode() {
  return $$("#label-form input[name=failure_mode]").find((r) => r.checked)?.value || "";
}

function updateFailureModeAvailability() {
  const sev = getSelectedSeverity();
  const fs = $("#fm-fieldset");
  const isNotWrong = sev === "not_wrong";
  fs.classList.toggle("disabled", isNotWrong);
  if (isNotWrong) {
    $$("#label-form input[name=failure_mode]").forEach((r) => { r.checked = false; });
  }
}

async function saveLabel(e) {
  e?.preventDefault?.();
  if (state.saving || !state.currentId) return;
  const severity = getSelectedSeverity();
  if (!severity) { setStatus("pick a severity first", true); return; }
  const failure_mode = severity === "not_wrong" ? "" : getSelectedFailureMode();
  if (severity !== "not_wrong" && !failure_mode) {
    setStatus("severe/minor needs a failure_mode", true);
    return;
  }
  state.saving = true;
  setStatus("saving…");
  const notes = $("#label-form textarea[name=notes]").value;
  const resp = await fetch(`/api/rows/${state.currentId}/label`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ labeler: state.labeler, severity, failure_mode, notes }),
  });
  state.saving = false;
  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    setStatus(err.detail || `save failed (${resp.status})`, true);
    return;
  }
  setStatus("saved");
  // Update local row list state without re-fetching the entire list.
  const rec = state.rows.find((r) => r.row_id === state.currentId);
  if (rec) rec.my_label = { severity, failure_mode, notes };
  renderRowList();
  renderProgress(state.rows.filter((r) => r.my_label).length, state.rows.length);
  advance(1);
}

function advance(direction) {
  if (!state.rows.length) return;
  const idx = state.rows.findIndex((r) => r.row_id === state.currentId);
  if (idx === -1) { selectRow(state.rows[0].row_id); return; }
  const next = (idx + direction + state.rows.length) % state.rows.length;
  selectRow(state.rows[next].row_id);
}

function bindEvents() {
  $("#change-labeler").addEventListener("click", async () => {
    localStorage.removeItem(LS_KEY);
    await ensureLabeler(true);
    renderHeader();
    await loadRows();
  });
  $("#label-form").addEventListener("submit", saveLabel);
  $("#prev-button").addEventListener("click", () => advance(-1));
  $("#next-button").addEventListener("click", () => advance(1));
  $$("#label-form input[name=severity]").forEach((r) =>
    r.addEventListener("change", updateFailureModeAvailability)
  );
  document.addEventListener("keydown", (e) => {
    // Don't intercept while typing in the notes textarea.
    if (e.target instanceof HTMLTextAreaElement) {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); saveLabel(); }
      return;
    }
    if (e.key === "j" || (e.key === "Tab" && !e.shiftKey)) { e.preventDefault(); advance(1); }
    else if (e.key === "k" || (e.key === "Tab" && e.shiftKey)) { e.preventDefault(); advance(-1); }
    else if (e.key === "1") { selectSeverity("severe"); }
    else if (e.key === "2") { selectSeverity("minor"); }
    else if (e.key === "3") { selectSeverity("not_wrong"); }
    else if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); saveLabel(); }
  });
}

function selectSeverity(value) {
  const radio = $$("#label-form input[name=severity]").find((r) => r.value === value);
  if (radio) {
    radio.checked = true;
    updateFailureModeAvailability();
  }
}

(async function main() {
  await ensureLabeler(false);
  renderHeader();
  bindEvents();
  await loadRows();
  if (state.rows.length) {
    // Jump to the first unlabeled row, falling back to the first row.
    const firstUnlabeled = state.rows.find((r) => !r.my_label);
    selectRow((firstUnlabeled || state.rows[0]).row_id);
  }
})();
