/**
 * PGS Dashboard – client-side utilities
 *
 * Deliberately minimal: most rendering is server-side.
 * JS only handles:
 *   1. Local catalog table filtering (index page)
 *   2. Histogram rendering is inlined in dashboard.tpl (uses Plotly)
 */

"use strict";

// ── Local catalog filter ────────────────────────────────────────────────────

/**
 * Filter catalog table rows by matching the search term against
 * the data-search attribute of each row.
 * Called by the input's oninput handler in index.tpl.
 */
function filterTable(term) {
  const rows = document.querySelectorAll(".catalog-row");
  const t = term.trim().toLowerCase();
  let visible = 0;

  rows.forEach(row => {
    const haystack = (row.dataset.search || "").toLowerCase();
    const match = !t || haystack.includes(t);
    row.style.display = match ? "" : "none";
    if (match) visible++;
  });

  const noResults = document.getElementById("noFilterResults");
  if (noResults) {
    noResults.classList.toggle("hidden", visible > 0 || !t);
  }
}

// ── Generic JSON fetch helper ────────────────────────────────────────────────

/**
 * Fetch JSON from a URL and call onSuccess(data) or onError(msg).
 * @param {string} url
 * @param {function} onSuccess
 * @param {function} [onError]
 */
function fetchJSON(url, onSuccess, onError) {
  fetch(url)
    .then(r => {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    })
    .then(onSuccess)
    .catch(err => {
      if (onError) onError(err.message || String(err));
    });
}

// ── Copy-to-clipboard helper (used by pipeline plan page) ───────────────────

function copyText(text, btn) {
  navigator.clipboard.writeText(text).then(() => {
    const orig = btn.textContent;
    btn.textContent = "Copied!";
    setTimeout(() => { btn.textContent = orig; }, 1500);
  });
}
