%rebase('_base.tpl', title='Gene Browser')

<!-- ── Page header ─────────────────────────────────────────────────────────── -->
<div class="flex flex-wrap items-center justify-between gap-4 mb-6">
  <div>
    <h1 class="text-2xl font-bold text-gray-900">Gene Browser</h1>
    <p class="text-gray-500 text-sm mt-1">
      Gene-centric interpretation browser
      % if pgs_id_filter:
      <span class="ml-2 inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-800 text-xs font-medium">
        Scope: {{pgs_id_filter}}
        <a href="/genes" class="ml-0.5 text-indigo-400 hover:text-indigo-700" title="Clear PGS filter">✕</a>
      </span>
      % else:
      <span class="ml-2 text-xs text-gray-400">Global view · all PGS</span>
      % end
    </p>
  </div>
  <!-- PGS scope selector (server-rendered from available annotation dirs) -->
  % if available_pgs_ids:
  <div class="flex items-center gap-2">
    <label class="text-xs text-gray-500 font-medium">PGS scope:</label>
    <select id="pgs-scope-select"
            class="text-xs border border-gray-300 rounded px-2 py-1.5 focus:outline-none focus:ring-2 focus:ring-indigo-300"
            onchange="gbScopeChange(this.value)">
      <option value="">All PGS</option>
      % for pid in available_pgs_ids:
      <option value="{{pid}}" {{'selected' if pgs_id_filter == pid else ''}}>{{pid}}</option>
      % end
    </select>
  </div>
  % end
</div>

<!-- ── Filter toolbar ──────────────────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow p-4 mb-4">
  <div class="flex flex-wrap gap-3 items-end">

    <!-- Symbol search -->
    <div class="flex-1 min-w-40">
      <label class="block text-xs text-gray-500 mb-1">Gene symbol</label>
      <input id="gb-search" type="text" placeholder="e.g. BRCA1, CFTR…"
             value="{{query or ''}}"
             class="w-full border border-gray-300 rounded px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-300"
             oninput="gbDebounce()" />
    </div>

    <!-- Sort by -->
    <div>
      <label class="block text-xs text-gray-500 mb-1">Sort by</label>
      <select id="gb-sort-by" class="text-xs border border-gray-300 rounded px-2 py-1.5"
              onchange="gbRefresh()">
        <option value="sum_abs_beta">Σ|BETA|</option>
        <option value="ranking_score">Ranking score</option>
        <option value="variant_count">Variant count</option>
        <option value="mean_cadd">Mean CADD</option>
        <option value="alphabetical">Alphabetical</option>
      </select>
    </div>

    <!-- Sort dir -->
    <div>
      <label class="block text-xs text-gray-500 mb-1">Order</label>
      <select id="gb-sort-dir" class="text-xs border border-gray-300 rounded px-2 py-1.5"
              onchange="gbRefresh()">
        <option value="desc">↓ Desc</option>
        <option value="asc">↑ Asc</option>
      </select>
    </div>

    <!-- Min variants -->
    <div>
      <label class="block text-xs text-gray-500 mb-1">Min variants</label>
      <input id="gb-min-var" type="number" min="1" value="1"
             class="w-16 text-xs border border-gray-300 rounded px-2 py-1.5"
             onchange="gbRefresh()" />
    </div>

    <!-- Clinical only -->
    <label class="flex items-center gap-1.5 text-xs text-emerald-800 cursor-pointer px-2 py-1.5 rounded border border-emerald-200 bg-emerald-50 hover:bg-emerald-100 self-end">
      <input type="checkbox" id="gb-clinical-only" class="rounded accent-emerald-600"
             onchange="gbRefresh()" {{'checked' if clinical_only else ''}} />
      ✚ Clinical only
    </label>

    <!-- Clinical confidence -->
    <div class="self-end">
      <select id="gb-confidence" class="text-xs border border-emerald-300 bg-emerald-50 rounded px-2 py-1.5"
              onchange="gbRefresh()">
        <option value="">All evidence</option>
        <option value="high">High (Definitive/Strong)</option>
        <option value="medium">Medium (Moderate)</option>
        <option value="low">Low (Limited)</option>
      </select>
    </div>

    <!-- Page size -->
    <div>
      <label class="block text-xs text-gray-500 mb-1">Per page</label>
      <select id="gb-page-size" class="text-xs border border-gray-300 rounded px-2 py-1.5"
              onchange="gbRefresh()">
        <option value="25">25</option>
        <option value="50" selected>50</option>
        <option value="100">100</option>
        <option value="200">200</option>
      </select>
    </div>

    <!-- Refresh button -->
    <button onclick="gbRefresh()"
            class="self-end text-xs px-4 py-1.5 bg-indigo-600 text-white rounded hover:bg-indigo-700">
      Apply
    </button>
  </div>

  <!-- Counters -->
  <div class="mt-3 flex items-center gap-4 text-xs text-gray-500">
    <span id="gb-count-total"></span>
    <span id="gb-count-filtered" class="text-indigo-600 font-medium"></span>
    <span id="gb-page-info" class="ml-auto"></span>
  </div>
</div>

<!-- ── Gene list (JS-loaded) ───────────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow overflow-hidden mb-6">
  <div class="overflow-x-auto">
    <table class="w-full text-sm border-collapse">
      <thead>
        <tr class="bg-gray-50 text-left">
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b">Gene</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b text-right">Variants</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b text-right">Σ|BETA|</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b text-right">Mean CADD</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b text-right">Ranking</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b">Clinical</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b">Top consequence</th>
          <th class="px-4 py-2.5 font-semibold text-gray-600 border-b"></th>
        </tr>
      </thead>
      <tbody id="gb-tbody">
        <tr><td colspan="8" class="px-4 py-8 text-center text-gray-400">Loading genes…</td></tr>
      </tbody>
    </table>
  </div>
  <!-- Pagination -->
  <div class="px-4 py-2.5 border-t border-gray-100 flex items-center gap-3 text-xs text-gray-500">
    <button id="gb-prev" onclick="gbPage(-1)"
            class="px-2 py-1 rounded border border-gray-300 hover:bg-gray-100 disabled:opacity-40">
      ← Prev
    </button>
    <span id="gb-page-label">Page 1</span>
    <button id="gb-next" onclick="gbPage(1)"
            class="px-2 py-1 rounded border border-gray-300 hover:bg-gray-100 disabled:opacity-40">
      Next →
    </button>
  </div>
</div>

<!-- ── Gene heatmap (Heatmap 1: genes × metrics) ──────────────────────────── -->
<div class="bg-white rounded-lg shadow p-6 mt-6" id="heatmap-section">
  <div class="flex flex-wrap items-center justify-between gap-3 mb-4">
    <div>
      <h2 class="text-sm font-semibold text-gray-700">
        Gene Heatmap
        <span class="ml-1 text-xs font-normal text-gray-400">— top genes × impact metrics</span>
        % if pgs_id_filter:
        <span class="ml-1 text-xs font-normal text-indigo-600">({{pgs_id_filter}} only)</span>
        % else:
        <span class="ml-1 text-xs font-normal text-gray-400">(global · all PGS)</span>
        % end
      </h2>
      <p class="text-xs text-gray-400 mt-0.5">
        Heatmap 1: rows = metrics · columns = genes (sorted by current filters)
      </p>
    </div>
    <div class="flex items-center gap-2">
      <label class="text-xs text-gray-500">Top N:</label>
      <select id="hm-top-n" class="text-xs border border-gray-300 rounded px-2 py-1">
        <option value="30">30</option>
        <option value="60" selected>60</option>
        <option value="100">100</option>
      </select>
      <button onclick="gbLoadHeatmap()"
              class="text-xs px-3 py-1 rounded bg-indigo-50 text-indigo-700 hover:bg-indigo-100 border border-indigo-200">
        Load Heatmap
      </button>
    </div>
  </div>
  <div id="heatmap-status" class="text-xs text-gray-400 mb-3">
    Click "Load Heatmap" to render gene impact matrix.
  </div>
  <div id="chart-heatmap" style="height:280px;display:none;"></div>
</div>

<!-- ── Server-rendered static grid (fallback / no-JS) ────────────────────── -->
% display_genes = [g for g in all_genes if g.get('is_clinical_gene')] if clinical_only else all_genes
<noscript>
<div class="bg-white rounded-lg shadow p-6 mt-4">
  <h2 class="text-sm font-semibold text-gray-700 mb-3">Genes ({{len(display_genes)}})</h2>
  <div class="grid grid-cols-3 md:grid-cols-6 gap-2">
    % for g in display_genes[:120]:
    <a href="/gene/{{g['gene_symbol']}}"
       class="{{'text-xs px-2 py-1.5 bg-emerald-50 text-emerald-800 rounded border border-emerald-200 text-center' if g.get('is_clinical_gene') else 'text-xs px-2 py-1.5 bg-gray-50 text-gray-700 rounded border border-gray-200 text-center'}}">
      {{g['gene_symbol']}}
    </a>
    % end
  </div>
</div>
</noscript>

<script>
(function () {
  "use strict";

  // ── State ─────────────────────────────────────────────────────────────────
  var gbPage_     = 1;
  var gbTotal_    = 1;
  var gbDebTimer  = null;
  var PGS_FILTER  = "{{pgs_id_filter or ''}}";

  // ── Helpers ───────────────────────────────────────────────────────────────
  function val(id)     { var e = document.getElementById(id); return e ? e.value   : ''; }
  function checked(id) { var e = document.getElementById(id); return e ? e.checked : false; }
  function setText(id, t) { var e = document.getElementById(id); if (e) e.textContent = t || ''; }

  function gbBuildUrl(page) {
    var q    = val('gb-search').trim().toUpperCase();
    var sort = val('gb-sort-by')   || 'sum_abs_beta';
    var dir  = val('gb-sort-dir')  || 'desc';
    var minv = parseInt(val('gb-min-var'), 10) || 1;
    var ps   = parseInt(val('gb-page-size'), 10) || 50;
    var conf = val('gb-confidence');
    var clin = checked('gb-clinical-only') ? '1' : '';

    var url = '/api/genes?page=' + page
      + '&page_size=' + ps
      + '&sort_by='   + encodeURIComponent(sort)
      + '&sort_dir='  + encodeURIComponent(dir)
      + '&min_variants=' + minv;
    if (q)         url += '&q='                    + encodeURIComponent(q);
    if (clin)      url += '&clinical_only=1';
    if (conf)      url += '&clinical_confidence='  + encodeURIComponent(conf);
    if (PGS_FILTER) url += '&pgs_id='             + encodeURIComponent(PGS_FILTER);
    return url;
  }

  // ── Render gene table ─────────────────────────────────────────────────────
  var SCORE_CLS = function(s) {
    return s >= 0.6 ? 'bg-red-100 text-red-700 font-semibold'
         : s >= 0.4 ? 'bg-orange-100 text-orange-700'
         :             'bg-gray-100 text-gray-500';
  };

  function gbRenderRow(g) {
    var score  = Number(g.ranking_score_mean || 0);
    var beta   = Number(g.sum_abs_beta || 0);
    var cadd   = g.mean_cadd != null ? Number(g.mean_cadd).toFixed(1) : '—';

    // Top consequence
    var cc = g.consequence_counts || {};
    var topCsq = '', maxCnt = 0;
    Object.keys(cc).forEach(function(k) { if (cc[k] > maxCnt) { maxCnt = cc[k]; topCsq = k; } });

    var clinBadge = g.is_clinical_gene
      ? '<span class="px-1.5 py-0.5 rounded text-[10px] bg-emerald-100 text-emerald-800 border border-emerald-200">✚ Clinical</span>'
      : '<span class="text-gray-300 text-xs">—</span>';

    return '<tr class="border-b border-gray-50 hover:bg-gray-50">'
      + '<td class="px-4 py-2 font-semibold text-indigo-700">'
      +   '<a href="/gene/' + encodeURIComponent(g.gene_name) + '" class="hover:underline">'
      +   g.gene_name + '</a></td>'
      + '<td class="px-4 py-2 text-right font-mono text-gray-700">' + (g.n_variants||0).toLocaleString() + '</td>'
      + '<td class="px-4 py-2 text-right font-mono text-gray-700">' + beta.toFixed(4) + '</td>'
      + '<td class="px-4 py-2 text-right font-mono text-gray-500">' + cadd + '</td>'
      + '<td class="px-4 py-2 text-center">'
      +   '<span class="px-1.5 py-0.5 rounded text-xs ' + SCORE_CLS(score) + '">' + score.toFixed(3) + '</span>'
      + '</td>'
      + '<td class="px-4 py-2">' + clinBadge + '</td>'
      + '<td class="px-4 py-2 text-gray-400 text-xs truncate max-w-32" title="' + topCsq + '">' + topCsq + '</td>'
      + '<td class="px-4 py-2">'
      +   '<a href="/gene/' + encodeURIComponent(g.gene_name) + '"'
      +   ' class="text-xs px-2 py-0.5 rounded bg-indigo-50 text-indigo-700 hover:bg-indigo-100">Open →</a>'
      + '</td>'
      + '</tr>';
  }

  function gbLoad() {
    var tbody = document.getElementById('gb-tbody');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-gray-400">Loading…</td></tr>';

    fetch(gbBuildUrl(gbPage_))
      .then(function(r) { return r.json(); })
      .then(function(d) {
        if (d.error) {
          tbody.innerHTML = '<tr><td colspan="8" class="px-4 py-6 text-center text-red-500">' + d.error + '</td></tr>';
          return;
        }
        gbTotal_ = d.total_pages || 1;

        setText('gb-count-total',    'Total genes: ' + (d.total_genes || 0).toLocaleString());
        setText('gb-count-filtered', 'Filtered: '    + (d.total_filtered || 0).toLocaleString());
        setText('gb-page-info',      'Page ' + (d.page || 1) + ' / ' + gbTotal_);
        setText('gb-page-label',     'Page ' + (d.page || 1) + ' / ' + gbTotal_);

        var prev = document.getElementById('gb-prev');
        var next = document.getElementById('gb-next');
        if (prev) prev.disabled = (d.page || 1) <= 1;
        if (next) next.disabled = (d.page || 1) >= gbTotal_;

        var rows = d.genes || [];
        if (rows.length === 0) {
          var emptyMsg;
          if (d.total_genes === 0) {
            emptyMsg = 'No annotated genes found. Run the annotation pipeline first.';
          } else {
            var hints = [];
            var q = val('gb-search').trim();
            if (q)                        hints.push('search term "' + q + '"');
            if (checked('gb-clinical-only')) hints.push('Clinical only');
            var minv = parseInt(val('gb-min-var'), 10) || 1;
            if (minv > 1)                 hints.push('Min variants ≥ ' + minv);
            emptyMsg = hints.length
              ? 'No genes match: ' + hints.join(' + ') + '. Try relaxing filters.'
              : 'No rows after applying current filters.';
          }
          tbody.innerHTML = '<tr><td colspan="8" class="px-4 py-6 text-center text-gray-400">' + emptyMsg + '</td></tr>';
          return;
        }
        tbody.innerHTML = rows.map(gbRenderRow).join('');
      })
      .catch(function(err) {
        tbody.innerHTML = '<tr><td colspan="8" class="px-4 py-6 text-center text-red-500">Error: ' + err.message + '</td></tr>';
      });
  }

  // ── Controls ──────────────────────────────────────────────────────────────
  window.gbRefresh = function() { gbPage_ = 1; gbLoad(); };
  window.gbDebounce = function() {
    clearTimeout(gbDebTimer);
    gbDebTimer = setTimeout(window.gbRefresh, 350);
  };
  window.gbPage = function(delta) {
    var next = gbPage_ + delta;
    if (next < 1 || next > gbTotal_) return;
    gbPage_ = next;
    gbLoad();
  };
  window.gbScopeChange = function(pid) {
    var base = '/genes';
    if (pid) base += '?pgs_id=' + encodeURIComponent(pid);
    window.location.href = base;
  };

  // ── Heatmap ───────────────────────────────────────────────────────────────
  window.gbLoadHeatmap = function() {
    var statusEl = document.getElementById('heatmap-status');
    var chartEl  = document.getElementById('chart-heatmap');
    if (statusEl) statusEl.textContent = 'Loading heatmap…';

    var topN  = parseInt(val('hm-top-n'), 10) || 60;
    var sort  = val('gb-sort-by')  || 'sum_abs_beta';
    var dir   = val('gb-sort-dir') || 'desc';
    var conf  = val('gb-confidence');
    var clin  = checked('gb-clinical-only') ? '1' : '';
    var minv  = parseInt(val('gb-min-var'), 10) || 1;

    var url = '/api/genes/summary?top_n=' + topN
      + '&sort_by='      + encodeURIComponent(sort)
      + '&sort_dir='     + encodeURIComponent(dir)
      + '&min_variants=' + minv;
    if (clin)       url += '&clinical_only=1';
    if (conf)       url += '&clinical_confidence=' + encodeURIComponent(conf);
    if (PGS_FILTER) url += '&pgs_id='             + encodeURIComponent(PGS_FILTER);

    fetch(url)
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var hm     = data.heatmap || {};
        var genes  = hm.gene_names || [];
        var metrics= hm.metrics    || [];
        var z      = hm.z          || [];

        if (genes.length === 0) {
          if (statusEl) statusEl.textContent = 'No gene data available.';
          return;
        }

        var metricLabels = {
          n_variants:    'Variants (norm)',
          sum_abs_beta:  'Σ|BETA| (norm)',
          mean_cadd:     'Mean CADD (norm)',
          ranking_score: 'Ranking Score',
        };
        var yLabels = metrics.map(function(m) { return metricLabels[m] || m; });

        if (chartEl) {
          chartEl.style.display = 'block';
          Plotly.newPlot(chartEl, [{
            type:         'heatmap',
            x:            genes,
            y:            yLabels,
            z:            z,
            colorscale:   'Reds',
            showscale:    true,
            hovertemplate: '<b>%{x}</b><br>%{y}: %{z:.3f}<extra></extra>',
          }], {
            margin:        { t: 10, b: 80, l: 130, r: 20 },
            xaxis:         { tickangle: -45, tickfont: { size: 9 } },
            yaxis:         { tickfont: { size: 10 } },
            paper_bgcolor: 'white',
            plot_bgcolor:  'white',
          }, { responsive: true, displayModeBar: false });
        }

        if (statusEl) statusEl.textContent =
          'Showing top ' + genes.length + ' of ' + (data.total_filtered || data.total_genes || '?') + ' genes.';
      })
      .catch(function(err) {
        if (statusEl) statusEl.textContent = 'Error loading heatmap: ' + err.message;
      });
  };

  // ── Bootstrap ─────────────────────────────────────────────────────────────
  gbLoad();

})();
</script>
