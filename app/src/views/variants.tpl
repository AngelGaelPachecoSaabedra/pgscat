%rebase('_base.tpl', title='Variant Annotation — ' + pgs_id)

<!-- Breadcrumb -->
<nav class="text-sm text-gray-500 mb-4">
  <a href="/" class="hover:text-indigo-600">Catalog</a>
  <span class="mx-1">›</span>
  <a href="/dashboard/{{pgs_id}}" class="hover:text-indigo-600">{{pgs_id}}</a>
  <span class="mx-1">›</span>
  <span class="text-gray-800 font-medium">Variant Annotation</span>
</nav>

<!-- Page header -->
<div class="flex flex-wrap items-center justify-between gap-4 mb-6">
  <div>
    <h1 class="text-2xl font-bold text-gray-900">Variant Annotation</h1>
    <p class="text-gray-500 text-sm mt-1">{{pgs_id}} — Variant Annotator v1.2 · GENCODE v49 · GRCh38</p>
  </div>
  % if status_info.get('status') == 'annotated':
  <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800">
    ✓ Annotated
  </span>
  % elif status_info.get('status') == 'running':
  <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-yellow-100 text-yellow-800">
    ⏳ Running
  </span>
  % elif status_info.get('status') == 'partial':
  <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-orange-100 text-orange-800">
    ⚠ Partial
  </span>
  % else:
  <span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-gray-100 text-gray-600">
    ○ Not annotated
  </span>
  % end
</div>

% if status_info.get('status') == 'annotated':

<!-- Error banner (hidden by default, shown on fetch failure) -->
<div id="va-error-banner" class="hidden bg-red-50 border border-red-200 rounded-lg p-3 mb-4 text-sm text-red-700"></div>

<!-- ── Summary cards ──────────────────────────────────────────────────────── -->
<div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-indigo-700" id="card-total">—</div>
    <div class="text-xs text-gray-500 mt-1">Total Variants</div>
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-emerald-600" id="card-coding">—</div>
    <div class="text-xs text-gray-500 mt-1">Coding</div>
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-slate-600" id="card-intergenic">—</div>
    <div class="text-xs text-gray-500 mt-1">Intergenic</div>
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-violet-600" id="card-intronic">—</div>
    <div class="text-xs text-gray-500 mt-1">Intronic</div>
  </div>
</div>

<!-- ── Charts row ─────────────────────────────────────────────────────────── -->
<div class="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
  <div class="bg-white rounded-lg shadow p-4">
    <h2 class="text-sm font-semibold text-gray-700 mb-3">Region Class Distribution</h2>
    <div id="chart-region" style="height:260px;"></div>
  </div>
  <div class="bg-white rounded-lg shadow p-4">
    <h2 class="text-sm font-semibold text-gray-700 mb-3">Variants per Chromosome</h2>
    <div id="chart-chrom" style="height:260px;"></div>
  </div>
</div>

<!-- ── Metadata panel (always visible when annotated) ─────────────────────── -->
<div class="bg-white rounded-lg shadow p-4 mb-6 text-sm text-gray-600">
  <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
    <div>
      <span class="font-medium text-gray-800 block mb-1">Annotated at</span>
      <span id="meta-date" class="text-xs">—</span>
    </div>
    <div>
      <span class="font-medium text-gray-800 block mb-1">GFF3 reference</span>
      <span id="meta-gff3" class="text-xs break-all">—</span>
    </div>
    <div>
      <span class="font-medium text-gray-800 block mb-1">Elapsed</span>
      <span id="meta-elapsed" class="text-xs">—</span>
    </div>
    <div>
      <span class="font-medium text-gray-800 block mb-1">Tool</span>
      <span id="meta-tool" class="text-xs">—</span>
    </div>
  </div>
</div>

<!-- ── Annotation scope notice (populated dynamically from summary JSON) ── -->
<div id="scope-notice" class="bg-blue-50 border border-blue-200 rounded-lg p-3 mb-6 text-sm text-blue-800">
  <!-- filled by fillScopeNotice() once summary JSON is loaded -->
  <span class="text-blue-400">Loading annotation scope…</span>
</div>

<!-- ── Variant table ─────────────────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow overflow-hidden">
  <div class="px-4 py-3 border-b border-gray-200 flex flex-wrap items-center gap-3">
    <h2 class="text-sm font-semibold text-gray-700 flex-1">Annotated Variants</h2>
    <select id="filter-chrom" class="text-xs border border-gray-300 rounded px-2 py-1"
            onchange="vaApplyFilters()">
      <option value="">All chromosomes</option>
      % for c in ([str(i) for i in range(1,23)] + ['X','Y','MT']):
      <option value="{{c}}">chr{{c}}</option>
      % end
    </select>
    <select id="filter-region" class="text-xs border border-gray-300 rounded px-2 py-1"
            onchange="vaApplyFilters()">
      <option value="">All regions</option>
      <option value="coding">coding</option>
      <option value="UTR_5prime">UTR_5prime</option>
      <option value="UTR_3prime">UTR_3prime</option>
      <option value="intronic">intronic</option>
      <option value="splice_region">splice site / region</option>
      <option value="non_coding_exon">non_coding_exon</option>
      <option value="near_gene">near_gene</option>
      <option value="regulatory">regulatory</option>
      <option value="intergenic">intergenic</option>
    </select>
    <input id="filter-gene" type="text" placeholder="Gene name…"
           class="text-xs border border-gray-300 rounded px-2 py-1 w-28"
           oninput="vaDebouncedFilter()" />
    <!-- Clinical confidence filter -->
    <select id="filter-confidence" class="text-xs border border-emerald-300 bg-emerald-50 rounded px-2 py-1"
            onchange="vaApplyFilters()">
      <option value="">All clinical evidence</option>
      <option value="high">✚ High (Definitive/Strong)</option>
      <option value="medium">◆ Medium (Moderate)</option>
      <option value="low">○ Low (Limited)</option>
    </select>
    <!-- Ranking mode toggle -->
    <label class="flex items-center gap-1.5 text-xs text-indigo-800 cursor-pointer px-2 py-1 rounded border border-indigo-200 bg-indigo-50 hover:bg-indigo-100">
      <input type="checkbox" id="toggle-ranking" class="rounded accent-indigo-600"
             onchange="vaApplyFilters()" />
      Sort by ranking score
    </label>
    <span id="table-count" class="text-xs text-gray-400 ml-auto"></span>
  </div>

  <div class="overflow-x-auto">
    <table class="w-full text-xs text-left">
      <thead class="bg-gray-50 text-gray-600 uppercase tracking-wide">
        <tr>
          <th class="px-3 py-2">CHROM</th>
          <th class="px-3 py-2">POS</th>
          <th class="px-3 py-2">ID</th>
          <th class="px-3 py-2">Effect</th>
          <th class="px-3 py-2">Other</th>
          <th class="px-3 py-2">BETA</th>
          <th class="px-3 py-2 text-center" title="IS_FLIP=1: effect allele is on REF strand (dosage = 2 − DS)">Flip</th>
          <th class="px-3 py-2">Gene</th>
          <th class="px-3 py-2">Gene type</th>
          <th class="px-3 py-2">Region</th>
          <th class="px-3 py-2">Consequence</th>
          <th class="px-3 py-2 text-center" title="bp to nearest exon boundary">Spl.dist</th>
          <th class="px-3 py-2 text-center" title="Amino acid change (FASTA-backed)">AA change</th>
          <th class="px-3 py-2 text-center" title="CADD phred score">CADD</th>
          <th class="px-3 py-2 text-center" title="Clinical priority ranking score [0–1]">Score</th>
          <th class="px-3 py-2">Flags</th>
        </tr>
      </thead>
      <tbody id="variant-tbody" class="divide-y divide-gray-100 text-gray-700">
        <tr><td colspan="17" class="px-3 py-8 text-center text-gray-400">Loading variants…</td></tr>
      </tbody>
    </table>
  </div>

  <div class="px-4 py-3 border-t border-gray-200 flex items-center gap-3 text-xs text-gray-500">
    <button id="btn-prev" onclick="vaChangePage(-1)"
            class="px-2 py-1 rounded border border-gray-300 hover:bg-gray-100 disabled:opacity-40">
      ← Prev
    </button>
    <span id="page-info">Page 1</span>
    <button id="btn-next" onclick="vaChangePage(1)"
            class="px-2 py-1 rounded border border-gray-300 hover:bg-gray-100 disabled:opacity-40">
      Next →
    </button>
    <a href="/api/variants/{{pgs_id}}/summary" target="_blank"
       class="ml-auto text-indigo-600 hover:underline">JSON summary ↗</a>
    <a href="/api/variants/{{pgs_id}}/ideogram" target="_blank"
       class="text-indigo-600 hover:underline">Ideogram data ↗</a>
  </div>
</div>

% else:
<!-- ── Not annotated panel ────────────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow p-8 text-center max-w-2xl mx-auto">
  % if status_info.get('status') == 'running':
  <div class="text-4xl mb-4">⏳</div>
  <h2 class="text-lg font-semibold text-gray-800 mb-2">Annotation is running</h2>
  <p class="text-gray-500 text-sm mb-4">
    The annotation pipeline appears to be active. Refresh this page in a few minutes.
  </p>
  % else:
  <div class="text-4xl mb-4">🧬</div>
  <h2 class="text-lg font-semibold text-gray-800 mb-2">Annotation not available</h2>
  <p class="text-gray-500 text-sm mb-6">
    No annotation results found for <strong>{{pgs_id}}</strong>.
    Run the Variant Annotator pipeline on an HPC node using Apptainer.
  </p>

  <div class="text-left bg-gray-900 rounded-lg p-4 text-xs font-mono text-green-400 mb-4 overflow-x-auto">
    <div class="text-gray-400 mb-2"># 1. Build the container (once — only if image is missing)</div>
    <div>apptainer build \</div>
    <div class="ml-4">/path/to/variant_annotator.sif \</div>
    <div class="ml-4">apptainer/variant_annotator.def</div>
    <div class="mt-3 text-gray-400"># 2. Run full v1.3 annotation (FASTA + dbNSFP5 + dbSNP)</div>
    <div>DATA_DIR={{cfg.DATA_DIR}} \</div>
    <div>ANNOTATIONS_DIR={{cfg.ANNOTATIONS_DIR}} \</div>
    <div>GFF3_PATH=/path/to/gencode.annotation.gff3 \</div>
    <div>FASTA_PATH=/path/to/hg38.fa \</div>
    <div>DBNSFP_PATH=/path/to/dbNSFP5.0a_grch38.gz \</div>
    <div>DBSNP_FREQ_PATH=/path/to/freq.vcf.gz \</div>
    <div>SIF_PATH=/path/to/variant_annotator.sif \</div>
    <div>/path/to/annotator/run_annotation.sh {{pgs_id}}</div>
  </div>
  % end

  <a href="/dashboard/{{pgs_id}}"
     class="inline-block mt-2 text-sm text-indigo-600 hover:underline">
    ← Back to dashboard
  </a>
</div>
% end

<!--
  ══════════════════════════════════════════════════════════════════════════════
  JAVASCRIPT — Variant Annotation view
  ══════════════════════════════════════════════════════════════════════════════

  Uses native fetch() directly (NOT fetchJSON from app.js) so this script is
  independent of app.js load order. app.js is loaded AFTER this block in the
  base template, making any call to fetchJSON() here a ReferenceError.

  Expected summary JSON structure (/api/variants/<pgs_id>/summary):
  {
    "pgs_id":           str,
    "annotated_at":     str  (ISO 8601),
    "annotation_tool":  str,
    "gff3_reference":   str,
    "elapsed_seconds":  float,
    "stats": {
      "total_variants":       int,
      "n_coding":             int,
      "n_intergenic":         int,
      "region_class_counts":  { "intronic": int, "intergenic": int, ... },
    },
    "per_chromosome": {
      "1": { "n_variants": int, "n_coding": int },
      ...
    }
  }

  Expected variants JSON structure (/api/variants/<pgs_id>?page=N&page_size=100):
  {
    "pgs_id":       str,
    "page":         int,
    "total_rows":   int,
    "total_pages":  int,
    "columns":      [str, ...],
    "rows":         [{ CHROM, POS, ID, EFFECT_ALLELE, OTHER_ALLELE, BETA, gene_name,
                       gene_type, region_class, consequence, is_coding }, ...]
  }
  ══════════════════════════════════════════════════════════════════════════════
-->
<script>
(function () {
  "use strict";

  // ── Constants (server-rendered) ──────────────────────────────────────────
  var PGS_ID  = "{{pgs_id}}";
  var STATUS  = "{{status_info.get('status', 'not_annotated')}}";

  // ── Pagination state ─────────────────────────────────────────────────────
  var currentPage  = 1;
  var totalPages   = 1;
  var debounceTimer = null;

  // ── Utility: set text content safely (no-op if element missing) ──────────
  function setText(id, value) {
    var el = document.getElementById(id);
    if (el) el.textContent = (value === null || value === undefined) ? '—' : String(value);
  }

  // ── Utility: show error banner ────────────────────────────────────────────
  function showError(msg) {
    var el = document.getElementById('va-error-banner');
    if (!el) return;
    el.textContent = msg;
    el.classList.remove('hidden');
  }

  // ── Pie chart colours ─────────────────────────────────────────────────────
  var REGION_COLORS = {
    coding:          '#10b981',
    UTR_5prime:      '#6366f1',
    UTR_3prime:      '#8b5cf6',
    intronic:        '#94a3b8',
    splice_region:   '#ef4444',
    non_coding_exon: '#f59e0b',
    near_gene:       '#f97316',
    regulatory:      '#0ea5e9',
    intergenic:      '#cbd5e1',
  };

  // ── Region badge CSS ──────────────────────────────────────────────────────
  var REGION_BADGE = {
    coding:          'bg-emerald-100 text-emerald-800',
    UTR_5prime:      'bg-violet-100 text-violet-800',
    UTR_3prime:      'bg-purple-100 text-purple-800',
    intronic:        'bg-slate-100 text-slate-700',
    splice_region:   'bg-red-100 text-red-800',
    non_coding_exon: 'bg-amber-100 text-amber-800',
    near_gene:       'bg-orange-100 text-orange-800',
    regulatory:      'bg-sky-100 text-sky-800',
    intergenic:      'bg-gray-100 text-gray-600',
  };

  // ── Fill summary cards ────────────────────────────────────────────────────
  function fillCards(data) {
    var s = (data && data.stats) ? data.stats : {};
    var rc = s.region_class_counts || {};

    setText('card-total',      (s.total_variants || 0).toLocaleString());
    setText('card-coding',     (s.n_coding        || 0).toLocaleString());
    setText('card-intergenic', (s.n_intergenic    || 0).toLocaleString());
    setText('card-intronic',   (rc.intronic       || 0).toLocaleString());
  }

  // ── Fill metadata row ────────────────────────────────────────────────────
  function fillMeta(data) {
    setText('meta-date',    data.annotated_at   || '—');
    setText('meta-gff3',    data.gff3_reference || '—');
    setText('meta-elapsed', data.elapsed_seconds != null ? data.elapsed_seconds + ' s' : '—');
    setText('meta-tool',    data.annotation_tool || '—');

    fillScopeNotice(data);
  }

  // ── Build annotation scope notice from summary JSON ────────────────────────
  function fillScopeNotice(data) {
    var el = document.getElementById('scope-notice');
    if (!el) return;

    var tool       = data.annotation_tool  || '';
    var schema     = data.schema_version   || '';
    var hasFasta   = !!(data.fasta_reference);
    var hasDbnsfp  = !!(data.dbnsfp_reference);
    var hasDbsnp   = !!(data.dbsnp_reference);
    var regBeds    = (data.regulatory_beds || []).length > 0;
    var stats      = data.stats || {};

    // Derive displayed version label from schema_version or tool string
    var verLabel = schema ? 'v' + schema : (tool.replace('variant_annotator/', 'v') || 'unknown');

    // Build capability badges
    function badge(color, text) {
      return '<span class="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium mr-1 ' + color + '">' + text + '</span>';
    }

    var badges = badge('bg-blue-200 text-blue-900', verLabel);
    badges += badge('bg-indigo-100 text-indigo-800', 'GFF3 annotation');
    badges += badge('bg-indigo-100 text-indigo-800', 'splice-site distance');
    badges += badge('bg-indigo-100 text-indigo-800', 'all-gene overlap');

    if (hasFasta) {
      badges += badge('bg-emerald-100 text-emerald-800', 'FASTA coding consequences');
      badges += badge('bg-emerald-100 text-emerald-800', 'splice donor/acceptor');
    }
    if (hasDbnsfp) {
      badges += badge('bg-violet-100 text-violet-800', 'CADD · REVEL · SIFT · PolyPhen2 · ClinVar');
    }
    if (hasDbsnp) {
      badges += badge('bg-teal-100 text-teal-800', 'dbSNP AF · rsid · rarity');
    }
    if (regBeds) {
      badges += badge('bg-sky-100 text-sky-800', 'regulatory BED');
    } else {
      badges += badge('bg-gray-100 text-gray-600', 'regulatory: ±2 kb heuristic');
    }

    // Build detail lines
    var lines = [];

    if (hasFasta) {
      lines.push('<span class="text-emerald-700">✓ Coding consequences active</span>: ' +
        'missense, synonymous, stop_gained, stop_lost, start_lost, frameshift, inframe indels.');
      lines.push('<span class="text-emerald-700">✓ Splice donor/acceptor</span>: ' +
        'classified by GT/AG sequence inspection from FASTA.');
    } else {
      lines.push('<span class="text-amber-700">△ FASTA not provided</span>: ' +
        'coding variants reported as <em>coding_sequence_variant</em>. ' +
        'Run with <code class="font-mono text-xs bg-blue-100 px-1 rounded">FASTA_PATH=…</code> to enable missense/stop/frameshift annotation.');
    }

    if (hasDbnsfp) {
      lines.push('<span class="text-violet-700">✓ dbNSFP5 scores active</span>: ' +
        'CADD_phred, REVEL_score, SIFT_pred, Polyphen2_HDIV_pred, clinvar_clnsig.');
    } else {
      lines.push('<span class="text-amber-700">△ dbNSFP5 not provided</span>: ' +
        'functional scores absent. Run with <code class="font-mono text-xs bg-blue-100 px-1 rounded">DBNSFP_PATH=…</code> to enable.');
    }
    if (hasDbsnp) {
      lines.push('<span class="text-teal-700">✓ dbSNP population frequencies active</span>: ' +
        'rsid, af_global, af_max_population, af_population_summary, rarity_class (common/low_freq/rare/ultra_rare/novel).');
    } else {
      lines.push('<span class="text-amber-700">△ dbSNP freq not provided</span>: ' +
        'rsid, population AF, and rarity classification absent. Run with ' +
        '<code class="font-mono text-xs bg-blue-100 px-1 rounded">DBSNP_FREQ_PATH=…</code> to enable.');
    }

    if (regBeds) {
      lines.push('<span class="text-sky-700">✓ Regulatory BED</span>: ' +
        data.regulatory_beds.map(function(p) {
          return p.split('/').pop();
        }).join(', ') + '.');
    } else {
      lines.push('<span class="text-gray-500">△ Regulatory: ±2 kb TSS heuristic</span> ' +
        '(no BED files). Provide <code class="font-mono text-xs bg-blue-100 px-1 rounded">REGULATORY_BED=…</code> for element-level annotation.');
    }

    el.innerHTML =
      '<div class="mb-1.5">' + badges + '</div>' +
      '<div class="space-y-0.5 text-xs leading-relaxed">' +
        lines.map(function(l) { return '<div>' + l + '</div>'; }).join('') +
      '</div>';
  }

  // ── Region class pie chart ────────────────────────────────────────────────
  function drawRegionPie(data) {
    var s  = (data && data.stats) ? data.stats : {};
    var rc = s.region_class_counts || {};

    var labels = Object.keys(rc);
    var values = labels.map(function(l) { return rc[l] || 0; });
    if (labels.length === 0) return;

    var colors = labels.map(function(l) { return REGION_COLORS[l] || '#a5b4fc'; });

    try {
      Plotly.newPlot('chart-region', [{
        type:   'pie',
        labels: labels,
        values: values,
        marker: { colors: colors },
        textinfo: 'label+percent',
        textposition: 'inside',
        insidetextorientation: 'auto',
        hovertemplate: '%{label}: %{value:,} (%{percent})<extra></extra>',
      }], {
        margin:          { t: 10, b: 10, l: 10, r: 10 },
        showlegend:      false,
        paper_bgcolor:   'white',
        plot_bgcolor:    'white',
      }, {
        responsive:      true,
        displayModeBar:  false,
      });
    } catch (e) {
      console.error('[VA] Pie chart error:', e);
    }
  }

  // ── Variants-per-chromosome bar chart ─────────────────────────────────────
  function drawChromBar(data) {
    var perChrom   = (data && data.per_chromosome) ? data.per_chromosome : {};
    var chromOrder = [];
    for (var i = 1; i <= 22; i++) chromOrder.push(String(i));
    chromOrder.push('X', 'Y', 'MT');

    var chroms = chromOrder.filter(function(c) { return c in perChrom; });
    if (chroms.length === 0) return;

    var nVar = chroms.map(function(c) { return (perChrom[c].n_variants || 0); });
    var nCod = chroms.map(function(c) { return (perChrom[c].n_coding   || 0); });
    var xLabels = chroms.map(function(c) { return 'chr' + c; });

    try {
      Plotly.newPlot('chart-chrom', [
        { type: 'bar', x: xLabels, y: nVar, name: 'Total',
          marker: { color: '#c7d2fe' } },
        { type: 'bar', x: xLabels, y: nCod, name: 'Coding',
          marker: { color: '#10b981' } },
      ], {
        barmode:       'overlay',
        margin:        { t: 10, b: 50, l: 45, r: 10 },
        xaxis:         { tickangle: -45, tickfont: { size: 9 } },
        yaxis:         { title: { text: 'Variants' } },
        paper_bgcolor: 'white',
        plot_bgcolor:  'white',
        legend:        { orientation: 'h', y: -0.35 },
      }, {
        responsive:     true,
        displayModeBar: false,
      });
    } catch (e) {
      console.error('[VA] Chrom bar chart error:', e);
    }
  }

  // ── Fetch and render summary (cards + meta + charts) ─────────────────────
  function loadSummary() {
    fetch('/api/variants/' + PGS_ID + '/summary')
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status + ' on ' + r.url);
        return r.json();
      })
      .then(function(data) {
        if (data && data.error) {
          showError('Summary error: ' + data.error);
          console.error('[VA] Summary returned error:', data.error);
          return;
        }
        console.log('[VA] Summary loaded:', data.pgs_id,
                    '— total_variants:', data.stats && data.stats.total_variants);
        fillCards(data);
        fillMeta(data);
        drawRegionPie(data);
        drawChromBar(data);
      })
      .catch(function(err) {
        showError('Could not load annotation summary: ' + err.message);
        console.error('[VA] Summary fetch failed:', err);
      });
  }

  // ── Build variants API URL with current filters ───────────────────────────
  function buildVariantsUrl() {
    var chrom      = (document.getElementById('filter-chrom')      || {}).value || '';
    var region     = (document.getElementById('filter-region')     || {}).value || '';
    var gene       = (document.getElementById('filter-gene')       || {}).value || '';
    var confidence = (document.getElementById('filter-confidence') || {}).value || '';
    var useRanking = (document.getElementById('toggle-ranking')    || {}).checked || false;

    var baseUrl = useRanking
      ? '/api/variants/ranked?pgs_id=' + encodeURIComponent(PGS_ID)
      : '/api/variants/' + PGS_ID + '?';

    var url = baseUrl + (useRanking ? '&' : '') +
              'page=' + currentPage + '&page_size=100';

    if (!useRanking) {
      if (chrom)      url += '&chrom='        + encodeURIComponent(chrom);
      if (region)     url += '&region_class=' + encodeURIComponent(region);
      if (gene)       url += '&gene_name='    + encodeURIComponent(gene);
      if (confidence) url += '&clinical_confidence=' + encodeURIComponent(confidence);
      url += '&add_ranking=1';
    } else {
      if (confidence === 'high' || confidence === 'medium' || confidence === 'low') {
        url += '&clinical_only=1';
      }
    }
    return url;
  }

  // ── Render one row ────────────────────────────────────────────────────────
  function renderRow(r) {
    var rc    = r.region_class || 'intergenic';
    // Splice / LOF consequences override region_class badge colour
    var badgeKey = rc;
    var csq = r.consequence || '';
    if (csq === 'splice_donor_variant' || csq === 'splice_acceptor_variant' ||
        csq === 'splice_site_variant'  || csq === 'splice_region_variant') {
      badgeKey = 'splice_region';
    }
    var badge = REGION_BADGE[badgeKey] || 'bg-gray-100 text-gray-600';
    var beta  = (typeof r.BETA === 'number') ? r.BETA.toFixed(4) : (r.BETA || '');
    var flipCell = (r.IS_FLIP === 1 || r.IS_FLIP === '1' || r.IS_FLIP === true)
      ? '<span class="px-1 py-0.5 rounded bg-amber-100 text-amber-700 text-xs font-medium" title="Effect allele on REF strand; dosage = 2 − DS">F</span>'
      : '<span class="text-gray-300 text-xs">—</span>';
    var pos   = (r.POS != null) ? Number(r.POS).toLocaleString() : '—';
    // Splice distance: show bp value, highlight if near splice site
    var spliceDist = '—';
    if (r.distance_to_splice_site != null) {
      var d = Number(r.distance_to_splice_site);
      var dClass = d <= 2
        ? 'text-red-600 font-semibold'
        : d <= 8
          ? 'text-orange-600 font-medium'
          : 'text-gray-400';
      spliceDist = '<span class="' + dClass + '">' + d + '</span>';
    }
    // Gene display: show best hit; link to gene browser; tooltip shows all overlapping
    var geneName = r.gene_name || '—';
    var geneTitle = r.all_overlapping_genes || geneName;
    var geneBrowserLink = geneName && geneName !== '—'
      ? '<a href="/gene/' + encodeURIComponent(geneName) + '" class="hover:underline text-indigo-700">' + geneName + '</a>'
      : geneName;
    var geneCell = (r.n_overlapping_genes > 1)
      ? '<span title="All: ' + geneTitle + '" class="cursor-help">' +
          geneBrowserLink + ' <span class="text-gray-400 text-xs">+' + (r.n_overlapping_genes - 1) + '</span></span>'
      : geneBrowserLink;

    // Rarity badge (v1.3 dbSNP)
    var rarityBadge = '';
    if (r.rarity_class && r.rarity_class !== 'novel') {
      var rarityColors = {
        common:        'bg-green-100 text-green-700',
        low_frequency: 'bg-blue-100 text-blue-700',
        rare:          'bg-yellow-100 text-yellow-700',
        ultra_rare:    'bg-red-100 text-red-700',
      };
      var rc2 = rarityColors[r.rarity_class] || 'bg-gray-100 text-gray-600';
      var afStr = r.af_global != null ? ' AF=' + Number(r.af_global).toExponential(2) : '';
      rarityBadge = '<span class="px-1 py-0.5 rounded text-xs ' + rc2 + '" title="' +
        r.rarity_class + afStr + '">' + r.rarity_class.replace('_', ' ') + '</span> ';
    }
    // Amino acid change (FASTA-backed, v1.2)
    var aaCell = '—';
    if (r.aa_ref_3 && r.aa_alt_3 && r.aa_ref_3 !== r.aa_alt_3) {
      var codonIdx = (r.codon_index != null) ? (Number(r.codon_index) + 1) : '';
      aaCell = '<span class="font-mono text-xs text-emerald-700" title="' +
        (r.codon_ref || '') + '→' + (r.codon_alt || '') + '">' +
        r.aa_ref_3 + (codonIdx ? codonIdx : '') + r.aa_alt_3 + '</span>';
    } else if (r.aa_ref_3 && r.aa_ref_3 === r.aa_alt_3) {
      aaCell = '<span class="font-mono text-xs text-gray-400">=' + r.aa_ref_3 + '</span>';
    }
    // CADD score
    var caddCell = '—';
    if (r.cadd_phred != null) {
      var cadd = Number(r.cadd_phred);
      var caddClass = cadd >= 30 ? 'text-red-600 font-semibold'
                    : cadd >= 20 ? 'text-orange-600'
                    : 'text-gray-400';
      caddCell = '<span class="font-mono text-xs ' + caddClass + '">' + cadd.toFixed(1) + '</span>';
    }
    // Flags: LoF, splice type, ClinVar
    var flags = [];
    if (r.is_lof) {
      flags.push('<span class="px-1 py-0.5 rounded bg-red-100 text-red-700 text-xs font-medium">LoF</span>');
    }
    if (r.splice_type === 'donor') {
      flags.push('<span class="px-1 py-0.5 rounded bg-red-100 text-red-700 text-xs">D</span>');
    } else if (r.splice_type === 'acceptor') {
      flags.push('<span class="px-1 py-0.5 rounded bg-red-100 text-red-700 text-xs">A</span>');
    }
    if (r.clinvar_clnsig && r.clinvar_clnsig !== '.' && r.clinvar_clnsig !== '') {
      var cvClass = /pathogenic/i.test(r.clinvar_clnsig) ? 'bg-red-100 text-red-700'
                  : /benign/i.test(r.clinvar_clnsig)     ? 'bg-green-100 text-green-700'
                  : 'bg-gray-100 text-gray-600';
      flags.push('<span class="px-1 py-0.5 rounded text-xs ' + cvClass +
        '" title="ClinVar: ' + r.clinvar_clnsig + '">CV</span>');
    }
    if (r.is_coding) {
      flags.push('<span class="text-emerald-600 font-semibold text-xs">CDS</span>');
    }
    var flagsCell = rarityBadge + (flags.length ? flags.join(' ') : '');

    return '<tr class="hover:bg-gray-50">' +
      '<td class="px-3 py-1.5 font-mono text-gray-500">chr' + (r.CHROM || '') + '</td>' +
      '<td class="px-3 py-1.5 font-mono">' + pos + '</td>' +
      '<td class="px-3 py-1.5 font-mono text-gray-500 max-w-24 truncate" title="' + (r.ID || '') + '">' + (r.ID || '.') + '</td>' +
      '<td class="px-3 py-1.5 font-mono">' + (r.EFFECT_ALLELE || '') + '</td>' +
      '<td class="px-3 py-1.5 font-mono">' + (r.OTHER_ALLELE  || '') + '</td>' +
      '<td class="px-3 py-1.5 font-mono text-right">' + beta + '</td>' +
      '<td class="px-3 py-1.5 text-center">' + flipCell + '</td>' +
      '<td class="px-3 py-1.5 font-medium text-indigo-700">' + geneCell + '</td>' +
      '<td class="px-3 py-1.5 text-gray-400 text-xs">' + (r.gene_type || '') + '</td>' +
      '<td class="px-3 py-1.5"><span class="px-1.5 py-0.5 rounded text-xs font-medium ' + badge + '">' + rc + '</span></td>' +
      '<td class="px-3 py-1.5 text-gray-500 text-xs">' + csq + '</td>' +
      '<td class="px-3 py-1.5 text-center font-mono text-xs">' + spliceDist + '</td>' +
      '<td class="px-3 py-1.5 text-center">' + aaCell + '</td>' +
      '<td class="px-3 py-1.5 text-center">' + caddCell + '</td>' +
      '<td class="px-3 py-1.5 text-center">' + renderScoreCell(r) + '</td>' +
      '<td class="px-3 py-1.5 text-center whitespace-nowrap">' + flagsCell + '</td>' +
      '</tr>';
  }

  // ── Ranking score cell ─────────────────────────────────────────────────────
  function renderScoreCell(r) {
    if (r.ranking_score == null) return '<span class="text-gray-300 text-xs">—</span>';
    var s = Number(r.ranking_score);
    var cls = s >= 0.7  ? 'bg-red-100 text-red-700 font-semibold'
            : s >= 0.45 ? 'bg-orange-100 text-orange-700'
            : s >= 0.25 ? 'bg-yellow-100 text-yellow-700'
            :              'bg-gray-100 text-gray-500';
    return '<span class="px-1.5 py-0.5 rounded text-xs ' + cls + '">' + s.toFixed(3) + '</span>';
  }

  // ── Fetch and render variant table ────────────────────────────────────────
  function loadVariants() {
    var tbody = document.getElementById('variant-tbody');
    if (!tbody) return;
    tbody.innerHTML = '<tr><td colspan="17" class="px-3 py-6 text-center text-gray-400">Loading…</td></tr>';

    fetch(buildVariantsUrl())
      .then(function(r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function(data) {
        if (data && data.error) {
          tbody.innerHTML = '<tr><td colspan="17" class="px-3 py-6 text-center text-red-500">' +
            'Error:' + data.error + '</td></tr>';
          return;
        }

        totalPages = data.total_pages || 1;
        var count  = data.total_rows  || 0;

        setText('table-count', count.toLocaleString() + ' variant' + (count !== 1 ? 's' : ''));
        setText('page-info',   'Page ' + (data.page || 1) + ' / ' + totalPages);

        var btnPrev = document.getElementById('btn-prev');
        var btnNext = document.getElementById('btn-next');
        if (btnPrev) btnPrev.disabled = (data.page || 1) <= 1;
        if (btnNext) btnNext.disabled = (data.page || 1) >= totalPages;

        if (!data.rows || data.rows.length === 0) {
          tbody.innerHTML = '<tr><td colspan="17" class="px-3 py-6 text-center text-gray-400">No variants match the current filter.</td></tr>';
          return;
        }

        tbody.innerHTML = data.rows.map(renderRow).join('');
      })
      .catch(function(err) {
        tbody.innerHTML = '<tr><td colspan="17" class="px-3 py-6 text-center text-red-500">' +
          'Failed to load variants: ' + err.message + '</td></tr>';
        console.error('[VA] Variants fetch failed:', err);
      });
  }

  // ── Pagination ────────────────────────────────────────────────────────────
  window.vaChangePage = function(delta) {
    var next = currentPage + delta;
    if (next < 1 || next > totalPages) return;
    currentPage = next;
    loadVariants();
  };

  // ── Filters ───────────────────────────────────────────────────────────────
  window.vaApplyFilters = function() {
    currentPage = 1;
    loadVariants();
  };

  window.vaDebouncedFilter = function() {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(window.vaApplyFilters, 400);
  };

  // ── Bootstrap ─────────────────────────────────────────────────────────────
  if (STATUS === 'annotated') {
    loadSummary();
    loadVariants();
  }

})(); // end IIFE
</script>
