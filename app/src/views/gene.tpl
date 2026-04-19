%rebase('_base.tpl', title='Gene Browser — ' + gene_symbol)

<!-- Breadcrumb -->
<nav class="text-sm text-gray-500 mb-4">
  <a href="/" class="hover:text-indigo-600">Catalog</a>
  <span class="mx-1">›</span>
  <a href="/genes" class="hover:text-indigo-600">Gene Browser</a>
  <span class="mx-1">›</span>
  <span class="text-gray-800 font-medium">{{gene_symbol}}</span>
</nav>

<!-- Page header -->
<div class="flex flex-wrap items-center justify-between gap-4 mb-6">
  <div>
    <h1 class="text-2xl font-bold text-gray-900">
      🧬 {{gene_symbol}}
      % if gene_info.get('is_clinical_gene'):
      <span class="inline-flex items-center gap-1 ml-2 px-2 py-0.5 rounded-full text-xs font-semibold bg-emerald-100 text-emerald-800 border border-emerald-300" title="Clinical gene — present in curated AR/carrier screen dataset">
        ✚ Clinical Gene
        % conf = gene_info.get('clinical_confidence')
        % if conf == 'high':
        <span class="ml-0.5 px-1 rounded bg-emerald-200 text-emerald-900 text-[10px]">HIGH</span>
        % elif conf == 'medium':
        <span class="ml-0.5 px-1 rounded bg-yellow-200 text-yellow-900 text-[10px]">MED</span>
        % end
      </span>
      % end
      % if gene_info.get('found'):
      <span class="text-base font-normal text-gray-500 ml-2">
        chr{{gene_info.get('chrom', '?')}}:{{'{:,}'.format(gene_info.get('genomic_start', 0))}}–{{'{:,}'.format(gene_info.get('genomic_end', 0))}}
        % if gene_info.get('genomic_span_kb'):
        <span class="text-xs text-gray-400">({{gene_info['genomic_span_kb']}} kb span in PRS variants)</span>
        % end
      </span>
      % end
    </h1>
    <p class="text-gray-500 text-sm mt-1">
      Gene-centric interpretation browser · GRCh38 · Plotly tracks
      <span class="ml-2 text-xs text-gray-400">MTR track excluded by design</span>
    </p>
  </div>

  <!-- Gene search form -->
  <form action="/genes" method="GET" class="flex gap-2">
    <input name="q" type="text" placeholder="Search gene…" value="{{gene_symbol}}"
           class="border border-gray-300 rounded px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 w-40" />
    <button type="submit"
            class="bg-indigo-600 text-white text-sm px-4 py-1.5 rounded hover:bg-indigo-700">
      Go
    </button>
  </form>
</div>

% if not gene_info.get('found'):
<!-- Not found -->
<div class="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
  <div class="text-2xl mb-2">🔍</div>
  <h2 class="text-lg font-semibold text-yellow-800 mb-2">Gene not found in annotations</h2>
  <p class="text-yellow-700 text-sm">{{gene_info.get('message', 'No annotated variants for this gene.')}}</p>
  <p class="text-yellow-600 text-xs mt-3">
    Make sure the variant annotation pipeline has been run and parquet files are available
    in the annotations directory.
  </p>
  <a href="/genes" class="mt-4 inline-block text-indigo-600 hover:underline text-sm">← Back to gene list</a>
</div>

% else:
<!-- ── Summary cards ──────────────────────────────────────────────────────── -->
<div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
  <div class="bg-white rounded-lg shadow p-4 text-center" title="Unique genomic positions (CHROM · POS · Effect allele) across all PGS scores. Each variant counted once regardless of how many PGS files include it.">
    <div class="text-2xl font-bold text-indigo-700">{{'{:,}'.format(gene_info.get('n_variants', 0))}}</div>
    <div class="text-xs text-gray-500 mt-1">Unique Variants</div>
    % if gene_info.get('n_variant_records', 0) != gene_info.get('n_variants', 0):
    <div class="text-[10px] text-gray-400 mt-0.5">{{'{:,}'.format(gene_info.get('n_variant_records', 0))}} records across PGS</div>
    % end
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-red-600" id="card-lof">
      {{gene_info.get('consequence_counts', {}).get('stop_gained', 0) + gene_info.get('consequence_counts', {}).get('frameshift_variant', 0)}}
    </div>
    <div class="text-xs text-gray-500 mt-1">pLoF Variants</div>
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-orange-500">
      {{gene_info.get('consequence_counts', {}).get('missense_variant', 0)}}
    </div>
    <div class="text-xs text-gray-500 mt-1">Missense</div>
  </div>
  <div class="bg-white rounded-lg shadow p-4 text-center">
    <div class="text-2xl font-bold text-purple-600">{{gene_info.get('n_pgs_ids', 0)}}</div>
    <div class="text-xs text-gray-500 mt-1">PGS Scores</div>
  </div>
</div>

<!-- ── Rarity badges ────────────────────────────────────────────────────────── -->
% rarity_counts = gene_info.get('rarity_counts', {})
% if rarity_counts:
<div class="flex flex-wrap gap-2 mb-6">
  <span class="text-xs text-gray-500 self-center">Rarity:</span>
  % if rarity_counts.get('common'):
  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-green-100 text-green-800">
    Common (AF≥1%) · {{rarity_counts['common']}}
  </span>
  % end
  % if rarity_counts.get('low_frequency'):
  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
    Low freq (0.1–1%) · {{rarity_counts['low_frequency']}}
  </span>
  % end
  % if rarity_counts.get('rare'):
  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
    Rare (0.01–0.1%) · {{rarity_counts['rare']}}
  </span>
  % end
  % if rarity_counts.get('ultra_rare'):
  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-red-100 text-red-800">
    Ultra-rare (&lt;0.01%) · {{rarity_counts['ultra_rare']}}
  </span>
  % end
  % if rarity_counts.get('novel'):
  <span class="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-600">
    Novel (no AF) · {{rarity_counts['novel']}}
  </span>
  % end
</div>
% end

% if gene_info.get('is_clinical_gene'):
<!-- ── Clinical gene panel ───────────────────────────────────────────────────── -->
<div class="bg-emerald-50 border border-emerald-200 rounded-lg p-4 mb-6 flex flex-wrap gap-x-6 gap-y-2 text-sm">
  <div class="flex items-center gap-2">
    <span class="text-emerald-700 font-semibold">✚ Clinical Gene</span>
    % conf = gene_info.get('clinical_confidence', '')
    % conf_color = {'high': 'bg-emerald-200 text-emerald-900', 'medium': 'bg-yellow-200 text-yellow-900', 'standard': 'bg-gray-200 text-gray-700'}.get(conf, 'bg-gray-200 text-gray-700')
    <span class="px-2 py-0.5 rounded text-xs font-medium {{conf_color}}">{{(conf or 'standard').upper()}} confidence</span>
  </div>
  % sources = gene_info.get('clinical_sources', [])
  % if sources:
  <div class="text-emerald-800">
    <span class="font-medium">Sources:</span>
    % for src in sources:
    <span class="ml-1 px-1.5 py-0.5 rounded bg-white border border-emerald-200 text-xs text-emerald-700">{{src}}</span>
    % end
  </div>
  % end
  % moi = gene_info.get('clinical_moi', '')
  % if moi:
  <div class="text-emerald-800"><span class="font-medium">MOI:</span> <span class="text-xs">{{moi}}</span></div>
  % end
  % ev = gene_info.get('clinical_evidence', '')
  % if ev:
  <div class="text-emerald-800"><span class="font-medium">Evidence:</span> <span class="text-xs capitalize">{{ev}}</span></div>
  % end
  % disease = gene_info.get('clinical_disease', '')
  % if disease:
  <div class="text-emerald-800 truncate max-w-xs"><span class="font-medium">Disease:</span> <span class="text-xs">{{disease}}</span></div>
  % end
</div>
% end

<!-- ── Browser loading state ────────────────────────────────────────────────── -->
<div id="browser-loading" class="bg-white rounded-lg shadow p-6 mb-6 text-center text-gray-400">
  <svg class="animate-spin h-8 w-8 mx-auto mb-2 text-indigo-400" fill="none" viewBox="0 0 24 24">
    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
    <path class="opacity-75" fill="currentColor"
          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
  </svg>
  Loading genomic tracks…
</div>

<!-- ── Gene browser: Plotly stacked tracks ──────────────────────────────────── -->
<div id="gene-browser" class="bg-white rounded-lg shadow p-4 mb-6" style="display:none;">

  <!-- Track toolbar -->
  <div class="flex flex-wrap items-center gap-x-4 gap-y-2 mb-4 pb-3 border-b border-gray-100">

    <!-- Track visibility toggles -->
    <div class="flex items-center gap-3">
      <span class="text-xs font-semibold text-gray-600">Tracks:</span>
      <label class="flex items-center gap-1 text-xs text-gray-600 cursor-pointer">
        <input type="checkbox" id="toggle-variants" checked class="rounded" /> Variants
      </label>
      <label class="flex items-center gap-1 text-xs text-gray-600 cursor-pointer">
        <input type="checkbox" id="toggle-gene-model" checked class="rounded" /> Gene Model
      </label>
      <label class="flex items-center gap-1 text-xs text-gray-600 cursor-pointer">
        <input type="checkbox" id="toggle-density" checked class="rounded" /> Density
      </label>
      <label class="flex items-center gap-1 text-xs text-gray-600 cursor-pointer">
        <input type="checkbox" id="toggle-af" checked class="rounded" /> AF
      </label>
    </div>

    <!-- Divider -->
    <div class="h-4 w-px bg-gray-200 hidden sm:block"></div>

    <!-- Y-axis score selector -->
    <select id="y-score" class="text-xs border border-gray-200 rounded px-2 py-1"
            title="Y-axis metric for the variants track">
      <option value="cadd">Y: CADD phred</option>
      <option value="revel">Y: REVEL score</option>
      <option value="beta">Y: |BETA|</option>
    </select>

    <!-- Consequence filter -->
    <select id="filter-consequence" class="text-xs border border-gray-200 rounded px-2 py-1">
      <option value="">All consequences</option>
      <option value="pLoF">pLoF only</option>
      <option value="missense">Missense only</option>
      <option value="synonymous">Synonymous only</option>
      <option value="splice_region">Splice region</option>
    </select>

    <!-- Clinical gene filter (only relevant when browsing multiple genes) -->
    <label id="clinical-only-toggle" class="flex items-center gap-1 text-xs cursor-pointer px-2 py-1 rounded border border-emerald-200 bg-emerald-50 text-emerald-800 hover:bg-emerald-100" title="Highlight variants in clinical genes">
      <input type="checkbox" id="filter-clinical-only" class="rounded accent-emerald-600" />
      ✚ Clinical only
    </label>

    <!-- Zoom controls -->
    <div class="ml-auto flex items-center gap-1">
      <span class="text-xs text-gray-500">Zoom:</span>
      <button id="zoom-gene"  class="text-xs border border-gray-200 rounded px-2 py-1 hover:bg-gray-50 active:bg-gray-100" title="Full gene view">Gene</button>
      <button id="zoom-75"    class="text-xs border border-gray-200 rounded px-2 py-1 hover:bg-gray-50 active:bg-gray-100">75%</button>
      <button id="zoom-50"    class="text-xs border border-gray-200 rounded px-2 py-1 hover:bg-gray-50 active:bg-gray-100">50%</button>
      <button id="zoom-25"    class="text-xs border border-gray-200 rounded px-2 py-1 hover:bg-gray-50 active:bg-gray-100">25%</button>
    </div>
  </div>

  <!-- Sampling notice (shown when variants were downsampled) -->
  <div id="sampling-notice" class="hidden mb-3 px-3 py-2 rounded bg-amber-50 border border-amber-200 text-xs text-amber-700"></div>

  <!-- Plotly figure container -->
  <div id="plotly-gene-browser" style="width:100%; height:1000px;"></div>

  <!-- Legend pills -->
  <div class="flex flex-wrap gap-x-4 gap-y-1 mt-3 pt-3 border-t border-gray-100 text-xs">
    <div class="flex items-center gap-1">
      <span class="font-semibold text-gray-500">Consequence:</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#e53935">pLoF</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#fb8c00">Missense</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#43a047">Synonymous</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#8e24aa">Splice</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#78909c">Non-coding</span>
    </div>
    <div class="flex items-center gap-1">
      <span class="font-semibold text-gray-500">AF rarity:</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#f44336">Ultra-rare</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#ff9800">Rare</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#2196f3">Low-freq</span>
      <span class="px-2 py-0.5 rounded text-white" style="background:#4caf50">Common</span>
    </div>
    <div class="flex items-center gap-1 text-gray-400">
      <span>● = CADD scored</span>
      <span class="mx-1">·</span>
      <span>◆ = consequence estimate</span>
      <span class="mx-1">·</span>
      <span>MTR track: excluded by design</span>
    </div>
  </div>
</div>

<!-- ── Error panel ────────────────────────────────────────────────────────────── -->
<div id="browser-error" class="hidden bg-red-50 border border-red-200 rounded-lg p-4 mb-6 text-sm text-red-700"></div>

<!-- ── Gene model note ────────────────────────────────────────────────────────── -->
<div class="bg-blue-50 border border-blue-200 rounded-lg p-3 mb-6 text-xs text-blue-700">
  <strong>Note:</strong> Gene model (Track 2) is approximated from PRS variant positions in this gene.
  Exon blocks represent clusters of coding variants. For exact exon boundaries, a precomputed GFF3 gene
  model cache is needed (see docs/VARIANT_ANNOTATION.md).
</div>

% if gene_info.get('is_clinical_gene'):
<!-- ── PRS clinical impact panel ─────────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow p-6 mb-6" id="prs-clinical-panel">
  <div class="flex items-center justify-between mb-4">
    <h2 class="text-sm font-semibold text-gray-700">
      PRS × Clinical Impact
      <span class="ml-1 text-xs font-normal text-gray-400">— genetic load for {{gene_symbol}}</span>
    </h2>
    <button onclick="loadPrsClinical()"
            class="text-xs px-3 py-1 rounded bg-emerald-50 text-emerald-700 hover:bg-emerald-100 border border-emerald-200">
      Load PRS Impact
    </button>
  </div>
  <div id="prs-clinical-status" class="text-xs text-gray-400 mb-3">
    Click "Load PRS Impact" to compute genetic load for all clinical genes.
  </div>
  <div id="prs-clinical-content" style="display:none;">
    <!-- Gene row for this gene -->
    <div id="prs-gene-row" class="bg-emerald-50 border border-emerald-200 rounded-lg p-4 mb-4 text-sm"></div>
    <!-- Score distribution chart -->
    <div id="chart-prs-scores" style="height:220px;"></div>
  </div>
</div>
% end

<!-- ═══════════════════════════════════════════════════════════════════════════
     Gene × PGS Matrix  (Heatmap 2)
     ═══════════════════════════════════════════════════════════════════════════
     Shows per-PGS metrics for this specific gene.
     Answers: in which PGS does this gene contribute most?
     This is distinct from:
       Heatmap 1 (/genes): genes × metrics across all genes
       Heatmap 3 (future): intra-gene bins × metrics (variant density by position)
-->
<div class="bg-white rounded-lg shadow p-6 mb-6" id="pgs-matrix-section">
  <div class="flex flex-wrap items-center justify-between gap-3 mb-4">
    <div>
      <h2 class="text-sm font-semibold text-gray-700">
        {{gene_symbol}} across PGS scores
        <span class="ml-1 text-xs font-normal text-gray-400">— Heatmap 2: Gene × PGS</span>
      </h2>
      <p class="text-xs text-gray-400 mt-0.5">
        Per-PGS genetic load and clinical priority for this gene
      </p>
    </div>
    <!-- Filters -->
    <div class="flex items-center gap-2 flex-wrap">
      <label class="text-xs text-gray-500">Sort:</label>
      <select id="pgsm-sort" class="text-xs border border-gray-300 rounded px-2 py-1"
              onchange="loadPgsMatrix()">
        <option value="sum_abs_beta">Σ|BETA|</option>
        <option value="max_ranking_score">Max score</option>
        <option value="variant_count">Variants</option>
      </select>
      <label class="text-xs text-gray-500">Min variants:</label>
      <input id="pgsm-min-var" type="number" min="1" value="1"
             class="w-12 text-xs border border-gray-300 rounded px-1.5 py-1"
             onchange="loadPgsMatrix()" />
      <button onclick="loadPgsMatrix()"
              class="text-xs px-3 py-1 rounded bg-indigo-50 text-indigo-700 hover:bg-indigo-100 border border-indigo-200">
        Load Matrix
      </button>
    </div>
  </div>

  <div id="pgsm-status" class="text-xs text-gray-400 mb-3">
    Click "Load Matrix" to compute per-PGS metrics for {{gene_symbol}}.
  </div>

  <!-- Heatmap -->
  <div id="chart-pgsm" style="display:none; min-height:220px;"></div>

  <!-- Summary table -->
  <div id="pgsm-table-wrap" class="overflow-x-auto mt-4" style="display:none;">
    <table class="w-full text-xs border-collapse">
      <thead>
        <tr class="bg-gray-50 text-left">
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">PGS ID</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right" title="Variant records in this PGS score (each PGS lists each variant once)">Variants in PGS</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Coding</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Splice</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Σ|BETA|</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Mean CADD</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Max score</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b text-right">Mean score</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b"></th>
        </tr>
      </thead>
      <tbody id="pgsm-tbody"></tbody>
    </table>
  </div>
</div>

<!-- ── PGS scores using this gene ──────────────────────────────────────────── -->
% pgs_ids = gene_info.get('pgs_ids', [])
% if pgs_ids:
<div class="bg-white rounded-lg shadow p-4 mb-6">
  <h2 class="text-sm font-semibold text-gray-700 mb-3">PGS Scores Containing Variants in {{gene_symbol}}</h2>
  <div class="flex flex-wrap gap-2">
    % for pid in pgs_ids:
    <a href="/variants/{{pid}}"
       class="inline-flex items-center px-2.5 py-1 rounded text-xs font-medium bg-indigo-50 text-indigo-700 hover:bg-indigo-100 transition-colors">
      {{pid}} →
    </a>
    % end
    % if gene_info.get('n_pgs_ids', 0) > len(pgs_ids):
    <span class="text-xs text-gray-400 self-center">
      + {{gene_info['n_pgs_ids'] - len(pgs_ids)}} more
    </span>
    % end
  </div>
</div>
% end

<!-- ── Variant table for this gene ─────────────────────────────────────────── -->
<div class="bg-white rounded-lg shadow p-4 mb-6">
  <div class="flex items-center justify-between mb-3">
    <h2 class="text-sm font-semibold text-gray-700">Variant Table</h2>
    <a href="/api/gene/{{gene_symbol}}/variants?format=tsv"
       class="text-xs text-indigo-600 hover:underline">↓ Download TSV</a>
  </div>
  <div class="overflow-x-auto">
    <table class="w-full text-xs border-collapse" id="gene-variant-table">
      <thead>
        <tr class="bg-gray-50 text-left">
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">PGS</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">CHROM:POS</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">rsID</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">Consequence</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">AA change</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">AF global</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">Rarity</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">CADD</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">REVEL</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">BETA</th>
          <th class="px-3 py-2 font-semibold text-gray-600 border-b">ClinVar</th>
        </tr>
      </thead>
      <tbody id="gene-variant-tbody">
        <tr><td colspan="11" class="px-3 py-4 text-center text-gray-400">Loading…</td></tr>
      </tbody>
    </table>
  </div>
  <div class="mt-2 text-xs text-gray-400" id="gene-variant-count"></div>
</div>

% end
<!-- end if gene_info.found -->

<script>
// =============================================================================
// Gene Browser — 4-track stacked genomic browser (Plotly)
// Tracks: [1] Variants  [2] Gene Model  [3] Density  [4] Population AF
// MTR (Missense Tolerance Ratio w31) is intentionally NOT implemented.
// =============================================================================

const GENE_SYMBOL = {{!repr(gene_symbol)}};
const GENE_FOUND  = {{!'true' if gene_info.get('found') else 'false'}};

if (GENE_FOUND) {

  // ── Module-level state ───────────────────────────────────────────────────────
  let _apiData        = null;   // cached API response
  let _geneXRange     = null;   // full gene x range from API
  let _currentXRange  = null;   // user's current zoom (null = full)
  let _plotConfig     = null;

  // Customdata column indices for client-side y-axis switching
  const SCORE_COL = { cadd: 0, revel: 1, beta: 2 };
  const SCORE_LABEL = {
    cadd: 'CADD Phred  (● scored, ◆ estimated)',
    revel: 'REVEL Score',
    beta: '|BETA|',
  };

  // ── Load tracks from API ─────────────────────────────────────────────────────
  async function loadTracks() {
    try {
      const resp = await fetch(`/api/gene/${GENE_SYMBOL}/tracks`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (!data.found) { showError(data.message || 'No track data.'); return; }
      _apiData = data;
      renderBrowser(data);
    } catch (err) {
      showError('Failed to load tracks: ' + err.message);
    }
  }

  function showError(msg) {
    document.getElementById('browser-loading').style.display = 'none';
    const el = document.getElementById('browser-error');
    el.textContent = msg;
    el.classList.remove('hidden');
  }

  // ── Initial render ───────────────────────────────────────────────────────────
  function renderBrowser(data) {
    const hints  = data.layout_hints || {};
    const tracks = data.tracks || {};
    _geneXRange  = hints.x_range || [data.genomic_start, data.genomic_end];
    _currentXRange = [..._geneXRange];

    const xTitle = hints.x_title || `chr${data.chrom} position (GRCh38)`;

    // Sampling notice
    const vi = tracks.variants || {};
    if (vi.n_sampled != null && vi.n_variants != null && vi.n_sampled < vi.n_variants) {
      const el = document.getElementById('sampling-notice');
      el.textContent =
        `⚠ ${vi.n_sampled.toLocaleString()} of ${vi.n_variants.toLocaleString()} unique variants ` +
        `shown (pLoF+missense always kept; synonymous+non-coding priority-sampled).`;
      el.classList.remove('hidden');
    }

    document.getElementById('browser-loading').style.display = 'none';
    document.getElementById('gene-browser').style.display = 'block';

    _plotConfig = {
      displayModeBar: true,
      modeBarButtonsToRemove: ['lasso2d', 'select2d'],
      responsive: true,
      toImageButtonOptions: { filename: `gene_browser_${GENE_SYMBOL}`, scale: 2 },
    };

    const scoreMode = _scoreMode();
    const { allTraces, shapes, annotations } = _buildTraces(data, tracks, _geneXRange, scoreMode);
    const layout = _buildLayout(_geneXRange, xTitle, tracks, shapes, annotations, scoreMode);

    Plotly.newPlot('plotly-gene-browser', allTraces, layout, _plotConfig);

    // Track x-range changes for zoom preservation on redraw
    document.getElementById('plotly-gene-browser').on('plotly_relayout', ev => {
      if ('xaxis.range[0]' in ev) {
        _currentXRange = [+ev['xaxis.range[0]'], +ev['xaxis.range[1]']];
      } else if (ev['xaxis.autorange']) {
        _currentXRange = [..._geneXRange];
      }
    });

    _setupControls(data, tracks, xTitle);
  }

  // ── Build all traces for the 4 tracks ────────────────────────────────────────
  // Layout domains:
  //   y  (variants)   [0.56, 1.00]  44%
  //   y2 (gene model) [0.40, 0.54]  14%  (shapes only — invisible anchor trace)
  //   y3 (density)    [0.22, 0.37]  15%
  //   y4 (AF)         [0.00, 0.19]  19%
  // All traces share xaxis:'x' → perfectly synchronized zoom/pan.

  function _buildTraces(data, tracks, xRange, scoreMode) {
    const yCol = SCORE_COL[scoreMode] ?? 0;
    const csqFilter = document.getElementById('filter-consequence')?.value || '';

    // ── Track 1 — Variant scatter ─────────────────────────────────────────────
    const variantTraces = (tracks.variants?.traces || []).map(t => {
      // Pick y values from customdata column for selected score
      const yVals = (t.customdata || []).map(d => {
        const v = d[yCol];
        return (v != null && isFinite(v)) ? v : 0;
      });
      const visible = (!csqFilter || t.name === csqFilter)
        ? (document.getElementById('toggle-variants')?.checked !== false ? true : false)
        : 'legendonly';
      return { ...t, xaxis: 'x', yaxis: 'y', y: yVals, visible };
    });

    // ── Track 2 — Gene model (invisible anchor trace; shapes carry the visuals)
    const geneVis = document.getElementById('toggle-gene-model')?.checked !== false;
    const geneModelTrace = [{
      type: 'scatter', mode: 'markers',
      x: [data.genomic_start, data.genomic_end],
      y: [0.5, 0.5],
      marker: { size: 0, opacity: 0 },
      showlegend: false, hoverinfo: 'none',
      xaxis: 'x', yaxis: 'y2',
      visible: geneVis,
    }];

    // ── Track 3 — Variant density (stacked bar, skip Mean CADD line)
    const densVis = document.getElementById('toggle-density')?.checked !== false;
    const densityTraces = (tracks.density?.traces || [])
      .filter(t => t.name !== 'Mean CADD')
      .map(t => ({ ...t, xaxis: 'x', yaxis: 'y3', visible: densVis }));

    // ── Track 4 — Population AF scatter (-log10 scale)
    const afVis = document.getElementById('toggle-af')?.checked !== false;
    const afTraces = (tracks.af?.traces || []).map(t => ({
      ...t, xaxis: 'x', yaxis: 'y4', visible: afVis,
    }));

    const allTraces = [...variantTraces, ...geneModelTrace, ...densityTraces, ...afTraces];

    // ── Shapes (gene model in y2 space) ──────────────────────────────────────
    const shapes = [];
    if (geneVis) {
      (tracks.gene_model?.shapes || []).forEach(s => {
        shapes.push({ ...s, xref: 'x', yref: 'y2' });
      });
    }

    // CADD=20 reference line (only in CADD score mode)
    if (tracks.variants?.has_cadd && scoreMode === 'cadd') {
      shapes.push({
        type: 'line', xref: 'paper', yref: 'y',
        x0: 0, x1: 1, y0: 20, y1: 20,
        line: { color: '#ef9a9a', width: 1.5, dash: 'dash' }, opacity: 0.8,
      });
    }

    // ── Annotations ───────────────────────────────────────────────────────────
    const annotations = [];
    if (geneVis) {
      (tracks.gene_model?.annotations || []).forEach(a => {
        annotations.push({ ...a, xref: 'x', yref: 'y2' });
      });
    }

    // Subplot title annotations (paper-coordinated)
    annotations.push(
      { text: `<b>Track 1 — Variants</b> <span style="font-weight:normal;color:#9e9e9e">(${SCORE_LABEL[scoreMode] || 'CADD'})</span>`,
        x: 0.01, y: 1.00, xref: 'paper', yref: 'paper', showarrow: false,
        font: { size: 11, color: '#455a64' }, xanchor: 'left', yanchor: 'bottom' },
      { text: '<b>Track 2 — Gene Model</b>',
        x: 0.01, y: 0.545, xref: 'paper', yref: 'paper', showarrow: false,
        font: { size: 11, color: '#455a64' }, xanchor: 'left', yanchor: 'bottom' },
      { text: '<b>Track 3 — Variant Density</b>',
        x: 0.01, y: 0.375, xref: 'paper', yref: 'paper', showarrow: false,
        font: { size: 11, color: '#455a64' }, xanchor: 'left', yanchor: 'bottom' },
      { text: '<b>Track 4 — Population AF</b> <span style="font-weight:normal;color:#9e9e9e">(−log₁₀ scale)</span>',
        x: 0.01, y: 0.195, xref: 'paper', yref: 'paper', showarrow: false,
        font: { size: 11, color: '#455a64' }, xanchor: 'left', yanchor: 'bottom' },
    );

    // CADD=20 label
    if (tracks.variants?.has_cadd && scoreMode === 'cadd') {
      annotations.push({
        text: 'CADD 20', x: 0.99, y: 20, xref: 'paper', yref: 'y',
        showarrow: false, font: { size: 9, color: '#ef9a9a' },
        xanchor: 'right', yanchor: 'bottom',
      });
    }

    return { allTraces, shapes, annotations };
  }

  // ── Build Plotly layout (manual domain assignment — no grid) ─────────────────
  function _buildLayout(xRange, xTitle, tracks, shapes, annotations, scoreMode) {
    return {
      height: 1000,
      xaxis: {
        // anchor:'free' + position:0 places the axis line at the bottom of the
        // plot area (y_paper=0), below Track 4's domain [0.00, 0.19].
        // Without this, Plotly defaults to anchor:'y' which draws the axis line
        // at the bottom of yaxis.domain (y_paper=0.56), causing the title to
        // float over the Gene Model track.
        anchor:      'free',
        position:    0,
        title:       { text: xTitle, standoff: 15 },
        range:       xRange,
        showgrid:    true,
        gridcolor:   '#e8e8e8',
        tickformat:  ',.0f',
        tickangle:   -30,
        automargin:  true,
        domain:      [0, 1],
        zeroline:    false,
      },
      // Track 1 — Variants
      yaxis: {
        title:      { text: SCORE_LABEL[scoreMode] || SCORE_LABEL.cadd, standoff: 6 },
        rangemode:  'tozero',
        showgrid:   true,
        gridcolor:  '#eeeeee',
        domain:     [0.56, 1.00],
        automargin: true,
        tickfont:   { size: 10 },
        zeroline:   false,
      },
      // Track 2 — Gene model (fixed range, no ticks, just shapes)
      yaxis2: {
        title:          '',
        range:          [0, 1],
        showgrid:       false,
        showticklabels: false,
        domain:         [0.40, 0.54],
        automargin:     true,
        fixedrange:     true,
        zeroline:       false,
      },
      // Track 3 — Density
      yaxis3: {
        title:      { text: 'Count', standoff: 6 },
        domain:     [0.22, 0.37],
        showgrid:   true,
        gridcolor:  '#eeeeee',
        automargin: true,
        tickfont:   { size: 10 },
        zeroline:   false,
      },
      // Track 4 — AF
      yaxis4: {
        title:      { text: '−log₁₀(AF)', standoff: 6 },
        domain:     [0.00, 0.19],
        showgrid:   true,
        gridcolor:  '#eeeeee',
        automargin: true,
        tickfont:   { size: 10 },
        zeroline:   false,
      },
      shapes,
      annotations,
      legend: {
        orientation: 'h',
        y:           1.07,
        x:           0,
        font:        { size: 11 },
        bgcolor:     'rgba(255,255,255,0.9)',
        bordercolor: '#e0e0e0',
        borderwidth: 1,
      },
      margin:          { l: 80, r: 40, t: 55, b: 130 },
      plot_bgcolor:    '#fafafa',
      paper_bgcolor:   '#ffffff',
      hovermode:       'closest',
      barmode:         'stack',
    };
  }

  // ── Controls: filters, score mode, track toggles, zoom ──────────────────────
  function _scoreMode() {
    return document.getElementById('y-score')?.value || 'cadd';
  }

  function _redraw() {
    if (!_apiData) return;
    const tracks  = _apiData.tracks || {};
    const hints   = _apiData.layout_hints || {};
    const xTitle  = hints.x_title || `chr${_apiData.chrom} position (GRCh38)`;
    const xRange  = _currentXRange || _geneXRange;
    const sm      = _scoreMode();

    const { allTraces, shapes, annotations } = _buildTraces(_apiData, tracks, xRange, sm);
    const layout = _buildLayout(xRange, xTitle, tracks, shapes, annotations, sm);

    Plotly.react('plotly-gene-browser', allTraces, layout, _plotConfig);
  }

  function _setupControls(data, tracks, xTitle) {
    // Score selector
    document.getElementById('y-score')?.addEventListener('change', _redraw);

    // Consequence filter
    document.getElementById('filter-consequence')?.addEventListener('change', _redraw);

    // Track visibility toggles
    ['toggle-variants', 'toggle-gene-model', 'toggle-density', 'toggle-af'].forEach(id => {
      document.getElementById(id)?.addEventListener('change', _redraw);
    });

    // Zoom buttons — zoom in to a fraction of the full gene span, centered
    function _zoomTo(fraction) {
      if (!_geneXRange) return;
      const center   = (_geneXRange[0] + _geneXRange[1]) / 2;
      const halfSpan = (_geneXRange[1] - _geneXRange[0]) / 2 * fraction;
      const newRange = [center - halfSpan, center + halfSpan];
      _currentXRange = newRange;
      Plotly.relayout('plotly-gene-browser', { 'xaxis.range': newRange });
    }

    document.getElementById('zoom-gene')?.addEventListener('click', () => {
      _currentXRange = [..._geneXRange];
      Plotly.relayout('plotly-gene-browser', { 'xaxis.range': _geneXRange });
    });
    document.getElementById('zoom-75')?.addEventListener('click', () => _zoomTo(0.75));
    document.getElementById('zoom-50')?.addEventListener('click', () => _zoomTo(0.50));
    document.getElementById('zoom-25')?.addEventListener('click', () => _zoomTo(0.25));
  }

  // ── Load variant table ──────────────────────────────────────────────────────
  async function loadVariantTable() {
    try {
      const resp = await fetch(`/api/gene/${GENE_SYMBOL}/variants`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();

      const tbody = document.getElementById('gene-variant-tbody');
      const rows  = data.rows || [];

      if (rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="11" class="px-3 py-4 text-center text-gray-400">No variants found.</td></tr>';
        return;
      }

      const rarityBadge = (r) => {
        const colors = {
          common:        'bg-green-100 text-green-800',
          low_frequency: 'bg-blue-100 text-blue-800',
          rare:          'bg-yellow-100 text-yellow-800',
          ultra_rare:    'bg-red-100 text-red-800',
          novel:         'bg-gray-100 text-gray-600',
        };
        const cls = colors[r] || 'bg-gray-100 text-gray-600';
        return `<span class="px-1.5 py-0.5 rounded text-xs font-medium ${cls}">${r || 'novel'}</span>`;
      };

      const csqColor = (csq) => {
        const colors = {
          stop_gained: '#d32f2f', frameshift_variant: '#b71c1c',
          splice_donor_variant: '#e64a19', splice_acceptor_variant: '#e64a19',
          missense_variant: '#f57c00', synonymous_variant: '#388e3c',
          splice_site_variant: '#e65100', splice_region_variant: '#fb8c00',
        };
        return colors[csq] || '#546e7a';
      };

      tbody.innerHTML = rows.map(row => {
        const aaChange = (row.aa_ref_3 && row.aa_alt_3)
          ? `${row.aa_ref_3}→${row.aa_alt_3}`
          : (row.aa_ref && row.aa_alt ? `${row.aa_ref}→${row.aa_alt}` : '—');
        const af = row.af_global != null ? row.af_global.toExponential(2) : '—';
        const rsid = row.rsid ? `<a href="https://www.ncbi.nlm.nih.gov/snp/${row.rsid}" target="_blank" class="text-indigo-600 hover:underline">${row.rsid}</a>` : '—';

        return `<tr class="border-b border-gray-50 hover:bg-gray-50">
          <td class="px-3 py-1.5">
            <a href="/variants/${row.PRS_ID}" class="text-indigo-600 hover:underline">${row.PRS_ID}</a>
          </td>
          <td class="px-3 py-1.5 font-mono text-xs">${row.CHROM}:${(row.POS||0).toLocaleString()}</td>
          <td class="px-3 py-1.5">${rsid}</td>
          <td class="px-3 py-1.5">
            <span style="color:${csqColor(row.consequence)}" class="font-medium">${row.consequence || '—'}</span>
          </td>
          <td class="px-3 py-1.5 font-mono">${aaChange}</td>
          <td class="px-3 py-1.5 font-mono">${af}</td>
          <td class="px-3 py-1.5">${rarityBadge(row.rarity_class)}</td>
          <td class="px-3 py-1.5">${row.cadd_phred != null ? row.cadd_phred.toFixed(1) : '—'}</td>
          <td class="px-3 py-1.5">${row.revel_score != null ? row.revel_score.toFixed(3) : '—'}</td>
          <td class="px-3 py-1.5 font-mono">${row.BETA != null ? row.BETA.toFixed(4) : '—'}</td>
          <td class="px-3 py-1.5 text-xs">${row.clinvar_clnsig || '—'}</td>
        </tr>`;
      }).join('');

      const countEl = document.getElementById('gene-variant-count');
      if (countEl) {
        countEl.textContent = `Showing ${rows.length} of ${data.total_rows} variants`;
      }

    } catch (err) {
      console.warn('Variant table load failed:', err);
    }
  }

  // ── Init ────────────────────────────────────────────────────────────────────
  loadTracks();
  loadVariantTable();

}  // end if GENE_FOUND

// ── Gene × PGS Matrix (Heatmap 2) ────────────────────────────────────────────
window.loadPgsMatrix = function() {
  var statusEl  = document.getElementById('pgsm-status');
  var chartEl   = document.getElementById('chart-pgsm');
  var tableWrap = document.getElementById('pgsm-table-wrap');
  var tbody     = document.getElementById('pgsm-tbody');

  if (statusEl) statusEl.textContent = 'Loading Gene × PGS matrix…';

  var sortEl   = document.getElementById('pgsm-sort');
  var minVarEl = document.getElementById('pgsm-min-var');
  var sort     = sortEl   ? sortEl.value   : 'sum_abs_beta';
  var minVar   = minVarEl ? (parseInt(minVarEl.value, 10) || 1) : 1;

  fetch('/api/gene/' + encodeURIComponent(GENE_SYMBOL) + '/pgs_matrix'
    + '?sort_by=' + encodeURIComponent(sort)
    + '&min_variants=' + minVar)
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.error || data.total_pgs === 0) {
        var msg = data.error
          ? 'Error loading matrix: ' + data.error
          : 'No PGS remain after min_variants threshold — try lowering it, or no variants were found for ' + GENE_SYMBOL + '.';
        if (statusEl) statusEl.textContent = msg;
        return;
      }

      var rows = data.pgs_rows || [];
      var hm   = data.heatmap  || {};

      // ── Heatmap ──────────────────────────────────────────────────────────
      if (chartEl && hm.pgs_ids && hm.pgs_ids.length > 0) {
        chartEl.style.display = 'block';

        var metricLabels = {
          n_variants:           'Variants (norm)',
          coding_count:         'Coding (norm)',
          splice_count:         'Splice (norm)',
          sum_abs_beta:         'Σ|BETA| (norm)',
          mean_cadd:            'Mean CADD (norm)',
          max_ranking_score:    'Max Score',
          mean_ranking_score:   'Mean Score',
        };
        var yLabels = (hm.metrics || []).map(function(m) { return metricLabels[m] || m; });

        Plotly.newPlot(chartEl, [{
          type:         'heatmap',
          x:            hm.pgs_ids,
          y:            yLabels,
          z:            hm.z,
          colorscale:   'Blues',
          showscale:    true,
          hovertemplate: '<b>%{x}</b><br>%{y}: %{z:.3f}<extra></extra>',
        }], {
          margin:        { t: 10, b: 70, l: 140, r: 20 },
          xaxis:         { tickangle: -45, tickfont: { size: 9 } },
          yaxis:         { tickfont: { size: 10 } },
          paper_bgcolor: 'white',
          plot_bgcolor:  'white',
        }, { responsive: true, displayModeBar: false });
      }

      // ── Table ─────────────────────────────────────────────────────────────
      if (tbody && rows.length > 0) {
        if (tableWrap) tableWrap.style.display = 'block';

        var scoreCls = function(s) {
          return s >= 0.6 ? 'bg-red-100 text-red-700 font-semibold'
               : s >= 0.35? 'bg-orange-100 text-orange-700'
               :             'bg-gray-100 text-gray-500';
        };

        tbody.innerHTML = rows.map(function(r) {
          return '<tr class="border-b border-gray-50 hover:bg-gray-50">'
            + '<td class="px-3 py-1.5 font-medium text-indigo-700">'
            +   '<a href="/variants/' + encodeURIComponent(r.pgs_id) + '" class="hover:underline">'
            +   r.pgs_id + '</a></td>'
            + '<td class="px-3 py-1.5 text-right font-mono">' + r.n_variants + '</td>'
            + '<td class="px-3 py-1.5 text-right font-mono">' + r.coding_count + '</td>'
            + '<td class="px-3 py-1.5 text-right font-mono">' + r.splice_count + '</td>'
            + '<td class="px-3 py-1.5 text-right font-mono">' + Number(r.sum_abs_beta).toFixed(4) + '</td>'
            + '<td class="px-3 py-1.5 text-right font-mono">' + (r.mean_cadd != null ? Number(r.mean_cadd).toFixed(1) : '—') + '</td>'
            + '<td class="px-3 py-1.5 text-center">'
            +   '<span class="px-1.5 py-0.5 rounded text-xs ' + scoreCls(r.max_ranking_score) + '">'
            +   Number(r.max_ranking_score).toFixed(3) + '</span></td>'
            + '<td class="px-3 py-1.5 text-center">'
            +   '<span class="px-1.5 py-0.5 rounded text-xs ' + scoreCls(r.mean_ranking_score) + '">'
            +   Number(r.mean_ranking_score).toFixed(3) + '</span></td>'
            + '<td class="px-3 py-1.5">'
            +   '<a href="/variants/' + encodeURIComponent(r.pgs_id) + '"'
            +   ' class="text-xs px-2 py-0.5 rounded bg-indigo-50 text-indigo-700 hover:bg-indigo-100">Variants →</a>'
            + '</td>'
            + '</tr>';
        }).join('');
      }

      if (statusEl) statusEl.textContent =
        GENE_SYMBOL + ' found in ' + data.total_pgs + ' PGS scores.';
    })
    .catch(function(err) {
      if (statusEl) statusEl.textContent = 'Error loading matrix: ' + err.message;
    });
};

// ── PRS × Clinical panel ──────────────────────────────────────────────────────
window.loadPrsClinical = function() {
  var statusEl  = document.getElementById('prs-clinical-status');
  var contentEl = document.getElementById('prs-clinical-content');
  var geneRow   = document.getElementById('prs-gene-row');

  if (statusEl) statusEl.textContent = 'Loading PRS × clinical impact…';

  fetch('/api/prs/clinical')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.error) {
        if (statusEl) statusEl.textContent = data.error;
        return;
      }

      var genes   = data.genes || [];
      var thisGene = genes.find(function(g) {
        return g.gene_name.toUpperCase() === GENE_SYMBOL.toUpperCase();
      });

      if (contentEl) contentEl.style.display = 'block';

      // ── Row for this gene ────────────────────────────────────────────────────
      if (geneRow) {
        if (thisGene) {
          var confMap = { high: 'bg-emerald-200 text-emerald-900', medium: 'bg-yellow-200 text-yellow-900', low: 'bg-gray-200 text-gray-700' };
          var confCls = confMap[thisGene.confidence] || 'bg-gray-200 text-gray-700';
          geneRow.innerHTML =
            '<div class="grid grid-cols-2 md:grid-cols-4 gap-4">' +
              '<div title="Unique genomic positions (CHROM · POS · Effect allele) where gene_name = ' + GENE_SYMBOL + ', counted once regardless of how many PGS files include them."><span class="block text-xs text-gray-500 mb-0.5">Unique Variants in PRS <span class="text-gray-400 cursor-help" title="Unique genomic positions across all PGS files">ⓘ</span></span><span class="text-xl font-bold text-indigo-700">' + (thisGene.n_variants || 0) + '</span>' + (thisGene.n_variant_records && thisGene.n_variant_records !== thisGene.n_variants ? '<span class="block text-[10px] text-gray-400">' + thisGene.n_variant_records + ' records across PGS</span>' : '') + '</div>' +
              '<div><span class="block text-xs text-gray-500 mb-0.5">Genetic Load (Σ|BETA|)</span><span class="text-xl font-bold text-orange-600">' + Number(thisGene.genetic_load || 0).toFixed(4) + '</span></div>' +
              '<div><span class="block text-xs text-gray-500 mb-0.5">Mean Ranking Score</span><span class="text-xl font-bold text-red-600">' + Number(thisGene.mean_ranking_score || 0).toFixed(3) + '</span></div>' +
              '<div><span class="block text-xs text-gray-500 mb-0.5">Confidence</span>' +
                (thisGene.confidence ? '<span class="px-2 py-0.5 rounded text-xs font-medium ' + confCls + '">' + thisGene.confidence.toUpperCase() + '</span>' : '<span class="text-gray-400">—</span>') +
              '</div>' +
            '</div>' +
            (thisGene.disease ? '<div class="mt-2 text-xs text-gray-500">Disease: <span class="text-emerald-700">' + thisGene.disease + '</span></div>' : '');
        } else {
          geneRow.innerHTML = '<p class="text-gray-500 text-sm">No PRS variants found for ' + GENE_SYMBOL + ' in clinical genes dataset.</p>';
        }
      }

      // ── Score distribution across all clinical genes ──────────────────────
      var chartEl = document.getElementById('chart-prs-scores');
      if (chartEl && genes.length > 0) {
        var top20 = genes.slice(0, 20);
        var labels  = top20.map(function(g) { return g.gene_name; });
        var loads   = top20.map(function(g) { return Number(g.genetic_load || 0); });
        var scores  = top20.map(function(g) { return Number(g.mean_ranking_score || 0); });
        var colors  = labels.map(function(lbl) {
          return lbl.toUpperCase() === GENE_SYMBOL.toUpperCase() ? '#e53935' : '#7986cb';
        });

        Plotly.newPlot(chartEl, [
          { type: 'bar', x: labels, y: loads, name: 'Genetic Load (Σ|BETA|)',
            marker: { color: colors }, yaxis: 'y' },
          { type: 'scatter', mode: 'markers', x: labels, y: scores,
            name: 'Mean Ranking Score', marker: { color: '#fb8c00', size: 8 },
            yaxis: 'y2' },
        ], {
          barmode:       'group',
          margin:        { t: 10, b: 70, l: 55, r: 55 },
          xaxis:         { tickangle: -45, tickfont: { size: 9 } },
          yaxis:         { title: { text: 'Genetic Load' }, side: 'left' },
          yaxis2:        { title: { text: 'Ranking Score' }, side: 'right', overlaying: 'y', range: [0, 1] },
          legend:        { orientation: 'h', y: -0.45 },
          paper_bgcolor: 'white', plot_bgcolor: 'white',
        }, { responsive: true, displayModeBar: false });
      }

      if (statusEl) statusEl.textContent =
        data.n_clinical_genes_with_variants + ' clinical genes with PRS variants · ' +
        'Total load: ' + Number(data.total_genetic_load || 0).toFixed(4);
    })
    .catch(function(err) {
      if (statusEl) statusEl.textContent = 'Error: ' + err.message;
    });
};

</script>
