%rebase('_base.tpl', title=pgs_id)

<!-- Breadcrumb -->
<nav class="text-sm text-gray-400 mb-4">
  <a href="/" class="hover:text-indigo-600">Catalog</a>
  <span class="mx-2">›</span>
  <span class="text-gray-700 font-medium">{{pgs_id}}</span>
</nav>

<!-- Header -->
<div class="flex flex-col sm:flex-row sm:items-start gap-4 mb-6">
  <div class="flex-1">
    <h1 class="text-2xl font-bold text-gray-800">{{pgs_id}}</h1>
    <p class="text-gray-500 mt-1">{{info.get('trait_name', 'Unknown trait')}}</p>
    % if info.get('efo_display'):
    <p class="text-xs text-gray-400 mt-0.5">EFO: {{info['efo_display']}}</p>
    % end
  </div>
  <div class="flex gap-2 flex-wrap">
    <a href="/pgs/{{pgs_id}}/source"
       class="px-3 py-1.5 text-sm bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
      Source Info
    </a>
    <a href="/api/pipeline/{{pgs_id}}/plan" target="_blank"
       class="px-3 py-1.5 text-sm bg-indigo-100 text-indigo-700 rounded hover:bg-indigo-200">
      Pipeline Plan ↗
    </a>
    <a href="/variants/{{pgs_id}}"
       class="px-3 py-1.5 text-sm bg-emerald-100 text-emerald-700 rounded hover:bg-emerald-200">
      🔬 Variant Annotation
    </a>
  </div>
</div>

<!-- Stats cards (populated by JS from /api/data/<pgs_id>) -->
<div id="statsError" class="hidden bg-red-50 border border-red-200 rounded p-4 mb-4 text-red-700 text-sm"></div>

<div id="statsCards" class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-5 gap-3 mb-6">
  % for label in ['n', 'mean', 'stddev', 'min', 'max']:
  <div class="bg-white border border-gray-200 rounded-lg px-4 py-3">
    <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">{{label}}</div>
    <div id="stat-{{label}}" class="text-lg font-semibold text-gray-700">—</div>
  </div>
  % end
</div>

<div class="grid sm:grid-cols-3 gap-3 mb-6">
  <div class="bg-white border border-gray-200 rounded-lg px-4 py-3">
    <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">Median</div>
    <div id="stat-median" class="text-lg font-semibold text-gray-700">—</div>
  </div>
  <div class="bg-white border border-gray-200 rounded-lg px-4 py-3">
    <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">P5 / P95</div>
    <div id="stat-p5p95" class="text-lg font-semibold text-gray-700">—</div>
  </div>
  <div class="bg-white border border-gray-200 rounded-lg px-4 py-3">
    <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">Score column</div>
    <div id="stat-col" class="text-base font-mono text-indigo-600">—</div>
  </div>
</div>

<!-- Histogram -->
<div class="bg-white border border-gray-200 rounded-lg shadow-sm mb-6 p-4">
  <h2 class="font-semibold text-gray-700 mb-3">Score distribution</h2>
  <div id="histogram" class="w-full" style="height:320px;">
    <div class="flex items-center justify-center h-full text-gray-400">Loading…</div>
  </div>
</div>

<!-- Metadata and files -->
<div class="grid sm:grid-cols-2 gap-4 mb-6">

  <!-- Local metadata -->
  <div class="bg-white border border-gray-200 rounded-lg p-4">
    <h2 class="font-semibold text-gray-700 mb-3">Metadata</h2>
    <dl class="text-sm space-y-1.5">
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Variants</dt>
        <dd class="text-gray-700">
          % nv = info.get('n_variants')
          {{'{:,}'.format(nv) if nv else '—'}}
        </dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Chromosomes</dt>
        <dd class="text-gray-700">{{info.get('chrom_display', '—')}}</dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Parquet</dt>
        <dd>
          % if info.get('has_parquet'):
          <span class="text-green-600 font-medium">Yes</span>
          % else:
          <span class="text-gray-400">No</span>
          % end
        </dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">TSV total</dt>
        <dd>
          % if info.get('has_tsv'):
          <span class="text-yellow-600 font-medium">Yes</span>
          % else:
          <span class="text-gray-400">No</span>
          % end
        </dd>
      </div>
      % tmeta = info.get('total_metadata') or {}
      % if tmeta.get('run_timestamp'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Last run</dt>
        <dd class="text-gray-700 text-xs">{{tmeta['run_timestamp'][:19]}}</dd>
      </div>
      % end
      % if tmeta.get('missing_strategy'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Missing strategy</dt>
        <dd class="text-gray-700">{{tmeta['missing_strategy']}}</dd>
      </div>
      % end
      % if tmeta.get('n_samples'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-32 shrink-0">Samples</dt>
        <dd class="text-gray-700">{{'{:,}'.format(tmeta['n_samples'])}}</dd>
      </div>
      % end
    </dl>
  </div>

  <!-- File listing -->
  <div class="bg-white border border-gray-200 rounded-lg p-4">
    <h2 class="font-semibold text-gray-700 mb-3">Files</h2>
    <div class="overflow-y-auto max-h-52">
      <table class="w-full text-xs">
        <thead>
          <tr class="text-gray-400 border-b border-gray-100">
            <th class="text-left pb-1 font-medium">Name</th>
            <th class="text-right pb-1 font-medium">MB</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-50">
          % for f in info.get('files', []):
          <tr class="hover:bg-gray-50">
            <td class="py-0.5 font-mono text-gray-600 truncate max-w-xs" title="{{f['name']}}">
              % if f['name'].endswith('.parquet'):
              <span class="text-green-600">{{f['name']}}</span>
              % elif f['name'].endswith('.tsv'):
              <span class="text-yellow-700">{{f['name']}}</span>
              % else:
              {{f['name']}}
              % end
            </td>
            <td class="py-0.5 text-right text-gray-400">{{f.get('size_mb', '—')}}</td>
          </tr>
          % end
        </tbody>
      </table>
    </div>
  </div>

</div>

<script>
(function () {
  const pgsId = "{{pgs_id}}";

  fetch("/api/data/" + pgsId)
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        document.getElementById("statsError").textContent = "Error loading stats: " + data.error;
        document.getElementById("statsError").classList.remove("hidden");
        document.getElementById("histogram").innerHTML =
          '<div class="flex items-center justify-center h-full text-red-400">No data available</div>';
        return;
      }

      const fmt = v => (v == null ? "—" : v.toExponential(4));
      const fmtN = v => (v == null ? "—" : v.toLocaleString());

      document.getElementById("stat-n").textContent    = fmtN(data.n);
      document.getElementById("stat-mean").textContent  = fmt(data.mean);
      document.getElementById("stat-stddev").textContent = fmt(data.stddev);
      document.getElementById("stat-min").textContent   = fmt(data.min);
      document.getElementById("stat-max").textContent   = fmt(data.max);
      document.getElementById("stat-median").textContent = fmt(data.median);
      document.getElementById("stat-p5p95").textContent =
        fmt(data.p05) + " / " + fmt(data.p95);
      document.getElementById("stat-col").textContent   = data.score_column || "—";

      // Histogram
      const hist = data.histogram || {};
      if (hist.edges && hist.edges.length > 1) {
        const midpoints = hist.edges.slice(0, -1).map(
          (e, i) => (e + hist.edges[i + 1]) / 2
        );
        Plotly.newPlot("histogram", [{
          type: "bar",
          x: midpoints,
          y: hist.counts,
          marker: { color: "#6366f1", opacity: 0.85 },
          hovertemplate: "Score: %{x:.4e}<br>Count: %{y:,}<extra></extra>",
        }], {
          margin: { t: 10, r: 10, b: 50, l: 60 },
          xaxis: {
            title: { text: data.score_column },
            tickformat: ".3e",
          },
          yaxis: { title: { text: "Count" } },
          paper_bgcolor: "transparent",
          plot_bgcolor: "#fafafa",
        }, { responsive: true });
      } else {
        document.getElementById("histogram").innerHTML =
          '<div class="flex items-center justify-center h-full text-gray-400">Histogram not available</div>';
      }
    })
    .catch(err => {
      document.getElementById("statsError").textContent = "Failed to fetch stats: " + err;
      document.getElementById("statsError").classList.remove("hidden");
    });
})();
</script>
