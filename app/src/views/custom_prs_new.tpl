%rebase('_base.tpl', title='Custom PRS Plan')

<div class="mb-6">
  <h1 class="text-2xl font-bold text-gray-800">Custom PRS Plan</h1>
  <p class="text-sm text-gray-500 mt-1">
    Define your own polygenic score and get a full pipeline plan — no jobs are submitted.
  </p>
</div>

<!-- Info banner -->
<div class="bg-indigo-50 border border-indigo-200 rounded-lg p-4 mb-6 text-sm text-indigo-800">
  <strong>How this works:</strong> Fill in the form → the platform generates a technical plan
  describing the exact files, commands, and Slurm scripts needed to compute your PRS on the
  MCPS cohort (~140k samples). The same pipeline used for PGS Catalog scores applies here.
</div>

<div class="bg-white border border-gray-200 rounded-lg p-6 shadow-sm">
  <form id="customPrsForm" class="space-y-5">

    <!-- Analysis name -->
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">
        Analysis name <span class="text-red-500">*</span>
      </label>
      <input name="analysis_name" type="text" required
             placeholder="e.g. MyGWAS_T2D_PRS_v1"
             class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400" />
      <p class="text-xs text-gray-400 mt-1">Descriptive name for this analysis (no spaces recommended)</p>
    </div>

    <!-- PGS ID (optional) -->
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">PGS Catalog ID (optional)</label>
      <input name="pgs_id" type="text"
             placeholder="e.g. PGS000004 — leave blank if not from catalog"
             class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400" />
      <p class="text-xs text-gray-400 mt-1">If this is a new catalog score, enter its PGS ID. Otherwise leave blank.</p>
    </div>

    <!-- Input type + genome build -->
    <div class="grid sm:grid-cols-2 gap-4">
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Input type</label>
        <select name="input_type"
                class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 bg-white">
          <option value="scoring_file">Scoring file (effect weights per variant)</option>
          <option value="gwas_sumstats">GWAS summary statistics (requires clumping)</option>
          <option value="ldpred2">LDpred2 posterior effects</option>
        </select>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Reference genome build</label>
        <select name="genome_build"
                class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 bg-white">
          <option value="GRCh38">GRCh38 / hg38 (preferred)</option>
          <option value="GRCh37">GRCh37 / hg19 (liftover required)</option>
        </select>
      </div>
    </div>

    <!-- Source file path -->
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">Source file path</label>
      <input name="source_file" type="text"
             placeholder="/path/to/scores/<ID>/<ID>_hmPOS_GRCh38.txt.gz"
             class="w-full border border-gray-300 rounded px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-400" />
      <p class="text-xs text-gray-400 mt-1">Absolute path to your scoring file on the cluster filesystem</p>
    </div>

    <!-- Output directory -->
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">Output directory</label>
      <input name="output_dir" type="text"
             placeholder="/path/to/scores/<ID>/"
             class="w-full border border-gray-300 rounded px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-indigo-400" />
      <p class="text-xs text-gray-400 mt-1">Where output files will be written. The pipeline follows the standard layout.</p>
    </div>

    <!-- Betamap + chromosomes -->
    <div class="grid sm:grid-cols-2 gap-4">
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Needs betamap preparation?</label>
        <select name="needs_betamap"
                class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 bg-white">
          <option value="true">Yes — run format/betamap step</option>
          <option value="false">No — betamap already exists</option>
        </select>
        <p class="text-xs text-gray-400 mt-1">
          Required columns: PRS_ID, CHROM, POS, ID, EFFECT_ALLELE, OTHER_ALLELE, BETA, IS_FLIP
        </p>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 mb-1">Chromosomes to compute</label>
        <input name="chromosomes_str" type="text" value="1-22"
               class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400" />
        <p class="text-xs text-gray-400 mt-1">Range (1-22), comma list (1,2,3), or "1-22,X"</p>
      </div>
    </div>

    <!-- Notes -->
    <div>
      <label class="block text-sm font-medium text-gray-700 mb-1">Notes / context</label>
      <textarea name="notes" rows="2"
                placeholder="Trait, publication, special considerations…"
                class="w-full border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 resize-none"></textarea>
    </div>

    <button type="submit"
            class="px-5 py-2 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700 font-medium">
      Generate Pipeline Plan →
    </button>
  </form>
</div>

<!-- Result area -->
<div id="planResult" class="hidden mt-6"></div>

<script>
(function () {
  const form = document.getElementById('customPrsForm');
  const resultDiv = document.getElementById('planResult');

  form.addEventListener('submit', async function (e) {
    e.preventDefault();
    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    btn.textContent = 'Generating…';
    resultDiv.classList.add('hidden');

    const fd = new FormData(form);
    const body = {};
    fd.forEach((v, k) => { body[k] = v; });

    // Parse chromosomes
    const chromStr = (body.chromosomes_str || '1-22').trim();
    let chroms = [];
    for (const part of chromStr.split(',')) {
      const m = part.trim().match(/^(\d+)-(\d+)$/);
      if (m) {
        for (let i = parseInt(m[1]); i <= parseInt(m[2]); i++) chroms.push(i);
      } else if (part.trim().toUpperCase() === 'X') {
        chroms.push('X');
      } else if (part.trim()) {
        chroms.push(parseInt(part.trim()));
      }
    }
    body.chromosomes = chroms;
    delete body.chromosomes_str;

    try {
      const resp = await fetch('/api/custom-prs/plan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      const data = await resp.json();

      if (data.error) {
        resultDiv.innerHTML = `<div class="bg-red-50 border border-red-200 rounded p-4 text-red-700 text-sm">${data.error}</div>`;
      } else {
        resultDiv.innerHTML = renderPlan(data);
      }
      resultDiv.classList.remove('hidden');
      resultDiv.scrollIntoView({ behavior: 'smooth' });
    } catch (err) {
      resultDiv.innerHTML = `<div class="bg-red-50 border border-red-200 rounded p-4 text-red-700 text-sm">Request failed: ${err}</div>`;
      resultDiv.classList.remove('hidden');
    } finally {
      btn.disabled = false;
      btn.textContent = 'Generate Pipeline Plan →';
    }
  });

  function renderPlan(data) {
    const steps = (data.steps || []).map((s, i) => `
      <div class="flex gap-4 p-3 rounded-lg border bg-yellow-50 border-yellow-200 mb-3">
        <div class="flex-none w-8 h-8 rounded-full bg-indigo-100 text-indigo-700 flex items-center justify-center font-bold text-sm">${s.step}</div>
        <div class="flex-1">
          <div class="font-medium text-gray-800">${s.name}</div>
          <div class="text-sm text-gray-600 mt-0.5">${s.description}</div>
          ${s.output_pattern ? `<div class="text-xs font-mono text-gray-400 mt-1">→ ${s.output_pattern}</div>` : ''}
          ${s.submit_example ? `<pre class="mt-2 text-xs bg-indigo-900 text-indigo-200 rounded p-2 overflow-x-auto">${s.submit_example}</pre>` : ''}
          ${s.command_example ? `<pre class="mt-2 text-xs bg-gray-900 text-green-300 rounded p-2 overflow-x-auto">${s.command_example}</pre>` : ''}
          ${s.notes ? `<div class="text-xs text-gray-400 italic mt-1">${s.notes}</div>` : ''}
        </div>
      </div>
    `).join('');

    const files = (data.expected_files || []).map(f =>
      `<li class="font-mono text-xs text-gray-600">${f}</li>`
    ).join('');

    const res = data.suggested_resources || {};

    return `
      <div class="bg-white border border-indigo-200 rounded-lg p-5 mb-4">
        <div class="flex items-center justify-between mb-4">
          <h2 class="font-semibold text-indigo-800">
            Pipeline plan: <span class="font-mono">${data.analysis_name}</span>
          </h2>
          <span class="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded">informational only</span>
        </div>

        <div class="grid sm:grid-cols-3 gap-3 mb-5 text-sm">
          <div><span class="text-gray-400">PGS ID:</span> <span class="font-mono font-medium">${data.pgs_id}</span></div>
          <div><span class="text-gray-400">Build:</span> ${data.genome_build}</div>
          <div><span class="text-gray-400">Chroms:</span> ${(data.chromosomes||[]).length}</div>
          <div><span class="text-gray-400">Output:</span> <code class="text-xs">${data.output_dir}</code></div>
          <div><span class="text-gray-400">Betamap prep:</span> ${data.needs_betamap ? 'Yes' : 'No (already exists)'}</div>
        </div>

        <h3 class="font-medium text-gray-700 mb-2">Steps</h3>
        ${steps}

        <h3 class="font-medium text-gray-700 mb-2 mt-4">Expected files</h3>
        <ul class="bg-gray-50 rounded p-3 space-y-0.5 list-disc list-inside">${files}</ul>

        <h3 class="font-medium text-gray-700 mb-2 mt-4">Suggested Slurm resources</h3>
        <div class="bg-gray-50 rounded p-3 text-xs font-mono text-gray-600 space-y-0.5">
          ${Object.entries(res).filter(([k])=>k!=='note').map(([k,v])=>`<div>#SBATCH --${k.replace(/_/g,'-')}=${v}</div>`).join('')}
        </div>
        ${res.note ? `<p class="text-xs text-gray-400 italic mt-1">${res.note}</p>` : ''}

        <div class="mt-4 text-xs text-gray-400 border-t border-gray-100 pt-3">
          Betamap columns: <code>${data.betamap_columns.join(', ')}</code>
        </div>
      </div>
    `;
  }
})();
</script>
