%rebase('_base.tpl', title=pgs_id + ' · Source')

<!-- Breadcrumb -->
<nav class="text-sm text-gray-400 mb-4">
  <a href="/" class="hover:text-indigo-600">Catalog</a>
  <span class="mx-2">›</span>
  <span class="text-gray-700 font-medium">{{pgs_id}}</span>
  <span class="mx-2">›</span>
  <span class="text-gray-500">Source</span>
</nav>

<!-- Header + source badge -->
<div class="flex flex-col sm:flex-row sm:items-center gap-4 mb-6">
  <div>
    <h1 class="text-2xl font-bold text-gray-800">{{pgs_id}}</h1>
    <%
      label_map = {
        "both":        ("Both local & remote", "bg-green-100 text-green-700"),
        "local_only":  ("Local only",          "bg-indigo-100 text-indigo-700"),
        "remote_only": ("Remote only — not computed locally", "bg-yellow-100 text-yellow-700"),
        "not_found":   ("Not found",           "bg-red-100 text-red-600"),
      }
      src_label, src_cls = label_map.get(source, ("Unknown", "bg-gray-100 text-gray-500"))
    %>
    <span class="inline-block mt-1 px-3 py-0.5 text-sm rounded-full font-medium {{src_cls}}">
      {{src_label}}
    </span>
  </div>
  <div class="sm:ml-auto flex gap-2 flex-wrap">
    % if source in ("both", "local_only") and local_info and local_info.get('has_results'):
    <a href="/dashboard/{{pgs_id}}"
       class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-700">
      Open Dashboard
    </a>
    % end
    <a href="/pipeline/{{pgs_id}}/plan"
       class="px-3 py-1.5 text-sm bg-yellow-600 text-white rounded hover:bg-yellow-700">
      Pipeline Plan
    </a>
    <a href="/api/pipeline/{{pgs_id}}/plan" target="_blank"
       class="px-3 py-1.5 text-sm bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
      Plan JSON ↗
    </a>
    % if source in ("remote_only", "not_found"):
    <a href="/api/pipeline/{{pgs_id}}/manifest" target="_blank"
       class="px-3 py-1.5 text-sm bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
      Manifest JSON ↗
    </a>
    % end
  </div>
</div>

% if source == "not_found":
<div class="bg-red-50 border border-red-200 rounded p-6 text-center text-red-700">
  <p class="font-medium">{{pgs_id}} was not found locally or remotely.</p>
  % if remote_error:
  <p class="text-sm mt-1 text-red-500">Remote error: {{remote_error}}</p>
  % end
  <a href="/search" class="inline-block mt-3 text-sm text-red-700 underline">Try searching</a>
</div>
% else:

<div class="grid sm:grid-cols-2 gap-4 mb-6">

  <!-- Local status -->
  <div class="bg-white border border-gray-200 rounded-lg p-5">
    <h2 class="font-semibold text-gray-700 mb-3">
      % if local_info:
      <span class="text-green-600">✓</span> Local status
      % else:
      <span class="text-gray-300">○</span> Local status
      % end
    </h2>
    % if local_info:
    <dl class="text-sm space-y-1.5">
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Trait</dt>
        <dd class="text-gray-700">{{local_info.get('trait_name','—')}}</dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Variants</dt>
        <dd class="text-gray-700">
          % nv = local_info.get('n_variants')
          {{'{:,}'.format(nv) if nv else '—'}}
        </dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Chromosomes</dt>
        <dd class="text-gray-700">{{local_info.get('chrom_display','—')}}</dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Parquet</dt>
        <dd>
          % if local_info.get('has_parquet'):
          <span class="text-green-600 font-medium">Yes</span>
          % else:
          <span class="text-gray-400">No</span>
          % end
        </dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">TSV total</dt>
        <dd>
          % if local_info.get('has_tsv'):
          <span class="text-yellow-600 font-medium">Yes</span>
          % else:
          <span class="text-gray-400">No</span>
          % end
        </dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Directory</dt>
        <dd class="text-xs font-mono text-gray-500 break-all">{{local_info.get('pgs_dir','—')}}</dd>
      </div>
    </dl>
    % else:
    <p class="text-sm text-gray-400 mb-3">Not present in local scores directory.</p>
    % if source == "remote_only":
    <a href="/pipeline/{{pgs_id}}/plan"
       class="inline-block px-3 py-1.5 text-xs bg-yellow-600 text-white rounded hover:bg-yellow-700">
      View preparation plan →
    </a>
    % end
    % end
  </div>

  <!-- Remote metadata -->
  <div class="bg-white border border-gray-200 rounded-lg p-5">
    <h2 class="font-semibold text-gray-700 mb-3">
      % if remote_info:
      <span class="text-blue-600">☁</span> Remote metadata (PGS Catalog)
      % elif not cfg.REMOTE_ENABLED:
      <span class="text-gray-300">○</span> Remote (disabled)
      % else:
      <span class="text-gray-300">○</span> Remote metadata
      % end
    </h2>
    % if remote_error:
    <p class="text-sm text-red-500">{{remote_error}}</p>
    % elif not cfg.REMOTE_ENABLED:
    <p class="text-sm text-gray-400">Remote queries are disabled (APP_REMOTE_ENABLED=false).</p>
    % elif remote_info:
    <dl class="text-sm space-y-1.5">
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Name</dt>
        <dd class="text-gray-700">{{remote_info.get('name','—')}}</dd>
      </div>
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Trait</dt>
        <dd class="text-gray-700">{{remote_info.get('trait_reported','—')}}</dd>
      </div>
      % if remote_info.get('efo_display'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">EFO</dt>
        <dd class="text-gray-500 text-xs">{{remote_info.get('efo_display','')}}</dd>
      </div>
      % end
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Variants</dt>
        <dd class="text-gray-700">
          % nv = remote_info.get('variants_number')
          {{'{:,}'.format(nv) if nv else '—'}}
        </dd>
      </div>
      % pub = remote_info.get('publication') or {}
      % if pub.get('title'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Publication</dt>
        <dd class="text-gray-700 text-xs">{{pub.get('title','')}}</dd>
      </div>
      % end
      % if pub.get('pmid'):
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">PMID</dt>
        <dd class="text-gray-700">{{pub.get('pmid','')}}</dd>
      </div>
      % end
      <div class="flex gap-2">
        <dt class="text-gray-400 w-28 shrink-0">Harmonised</dt>
        <dd>
          % if remote_info.get('is_harmonized'):
          <span class="text-green-600">GRCh38 ✓</span>
          % else:
          <span class="text-gray-400">Not confirmed</span>
          % end
        </dd>
      </div>
    </dl>
    % else:
    <p class="text-sm text-gray-400">No remote data available.</p>
    % end
  </div>

</div>

<!-- Preparation panel for remote-only scores -->
% if source in ("remote_only",) and manifest:
<div class="bg-amber-50 border border-amber-200 rounded-lg p-5 mb-6" id="prep-panel">
  <div class="flex items-center justify-between mb-4">
    <h2 class="font-semibold text-amber-800">
      Prepare score for pipeline
    </h2>
    <div class="flex gap-2">
      <!-- Prepare Score button — POSTs to /api/pgs/<id>/prepare -->
      % stage = manifest.get('current_stage', 'not_started')
      % ready = manifest.get('ready_for_compute', False)
      % if not ready:
      <button id="btn-prepare"
              onclick="startPreparation('{{pgs_id}}')"
              class="px-3 py-1.5 text-xs bg-green-600 text-white rounded hover:bg-green-700 font-medium">
        Prepare Score
      </button>
      % else:
      <span class="px-3 py-1.5 text-xs bg-green-100 text-green-700 rounded font-medium">
        Ready for compute
      </span>
      % end
      <a href="/pipeline/{{pgs_id}}/plan"
         class="px-3 py-1.5 text-xs bg-amber-600 text-white rounded hover:bg-amber-700">
        Full plan →
      </a>
    </div>
  </div>

  <!-- Preparation stage badge -->
  <%
    stage_labels = {
      'not_started':          ('Not started',                    'bg-gray-200 text-gray-600'),
      'catalog_metadata':     ('Metadata downloaded',            'bg-blue-100 text-blue-700'),
      'raw_harmonized_score': ('Score file downloaded',          'bg-blue-200 text-blue-800'),
      'column_map':           ('Column map created',             'bg-yellow-100 text-yellow-700'),
      'betamap':              ('Betamap ready – ready for compute', 'bg-green-100 text-green-700'),
      'aggregated_scores':    ('Scores aggregated',              'bg-green-200 text-green-800'),
      'parquet_export':       ('Dashboard ready',                'bg-green-600 text-white'),
    }
    sl, sc_ = stage_labels.get(stage, (stage, 'bg-gray-100 text-gray-600'))
  %>
  <div class="mb-3 text-sm" id="stage-badge-row">
    <span class="text-gray-500">Current stage:</span>
    <span id="stage-badge" class="ml-2 px-2 py-0.5 rounded-full text-xs font-medium {{sc_}}">{{sl}}</span>
  </div>
  % if manifest.get('next_step'):
  <div class="mb-4 text-sm text-amber-800" id="next-step-row">
    <span class="font-medium">Next action:</span>
    <span id="next-step-text">{{manifest.get('next_step','')}}</span>
  </div>
  % end

  <!-- Progress area (hidden until prepare is triggered) -->
  <div id="prep-progress" class="hidden mb-4">
    <div class="text-xs font-medium text-amber-700 mb-2">Preparation progress</div>
    <div id="prep-steps" class="space-y-1.5"></div>
    <div id="prep-error" class="hidden mt-2 text-xs text-red-600 bg-red-50 border border-red-200 rounded p-2"></div>
  </div>

  <!-- File layout -->
  <div class="overflow-x-auto">
    <table class="w-full text-xs" id="file-table">
      <thead>
        <tr class="text-gray-500 border-b border-amber-200">
          <th class="text-left pb-1 pr-4 font-medium">File</th>
          <th class="text-center pb-1 pr-2 font-medium">Required</th>
          <th class="text-center pb-1 pr-4 font-medium w-12">Exists</th>
          <th class="text-left pb-1 font-medium">Produced by</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-amber-100" id="file-tbody">
        % for f in manifest.get('files', []):
        % if '{N}' not in f.get('filename',''):
        <tr class="hover:bg-amber-50">
          <td class="py-1 pr-4 font-mono text-gray-700">{{f.get('filename','')}}</td>
          <td class="py-1 pr-2 text-center">
            % if f.get('required'):
            <span class="text-red-500">●</span>
            % else:
            <span class="text-gray-300">○</span>
            % end
          </td>
          <td class="py-1 pr-4 text-center exists-cell">
            % if f.get('exists'):
            <span class="text-green-600">✓</span>
            % else:
            <span class="text-gray-300">—</span>
            % end
          </td>
          <td class="py-1 text-gray-500">{{f.get('produced_by','')}}</td>
        </tr>
        % end
        % end
      </tbody>
    </table>
  </div>

  <!-- Key commands (first two steps, informational) -->
  % if plan:
  <div class="mt-4 space-y-3">
    % for step in (plan.get('steps') or [])[:2]:
    % if step.get('command_example') or step.get('pgscat_cmd'):
    <div>
      <div class="text-xs font-medium text-amber-700 mb-1">Step {{step['step']}}: {{step['name']}}</div>
      % cmd = step.get('command_example') or step.get('pgscat_cmd','')
      <pre class="text-xs bg-gray-900 text-green-300 rounded p-2 overflow-x-auto">{{cmd}}</pre>
    </div>
    % end
    % end
  </div>
  % end
</div>

<script>
(function () {
  var STAGE_LABELS = {
    not_started:          'Not started',
    catalog_metadata:     'Metadata downloaded',
    raw_harmonized_score: 'Score file downloaded',
    column_map:           'Column map created',
    betamap:              'Betamap ready \u2013 ready for compute',
    aggregated_scores:    'Scores aggregated',
    parquet_export:       'Dashboard ready'
  };
  var STAGE_CLASSES = {
    not_started:          'bg-gray-200 text-gray-600',
    catalog_metadata:     'bg-blue-100 text-blue-700',
    raw_harmonized_score: 'bg-blue-200 text-blue-800',
    column_map:           'bg-yellow-100 text-yellow-700',
    betamap:              'bg-green-100 text-green-700',
    aggregated_scores:    'bg-green-200 text-green-800',
    parquet_export:       'bg-green-600 text-white'
  };

  function stepIcon(status) {
    if (status === 'ok')      return '<span class="text-green-600 mr-1">✓</span>';
    if (status === 'skipped') return '<span class="text-gray-400 mr-1">↷</span>';
    if (status === 'error')   return '<span class="text-red-500 mr-1">✗</span>';
    return '<span class="text-gray-300 mr-1">…</span>';
  }

  function renderSteps(steps) {
    var html = steps.map(function (s) {
      return '<div class="flex items-start gap-1 text-xs">' +
        stepIcon(s.status) +
        '<span class="font-medium text-gray-700 mr-1">' + s.name + '</span>' +
        '<span class="text-gray-500 flex-1">' + (s.message || '') + '</span>' +
        (s.elapsed_s !== undefined ? '<span class="text-gray-400 ml-2 shrink-0">' + s.elapsed_s + 's</span>' : '') +
        '</div>';
    }).join('');
    document.getElementById('prep-steps').innerHTML = html;
  }

  function updateStageBadge(stage) {
    var badge = document.getElementById('stage-badge');
    if (!badge) return;
    var label = STAGE_LABELS[stage] || stage;
    var cls   = STAGE_CLASSES[stage] || 'bg-gray-100 text-gray-600';
    badge.textContent = label;
    badge.className = 'ml-2 px-2 py-0.5 rounded-full text-xs font-medium ' + cls;
  }

  window.startPreparation = function (pgsId) {
    var btn = document.getElementById('btn-prepare');
    var progressDiv = document.getElementById('prep-progress');
    var errorDiv    = document.getElementById('prep-error');

    // Disable button and show spinner text
    btn.disabled = true;
    btn.textContent = 'Preparing…';
    btn.className = btn.className.replace('bg-green-600 hover:bg-green-700', 'bg-gray-400 cursor-not-allowed');
    progressDiv.classList.remove('hidden');
    errorDiv.classList.add('hidden');
    document.getElementById('prep-steps').innerHTML =
      '<div class="text-xs text-gray-400">Starting preparation…</div>';

    fetch('/api/pgs/' + pgsId + '/prepare', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    })
    .then(function (resp) {
      return resp.json().then(function (data) {
        return { ok: resp.ok || resp.status === 207, data: data };
      });
    })
    .then(function (result) {
      var data = result.data;

      if (data.steps && data.steps.length) {
        renderSteps(data.steps);
      }

      if (data.current_stage) {
        updateStageBadge(data.current_stage);
      }

      var nextRow = document.getElementById('next-step-row');
      if (nextRow && data.next_step !== undefined) {
        var txt = document.getElementById('next-step-text');
        if (txt) txt.textContent = data.next_step || '';
        nextRow.classList.toggle('hidden', !data.next_step);
      }

      if (data.status === 'error' || data.error) {
        errorDiv.textContent = data.error || 'Preparation failed. See step log above.';
        errorDiv.classList.remove('hidden');
        btn.disabled = false;
        btn.textContent = 'Retry';
        btn.className = btn.className.replace('bg-gray-400 cursor-not-allowed', 'bg-green-600 hover:bg-green-700');
      } else if (data.ready_for_compute) {
        btn.textContent = 'Ready for compute';
        btn.className = 'px-3 py-1.5 text-xs bg-green-100 text-green-700 rounded font-medium cursor-default';
      } else {
        btn.disabled = false;
        btn.textContent = 'Prepare Score';
        btn.className = btn.className.replace('bg-gray-400 cursor-not-allowed', 'bg-green-600 hover:bg-green-700');
      }
    })
    .catch(function (err) {
      errorDiv.textContent = 'Request failed: ' + err.message;
      errorDiv.classList.remove('hidden');
      btn.disabled = false;
      btn.textContent = 'Retry';
      btn.className = btn.className.replace('bg-gray-400 cursor-not-allowed', 'bg-green-600 hover:bg-green-700');
    });
  };
}());
</script>
% end

<!-- Pipeline plan (shown for any source) -->
% if plan and source not in ("remote_only",):
<div class="bg-white border border-indigo-200 rounded-lg p-5 mb-4">
  <h2 class="font-semibold text-indigo-800 mb-4">
    Processing pipeline plan
    <span class="text-xs font-normal text-gray-400 ml-2">(informational — no jobs will be submitted)</span>
  </h2>
  <div class="space-y-4">
    % for step in plan.get('steps', []):
    <div class="flex gap-4">
      <div class="flex-none w-8 h-8 rounded-full bg-indigo-100 text-indigo-700 flex items-center justify-center font-bold text-sm">
        {{step['step']}}
      </div>
      <div class="flex-1">
        <div class="font-medium text-gray-700">{{step['name']}}</div>
        <div class="text-sm text-gray-500 mt-0.5">{{step['description']}}</div>
        % if step.get('output_pattern'):
        <div class="mt-1 text-xs font-mono text-gray-400">→ {{step['output_pattern']}}</div>
        % end
        % if step.get('command_example'):
        <pre class="mt-1.5 text-xs bg-gray-50 border border-gray-200 rounded p-2 overflow-x-auto text-gray-600">{{step['command_example']}}</pre>
        % end
        % if step.get('submit_example'):
        <pre class="mt-1.5 text-xs bg-indigo-50 border border-indigo-100 rounded p-2 overflow-x-auto text-indigo-700">{{step['submit_example']}}</pre>
        % end
        % if step.get('notes'):
        <div class="mt-1 text-xs text-gray-400 italic">{{step['notes']}}</div>
        % end
      </div>
    </div>
    % end
  </div>
  <div class="mt-4 text-xs text-gray-400 border-t border-gray-100 pt-3">
    Scripts directory: <code>{{plan.get('scripts_dir','')}}</code>
  </div>
</div>
% end

% end
