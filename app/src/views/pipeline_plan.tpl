%rebase('_base.tpl', title=pgs_id + ' · Pipeline Plan')

<!-- Breadcrumb -->
<nav class="text-sm text-gray-400 mb-4">
  <a href="/" class="hover:text-indigo-600">Catalog</a>
  <span class="mx-2">›</span>
  % if local_exists:
  <a href="/dashboard/{{pgs_id}}" class="hover:text-indigo-600">{{pgs_id}}</a>
  % else:
  <a href="/pgs/{{pgs_id}}/source" class="hover:text-indigo-600">{{pgs_id}}</a>
  % end
  <span class="mx-2">›</span>
  <span class="text-gray-500">Pipeline Plan</span>
</nav>

<!-- Header -->
<div class="flex flex-col sm:flex-row sm:items-start gap-4 mb-6">
  <div class="flex-1">
    <h1 class="text-2xl font-bold text-gray-800">Pipeline Plan — {{pgs_id}}</h1>
    <%
      flow_labels = {
        "local_existing":         ("Local — already computed",    "bg-green-100 text-green-700"),
        "catalog_remote_prepare": ("Remote → prepare for pipeline", "bg-yellow-100 text-yellow-700"),
        "custom_prs":             ("Custom PRS",                   "bg-purple-100 text-purple-700"),
      }
      fl, fc = flow_labels.get(plan.get('flow_mode',''), ("Unknown", "bg-gray-100 text-gray-500"))
    %>
    <span class="inline-block mt-1 px-3 py-0.5 text-sm rounded-full font-medium {{fc}}">{{fl}}</span>
    <p class="text-sm text-gray-500 mt-2">{{plan.get('description','')}}</p>
  </div>
  <div class="flex gap-2 flex-wrap">
    % if local_exists:
    <a href="/dashboard/{{pgs_id}}"
       class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-700">
      Open Dashboard
    </a>
    % end
    <a href="/api/pipeline/{{pgs_id}}/plan" target="_blank"
       class="px-3 py-1.5 text-sm bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
      JSON ↗
    </a>
    <a href="/api/pipeline/{{pgs_id}}/manifest" target="_blank"
       class="px-3 py-1.5 text-sm bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
      Manifest JSON ↗
    </a>
  </div>
</div>

<!-- Remote info banner (if available) -->
% if remote_info:
<div class="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6 grid sm:grid-cols-4 gap-3 text-sm">
  <div>
    <div class="text-xs text-gray-400 mb-0.5">Trait</div>
    <div class="text-gray-700 font-medium">{{remote_info.get('trait_reported','—')}}</div>
  </div>
  <div>
    <div class="text-xs text-gray-400 mb-0.5">Variants</div>
    <div class="text-gray-700 font-medium">
      % nv = remote_info.get('variants_number')
      {{'{:,}'.format(nv) if nv else '—'}}
    </div>
  </div>
  <div>
    <div class="text-xs text-gray-400 mb-0.5">Harmonised (GRCh38)</div>
    <div>
      % if remote_info.get('is_harmonized'):
      <span class="text-green-600 font-medium">Yes</span>
      % else:
      <span class="text-yellow-600">Not confirmed</span>
      % end
    </div>
  </div>
  % pub = remote_info.get('publication') or {}
  % if pub.get('pmid'):
  <div>
    <div class="text-xs text-gray-400 mb-0.5">PMID</div>
    <div class="text-gray-700">{{pub.get('pmid','')}}</div>
  </div>
  % end
</div>
% end

<!-- Local layout contract -->
<div class="bg-white border border-gray-200 rounded-lg p-5 mb-6">
  <h2 class="font-semibold text-gray-700 mb-3">
    Expected local file layout
    <span class="text-xs font-normal text-gray-400 ml-2">
      — see docs/ARCHITECTURE.md for full file layout contract
    </span>
  </h2>
  <div class="overflow-x-auto">
    <table class="w-full text-xs">
      <thead>
        <tr class="text-gray-400 border-b border-gray-100">
          <th class="text-left pb-1 font-medium pr-4">File</th>
          <th class="text-left pb-1 font-medium pr-4">Role</th>
          <th class="text-center pb-1 font-medium pr-4">Required</th>
          <th class="text-center pb-1 font-medium pr-4">Exists</th>
          <th class="text-left pb-1 font-medium">Produced by</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-gray-50">
        % for f in manifest.get('files', []):
        <tr class="hover:bg-gray-50">
          <td class="py-1 pr-4 font-mono text-gray-600">{{f.get('filename','')}}</td>
          <td class="py-1 pr-4 text-gray-500">{{f.get('role','').replace('_',' ')}}</td>
          <td class="py-1 pr-4 text-center">
            % if f.get('required'):
            <span class="text-red-500 font-bold">●</span>
            % else:
            <span class="text-gray-300">○</span>
            % end
          </td>
          <td class="py-1 pr-4 text-center">
            % if f.get('exists'):
            <span class="text-green-600">✓</span>
            % else:
            <span class="text-gray-300">—</span>
            % end
          </td>
          <td class="py-1 text-gray-400">{{f.get('produced_by','')}}</td>
        </tr>
        % end
      </tbody>
    </table>
  </div>
  <div class="mt-3 text-xs text-gray-400">
    Betamap columns: <code class="bg-gray-100 px-1 rounded">{{', '.join(manifest.get('betamap_columns',[]))}}</code>
  </div>
</div>

<!-- Processing steps -->
<div class="bg-white border border-indigo-200 rounded-lg p-5 mb-6">
  <h2 class="font-semibold text-indigo-800 mb-4">
    Processing steps
    <span class="text-xs font-normal text-gray-400 ml-2">(informational — no jobs submitted)</span>
  </h2>
  <div class="space-y-5">
    % for step in plan.get('steps', []):
    <%
      status = step.get('status', 'pending')
      status_cls = {
        'done':    'bg-green-100 text-green-700 border-green-200',
        'pending': 'bg-yellow-50 text-yellow-700 border-yellow-200',
      }.get(status, 'bg-gray-50 text-gray-600 border-gray-200')
      num_cls = {
        'done':    'bg-green-100 text-green-700',
        'pending': 'bg-indigo-100 text-indigo-700',
      }.get(status, 'bg-gray-100 text-gray-500')
    %>
    <div class="flex gap-4 p-3 rounded-lg border {{status_cls}}">
      <div class="flex-none w-9 h-9 rounded-full {{num_cls}} flex items-center justify-center font-bold text-sm shrink-0">
        % if status == 'done':
        ✓
        % else:
        {{step['step']}}
        % end
      </div>
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-2 flex-wrap">
          <span class="font-medium text-gray-800">{{step['name']}}</span>
          % auto = step.get('automation','')
          % if auto:
          <span class="text-xs px-2 py-0.5 rounded-full bg-white border border-gray-200 text-gray-500">{{auto}}</span>
          % end
        </div>
        <div class="text-sm text-gray-600 mt-1">{{step['description']}}</div>
        % if step.get('output_pattern'):
        <div class="mt-1.5 text-xs font-mono text-gray-400">→ {{step['output_pattern']}}</div>
        % end
        % if step.get('command_example'):
        <pre class="mt-2 text-xs bg-gray-900 text-green-300 rounded p-2 overflow-x-auto">{{step['command_example']}}</pre>
        % end
        % if step.get('submit_example'):
        <pre class="mt-2 text-xs bg-indigo-900 text-indigo-200 rounded p-2 overflow-x-auto">{{step['submit_example']}}</pre>
        % end
        % if step.get('pgscat_cmd'):
        <pre class="mt-2 text-xs bg-gray-900 text-green-300 rounded p-2 overflow-x-auto">{{step['pgscat_cmd']}}</pre>
        % end
        % if step.get('notes'):
        <div class="mt-1.5 text-xs text-gray-400 italic">{{step['notes']}}</div>
        % end
        % if step.get('slurm_params'):
        <%
          sp = step['slurm_params']
          slurm_str = ', '.join(f'{k}: {v}' for k,v in sp.items())
        %>
        <div class="mt-1.5 text-xs text-gray-400">Slurm: <span class="font-mono">{{slurm_str}}</span></div>
        % end
      </div>
    </div>
    % end
  </div>
</div>

<!-- Scripts directory -->
<div class="bg-white border border-gray-200 rounded-lg p-4 mb-4">
  <h2 class="font-semibold text-gray-700 mb-3 text-sm">Pipeline scripts</h2>
  <div class="text-xs text-gray-500 mb-2">
    Directory: <code class="bg-gray-100 px-1 rounded">{{plan.get('scripts_dir','')}}</code>
  </div>
  <div class="grid sm:grid-cols-3 gap-2">
    % for sc in plan.get('scripts_available', []):
    <div class="flex items-center gap-2 text-xs">
      % if sc.get('exists'):
      <span class="text-green-500">✓</span>
      <span class="font-mono text-gray-600">{{sc['name']}}</span>
      <span class="text-gray-400">({{sc.get('size_kb','?')}} KB)</span>
      % else:
      <span class="text-red-400">✗</span>
      <span class="font-mono text-gray-400 line-through">{{sc['name']}}</span>
      % end
    </div>
    % end
  </div>
</div>

<!-- Zarr paths -->
<div class="bg-gray-50 border border-gray-200 rounded p-3 text-xs text-gray-500">
  <span class="font-medium text-gray-600">Zarr genotype data:</span>
  <code class="ml-2 bg-white border border-gray-200 px-1 rounded">{{plan.get('zarr_base','')}}</code>
  <span class="ml-4 font-medium text-gray-600">Index:</span>
  <code class="ml-2 bg-white border border-gray-200 px-1 rounded">{{plan.get('duckdb_index','')}}</code>
</div>
