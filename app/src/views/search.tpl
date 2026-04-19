%rebase('_base.tpl', title='Remote Search')

<div class="mb-6">
  <h1 class="text-2xl font-bold text-gray-800">Search PGS Catalog</h1>
  <p class="text-sm text-gray-500 mt-1">
    Query the remote
    <a href="https://www.pgscatalog.org" target="_blank" class="text-indigo-600 hover:underline">PGS Catalog</a>
    % if not remote_enabled:
    <span class="ml-2 px-2 py-0.5 bg-red-100 text-red-600 text-xs rounded">Remote disabled</span>
    % end
  </p>
</div>

<!-- Search form -->
<div class="bg-white border border-gray-200 rounded-lg p-6 mb-6 shadow-sm">
  <form method="POST" action="/search" class="flex flex-col sm:flex-row gap-3">
    <select name="search_type"
            class="border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 bg-white">
      <option value="id"    {{"selected" if search_type=="id" else ""}}>PGS ID</option>
      <option value="trait" {{"selected" if search_type=="trait" else ""}}>EFO Trait ID</option>
      <option value="pmid"  {{"selected" if search_type=="pmid" else ""}}>PubMed ID</option>
      <option value="text"  {{"selected" if search_type=="text" else ""}}>Free text / EFO</option>
    </select>
    <input
      name="query"
      type="text"
      value="{{query}}"
      placeholder="e.g. PGS000004 / EFO_0001360 / 25855707"
      class="flex-1 border border-gray-300 rounded px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400"
      {{"disabled" if not remote_enabled else ""}}
    />
    <button type="submit"
            class="px-4 py-2 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700 disabled:opacity-50"
            {{"disabled" if not remote_enabled else ""}}>
      Search
    </button>
  </form>
  <div class="mt-3 text-xs text-gray-400 space-y-0.5">
    <p>• <strong>PGS ID</strong>: exact lookup, e.g. <code>PGS000004</code></p>
    <p>• <strong>EFO Trait ID</strong>: ontology term, e.g. <code>EFO_0001360</code> (type 2 diabetes)</p>
    <p>• <strong>PubMed ID</strong>: numeric PMID, e.g. <code>25855707</code></p>
    <p>• <strong>Free text</strong>: tries EFO term — works best with EFO IDs</p>
  </div>
</div>

<!-- Error -->
% if error:
<div class="bg-red-50 border border-red-200 rounded p-4 mb-4 text-red-700 text-sm">
  {{error}}
</div>
% end

<!-- Results -->
% if results is not None:
  % if results:
  <div class="bg-white border border-gray-200 rounded-lg shadow-sm overflow-hidden">
    <div class="px-4 py-3 bg-gray-50 border-b border-gray-200 text-sm text-gray-500">
      {{len(results)}} result(s) for <strong>{{query}}</strong>
    </div>
    <table class="w-full text-sm">
      <thead class="border-b border-gray-200">
        <tr>
          <th class="text-left px-4 py-3 font-semibold text-gray-600">PGS ID</th>
          <th class="text-left px-4 py-3 font-semibold text-gray-600">Name / Trait</th>
          <th class="text-right px-4 py-3 font-semibold text-gray-600">Variants</th>
          <th class="text-center px-4 py-3 font-semibold text-gray-600">Local</th>
          <th class="text-center px-4 py-3 font-semibold text-gray-600">Actions</th>
        </tr>
      </thead>
      <tbody class="divide-y divide-gray-100">
        % for r in results:
        % pid = r.get('pgs_id', '')
        <tr class="hover:bg-gray-50">
          <td class="px-4 py-3 font-mono font-semibold text-indigo-700">
            {{pid or '—'}}
          </td>
          <td class="px-4 py-3 max-w-sm">
            <div class="text-gray-700 truncate" title="{{r.get('name','')}}">
              {{r.get('name') or r.get('trait_reported') or '—'}}
            </div>
            <div class="text-xs text-gray-400 truncate">{{r.get('trait_reported','')}}</div>
          </td>
          <td class="px-4 py-3 text-right text-gray-500">
            % nv = r.get('variants_number')
            {{'{:,}'.format(nv) if nv else '—'}}
          </td>
          <td class="px-4 py-3 text-center">
            % if r.get('has_results'):
            <span class="px-2 py-0.5 text-xs rounded-full bg-green-100 text-green-700">✓ Results</span>
            % elif r.get('exists_locally'):
            <span class="px-2 py-0.5 text-xs rounded-full bg-yellow-100 text-yellow-700">Dir only</span>
            % else:
            <span class="px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-400">Remote only</span>
            % end
          </td>
          <td class="px-4 py-3 text-center">
            <div class="flex justify-center gap-1.5">
              % if pid:
              % if r.get('has_results'):
              <a href="/dashboard/{{pid}}"
                 class="px-2 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">
                Dashboard
              </a>
              % end
              <a href="/pgs/{{pid}}/source"
                 class="px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
                Details
              </a>
              % end
            </div>
          </td>
        </tr>
        % end
      </tbody>
    </table>
  </div>
  % else:
  <div class="text-center py-12 text-gray-400">
    <div class="text-4xl mb-3">🔍</div>
    <p>No results found for <strong class="text-gray-600">{{query}}</strong>.</p>
    <p class="text-sm mt-1">Try a different search type or term.</p>
  </div>
  % end
% end
