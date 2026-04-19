%rebase('_base.tpl', title='Local Catalog')

<div class="mb-6 flex flex-col sm:flex-row sm:items-center gap-4">
  <div>
    <h1 class="text-2xl font-bold text-gray-800">Local PGS Catalog</h1>
    <p class="text-sm text-gray-500 mt-1">
      {{len(pgs_list)}} score(s) found in <code class="bg-gray-100 px-1 rounded">{{cfg.DATA_DIR}}</code>
    </p>
  </div>
  <div class="sm:ml-auto flex gap-2">
    <input
      id="localFilter"
      type="text"
      placeholder="Filter by ID or trait…"
      class="border border-gray-300 rounded px-3 py-1.5 text-sm w-64 focus:outline-none focus:ring-2 focus:ring-indigo-400"
      oninput="filterTable(this.value)"
    />
    <a href="/search"
       class="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded hover:bg-indigo-700">
      Search Remote
    </a>
  </div>
</div>

% if not pgs_list:
<div class="text-center py-16 text-gray-400">
  <div class="text-5xl mb-4">📂</div>
  <p class="text-lg">No PGS directories found.</p>
  <p class="text-sm mt-1">Check that <code>APP_DATA_DIR</code> points to the correct scores mount.</p>
</div>
% else:

<!-- Summary stats bar -->
<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
  <%
    n_total   = len(pgs_list)
    n_parquet = sum(1 for p in pgs_list if p.get('has_parquet'))
    n_tsv     = sum(1 for p in pgs_list if p.get('has_tsv') and not p.get('has_parquet'))
    n_nodata  = sum(1 for p in pgs_list if not p.get('has_results'))
  %>
  <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 text-center">
    <div class="text-2xl font-bold text-indigo-600">{{n_total}}</div>
    <div class="text-xs text-gray-500 mt-0.5">Total PGS</div>
  </div>
  <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 text-center">
    <div class="text-2xl font-bold text-green-600">{{n_parquet}}</div>
    <div class="text-xs text-gray-500 mt-0.5">Parquet ready</div>
  </div>
  <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 text-center">
    <div class="text-2xl font-bold text-yellow-600">{{n_tsv}}</div>
    <div class="text-xs text-gray-500 mt-0.5">TSV only</div>
  </div>
  <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 text-center">
    <div class="text-2xl font-bold text-red-400">{{n_nodata}}</div>
    <div class="text-xs text-gray-500 mt-0.5">No results yet</div>
  </div>
</div>

<!-- Table -->
<div class="bg-white rounded-lg border border-gray-200 overflow-hidden shadow-sm">
  <table id="catalogTable" class="w-full text-sm">
    <thead class="bg-gray-50 border-b border-gray-200">
      <tr>
        <th class="text-left px-4 py-3 font-semibold text-gray-600">PGS ID</th>
        <th class="text-left px-4 py-3 font-semibold text-gray-600">Trait</th>
        <th class="text-right px-4 py-3 font-semibold text-gray-600">Variants</th>
        <th class="text-center px-4 py-3 font-semibold text-gray-600">Chrom</th>
        <th class="text-center px-4 py-3 font-semibold text-gray-600">Data</th>
        <th class="text-center px-4 py-3 font-semibold text-gray-600">Actions</th>
      </tr>
    </thead>
    <tbody class="divide-y divide-gray-100">
      % for pgs in pgs_list:
      <tr class="hover:bg-gray-50 catalog-row"
          data-search="{{pgs['pgs_id'].lower()}} {{pgs.get('trait_name','').lower()}}">
        <td class="px-4 py-3 font-mono font-semibold text-indigo-700">
          {{pgs['pgs_id']}}
        </td>
        <td class="px-4 py-3 text-gray-700 max-w-xs truncate" title="{{pgs.get('trait_name','')}}">
          {{pgs.get('trait_name', '—')}}
        </td>
        <td class="px-4 py-3 text-right text-gray-500">
          % nv = pgs.get('n_variants')
          {{'{:,}'.format(nv) if nv else '—'}}
        </td>
        <td class="px-4 py-3 text-center text-gray-500">
          {{pgs.get('n_chromosomes', 0)}} / 22
        </td>
        <td class="px-4 py-3 text-center">
          % if pgs.get('has_parquet'):
          <span class="inline-block px-2 py-0.5 text-xs rounded-full bg-green-100 text-green-700 font-medium">Parquet</span>
          % elif pgs.get('has_tsv'):
          <span class="inline-block px-2 py-0.5 text-xs rounded-full bg-yellow-100 text-yellow-700 font-medium">TSV</span>
          % else:
          <span class="inline-block px-2 py-0.5 text-xs rounded-full bg-gray-100 text-gray-500">None</span>
          % end
        </td>
        <td class="px-4 py-3 text-center">
          <div class="flex justify-center gap-2">
            % if pgs.get('has_results'):
            <a href="/dashboard/{{pgs['pgs_id']}}"
               class="px-2 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">
              Dashboard
            </a>
            % end
            <a href="/pgs/{{pgs['pgs_id']}}/source"
               class="px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded hover:bg-gray-200">
              Details
            </a>
          </div>
        </td>
      </tr>
      % end
    </tbody>
  </table>
</div>

<p id="noFilterResults" class="text-center text-gray-400 py-8 hidden">
  No PGS match your filter.
</p>
% end
