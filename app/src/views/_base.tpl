<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{{title}} — PGS Platform</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.plot.ly/plotly-2.29.1.min.js" charset="utf-8"></script>
  <link rel="stylesheet" href="/static/app.css" />
</head>
<body class="bg-gray-50 min-h-full flex flex-col text-gray-800">

  <!-- Nav -->
  <nav class="bg-indigo-700 text-white shadow">
    <div class="max-w-7xl mx-auto px-4 py-3 flex items-center gap-1 flex-wrap">
      <a href="/" class="font-bold text-lg tracking-tight hover:text-indigo-200 mr-4">
        🧬 PGS Platform
      </a>
      <!-- Mode A -->
      <a href="/"
         class="text-sm px-3 py-1 rounded hover:bg-indigo-600 transition-colors"
         title="View locally computed PGS/PRS">
        📂 Local Catalog
      </a>
      <!-- Mode B -->
      <a href="/search"
         class="text-sm px-3 py-1 rounded hover:bg-indigo-600 transition-colors"
         title="Search PGS Catalog and prepare remote scores">
        🔍 Remote Search
      </a>
      <!-- Mode C -->
      <a href="/custom-prs/new"
         class="text-sm px-3 py-1 rounded hover:bg-indigo-600 transition-colors"
         title="Plan a custom PRS analysis">
        ⚙ Custom PRS
      </a>
      <!-- Module D: Variant Annotator -->
      <a href="/"
         class="text-sm px-3 py-1 rounded hover:bg-indigo-600 transition-colors"
         title="Annotate PRS variants with GENCODE GFF3"
         id="nav-annotator">
        🔬 Variant Annotator
      </a>
      <!-- Module E: Gene Browser -->
      <a href="/genes"
         class="text-sm px-3 py-1 rounded hover:bg-indigo-600 transition-colors"
         title="Gene-centric interpretation browser (Plotly genomic tracks)">
        🧩 Gene Browser
      </a>
      <span class="ml-auto text-xs text-indigo-300">MCPS · Internal Use</span>
    </div>
  </nav>

  <!-- Main content -->
  <main class="max-w-7xl mx-auto w-full px-4 py-8 flex-1">
    {{!base}}
  </main>

  <footer class="text-center text-xs text-gray-400 py-4 border-t border-gray-200">
    PGS Platform · Data: {{cfg.DATA_DIR}} · Scripts: {{cfg.SCRIPTS_DIR}} · Remote: {{"enabled" if cfg.REMOTE_ENABLED else "disabled"}}
  </footer>

  <script src="/static/app.js"></script>
</body>
</html>
