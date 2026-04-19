%rebase('_base.tpl', title=f'Error {code}')
<div class="max-w-xl mx-auto mt-16 text-center">
  <div class="text-6xl font-bold text-indigo-200 mb-4">{{code}}</div>
  <h1 class="text-2xl font-semibold text-gray-700 mb-2">
    % if code == 404:
    Not Found
    % elif code == 400:
    Bad Request
    % else:
    Internal Error
    % end
  </h1>
  <p class="text-gray-500 mb-6">{{message}}</p>
  <a href="/" class="inline-block px-4 py-2 bg-indigo-600 text-white rounded hover:bg-indigo-700">
    ← Back to Catalog
  </a>
</div>
