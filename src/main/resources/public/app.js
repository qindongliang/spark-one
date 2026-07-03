const script = document.getElementById('script');
const limit = document.getElementById('limit');
const output = document.getElementById('output');
const statusText = document.getElementById('status');
const summary = document.getElementById('summary');
const compileButton = document.getElementById('compile');
const runButton = document.getElementById('run');
const engineSelect = document.getElementById('engine');
const editor = createEditor(script);
const appConfig = { showCompiledSql: false, previewMaxRows: 10, defaultResultTab: 'schema', defaultEngine: 'local' };
let limitTouched = false;

loadConfig();

compileButton.addEventListener('click', () => submit('/api/compile'));
runButton.addEventListener('click', () => submit('/api/run'));
limit.addEventListener('input', () => {
  limitTouched = true;
});

async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const data = await res.json();
    appConfig.showCompiledSql = Boolean(data.showCompiledSql);
    compileButton.hidden = !appConfig.showCompiledSql;
    appConfig.previewMaxRows = normalizePositiveInteger(data.previewMaxRows, 10);
    appConfig.defaultResultTab = normalizeResultTab(data.defaultResultTab);
    appConfig.defaultEngine = String(data.defaultEngine || 'local');
    renderEngines(Array.isArray(data.engines) ? data.engines : []);
    limit.max = String(appConfig.previewMaxRows);
    if (!limitTouched || !limit.value || Number(limit.value) > appConfig.previewMaxRows) {
      limit.value = String(appConfig.previewMaxRows);
    }
  } catch (err) {
    appConfig.showCompiledSql = false;
    compileButton.hidden = true;
  }
}

async function submit(path) {
  const scope = getScriptScope();
  setBusy(true);
  try {
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        engine: selectedEngine(),
        script: scope.script,
        limit: Number(limit.value || appConfig.previewMaxRows)
      })
    });
    const data = await res.json();
    render(data, path, scope);
  } catch (err) {
    render({ success: false, error: String(err) }, path, scope);
  } finally {
    setBusy(false);
  }
}

function normalizePositiveInteger(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? Math.floor(number) : fallback;
}

function normalizeResultTab(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return normalized === 'preview' ? 'preview' : 'schema';
}

function renderEngines(engines) {
  engineSelect.innerHTML = '';
  const available = engines.length ? engines : [{ id: 'local', label: 'Local' }];
  for (const engine of available) {
    const option = document.createElement('option');
    option.value = engine.id;
    option.textContent = engine.label || engine.id;
    option.selected = engine.id === appConfig.defaultEngine;
    engineSelect.appendChild(option);
  }
  engineSelect.hidden = available.length <= 1;
  const label = engineSelect.closest('label');
  if (label) label.hidden = available.length <= 1;
}

function selectedEngine() {
  return engineSelect && engineSelect.value ? engineSelect.value : appConfig.defaultEngine;
}

function createEditor(textarea) {
  if (!window.CodeMirror) return null;

  registerSparkOneSqlMode();

  return window.CodeMirror.fromTextArea(textarea, {
    mode: 'text/x-sparkone-sql',
    theme: 'idea',
    lineNumbers: true,
    lineWrapping: true,
    indentUnit: 2,
    tabSize: 2,
    smartIndent: true,
    viewportMargin: Infinity
  });
}

function registerSparkOneSqlMode() {
  const sparkSqlMode = window.CodeMirror.resolveMode('text/x-sparksql');
  const keywords = Object.assign({}, sparkSqlMode.keywords || {}, keywordSet('view load save options partitionby mysql doris'));

  window.CodeMirror.defineMIME('text/x-sparkone-sql', Object.assign({}, sparkSqlMode, {
    keywords
  }));
}

function keywordSet(words) {
  return words.split(/\s+/).filter(Boolean).reduce((result, word) => {
    result[word.toLowerCase()] = true;
    return result;
  }, {});
}

function getScriptScope() {
  if (editor) {
    const selection = editor.getSelection().trim();
    if (selection) return { script: selection, label: 'Selection' };
    return { script: editor.getValue(), label: 'Script' };
  }

  const start = script.selectionStart || 0;
  const end = script.selectionEnd || 0;
  const selection = script.value.substring(start, end).trim();
  if (selection) return { script: selection, label: 'Selection' };
  return { script: script.value, label: 'Script' };
}

function setBusy(busy) {
  compileButton.disabled = busy;
  runButton.disabled = busy;
  engineSelect.disabled = busy;
  statusText.textContent = busy ? 'Working' : 'Ready';
}

function render(data, path, scope) {
  output.innerHTML = '';
  const scopeLabel = scope ? ` · ${scope.label}` : '';
  summary.textContent = (data.success ? 'Success' : 'Failed') + scopeLabel;

  if (!data.success && data.error) {
    output.appendChild(errorBlock(data.error));
    return;
  }

  if (path === '/api/compile') {
    for (const statement of data.statements || []) {
      output.appendChild(section(`Statement ${statement.index}`, pre(statement.sql + ';')));
    }
    return;
  }

  const showCompiledSql = Object.prototype.hasOwnProperty.call(data, 'showCompiledSql')
    ? Boolean(data.showCompiledSql)
    : appConfig.showCompiledSql;

  for (const statement of data.statements || []) {
    const parts = [];
    if (showCompiledSql) parts.push(compiledSqlBlock(statement.sql + ';'));
    if (statement.error) parts.push(errorBlock(statement.error));
    if (statement.schema && statement.schema.length) {
      parts.push(resultTabs(statement));
    }
    if (!parts.length) parts.push(emptyBlock('Statement executed with no result rows.'));
    output.appendChild(section(`Statement ${statement.index} · ${statement.durationMs} ms`, parts));
  }
}

function section(title, content) {
  const node = document.createElement('div');
  node.className = 'section';

  const h = document.createElement('h2');
  h.textContent = title;
  node.appendChild(h);

  if (Array.isArray(content)) content.forEach(item => node.appendChild(item));
  else node.appendChild(content);

  return node;
}

function pre(text) {
  const node = document.createElement('pre');
  node.textContent = text;
  return node;
}

function compiledSqlBlock(sql) {
  const node = document.createElement('div');
  node.className = 'compiled-sql';

  const label = document.createElement('div');
  label.className = 'block-label';
  label.textContent = 'Compiled SQL';
  node.appendChild(label);
  node.appendChild(pre(sql));

  return node;
}

function emptyBlock(text) {
  const node = document.createElement('div');
  node.className = 'empty';
  node.textContent = text;
  return node;
}

function errorBlock(text) {
  const node = document.createElement('div');
  node.className = 'error';
  node.textContent = text;
  return node;
}

function resultTabs(statement) {
  const schema = statement.schema || [];
  const rows = statement.rows || [];
  const showPreviewFirst = appConfig.defaultResultTab === 'preview';
  const node = document.createElement('div');
  node.className = 'result-tabs';
  const tabs = document.createElement('div');
  tabs.className = 'tabs';
  const schemaButton = tabButton('Schema', !showPreviewFirst);
  const previewButton = tabButton('Preview', showPreviewFirst);
  const schemaPane = resultPane(schemaTable(schema));
  const previewPane = resultPane(previewContent(statement));
  schemaPane.hidden = showPreviewFirst;
  previewPane.hidden = !showPreviewFirst;

  schemaButton.addEventListener('click', () => showTab(schemaButton, schemaPane, previewButton, previewPane));
  previewButton.addEventListener('click', () => {
    showTab(previewButton, previewPane, schemaButton, schemaPane);
    if (statement.previewTable && !previewPane.dataset.loaded && !previewPane.dataset.loading) {
      loadPreview(statement.previewTable, previewPane);
    }
  });
  tabs.appendChild(schemaButton);
  tabs.appendChild(previewButton);
  node.appendChild(tabs);
  node.appendChild(schemaPane);
  node.appendChild(previewPane);
  if (showPreviewFirst && statement.previewTable && !previewPane.dataset.loaded && !previewPane.dataset.loading) {
    loadPreview(statement.previewTable, previewPane);
  }
  return node;
}

function resultPane(content) {
  const pane = document.createElement('div');
  pane.className = 'result-pane';
  pane.appendChild(content);
  return pane;
}

function previewContent(statement) {
  const rows = statement.rows || [];
  if (rows.length) return dataTable(statement.schema || [], rows);
  if (!statement.previewTable) return emptyBlock('No preview rows.');

  return emptyBlock('Click Preview to load rows.');
}

async function loadPreview(table, pane) {
  pane.dataset.loading = 'true';
  pane.textContent = 'Loading preview...';
  statusText.textContent = 'Working';
  try {
    const res = await fetch('/api/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        table,
        engine: selectedEngine(),
        limit: Number(limit.value || appConfig.previewMaxRows)
      })
    });
    const data = await res.json();
    pane.innerHTML = '';
    if (!data.success) {
      pane.appendChild(errorBlock(data.error || 'Preview failed.'));
      return;
    }
    const statement = data.statement || {};
    const rows = statement.rows || [];
    pane.appendChild(rows.length ? dataTable(statement.schema || [], rows) : emptyBlock('No preview rows.'));
    pane.dataset.loaded = 'true';
  } catch (err) {
    pane.innerHTML = '';
    pane.appendChild(errorBlock(String(err)));
  } finally {
    delete pane.dataset.loading;
    statusText.textContent = 'Ready';
  }
}

function tabButton(text, active) {
  const button = document.createElement('button');
  button.className = active ? 'tab active' : 'tab';
  button.textContent = text;
  return button;
}

function showTab(activeButton, activePane, otherButton, otherPane) {
  activeButton.classList.add('active');
  otherButton.classList.remove('active');
  activePane.hidden = false;
  otherPane.hidden = true;
}

function schemaTable(schema) {
  return simpleTable(['name', 'type', 'nullable'], schema.map(field => [
    field.name,
    field.dataType,
    String(field.nullable)
  ]));
}

function dataTable(schema, rows) {
  const node = document.createElement('table');
  const head = document.createElement('thead');
  const headRow = document.createElement('tr');

  schema.forEach(field => {
    const th = document.createElement('th');
    th.textContent = `${field.name} (${field.dataType})`;
    headRow.appendChild(th);
  });

  head.appendChild(headRow);
  node.appendChild(head);

  const body = document.createElement('tbody');
  rows.forEach(row => {
    const tr = document.createElement('tr');
    row.forEach(value => {
      const td = document.createElement('td');
      td.textContent = value === null ? 'NULL' : value;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
  node.appendChild(body);

  return node;
}

function simpleTable(headers, rows) {
  const node = document.createElement('table');
  const head = document.createElement('thead');
  const headRow = document.createElement('tr');
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headRow.appendChild(th);
  });
  head.appendChild(headRow);
  node.appendChild(head);

  const body = document.createElement('tbody');
  rows.forEach(row => {
    const tr = document.createElement('tr');
    row.forEach(value => {
      const td = document.createElement('td');
      td.textContent = value === null ? 'NULL' : value;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
  node.appendChild(body);
  return node;
}
