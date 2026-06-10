const script = document.getElementById('script');
const limit = document.getElementById('limit');
const output = document.getElementById('output');
const statusText = document.getElementById('status');
const summary = document.getElementById('summary');
const compileButton = document.getElementById('compile');
const runButton = document.getElementById('run');
const editor = createEditor(script);
const appConfig = { showCompiledSql: false };

loadConfig();

compileButton.addEventListener('click', () => submit('/api/compile'));
runButton.addEventListener('click', () => submit('/api/run'));

async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const data = await res.json();
    appConfig.showCompiledSql = Boolean(data.showCompiledSql);
  } catch (err) {
    appConfig.showCompiledSql = false;
  }
}

async function submit(path) {
  const scope = getScriptScope();
  setBusy(true);
  try {
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ script: scope.script, limit: Number(limit.value || 200) })
    });
    const data = await res.json();
    render(data, path, scope);
  } catch (err) {
    render({ success: false, error: String(err) }, path, scope);
  } finally {
    setBusy(false);
  }
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
  const keywords = Object.assign({}, sparkSqlMode.keywords || {}, keywordSet('view load save'));

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

  for (const statement of data.statements || []) {
    const parts = [];
    if (appConfig.showCompiledSql) parts.push(pre(statement.sql + ';'));
    if (statement.error) parts.push(errorBlock(statement.error));
    if (statement.schema && statement.schema.length) {
      parts.push(table(statement.schema, statement.rows || []));
    }
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

function errorBlock(text) {
  const node = document.createElement('div');
  node.className = 'error';
  node.textContent = text;
  return node;
}

function table(schema, rows) {
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
