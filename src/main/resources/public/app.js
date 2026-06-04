const script = document.getElementById('script');
const limit = document.getElementById('limit');
const output = document.getElementById('output');
const statusText = document.getElementById('status');
const summary = document.getElementById('summary');
const compileButton = document.getElementById('compile');
const runButton = document.getElementById('run');

compileButton.addEventListener('click', () => submit('/api/compile'));
runButton.addEventListener('click', () => submit('/api/run'));

async function submit(path) {
  setBusy(true);
  try {
    const res = await fetch(path, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ script: script.value, limit: Number(limit.value || 200) })
    });
    const data = await res.json();
    render(data, path);
  } catch (err) {
    render({ success: false, error: String(err) }, path);
  } finally {
    setBusy(false);
  }
}

function setBusy(busy) {
  compileButton.disabled = busy;
  runButton.disabled = busy;
  statusText.textContent = busy ? 'Working' : 'Ready';
}

function render(data, path) {
  output.innerHTML = '';
  summary.textContent = data.success ? 'Success' : 'Failed';

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
    const parts = [pre(statement.sql + ';')];
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
