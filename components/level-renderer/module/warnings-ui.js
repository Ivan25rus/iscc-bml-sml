'use strict';
const { READ_WARNINGS, WARNINGS_CHANGED } = require('./warnings-ipc');

const points = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 8 });
const time = new Intl.DateTimeFormat('ru-RU', { dateStyle: 'short', timeStyle: 'short' });
const styles = `
  .iscc-level-warnings-button { color:var(--muted,#a1a1aa); background:var(--panel,#121214);
    border:1px solid var(--border,#3f3f46); border-radius:6px; padding:5px 8px; cursor:pointer; font:inherit; }
  .iscc-level-warnings-button:hover { color:var(--text,#e5e7eb); }
  .iscc-level-warnings-box { position:fixed; z-index:6; right:12px; width:max-content;
    max-width:calc(100vw - 24px); box-sizing:border-box; border:1px solid var(--border,#3f3f46);
    border-radius:8px; background:var(--panel,#121214); color:var(--text,#e5e7eb); padding:9px 11px;
    font-size:12px; box-shadow:0 6px 20px #0005; }
  #iscc-level-warning-toast { bottom:56px; border-left:2px solid #b78a4a; }
  #iscc-level-warning-history { top:48px; max-height:min(50vh,360px); overflow:auto; }
  .iscc-level-warnings-heading { display:flex; gap:12px; align-items:center; justify-content:space-between; }
  .iscc-level-warnings-list { list-style:none; margin:6px 0 0; padding:0; }
  .iscc-level-warnings-list li { padding:6px 0; }
  .iscc-level-warnings-list li + li { border-top:1px solid var(--border,#3f3f46); }
  .iscc-level-warnings-time { color:var(--muted,#a1a1aa); margin-left:8px; }
`;

function warningText(row) {
  const difference = points.format(Math.abs(row.mappedPrice - row.referencePrice));
  return `${row.symbol} · ТВ ${points.format(row.referencePrice)} → MT5 ${points.format(row.mappedPrice)} · Δ ${difference} п.`;
}

function mountWarnings({ document, ipcRenderer, clock = Date.now, setTimer = setTimeout, clearTimer = clearTimeout }) {
  const toolbar = document.getElementById('settings-btn')?.parentElement;
  if (!toolbar) return;
  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  }
  function button(label) {
    const node = element('button', 'iscc-level-warnings-button', label);
    node.type = 'button';
    return node;
  }
  const style = element('style', '', styles);
  document.head.appendChild(style);

  const toggle = button('Уведомления уровней');
  toggle.hidden = true;
  toggle.setAttribute('aria-controls', 'iscc-level-warning-history');
  toggle.setAttribute('aria-expanded', 'false');
  toolbar.appendChild(toggle);
  const history = element('section', 'iscc-level-warnings-box');
  history.id = 'iscc-level-warning-history';
  history.hidden = true;
  history.setAttribute('aria-label', 'Уведомления уровней');
  const heading = element('div', 'iscc-level-warnings-heading');
  const close = button('Закрыть');
  heading.append(element('strong', '', 'Уведомления уровней'), close);
  const list = element('ul', 'iscc-level-warnings-list');
  const empty = element('div', '', 'Пока нет уведомлений');
  history.append(heading, list, empty);
  document.body.appendChild(history);

  const toast = element('aside', 'iscc-level-warnings-box');
  toast.id = 'iscc-level-warning-toast';
  toast.hidden = true;
  const toastHeading = element('div', 'iscc-level-warnings-heading');
  const hint = element('strong', '', 'Проверьте и при необходимости перерисуйте уровень в TradingView');
  const dismiss = button('×');
  dismiss.setAttribute('aria-label', 'Скрыть предупреждение');
  toastHeading.append(hint, dismiss);
  const details = element('div');
  details.setAttribute('role', 'status');
  toast.append(toastHeading, details);
  document.body.appendChild(toast);

  const dismissed = new Set();
  let active = [], current, received = false, expiry;
  function showHistory(show) {
    history.hidden = !show;
    toggle.setAttribute('aria-expanded', String(show));
  }
  function showWarning() {
    clearTimer(expiry);
    const now = clock();
    active = active.filter(row => row.expiresAt > now);
    current = active.findLast(row => !dismissed.has(row.id));
    toast.hidden = !current;
    details.textContent = current ? warningText(current) : '';
    if (current) expiry = setTimer(showWarning, current.expiresAt - now);
  }
  function render(state) {
    if (!state) return;
    toggle.hidden = !state.enabled;
    if (!state.enabled) showHistory(false);
    active = state.active;
    const ids = new Set(active.map(row => row.id));
    for (const id of dismissed) if (!ids.has(id)) dismissed.delete(id);
    showWarning();
    list.replaceChildren(...state.history.map(row => {
      const item = element('li', '', warningText(row));
      const when = element('time', 'iscc-level-warnings-time', time.format(row.at));
      when.dateTime = new Date(row.at).toISOString();
      item.appendChild(when);
      return item;
    }));
    empty.hidden = state.history.length > 0;
  }
  toggle.addEventListener('click', () => showHistory(history.hidden));
  close.addEventListener('click', () => { showHistory(false); toggle.focus(); });
  dismiss.addEventListener('click', () => { if (current) dismissed.add(current.id); showWarning(); });
  ipcRenderer.on(WARNINGS_CHANGED, (_event, state) => { received = true; render(state); });
  ipcRenderer.invoke(READ_WARNINGS).then(state => { if (!received) render(state); }).catch(() => {});
}

module.exports = { mountWarnings };
