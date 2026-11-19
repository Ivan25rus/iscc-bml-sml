'use strict';
const { snapshotCandlePage, candleWindowReader, readCandleHistory } = require('./page');
const { createHash } = require('crypto');

function localDebugger(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'ws:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
      && url.port === '9222' && !url.username && !url.password;
  } catch { return false; }
}
function chart(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && ['tradingview.com', 'www.tradingview.com', 'ru.tradingview.com'].includes(url.hostname)
      && url.pathname.startsWith('/chart/') && !url.username && !url.password;
  } catch { return false; }
}
function evaluate(url, expression, { signal, timeoutMs = 20000 } = {}) {
  if (!localDebugger(url)) return Promise.reject(Error('Разрешён только локальный отладчик TradingView'));
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(Error('Чтение остановлено')); return; }
    const WebSocket = require('ws');
    const socket = new WebSocket(url, { maxPayload: 2 * 1024 * 1024 });
    let done = false;
    const abort = () => finish(Error('Чтение остановлено'));
    const timer = setTimeout(() => finish(Error('TradingView не ответил')), timeoutMs);
    function finish(error, result) {
      if (done) return;
      done = true; clearTimeout(timer); signal?.removeEventListener('abort', abort);
      error ? socket.terminate() : socket.close();
      error ? reject(error) : resolve(result);
    }
    signal?.addEventListener('abort', abort, { once: true });
    socket.on('error', () => finish(Error('Нет связи с локальным TradingView')));
    socket.on('close', () => finish(Error('Соединение с TradingView закрыто')));
    socket.on('open', () => socket.send(JSON.stringify({ id: 1, method: 'Runtime.evaluate',
      params: { expression, awaitPromise: true, returnByValue: true } })));
    socket.on('message', bytes => {
      let value;
      try { value = JSON.parse(String(bytes)); } catch { finish(Error('Некорректный ответ TradingView')); return; }
      if (value.id !== 1) return;
      if (value.error || value.result?.exceptionDetails) finish(Error('Не удалось прочитать разметку TradingView'));
      else finish(null, value.result?.result?.value);
    });
  });
}
async function targets(signal) {
  const response = await require('node-fetch')('http://127.0.0.1:9222/json/list',
    { timeout: 1800, size: 1024 * 1024, redirect: 'error', signal });
  if (!response.ok) throw Error('Не удалось получить вкладки TradingView');
  const value = await response.json();
  if (!Array.isArray(value)) throw Error('Некорректный список вкладок TradingView');
  const charts=value.filter(row=>chart(row.url));
  if(charts.length>16)throw Error('Слишком много открытых графиков для полной сверки');
  return charts;
}
function startReader({ enabled, acceptSnapshot, acceptInventory = () => {}, acceptOpenCharts = () => {},
  sourceLayouts = () => ({}), sources, anchors, onError = () => {}, getTargets = targets, run = evaluate,
  intervalMs = 10000, clock = Date.now, setTimer = setTimeout, clearTimer = clearTimeout }) {
  let closed = false, running = false, timer, controller, refreshPending = false, historyCursor = 0;
  let fullReadAt = 0, pinnedCharts = [];
  const pinnedReadMs = 2000;
  const active = () => !closed && enabled();
  const layoutOf = target => target.url?.match(/\/chart\/([A-Za-z0-9_-]+)(?:\/|[?#]|$)/)?.[1];
  const seen = new Map(), fastSeen = new Map(), installed = new Set(), inventories = new Map();
  const collector = snapshotCandlePage.toString(), calendar = candleWindowReader.toString();
  const version = JSON.stringify(createHash('sha256').update(collector).update(calendar).digest('hex'));
  function schedule(delay, full = true) {
    clearTimer(timer);
    if (!closed) {
      timer = setTimer(() => tick(full), delay);
      timer.unref?.();
    }
  }
  function forget(id, full) {
    fastSeen.delete(id);
    pinnedCharts = pinnedCharts.filter(target => target.id !== id);
    if (full) {
      seen.delete(id);
      installed.delete(id);
      inventories.delete(id);
    }
  }
  function reset() {
    pinnedCharts = [];
    seen.clear();
    fastSeen.clear();
    installed.clear();
    inventories.clear();
  }
  async function collect(target, options, full) {
    const scopeCheck = options.scope ? `const here = window.location;
      if (here.protocol !== 'https:' || !['tradingview.com','www.tradingview.com','ru.tradingview.com'].includes(here.hostname)
        || !here.pathname.startsWith('/chart/') || here.pathname.split('/')[2] !== ${JSON.stringify(options.scope)})
        return { moved: true };` : '';
    const read = `return runtime.collect(runtime.calendar, ${JSON.stringify(options)});`;
    const args = { signal: controller.signal, ...(!full ? { timeoutMs: 1000 } : {}) };
    if (installed.has(target.id)) {
      const result = await run(target.webSocketDebuggerUrl, `(() => {
        ${scopeCheck}
        const runtime = window.__isccAddonCandleRuntime;
        if (runtime?.version !== ${version}) return { missingRuntime: true };
        ${read} })()`, args);
      if (!result?.missingRuntime || !active()) return result;
    }
    if (!active()) return null;
    const result = await run(target.webSocketDebuggerUrl, `(() => {
      ${scopeCheck}
      const runtime = window.__isccAddonCandleRuntime = {
        version: ${version}, collect: (${collector}), calendar: (${calendar})
      };
      ${read} })()`, args);
    if (!result?.moved) installed.add(target.id);
    return result;
  }
  async function tick(full) {
    if (closed || running) return;
    running = true; refreshPending = false; controller = new AbortController();
    try {
      if (!active()) { reset(); return; }
      const layouts = sourceLayouts(), pinned = new Set(Object.values(layouts));
      let charts = pinnedCharts.filter(target => pinned.has(layoutOf(target)));
      if (full) {
        const discoveredAt = clock(), allCharts = await getTargets(controller.signal);
        if (!active()) return;
        await acceptOpenCharts([...new Set(allCharts.map(layoutOf).filter(Boolean))], discoveredAt);
        charts = allCharts.filter(target => target.webSocketDebuggerUrl)
          .sort((a, b) => Number(pinned.has(layoutOf(b))) - Number(pinned.has(layoutOf(a))));
        // One fast reader per selected layout. Full discovery still checks duplicate tabs.
        pinnedCharts = [...new Map(charts.filter(target => pinned.has(layoutOf(target)))
          .map(target => [layoutOf(target), target])).values()];
      }
      for (const target of charts) {
        if (!active()) break;
        try {
          const scope = layoutOf(target), readStartedAt = clock();
          const selected = sources().filter(symbol => full || layouts[symbol] === scope);
          const required = anchors().filter(anchor => full || layouts[anchor.symbol] === scope);
          const result = await collect(target, { selective: true, scope, sources: selected, sourceLayouts: layouts,
            anchors: required, ack: (full ? seen : fastSeen).get(target.id),
            ...(!full ? { sourceOnly: true } : {}) }, full);
          if (!active()) break;
          if (!result) continue;
          if (result.moved) { forget(target.id, true); continue; }
          const hasSnapshot = Array.isArray(result.sources) && Array.isArray(result.series);
          if (!full) {
            const currentLayouts = sourceLayouts();
            if (hasSnapshot && result.inventory?.scope === scope && result.token
              && selected.length && selected.every(symbol => currentLayouts[symbol] === scope)) {
              // Deletion needs a full inventory and a fresh topology proof.
              await acceptSnapshot({ sources: result.sources, series: result.series, inventory: { scope }, sourceOnly: true });
              fastSeen.set(target.id, result.token);
            } else if (!result.unchanged) fastSeen.delete(target.id);
            continue;
          }
          if (result.unchanged && result.token === seen.get(target.id)) {
            const inventory = inventories.get(target.id);
            if (inventory) await acceptInventory(inventory, readStartedAt);
            continue;
          }
          if (!result.token && hasSnapshot && !result.sources.length && !result.series.length) {
            await acceptInventory({ scope, complete: false, symbols: [] }, readStartedAt);
            seen.delete(target.id); inventories.delete(target.id); continue;
          }
          if (hasSnapshot) {
            await acceptSnapshot({ ...result, readStartedAt });
            if (result.inventory) inventories.set(target.id, result.inventory); else inventories.delete(target.id);
            seen.set(target.id, result.token);
          }
        } catch (error) {
          forget(target.id, full);
          if (active()) onError(error);
        }
      }
      const required = anchors();
      if (full && active() && charts.length && required.length) {
        const anchor = required[(historyCursor++) % required.length];
        const series = await run(charts[0].webSocketDebuggerUrl,
          `(${readCandleHistory.toString()})(${JSON.stringify(anchor)},(${calendar}))`, { signal: controller.signal });
        if (active() && series) await acceptSnapshot({ sources: [], series: [series] });
      }
      if (full) {
        const available = new Set(charts.map(target => target.id));
        for (const id of installed) if (!available.has(id)) forget(id, true);
      }
    } catch (error) {
      reset();
      if (active()) { await acceptOpenCharts(null, clock()); onError(error); }
    } finally {
      running = false; controller = undefined;
      if (full) fullReadAt = clock() + intervalMs;
      const remaining = full ? intervalMs : Math.max(0, fullReadAt - clock());
      if (refreshPending) schedule(250);
      else if (pinnedCharts.length && remaining > pinnedReadMs) schedule(pinnedReadMs, false);
      else schedule(remaining);
    }
  }
  schedule(1000);
  return {
    refresh() {
      if (closed) return;
      const pending = refreshPending; refreshPending = true;
      if (!running && !pending) schedule(250);
    },
    stop() { closed = true; clearTimer(timer); controller?.abort(); reset(); }
  };
}
module.exports = { startReader, chart, localDebugger, evaluate };
