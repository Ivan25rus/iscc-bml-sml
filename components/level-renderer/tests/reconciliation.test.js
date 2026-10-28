const { test } = require('node:test');
const assert = require('node:assert/strict');
const { CandleLevelSync } = require('../module/candleLevels');
const { snapshotCandlePage } = require('../module/page');
const SYMBOL = 'CAPITALCOM:NAS100';
const source = { symbol: SYMBOL, serverUpdateTime: 10, state: { type: 'LineToolHorzLine',
  points: [{ time_t: 1791981720, interval: '1', price: 30000 }], state: { symbol: SYMBOL } } };
function fixture(initial) {
  let now = 1800000000000;
  const sync = new CandleLevelSync({ initial, clock: () => now, getConfig: () => ({ mappings: [{enabled:true,sourceSymbol:SYMBOL,mt5Symbol:'USTEC'},{enabled:true,sourceSymbol:'SPREADEX:SPX',mt5Symbol:'SPX500'}] }) });
  return { sync, advance(ms = 10000) { now += ms; }, snapshot(ids, { scope = 'origin', complete = true,
    symbol = SYMBOL, symbolComplete = true, sources = [], readStartedAt = now } = {}) {
    sync.acceptSnapshot({ sources, series: [], readStartedAt,
      inventory: { scope, complete, symbols: [{ symbol, ids, complete: symbolComplete }] } });
  } };
}
function imported() {
  const f = fixture(); f.snapshot(['one'], { sources: [['one', source]] }); return f;
}
test('restart reconciles a deletion missed while ISCC was offline, using two complete origin reads', () => {
  const old = imported(); const f = fixture(old.sync.serialize());
  f.snapshot([]); assert.equal(f.sync.records.get('one').removed, false);
  f.advance(); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, true);
  assert.equal(f.sync.records.get('one').revision, 2);
  assert.equal(f.sync.records.get('one').phase, 'queued');
});
test('unrelated layout, symbol, incomplete reads, and unscoped empty snapshots cannot delete', () => {
  for (const options of [{ scope: 'other' }, { symbol: 'SPREADEX:SPX' }, { complete: false }, { symbolComplete: false }]) {
    const f = imported(); f.advance(); f.snapshot([], options); f.advance(); f.snapshot([], options);
    f.sync.acceptSnapshot({ sources: [], series: [] });
    assert.equal(f.sync.records.get('one').removed, false, JSON.stringify(options));
  }
});
test('a legacy record needs proven origin before absence can retire it', () => {
  const f = fixture(); f.sync.acceptSource('one', source);
  f.snapshot([]); f.advance(); f.snapshot([]); assert.equal(f.sync.records.get('one').removed, false);
  f.snapshot(['one']); f.advance(); f.snapshot([]); f.advance(); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, true);
});
test('presence between missing reads cancels deletion confirmation', () => {
  const f = imported(); f.advance(); f.snapshot([]); f.advance(); f.snapshot(['one']);
  f.advance(); f.snapshot([]); assert.equal(f.sync.records.get('one').removed, false);
});
test('local edit and stale/in-flight page reads cannot imply a deletion', () => {
  const f = imported(), captured = 1800000000000;
  f.advance(); f.sync.acceptSource('one', { ...source, state: { ...source.state,
    points: [{ ...source.state.points[0], price: 30001 }] } }, { local: true });
  f.snapshot([], { readStartedAt: captured }); f.advance(); f.snapshot([]);
  f.advance(); f.snapshot([]); assert.equal(f.sync.records.get('one').removed, false);
  f.advance(20000); f.snapshot([]); f.advance(); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, true);
});
test('a copy in another known layout prevents retirement until both origins confirm absence', () => {
  const f = imported(); f.snapshot(['one'], { scope: 'copy' });
  f.advance(); f.snapshot([]); f.advance(); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, false);
  f.snapshot([], { scope: 'copy' }); f.advance(); f.snapshot([]); f.snapshot([], { scope: 'copy' });
  assert.equal(f.sync.records.get('one').removed, true);
});
test('incomplete origin cancels a pending retirement; repeated same capture does not count twice', () => {
  const f = imported(); f.advance(); f.snapshot([]); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, false);
  f.advance(); f.snapshot([], { complete: false }); f.advance(); f.snapshot([]);
  assert.equal(f.sync.records.get('one').removed, false);
});
test('extractor marks loading, unsaved, and truncated collections unsafe for deletion', () => {
  for (const kind of ['complete', 'loading', 'unsaved', 'truncated', 'manyWidgets', 'drawingLoad', 'drawingSave', 'unknownReadiness']) {
    const main = { symbol: () => SYMBOL, interval: () => '1', isLoading: () => kind === 'loading',
      bars: () => ({ firstIndex: () => 0, lastIndex: () => -1 }) };
    const valid = { id: () => 'one', serverUpdateTime: () => kind === 'unsaved' ? 0 : 10,
      state: () => source.state };
    const widget = { isReady: () => kind !== 'unknownReadiness',
      lineToolsSynchronizer: () => ({ hasPendingLoadRequests: () => kind === 'drawingLoad',
        hasPendingSaveRequests: () => kind === 'drawingSave' }), model: () => ({ mainSeries: () => main,
      dataSources: () => kind === 'truncated' ? Array(4001).fill(valid) : [valid] }) };
    global.window = { _exposed_chartWidgetCollection: { getAll: () => kind === 'manyWidgets' ? Array(17).fill(widget) : [widget] } };
    try {
      const result = snapshotCandlePage(undefined, { scope: 'origin', selective: true });
      const safe = result.inventory.complete && result.inventory.symbols[0]?.complete;
      assert.equal(safe, kind === 'complete', kind);
    } finally { delete global.window; }
  }
});
// Addon reader inventory handoff is covered in renderer.test.js.
test('confirmed absence uses durable owned deletion and waits for the indicator before completion', async () => {
  const f = imported(), requests = [], published = [], saved = [];
  const row = f.sync.records.get('one'); row.phase = 'ready'; row.price = 28870.83;
  let acknowledged = false;
  f.sync.persist = async state => saved.push(state);
  f.sync.publish = async levels => published.push(levels);
  f.sync.getAdapter = () => ({ capabilities: ['candle-levels'], request: async (route, payload) => {
    assert.equal(route, '/candle-levels');
    if (payload) {
      assert.equal(saved.at(-1).records.find(row => row.sourceId === 'one').removed, true);
      assert.deepEqual(payload, { id: row.id, revision: 2, action: 'delete' }); requests.push(payload);
    }
    return { candleLevels: { snapshotApplied: acknowledged, levels: [] } };
  } });
  f.advance(); f.snapshot([]); f.advance(); f.snapshot([]); await f.sync.tick();
  assert.equal(requests.length, 1); assert.equal(f.sync.records.get('one').phase, 'deleting');
  assert.deepEqual(published.at(-1), { USTEC: [] });
  acknowledged = true; await f.sync.tick();
  assert.equal(f.sync.records.get('one').phase, 'removed');
});
