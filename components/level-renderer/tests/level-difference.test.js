const test = require('node:test');
const assert = require('node:assert/strict');
const { CandleLevelSync } = require('../module/candleLevels');
const { mappings } = require('../module/candle-mappings');

const sourceSymbol = 'EXAMPLE:INDEX', symbol = 'INDEX.cash';
const start = 1760000400;

function fixture({ enabled = true, standardSpread = 1.5, prices = [] } = {}) {
  let now = (start + 7200) * 1000;
  let saved;
  const config = { enabled: true, warnOnLevelDifference: enabled,
    mappings: [{ enabled: true, sourceSymbol, mt5Symbol: symbol, standardSpread }] };
  const sync = new CandleLevelSync({ getConfig: () => config, clock: () => now,
    persist: async value => { saved = value; },
    getAdapter: () => ({ capabilities: ['candle-levels', 'candle-levels-window'], candleSymbol: value => value,
      request: async (_route, payload) => ({ candleLevels: { snapshotApplied: true, levels: [{
        ...payload, status: 'ready', price: prices.shift()
      }] } }) }) });
  function edit(id, price) {
    sync.acceptBars({ symbol: sourceSymbol, interval: '60', event: 'candle_bars',
      bars: [[start, price, price - 10]], windows: [[start, start + 3600]] });
    sync.acceptSource(id, { symbol: sourceSymbol, state: { type: 'LineToolHorzRay',
      points: [{ time_t: start, interval: '60', price }], state: {} } }, { local: true });
  }
  return { sync, config, edit, saved: () => saved, advance: ms => { now += ms; } };
}

test('A configured spread warns in both directions strictly beyond twice its value', async () => {
  const f = fixture({ prices: [27003, 27003.01, 26996.99] });
  for (const id of ['exact', 'above', 'below']) {
    f.edit(id, 27000);
    await f.sync.tick();
  }
  const history = f.sync.levelWarningHistory();
  assert.deepEqual(history.map(({ symbol, mappedPrice, threshold }) => [symbol, mappedPrice, threshold]),
    [[symbol, 26996.99, 3], [symbol, 27003.01, 3]]);
  assert.equal(f.sync.levelWarnings().length, 2);
  assert.deepEqual(f.sync.levelWarnings({ includeExpiry: true }).map(row => row.expiresAt),
    history.map(row => row.at + 30_000).reverse());
  assert.equal(f.saved().levelWarningHistory.length, 2);
  f.advance(31000);
  assert.deepEqual(f.sync.levelWarnings(), []);
  const restored = new CandleLevelSync({ initial: f.saved(), getConfig: () => f.config });
  assert.deepEqual(restored.levelWarningHistory(), history);
});

test('Warnings are opt-in and each mapping with spread 0 leaves both notice and history empty', async () => {
  for (const options of [{ enabled: false }, { standardSpread: 0 }]) {
    const f = fixture({ ...options, prices: [27020] });
    f.edit('disabled', 27000);
    await f.sync.tick();
    assert.deepEqual(f.sync.levelWarnings(), []);
    assert.deepEqual(f.sync.levelWarningHistory(), []);
    assert.equal(f.sync.status().ready, 1);
  }
  for (const spread of [-1, Infinity, '1.5']) {
    assert.throws(() => mappings(fixture({ standardSpread: spread }).config), /спред/);
  }
});

test('Changing a spread rechecks confirmed levels and opting out dismisses the active warning', async () => {
  const f = fixture({ prices: [27004], standardSpread: 3 });
  f.edit('confirmed', 27000);
  await f.sync.tick();
  assert.deepEqual(f.sync.levelWarnings(), []);

  f.config.mappings[0].standardSpread = 1.5;
  f.sync.refreshLevelWarnings();
  assert.equal(f.sync.levelWarnings()[0]?.threshold, 3);
  assert.equal(f.sync.levelWarningHistory().length, 1);

  f.config.warnOnLevelDifference = false;
  f.sync.refreshLevelWarnings();
  assert.deepEqual(f.sync.levelWarnings(), []);
  assert.equal(f.sync.levelWarningHistory().length, 1);
});
