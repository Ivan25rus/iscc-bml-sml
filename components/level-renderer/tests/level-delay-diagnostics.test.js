const test=require('node:test'),assert=require('node:assert/strict');
const {CandleLevelSync}=require('../module/candleLevels');
const time=1793808000;
function fixture(diagnose){
  let now=(time+7200)*1000,phase='waiting-history';const events=[],calls=[];
  const source={symbol:'CAPITALCOM:NAS100',serverUpdateTime:now-10,state:{type:'LineToolHorzRay',
    points:[{time_t:time,interval:'60',price:28339.4}],state:{linecolor:'#ffffff'}}};
  const adapter={capabilities:['candle-levels','candle-levels-window','candle-levels-color'],candleSymbol:value=>value,
    request:async(route,payload)=>{
      calls.push({route,payload});const row=sync.records.get('source');
      return {candleLevels:{snapshotApplied:phase==='ready',levels:[{...row,symbol:'USTEC',status:phase,price:28340.9,retryAt:now+1000}]}};
    }};
  const sync=new CandleLevelSync({getConfig:()=>({enabled:true,mappings:[{enabled:true,sourceSymbol:source.symbol,mt5Symbol:'USTEC'}]}),getAdapter:()=>adapter,clock:()=>now,
    diagnose:diagnose || (event=>events.push(event)),publish:async levels=>levels});
  sync.acceptBars({event:'candle_bars',symbol:source.symbol,interval:'60',bars:[[time,28339.4,28300]],windows:[[time,time+3600]]});
  return {sync,source,events,calls,advance(ms){now+=ms;},phase(value){phase=value;}};
}
test('Level diagnostics separate source observation, native history, indicator acknowledgement and LT publication',async()=>{
  const f=fixture();f.sync.acceptSource('source',f.source);await f.sync.tick();
  assert.ok(f.events.some(row=>row.event==='source-observed' && row.sourceAgeMs===10));
  assert.ok(f.events.some(row=>row.event==='phase' && row.phase==='waiting-history'));
  assert.equal(f.events.filter(row=>row.event==='lt-published').length,0);
  f.advance(1000);f.phase('drawing');await f.sync.tick();
  assert.ok(f.events.some(row=>row.event==='phase' && row.phase==='drawing' && row.elapsedMs===1000));
  assert.equal(f.events.filter(row=>row.event==='lt-published').length,0);
  f.advance(1000);f.phase('ready');await f.sync.tick();
  const done=f.events.find(row=>row.event==='lt-published');
  assert.equal(done.price,28340.9);assert.equal(done.elapsedMs,2000);
  assert.ok(f.events.some(row=>row.event==='indicator-read' && row.snapshotApplied));
  const count=f.events.length,requests=f.calls.length;await f.sync.tick();
  assert.equal(f.events.length,count,'Unchanged ready levels produce no diagnostic writes');
  assert.equal(f.calls.length,requests,'Diagnostics introduce no extra network reads');
});
test('Failed diagnostic storage cannot delay or reject level synchronization',async()=>{
  for(const diagnose of [()=>{throw Error('disk failure');},()=>Promise.reject(Error('disk failure')),()=>new Promise(()=>{})]){
    const f=fixture(diagnose);f.phase('ready');f.sync.acceptSource('source',f.source);await f.sync.tick();
    assert.equal(f.sync.status().ready,1);assert.equal(f.calls.length,1);
  }
});
test('A failed level request is recorded separately and does not publish an unconfirmed price',async()=>{
  const f=fixture();f.sync.getAdapter=()=>({capabilities:['candle-levels','candle-levels-window','candle-levels-color'],
    candleSymbol:value=>value,request:async()=>{throw Error('offline');}});
  f.sync.acceptSource('source',f.source);await f.sync.tick();
  assert.ok(f.events.some(row=>row.event==='vps-error' && row.reason==='offline'));
  assert.equal(f.events.some(row=>row.event==='lt-published'),false);
});
test('Upgrade retries an incomplete old history window once without changing its daily anchor or inventing a price',()=>{
  const initial={records:[{id:'a'.repeat(24),sourceId:'daily',revision:4,phase:'error',windowed:true,
    symbol:'USTEC',sourceSymbol:'CAPITALCOM:NAS100',timeframe:'D1',time:1777240800,endTime:1777323600,
    side:'low',sourcePrice:27404.7,price:null,removed:false,reason:'Invalid or incomplete MT5 minute window coverage'}]};
  const sync=new CandleLevelSync({initial});const row=sync.records.get('daily');
  assert.equal(row.phase,'queued');assert.equal(row.revision,5);assert.equal(row.historyReaderRecovery,1);
  assert.equal(row.time,1777240800);assert.equal(row.endTime,1777323600);assert.equal(row.price,null);
  assert.deepEqual(sync.currentLevels(),{USTEC:[]});
  row.phase='error';row.reason='Invalid or incomplete MT5 minute window coverage';
  const restarted=new CandleLevelSync({initial:sync.serialize()});
  assert.equal(restarted.records.get('daily').revision,5,'A restart cannot create an infinite retry cycle');
});

function scheduledFixture({ request, health, capabilities } = {}) {
  let now = (time + 7200) * 1000;
  const timers = new Map();
  const calls = [];
  const config = { mappings: [{ enabled: true, sourceSymbol: 'CAPITALCOM:NAS100', mt5Symbol: 'USTEC', sourceLayout: 'working' }] };
  const source = { symbol: 'CAPITALCOM:NAS100', serverUpdateTime: now, state: { type: 'LineToolHorzRay',
    points: [{ time_t: time, interval: '60', price: 28339.4 }] } };
  const series = { event: 'candle_bars', symbol: source.symbol, interval: '60', bars: [[time, 28339.4, 28300]],
    windows: [[time, time + 3600]] };
  const applied = payload => ({ candleLevels: { snapshotApplied: true,
    levels: payload ? [{ ...payload, status: 'ready', price: 28340.9 }] : [] } });
  const sync = new CandleLevelSync({
    getConfig: () => config,
    getAdapter: () => ({ capabilities: capabilities || ['candle-levels', 'candle-levels-window'], candleSymbol: value => value, health,
      request: async (route, payload) => {
        calls.push({ at: now, route, payload });
        return request ? request(payload, now) : applied(payload);
      } }),
    clock: () => now,
    setTimer: (callback, delay) => {
      const timer = Symbol();
      timers.set(timer, { callback, at: now + delay });
      return timer;
    },
    clearTimer: timer => timers.delete(timer),
    publish: async levels => levels
  }).start();
  return { sync, calls, source, series, applied, clock: () => now,
    snapshot: (ids, withBars = true) => sync.acceptSnapshot({ sources: ids.map(id => [id, source]),
      series: withBars ? [series] : [], inventory: { scope: 'working' } }),
    nextAt: () => Math.min(...[...timers.values()].map(timer => timer.at)),
    fire: () => {
      assert.equal(timers.size, 1);
      const [key, timer] = timers.entries().next().value;
      timers.delete(key);
      now = timer.at;
      return timer.callback();
    }
  };
}

test('An authoritative snapshot wakes a newly runnable level before the next idle tick', async () => {
  const f = scheduledFixture();
  const start = f.clock();
  f.snapshot(['source'], false);
  assert.equal(f.nextAt(), start + 1000);
  assert.equal(f.sync.records.get('source').phase, 'awaiting-tv');

  f.snapshot([]);
  assert.equal(f.nextAt(), start);
  assert.equal(f.calls.length, 0);
  await f.fire();
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].payload.action, 'upsert');
  assert.equal(f.calls[0].payload.revision, 2);
  assert.equal(f.sync.status().ready, 1);

  f.snapshot(['source']);
  assert.equal(f.nextAt(), start + 1000);
  await f.fire();
  assert.equal(f.calls.length, 1, 'An unchanged snapshot and idle timer send no requests');
  f.sync.stop();
  assert.equal(f.nextAt(), Infinity);
});

test('A nonaligned history retry keeps its due time and only publishes an indicator-confirmed price', async () => {
  let received, acknowledge = false, retryAt;
  const f = scheduledFixture({ request(payload, now) {
    if (payload) received = payload;
    retryAt ??= now + 1350;
    return { candleLevels: { snapshotApplied: acknowledge, levels: [{ ...received, price: 28340.9, retryAt,
      status: acknowledge ? 'ready' : now < retryAt ? 'waiting-history' : 'drawing' }] } };
  } });
  const start = f.clock();
  f.snapshot(['source']);
  await f.fire();
  assert.equal(f.sync.status().ready, 0);
  await f.fire();
  assert.equal(f.clock(), start + 1000);
  assert.equal(f.calls.length, 1);
  assert.equal(f.nextAt(), retryAt);
  await f.fire();
  assert.deepEqual(f.calls.map(call => [call.at - start, call.payload?.action]), [[0, 'upsert'], [1350, 'upsert'], [1350, undefined]]);
  assert.equal(f.sync.status().ready, 0);
  assert.deepEqual(f.sync.currentLevels(), { USTEC: [] });

  acknowledge = true;
  await f.fire();
  assert.equal(f.calls.at(-1).at, start + 2350);
  assert.deepEqual(f.sync.currentLevels(), { USTEC: [28340.9] });
  f.sync.stop();
});

test('An unrelated indicator read cannot postpone a due history retry or bypass its read cadence', async () => {
  const requests = new Map();
  const f = scheduledFixture({ request(payload, now) {
    if (payload) requests.set(payload.id, payload);
    const levels = [...requests.values()].map(row => {
      const history = row === requests.values().next().value;
      return { ...row, price: 28340.9, ...(history ? { status: 'waiting-history', retryAt: start + 1350 } : { status: 'drawing' }) };
    });
    return { candleLevels: { snapshotApplied: false, levels } };
  } });
  const start = f.clock();
  f.snapshot(['source', 'second']);
  await f.fire();
  await f.fire();
  assert.equal(f.nextAt(), start + 1350);
  await f.fire();
  assert.equal(f.calls.at(-1).at, start + 1350);
  assert.equal(f.calls.at(-1).payload.id, f.sync.records.get('source').id);
  assert.deepEqual(f.calls.filter(call => !call.payload).map(call => call.at - start), [0, 1000]);
  f.sync.stop();
});

test('A wake during a VPS write remains single-flight and four writes are the batch limit', async () => {
  let release, entered;
  const firstWrite = new Promise(resolve => { entered = resolve; });
  const held = new Promise(resolve => { release = resolve; });
  const f = scheduledFixture({ request: async payload => {
    if (f.calls.length === 1) { entered(); await held; }
    return f.applied(payload);
  } });
  const start = f.clock();
  f.snapshot(['first']);
  const running = f.fire();
  await firstWrite;
  f.snapshot(['second', 'third', 'fourth', 'fifth', 'sixth']);
  await f.sync.tick();
  assert.equal(f.calls.length, 1);
  assert.equal(f.nextAt(), Infinity);
  release();
  await running;

  assert.equal(f.nextAt(), start);
  await f.fire();
  assert.equal(f.calls.length, 5);
  assert.equal(f.nextAt(), start + 1000);
  await f.fire();
  assert.equal(f.calls.length, 6);
  f.sync.stop();
});

test('A new snapshot respects network and per-level backoff', async () => {
  const capabilities = [];
  const checks = [];
  let attempts = 0;
  const f = scheduledFixture({ capabilities, health: async () => {
    checks.push(f.clock());
    if (checks.length === 1) throw Error('offline');
    capabilities.push('candle-levels', 'candle-levels-window');
  }, request(payload) {
    if (++attempts === 1) throw Error('temporary write failure');
    return f.applied(payload);
  } });
  const start = f.clock();
  f.snapshot(['first']);
  await f.fire();
  f.snapshot(['second']);
  await f.fire();
  assert.deepEqual(checks, [start]);
  assert.equal(f.calls.length, 0);
  await f.fire();
  assert.deepEqual(checks, [start, start + 2000]);
  assert.equal(f.calls.length, 2);
  assert.equal(f.sync.records.get('first').requestError, 'temporary write failure');
  await f.fire();
  assert.equal(f.calls.length, 2);
  await f.fire();
  assert.equal(f.calls.length, 3);
  assert.equal(f.calls.at(-1).at, start + 4000);
  assert.equal(f.sync.status().ready, 2);
  f.sync.stop();
});
