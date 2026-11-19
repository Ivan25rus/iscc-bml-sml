const { test } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('events');
const { PassThrough } = require('stream');
const { pathToFileURL } = require('url');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { readOnlyAdapter, StdioAdapter } = require('../module/transport');
const { chart, localDebugger } = require('../module/reader');
const { startReader } = require('../module/reader');
const { attach } = require('../module');
const { relayWarnings, READ_WARNINGS, WARNINGS_CHANGED } = require('../module/warnings-ipc');
const { mountWarnings } = require('../module/warnings-ui');

test('existing adapter facade exposes candle requests without trading methods', async () => {
  const calls = [];
  const raw = { capabilities: ['candle-levels'], health: async () => ({}),
    candleSymbol: value => value, request: async (...args) => { calls.push(args); return { candleLevels: {} }; },
    place() { throw Error('trading must be unreachable'); } };
  const adapter = readOnlyAdapter(raw);
  await adapter.request('/candle-levels', { action: 'delete' });
  assert.throws(() => adapter.request('/place', {}));
  assert.equal(calls.length, 1); assert.equal(adapter.place, undefined);
  assert.deepEqual(adapter.capabilities, ['candle-levels']);
});

test('local chart discovery excludes other websites and remote debug endpoints', () => {
  assert.ok(chart('https://www.tradingview.com/chart/'));
  assert.ok(chart('https://ru.tradingview.com/chart/demo/'));
  assert.equal(chart('https://tradingview.com.attacker.invalid/chart/'), false);
  assert.equal(chart('https://user@tradingview.com/chart/'), false);
  assert.ok(localDebugger('ws://127.0.0.1:9222/devtools/page/one'));
  assert.equal(localDebugger('ws://203.0.113.1:9222/devtools/page/one'), false);
  assert.equal(localDebugger('ws://127.0.0.1:9223/devtools/page/one'), false);
});

function processFixture() {
  const child = new EventEmitter();
  child.stdin = new PassThrough(); child.stdout = new PassThrough(); child.stderr = new PassThrough();
  child.killed = false; child.kill = () => { child.killed = true; };
  let invocation, input = '';
  child.stdin.on('data', bytes => { input += bytes; });
  const adapter = new StdioAdapter(['python', 'reader.py', '--config', 'private-config.json'], {
    spawnProcess(...args) { invocation = args; return child; }
  });
  return { child, adapter, invocation, input: () => input,
    reply(value) { child.stdout.write(JSON.stringify(value) + '\n'); } };
}

test('stdio transport uses literal arguments, validates capabilities and correlates replies', async () => {
  const fixture = processFixture();
  assert.equal(fixture.invocation[2].shell, false);
  const health = fixture.adapter.health();
  const request = JSON.parse(fixture.input().trim());
  assert.equal(request.route, '/health');
  fixture.reply({ id: request.id, result: { capabilities: ['candle-levels', 'candle-levels-color', 'trade'] } });
  await health;
  assert.deepEqual(fixture.adapter.capabilities, ['candle-levels', 'candle-levels-color']);
  await assert.rejects(fixture.adapter.request('/place', {}));
  fixture.adapter.close();
});

test('stdio invalid JSON or stopped receiver rejects pending work', async () => {
  const fixture = processFixture();
  const pending = fixture.adapter.health();
  fixture.child.stdout.write('null\n');
  await assert.rejects(pending);
  assert.equal(fixture.child.killed, true);
});

test('stdio never constructs a shell command from downloaded plan data', () => {
  for (const command of [[], 'python reader.py', ['python', 'bad\nargument'], [null]]) {
    assert.throws(() => new StdioAdapter(command));
  }
});

test('shipped settings start disabled with no recipient mappings or commands', () => {
  const config = require('../module/config/candle-renderer.json');
  assert.equal(config.enabled, false);
  assert.equal(config.warnOnLevelDifference, false);
  assert.deepEqual(config.mappings, []);
  assert.deepEqual(config.readerCommand, []);
  assert.deepEqual(require('../module/candle-mappings').sourceMap(), {});
});

test('disabled addon consumes no drawings and does not access the broker', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'candle-addon-test-'));
  let reads = 0, stopped = false, registration;
  const servicesApi = { mt5VPS: { getAdapter() { reads++; throw Error('disabled'); } } };
  const config = { enabled: false, mappings: [], adapterMode: 'iscc', publishToLT: true };
  const settings = { register(...args) { registration = args; }, readConfig: () => ({ config }), onApply() {} };
  const renderer = attach({ servicesApi, root, settings,
    createReader() { return { refresh() {}, stop() { stopped = true; } }; } });
  try {
    assert.equal(renderer.acceptSource('manual', {}), false);
    assert.equal(renderer.status().enabled, false);
    assert.equal(reads, 0);
    assert.equal(attach({ servicesApi, root, settings }), renderer);
    assert.ok(registration[1].endsWith('candle-renderer.json'));
  } finally { renderer.stop(); fs.rmSync(root, { recursive: true, force: true }); }
  assert.equal(stopped, true);
});

test('source state corruption is preserved instead of silently clearing saved levels', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'candle-addon-test-'));
  fs.writeFileSync(path.join(root, 'candle-level-renderer-state.json'), 'invalid-state');
  const settings = { register() {}, readConfig: () => ({ config: {} }) };
  try { assert.throws(() => attach({ servicesApi: {}, root, settings }), /состояние/); }
  finally { fs.rmSync(root, { recursive: true, force: true }); }
});

function readerHarness(overrides={}) {
  const timers = new Map(), expressions = [], snapshots = [], errors = [];
  let sequence = 0, now = 1700000000000;
  const reader = startReader({ enabled: () => true, sources: () => [], anchors: () => [],
    acceptSnapshot: value => snapshots.push(value), onError: error => errors.push(error.message),
    getTargets: async () => [{id:'one',webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/one'}],
    run: async (_url, expression) => {
      expressions.push(expression);
      return expressions.length===1 ? {sources:[],series:[],token:'one:1'} : {unchanged:true,token:'one:1'};
    },
    clock: () => now,
    setTimer(fn,delay) { const id=++sequence;timers.set(id,{fn,delay});return id; },
    clearTimer(id) { timers.delete(id); },...overrides });
  return {reader,expressions,snapshots,errors,timers,async next(){
    const [id,timer]=timers.entries().next().value;timers.delete(id);now+=timer.delay;await timer.fn();
  }};
}

test('warm page reads keep ten-second cadence and avoid resending collector source', async () => {
  const fixture=readerHarness();
  try {
    await fixture.next();await fixture.next();
    assert.ok(fixture.expressions[0].length>5000);
    assert.ok(fixture.expressions[1].length<1200);
    assert.equal(fixture.snapshots.length,1);
    assert.equal([...fixture.timers.values()][0].delay,10000);
  } finally {fixture.reader.stop();}
});

test('disabled page reader never accesses TradingView', async () => {
  let discoveries=0;
  const fixture=readerHarness({enabled:()=>false,getTargets:async()=>{discoveries++;return [];}});
  try {await fixture.next();assert.equal(discoveries,0);assert.equal(fixture.expressions.length,0);}
  finally {fixture.reader.stop();}
});

test('a failed chart does not block later chart snapshots', async () => {
  const fixture=readerHarness({getTargets:async()=>[{id:'bad',webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/bad'},{id:'good',webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/good'}],run:async(_url,expression)=>{
    if(expression.includes('collect: (')) {
      if(!fixture.tried){fixture.tried=true;throw Error('one chart unavailable');}
    }
    return {sources:[],series:[],token:'good:1'};
  }});
  try {await fixture.next();assert.equal(fixture.errors.length,1);assert.equal(fixture.snapshots.length,1);}
  finally {fixture.reader.stop();}
});

test('unchanged addon reads refresh scoped inventory proof without replaying sources', async () => {
  const inventories = [], inventory = {scope:'probe',complete:true,
    symbols:[{symbol:'CAPITALCOM:NAS100',complete:true,ids:[]}]};
  let reads = 0;
  const fixture=readerHarness({acceptInventory:(value,readStartedAt)=>inventories.push({value,readStartedAt}),
    getTargets:async()=>[{id:'one',url:'https://tradingview.com/chart/probe/',
      webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/one'}],
    run:async(_url,expression)=>{
      assert.ok(expression.includes('"scope":"probe"'));
      reads++;
      return reads===1 ? {sources:[],series:[],token:'one:1',inventory}
        : reads===2 ? {unchanged:true,token:'one:1'} : {sources:[],series:[]};
    }});
  try {
    await fixture.next();await fixture.next();
    assert.equal(fixture.snapshots.length,1);
    assert.ok(fixture.snapshots[0].readStartedAt>0);
    assert.deepEqual(inventories[0].value,inventory);
    assert.ok(inventories[0].readStartedAt>0);
    await fixture.next();
    assert.deepEqual(inventories.at(-1).value,{scope:'probe',complete:false,symbols:[]});
    assert.equal(fixture.snapshots.length,1);
  } finally {fixture.reader.stop();}
});

test('a failed addon read discards cached inventory before recovery', async () => {
  const inventories=[];let reads=0;
  const fixture=readerHarness({acceptInventory:value=>inventories.push(value),
    getTargets:async()=>[{id:'one',url:'https://tradingview.com/chart/probe/',webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/one'}],
    run:async()=>{
      if(++reads===1)return {sources:[],series:[],token:'one:1',
        inventory:{scope:'probe',complete:true,symbols:[]}};
      if(reads===2)throw Error('temporary chart failure');
      return {unchanged:true,token:'one:1'};
    }});
  try {
    await fixture.next();await fixture.next();await fixture.next();
    assert.equal(fixture.errors.length,1);
    assert.equal(inventories.length,0);
  } finally {fixture.reader.stop();}
});

test('addon discovery includes inaccessible chart origins before reading accessible drawings',async()=>{
  const events=[];const fixture=readerHarness({
    getTargets:async()=>[{id:'one',url:'https://tradingview.com/chart/probe/',webSocketDebuggerUrl:'ws://127.0.0.1:9222/devtools/page/one'},
      {id:'two',url:'https://tradingview.com/chart/other/'}],
    acceptOpenCharts:scopes=>events.push({scopes}),acceptSnapshot:()=>events.push({snapshot:true})});
  try{await fixture.next();assert.deepEqual(events[0],{scopes:['probe','other']});assert.deepEqual(events[1],{snapshot:true});
    assert.equal(fixture.expressions.length,1);
  }finally{fixture.reader.stop();}
});

test('failed addon discovery invalidates topology instead of reusing a prior proof',async()=>{
  const events=[];let failed=false;const fixture=readerHarness({
    getTargets:async()=>{if(failed)throw Error('offline');return [];},acceptOpenCharts:scopes=>events.push(scopes)});
  try{await fixture.next();failed=true;await fixture.next();assert.deepEqual(events,[[],null]);}
  finally{fixture.reader.stop();}
});

test('missing old candle history uses a bounded separate query and a refresh requested in flight survives',async()=>{
  const snapshots=[],anchor={symbol:'CAPITALCOM:NAS100',interval:'D1',time:1789336800};let fixture;
  fixture=readerHarness({anchors:()=>[anchor],acceptSnapshot:value=>snapshots.push(value),
    run:async(_url,expression)=>{
      if(expression.includes('readCandleHistory')){fixture.reader.refresh();return {symbol:anchor.symbol,interval:'D1',bars:[[anchor.time,100,90]],windows:[[anchor.time,anchor.time+86400]]};}
      return {sources:[],series:[],token:'one:1'};
    }});
  try{await fixture.next();assert.equal(snapshots.length,2);assert.equal(snapshots[1].series[0].interval,'D1');
    assert.equal([...fixture.timers.values()][0].delay,250);
  }finally{fixture.reader.stop();}
});

const target = (id, scope = id) => ({ id, url: `https://tradingview.com/chart/${scope}/`,
  webSocketDebuggerUrl: `ws://127.0.0.1:9222/devtools/page/${id}` });

test('fast rereads one configured tab per layout and preserve full inventory cadence', async () => {
  const symbol = 'SYNTH:INDEX', inventory = { scope: 'work', complete: true, symbols: [] };
  const reads = [], inventories = [];
  let discoveries = 0;
  const fixture = readerHarness({
    sourceLayouts: () => ({ [symbol]: 'work' }), sources: () => [symbol],
    getTargets: async () => { discoveries++; return [target('other'), target('first', 'work'), target('second', 'work')]; },
    acceptInventory: value => inventories.push(value),
    run: async (url, expression, options) => {
      reads.push({ url, expression, timeout: options.timeoutMs });
      const token = options.timeoutMs ? 'fast:1' : 'full:1';
      if (expression.includes(`"ack":"${token}"`)) return { unchanged: true, token };
      return { sources: [], series: [], inventory, token };
    }
  });
  try {
    await fixture.next();
    assert.equal(discoveries, 1);
    assert.equal(reads.length, 3);
    assert.equal([...fixture.timers.values()][0].delay, 2000);
    for (let i = 0; i < 4; i++) await fixture.next();
    assert.equal(discoveries, 1);
    const fastReads = reads.filter(read => read.timeout);
    assert.equal(fastReads.length, 4);
    assert.ok(fastReads.every(read => read.url.endsWith('/second') && read.timeout === 1000
      && read.expression.includes('"sourceOnly":true')));
    assert.deepEqual(fixture.snapshots.at(-1), { sources: [], series: [], inventory: { scope: 'work' }, sourceOnly: true });
    assert.equal(inventories.length, 0);
    await fixture.next();
    assert.equal(discoveries, 2);
    assert.equal(inventories.length, 3);
  } finally { fixture.reader.stop(); }
});

test('a moved or newly unassigned fast tab contributes no drawings', async () => {
  const symbol = 'SYNTH:INDEX';
  let layout = 'work', discoveries = 0, fastReads = 0;
  const fixture = readerHarness({
    sourceLayouts: () => ({ [symbol]: layout }), sources: () => [symbol],
    getTargets: async () => { discoveries++; return [target('page', 'work')]; },
    run: async (_url, expression, options) => {
      if (options.timeoutMs && ++fastReads === 1) layout = 'new';
      if (options.timeoutMs && fastReads === 2) {
        assert.ok(expression.includes("here.pathname.split('/')[2]"));
        return { moved: true };
      }
      return { sources: [], series: [], token: 'read:1', inventory: { scope: 'work', complete: true, symbols: [] } };
    }
  });
  try {
    await fixture.next();
    await fixture.next();
    assert.equal(fixture.snapshots.length, 1);
    layout = 'work';
    await fixture.next();
    assert.equal(fixture.snapshots.length, 1);
    assert.equal(fastReads, 2);
    assert.equal([...fixture.timers.values()][0].delay, 6000);
    await fixture.next();
    assert.equal(discoveries, 2);
  } finally { fixture.reader.stop(); }
});

test('warning IPC serves only the main local panel and sends changes once', () => {
  let handler, removed;
  const changes = [];
  const ipcMain = { handle(channel, cb) { assert.equal(channel, READ_WARNINGS); handler = cb; },
    removeHandler(channel) { removed = channel; } };
  const url = pathToFileURL(path.resolve(__dirname, '../../index.html')).href;
  const mainFrame = { url };
  const sender = { mainFrame, isDestroyed: () => false, getURL: () => mainFrame.url,
    send: (...args) => changes.push(args) };
  let state = { enabled: false, active: [], history: [] };
  const relay = relayWarnings(ipcMain, () => state);
  assert.equal(handler({ sender, senderFrame: { url } }), null);
  assert.deepEqual(handler({ sender, senderFrame: mainFrame }), state);
  relay.publish();
  assert.equal(changes.length, 0);
  state = { ...state, enabled: true };
  relay.publish();
  relay.publish();
  assert.deepEqual(changes, [[WARNINGS_CHANGED, state]]);
  mainFrame.url = 'https://www.tradingview.com/chart/example/';
  assert.equal(handler({ sender, senderFrame: mainFrame }), null);
  state = { ...state, enabled: false };
  relay.publish();
  assert.equal(changes.length, 1);
  relay.stop();
  assert.equal(removed, READ_WARNINGS);
});

test('warning UI offers redrawing, permits dismissing and keeps local history available', async () => {
  class Element {
    constructor() { this.children = []; this.events = {}; this.attributes = {}; }
    append(...nodes) { this.children.push(...nodes); }
    appendChild(node) { this.append(node); }
    setAttribute(key, value) { this.attributes[key] = value; }
    replaceChildren(...nodes) { this.children = nodes; }
    addEventListener(event, cb) { this.events[event] = cb; }
  }
  const toolbar = new Element(), head = new Element(), body = new Element();
  const document = { head, body, getElementById: () => ({ parentElement: toolbar }), createElement: () => new Element() };
  const ipcRenderer = new EventEmitter();
  ipcRenderer.invoke = async () => ({ enabled: false, active: [], history: [] });
  let now = 1700000000000, timer;
  mountWarnings({ document, ipcRenderer, clock: () => now,
    setTimer(fn, delay) { timer = { fn, delay }; return timer; },
    clearTimer(handle) { if (handle === timer) timer = undefined; } });
  await Promise.resolve();
  const [toggle] = toolbar.children, [history, toast] = body.children;
  assert.equal(toggle.hidden, true);
  const row = { id: 'warning', symbol: 'SYNTH_INDEX', referencePrice: 100, mappedPrice: 105, threshold: 2 };
  const state = { enabled: true, active: [{ ...row, expiresAt: now + 30000 }], history: [{ ...row, at: now }] };
  ipcRenderer.emit(WARNINGS_CHANGED, {}, state);
  assert.equal(toggle.hidden, false);
  assert.equal(toast.hidden, false);
  assert.match(toast.children[0].children[0].textContent, /перерисуйте уровень/);
  assert.match(toast.children[1].textContent, /ТВ 100 → MT5 105/);
  assert.equal(timer.delay, 30000);
  toast.children[0].children[1].events.click();
  assert.equal(toast.hidden, true);
  assert.equal(timer, undefined);
  toggle.events.click();
  assert.equal(history.hidden, false);
  assert.match(history.children[1].children[0].textContent, /ТВ 100 → MT5 105/);
  const later = { ...row, id: 'later', expiresAt: now + 2000 };
  ipcRenderer.emit(WARNINGS_CHANGED, {}, { ...state, active: [later] });
  assert.equal(toast.hidden, false);
  now += 2000;
  timer.fn();
  assert.equal(toast.hidden, true);
  assert.equal(history.hidden, false);
});
