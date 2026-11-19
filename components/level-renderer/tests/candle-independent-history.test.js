const test=require('node:test'),assert=require('node:assert/strict');
const {readCandleHistory}=require('../module/page');
test('Missing D1 candle uses its own bounded data session and exact calendar without changing the M1 chart',async()=>{
  const anchor={symbol:'CAPITALCOM:NAS100',interval:'D1',time:1789336800};
  const calls=[];let destroyed=0;
  class Session {
    constructor(api,wire,quiet){calls.push(['construct',quiet]);}
    connect(){calls.push(['connect']);}
    resolveSymbol(id,symbol,callback){calls.push(['resolve',symbol]);callback({method:'symbol_resolved',params:[id,{timezone:'Europe/London',session:'24x7'}]});}
    createSeries(id,version,symbol,resolution,count,range,callback){
      calls.push(['series',resolution,count,range]);queueMicrotask(()=>callback({method:'data_update',params:{plots:[{value:[anchor.time,29057.4,29283.7,28795.5]}]}}));
    }
    destroy(){destroyed++;}
  }
  const existing={constructor:Session,chartApi:()=>({}),wireFormat:()=>({})};
  const api={_chartWidget:{chartSession:()=>existing},_prepareEndOfPeriodArgs(){
    const series=this._chartWidget.model().model().mainSeries();assert.equal(series.interval(),'1D');return {};
  }};
  global.window={TradingViewApi:{chart:()=>api}};
  try {
    const result=await readCandleHistory(anchor,(calendar,info)=>{
      calendar._prepareEndOfPeriodArgs();assert.equal(info.timezone,'Europe/London');return ()=>1789419600;
    });
    assert.deepEqual(result.bars,[[anchor.time,29283.7,28795.5]]);
    assert.deepEqual(result.windows,[[anchor.time,1789419600]]);assert.equal(destroyed,1);
    assert.equal(calls.find(row=>row[0]==='series')[2],4);
    assert.equal((await readCandleHistory(anchor)).bars[0][2],28795.5);
    assert.equal(destroyed,1,'A cached historical candle opens no new session');
  }finally{delete global.window;}
});

test('missing and malformed historical results use the bounded retry cache', async () => {
  const anchor = { symbol: 'CAPITALCOM:NAS100', interval: 'M1', time: 1791981720 };
  let opened = 0, destroyed = 0;
  let plots = [null, { value: [anchor.time] }, { value: [anchor.time + 60, 1, 2, 1] }];
  class Session {
    constructor() { opened++; }
    connect() {}
    resolveSymbol(id, _symbol, callback) {
      callback({ method: 'symbol_resolved', params: [id, { timezone: 'UTC', session: '24x7' }] });
    }
    createSeries(...args) { args.at(-1)({ method: 'data_update', params: { plots } }); }
    destroy() { destroyed++; }
  }
  const widget = { chartSession: () => ({ constructor: Session, chartApi: () => ({}), wireFormat: () => ({}) }) };
  const api = { _chartWidget: widget, _prepareEndOfPeriodArgs() {} };
  const collection = { _loadingContent: true };
  global.window = { TradingViewApi: { chart: () => api }, _exposed_chartWidgetCollection: collection };
  try {
    assert.equal(await readCandleHistory(anchor), null);
    collection._loadingContent = false;
    widget._inLoadingState = true;
    assert.equal(await readCandleHistory(anchor), null);
    assert.equal(opened, 0);
    widget._inLoadingState = false;

    const start = Date.now();
    assert.equal(await readCandleHistory(anchor), null);
    assert.equal(await readCandleHistory(anchor), null);
    assert.equal(opened, 1);
    assert.equal(destroyed, 1);
    const cache = window.__isccCandleHistoryCache;
    const miss = cache.get(JSON.stringify(anchor));
    assert.ok(miss.retryAt >= start + 30000 && miss.retryAt <= Date.now() + 30000);

    miss.retryAt = 0;
    plots = [{ value: [anchor.time, 1, 2, 1] }];
    const result = await readCandleHistory(anchor, () => time => time + 60);
    assert.deepEqual(result.bars, [[anchor.time, 2, 1]]);
    assert.deepEqual(result.windows, [[anchor.time, anchor.time + 60]]);
    assert.equal(opened, 2);
    assert.equal(destroyed, 2);

    plots = undefined;
    for (let index = 1; index <= 257; index++) {
      assert.equal(await readCandleHistory({ ...anchor, time: anchor.time + index * 60 }), null);
    }
    assert.equal(cache.size, 256);
    assert.equal(destroyed, opened);
  } finally { delete global.window; }
});
