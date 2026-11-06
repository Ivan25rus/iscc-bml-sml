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
