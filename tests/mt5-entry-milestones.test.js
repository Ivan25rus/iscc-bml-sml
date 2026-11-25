const test = require('node:test');
const assert = require('node:assert/strict');
const {createEntryMilestones} = require('../payload/src/app/services/mt5-vps/entryMilestones');

test('Click-to-entry timing reports each confirmed fill and completion once', () => {
  let now = 120;
  const events = [];
  const tracker = createEntryMilestones({log: row => events.push(row), wall: () => 10020, clock: () => now});
  const parent = {id:'plan-1',symbol:'USTEC',createdAt:10030,phase:'armed',accounts:{
    900000003:{filledVolume:0,targetVolume:1},900000004:{filledVolume:0,targetVolume:0.1}
  }};
  tracker.start({requestId:parent.id,ticker:parent.symbol,clickAt:10000});
  tracker.result(parent.id,{entry:{...parent,createdAt:undefined}});
  assert.equal(events.length,0);
  now = 170;
  assert.equal(tracker.observeMany([parent])[0],parent);
  tracker.result(parent.id,{entry:parent});
  now = 210;
  parent.accounts[900000003].filledVolume = 0.5;
  tracker.observeMany([parent]);
  now = 250;
  parent.accounts[900000004].filledVolume = 0.1;
  tracker.observeMany([parent]);
  now = 300;
  parent.accounts[900000003].filledVolume = 1;
  parent.phase = 'completed';
  tracker.observeMany([parent,parent]);
  assert.deepEqual(events.map(event => [event.stage,event.account,event.elapsedMs]),[
    ['accepted',undefined,70],['first-fill','900000003',110],
    ['first-fill','900000004',150],['target-filled','900000004',150],
    ['target-filled','900000003',200],['completed',undefined,200]
  ]);
});

test('Stopped SPX entries do not appear fully filled and early cancellation is not accepted', () => {
  const events = [];
  const tracker = createEntryMilestones({log: row => events.push(row.stage), wall: () => 10000, clock: () => 100});
  const click = {ticker:'SPX500',clickAt:10000};
  tracker.start({...click,requestId:'early-stop'});
  tracker.result('early-stop',{entry:{id:'early-stop',cancelledBeforeAdmission:true}});
  tracker.start({...click,requestId:'partial-stop'});
  tracker.result('partial-stop',{entry:{id:'partial-stop',symbol:'SPX500',createdAt:10001,phase:'stopped',
    accounts:{900000003:{filledVolume:0.1,targetVolume:0.3}}}});
  assert.deepEqual(events,['cancelled-before-admission','accepted','first-fill','stopped']);
});

test('Finished-account timing reaches the journal once across the list and event feeds', () => {
  const events = [];
  const tracker = createEntryMilestones({log: row => events.push(row)});
  const timing = {schema:'entry-timing-summary-v1',waitSummary:{observedMs:7000},
    fills:{firstConfirmedAtMs:8000},stopConfirmedAtMs:8120};
  const entry = {id:'another-device',symbol:'SPX500',createdAt:1000,phase:'running',
    lanePhases:{900000003:'completed',900000001:'stopped'},accounts:{
      900000003:{filledVolume:1,targetVolume:1,entryTimingSummary:timing},
      900000001:{filledVolume:0,targetVolume:0.4}
    }};
  tracker.observeMany([entry]);
  assert.deepEqual(events, []);
  const finished = {...entry,phase:'stopped'};
  assert.equal(tracker.observeMany([finished])[0],finished);
  tracker.result(finished.id,{entry:finished});
  assert.deepEqual(events,[{kind:'chunk-entry-timing-summary',entryId:finished.id,symbol:'SPX500',
    account:'900000003',admittedAtMs:1000,outcome:'completed',filledVolume:1,targetVolume:1,timing}]);
});
