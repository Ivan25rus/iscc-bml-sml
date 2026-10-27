const test=require('node:test');
const assert=require('node:assert/strict');
const {CommandJournal}=require('../payload/src/app/services/mt5-vps/commandJournal');
function fixture() {
  const journal=Object.create(CommandJournal.prototype);
  journal.records=[{id:'old',ticket:'entry:old',result:{status:'unknown'}},
    {id:'current',ticket:'entry:current',result:{status:'unknown'}}];
  journal.save=async()=>{};
  return journal;
}
test('Scoped reconciliation never reads or releases unrelated unknown commands',async()=>{
  const journal=fixture(),seen=[];
  await journal.reconcile(async id=>{seen.push(id);if(id==='old')throw Error('Unrelated receipt unavailable');return {status:'ok'};},{ticket:'entry:current'});
  assert.deepEqual(seen,['current']);
  assert.equal(journal.records[0].result.status,'unknown');
  assert.equal(journal.records[1].result.status,'ok');
});
test('Unknown current-scope receipt remains unknown, and no other scope is reconciled',async()=>{
  const journal=fixture(),seen=[];
  await journal.reconcile(async id=>{seen.push(id);return {status:'unknown'};},{ticket:'entry:current'});
  assert.deepEqual(seen,['current']);assert.ok(journal.records.every(row=>row.result.status==='unknown'));
});
test('Default reconciliation still reads all unknown commands',async()=>{
  const journal=fixture(),seen=[];
  await journal.reconcile(async id=>{seen.push(id);return {status:'rejected'};});
  assert.deepEqual(seen,['old','current']);assert.ok(journal.records.every(row=>row.result.status==='rejected'));
});
