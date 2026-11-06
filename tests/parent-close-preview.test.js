const test=require('node:test');
const assert=require('node:assert/strict');
const {parentClosePreview}=require('../payload/src/app/services/mt5-vps/parentClosePreview');
function fixture(){
  const row={ticket:'1',symbol:'USTEC',type:'buy',lots:.3,__isPosition:true,quantityStep:.1,minQty:.1,maxQty:100,
    entryPlan:{id:'parent',accounts:['900000002','900000001']}};
  const state=lots=>({connected:true,evidenceReady:true,orders:lots.map((n,i)=>({...row,ticket:String(i+1),lots:n}))});
  return {row,states:{900000002:state([.3,.2]),900000001:state([.8])}};
}
test('Percentage preview totals every parent ticket on each account and ignores unrelated positions',()=>{
  const f=fixture();f.states[900000002].orders.push({...f.row,ticket:'9',lots:99,entryPlan:{id:'other'}});
  const result=parentClosePreview(f.row,'900000002',f.states,'percent',50);
  assert.equal(result.percent,50);
  assert.deepEqual(result.preview,[{account:'900000002',volume:.2,remaining:.3,step:.1},{account:'900000001',volume:.4,remaining:.4,step:.1}]);
  assert.deepEqual(parentClosePreview(f.row,'900000001',f.states,'percent',50),result);
});
test('A custom size means a fraction of the whole selected parent and scales each linked remainder',()=>{
  const f=fixture(),result=parentClosePreview(f.row,'900000002',f.states,'lots',.2);
  assert.equal(result.percent,40);assert.equal(result.preview[1].volume,.3);
});
test('Missing or repeated account evidence cannot silently become a selected-ticket preview',()=>{
  const f=fixture();f.states[900000001].evidenceReady=false;
  assert.ok(parentClosePreview(f.row,'900000002',f.states,'percent',50).preview[1].error);
  f.states[900000001].evidenceReady=true;f.states[900000002].orders.push(f.states[900000002].orders[0]);
  assert.ok(parentClosePreview(f.row,'900000002',f.states,'percent',50).preview[0].error);
});
