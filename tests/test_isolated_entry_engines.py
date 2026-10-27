"""Independent planning ledgers, real state machine, synthetic MT5 only."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as fixture
from chunk_entry import IsolatedEntries, ChunkEntries, SingleAccountLane
from mt5_bridge import CoordinatorBridge,worker_result


class IsolatedEngineTests(unittest.TestCase):
    def make(self, side='buy'):
        f=fixture.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        f.make(side,2)
        f.engine.close()
        f.engine=IsolatedEntries(f.primary);f.primary.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        f.payload.update(accountExecution='independent-v5',chunkPercent=25,chunkVolume=.25,fillPriority='risk')
        for native in f.natives:native.info.volume_step=native.info.volume_min=.01
        return f

    def until(self,f,predicate,timeout=None):
        timeout=20 if sys.platform=='win32' else 6 if timeout is None else timeout
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            f.engine.tick();state=f.state()
            if predicate(state):return state
            time.sleep(.002)
        self.fail(repr(state))

    def test_both_sides_complete_with_distinct_ids_and_journals(self):
        for side in ('buy','sell'):
            f=self.make(side);f.engine.arm(f.payload)
            state=self.until(f,lambda s:s['phase']=='completed')
            self.assertEqual([x['filledVolume'] for x in state['accounts'].values()],[1,1])
            self.assertEqual(len({x['id'] for x in state['children']}),len(state['children']))
            paths=[engine.bridge.db.execute('PRAGMA database_list').fetchone()[2] for engine in f.engine.shards.values()]
            self.assertEqual(len(set(paths)),2)
            self.assertTrue(all(engine._load(f.payload['id'])[1]['initialSizingOnly'] for engine in f.engine.shards.values()))

    def test_slow_admission_does_not_hold_peer_or_root_lock(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        original=f.primary.linked['900000001'].call
        def call(method,*args):
            if method=='entry_admission':entered.set();release.wait(3)
            return original(method,*args)
        f.primary.linked['900000001'].call=call
        started=time.monotonic();f.engine.arm(f.payload)
        self.assertLess(time.monotonic()-started,.5)
        try:
            state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']==1)
            self.assertTrue(entered.is_set());self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        finally:release.set()
        self.until(f,lambda s:s['phase']=='completed')

    def test_stop_and_protection_keep_exact_parent_ownership(self):
        f=self.make();f.engine.arm(f.payload)
        self.until(f,lambda s:s['phase']=='completed')
        response=f.engine.protection(f.command(sl=24991.,mirrorAccounts=[dict(account='900000001')]))
        self.assertEqual(response['result']['status'],'ok',response)
        self.assertTrue(all(row.sl==24991. for native in f.natives for row in native.positions))
        result=f.engine.stop(dict(id='stop-isolated',entryId=f.payload['id'],expiresAt=time.time()*1000+3000))
        self.assertNotIn(result['phase'],('armed','running','sending','paused'))
        self.assertTrue(all(len(native.positions)==4 for native in f.natives))

    def test_restart_does_not_rearm_and_keeps_exact_fills(self):
        f=self.make();f.engine.arm(f.payload)
        before=self.until(f,lambda s:s['phase']=='completed')
        f.engine.close();f.engine=IsolatedEntries(f.primary);f.primary.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        after=f.state()
        self.assertEqual(after['phase'],'completed')
        self.assertEqual(after['children'],before['children'])

    def test_peer_fills_while_other_account_journal_is_held(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        original=ChunkEntries._save
        def save(engine,state,receipt=None):
            if getattr(engine,'account_id',None)=='900000001' and any(row['state']=='sending' for row in state['children']):
                entered.set();release.wait(3)
            return original(engine,state,receipt)
        with patch.object(ChunkEntries,'_save',save):
            f.engine.arm(f.payload)
            try:
                state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']==1)
                self.assertTrue(entered.is_set());self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
            finally:release.set()
            self.until(f,lambda s:s['phase']=='completed')

    def test_no_monetary_reservation_limits_target_parts(self):
        f=self.make();f.engine.arm(f.payload)
        self.until(f,lambda s:bool(f.engine.account_states))
        engine=next(iter(f.engine.shards.values()))
        found=engine._load(f.payload['id'])
        while not found:
            time.sleep(.002);found=engine._load(f.payload['id'])
        payload,state=found[1:];account=engine.primary
        state['accounts'][account]['riskUsed']=1000
        self.assertEqual(engine._priority_volume(payload,state,account,'market',25002.,.25,{account:1000}),.25)
        guards=engine._hybrid_guards(payload,state,{account:25002.},{account:.25},{account:1000})
        self.assertGreater(guards[account]['riskBudget'],0)

    def test_stopping_during_admission_cannot_send_a_late_part(self):
        f=self.make();f.engine.arm(f.payload)
        f.engine.stop(dict(id='stop-before-tick',entryId=f.payload['id'],expiresAt=time.time()*1000+3000))
        for _ in range(5):f.engine.tick()
        self.assertFalse(any(native.sent for native in f.natives))

    def test_primary_connector_facade_and_independent_management(self):
        f=self.make();f.engine.close()
        class Worker:
            def call(self,method,*args):
                with f.primary.lock:
                    return worker_result(method,getattr(f.primary,method)(*args))
        coordinator=CoordinatorBridge(f.primary.config,f.paths[0],Worker())
        class QuoteClient(Worker):
            def __init__(self,config,path):pass
            def available(self):return True
            def rpc_snapshot(self):return {}
            def close(self):pass
        coordinator.quote_worker_factory=QuoteClient
        self.addCleanup(coordinator.db.close)
        self.addCleanup(coordinator.close_group_executor)
        self.addCleanup(coordinator.chunk_entries.close)
        coordinator.linked=f.primary.linked
        coordinator.start_rules()
        self.assertIsNone(coordinator.mt5)
        self.assertIn('independent-account-engines-v5',coordinator.health()['capabilities'])
        self.assertEqual(coordinator.snapshot('USTEC')['account'],900000002)
        views=coordinator.management_accounts()
        self.assertTrue(views['900000002']['connected']);self.assertTrue(views['900000001']['connected'])
        self.assertIn('independent-account-engines-v5',coordinator.management()['capabilities'])
        self.assertIsNone(coordinator.chunk_entries.progress_observer.snapshot())
        coordinator.frames=3
        self.assertEqual(coordinator.diagnostics()['quoteStream']['frames'],3)
        f.engine=coordinator.chunk_entries
        f.engine.arm(f.payload)
        self.until(f,lambda s:s['phase']=='completed')

    def test_native_protection_guard_rejects_wrong_side_and_broker_minimum_without_send(self):
        f=self.make();native=f.natives[0];bridge=f.primary
        payload=dict(id='native-sl',symbol='USTEC',side='buy',type='market',volume=.2,price=25002.,sl=25001.9,tp=0.,
            expiresAt=time.time()*1000+3000,entryGuard=dict(anchor=25002.,maxGap=1.,maxRiskPrice=25003.,positions=[]))
        result=bridge.execute('place',payload)['result']
        self.assertEqual(result['status'],'rejected');self.assertFalse(native.sent)
        native.info.trade_stops_level=200;native.info.point=.1
        result=bridge.execute('place',dict(payload,id='native-minimum',sl=24986.))['result']
        self.assertEqual(result['status'],'rejected');self.assertFalse(native.sent)

    def test_unknown_cancellation_keeps_its_reservation_and_does_not_block_peer(self):
        f=self.make();f.natives[0].cancel_unknown=True
        f.engine.arm(f.payload)
        state=self.until(f,lambda s:s['accounts']['900000001']['filledVolume']==1)
        self.assertEqual(state['accounts']['900000002']['filledVolume'],.25)
        self.assertGreater(state['accounts']['900000002']['pendingVolume'],0)
        self.assertEqual(len(f.natives[0].positions),1)

    def test_proven_empty_rejections_do_not_exhaust_ledger_or_reuse_ids(self):
        f=self.make();f.engine.arm(f.payload)
        for job in f.engine.admitting.values():job.result(timeout=5)
        engine=f.engine.shards['900000002'];_,payload,state=engine._load(f.payload['id'])
        lane=SingleAccountLane(engine,'900000002',0);ids=[]
        result=dict(result=dict(status='rejected',retcode=10004,reason='quote changed'))
        with patch('chunk_entry.HYBRID_MAX_SUBMISSIONS',16),patch.object(lane,'_call',return_value=result):
            for _ in range(32):
                guard=lane._hybrid_guards(payload,state,{'900000002':25002.},{'900000002':.25})
                lane._hybrid_send(payload,state,'market',{'900000002':25002.},{'900000002':.25},guard)
                ids.append(state['children'][-1]['id'])
        self.assertEqual(len(set(ids)),32);self.assertLessEqual(len(state['children']),9)
        self.assertEqual(engine._load(f.payload['id'])[2]['accounts']['900000002']['submissionSerial'],32)
        self.assertFalse(any(native.sent for native in f.natives))
