"""Friday incident regressions: synthetic SDK and temporary ledgers only."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_account_controls as controls
import test_mt5_bridge as native_fixture
import test_entry_check_batch as management_fixture
from chunk_entry import ChunkEntries,SingleAccountLane


def command_budget(send=None, initial=True, commit_seconds=2.1):
    """Exercise the real reservation/send method with a measured fake clock."""
    clock=[100.];captured=[];claims=[]
    lane=object.__new__(SingleAccountLane)
    lane.lane_account='900000001';lane.lane_slot=1
    lane.bridge=N(config={'account':900000001})
    lane._stopping=lambda identifier:False
    lane._hybrid_stop=lambda *args:84.
    lane._hybrid_tp=lambda *args:0.
    lane._save=lambda state:None
    def phase(state,value,reason):
        state['phase']=value
        if value=='sending':claims.append(copy.deepcopy(state));clock[0]+=commit_seconds
    lane._phase=phase
    def call(account,method,operation,request,*native_options):
        captured.append((copy.deepcopy(request),native_options))
        return dict(result=dict(status='rejected',reason='Test stops before a broker call'))
    lane._call=call
    state=dict(id='expiry-parent',createdAt=100000.,phase='running',accounts={'900000001':{}},children=[])
    payload=dict(accountExecution='independent-v4',executionPolicy='hybrid-market-limit-v1',
        initialSizingOnly=initial,symbol='SPX500',side='buy',closeAtUtc=200000.,
        closeTimeZone='America/New_York',closeLocalDate='2026-10-30',closeLocalTime='15:59',cancelPendingAtDeadline=True)
    with patch('time.time',side_effect=lambda:clock[0]):
        (send or ChunkEntries._hybrid_send)(lane,payload,state,'limit',{'900000001':100.},{'900000001':.8},{'900000001':{}})
    request,native_options=captured[0]
    return dict(remainingMs=request['expiresAt']-clock[0]*1000,request=request,claims=claims,nativeOptions=native_options)


class NativeExpiryTests(unittest.TestCase):
    def test_new_unsent_child_keeps_three_seconds_after_durable_parent_claim(self):
        result=command_budget()
        self.assertEqual(result['remainingMs'],3000)
        self.assertEqual(result['claims'][0]['children'][0]['state'],'sending')
        self.assertEqual(result['nativeOptions'],(None,True))

    def test_legacy_captured_policy_keeps_original_deadline(self):
        result=command_budget(initial=False)
        self.assertAlmostEqual(result['remainingMs'],900)
        self.assertEqual(result['nativeOptions'],())

    def test_only_proven_unsent_expiry_allows_a_new_checked_intent(self):
        payload=dict(initialSizingOnly=True,accountExecution='independent-v4')
        result=dict(status='rejected',retryableExpiry=True,mutationStarted=False,finalPreMutationRejection=True)
        self.assertTrue(ChunkEntries._retryable_expiry_refusal(payload,result))
        for change in (dict(status='unknown'),dict(mutationStarted=True),dict(finalPreMutationRejection=False),
                       dict(providerOrderId='77'),dict(filledVolume=.1),dict(retcode=10012)):
            self.assertFalse(ChunkEntries._retryable_expiry_refusal(payload,dict(result,**change)))
        self.assertFalse(ChunkEntries._retryable_expiry_refusal(dict(payload,initialSizingOnly=False),result))

    def test_expiry_after_native_durable_claim_is_known_unsent_and_never_replayed(self):
        fixture=native_fixture.BridgeTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        original=fixture.bridge.db;clock=[100.]
        class Database:
            def __getattr__(self,name):return getattr(original,name)
            def commit(self):original.commit();clock[0]+=2.1
        fixture.bridge.db=Database()
        with patch('time.time',side_effect=lambda:clock[0]):
            result=fixture.bridge.place(dict(fixture.order,expiresAt=102000.))['result']
            self.assertEqual(result['status'],'rejected')
            self.assertFalse(result['mutationStarted']);self.assertTrue(result['finalPreMutationRejection'])
            self.assertFalse(result['retryableExpiry'],'Ordinary orders do not opt into parent acquisition')
            self.assertEqual(fixture.mt5.sent,[])
            fixture.bridge.place(dict(fixture.order,expiresAt=clock[0]*1000+3000))
            self.assertEqual(fixture.mt5.sent,[],'An expired saved ID cannot be renewed or replayed')


class ClosedPositionRecoveryTests(unittest.TestCase):
    def model(self):
        helper=controls.AccountControlTests();self.addCleanup(helper.doCleanups)
        f,root=helper.model()
        state=f.engine._load(f.payload['id'])[2]
        captured={account:{child['positions'][account]['ticket']:child['positions'][account]
            for child in state['children'] if account in child.get('positions',{})} for account in ('900000002','900000001')}
        # Enrich the old SDK fake with the actual deal/order fields required
        # by the strict production balanced-opening validator.
        for account,native in zip(('900000002','900000001'),f.natives):
            for order in native.history_orders:
                position=captured[account].get(str(order.ticket))
                if position:order.type=0;order.volume_initial=position['volume'];order.sl=position['sl'];order.tp=position['tp']
            for index,deal in enumerate(native.history_deals):
                position=captured[account].get(str(deal.position_id))
                if position:deal.ticket=deal.position_id*10+index+1;deal.order=deal.position_id;deal.type=0;deal.price=position['fillPrice']
        original=f.natives[1].order_send
        def freeze(request):
            if request['action']==f.natives[1].TRADE_ACTION_DEAL and 'position' in request:
                f.natives[1].sent.append(copy.deepcopy(request))
                return N(retcode=10029,order=0,volume=0.,price=0.,comment='frozen')
            return original(request)
        f.natives[1].order_send=freeze
        command=dict(id='friday-frozen-close',entryId=f.payload['id'],symbol='USTEC',expiresAt=time.time()*1000+3000)
        response=f.engine.close_position(command)
        self.assertEqual(response['result']['status'],'unknown')
        self.assertTrue(f.natives[1].positions)
        return f,root,command,captured

    def broker_exit(self,f,captured,change=None):
        native=f.natives[1]
        for index,position in enumerate(list(native.positions)):
            opening=captured['900000001'][str(position.ticket)]
            exit=N(ticket=position.ticket*100+index+1,order=999999+index,position_id=position.identifier,
                symbol='USTEC',entry=1,volume=position.volume,price=opening['sl'],type=1,reason=4)
            if change:change(exit)
            native.history_deals.append(exit)
        native.positions.clear()

    def test_broker_stop_exits_retire_parent_without_rewriting_failed_command_or_sending(self):
        f,root,command,captured=self.model();self.broker_exit(f,captured)
        before=[len(native.sent) for native in f.natives]
        receipt=copy.deepcopy(f.engine._closure_receipt(command['id']))
        local=f.engine.shards['900000001'];pending=local._load(f.payload['id'])[2]['closurePending']
        local_receipt=copy.deepcopy(local._closure_receipt(pending))
        recovered=f.engine.get(f.payload['id'])
        self.assertEqual(recovered['phase'],'stopped');self.assertTrue(recovered['positionsClosed'])
        self.assertEqual(recovered['positionLifecycle'],'closed');self.assertFalse(recovered['hasOpenPositions'])
        self.assertNotIn('closurePending',recovered['accounts']['900000001'])
        self.assertEqual(f.engine._closure_receipt(command['id']),receipt)
        self.assertEqual(local._closure_receipt(pending),local_receipt)
        self.assertEqual([len(native.sent) for native in f.natives],before)
        with patch.object(local,'_call',side_effect=AssertionError('Closed proof must not poll again')):
            self.assertEqual(f.engine.get(f.payload['id'])['positionLifecycle'],'closed')

    def test_live_or_absent_without_exact_history_never_retires_parent(self):
        for remove in (False,True):
            f,root,command,captured=self.model()
            if remove:f.natives[1].positions.clear()
            before=[len(native.sent) for native in f.natives]
            self.assertEqual(f.engine.get(f.payload['id'])['phase'],'unknown')
            self.assertEqual([len(native.sent) for native in f.natives],before)

    def test_wrong_exit_side_and_unbalanced_or_wrong_opening_price_do_not_pass(self):
        for change in (lambda row:setattr(row,'type',0),lambda row:setattr(row,'volume',row.volume/2)):
            f,root,command,captured=self.model();self.broker_exit(f,captured,change)
            self.assertEqual(f.engine.get(f.payload['id'])['phase'],'unknown')
        f,root,command,captured=self.model();self.broker_exit(f,captured)
        for deal in f.natives[1].history_deals:
            if deal.entry==0:deal.price+=.1
        self.assertEqual(f.engine.get(f.payload['id'])['phase'],'unknown')

    def test_wrong_native_account_and_unresolved_remainder_remain_blocked(self):
        f,root,command,captured=self.model();self.broker_exit(f,captured);f.natives[1].account.login=999
        self.assertEqual(f.engine.get(f.payload['id'])['phase'],'unknown')
        f,root,command,captured=self.model();self.broker_exit(f,captured)
        local=f.engine.shards['900000001'];state=local._load(f.payload['id'])[2]
        state['children'][0]['state']='unknown';local._save(state)
        self.assertEqual(f.engine.get(f.payload['id'])['phase'],'unknown')


class ManagementProofHandoffTests(unittest.TestCase):
    def model(self,delay):
        helper=management_fixture.EntryCheckBatchTests();self.addCleanup(helper.doCleanups)
        case,f,root=helper.model();calls=[]
        for account,worker in [('900000002',root.primary_worker),('900000001',root.linked['900000001'])]:
            def before(method,args,account=account):
                if method=='management_snapshot':
                    calls.append(account)
                    if isinstance(delay,threading.Event):delay.wait(3)
                    else:time.sleep(delay)
            worker.before=before
        return f,root,calls

    def test_ready_exact_active_proof_uses_same_two_reads_in_first_reply(self):
        f,root,calls=self.model(.05)
        watch=[dict(account=a,ticket='999',symbol='USTEC') for a in ('900000001','900000002')]
        states=root.management_accounts(watch=watch,compact=True)
        self.assertTrue(all(row['evidenceReady'] for row in states.values()))
        self.assertCountEqual(calls,['900000002','900000001'])
        self.assertFalse(any(native.sent for native in f.natives))

    def test_slow_proof_stays_unready_with_bounded_wait_and_ordinary_refresh_unchanged(self):
        from concurrent.futures import wait
        release=threading.Event();f,root,calls=self.model(release)
        self.addCleanup(release.set)
        # Keep native reads blocked beyond the UI handoff. Windows scheduling
        # can overshoot a 100ms wait; verify its actual contract separately
        # from a generous wall-clock deadlock guard.
        try:
            with patch('concurrent.futures.wait',wraps=wait) as handoff:
                began=time.perf_counter();states=root.management_accounts(watch=[dict(account='900000001',ticket='999',symbol='USTEC')],compact=True)
                self.assertEqual(handoff.call_args.kwargs['timeout'],.1)
            self.assertLess(time.perf_counter()-began,1)
        finally:release.set()
        self.assertTrue(all(not row['evidenceReady'] for row in states.values()))
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        release.clear()  # A new watch signature needs its own still-pending proof.
        try:
            with patch('concurrent.futures.wait',wraps=wait) as handoff:
                began=time.perf_counter();states=root.management_accounts(compact=True)
                self.assertEqual(handoff.call_args.kwargs['timeout'],.02)
        finally:release.set()
        self.assertLess(time.perf_counter()-began,1)
        self.assertFalse(states['900000001']['evidenceReady'],'A new query cannot reuse another watch proof')
        self.assertFalse(any(native.sent for native in f.natives))


if __name__=='__main__':unittest.main()
