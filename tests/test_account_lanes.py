"""Independent account execution through real coordinator, fake MT5 only."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as protection

class AccountLaneTests(unittest.TestCase):
    def test_admission_does_not_queue_behind_saturated_lane_executor(self):
        f=self.fixture();release=threading.Event();self.addCleanup(release.set)
        executor=f.primary.warm_group_executor()
        barrier=threading.Barrier(18)
        def occupied():
            barrier.wait(timeout=2);release.wait(2)
        jobs=[executor.submit(occupied) for _ in range(17)]
        barrier.wait(timeout=2)
        try:
            started=time.monotonic();f.engine.arm(f.payload)
            self.assertLess(time.monotonic()-started,1)
            self.assertFalse(any(native.sent for native in f.natives))
        finally:
            release.set()
            for job in jobs:job.result(timeout=2)

    def test_impossible_unchanged_limit_does_not_consume_budget_with_fresh_ticks(self):
        f=self.fixture()
        for broker in f.natives:
            f.quote(broker,25006.1)
            broker.info.trade_stops_level=70
            broker.info.point=.1
        f.engine.arm(f.payload)
        self.until(f,lambda s:len(s['children'])>=2)
        f.engine._drain_lanes(f.payload['id'])
        before=len(f.state()['children'])
        for index in range(1,10):
            time.time.return_value=f.now+index*.3
            for broker in f.natives:broker.tick.time=f.now+index*.3
            f.tick();f.engine._drain_lanes(f.payload['id'])
        self.assertEqual(len(f.state()['children']),before)
        self.assertFalse(any(broker.sent for broker in f.natives))

    def test_definite_localized_limit_price_refusal_does_not_block_market(self):
        for side in ('buy', 'sell'):
            f=self.fixture(side);native=f.natives[0];original=native.order_send;refused=[]
            def send(request):
                if request['action']==native.TRADE_ACTION_PENDING and not refused:
                    refused.append(dict(request))
                    return SimpleNamespace(retcode=10015,comment='Недопустимая цена',order=0,volume=0.,price=0.)
                return original(request)
            native.order_send=send;f.engine.arm(f.payload)
            self.until(f,lambda s:s['phase']=='completed')
            self.assertEqual(len(refused),1)
            for broker in f.natives:
                self.assertAlmostEqual(sum(row.volume for row in broker.positions),1)
                self.assertFalse(broker.orders)

    def test_requote_with_fresh_unchanged_prices_resumes_without_support_churn(self):
        f=self.fixture();native=f.natives[0];original=native.order_send;refused=[]
        def send(request):
            if request['action']==native.TRADE_ACTION_DEAL and 'position' not in request and not refused:
                refused.append(dict(request))
                return SimpleNamespace(retcode=10004,comment='Перекотировка',order=0,volume=0.,price=0.)
            return original(request)
        native.order_send=send;f.engine.arm(f.payload)
        self.until(f,lambda s:bool(refused))
        # Let the original failed round publish its exact receipt.
        f.engine._drain_lanes(f.payload['id'])
        for _ in range(12):f.tick();time.sleep(.002)
        self.assertEqual(sum(row['action']==native.TRADE_ACTION_PENDING for row in native.sent),1)
        self.assertFalse(any(row['action']==native.TRADE_ACTION_REMOVE for row in native.sent))
        time.time.return_value=f.now+.3
        for broker in f.natives:broker.tick.time=f.now+.3
        self.until(f,lambda s:s['phase']=='completed')
        self.assertAlmostEqual(sum(row.volume for row in native.positions),1)
        self.assertLessEqual(sum(row['action']==native.TRADE_ACTION_PENDING for row in native.sent),4)

    def test_risk_or_protection_refusal_never_falls_through_to_market(self):
        f=self.fixture();native=f.natives[0];original=native.order_send
        def send(request):
            if request['action']==native.TRADE_ACTION_PENDING:
                return SimpleNamespace(retcode=10016,comment='Invalid stops; quote changed',order=0,volume=0.,price=0.)
            return original(request)
        native.order_send=send;f.engine.arm(f.payload)
        state=self.until(f,lambda s:s['phase']=='stopped')
        self.assertEqual(state['accounts']['900000002']['filledVolume'],0)
        self.assertFalse(native.sent)
        self.assertIn('Invalid stops',state['accounts']['900000002']['entryReason'])

    def test_admission_reads_and_checks_overlap_without_early_submission(self):
        f=self.fixture();original=f.engine._call
        barriers={method:threading.Barrier(2) for method in ('snapshot','check_opening')}
        observed=[]
        def call(account,method,*args,**kwargs):
            if method in barriers:
                self.assertFalse(any(native.sent for native in f.natives))
                barriers[method].wait(timeout=1)
                observed.append((account,method))
            return original(account,method,*args,**kwargs)
        f.engine._call=call;f.engine.arm(f.payload)
        self.assertEqual(len(observed),4)

    def test_combined_parent_budget_remains_readable_at_exhaustion(self):
        import chunk_entry
        f=self.fixture()
        with patch.object(chunk_entry,'HYBRID_MAX_SUBMISSIONS',8):
            f.engine.arm(f.payload)
            state=self.until(f,lambda s:s['phase'] not in ('armed','running','paused','sending'))
            self.assertLessEqual(len(state['children']),8)
            for lane in f.engine.lanes.values():self.assertEqual(lane.lane_submission_limit,4)
            self.assertLessEqual(len(f.state()['children']),8)
        for broker in f.natives:self.assertLessEqual(sum(row.volume for row in broker.positions),1)

    def fixture(self,side='buy'):
        f=protection.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        f.make(side,2);f.payload.update(accountExecution='independent-v4',chunkPercent=25,chunkVolume=.25)
        for native in f.natives:native.info.volume_step=native.info.volume_min=.01
        return f

    def until(self,f,predicate,timeout=None):
        # Functional convergence includes two SQLite FULL journals. Windows
        # host contention must not masquerade as a state-machine failure.
        # Explicit dispatch/arm latency assertions remain separate below.
        if timeout is None:timeout=12 if sys.platform=='win32' else 4
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            state=f.tick()
            if predicate(state):return state
            time.sleep(.002)
        self.fail(repr(state))

    def test_both_sides_ladder_volume_risk_and_protection(self):
        for side in ('buy','sell'):
            with self.subTest(side=side):
                f=self.fixture(side);f.engine.arm(f.payload)
                state=self.until(f,lambda s:s['phase'] not in ('armed','running','sending','paused'))
                self.assertEqual(state['phase'],'completed',state)
                for native in f.natives:
                    self.assertAlmostEqual(sum(row.volume for row in native.positions),1)
                    self.assertFalse(native.orders)
                    self.assertEqual(len({row.sl for row in native.positions}),1)
                children=f.engine._load(f.payload['id'])[2]['children']
                self.assertEqual(len({child['id'] for child in children}),len(children))

    def test_slow_follower_response_does_not_block_primary_completion(self):
        f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def call(method,*args):
            result=original(method,*args)
            if method=='execute' and args[0]=='place' and args[1]['type']=='limit' and not entered.is_set():
                entered.set();release.wait(2)
            return result
        worker.call=call;f.engine.arm(f.payload)
        try:
            state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']>=1)
            self.assertTrue(entered.is_set())
            self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        finally:release.set()
        state=self.until(f,lambda s:s['phase']=='completed')
        self.assertEqual(state['phase'],'completed')

    def test_primary_native_delay_does_not_hold_follower_journal(self):
        f=self.fixture();native=f.natives[0];original=native.order_send
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def send(request):
            if not entered.is_set():entered.set();release.wait(2)
            return original(request)
        native.order_send=send;f.engine.arm(f.payload)
        try:
            state=self.until(f,lambda s:s['accounts']['900000001']['filledVolume']>=1)
            self.assertTrue(entered.is_set());self.assertEqual(state['accounts']['900000002']['filledVolume'],0)
        finally:release.set()
        self.until(f,lambda s:s['phase']=='completed')

    def test_outside_price_relocates_only_exact_residual(self):
        f=self.fixture()
        for native in f.natives:
            native.after_market=lambda broker:f.quote(broker,25010.)
            native.partial_cancel_volume=.2
        f.engine.arm(f.payload)
        state=self.until(f,lambda s:all(row['filledVolume']>=.45 and row['pendingVolume']>0 for row in s['accounts'].values())
            and all(native.orders and abs(native.orders[0].price_open-25006.)<1e-8 for native in f.natives))
        for row in state['accounts'].values():self.assertAlmostEqual(row['filledVolume']+row['pendingVolume'],1)
        for native in f.natives:self.assertAlmostEqual(native.orders[0].price_open,25006.)

    def test_stop_drains_lanes_and_retains_existing_positions(self):
        f=self.fixture()
        for native in f.natives:native.after_market=lambda broker:f.quote(broker,25010.)
        f.engine.arm(f.payload)
        self.until(f,lambda s:all(row['filledVolume']>=.25 and row['pendingVolume']>.1 for row in s['accounts'].values()))
        before=[sum(row.volume for row in n.positions) for n in f.natives]
        result=f.engine.stop(dict(id='lane-stop',entryId=f.payload['id'],expiresAt=f.now*1000+3000))
        self.assertEqual(result['phase'],'stopped',result)
        for n,volume in zip(f.natives,before):
            self.assertFalse(n.orders);self.assertAlmostEqual(sum(row.volume for row in n.positions),volume)
        sends=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(5):f.tick()
        self.assertEqual([n.sent for n in f.natives],sends)

    def test_combined_context_proof_precedes_quote(self):
        f=self.fixture();bridge=f.primary;order=[]
        management,snapshot=bridge._entry_positions_for,bridge.entry_snapshot
        bridge._entry_positions_for=lambda watch,account:(order.append('proof') or management(watch,account))
        bridge.entry_snapshot=lambda symbol:(order.append('quote') or snapshot(symbol))
        data=bridge.entry_context('USTEC',[dict(ticket='100',symbol='USTEC')])
        self.assertEqual(order,['proof','quote']);self.assertIn('contextMs',data)

    def test_wait_trace_is_bounded_and_uses_monotonic_time(self):
        f=self.fixture();f.engine.arm(f.payload)
        self.until(f,lambda s:s['phase']=='completed')
        state=f.engine.get(f.payload['id'])
        for row in state['accounts'].values():
            trace=row['entryTiming']['recentStages'];self.assertLessEqual(len(trace),48)
            self.assertTrue(all(item.get('sinceAccountStartMs',0)>=0 and item['durationMs']>=0 for item in trace))

    def test_event_read_is_deferred_and_never_reads_native_state(self):
        f=self.fixture();f.engine.arm(f.payload)
        before=copy.deepcopy([n.sent for n in f.natives])
        view=f.engine.entry_events(-1,timeout=0)
        self.assertTrue(all(row['lifecycleDeferred'] and not row['positionsConfirmed'] for row in view['entries']))
        self.assertEqual([n.sent for n in f.natives],before)

    def test_unknown_history_lookup_uses_partial_index(self):
        f=self.fixture()
        plan=f.primary.db.execute("EXPLAIN QUERY PLAN SELECT result FROM requests WHERE CASE WHEN json_valid(result) THEN json_extract(result,'$.status') ELSE 'invalid' END IN ('unknown','invalid') AND CASE WHEN json_valid(result) THEN json_extract(result,'$.ticket') END=?",('123',)).fetchall()
        self.assertIn('request_unknown_ticket',str(plan))
        joined=f.primary.db.execute("EXPLAIN QUERY PLAN SELECT r.result FROM requests r INDEXED BY request_unknown_ticket JOIN request_targets t ON r.id=t.id WHERE t.symbol=? AND CASE WHEN json_valid(r.result) THEN json_extract(r.result,'$.status') ELSE 'invalid' END IN ('unknown','invalid')",('USTEC',)).fetchall()
        self.assertIn('SCAN r USING INDEX request_unknown_ticket',str(joined))

    def test_stale_follower_context_keeps_primary_independent(self):
        f=self.fixture();f.natives[1].tick.time=f.now-5;f.engine.arm(f.payload)
        state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']>=1)
        self.assertFalse(f.natives[1].sent);self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        time.sleep(.01)
        f.natives[1].tick.time=f.now
        resumed=self.until(f,lambda s:s['accounts']['900000001']['filledVolume']>=.25)
        trace=resumed['accounts']['900000001']['entryTiming']['recentStages']
        self.assertTrue(any(row['phase']=='waiting-quote' and row.get('ended') and row['durationMs']>=10 for row in trace),trace)
        self.until(f,lambda s:s['phase']=='completed')

    def test_unknown_cancel_keeps_reservation_and_does_not_replay(self):
        f=self.fixture()
        for native in f.natives:native.after_market=lambda broker:f.quote(broker,25010.)
        f.natives[1].cancel_unknown=True;f.engine.arm(f.payload)
        state=self.until(f,lambda s:any(child.get('cancellations',{}).get('900000001',{}).get('status')=='unknown'
            for child in f.engine._load(f.payload['id'])[2]['children']))
        count=len(f.natives[1].sent)
        for _ in range(12):f.tick();time.sleep(.003)
        self.assertEqual(len(f.natives[1].sent),count)
        self.assertLessEqual(sum(row.volume for row in f.natives[1].positions),1)

    def stopped_partial(self):
        f=self.fixture()
        for native in f.natives:native.after_market=lambda broker:f.quote(broker,25010.)
        f.engine.arm(f.payload)
        self.until(f,lambda s:all(row['filledVolume']>=.25 and row['pendingVolume']>.1 for row in s['accounts'].values()))
        f.engine.stop(dict(id='explicit-stop',entryId=f.payload['id'],expiresAt=f.now*1000+3000))
        for native in f.natives:
            native.after_market=None;f.quote(native,25002.)
        return f

    def test_explicit_continuation_only_adds_missing_volume_on_selected_account(self):
        f=self.stopped_partial();peer=copy.deepcopy(f.natives[1].sent)
        command=dict(id='continue-primary',entryId=f.payload['id'],account='900000002',expiresAt=f.now*1000+3000)
        original=copy.deepcopy(f.engine._load(f.payload['id'])[1])
        f.engine.continue_account(command)
        self.until(f,lambda s:s.get('lanePhases',{}).get('900000002')=='completed')
        self.assertEqual(f.natives[1].sent,peer)
        self.assertAlmostEqual(sum(row.volume for row in f.natives[0].positions),1)
        self.assertEqual(f.engine._load(f.payload['id'])[1],original)
        before=copy.deepcopy([n.sent for n in f.natives])
        f.engine.continue_account(command)
        for _ in range(5):f.tick();time.sleep(.003)
        self.assertEqual([n.sent for n in f.natives],before)

    def test_second_stopped_account_can_continue_after_first_completed(self):
        f=self.stopped_partial()
        for index,account in enumerate(('900000002','900000001')):
            f.engine.continue_account(dict(id='continue-'+str(index),entryId=f.payload['id'],account=account,expiresAt=f.now*1000+3000))
            self.until(f,lambda s:s['accounts'][account]['filledVolume']>=1)
        self.until(f,lambda s:s['phase']=='completed')
        for native in f.natives:self.assertAlmostEqual(sum(row.volume for row in native.positions),1)

    def test_continuation_cannot_override_safety_stop_or_change_identity(self):
        f=self.stopped_partial()
        _,_,state=f.engine._load(f.payload['id']);state['hybridRemainderStop']='Captured SL reached'
        f.engine._save(state)
        command=dict(id='no-override',entryId=f.payload['id'],account='900000002',expiresAt=f.now*1000+3000)
        before=copy.deepcopy([n.sent for n in f.natives])
        with self.assertRaisesRegex(ValueError,'stopped'):f.engine.continue_account(command)
        command['account']='00000'
        with self.assertRaises(ValueError):f.engine.continue_account(command)
        self.assertEqual([n.sent for n in f.natives],before)

    def test_unknown_cancel_automatically_checks_and_manual_continue_never_replays(self):
        f=self.fixture()
        for native in f.natives:native.after_market=lambda broker:f.quote(broker,25010.)
        f.natives[1].cancel_unknown=True;f.engine.arm(f.payload)
        self.until(f,lambda s:f.engine._load(f.payload['id'])[2].get('laneReconciliation',{}).get('900000001',{}).get('status')=='checking')
        before=copy.deepcopy(f.natives[1].sent)
        f.engine._drain_lanes(f.payload['id'])
        f.engine.continue_account(dict(id='continue-unknown',entryId=f.payload['id'],account='900000001',expiresAt=f.now*1000+3000))
        for _ in range(20):f.tick();time.sleep(.003)
        self.assertEqual(f.natives[1].sent,before)

    def test_proven_unsent_cancel_is_read_checked_and_can_continue(self):
        from mt5_bridge import WorkerDispatchNotSent
        f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call;refused=[]
        def call(method,*args):
            if method=='entry_cancel_limit' and not refused:
                refused.append(args[0]['id']);raise WorkerDispatchNotSent('Linked MT5 is busy; request was not sent')
            return original(method,*args)
        worker.call=call;f.engine.arm(f.payload)
        state=self.until(f,lambda s:s['phase']=='completed')
        self.assertEqual(len(refused),1)
        for native in f.natives:self.assertAlmostEqual(sum(row.volume for row in native.positions),1)
        children=f.engine._load(f.payload['id'])[2]['children']
        self.assertTrue(any(row.get('dispatchNotSent') for child in children
            for history in child.get('cancelHistory',{}).values() for row in history))

    def test_proven_unsent_opening_waits_then_rechecks_without_halting_either_account(self):
        from mt5_bridge import WorkerDispatchNotSent
        for kind in ('market','limit'):
            with self.subTest(kind=kind):
                f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call;refused=[]
                def call(method,*args):
                    if method=='execute' and args[0]=='place' and args[1]['type']==kind and not refused:
                        refused.append(args[1]['id']);raise WorkerDispatchNotSent('Linked MT5 is busy; request was not sent')
                    return original(method,*args)
                worker.call=call;f.engine.arm(f.payload)
                self.until(f,lambda s:s['phase']=='completed')
                self.assertEqual(len(refused),1)
                for native in f.natives:self.assertAlmostEqual(sum(row.volume for row in native.positions),1)
                self.assertFalse(any(row.get('entryHalted') for row in f.engine._load(f.payload['id'])[2]['accounts'].values()))

    def test_continuation_does_not_reopen_a_position_that_has_disappeared(self):
        f=self.stopped_partial();f.natives[0].positions.clear()
        before=len(f.markets(f.natives[0]))
        f.engine.continue_account(dict(id='closed-no-reopen',entryId=f.payload['id'],account='900000002',expiresAt=f.now*1000+3000))
        for _ in range(25):f.tick();time.sleep(.003)
        self.assertEqual(len(f.markets(f.natives[0])),before)

    def test_parent_protection_after_lanes_is_exact_and_preserves_volume(self):
        f=self.fixture();f.engine.arm(f.payload);self.until(f,lambda s:s['phase']=='completed')
        before=[sum(row.volume for row in native.positions) for native in f.natives]
        command=dict(id='lane-protection',entryId=f.payload['id'],symbol='USTEC',operation='modify',
            account='900000002',mirrorAccounts=[dict(account='900000001')],sl=24990.,expiresAt=f.now*1000+3000)
        result=f.engine.protection(command)
        self.assertEqual(result['result']['status'],'ok',result)
        for native,volume in zip(f.natives,before):
            self.assertAlmostEqual(sum(row.volume for row in native.positions),volume)
            self.assertTrue(all(row.sl==24990. for row in native.positions))

    def test_stop_flag_before_drain_prevents_next_part(self):
        f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def call(method,*args):
            result=original(method,*args)
            if method=='execute' and args[0]=='place' and not entered.is_set():entered.set();release.wait(2)
            return result
        worker.call=call;f.engine.arm(f.payload)
        self.until(f,lambda s:entered.is_set())
        results=[]
        thread=threading.Thread(target=lambda:results.append(f.engine.stop(dict(id='drain-stop',entryId=f.payload['id'],expiresAt=f.now*1000+3000))))
        thread.start()
        end=time.monotonic()+1
        while not f.engine._stopping(f.payload['id']) and time.monotonic()<end:time.sleep(.001)
        self.assertTrue(f.engine._stopping(f.payload['id']));release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(results[0]['phase'],'stopped')
        self.assertFalse(f.markets(f.natives[1]))

    def test_history_index_preserves_unknown_command_block(self):
        f=self.fixture();bridge=f.primary
        import json
        bridge.db.execute('INSERT INTO requests VALUES (?,?,?)',('unknown-close','x',json.dumps(dict(status='unknown',operation='close',ticket='123'))))
        bridge.db.commit()
        state=bridge.opening_state('123','USTEC',[])
        self.assertEqual(state['phase'],'unknown')

    def test_durable_duplicate_merge_adds_no_commit(self):
        f=self.fixture();f.engine.arm(f.payload);self.until(f,lambda s:s['phase']=='completed')
        lane=f.engine.lanes[(f.payload['id'],'900000002')]
        state=copy.deepcopy(f.engine._load(f.payload['id'])[2]);state['accounts']={'900000002':state['accounts']['900000002']}
        state['children']=[c for c in state['children'] if '900000002' in c['volumes']];state['phase']='completed'
        changes=f.engine.storage.db.total_changes;lane._save(state)
        self.assertEqual(f.engine.storage.db.total_changes,changes)

    def test_unknown_market_is_not_replayed_while_peer_completes(self):
        f=self.fixture();native=f.natives[1];original=native.order_send;lost=[False]
        def send(request):
            result=original(request)
            if request['action']==native.TRADE_ACTION_DEAL and 'position' not in request and not lost[0]:
                lost[0]=True;return None
            return result
        native.order_send=send;f.engine.arm(f.payload)
        self.until(f,lambda s:s['accounts']['900000002']['filledVolume']>=1 and lost[0])
        count=len(f.markets(native))
        for _ in range(15):f.tick();time.sleep(.003)
        self.assertEqual(len(f.markets(native)),count)
        self.assertLessEqual(sum(row.volume for row in native.positions),1)

    def test_restart_never_resumes_existing_lanes(self):
        f=self.fixture()
        for native in f.natives:native.after_market=lambda broker:f.quote(broker,25010.)
        f.engine.arm(f.payload)
        self.until(f,lambda s:all(native.orders for native in f.natives))
        f.engine.close()
        from chunk_entry import ChunkEntries
        f.primary.chunk_entries=ChunkEntries(f.primary);f.engine=f.primary.chunk_entries
        before=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(5):f.tick()
        self.assertEqual([n.sent for n in f.natives],before)
        self.assertIn(f.state()['phase'],('unknown','stopped'))

    def test_close_all_owned_lane_positions_and_replay_receipt(self):
        f=self.fixture();f.engine.arm(f.payload);self.until(f,lambda s:s['phase']=='completed')
        command=f.close_command(id='lane-close')
        result=f.engine.close_position(command)
        self.assertEqual(result['result']['status'],'ok',result)
        before=copy.deepcopy([n.sent for n in f.natives])
        self.assertEqual(f.engine.close_position(command)['result'],result['result'])
        self.assertEqual([n.sent for n in f.natives],before)
        self.assertTrue(all(not n.positions and not n.orders for n in f.natives))

    def test_blocked_read_uses_completion_event_without_busy_polling(self):
        f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call
        entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def call(method,*args):
            if method=='entry_context':entered.set();release.wait(2)
            return original(method,*args)
        worker.call=call
        from chunk_entry import AccountEntryLane
        run=AccountEntryLane.run;calls=[]
        def counted(lane,*args):calls.append(lane.lane_account);return run(lane,*args)
        f.engine.arm(f.payload)
        with patch.object(AccountEntryLane,'run',counted):
            f.engine.start()
            try:
                self.assertTrue(entered.wait(1));time.sleep(.15)
                self.assertLess(len(calls),20,'A blocked read must not wake a polling loop')
            finally:release.set()
            self.until(f,lambda s:s['phase']=='completed')

if __name__=='__main__':unittest.main()
