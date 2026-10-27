"""Parallel ready-account entry, edge relocation and failure races; fake MT5."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as protection
from chunk_entry import ChunkEntries

class ParallelEntryTests(unittest.TestCase):
    def fixture(self,side='buy'):
        f=protection.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        f.make(side,2);f.payload.update(accountExecution='independent-v3',chunkPercent=25,chunkVolume=.25)
        for native in f.natives:native.info.volume_step=native.info.volume_min=.01
        return f

    def run_until(self,f,condition,limit=100):
        for _ in range(limit):
            state=f.tick()
            if condition(state):return state
            time.sleep(.003)
        self.fail('Entry condition did not settle: '+repr(state))

    def arm(self,f,condition=lambda s:s['phase'] in ('completed','stopped','unknown')):
        f.engine.arm(f.payload)
        return self.run_until(f,condition)

    def outside_after_market(self,f):
        for native in f.natives:
            native.after_market=lambda broker:f.quote(broker,25010. if f.side=='buy' else 24990.)

    def test_support_first_quarter_ladder_and_common_stop_on_both_sides(self):
        for side in ('buy','sell'):
            with self.subTest(side=side):
                f=self.fixture(side);state=self.arm(f)
                self.assertEqual(state['phase'],'completed',state['reason'])
                for native in f.natives:
                    actions=[row for row in native.sent if row['action'] in (native.TRADE_ACTION_PENDING,native.TRADE_ACTION_DEAL)]
                    self.assertEqual([(row['action'],row['volume']) for row in actions],
                        [(native.TRADE_ACTION_PENDING,.75),(native.TRADE_ACTION_DEAL,.25),
                         (native.TRADE_ACTION_PENDING,.5),(native.TRADE_ACTION_DEAL,.25),
                         (native.TRADE_ACTION_PENDING,.25),(native.TRADE_ACTION_DEAL,.25),(native.TRADE_ACTION_DEAL,.25)])
                    self.assertAlmostEqual(sum(row.volume for row in native.positions),1.)
                    self.assertFalse(native.orders)
                    self.assertEqual(len({row.sl for row in native.positions}),1)
                self.assertLessEqual(len(state['children']),14,'Each account has at most seven exact tranches')

    def test_resting_support_moves_to_inclusive_outer_edge_once(self):
        for side,edge in [('buy',25006.),('sell',24994.)]:
            with self.subTest(side=side):
                f=self.fixture(side);self.outside_after_market(f)
                state=self.arm(f,lambda s:len(s['children'])>=3)
                self.assertEqual(state['phase'],'running',state['reason'])
                for native in f.natives:
                    self.assertEqual(len(native.orders),1)
                    self.assertAlmostEqual(native.orders[0].price_open,edge)
                    self.assertAlmostEqual(native.orders[0].volume_current,.75)
                before=copy.deepcopy([n.sent for n in f.natives])
                for _ in range(5):f.tick();time.sleep(.003)
                self.assertEqual([n.sent for n in f.natives],before,'Outside quotes do not churn an edge limit')

    def test_edge_relocation_reconciles_fill_during_cancel(self):
        f=self.fixture();self.outside_after_market(f)
        for native in f.natives:native.fill_on_cancel=True
        state=self.arm(f)
        self.assertEqual(state['phase'],'completed',state['reason'])
        for native in f.natives:
            self.assertFalse(native.orders)
            self.assertAlmostEqual(sum(row.volume for row in native.positions),1.)
            self.assertEqual(len(f.markets(native)),1)

    def test_definite_limit_rejection_keeps_both_market_ladders_independent(self):
        f=self.fixture();f.natives[1].info.trade_stops_level=100;f.natives[1].info.point=.1
        state=self.arm(f)
        self.assertEqual(state['phase'],'completed',state['reason'])
        for native in f.natives:self.assertAlmostEqual(sum(row.volume for row in native.positions),1.)
        self.assertTrue(any(child.get('result',{}).get('status')=='unknown' for child in f.engine._load(f.payload['id'])[2]['children']),
            'Mixed definite native receipts remain untouched in the parent')

    def test_blocked_follower_read_does_not_block_primary_ladder(self):
        f=self.fixture();worker=f.primary.linked['900000001'];original=worker.call
        entered,release=threading.Event(),threading.Event()
        self.addCleanup(release.set)
        def call(method,*args):
            if method=='entry_snapshot':entered.set();release.wait(2)
            return original(method,*args)
        worker.call=call
        f.engine.arm(f.payload)
        try:
            state=self.run_until(f,lambda s:s['accounts']['900000002']['filledVolume']>=1)
            self.assertTrue(entered.is_set())
            self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
            self.assertEqual(state['accounts']['900000001']['entryStatus'],'waiting-read')
            self.assertIn('readWaitMs',state['accounts']['900000001']['entryTiming'])
        finally:release.set()
        state=self.run_until(f,lambda s:s['phase'] in ('completed','stopped','unknown'))
        self.assertEqual(state['phase'],'completed',state['reason'])

    def test_initial_adverse_price_reserves_full_target_at_edge_without_market(self):
        f=self.fixture('sell')
        for native in f.natives:f.quote(native,24990.)
        state=self.arm(f,lambda s:len(s['children'])>0)
        for native in f.natives:
            self.assertEqual(len(native.orders),1)
            self.assertEqual(native.orders[0].price_open,24994.)
            self.assertEqual(native.orders[0].volume_current,1.)
            self.assertFalse(f.markets(native))
        self.assertTrue(all(row['pendingVolume']==1 and row['freeVolume']==0 for row in state['accounts'].values()))

    def test_fast_listing_is_deferred_during_read_wait_and_has_no_native_mutations(self):
        f=self.fixture();f.engine.arm(f.payload)
        before=copy.deepcopy([n.sent for n in f.natives])
        view=f.primary.entry_plans()
        self.assertEqual(len(view['entries']),1)
        self.assertTrue(view['entries'][0]['lifecycleDeferred'])
        self.assertFalse(view['entries'][0]['positionsConfirmed'])
        self.assertEqual([n.sent for n in f.natives],before)

    def test_legacy_v2_does_not_move_existing_support(self):
        f=self.fixture();f.payload['accountExecution']='independent-v2';self.outside_after_market(f)
        state=self.arm(f,lambda s:all(row['filledVolume']>=.25 for row in s['accounts'].values()))
        before=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(4):f.tick()
        self.assertEqual([n.sent for n in f.natives],before)

    def test_stale_follower_quote_never_blocks_fresh_primary_or_relaxes_freshness(self):
        f=self.fixture();f.natives[1].tick.time=f.now-5
        # The healthy account can finish while its peer is between quote
        # reads. Wait for the stale peer's own classification, not an implied
        # barrier between the two independent accounts.
        state=self.arm(f,lambda s:s['accounts']['900000002']['filledVolume']>=1 and
            s['accounts']['900000001']['entryStatus']=='waiting-quote')
        self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        self.assertFalse(f.natives[1].sent)
        self.assertEqual(state['accounts']['900000001']['entryStatus'],'waiting-quote')
        self.assertGreaterEqual(state['accounts']['900000001']['entryTiming']['quoteAgeMs'],5000)
        f.natives[1].tick.time=f.now
        state=self.run_until(f,lambda s:s['phase'] in ('completed','stopped','unknown'))
        self.assertEqual(state['phase'],'completed',state['reason'])

    def test_different_account_zones_do_not_force_market_on_waiting_account(self):
        f=self.fixture();f.quote(f.natives[1],25010.)
        state=self.arm(f,lambda s:s['accounts']['900000002']['filledVolume']>=1 and s['accounts']['900000001']['pendingVolume']>=1)
        self.assertFalse(f.markets(f.natives[1]))
        self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        self.assertEqual(f.natives[1].orders[0].price_open,25006.)

    def test_broker_distance_clamps_edge_inward_without_repeated_replacements(self):
        f=self.fixture();self.outside_after_market(f)
        for native in f.natives:native.info.trade_stops_level=50;native.info.point=.1
        self.arm(f,lambda s:all(row['filledVolume']>=.25 for row in s['accounts'].values()))
        with patch('time.time',return_value=f.now+.3):
            state=self.run_until(f,lambda s:all(row['pendingVolume']>=.75 for row in s['accounts'].values()))
        for native in f.natives:self.assertAlmostEqual(native.orders[0].price_open,25005.)
        before=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(5):f.tick();time.sleep(.003)
        self.assertEqual([n.sent for n in f.natives],before)

    def test_pending_partial_fill_is_counted_before_edge_replacement(self):
        f=self.fixture();self.outside_after_market(f)
        for native in f.natives:native.partial_cancel_volume=.2
        state=self.arm(f,lambda s:all(row['filledVolume']>=.45 and row['pendingVolume']>0 for row in s['accounts'].values()))
        for account,row in state['accounts'].items():
            self.assertAlmostEqual(row['filledVolume']+row['pendingVolume'],1.)
            self.assertLessEqual(row['riskUsed'],16.+1e-8)
        self.assertEqual(state['phase'],'running',state['reason'])

    def test_unknown_cancel_does_not_release_volume_or_send_replacement(self):
        f=self.fixture();self.outside_after_market(f)
        native=f.natives[1];native.cancel_unknown=True
        state=self.arm(f,lambda s:s['phase']=='unknown' or s.get('reconciliation',{}).get('status')=='checking')
        before=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(4):f.tick();time.sleep(.003)
        self.assertEqual([n.sent for n in f.natives],before)
        self.assertLessEqual(sum(row.volume for row in native.positions),1.)

    def test_unknown_market_never_replays_a_native_submission(self):
        f=self.fixture();native=f.natives[1];original=native.order_send
        def lost(request):
            result=original(request)
            return None if request['action']==native.TRADE_ACTION_DEAL else result
        native.order_send=lost
        state=self.arm(f,lambda s:s['phase']=='unknown' or s.get('reconciliation',{}).get('status')=='checking')
        for _ in range(6):f.tick();time.sleep(.003)
        self.assertEqual(len(f.markets(native)),1)
        self.assertLessEqual(sum(row.volume for row in native.positions),1.)

    def test_restart_keeps_edge_reservation_and_does_not_rearm_it(self):
        f=self.fixture();self.outside_after_market(f)
        self.arm(f,lambda s:len(s['children'])>=3)
        f.engine.close();f.engine=ChunkEntries(f.primary);f.primary.chunk_entries=f.engine
        before=copy.deepcopy([n.sent for n in f.natives])
        for _ in range(5):f.tick();time.sleep(.003)
        self.assertEqual([n.sent for n in f.natives],before)
        self.assertTrue(all(row['pendingVolume']==.75 for row in f.state()['accounts'].values()))

    def test_stopping_cancels_relocated_pending_and_retains_exact_positions(self):
        f=self.fixture();self.outside_after_market(f)
        self.arm(f,lambda s:len(s['children'])>=3)
        before=[[(row.ticket,row.volume,row.sl,row.tp) for row in n.positions] for n in f.natives]
        state=f.engine.stop(dict(id='stop-parallel',entryId=f.payload['id'],expiresAt=f.now*1000+3000))
        self.assertEqual(state['phase'],'stopped',state['reason'])
        self.assertTrue(all(not n.orders for n in f.natives))
        self.assertEqual([[(row.ticket,row.volume,row.sl,row.tp) for row in n.positions] for n in f.natives],before)

    def test_whole_parent_protection_still_covers_all_parallel_fills(self):
        f=self.fixture();self.arm(f)
        result=f.protect(sl=24990.,tp=25050.,mirrorAccounts=[dict(account='900000001')])
        self.assertEqual(result['result']['status'],'ok',result['result'])
        for native in f.natives:
            self.assertTrue(all(row.sl==24990. and row.tp==25050. for row in native.positions))

    def test_whole_parent_close_still_uses_every_exact_parallel_opening(self):
        f=self.fixture();self.arm(f)
        result=f.engine.close_position(dict(id='parallel-close',entryId=f.payload['id'],symbol='USTEC',expiresAt=f.now*1000+3000))
        self.assertEqual(result['result']['status'],'ok',result['result'])
        self.assertTrue(all(not native.positions and not native.orders for native in f.natives))

    def test_ready_support_batch_enters_both_native_sends_concurrently(self):
        f=self.fixture();f.engine.arm(f.payload)
        state=f.engine._load(f.payload['id'])[2]
        gate=threading.Barrier(2,timeout=2)
        plans={}
        for native in f.natives:
            original=native.order_send
            def send(request,original=original,native=native):
                if request['action']==native.TRADE_ACTION_PENDING:gate.wait()
                return original(request)
            native.order_send=send
            account=str(native.account.login)
            plans[account]=(native.tick.bid,.75,native.tick.bid,native.tick.ask,4.)
        self.assertTrue(f.engine._parallel_batch(f.payload,state,'limit',plans))
        self.assertTrue(all(len(native.orders)==1 for native in f.natives))
        self.assertEqual(len(state['children']),1)

if __name__=='__main__':unittest.main()
