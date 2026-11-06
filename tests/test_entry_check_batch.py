"""Seven entry-check improvements: fake MT5, temporary SQLite, no network."""
import copy,json,pathlib,sys,threading,time,unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_entry_independence as fixtures
from chunk_entry import ChunkEntries
from mt5_bridge import format_management_snapshot

class EntryCheckBatchTests(unittest.TestCase):
    def model(self):
        case=fixtures.EntryIndependenceTests();self.addCleanup(case.doCleanups)
        f,root=case.model();return case,f,root

    def completed(self):
        case,f,root=self.model();f.engine.start();f.engine.arm(f.payload)
        # Functional fixture convergence includes durable SQLite journals.
        # Match the existing Windows fixture allowance; explicit latency and
        # every production deadline/freshness assertion remain unchanged.
        case.wait(lambda:all(a._load(f.payload['id']) and a._load(f.payload['id'])[2]['phase']=='completed'
            for a in tuple(f.engine.shards.values())) and len(f.engine.shards)==2,
            seconds=20 if sys.platform=='win32' else 4)
        return case,f,root

    def test_market_stop_refusal_rechecks_unchanged_price_after_bounded_wait(self):
        now=time.time()*1000
        state=dict(accounts={'900000001':dict(priceRejections={'market':dict(quote=[100,101],volume=.2,
            at=now,retryMs=250,recheckMs=1000,waitForQuoteChange=True)})})
        self.assertFalse(ChunkEntries._parallel_retry_ready(state,'900000001','market',[100,101],.2))
        state['accounts']['900000001']['priceRejections']['market']['at']-=1001
        self.assertTrue(ChunkEntries._parallel_retry_ready(state,'900000001','market',[100,101],.2))

    def test_only_definite_unfilled_opening_refusal_is_retryable(self):
        payload=dict(fillPriority='volume',initialSizingOnly=True,accountExecution='independent-v5',executionPolicy='hybrid-market-limit-v1')
        result=dict(status='rejected',retcode=10016)
        self.assertTrue(ChunkEntries._retryable_market_protection_refusal(payload,dict(type='market'),result))
        for change in [dict(status='unknown'),dict(providerOrderId='77'),dict(filledVolume=.2)]:
            self.assertFalse(ChunkEntries._retryable_market_protection_refusal(payload,dict(type='market'),dict(result,**change)))
        self.assertFalse(ChunkEntries._retryable_market_protection_refusal(payload,dict(type='limit'),result))

    def test_normal_admission_is_one_native_batch_per_account(self):
        case,f,root=self.model();calls=[]
        for account,worker in [('900000002',root.primary_worker),('900000001',root.linked['900000001'])]:
            worker.before=lambda method,args,a=account:calls.append((a,method))
        f.engine.arm(f.payload)
        for job in list(f.engine.admitting.values()):job.result(timeout=3)
        self.assertCountEqual(calls,[('900000002','entry_admission'),('900000001','entry_admission')])
        self.assertTrue(all(not native.sent for native in f.natives))

    def test_batch_checks_selected_symbol_and_does_not_send(self):
        case,f,root=self.model();payload=copy.deepcopy(f.payload)
        payload.pop('mirrorAccounts',None);payload.pop('mirrorAccount',None)
        with patch.object(f.natives[0],'symbol_info',wraps=f.natives[0].symbol_info) as spec:
            batch=f.primary.entry_admission(payload)
        self.assertEqual(batch['identity']['login'],900000002)
        self.assertEqual({c.args[0] for c in spec.call_args_list},{'USTEC'})
        self.assertEqual(batch['checkedRequest']['volume'],payload['chunkVolume'])
        self.assertFalse(f.natives[0].sent)

    def test_batch_identity_failure_blocks_admission_without_send(self):
        case,f,root=self.model();f.natives[0].account.login=1
        with self.assertRaisesRegex(ValueError,'account or server mismatch'):f.primary.entry_admission(f.payload)
        self.assertFalse(f.natives[0].sent)

    def test_changed_first_part_uses_original_preflight_fallback(self):
        case,f,root=self.model();engine=f.engine._shard('900000002')
        first=dict(id='part',volume=.2,symbol='USTEC',expiresAt=time.time()*1000+3000)
        engine.admission_batches={'900000002':dict(checkedRequest=dict(first))}
        with patch.object(engine,'_call',return_value=True) as call:
            engine._admission_checks(dict(accountExecution='independent-v4'),{'900000002':{}},first,{'900000002':[.1]})
        self.assertEqual(call.call_args.args[1],'check_opening')

    def test_market_fill_returns_fresh_proof_without_followup_rpc(self):
        case,f,root=self.model();calls=[]
        for worker in (root.primary_worker,root.linked['900000001']):
            worker.before=lambda method,args:calls.append(method)
        f.engine.start();f.engine.arm(f.payload)
        case.wait(lambda:all(sum(p.volume for p in n.positions)>=1-1e-8 for n in f.natives))
        case.wait(lambda:all(a._load(f.payload['id'])[2]['phase']=='completed' for a in f.engine.shards.values()))
        self.assertNotIn('entry_positions',calls)
        self.assertNotIn('entry_management',calls)
        for bridge in f.bridges:
            for text, in bridge.db.execute('SELECT result FROM requests'):
                self.assertNotIn('positionProof',json.loads(text))

    def test_missing_or_stale_opening_proof_falls_back_to_fresh_positions(self):
        case,f,root=self.completed();actor=f.engine.shards['900000002']
        _,payload,state=actor._load(f.payload['id']);lane=actor.lanes[(state['id'],'900000002')]
        child=next(c for c in state['children'] if c['type']=='market')
        outcome=lane._limit_results(state,child)['900000002']
        # No durable/native receipt can carry a reusable live-position proof.
        for altered in ({},dict(schema='entry-opening-proof-v1',commandId=child['id'],account='900000002',
                server=f.primary.config['server'],checkedAt=time.time()*1000-1000)):
            lane.opening_proofs={(child['id'],'900000002'):altered}
            with patch.object(lane,'_call',wraps=lane._call) as call:
                # Current SL changed as the lot-weighted entry grew. The fresh
                # read must detect that instead of trusting the old child SL.
                with self.assertRaises(ValueError):lane._confirmed_position(payload,state,child,'900000002',outcome)
            self.assertEqual(call.call_args.args[1],'entry_positions')

    def test_position_context_skips_pending_inventory_and_ui_specs(self):
        case,f,root=self.completed();native=f.natives[0]
        watch=[dict(ticket=str(native.positions[0].ticket),symbol='USTEC')]
        with patch.object(native,'orders_get',side_effect=AssertionError('No pending read')), patch.object(native,'symbol_info',side_effect=AssertionError('No UI spec read')):
            view=f.primary.entry_positions(watch)
        self.assertEqual(len(view['orders']),4);self.assertTrue(view['openingStates'])

    def test_missing_position_still_needs_exact_broker_history(self):
        case,f,root=self.model()
        with patch.object(f.natives[0],'history_orders_get',wraps=f.natives[0].history_orders_get) as history:
            view=f.primary.entry_positions([dict(ticket='999',symbol='USTEC')])
        self.assertGreaterEqual(history.call_count,1)
        self.assertEqual(view['openingStates']['999']['phase'],'unknown')

    def test_absent_cancel_view_reads_same_order_history_once(self):
        case,f,root=self.model();native=f.natives[0]
        with patch.object(native,'history_orders_get',wraps=native.history_orders_get) as history:
            view=f.primary._entry_limit_view('999','USTEC',dict(account='900000002',server=f.primary.config['server'],orders=[]))
        self.assertEqual(history.call_count,1);self.assertEqual(view['terminal']['phase'],'unknown')

    def test_status_separates_connectivity_from_exact_query_readiness(self):
        case,f,root=self.model()
        for worker in (root.primary_worker,root.linked['900000001']):
            worker.before=lambda method,args:time.sleep(.08) if method=='management_snapshot' else None
        cold=root.management_accounts();self.assertTrue(all(r['connected'] is None and not r['evidenceReady'] for r in cold.values()))
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        warm=root.management_accounts();self.assertTrue(all(r['connected'] and r['evidenceReady'] for r in warm.values()))
        for _,job in list(root.management_jobs.values()):job.result(timeout=2)
        changed=root.management_accounts(watch=[dict(account=a,ticket='999',symbol='USTEC') for a in ('900000001','900000002')])
        self.assertTrue(all(r['connected'] and not r['evidenceReady'] for r in changed.values()))
        self.assertTrue(all(not r['openingStates'] and not r['receipts'] for r in changed.values()))
        original=root.management_accounts();self.assertTrue(all(r['connected'] and r['evidenceReady'] for r in original.values()))

    def test_query_cache_is_bounded_and_failure_cannot_reuse_evidence(self):
        case,f,root=self.model()
        value=f.primary.local_management()
        for n in range(12):
            job=Future();job.set_result(copy.deepcopy(value));root._cache_management('900000002',str(n),job)
        self.assertEqual(len(root.management_queries['900000002']),8)
        job=Future();job.set_exception(ValueError('MT5 is disconnected'));root._cache_management('900000002','failed',job)
        self.assertNotIn('900000002',root.management_queries)
        self.assertNotIn('900000002',root.management_cache);self.assertIn('900000002',root.management_errors)

    def test_checking_evidence_never_confirms_closed_positions(self):
        case,f,root=self.model();f.engine.arm(f.payload)
        states={a:dict(connected=True,evidenceReady=False,orders=[]) for a in ('900000001','900000002')}
        views=root.entry_plan_views(states)
        self.assertTrue(views);self.assertFalse(views[0]['positionsConfirmed']);self.assertIsNone(views[0]['hasOpenPositions'])

    def test_slow_display_format_does_not_hold_native_account_or_peer(self):
        case,f,root=self.model();entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=format_management_snapshot
        def slow(*args):entered.set();release.wait(3);return original(*args)
        with patch('mt5_bridge.format_management_snapshot',side_effect=slow):
            thread=threading.Thread(target=lambda:root._read_management('900000002',root.primary_worker,[],[]));thread.start()
            try:
                self.assertTrue(entered.wait(1));f.engine.start();f.engine.arm(f.payload)
                case.wait(lambda:all(sum(p.volume for p in n.positions)>=1-1e-8 for n in f.natives))
            finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_native_status_contains_raw_history_and_pure_formatter_preserves_fields(self):
        case,f,root=self.model();native=f.primary.management_snapshot()
        self.assertIn('_timeRules',native);self.assertNotIn('timeRules',native)
        formatted=format_management_snapshot(native,'900000002')
        old=f.primary.local_management()
        for key in ('orders','rules','timeRules','openingStates','receipts'):self.assertEqual(formatted[key],old[key])
        self.assertNotIn('_timeRules',formatted)

if __name__=='__main__':unittest.main()
