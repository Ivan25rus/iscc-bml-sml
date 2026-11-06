"""Read-path regressions; temporary ledgers and fake SDKs only."""
import copy
import threading
import time
import unittest
from concurrent.futures import Future
from unittest.mock import patch
from mt5_bridge import QuotePublisher
from chunk_entry import public_record
import test_entry_independence as fixtures


class PipelineLatencyTests(unittest.TestCase):
    def model(self):
        case=fixtures.EntryIndependenceTests();self.addCleanup(case.doCleanups)
        return case.model()

    def test_exact_timing_is_retained_but_frequent_publication_has_same_trading_fields(self):
        f,root=self.model();state=f.engine.arm(f.payload)
        account=next(iter(state['accounts']))
        state['accounts'][account]['entryTiming']={'readWaitMs':12,'recentStages':[{'phase':'ready'}]*48}
        exact=public_record(state);small=public_record(state,diagnostics=False)
        self.assertIn('entryTiming',exact['accounts'][account]);self.assertNotIn('entryTiming',small['accounts'][account])
        expected=copy.deepcopy(exact)
        for row in expected['accounts'].values():row.pop('entryTiming',None)
        self.assertEqual(expected,small)
        f.engine._remember(state)
        listed=next(row for row in f.engine.list() if row['id']==state['id'])
        self.assertNotIn('entryTiming',listed['accounts'][account])
        self.assertIn('entryTiming',state['accounts'][account])
        self.assertFalse(any(native.sent for native in f.natives))

    def test_completed_native_read_is_cached_without_a_second_status_poll(self):
        f,root=self.model();release=threading.Event();entered=threading.Event();self.addCleanup(release.set)
        def held(method,args):
            if method=='management_snapshot':entered.set();release.wait(2)
        root.primary_worker.before=held
        root.management_accounts(compact=True)
        self.assertTrue(entered.wait(1));self.assertNotIn('900000002',root.management_cache)
        release.set();deadline=time.monotonic()+1
        while '900000002' not in root.management_cache and time.monotonic()<deadline:time.sleep(.002)
        self.assertIn('900000002',root.management_cache)
        self.assertNotIn('900000002',root.management_jobs)
        self.assertTrue(root.management_accounts(compact=True)['900000002']['evidenceReady'])
        self.assertFalse(any(native.sent for native in f.natives))

    def test_entry_lifecycle_uses_same_active_view_as_panel(self):
        f,root=self.model()
        entry={'id':'fixture','phase':'completed','accounts':{'900000002':{}},'children':[],'symbol':'USTEC'}
        states={'900000002':{'connected':True,'evidenceReady':True,'orders':[]}}
        with patch.object(root.chunk_entries,'list',return_value=[entry]),patch.object(root,'management_accounts',return_value=states) as read:
            self.assertEqual(root.entry_plans()['entries'],[])
        read.assert_called_once_with(compact=True)
        self.assertFalse(any(native.sent for native in f.natives))

    def test_immediate_completion_cannot_remove_an_account_from_the_response(self):
        f,root=self.model()
        class ImmediateExecutor:
            def submit(self,fn,*args,**kwargs):
                result=Future();result.set_result(fn(*args,**kwargs));return result
        with patch.object(root,'warm_group_executor',return_value=ImmediateExecutor()):
            views=root.management_accounts(compact=True)
        self.assertEqual(set(views),{'900000002','900000001'})
        self.assertTrue(all(row['connected'] and row['evidenceReady'] for row in views.values()))
        self.assertFalse(root.management_jobs)
        self.assertFalse(any(native.sent for native in f.natives))

    def test_quote_reuse_keeps_native_time_and_returns_an_isolated_copy(self):
        _,root=self.model();publisher=QuotePublisher(root)
        value={'account':900000002,'server':root.config['server'],'symbol':'USTEC','quote':{'nativeCapturedAt':123,'utcTime':122,'bid':100,'ask':101}}
        publisher.latest_frame={'snapshots':{'USTEC':value}};publisher.latest_at=time.monotonic()
        root.quote_publisher=publisher
        with patch.object(root,'_quote_call',side_effect=AssertionError('Duplicate native read')):
            result=root.snapshot('USTEC')
        self.assertEqual(result,value);result['quote']['bid']=1
        self.assertEqual(value['quote']['bid'],100)
        self.assertEqual(root.read_timings.snapshot()['operations']['quoteSnapshotReused']['calls'],1)

    def test_expired_failed_missing_or_closed_quotes_require_existing_fresh_native_path(self):
        _,root=self.model();publisher=QuotePublisher(root);root.quote_publisher=publisher
        for condition in ('expired','failed','missing','closed'):
            publisher.latest_frame={'snapshots':{'USTEC':{'quote':{'bid':100}}}}
            publisher.latest_at=time.monotonic();publisher.failed=False;publisher.closed=False
            if condition=='expired':publisher.latest_at-=.101
            if condition=='failed':publisher.failed=True
            if condition=='missing':publisher.latest_frame['snapshots']={}
            if condition=='closed':publisher.closed=True
            with patch.object(root,'_quote_call',return_value={'fresh':True}) as read:
                self.assertEqual(root.snapshot('USTEC'),{'fresh':True})
            read.assert_called_once_with('snapshot','USTEC')


if __name__=='__main__':unittest.main()
