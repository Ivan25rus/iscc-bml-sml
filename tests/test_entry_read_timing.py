"""Diagnostic accounting must separate queue, native read and actor resumption."""
import concurrent.futures
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from chunk_entry import ChunkEntries, EntryDiagnostics


class EntryReadTimingTests(unittest.TestCase):
    def read(self, age=0, resumed_at=100.2, exact=None):
        engine=object.__new__(ChunkEntries)
        engine.retired_snapshot_jobs=set()
        engine.bridge=SimpleNamespace(warm_group_executor=lambda:None)
        engine.entry_diagnostics=None
        notes=[]
        engine._parallel_note=lambda state,account,phase,reason,**metrics:notes.append((phase,metrics))
        snapshot=dict(account='900000001',quote=dict(bid=10,ask=11,utcTime=1000-age),metadata=dict(contractSize=1))
        timing=dict(readQueueMs=5.,readWorkMs=15.,_readFinishedAt=100.020,contextMs=2.,workerRpcMs=3.)
        timing['_exactLimit'] = exact
        future=concurrent.futures.Future();future.set_result((snapshot,None,timing))
        engine.snapshot_jobs={('entry','900000001'):(100.,future)}
        state=dict(id='entry',accounts={'900000001':dict(pointValue=1)},children=[])
        with patch('chunk_entry.time.monotonic',return_value=resumed_at),patch('chunk_entry.time.time',return_value=1000):
            ready,_,_,exact_limits=engine._parallel_quotes(dict(symbol='SPX500'),state)
        return ready,notes,timing,exact_limits

    def test_completed_read_distinguishes_actor_delay_from_native_work(self):
        ready,notes,original,_=self.read()
        self.assertEqual(ready,{'900000001':(10,11)})
        phase,metrics=notes[0]
        self.assertEqual(phase,'ready')
        self.assertEqual(metrics['readWaitMs'],200)
        self.assertEqual(metrics['readQueueMs'],5)
        self.assertEqual(metrics['readWorkMs'],15)
        self.assertEqual(metrics['readResumeMs'],180)
        self.assertNotIn('_readFinishedAt',metrics)
        self.assertIn('_readFinishedAt',original,'Never mutate the cross-thread result')
        self.assertTrue({'readQueueMs','readWorkMs','readResumeMs'}<=EntryDiagnostics.NUMBERS)

    def test_timing_does_not_make_an_old_unchanged_quote_fresh(self):
        ready,notes,_,_=self.read(age=1.8)
        self.assertEqual(ready,{})
        self.assertEqual(notes[0][0],'waiting-quote')
        self.assertAlmostEqual(notes[0][1]['quoteAgeMs'],1800)
        self.assertEqual(notes[0][1]['readResumeMs'],180)

    def test_batched_order_status_requires_current_quote_and_recent_read(self):
        view = ('7', {'account': '900000001', 'terminal': {'phase': 'active'}})
        self.assertEqual(self.read(resumed_at=100.04, exact=view)[3], {'900000001': view})
        self.assertEqual(self.read(exact=view)[3], {})
        self.assertEqual(self.read(age=1.8, resumed_at=100.04, exact=view)[3], {})


if __name__=='__main__':unittest.main()
