"""Exact full close and control publication; synthetic MT5, no network."""
import copy
import pathlib
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_isolated_entry_engines as isolated
from chunk_entry import ChunkEntries


class EntryCloseOptimizationTests(unittest.TestCase):
    def make(self):
        helper=isolated.IsolatedEngineTests('test_stop_and_protection_keep_exact_parent_ownership')
        self.addCleanup(helper.doCleanups)
        f=helper.make()
        f.payload.update(chunkPercent=50,chunkVolume=.5,fillPriority='volume')
        f.engine.arm(f.payload)
        helper.until(f,lambda row:row['phase']=='completed')
        return f

    def command(self,f,identifier='exact-close'):
        return dict(id=identifier,entryId=f.payload['id'],symbol='USTEC',expiresAt=f.now*1000+3000)

    def until(self,predicate):
        deadline=time.monotonic()+(10 if sys.platform=='win32' else 2)
        while time.monotonic()<deadline:
            if predicate():return
            time.sleep(.002)
        self.fail('Synthetic close did not reach its expected account boundary')

    def test_active_protection_is_paused_while_a_real_interruption_remains_unknown(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        primary=f.engine.shards['900000002'];original=primary._call;answers=[]
        def call(account,method,*args,**kwargs):
            if method=='entry_protection_plan' and account=='900000002':
                entered.set();release.wait()
            return original(account,method,*args,**kwargs)
        with patch.object(primary,'_call',call):
            command=f.command(sl=24991.,mirrorAccounts=[dict(account='900000001')])
            worker=threading.Thread(target=lambda:answers.append(f.engine.protection(command)))
            worker.start()
            try:
                self.assertTrue(entered.wait(10 if sys.platform=='win32' else 3))
                self.until(lambda:f.engine._protection_receipt(command['id'])[1]['accountResults']['900000001']['status']=='ok')
                f.engine._refresh_projections()
                f.engine._publish_root(f.payload['id'],checkpoint=False)
                observed=next(row for row in ChunkEntries.list(f.engine) if row['id']==f.payload['id'])
                self.assertEqual(observed['lanePhases']['900000002'],'paused')
                self.assertEqual(observed['lanePhases']['900000001'],'completed')
                self.assertIn('protectionPending',observed['accounts']['900000002'])
                self.assertNotIn('protectionPending',observed)
            finally:release.set();worker.join(5)
        self.assertFalse(worker.is_alive());self.assertEqual(answers[0]['result']['status'],'ok')
        interrupted=copy.deepcopy(answers[0]['entry']);interrupted['protectionPending']='lost-control'
        f.engine._remember(interrupted);f.engine._publish_root(interrupted['id'],checkpoint=False)
        observed=next(row for row in ChunkEntries.list(f.engine) if row['id']==interrupted['id'])
        self.assertEqual(observed['phase'],'unknown')

    def test_slow_primary_does_not_delay_peer_close_and_its_own_preflight_precedes_submission(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        original=ChunkEntries._owned_close_view;preflight=set();lock=threading.Lock();answers=[]
        def view(engine,payload,state,account,**options):
            result=original(engine,payload,state,account,**options)
            with lock:preflight.add(account)
            return result
        def before_primary():
            self.assertIn('900000002',preflight)
            self.assertTrue(all(not broker.orders for broker in f.natives))
            entered.set();release.wait(5)
        f.natives[0].before_close=before_primary
        with patch.object(ChunkEntries,'_owned_close_view',view):
            worker=threading.Thread(target=lambda:answers.append(f.engine.close_position(self.command(f))))
            worker.start()
            try:
                self.assertTrue(entered.wait(3));self.until(lambda:not f.natives[1].positions)
                self.assertTrue(f.natives[0].positions)
                self.assertTrue(f.engine.shards['900000002']._protecting(f.payload['id']))
                self.assertFalse(f.engine._protecting(f.payload['id']))
            finally:release.set();worker.join(5)
        self.assertFalse(worker.is_alive());self.assertEqual(answers[0]['result']['status'],'ok')
        self.assertEqual([len(native.positions) for native in f.natives],[0,0])
        self.assertEqual(set(answers[0]['result']['accountResults']),{'900000002','900000001'})

    def test_preflight_reads_account_once_with_positions_and_keeps_native_send_checks(self):
        f=self.make();calls=[]
        shard=f.engine.shards['900000001'];original=shard._call
        def record(account,method,*args,**kwargs):
            calls.append((method,args))
            return original(account,method,*args,**kwargs)
        with patch.object(shard,'_call',record):
            result=f.engine.close_position(self.command(f))['result']
        self.assertEqual(result['status'],'ok',result)
        send=next(i for i,(method,args) in enumerate(calls) if method=='execute' and args[0]=='close')
        preflight=calls[:send]
        self.assertEqual([method for method,_ in preflight],['entry_management'])
        self.assertTrue(preflight[0][1][1])
        self.assertEqual(calls[send][1][1]['expectedVolume'],f.payload['chunkVolume'])

    def test_unknown_primary_stops_its_account_but_peer_finishes_without_replay(self):
        f=self.make();f.natives[0].close_unknown=f.natives[0].close_apply_then_unknown=True
        command=self.command(f);response=f.engine.close_position(command)
        self.assertEqual(response['result']['status'],'unknown')
        self.assertEqual(response['result']['accountResults']['900000002']['status'],'unknown')
        self.assertEqual(response['result']['accountResults']['900000001']['status'],'ok')
        self.assertEqual([len(native.positions) for native in f.natives],[1,0])
        counts=[len(native.sent) for native in f.natives]
        self.assertEqual(f.engine.close_position(command)['result'],response['result'])
        next_result=f.engine.close_position(self.command(f,'no-new-id-retry'))['result']
        self.assertEqual(next_result['accountResults']['900000002']['status'],'rejected')
        self.assertEqual(next_result['accountResults']['900000001']['status'],'ok')
        self.assertEqual([len(native.sent) for native in f.natives],counts)

    def test_failed_peer_preflight_never_sends_on_that_account_and_healthy_peer_closes(self):
        f=self.make();f.natives[1].account.currency='EUR'
        before=[len(native.sent) for native in f.natives]
        response=f.engine.close_position(self.command(f))
        self.assertEqual(response['result']['status'],'unknown')
        self.assertFalse(f.natives[0].positions)
        self.assertEqual(len(f.natives[1].sent),before[1])
        self.assertEqual(response['result']['accountResults']['900000002']['status'],'ok')

    def test_absent_peer_position_without_proof_locks_only_that_account(self):
        f=self.make();f.natives[1].positions.pop()
        before=[len(native.sent) for native in f.natives]
        response=f.engine.close_position(self.command(f))
        self.assertEqual(response['result']['status'],'unknown')
        self.assertFalse(f.natives[0].positions)
        self.assertEqual(len(f.natives[1].sent),before[1])

    def test_dispatch_failure_drains_started_account_before_releasing_control(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        pool=ThreadPoolExecutor(max_workers=1);self.addCleanup(pool.shutdown,wait=True)
        class RejectSecond:
            count=0
            def submit(self,fn,*args):
                self.count+=1
                if self.count==2:raise RuntimeError('Synthetic second dispatch failed')
                return pool.submit(fn,*args)
        f.natives[0].before_close=lambda:(entered.set(),release.wait(5))
        answers=[]
        def close():
            try:answers.append(f.engine.close_position(self.command(f)))
            except RuntimeError as error:answers.append(error)
        with patch.object(f.engine,'account_stop_executor',RejectSecond()):
            worker=threading.Thread(target=close)
            worker.start()
            try:
                self.assertTrue(entered.wait(3));self.assertTrue(worker.is_alive())
                self.assertTrue(f.engine.shards['900000002']._protecting(f.payload['id']))
            finally:release.set();worker.join(5)
        self.assertFalse(worker.is_alive());self.assertIsInstance(answers[0],RuntimeError)
        self.assertEqual(f.engine._closure_receipt('exact-close')[1]['status'],'unknown')
        self.assertEqual([len(native.positions) for native in f.natives],[0,2])


if __name__=='__main__':unittest.main()
