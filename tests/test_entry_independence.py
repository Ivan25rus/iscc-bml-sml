"""Real entry state machines and temporary SQLite; no live SDK or network."""
import copy
import json
import pathlib
import sqlite3
import sys
import threading
import time
import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_isolated_entry_engines as support
from chunk_entry import ChunkEntries,IsolatedEntries
from mt5_bridge import CoordinatorBridge,WorkerDispatchNotSent,worker_result,start_account_workers


class RPC:
    def __init__(self,bridge):self.bridge=bridge;self.before=None
    def available(self):return True
    def call(self,method,*args):
        if self.before:self.before(method,args)
        with self.bridge.lock:return worker_result(method,getattr(self.bridge,method)(*args))


class EntryIndependenceTests(unittest.TestCase):
    def model(self):
        f=support.IsolatedEngineTests.make(self);f.engine.close()
        root=CoordinatorBridge(f.primary.config,f.paths[0],RPC(f.primary))
        root.linked={'900000001':RPC(f.bridges[1])}
        self.addCleanup(root.db.close);self.addCleanup(root.close_group_executor)
        self.addCleanup(root.chunk_entries.close)
        f.engine=root.chunk_entries
        return f,root

    def wait(self,predicate,seconds=4):
        end=time.perf_counter()+seconds
        while time.perf_counter()<end:
            if predicate():return
            time.sleep(.002)
        self.fail('Expected independent progress was not observed')

    def filled(self,native):return sum(row.volume for row in native.positions)>=1-1e-8

    def test_status_reader_does_not_own_event_condition_during_snapshot(self):
        f,root=self.model();f.engine.arm(f.payload)
        original=f.engine.list;entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        def listed():entered.set();release.wait(2);return original()
        f.engine.list=listed;errors=[]
        def read():
            try:f.engine.entry_events(-1,timeout=0)
            except Exception as error:errors.append(error)
        thread=threading.Thread(target=read);thread.start()
        try:
            self.assertTrue(entered.wait(1))
            acquired=f.engine.events.acquire(timeout=.2)
            try:self.assertTrue(acquired,'A slow snapshot must not own the event condition')
            finally:
                if acquired:f.engine.events.release()
        finally:release.set();thread.join(3)
        self.assertFalse(thread.is_alive());self.assertFalse(errors)

    def test_status_revision_does_not_acknowledge_a_later_update(self):
        f,root=self.model();before=f.engine.event_revision
        def listed():
            with f.engine.events:f.engine.event_revision+=1
            return []
        with patch.object(f.engine,'list',side_effect=listed):
            response=f.engine.entry_events(-1,timeout=0)
        self.assertEqual(response['revision'],before)
        self.assertGreater(f.engine.event_revision,response['revision'])

    def test_both_accounts_fill_while_shared_projection_is_held(self):
        f,root=self.model();f.engine.start()
        with f.engine.projection_lock:
            f.engine.arm(f.payload)
            self.wait(lambda:all(self.filled(native) for native in f.natives))
        self.assertTrue(all(len(native.positions)==4 for native in f.natives))

    def test_both_accounts_fill_while_ui_event_condition_is_held(self):
        f,root=self.model();f.engine.start()
        with f.engine.events:
            f.engine.arm(f.payload)
            self.wait(lambda:all(self.filled(native) for native in f.natives))

    def test_cold_peer_storage_does_not_hold_acceptance_or_first_account(self):
        f,root=self.model();entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
        original=f.engine._shard
        def shard(account):
            if account=='900000001':entered.set();release.wait(3)
            return original(account)
        with patch.object(f.engine,'_shard',side_effect=shard):
            f.engine.start();began=time.perf_counter();f.engine.arm(f.payload)
            self.assertLess(time.perf_counter()-began,.5)
            try:
                self.assertTrue(entered.wait(1));self.wait(lambda:self.filled(f.natives[0]))
                self.assertFalse(f.natives[1].positions)
            finally:release.set()
            self.wait(lambda:self.filled(f.natives[1]))

    def test_coordinator_writer_does_not_delay_either_native_intent(self):
        f,root=self.model();go=threading.Event();self.addCleanup(go.set)
        entered={account:threading.Event() for account in ('900000002','900000001')}
        for account,rpc in [('900000002',root.primary_worker),('900000001',root.linked['900000001'])]:
            def before(method,args,account=account):
                if method=='execute' and not entered[account].is_set():entered[account].set();go.wait(3)
            rpc.before=before
        f.engine.start();f.engine.arm(f.payload);self.wait(lambda:all(event.is_set() for event in entered.values()))
        database=root.db.execute('PRAGMA database_list').fetchone()[2]
        self.assertNotEqual(pathlib.Path(database).resolve(),f.paths[0].resolve())
        with sqlite3.connect(database,timeout=.2) as writer:
            writer.execute('BEGIN IMMEDIATE');go.set()
            self.wait(lambda:all(self.filled(native) for native in f.natives));writer.rollback()

    def test_each_slow_native_account_leaves_its_peer_free(self):
        for slow in ('900000002','900000001'):
            with self.subTest(account=slow):
                f,root=self.model();entered,release=threading.Event(),threading.Event();self.addCleanup(release.set)
                worker=root.primary_worker if slow=='900000002' else root.linked[slow]
                def before(method,args):
                    if method=='execute':entered.set();release.wait(3)
                worker.before=before
                f.engine.start();f.engine.arm(f.payload)
                try:
                    self.assertTrue(entered.wait(1))
                    healthy=f.natives[1] if slow=='900000002' else f.natives[0]
                    self.wait(lambda:self.filled(healthy))
                    self.assertFalse(worker.bridge.mt5.positions)
                finally:release.set()
                self.wait(lambda:all(self.filled(native) for native in f.natives))

    def test_latched_ready_read_wakes_actor_after_busy_lane_finishes(self):
        f,root=self.model();actor=f.engine._shard('900000001');lane=SimpleNamespace(read_ready=threading.Event())
        lane.read_ready.set();actor.wake_event.clear();future=Future();future.set_result(False)
        actor._lane_finished(future,lane)
        self.assertTrue(actor.wake_event.is_set())

    def test_pending_read_does_not_create_a_polling_spin(self):
        f,root=self.model();actor=f.engine._shard('900000001');lane=SimpleNamespace(read_ready=threading.Event())
        actor.wake_event.clear();future=Future();future.set_result(False)
        actor._lane_finished(future,lane)
        self.assertFalse(actor.wake_event.is_set())

    def test_admission_wakes_its_own_already_started_actor(self):
        f,root=self.model()
        for account in ('900000001','900000002'):f.engine._shard(account)
        f.engine.thread=SimpleNamespace(is_alive=lambda:True)
        try:
            with patch.object(ChunkEntries,'start',return_value=None):
                f.engine.arm(f.payload)
                for future in f.engine.admitting.values():future.result(timeout=3)
            self.assertTrue(all(actor.wake_event.is_set() for actor in f.engine.shards.values()))
        finally:f.engine.thread=None

    def test_native_identity_is_validated_once_within_context_and_quote_is_last(self):
        f,root=self.model();native=f.natives[0];bridge=f.primary;calls=[]
        for name in ('account_info','terminal_info','positions_get','symbol_info_tick'):
            original=getattr(native,name)
            def called(*args,name=name,original=original,**kwargs):calls.append(name);return original(*args,**kwargs)
            setattr(native,name,called)
        view=bridge.entry_context('USTEC',[dict(ticket='999',symbol='USTEC')])
        self.assertEqual(calls.count('account_info'),1);self.assertEqual(calls.count('terminal_info'),1)
        self.assertGreater(calls.index('symbol_info_tick'),calls.index('positions_get'))
        self.assertEqual(view['snapshot']['account'],900000002)
        native.account.login=1
        with self.assertRaises(ValueError):bridge.entry_context('USTEC',[])
        self.assertFalse(native.sent)

    def test_link_cache_reuses_history_but_matches_current_live_row(self):
        f,root=self.model();bridge=f.primary;native=f.natives[0]
        native.history_orders.append(SimpleNamespace(ticket=71,symbol='USTEC',position_id=91))
        row=dict(symbol='USTEC',ticket='81',identifier='91',__isPosition=True,lots=.25)
        with patch.object(native,'history_orders_get',wraps=native.history_orders_get) as history:
            self.assertEqual(bridge.opening_rows('71','USTEC',[row]),[row])
            changed=dict(row,lots=.1,sl=24990.)
            self.assertEqual(bridge.opening_rows('71','USTEC',[changed]),[changed])
            self.assertEqual(history.call_count,1)
            self.assertEqual(bridge.opening_rows('71','USTEC',[]),[])
            self.assertEqual(history.call_count,2,'Absence still needs fresh broker history')

    def test_missing_link_is_not_cached_as_a_final_absence(self):
        f,root=self.model();bridge=f.primary;native=f.natives[0]
        row=dict(symbol='USTEC',ticket='81',identifier='91',__isPosition=True)
        self.assertEqual(bridge.opening_rows('71','USTEC',[row]),[])
        native.history_orders.append(SimpleNamespace(ticket=71,symbol='USTEC',position_id=91))
        self.assertEqual(bridge.opening_rows('71','USTEC',[row]),[row])

    def test_link_cache_never_authorizes_wrong_live_identity(self):
        f,root=self.model();bridge=f.primary;native=f.natives[0]
        native.history_orders.append(SimpleNamespace(ticket=71,symbol='USTEC',position_id=91))
        row=dict(symbol='USTEC',ticket='81',identifier='91',__isPosition=True)
        bridge.opening_rows('71','USTEC',[row])
        self.assertEqual(bridge.opening_rows('71','USTEC',[dict(row,ticket='82',identifier='92')]),[])
        native.history_orders[0].position_id=92
        with self.assertRaisesRegex(ValueError,'link changed'):bridge.opening_rows('71','USTEC',[dict(row,identifier='92')])

    def test_coordinator_migration_is_exact_idempotent_and_native_history_is_unchanged(self):
        f,root=self.model();native_before=list(f.primary.db.iterdump())
        root.chunk_entries.arm(f.payload)
        support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        for job in tuple(f.engine.checkpoint_jobs.values()):job.result(timeout=3)
        roots=list(root.db.execute('SELECT * FROM chunk_entries'))
        self.assertEqual(list(f.primary.db.execute('SELECT * FROM chunk_entries')),[])
        self.assertEqual(len(roots),1)
        root._migrate_coordinator_storage()
        self.assertEqual(list(root.db.execute('SELECT * FROM chunk_entries')),roots)
        # Native trade receipts were added by the synthetic entry, never by migration.
        after_entry=list(f.primary.db.iterdump());root._migrate_coordinator_storage()
        self.assertEqual(list(f.primary.db.iterdump()),after_entry)
        self.assertNotEqual(native_before,after_entry)

    def test_receipts_after_migration_use_the_live_native_ledger(self):
        f,root=self.model();result=dict(status='ok',providerOrderId='123')
        f.primary.db.execute('INSERT INTO requests VALUES (?,?,?)',('new-native','digest',json.dumps(result)));f.primary.db.commit()
        self.assertEqual(root.receipt('new-native')['result'],result)
        self.assertIsNone(root.db.execute('SELECT 1 FROM requests WHERE id=?',('new-native',)).fetchone())

    def test_coordinator_starts_and_healthy_peer_fills_without_primary_identity_call(self):
        f,root=self.model()
        class Unavailable:
            def available(self):return False
            def call(self,*args):raise WorkerDispatchNotSent('starting; request was not sent')
        root.primary_worker=Unavailable()
        health=root.health();self.assertFalse(health['connected']);self.assertFalse(health['brokerIdentityVerified'])
        self.assertEqual(health['accountReadiness'],{'900000002':False,'900000001':True})
        f.engine.start();f.engine.arm(f.payload)
        self.wait(lambda:self.filled(f.natives[1]))
        self.assertFalse(f.natives[0].sent)

    def test_legacy_group_link_and_unknown_receipt_survive_storage_split(self):
        f,root=self.model();request=dict(id='native-group-part')
        stored=dict(operation='place',payload=dict(symbol='USTEC'),requests={'900000002':request})
        result=dict(status='unknown',accountResults={'900000002':dict(status='unknown')})
        root.db.execute('INSERT INTO groups VALUES (?,?,?,?)',('legacy-group','digest',json.dumps(stored),json.dumps(result)));root.db.commit()
        confirmed=dict(status='ok',providerOrderId='81')
        f.primary.db.execute('INSERT INTO requests VALUES (?,?,?)',(request['id'],'digest',json.dumps(confirmed)));f.primary.db.commit()
        self.assertEqual(root.receipt('legacy-group')['result']['status'],'ok')
        rows=[dict(symbol='USTEC',ticket='81',identifier='91')]
        self.assertEqual(root.opening_group('81','USTEC',rows)[0],'legacy-group')

    def test_both_workers_start_without_waiting_for_primary_readiness(self):
        created=[]
        class Starting:
            def __init__(self,config,path):self.config=config;created.append(self)
            def available(self):return False
            def call(self,*args):raise AssertionError('Startup must not wait for broker identity')
            def close(self):pass
        config=dict(account=900000002,terminalPath='first-terminal',linkedAccounts=[dict(account=900000001,terminalPath='second-terminal')])
        primary,linked=start_account_workers(config,pathlib.Path('config.json'),Starting)
        self.assertEqual([str(worker.config['account']) for worker in created],['900000002','900000001'])
        self.assertIs(primary,created[0]);self.assertIs(linked['900000001'],created[1])

    def test_duplicate_terminal_configuration_is_rejected_before_starting_any_worker(self):
        created=[]
        config=dict(account=900000002,terminalPath='same-terminal',linkedAccounts=[dict(account=900000001,terminalPath='same-terminal')])
        with self.assertRaises(ValueError):start_account_workers(config,pathlib.Path('config.json'),lambda *args:created.append(args))
        self.assertFalse(created)

    def test_existing_completed_parent_controls_and_exact_history_migrate_without_native_writes(self):
        f=support.IsolatedEngineTests.make(self);f.engine.arm(f.payload)
        support.IsolatedEngineTests.until(self,f,lambda state:state['phase']=='completed')
        for job in tuple(f.engine.checkpoint_jobs.values()):job.result(timeout=3)
        f.engine.close();before=list(f.primary.db.iterdump())
        parent=f.primary.db.execute('SELECT * FROM chunk_entries').fetchone()
        root=CoordinatorBridge(f.primary.config,f.paths[0],RPC(f.primary));root.linked=f.primary.linked
        self.addCleanup(root.db.close);self.addCleanup(root.close_group_executor);self.addCleanup(root.chunk_entries.close)
        self.assertEqual(root.db.execute('SELECT * FROM chunk_entries').fetchone(),parent)
        self.assertEqual(root.chunk_entries.get(f.payload['id'])['accounts']['900000001']['filledVolume'],1)
        self.assertEqual(list(f.primary.db.iterdump()),before)

    def test_later_legacy_ids_are_imported_without_replacing_newer_projection(self):
        f,root=self.model();state=json.dumps(dict(phase='completed',version='newer'))
        old=json.dumps(dict(phase='completed',version='older'))
        root.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',('same','d','{}',state));root.db.commit()
        f.primary.db.executemany('INSERT INTO chunk_entries VALUES (?,?,?,?)',[('same','d','{}',old),('after-rollback','e','{}',old)]);f.primary.db.commit()
        root._migrate_coordinator_storage()
        self.assertEqual(root.db.execute('SELECT state FROM chunk_entries WHERE id=?',('same',)).fetchone()[0],state)
        self.assertEqual(root.db.execute('SELECT state FROM chunk_entries WHERE id=?',('after-rollback',)).fetchone()[0],old)

    def test_readiness_reports_each_worker_without_a_global_native_read(self):
        f,root=self.model()
        class Starting:
            def available(self):return False
            def call(self,*args):raise AssertionError('Readiness must not enqueue native work')
        root.primary_worker=Starting()
        with patch.object(root.linked['900000001'],'call',side_effect=AssertionError('No native poll')):
            view=f.engine.account_readiness()
        self.assertFalse(view['ready']);self.assertFalse(view['accountStates']['900000002']['ready'])
        self.assertTrue(view['accountStates']['900000001']['ready'])
        self.assertTrue(view['accountStates']['900000001']['nativeProofRequired'])

    def test_shutdown_drains_projection_writer_before_closing_legacy_connection(self):
        f,root=self.model();engine=f.engine;engine._enable_lane_storage()
        real=engine.storage.db;entered,release,closed=threading.Event(),threading.Event(),threading.Event()
        self.addCleanup(release.set)
        class Connection:
            def __getattr__(self,name):return getattr(real,name)
            def close(self):closed.set();return real.close()
        engine.storage.db=Connection()
        def writer():
            entered.set();release.wait(2);return engine.storage.db.execute('SELECT 1').fetchone()
        job=engine.checkpoint_executor.submit(writer);self.assertTrue(entered.wait(1));errors=[]
        def close():
            try:engine.close()
            except Exception as error:errors.append(error)
        thread=threading.Thread(target=close);thread.start()
        try:self.assertFalse(closed.wait(.05),'A pending checkpoint must retain its live connection')
        finally:release.set();thread.join(3)
        self.assertEqual(job.result(timeout=1),(1,));self.assertFalse(errors);self.assertTrue(closed.is_set())


if __name__=='__main__':unittest.main()
