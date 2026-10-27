"""Stop/resume reordering through real account engines; synthetic MT5 only."""
import copy
import pathlib
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_account_lanes as lanes
import test_isolated_entry_engines as isolated
from chunk_entry import EntryControlBarrier, IsolatedEntries


class EntryStopPriorityTests(unittest.TestCase):
    def model(self, version=5):
        case = isolated.IsolatedEngineTests() if version == 5 else lanes.AccountLaneTests()
        self.addCleanup(case.doCleanups)
        if version == 4:
            return case, case.stopped_partial()
        f = case.make()
        for native in f.natives: native.after_market = lambda broker: f.quote(broker, 25010.)
        f.engine.arm(f.payload)
        case.until(f, lambda state: all(row['filledVolume'] >= .25 and row['pendingVolume'] > .1
                                       for row in state['accounts'].values()))
        f.engine.stop(dict(id='initial-stop', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
        for native in f.natives:
            native.after_market = None
            f.quote(native, 25002.)
        return case, f

    def continuation(self, f, identifier='late-continue', account='900000001'):
        return dict(id=identifier, entryId=f.payload['id'], account=account, expiresAt=f.now*1000+3000)

    def stop(self, f, identifiers, identifier='priority-stop'):
        return dict(id=identifier, entryId=f.payload['id'], cancelContinueIds=identifiers, expiresAt=f.now*1000+3000)

    def test_stop_before_delivery_revokes_only_old_ids_and_allows_a_new_explicit_resume(self):
        for version in (4, 5):
            with self.subTest(version=version):
                case, f = self.model(version)
                before = copy.deepcopy([native.sent for native in f.natives])
                protection = [[(p.ticket,p.volume,p.sl,p.tp) for p in native.positions] for native in f.natives]
                f.engine.stop(self.stop(f, ['late-continue']))
                with self.assertRaisesRegex(ValueError, 'revoked'):
                    f.engine.continue_account(self.continuation(f))
                self.assertEqual([native.sent for native in f.natives], before)
                self.assertEqual([[(p.ticket,p.volume,p.sl,p.tp) for p in native.positions] for native in f.natives], protection)
                f.engine.continue_account(self.continuation(f, 'new-explicit-resume'))
                case.until(f, lambda state: state['accounts']['900000001']['filledVolume'] == 1)
                self.assertEqual(f.natives[0].sent, before[0], 'A resumed account never adds volume on its peer')

    def test_independent_continuations_do_not_wait_for_the_peer_and_stop_wins_the_pending_one(self):
        case, f = self.model()
        engine = f.engine.shards['900000001']; original = engine._continue_account
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        def delayed(request):
            entered.set();release.wait(15)
            return original(request)
        with ThreadPoolExecutor(3) as pool, patch.object(engine, '_continue_account', delayed):
            pending = pool.submit(f.engine.continue_account, self.continuation(f))
            self.assertTrue(entered.wait(5))
            peer = f.engine.continue_account(self.continuation(f, 'peer-continue', '900000002'))
            self.assertEqual(peer['accounts']['900000001']['filledVolume'], .25)
            case.until(f, lambda state: state.get('lanePhases',{}).get('900000002') == 'completed')
            self.assertFalse(release.is_set(), 'The peer completed while the other continuation still waits')
            before = copy.deepcopy([native.sent for native in f.natives])
            stopping = pool.submit(f.engine.stop, self.stop(f, ['late-continue','peer-continue']))
            until = time.monotonic()+5
            while time.monotonic()<until and 'late-continue' not in f.engine.entry_controls.revoked: time.sleep(.001)
            self.assertTrue(f.engine._stopping(f.payload['id']))
            self.assertIn('late-continue', f.engine.entry_controls.revoked)
            self.assertFalse(stopping.done())
            release.set()
            with self.assertRaisesRegex(ValueError, 'revoked'): pending.result(timeout=10)
            self.assertEqual(stopping.result(timeout=10)['phase'], 'stopped')
            for _ in range(4): f.engine.tick()
            self.assertEqual([native.sent for native in f.natives], before)

    def test_revocation_survives_receiver_restart_without_replaying_any_order(self):
        _, f = self.model()
        f.engine.stop(self.stop(f, ['late-continue']))
        before = copy.deepcopy([native.sent for native in f.natives])
        f.engine.close();f.engine = IsolatedEntries(f.primary);f.primary.chunk_entries = f.engine
        self.addCleanup(f.engine.close)
        with self.assertRaisesRegex(ValueError, 'revoked'):
            f.engine.continue_account(self.continuation(f))
        self.assertEqual([native.sent for native in f.natives], before)

    def test_accepted_priority_stop_finishes_after_its_admission_deadline_but_expired_arrival_is_refused(self):
        _, f = self.model()
        original = f.engine._stop_locked
        def delayed(*args):
            with patch('time.time', return_value=f.now+10): return original(*args)
        with patch.object(f.engine, '_stop_locked', delayed):
            self.assertEqual(f.engine.stop(self.stop(f, ['late-continue']))['phase'], 'stopped')
        request = self.stop(f, ['not-revoked'], 'expired-stop');request['expiresAt'] = f.now*1000-1
        with self.assertRaisesRegex(ValueError, 'expired'): f.engine.stop(request)
        self.assertNotIn('not-revoked', f.engine.entry_controls.revoked)

    def test_malformed_or_cross_parent_revocations_are_rejected_and_never_widen_scope(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        db = sqlite3.connect(str(pathlib.Path(directory.name)/'controls.sqlite3'), check_same_thread=False)
        self.addCleanup(db.close)  # Windows must close SQLite before removing its directory.
        barrier = EntryControlBarrier(SimpleNamespace(lock=threading.RLock(), db=db))
        for value in ('id', [None], ['duplicate','duplicate'], ['bad id'], ['x']*2049):
            with self.assertRaises(ValueError): barrier.revoke(dict(id='stop',entryId='parent',cancelContinueIds=value))
        self.assertEqual(barrier.revoked,{})
        barrier.revoke(dict(id='stop',entryId='parent',cancelContinueIds=['one']))
        with self.assertRaisesRegex(ValueError,'another entry'):
            barrier.revoke(dict(id='other-stop',entryId='another',cancelContinueIds=['one','two']))
        self.assertEqual(barrier.revoked,{'one':'parent'})
        with barrier.continuing(dict(id='two',entryId='another')): pass
        self.assertEqual(barrier.running,{})

    def test_priority_contract_is_exposed_for_both_independent_architectures(self):
        for version in (4,5):
            _, f = self.model(version)
            self.assertEqual(f.state()['controlContract'],'entry-stop-priority-v1')
            self.assertIn('entry-stop-priority-v1',f.primary._capabilities())


if __name__ == '__main__': unittest.main()
