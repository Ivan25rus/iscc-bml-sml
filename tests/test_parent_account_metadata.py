"""Coordinator routing identity must never substitute for native account proof."""
import threading
import time
import unittest

import test_isolated_entry_engines as support
from mt5_bridge import CoordinatorBridge, worker_result


class ParentAccountMetadataTests(unittest.TestCase):
    def make(self):
        runner = support.IsolatedEngineTests()
        self.addCleanup(runner.doCleanups)
        fixture = runner.make()
        fixture.engine.close()
        calls = []

        class Worker:
            def call(self, method, *args):
                calls.append(method)
                with fixture.primary.lock:
                    return worker_result(method, getattr(fixture.primary, method)(*args))

        coordinator = CoordinatorBridge(fixture.primary.config, fixture.paths[0], Worker())
        self.addCleanup(coordinator.db.close)
        self.addCleanup(coordinator.close_group_executor)
        self.addCleanup(coordinator.chunk_entries.close)
        coordinator.linked = fixture.primary.linked
        fixture.engine = coordinator.chunk_entries
        return runner, fixture, coordinator, calls

    def test_primary_account_check_reads_verified_native_metadata(self):
        _, fixture, coordinator, calls = self.make()
        self.assertFalse(hasattr(coordinator.account(), 'margin_mode'))
        result = fixture.engine._account('900000002')
        self.assertEqual(result['margin_mode'], 2)
        self.assertIn('account', calls)
        self.assertFalse(any(native.sent for native in fixture.natives))
        self.assertFalse(hasattr(coordinator.account(), 'margin_mode'))

    def test_whole_parent_close_works_through_real_coordinator_facade(self):
        runner, fixture, _, calls = self.make()
        fixture.engine.arm(fixture.payload)
        runner.until(fixture, lambda state: state['phase'] == 'completed')
        before = len(calls)
        result = fixture.engine.close_position(dict(id='close-metadata-proof',
            entryId=fixture.payload['id'], symbol=fixture.payload['symbol'],
            expiresAt=time.time()*1000+3000))
        self.assertEqual(result['result']['status'], 'ok', result['result'])
        self.assertEqual(calls[before:before+2], ['entry_management', 'execute'])
        self.assertNotIn('account', calls[before:])
        self.assertTrue(all(not native.positions for native in fixture.natives))

    def test_invalid_native_account_properties_are_still_rejected(self):
        _, fixture, _, _ = self.make()
        native = fixture.natives[0]
        for name, value, reason in [('margin_mode', 0, 'hedging'),
                                    ('trade_allowed', False, 'algorithmic'),
                                    ('trade_expert', False, 'algorithmic')]:
            old = getattr(native.account, name)
            with self.subTest(name=name):
                setattr(native.account, name, value)
                try:
                    with self.assertRaisesRegex(ValueError, reason):
                        fixture.engine._account('900000002')
                finally:
                    setattr(native.account, name, old)
        self.assertFalse(any(item.sent for item in fixture.natives))

    def test_waiting_primary_metadata_does_not_lock_peer_or_coordinator(self):
        _, fixture, coordinator, _ = self.make()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        original = coordinator.primary_worker.call

        def delayed(method, *args):
            if method == 'account':
                entered.set()
                if not release.wait(2):
                    raise RuntimeError('Test release timed out')
            return original(method, *args)

        coordinator.primary_worker.call = delayed
        failures = []
        def read():
            try:
                fixture.engine._account('900000002')
            except Exception as error:
                failures.append(error)
        thread = threading.Thread(target=read)
        thread.start()
        try:
            self.assertTrue(entered.wait(.5))
            self.assertTrue(coordinator.lock.acquire(blocking=False))
            coordinator.lock.release()
            self.assertEqual(fixture.engine._account('900000001').margin_mode, 2)
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])


if __name__ == '__main__':
    unittest.main()
