"""Real SQLite WAL fixtures and simulated MT5 orders only."""
import json
import pathlib
import sqlite3
import struct
import threading
import time
import unittest
from unittest.mock import patch

import test_mt5_bridge as fixtures


class LedgerCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.BridgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.bridge = self.fixture.bridge
        self.bridge.entry_diagnostics.last_command_at = time.monotonic()-10
        self.checkpoint = self.bridge.ledger_checkpoint
        self.addCleanup(self.checkpoint.close)

    def start(self):
        self.bridge.native_command_waiting.set()
        self.checkpoint.start()
        self.assertTrue(self.checkpoint.ready.wait(2), self.checkpoint.snapshot())
        self.checkpoint.maintain_writer()
        self.assertTrue(self.checkpoint.active, self.checkpoint.snapshot())
        self.assertEqual(self.bridge.db.execute('PRAGMA wal_autocheckpoint').fetchone()[0], 0)
        self.assertEqual(self.bridge.db.execute('PRAGMA synchronous').fetchone()[0], 2)

    def connection(self):
        connection = sqlite3.connect(self.fixture.file, timeout=0)
        self.addCleanup(connection.close)
        return connection

    def frames(self):
        with pathlib.Path(str(self.fixture.file)+'-shm').open('rb') as stream:
            return struct.unpack_from('=I', stream.read(48), 16)[0]

    def test_large_wal_is_not_checkpointed_during_durable_order_commit(self):
        self.start()
        self.bridge.db.execute('CREATE TABLE checkpoint_fixture(value BLOB)')
        self.bridge.db.execute('INSERT INTO checkpoint_fixture VALUES (?)', (b'x' * (1100*4096),))
        self.bridge.db.commit()
        self.assertGreater(self.frames(), 1000)
        sent = self.fixture.mt5.order_send

        def verify_durable_claim(request):
            with sqlite3.connect(self.fixture.file, timeout=0) as reader:
                result = reader.execute('SELECT result FROM requests WHERE id=?', (self.fixture.order['id'],)).fetchone()
                self.assertEqual(json.loads(result[0])['status'], 'unknown')
            self.assertGreater(self.frames(), 1000)
            return sent(request)

        self.fixture.mt5.order_send = verify_durable_claim
        response = self.bridge.place(self.fixture.order)['result']
        self.assertEqual(response['status'], 'ok')
        self.assertEqual(response['timings']['server']['ledgerAutoCheckpointPages'], 0)
        self.assertGreaterEqual(response['timings']['server']['ledgerCommitCpuMs'], 0)
        self.assertEqual(len(self.fixture.mt5.sent), 1)
        restarted = fixtures.Bridge(self.fixture.mt5, self.fixture.config, self.fixture.file)
        self.addCleanup(restarted.db.close)
        self.assertEqual(restarted.place(self.fixture.order)['result']['status'], 'ok')
        self.assertEqual(len(self.fixture.mt5.sent), 1)

    def test_blocked_checkpoint_holds_no_native_account_lock(self):
        entered, release = threading.Event(), threading.Event()

        class SlowConnection:
            def execute(self, statement):
                self.statement = statement
                entered.set()
                if not release.wait(2):
                    raise RuntimeError('Fixture checkpoint was not released')
                return self

            def fetchone(self):
                return 0, 12, 12

        connection = SlowConnection()
        thread = threading.Thread(target=self.checkpoint.checkpoint, args=(connection,))
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            with self.bridge.lock:
                result = self.bridge.place(self.fixture.order)['result']
            self.assertEqual(result['status'], 'ok')
            self.assertEqual(len(self.fixture.mt5.sent), 1)
            self.assertEqual(connection.statement, 'PRAGMA wal_checkpoint(PASSIVE)')
            self.assertTrue(thread.is_alive())
        finally:
            release.set()
            thread.join(2)

    def test_recent_command_pending_command_and_open_transaction_defer_checkpoint(self):
        connection = self.connection()
        self.bridge.native_command_waiting.set()
        self.checkpoint.checkpoint(connection)
        self.assertIsNone(self.checkpoint.last)
        self.bridge.native_command_waiting.clear()
        self.bridge.entry_diagnostics.last_command_at = time.monotonic()
        self.checkpoint.checkpoint(connection)
        self.assertIsNone(self.checkpoint.last)
        self.bridge.entry_diagnostics.last_command_at = time.monotonic()-10
        self.bridge.db.execute('INSERT INTO requests VALUES (?,?,?)', ('uncommitted', 'digest', '{}'))
        self.checkpoint.checkpoint(connection)
        self.assertIsNone(self.checkpoint.last)
        self.bridge.db.rollback()
        self.checkpoint.checkpoint(connection)
        self.assertIsNotNone(self.checkpoint.last)

    def test_reader_defers_pages_and_same_changes_are_retried_after_reader_finishes(self):
        self.start()
        connection = self.connection()
        reader = self.connection()
        reader.execute('BEGIN')
        reader.execute('SELECT COUNT(*) FROM requests').fetchone()
        self.bridge.db.execute('INSERT INTO requests VALUES (?,?,?)', ('new-row', 'digest', '{}'))
        self.bridge.db.commit()
        self.bridge.native_command_waiting.clear()
        self.checkpoint.checkpoint(connection)
        self.assertLess(self.checkpoint.last['checkpointedFrames'], self.checkpoint.last['frames'])
        self.assertEqual(self.checkpoint.completed_changes, -1)
        reader.rollback()
        self.checkpoint.checkpoint(connection)
        self.assertEqual(self.checkpoint.last['frames'], self.checkpoint.last['checkpointedFrames'])
        self.assertEqual(self.checkpoint.completed_changes, self.bridge.db.total_changes)

    def test_failed_and_delayed_worker_start_recovers_without_disabling_fallback_early(self):
        second_attempt, release = threading.Event(), threading.Event()
        self.checkpoint.RETRY_SECONDS = .01
        connect = sqlite3.connect

        def delayed_connect(*args, **kwargs):
            if self.checkpoint.startup_attempts == 1:
                raise sqlite3.OperationalError('fixture database unavailable')
            second_attempt.set()
            if not release.wait(2):
                raise RuntimeError('Fixture initialization was not released')
            return connect(*args, **kwargs)

        with patch('mt5_bridge.sqlite3.connect', side_effect=delayed_connect):
            self.checkpoint.start()
            try:
                self.assertTrue(second_attempt.wait(2), self.checkpoint.snapshot())
                self.checkpoint.maintain_writer()
                self.assertFalse(self.checkpoint.active)
                self.assertEqual(self.bridge.db.execute('PRAGMA wal_autocheckpoint').fetchone()[0], 1000)
                self.assertIn('fixture database unavailable', self.checkpoint.error)
                result = self.bridge.place(self.fixture.order)['result']
                self.assertEqual(result['status'], 'ok')
                self.assertEqual(result['timings']['server']['ledgerAutoCheckpointPages'], 1000)
            finally:
                release.set()
            self.assertTrue(self.checkpoint.ready.wait(2), self.checkpoint.snapshot())
            self.checkpoint.maintain_writer()
        self.assertEqual(self.checkpoint.startup_attempts, 2)
        self.assertIsNone(self.checkpoint.error)
        self.assertEqual(self.bridge.db.execute('PRAGMA wal_autocheckpoint').fetchone()[0], 0)
        self.assertEqual(self.bridge.db.execute('PRAGMA synchronous').fetchone()[0], 2)

    def test_maintenance_restores_automatic_checkpointing_when_worker_stops(self):
        self.start()
        self.checkpoint.close()
        self.assertFalse(self.checkpoint.active)
        self.checkpoint.maintain_writer()
        self.assertEqual(self.bridge.db.execute('PRAGMA wal_autocheckpoint').fetchone()[0], 1000)

    def test_idle_checkpoint_does_not_repeat_without_new_commits(self):
        connection = self.connection()
        self.checkpoint.checkpoint(connection)
        last = self.checkpoint.last
        self.checkpoint.checkpoint(connection)
        self.assertIs(self.checkpoint.last, last)


if __name__ == '__main__':
    unittest.main()
