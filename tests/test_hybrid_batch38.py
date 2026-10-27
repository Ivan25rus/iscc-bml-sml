"""Fresh internal entry batches and cancellation proof; fake terminals only."""
import copy
import json
import pathlib
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_mt5_bridge import FakeMT5
import test_hybrid_protection as protection_fixture
from mt5_bridge import Bridge, ENTRY_PROTOCOL_CAPABILITIES
from chunk_entry import ChunkEntries


class HybridBatch38Tests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.native = FakeMT5()
        self.native.account.margin_mode = 2
        self.config = dict(account=900000002, server='Example-MT5', symbols=['USTEC'], tradingEnabled=True)
        self.bridge = Bridge(self.native, self.config, pathlib.Path(self.directory.name)/'primary.sqlite')
        self.addCleanup(self.bridge.db.close)
        self.addCleanup(self.bridge.close_group_executor)

    def fixture(self):
        fixture = protection_fixture.HybridProtectionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.make(accounts=2)
        return fixture

    def test_entry_snapshot_reads_account_and_terminal_once_and_a_fresh_quote_each_time(self):
        with patch.object(self.native, 'account_info', wraps=self.native.account_info) as account, \
                patch.object(self.native, 'terminal_info', wraps=self.native.terminal_info) as terminal, \
                patch.object(self.native, 'symbol_info_tick', wraps=self.native.symbol_info_tick) as tick:
            first = self.bridge.entry_snapshot('USTEC')
            self.native.tick.ask += .1
            second = self.bridge.entry_snapshot('USTEC')
        self.assertEqual((account.call_count, terminal.call_count, tick.call_count), (2, 2, 2))
        self.assertNotEqual(first['quote']['ask'], second['quote']['ask'])
        self.assertEqual(second['account'], 900000002)
        self.assertEqual(self.native.sent, [])

    def test_entry_snapshot_preserves_every_account_permission_check_before_reading_quote(self):
        cases = [('login', 999), ('server', 'other'), ('currency', 'EUR'),
                 ('margin_mode', 0), ('trade_allowed', False), ('trade_expert', False)]
        for key, value in cases:
            with self.subTest(key=key), patch.object(self.native.account, key, value), \
                    patch.object(self.native, 'symbol_info_tick', side_effect=AssertionError('Read quote before rejecting account')):
                with self.assertRaises(ValueError):
                    self.bridge.entry_snapshot('USTEC')
        with patch.object(self.native.terminal, 'connected', False):
            with self.assertRaises(ValueError):
                self.bridge.entry_snapshot('USTEC')

    def test_shared_snapshot_parser_keeps_the_original_freshness_and_identity_limits(self):
        good = self.bridge.entry_snapshot('USTEC')
        for age_ms in (-251, 1001):
            data = copy.deepcopy(good)
            data['quote']['utcTime'] = time.time()-age_ms/1000.
            with self.subTest(age_ms=age_ms), self.assertRaisesRegex(ValueError, 'stale'):
                ChunkEntries._quote_snapshot('900000002', data)
        with self.assertRaisesRegex(ValueError, 'account mismatch'):
            ChunkEntries._quote_snapshot('900000001', good)

    def test_order_specs_are_reused_only_inside_one_fresh_read_batch(self):
        self.native.positions.append(N(**{**vars(self.native.positions[0]), 'ticket': 72}))
        with patch.object(self.native, 'symbol_info', wraps=self.native.symbol_info) as metadata:
            first = self.bridge.orders()
            self.assertEqual(metadata.call_count, 1)
            self.native.info.volume_step = .25
            second = self.bridge.orders()
            self.assertEqual(metadata.call_count, 2)
        self.assertTrue(all(row['quantityStep'] == .1 for row in first['orders']))
        self.assertTrue(all(row['quantityStep'] == .25 for row in second['orders']))
        self.assertEqual(self.native.sent, [])

    def test_order_specs_still_read_each_distinct_symbol_and_preserve_native_rows(self):
        self.config['symbols'].append('SPX500')
        self.native.positions.append(N(**{**vars(self.native.positions[0]), 'symbol': 'SPX500', 'ticket': 72}))
        with patch.object(self.native, 'symbol_info', wraps=self.native.symbol_info) as metadata:
            rows = self.bridge.orders()['orders']
        self.assertEqual(metadata.call_count, 2)
        self.assertEqual({row['ticket'] for row in rows}, {'71', '72', '81'})
        self.assertEqual({row['symbol'] for row in rows}, {'USTEC', 'SPX500'})

    def test_status_listing_matches_full_management_for_armed_completed_and_closed_entries(self):
        fixture = self.fixture()
        fixture.engine.arm(fixture.payload)
        self.assertEqual(fixture.primary.entry_plans()['entries'], fixture.primary.management()['entryPlans'])
        fixture.tick()
        self.assertEqual(fixture.state()['phase'], 'completed')
        self.assertEqual(fixture.primary.entry_plans()['entries'], fixture.primary.management()['entryPlans'])
        for native in fixture.natives:
            native.positions = []
        self.assertEqual(fixture.primary.entry_plans()['entries'], [])
        self.assertEqual(fixture.primary.management()['entryPlans'], [])

    def test_status_listing_matches_full_management_for_stopped_partial_parent(self):
        fixture = self.fixture()
        fixture.entry(active=True)
        state = fixture.engine.stop(dict(id='stop-partial', entryId=fixture.payload['id'], expiresAt=fixture.now*1000+2000))
        self.assertEqual(state['phase'], 'stopped')
        narrow = fixture.primary.entry_plans()['entries']
        self.assertEqual(narrow, fixture.primary.management()['entryPlans'])
        self.assertTrue(narrow[0]['hasOpenPositions'])
        self.assertEqual(len(narrow[0]['livePositions']), 2)

    def test_status_listing_defers_extra_native_scans_during_pending_cancellation_reconciliation(self):
        fixture = self.fixture()
        fixture.natives[1].cancel_unknown = True
        fixture.engine.arm(fixture.payload)
        fixture.tick()
        self.assertEqual(fixture.state()['phase'], 'paused')
        with patch.object(fixture.primary, 'entry_management', side_effect=AssertionError('extra native scan')):
            narrow = fixture.primary.entry_plans()['entries']
        self.assertEqual(narrow[0]['phase'], 'paused')
        self.assertTrue(narrow[0]['lifecycleDeferred'])
        self.assertFalse(narrow[0]['positionsConfirmed'])
        self.assertEqual(narrow[0]['accounts'], fixture.state()['accounts'])

    def test_status_listing_never_collects_unrelated_display_history_or_receipts(self):
        fixture = self.fixture()
        fixture.engine.arm(fixture.payload)
        with patch.object(fixture.primary, 'management', side_effect=AssertionError('Full management')), \
                patch.object(fixture.primary, 'decorated_orders', side_effect=AssertionError('Display history')), \
                patch.object(fixture.primary, 'time_rules', side_effect=AssertionError('Time rule history')), \
                patch.object(fixture.primary, 'receipt', side_effect=AssertionError('Receipt collection')):
            result = fixture.primary.entry_plans()
        self.assertEqual(result['entries'][0]['phase'], 'armed')
        self.assertTrue(all(not native.sent for native in fixture.natives))

    def test_unavailable_or_wrong_linked_account_keeps_positions_uncertain_and_never_hides_parent(self):
        fixture = self.fixture()
        fixture.entry()
        worker = fixture.primary.linked['900000001']
        for response in (None, dict(account=999, orders=[]), dict(account=900000001)):
            with self.subTest(response=response):
                behavior = dict(side_effect=RuntimeError('Unavailable')) if response is None else dict(return_value=response)
                with patch.object(worker, 'call', **behavior):
                    entry = fixture.primary.entry_plans()['entries'][0]
                self.assertFalse(entry['positionsConfirmed'])
                self.assertIsNone(entry['hasOpenPositions'])
                self.assertEqual(entry['phase'], 'completed')
        fixture.natives[0].terminal.connected = False
        with self.assertRaises(ValueError):
            fixture.primary.entry_plans()

    def test_new_cancellation_proof_is_durable_and_blocks_delayed_arm_across_restart(self):
        fixture = self.fixture()
        command = dict(id='cancel-unadmitted', entryId=fixture.payload['id'], expiresAt=fixture.now*1000+2000)
        cancelled = fixture.engine.stop(command)
        self.assertTrue(cancelled['cancelledBeforeAdmission'])
        self.assertEqual((cancelled['phase'], cancelled['accounts'], cancelled['children']), ('stopped', {}, []))
        self.assertIsNone(fixture.engine._load(fixture.payload['id']))
        restored = ChunkEntries(fixture.primary)
        self.assertEqual(restored.get(fixture.payload['id']), cancelled)
        self.assertEqual(restored.arm(fixture.payload), cancelled)
        self.assertEqual(restored.stop(command), cancelled)
        self.assertTrue(all(not native.sent for native in fixture.natives))

    def test_existing_parent_and_legacy_tombstone_never_gain_new_cancel_proof(self):
        fixture = self.fixture()
        fixture.engine.arm(fixture.payload)
        state = fixture.engine.stop(dict(id='stop-admitted', entryId=fixture.payload['id'], expiresAt=fixture.now*1000+2000))
        self.assertNotIn('cancelledBeforeAdmission', state)
        legacy = dict(id='legacy', phase='stopped', accounts={}, children=[], reason='Legacy cancelled')
        fixture.primary.db.execute('INSERT INTO chunk_entry_stops VALUES (?,?,?)', ('legacy-stop', 'old', json.dumps(legacy)))
        fixture.primary.db.execute('INSERT INTO chunk_entry_cancels VALUES (?,?)', ('legacy', 'legacy-stop'))
        fixture.primary.db.commit()
        state = fixture.engine.stop(dict(id='stop-legacy-again', entryId='legacy', expiresAt=fixture.now*1000+2000))
        self.assertNotIn('cancelledBeforeAdmission', state)
        self.assertNotIn('cancelledBeforeAdmission', fixture.engine.get('legacy'))

    def test_health_advertises_versioned_batch_and_cancel_proof_without_losing_prior_capabilities(self):
        health = self.bridge.health()
        self.assertEqual(health['version'], 39)
        self.assertTrue(set(ENTRY_PROTOCOL_CAPABILITIES).issubset(health['capabilities']))
        self.assertTrue({'hybrid-entry-speed-v1', 'hybrid-market-limit-v1', 'hybrid-position-protection-v1'}
                        .issubset(health['capabilities']))


class HybridBatch38HttpTests(unittest.TestCase):
    def test_exact_get_and_stop_reply_authenticate_new_proof_and_delayed_arm_stays_stopped(self):
        fixture = protection_fixture.HybridProtectionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.make(accounts=2)
        fixture.start_http()
        command = dict(id='cancel-unadmitted', entryId=fixture.payload['id'], expiresAt=fixture.now*1000+2000)
        status, stopped = fixture.request_api('POST', '/entry-plan-stop', command)
        self.assertEqual(status, 200)
        self.assertEqual(stopped['version'], 39)
        self.assertIn('entry-admission-cancel-proof-v1', stopped['capabilities'])
        self.assertEqual(stopped['capabilities'], fixture.primary.health()['capabilities'])
        self.assertTrue(stopped['entry']['cancelledBeforeAdmission'])
        status, recovered = fixture.request_api('GET', '/entry-plan?id='+fixture.payload['id'])
        self.assertEqual(status, 200)
        self.assertEqual(recovered['version'], 39)
        self.assertIn('entry-admission-cancel-proof-v1', recovered['capabilities'])
        self.assertEqual(recovered['capabilities'], fixture.primary.health()['capabilities'])
        self.assertEqual(recovered['entry'], stopped['entry'])
        status, delayed = fixture.request_api('POST', '/entry-plan', fixture.payload)
        self.assertEqual(status, 200)
        self.assertEqual(delayed['entry'], stopped['entry'])
        status, listing = fixture.request_api('GET', '/entry-plans')
        self.assertEqual(status, 200)
        self.assertEqual(listing['entries'], [])
        self.assertTrue(all(not native.sent for native in fixture.natives))


if __name__ == '__main__':
    unittest.main()
