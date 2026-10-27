"""Native quote-wait proof and bounded timing evidence; fake MT5 only."""
import copy
import pathlib
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_bridge as bridge_fixtures
import test_hybrid_protection as protection_fixtures


class QuoteWait39BridgeTests(unittest.TestCase):
    def fixture(self):
        fixture = bridge_fixtures.HybridBridgeTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        return fixture

    def test_stale_common_stop_has_durable_no_mutation_proof_and_no_unknown_target(self):
        f = self.fixture()
        f.mt5.tick.time = time.time()-2
        payload = f.payload()
        result = f.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'], 'rejected')
        self.assertIs(result['retryableQuoteWait'], True)
        self.assertIs(result['mutationStarted'], False)
        self.assertEqual(result['account'], '900000002')
        self.assertEqual(result['symbol'], 'USTEC')
        self.assertGreater(result['quoteWait']['nativeAgeMs'], 1000)
        self.assertEqual(f.mt5.sent, [])
        self.assertEqual(f.bridge.db.execute('SELECT COUNT(*) FROM entry_common_stop_targets').fetchone()[0], 0)
        self.assertEqual(f.bridge.entry_common_stop(payload), result)
        f.mt5.tick.time = time.time()
        fresh = {**payload, 'id': 'fresh-after-wait'}
        self.assertEqual(f.bridge.entry_common_stop(fresh)['status'], 'ok')
        self.assertEqual(len(f.mt5.sent), 2)

    def test_invalid_quote_and_other_refusals_never_claim_retry_proof(self):
        for change in ('invalid-bid', 'crossed', 'invalid-time', 'budget'):
            with self.subTest(change=change):
                f = self.fixture()
                payload = f.payload()
                if change == 'invalid-bid': f.mt5.tick.bid = float('nan')
                elif change == 'crossed': f.mt5.tick.ask = f.mt5.tick.bid-1
                elif change == 'invalid-time': f.mt5.tick.time = float('nan')
                else: payload['riskBudget'] = 31
                result = f.bridge.entry_common_stop(payload)
                self.assertEqual(result['status'], 'rejected')
                self.assertNotIn('retryableQuoteWait', result)
                self.assertEqual(f.mt5.sent, [])

    def test_unknown_or_claimed_native_failure_has_no_retry_proof(self):
        f = self.fixture()
        f.mt5.order_send = lambda request: None
        result = f.bridge.entry_common_stop(f.payload())
        self.assertEqual(result['status'], 'unknown')
        self.assertNotIn('retryableQuoteWait', result)
        self.assertEqual(f.bridge.db.execute('SELECT COUNT(*) FROM entry_common_stop_targets').fetchone()[0], 2)

    def test_millisecond_broker_clock_and_native_capture_do_not_refresh_old_ticks(self):
        f = self.fixture()
        f.bridge.broker_utc_offset_seconds = 10800
        now = time.time()
        f.mt5.tick.time_msc = int((now+10800-2)*1000)
        with patch.object(f.mt5, 'symbol_info_tick', wraps=f.mt5.symbol_info_tick) as read:
            first = f.bridge.snapshot('USTEC')
            second = f.bridge.snapshot('USTEC')
        self.assertEqual(read.call_count, 2)
        self.assertEqual(first['quote']['utcTime'], second['quote']['utcTime'])
        self.assertGreater(first['quote']['nativeAgeMs'], 1000)
        self.assertGreaterEqual(second['quote']['nativeCapturedAt'], first['quote']['nativeCapturedAt'])
        self.assertGreaterEqual(second['quote']['nativeAgeMs'], first['quote']['nativeAgeMs'])
        self.assertEqual(f.mt5.sent, [])

    def test_last_stale_survives_fresh_reads_and_diagnostics_copy_is_independent(self):
        f = self.fixture()
        f.mt5.tick.time = time.time()-2
        f.bridge.snapshot('USTEC')
        stale = copy.deepcopy(f.bridge.quote_freshness['USTEC']['lastStale'])
        f.mt5.tick.time = time.time()
        for _ in range(3): f.bridge.snapshot('USTEC')
        report = f.bridge.diagnostics()['quoteFreshness']
        self.assertEqual(report['USTEC']['samples'], 4)
        self.assertEqual(report['USTEC']['staleSamples'], 1)
        self.assertEqual(report['USTEC']['lastStale'], stale)
        self.assertLess(report['USTEC']['latest']['nativeAgeMs'], 1000)
        report['USTEC']['lastStale']['source'] = 'modified-copy'
        self.assertEqual(f.bridge.quote_freshness['USTEC']['lastStale'], stale)
        self.assertEqual(len(f.bridge.quote_freshness), 1)

    def test_linked_snapshot_diagnostics_are_read_only_and_preserve_account_identity(self):
        f = protection_fixtures.HybridProtectionTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.make(accounts=2)
        f.start_http()
        status, data = f.request_api('GET', '/accounts/900000001/snapshot?symbol=USTEC')
        self.assertEqual(status, 200)
        self.assertEqual(str(data['account']), '900000001')
        self.assertEqual(data['server'], f.primary.config['server'])
        self.assertIn('nativeCapturedAt', data['quote'])
        self.assertIn('nativeAgeMs', data['quote'])
        status, invalid = f.request_api('GET', '/accounts/900000001/snapshot?symbol=FORBIDDEN')
        self.assertEqual(status, 400)
        self.assertEqual(f.natives[0].sent, [])
        self.assertEqual(f.natives[1].sent, [])


if __name__ == '__main__':
    unittest.main()
