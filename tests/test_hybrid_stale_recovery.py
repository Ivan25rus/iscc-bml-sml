"""Stale-quote entry recovery against in-memory brokers; no live MT5 calls."""
import copy
import math
import pathlib
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_hybrid_entry import hybrid_fixture
from chunk_entry import ChunkEntries


class HybridStaleRecoveryTests(unittest.TestCase):
    def fixture(self, side):
        primary, follower, payload = hybrid_fixture(side)
        engine = ChunkEntries(primary)
        self.addCleanup(primary.db.close)
        self.addCleanup(follower.db.close)
        self.addCleanup(engine.close)
        return primary, follower, payload, engine

    def assert_protected_exposure(self, bridge, side, expected_volume):
        positions = [row for row in bridge.rows if row['__isPosition']]
        self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
        volume = sum(row['lots'] for row in positions)
        self.assertAlmostEqual(volume, expected_volume)
        average = sum(row['lots'] * row['open_price'] for row in positions) / volume
        direction = 1 if side == 'buy' else -1
        raw_stop = average - direction * 16.
        expected_stop = (math.ceil(raw_stop / .1 - 1e-8) if direction == 1
                         else math.floor(raw_stop / .1 + 1e-8)) * .1
        for row in positions:
            self.assertGreater(row['sl'], 0)
            self.assertGreater(direction * (row['open_price'] - row['sl']), 0)
            self.assertAlmostEqual(row['sl'], expected_stop)
        for observation in bridge.observations:
            self.assertLessEqual(observation['volume'], bridge.target_volume + 1e-8)
            self.assertLessEqual(observation['risk'], bridge.risk_budget + 1e-8)

    def assert_initial_stale_waits_and_recovers(self, side):
        for stale_account in ('primary', 'follower'):
            with self.subTest(side=side, stale_account=stale_account):
                primary, follower, payload, engine = self.fixture(side)
                stale_bridge = primary if stale_account == 'primary' else follower
                stale_bridge.stale = True
                result = engine.arm(payload)
                self.assertEqual(result['phase'], 'paused')
                self.assertIn('Broker quote is stale', result['reason'])
                self.assertEqual(result['quoteWait']['stage'], 'admission')
                self.assertEqual(result['quoteWait']['account'], str(stale_bridge.login.login))
                self.assertGreater(result['quoteWait']['ageMs'], 1000)
                self.assertEqual(primary.db.execute('SELECT COUNT(*) FROM chunk_entries').fetchone()[0], 1)
                self.assertEqual(engine.active_ids, {payload['id']})
                engine.tick()
                before = primary.db.total_changes
                for _ in range(3):
                    engine.tick()
                self.assertEqual(primary.db.total_changes, before, 'An unchanged wait must not commit on every tick')
                for bridge in (primary, follower):
                    self.assertEqual(bridge.sent, [])
                    self.assertEqual(bridge.rows, [])
                    self.assertEqual(bridge.cancelled, [])
                    self.assertEqual(bridge.common_calls, [])
                stale_bridge.stale = False
                # The original accepted command is enough, even after the
                # short HTTP command expiry. Never press or arm a second time.
                with patch('chunk_entry.time.time', return_value=time.time()+4):
                    engine.tick()
                result = engine.get(payload['id'])
                self.assertEqual(result['phase'], 'completed')
                self.assertEqual(result['quoteWait']['status'], 'resumed')
                self.assertEqual(primary.db.execute('SELECT COUNT(*) FROM chunk_entries').fetchone()[0], 1)
                for bridge in (primary, follower):
                    self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
                    self.assert_protected_exposure(bridge, side, 1.)

    def assert_partial_stale_recovers(self, side):
        for stale_account in ('primary', 'follower'):
            with self.subTest(side=side, stale_account=stale_account):
                primary, follower, payload, engine = self.fixture(side)
                stale_bridge = primary if stale_account == 'primary' else follower
                engine.arm(payload)
                primary.market_hook = lambda *_: setattr(stale_bridge, 'stale', True)
                engine.tick()
                paused = engine.get(payload['id'])
                self.assertEqual(paused['phase'], 'paused')
                self.assertIn('Broker quote is stale', paused['reason'])
                held_tickets = {}
                for bridge in (primary, follower):
                    self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
                    self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'limit'], [.7])
                    self.assertEqual(len(bridge.cancelled), 1)
                    self.assert_protected_exposure(bridge, side, .3)
                    held_tickets[bridge.login.login] = {row['ticket'] for row in bridge.rows}
                before = [(len(bridge.sent), len(bridge.cancelled), len(bridge.common_calls))
                          for bridge in (primary, follower)]
                for _ in range(3):
                    engine.tick()
                    self.assertEqual(engine.get(payload['id'])['phase'], 'paused')
                self.assertEqual(before, [(len(bridge.sent), len(bridge.cancelled), len(bridge.common_calls))
                                          for bridge in (primary, follower)])
                primary.market_hook = None
                stale_bridge.stale = False
                engine.tick()
                completed = engine.get(payload['id'])
                self.assertEqual(completed['phase'], 'completed')
                self.assertEqual(primary.db.execute('SELECT COUNT(*) FROM chunk_entries').fetchone()[0], 1)
                for bridge in (primary, follower):
                    self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
                    self.assertTrue(held_tickets[bridge.login.login].issubset({row['ticket'] for row in bridge.rows}))
                    self.assert_protected_exposure(bridge, side, 1.)
                    details = completed['accounts'][str(bridge.login.login)]
                    self.assertAlmostEqual(details['filledVolume'], 1.)
                    self.assertLessEqual(details['riskUsed'], details['riskUsd'] + 1e-8)

    def test_buy_initial_stale_is_durable_and_resumes_without_new_arm(self):
        self.assert_initial_stale_waits_and_recovers('buy')

    def test_sell_initial_stale_is_durable_and_resumes_without_new_arm(self):
        self.assert_initial_stale_waits_and_recovers('sell')

    def test_buy_partial_stale_cancels_support_and_resumes_protected(self):
        self.assert_partial_stale_recovers('buy')

    def test_sell_partial_stale_cancels_support_and_resumes_protected(self):
        self.assert_partial_stale_recovers('sell')

    def test_initial_wait_can_be_cancelled_on_both_sides_and_never_restarts(self):
        for side in ('buy', 'sell'):
            for account in ('primary', 'follower'):
                with self.subTest(side=side, account=account):
                    primary, follower, payload, engine = self.fixture(side)
                    stale = primary if account == 'primary' else follower
                    stale.stale = True
                    engine.arm(payload)
                    stopped = engine.stop(dict(id='cancel-wait', entryId=payload['id'], expiresAt=time.time()*1000+3000))
                    self.assertEqual(stopped['phase'], 'stopped')
                    stale.stale = False
                    engine.tick()
                    self.assertEqual(engine.arm(payload)['phase'], 'stopped')
                    self.assertEqual(primary.sent+follower.sent, [])
                    self.assertEqual(primary.cancelled+follower.cancelled, [])

    def test_initial_wait_stops_at_deadline_without_any_submission(self):
        for side in ('buy', 'sell'):
            primary, follower, payload, engine = self.fixture(side)
            primary.stale = True
            engine.arm(payload)
            with patch('chunk_entry.time.time', return_value=payload['closeAtUtc']/1000+1):
                engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'stopped')
            primary.stale = False
            engine.tick()
            self.assertEqual(primary.sent+follower.sent, [])

    def test_initial_wait_does_not_resume_after_receiver_restart(self):
        primary, follower, payload, engine = self.fixture('buy')
        follower.stale = True
        engine.arm(payload)
        engine.close()
        replacement = ChunkEntries(primary)
        self.addCleanup(replacement.close)
        self.assertEqual(replacement.get(payload['id'])['phase'], 'stopped')
        follower.stale = False
        replacement.tick()
        self.assertEqual(primary.sent+follower.sent, [])

    def test_initial_stale_malformed_quote_metadata_or_plan_is_still_rejected(self):
        cases = [('quote', 'bid', 0), ('quote', 'ask', float('nan')),
                 ('quote', 'utcTime', float('nan')), ('metadata', 'contractSize', 0),
                 ('metadata', 'tickSize', 0)]
        for section, key, value in cases:
            with self.subTest(section=section, key=key):
                primary, follower, payload, engine = self.fixture('buy')
                primary.stale = True
                original = primary.snapshot
                def invalid(symbol):
                    data = original(symbol)
                    data[section][key] = value
                    return data
                primary.snapshot = invalid
                with self.assertRaises(ValueError):
                    engine.arm(payload)
                self.assertEqual(engine.get(payload['id']), None)
                self.assertEqual(primary.sent+follower.sent, [])
        for change in (dict(riskUsd=1.), dict(volume=1.05), dict(chunkVolume=.4),
                       dict(expiresAt=time.time()*1000-1), dict(closeAtUtc=time.time()*1000-1)):
            with self.subTest(change=change):
                primary, follower, payload, engine = self.fixture('buy')
                primary.stale = True
                payload.update(change)
                with self.assertRaises(ValueError):
                    engine.arm(payload)
                self.assertEqual(engine.get(payload['id']), None)

    def test_initial_stale_still_checks_real_account_permissions_and_read_only(self):
        for account in ('primary', 'follower'):
            for field, value in (('login', 999), ('server', 'wrong'), ('currency', 'EUR'),
                                 ('margin_mode', 0), ('trade_allowed', False), ('trade_expert', False)):
                with self.subTest(account=account, field=field):
                    primary, follower, payload, engine = self.fixture('buy')
                    stale = primary if account == 'primary' else follower
                    stale.stale = True
                    setattr(stale.login, field, value)
                    with self.assertRaises(ValueError):
                        engine.arm(payload)
                    self.assertEqual(engine.get(payload['id']), None)
            primary, follower, payload, engine = self.fixture('buy')
            stale = primary if account == 'primary' else follower
            stale.stale = True
            stale.config['tradingEnabled'] = False
            with self.assertRaisesRegex(ValueError, 'Read only'):
                engine.arm(payload)
            self.assertEqual(engine.get(payload['id']), None)

    def test_fresh_quote_after_wait_rechecks_zone_and_uses_full_boundary_limit(self):
        for side in ('buy', 'sell'):
            primary, follower, payload, engine = self.fixture(side)
            primary.stale = True
            engine.arm(payload)
            primary.stale = False
            for bridge in (primary, follower):
                bridge.price = dict(ask=99., bid=98.8) if side == 'buy' else dict(bid=101., ask=101.2)
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'armed')
            self.assertEqual(primary.sent+follower.sent, [])
            for bridge in (primary, follower):
                bridge.price = dict(ask=107., bid=106.8) if side == 'buy' else dict(bid=93., ask=93.2)
            engine.tick()
            for bridge in (primary, follower):
                self.assertEqual([(row['type'], row['volume'], row['price']) for row in bridge.sent],
                                 [('limit', 1., 106. if side == 'buy' else 94.)])
                bridge.fill_limit(1.)
            engine.pending_poll_at.clear()
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'completed')
            for bridge in (primary, follower):
                self.assert_protected_exposure(bridge, side, 1.)

    def test_native_support_stale_rejection_waits_before_any_market(self):
        for side in ('buy', 'sell'):
            primary, follower, payload, engine = self.fixture(side)
            original = primary.group_execute
            def reject(operation, request, **options):
                primary.stale = True
                return dict(result=dict(status='rejected', reason='Chunk tick is stale; no submission'))
            primary.group_execute = reject
            engine.arm(payload)
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'paused')
            self.assertEqual(primary.sent+follower.sent, [])
            before = copy.deepcopy(engine.get(payload['id'])['children'])
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['children'], before)
            primary.group_execute = original
            primary.stale = False
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'completed')
            for bridge in (primary, follower):
                self.assert_protected_exposure(bridge, side, 1.)

    def test_stale_during_cancel_fill_race_never_adds_volume_after_target(self):
        for side in ('buy', 'sell'):
            primary, follower, payload, engine = self.fixture(side)
            for bridge in (primary, follower):
                bridge.fill_on_cancel = True
            primary.market_hook = lambda *_: setattr(follower, 'stale', True)
            engine.arm(payload)
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'completed')
            before = [copy.deepcopy(bridge.sent) for bridge in (primary, follower)]
            follower.stale = False
            engine.tick()
            self.assertEqual([bridge.sent for bridge in (primary, follower)], before)
            for bridge in (primary, follower):
                self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
                self.assert_protected_exposure(bridge, side, 1.)

    def test_stale_with_unknown_support_cancellation_remains_locked_after_fresh_quote(self):
        primary, follower, payload, engine = self.fixture('buy')
        primary.market_hook = lambda *_: setattr(follower, 'stale', True)
        follower.cancel_unknown = True
        engine.arm(payload)
        engine.tick()
        self.assertEqual(engine.get(payload['id'])['phase'], 'unknown')
        before = [copy.deepcopy(bridge.sent) for bridge in (primary, follower)]
        follower.cancel_unknown = follower.stale = False
        engine.tick()
        self.assertEqual([bridge.sent for bridge in (primary, follower)], before)

    def test_cancel_before_delayed_stale_arm_wins_without_quote_or_orders(self):
        primary, follower, payload, engine = self.fixture('buy')
        stopped = engine.stop(dict(id='before-stale-arm', entryId=payload['id'], expiresAt=time.time()*1000+3000))
        self.assertEqual(stopped['phase'], 'stopped')
        primary.stale = follower.stale = True
        primary.snapshot = lambda *_: self.fail('Cancelled arm read a quote')
        self.assertEqual(engine.arm(payload)['phase'], 'stopped')
        self.assertEqual(primary.sent+follower.sent, [])

    def test_future_native_tick_waits_without_relaxing_clock_guard(self):
        primary, follower, payload, engine = self.fixture('buy')
        follower.tick_offset = 2.
        self.assertEqual(engine.arm(payload)['phase'], 'paused')
        engine.tick()
        self.assertEqual(primary.sent+follower.sent, [])
        follower.tick_offset = 0.
        engine.tick()
        self.assertEqual(engine.get(payload['id'])['phase'], 'completed')

    @staticmethod
    def stale_common_proof(bridge, payload):
        now = time.time()*1000
        return dict(status='rejected', reason='Common stop quote is stale; waiting for fresh quote',
            retryableQuoteWait=True, mutationStarted=False, account=str(bridge.login.login), symbol=payload['symbol'],
            quoteWait=dict(nativeTickUtcMs=now-2000, nativeCapturedAtMs=now, nativeAgeMs=2000))

    def pause_common_stop(self, side, account):
        primary, follower, payload, engine = self.fixture(side)
        stale = primary if account == 'primary' else follower
        original = stale.entry_common_stop
        def pause(request):
            stale.stale = True
            return self.stale_common_proof(stale, request)
        stale.entry_common_stop = pause
        engine.arm(payload)
        engine.tick()
        return primary, follower, payload, engine, stale, original

    def test_proven_pre_mutation_common_stop_staleness_waits_then_resumes(self):
        for side in ('buy', 'sell'):
            for account in ('primary', 'follower'):
                with self.subTest(side=side, account=account):
                    primary, follower, payload, engine, stale, original = self.pause_common_stop(side, account)
                    paused = engine.get(payload['id'])
                    self.assertEqual(paused['phase'], 'paused')
                    self.assertEqual(paused['quoteWait']['stage'], 'common-stop')
                    private = engine._load(payload['id'])[2]
                    self.assertFalse(any(row.get('commonStopUncertain') for row in private['accounts'].values()))
                    before = [len(bridge.sent) for bridge in (primary, follower)]
                    engine.tick()
                    self.assertEqual([len(bridge.sent) for bridge in (primary, follower)], before)
                    for bridge in (primary, follower):
                        self.assert_protected_exposure(bridge, side, .3)
                    stale.entry_common_stop = original
                    stale.stale = False
                    engine.tick()
                    self.assertEqual(engine.get(payload['id'])['phase'], 'completed')
                    for bridge in (primary, follower):
                        self.assert_protected_exposure(bridge, side, 1.)

    def test_repeated_stale_ticks_with_held_fills_do_not_retry_stops_or_write_ledger(self):
        for side in ('buy', 'sell'):
            for account in ('primary', 'follower'):
                with self.subTest(side=side, account=account):
                    primary, follower, payload, engine, stale, original = self.pause_common_stop(side, account)
                    native_retries = []
                    paused_call = stale.entry_common_stop
                    def observe(request):
                        native_retries.append(copy.deepcopy(request))
                        return paused_call(request)
                    stale.entry_common_stop = observe
                    before = engine._load(payload['id'])[2]
                    before_changes = primary.db.total_changes
                    before_sends = [copy.deepcopy(bridge.sent) for bridge in (primary, follower)]
                    for _ in range(5):
                        engine.tick()
                    after = engine._load(payload['id'])[2]
                    self.assertEqual(after['phase'], 'paused')
                    self.assertEqual(after['stopRevision'], before['stopRevision'])
                    self.assertEqual(after['quoteWait'], before['quoteWait'])
                    self.assertEqual(native_retries, [])
                    self.assertEqual(primary.db.total_changes, before_changes)
                    self.assertEqual([bridge.sent for bridge in (primary, follower)], before_sends)
                    stale.entry_common_stop = original
                    stale.stale = False
                    engine.tick()
                    self.assertEqual(engine.get(payload['id'])['phase'], 'completed')
                    for bridge in (primary, follower):
                        self.assert_protected_exposure(bridge, side, 1.)

    def test_cancel_during_proven_common_stop_wait_preserves_all_positions(self):
        for side in ('buy', 'sell'):
            primary, follower, payload, engine, stale, original = self.pause_common_stop(side, 'follower')
            before = [copy.deepcopy(bridge.rows) for bridge in (primary, follower)]
            stopped = engine.stop(dict(id='cancel-common-wait', entryId=payload['id'], expiresAt=time.time()*1000+3000))
            self.assertEqual(stopped['phase'], 'paused')
            self.assertEqual(stopped['quoteWait']['stage'], 'common-stop')
            self.assertTrue(engine._load(payload['id'])[2]['hybridRemainderStop'])
            stale.entry_common_stop = original
            stale.stale = False
            engine.tick()
            self.assertEqual(engine.get(payload['id'])['phase'], 'stopped')
            self.assertEqual([bridge.rows for bridge in (primary, follower)], before)
            for bridge in (primary, follower):
                self.assert_protected_exposure(bridge, side, .3)

    def test_deadline_during_common_stop_wait_preserves_all_positions(self):
        primary, follower, payload, engine, stale, original = self.pause_common_stop('buy', 'follower')
        with patch('chunk_entry.time.time', return_value=payload['closeAtUtc']/1000+1):
            engine.tick()
        self.assertEqual(engine.get(payload['id'])['phase'], 'stopped')
        stale.entry_common_stop = original
        stale.stale = False
        engine.tick()
        for bridge in (primary, follower):
            self.assert_protected_exposure(bridge, 'buy', .3)

    def test_unproven_common_stop_rejection_or_partial_mutation_never_auto_resumes(self):
        cases = [dict(retryableQuoteWait=False), dict(mutationStarted=True), dict(account='wrong'),
                 dict(symbol='wrong'), dict(quoteWait={}), dict(status='unknown')]
        for change in cases:
            with self.subTest(change=change):
                primary, follower, payload, engine = self.fixture('buy')
                original = follower.entry_common_stop
                follower.entry_common_stop = lambda request: {**self.stale_common_proof(follower, request), **change}
                engine.arm(payload)
                engine.tick()
                self.assertEqual(engine.get(payload['id'])['phase'], 'unknown')
                before = [copy.deepcopy(bridge.rows) for bridge in (primary, follower)]
                follower.entry_common_stop = original
                engine.tick()
                self.assertEqual([bridge.rows for bridge in (primary, follower)], before)


if __name__ == '__main__':
    unittest.main()
