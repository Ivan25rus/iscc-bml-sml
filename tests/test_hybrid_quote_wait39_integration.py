"""Independent stale-quote recovery on real Bridge/coordinator, fake MT5 only.

No sockets, worker processes, installed applications or broker APIs are used.
The native simulation asserts target volume and protected risk after every
individual pending, market, cancellation and protection transition.
"""
import copy
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as protection_fixture


class HybridQuoteWait39IntegrationTests(unittest.TestCase):
    def fixture(self, side='buy', accounts=2):
        fixture = protection_fixture.HybridProtectionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.make(side, accounts)
        # Exercise SPX explicitly; the fake broker has configurable shared
        # instrument specifications and never imports the MT5 package.
        fixture.payload['symbol'] = 'SPX500'
        fixture.payload['tp'] = 25080. if side == 'buy' else 24920.
        for bridge in fixture.bridges:
            bridge.config['symbols'] = ['SPX500']
        return fixture

    @staticmethod
    def timestamp(fixture, native, age=0.):
        native.tick.time = fixture.now - age
        native.tick.time_msc = int((fixture.now - age) * 1000)

    def restore(self, fixture):
        for native in fixture.natives:
            native.after_market = None
            self.timestamp(fixture, native)

    @staticmethod
    def sent_counts(fixture):
        return [len(native.sent) for native in fixture.natives]

    def finish(self, fixture):
        for _ in range(8):
            state = fixture.tick()
            if state['phase'] in ('completed', 'stopped', 'unknown'):
                break
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        direction = 1 if fixture.side == 'buy' else -1
        for native in fixture.natives:
            self.assertFalse(native.orders)
            volume = sum(row.volume for row in native.positions)
            self.assertAlmostEqual(volume, 1.)
            average = sum(row.volume * row.price_open for row in native.positions) / volume
            raw_stop = average - direction * 16.
            tick_size = native.info.trade_tick_size
            self.assertEqual(len({round(row.sl, 8) for row in native.positions}), 1)
            # A weighted mean can fall between ticks. The common stop may
            # round by at most one tick toward price, never toward more risk.
            rounding = direction * (native.positions[0].sl - raw_stop)
            self.assertGreaterEqual(rounding, -1e-8)
            self.assertLessEqual(rounding, tick_size + 1e-8)
            self.assertTrue(all(row.tp == fixture.payload['tp'] for row in native.positions))
            risk = sum(max(0., -native.order_calc_profit(row.type, row.symbol, row.volume,
                                                       row.price_open, row.sl))
                       for row in native.positions)
            self.assertLessEqual(risk, 16. + 1e-8)
            self.assertTrue(all(request.get('position') is None
                                for request in native.sent
                                if request['action'] == native.TRADE_ACTION_DEAL))
        return state

    def wait_after_first_fill(self, fixture):
        for native in fixture.natives:
            native.after_market = lambda broker: self.timestamp(fixture, broker, 2.)
        fixture.engine.arm(fixture.payload)
        state = fixture.tick()
        self.assertEqual(state['phase'], 'paused', state.get('reason'))
        self.assertEqual(state['quoteWait']['stage'], 'common-stop')
        self.assertEqual(state['quoteWait']['status'], 'waiting')
        for native in fixture.natives:
            self.assertAlmostEqual(sum(row.volume for row in native.positions), .3)
            self.assertFalse(native.orders)
            self.assertFalse(any(request['action'] == native.TRADE_ACTION_SLTP
                                 for request in native.sent))
        return state

    def test_initial_stale_on_either_account_is_durable_then_resumes_same_plan(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                for stale_index in range(accounts):
                    with self.subTest(side=side, accounts=accounts, stale_index=stale_index):
                        fixture = self.fixture(side, accounts)
                        self.timestamp(fixture, fixture.natives[stale_index], 2.)
                        admitted = fixture.engine.arm(fixture.payload)
                        self.assertEqual(admitted['phase'], 'paused')
                        self.assertEqual(admitted['quoteWait']['stage'], 'admission')
                        self.assertEqual(admitted['quoteWait']['account'],
                                         str(fixture.natives[stale_index].account.login))
                        self.assertIsNotNone(fixture.engine._load(fixture.payload['id']))
                        for _ in range(3):
                            self.assertEqual(fixture.tick()['phase'], 'paused')
                        self.assertEqual(self.sent_counts(fixture), [0] * accounts)
                        self.restore(fixture)
                        completed = self.finish(fixture)
                        self.assertEqual(completed['id'], admitted['id'])
                        self.assertEqual(completed['quoteWait']['status'], 'resumed')
                        count = self.sent_counts(fixture)
                        fixture.engine.arm(fixture.payload)
                        fixture.tick()
                        self.assertEqual(self.sent_counts(fixture), count)

    def test_stale_after_first_fill_before_common_stop_resumes_with_common_protection(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                with self.subTest(side=side, accounts=accounts):
                    fixture = self.fixture(side, accounts)
                    self.wait_after_first_fill(fixture)
                    before = self.sent_counts(fixture)
                    for _ in range(3):
                        self.assertEqual(fixture.tick()['phase'], 'paused')
                    self.assertEqual(self.sent_counts(fixture), before)
                    self.restore(fixture)
                    self.finish(fixture)

    def test_only_follower_common_stop_wait_preserves_confirmed_primary_protection(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                fixture = self.fixture(side)
                fixture.natives[1].after_market = lambda broker: self.timestamp(fixture, broker, 2.)
                fixture.engine.arm(fixture.payload)
                paused = fixture.tick()
                self.assertEqual(paused['phase'], 'paused', paused.get('reason'))
                self.assertEqual(paused['quoteWait']['account'], '900000001')
                self.assertEqual(paused['quoteWait']['stage'], 'common-stop')
                details = fixture.engine._load(fixture.payload['id'])[2]['accounts']
                self.assertTrue(details['900000002']['stopSignature'])
                self.assertNotIn('stopSignature', details['900000001'])
                self.assertFalse(any(row.get('commonStopUncertain') for row in details.values()))
                self.restore(fixture)
                self.finish(fixture)

    def test_existing_fill_wait_does_not_grow_native_journals_or_parent_revision(self):
        fixture = self.fixture()
        self.wait_after_first_fill(fixture)
        initial = fixture.engine._load(fixture.payload['id'])[2]
        revision = initial['stopRevision']
        rows = [bridge.db.execute('SELECT count(*) FROM entry_common_stops').fetchone()[0]
                for bridge in fixture.bridges]
        changes = [bridge.db.total_changes for bridge in fixture.bridges]
        sent = self.sent_counts(fixture)
        for _ in range(5):
            state = fixture.tick()
            self.assertEqual(state['phase'], 'paused', state.get('reason'))
            saved = fixture.engine._load(fixture.payload['id'])[2]
            self.assertEqual(saved['stopRevision'], revision)
            self.assertEqual([bridge.db.execute('SELECT count(*) FROM entry_common_stops').fetchone()[0]
                              for bridge in fixture.bridges], rows)
            self.assertEqual([bridge.db.total_changes for bridge in fixture.bridges], changes)
            self.assertEqual(self.sent_counts(fixture), sent)
        self.restore(fixture)
        self.finish(fixture)

    def test_runtime_stale_retains_same_outer_limit_and_resumes_without_another_arm(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                fixture = self.fixture(side)
                fixture.entry(active=True)
                stops = [[row.sl for row in native.positions] for native in fixture.natives]
                tickets = [[row.ticket for row in native.orders] for native in fixture.natives]
                sent = self.sent_counts(fixture)
                self.timestamp(fixture, fixture.natives[1], 2.)
                state = fixture.tick()
                self.assertEqual(state['phase'], 'paused', state.get('reason'))
                self.assertEqual(state['quoteWait']['stage'], 'runtime')
                for index, native in enumerate(fixture.natives):
                    self.assertEqual([row.ticket for row in native.orders], tickets[index])
                    self.assertEqual([row.sl for row in native.positions], stops[index])
                    self.assertAlmostEqual(sum(row.volume for row in native.positions), .3)
                changes = [bridge.db.total_changes for bridge in fixture.bridges]
                for _ in range(5):
                    self.assertEqual(fixture.tick()['phase'], 'paused')
                self.assertEqual(self.sent_counts(fixture), sent)
                self.assertEqual([bridge.db.total_changes for bridge in fixture.bridges], changes)
                # Alternate stale/fresh snapshots while still outside the
                # market zone: the exact broker tickets must survive both.
                for _ in range(3):
                    self.restore(fixture)
                    fixture.tick()
                    self.timestamp(fixture, fixture.natives[0], 2.)
                    fixture.tick()
                self.assertEqual(self.sent_counts(fixture), sent)
                for index, native in enumerate(fixture.natives):
                    self.assertEqual([row.ticket for row in native.orders], tickets[index])
                self.restore(fixture)
                for native in fixture.natives:
                    fixture.quote(native, 25002. if side == 'buy' else 24998.)
                self.finish(fixture)

    def test_native_freshness_guard_between_support_limit_and_market_sends_no_stale_market(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                with self.subTest(side=side, accounts=accounts):
                    fixture = self.fixture(side, accounts)
                    original = fixture.engine._hybrid_send
                    triggered = []

                    def guarded(payload, state, kind, prices, volumes, guards, support=False):
                        result = original(payload, state, kind, prices, volumes, guards, support)
                        if kind == 'limit' and support and result and not triggered:
                            triggered.append(True)
                            for native in fixture.natives:
                                self.timestamp(fixture, native, 2.)
                        return result

                    fixture.engine.arm(fixture.payload)
                    with patch.object(fixture.engine, '_hybrid_send', side_effect=guarded):
                        state = fixture.tick()
                    self.assertTrue(triggered)
                    self.assertEqual(state['phase'], 'paused', state.get('reason'))
                    for native in fixture.natives:
                        self.assertFalse(native.positions)
                        self.assertFalse(native.orders)
                        self.assertFalse(any(row['action'] == native.TRADE_ACTION_DEAL
                                             for row in native.sent))
                    self.restore(fixture)
                    self.finish(fixture)

    def test_stale_boundary_limit_without_fills_is_not_cancelled_or_replaced(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                with self.subTest(side=side, accounts=accounts):
                    fixture = self.fixture(side, accounts)
                    for native in fixture.natives:
                        fixture.quote(native, 25007. if side == 'buy' else 24993.)
                    fixture.engine.arm(fixture.payload)
                    fixture.tick()
                    orders = [copy.deepcopy(native.orders) for native in fixture.natives]
                    sent = self.sent_counts(fixture)
                    for _ in range(4):
                        self.timestamp(fixture, fixture.natives[-1], 2.)
                        self.assertEqual(fixture.tick()['phase'], 'paused')
                        self.restore(fixture)
                        fixture.tick()
                    self.assertEqual(self.sent_counts(fixture), sent)
                    self.assertEqual([native.orders for native in fixture.natives], orders)
                    for native in fixture.natives:
                        fixture.quote(native, 25002. if side == 'buy' else 24998.)
                    self.finish(fixture)

    def test_fill_during_stale_wait_is_counted_and_peer_remainder_cancelled_before_resume(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                fixture = self.fixture(side)
                fixture.entry(active=True)
                self.timestamp(fixture, fixture.natives[1], 2.)
                fixture.tick()
                native = fixture.natives[0]
                order = native.orders.pop()
                native.positions.append(native.position_from(order.ticket, dict(symbol=order.symbol,
                    type=native.ORDER_TYPE_BUY if side == 'buy' else native.ORDER_TYPE_SELL,
                    volume=order.volume_current, price=order.price_open, sl=order.sl, tp=order.tp,
                    comment=order.comment)))
                native.history_orders.append(protection_fixture.N(ticket=order.ticket, symbol=order.symbol,
                    position_id=order.ticket, state=4))
                native.history_deals.append(protection_fixture.N(symbol=order.symbol, position_id=order.ticket,
                    entry=0, volume=order.volume_current, reason=0))
                native.observe('broker-fill')
                sent = self.sent_counts(fixture)
                state = fixture.tick()
                self.assertEqual(state['phase'], 'paused', state.get('reason'))
                self.assertAlmostEqual(state['accounts']['900000002']['filledVolume'], 1.)
                self.assertAlmostEqual(state['accounts']['900000001']['filledVolume'], .3)
                self.assertFalse(any(broker.orders for broker in fixture.natives))
                for index, broker in enumerate(fixture.natives):
                    self.assertTrue(all(row['action'] == broker.TRADE_ACTION_REMOVE
                                        for row in broker.sent[sent[index]:]))
                for _ in range(3):
                    fixture.tick()
                self.restore(fixture)
                for broker in fixture.natives:
                    fixture.quote(broker, 25002. if side == 'buy' else 24998.)
                # Only the unfinished peer resumes. The completed primary
                # remains protected and receives no additional market lot.
                primary_markets = len(fixture.markets(fixture.natives[0]))
                state = fixture.tick()
                self.assertEqual(state['phase'], 'completed', state.get('reason'))
                self.assertEqual(len(fixture.markets(fixture.natives[0])), primary_markets)
                self.assertAlmostEqual(sum(row.volume for row in fixture.natives[0].positions), 1.)
                self.assertAlmostEqual(sum(row.volume for row in fixture.natives[1].positions), 1.)
                for broker in fixture.natives:
                    self.assertEqual(len({row.sl for row in broker.positions}), 1)
                    self.assertFalse(any(row.get('position') for row in broker.sent
                        if row['action'] == broker.TRADE_ACTION_DEAL))

    def test_stop_or_deadline_during_retained_limit_wait_cancels_only_pending(self):
        for cause in ('stop', 'deadline'):
            with self.subTest(cause=cause):
                fixture = self.fixture()
                fixture.entry(active=True)
                self.timestamp(fixture, fixture.natives[1], 2.)
                fixture.tick()
                positions = [copy.deepcopy(native.positions) for native in fixture.natives]
                sent = self.sent_counts(fixture)
                if cause == 'stop':
                    state = fixture.engine.stop(dict(id='cancel-retained', entryId=fixture.payload['id'],
                        expiresAt=fixture.now*1000+2000))
                else:
                    with patch('time.time', return_value=fixture.payload['closeAtUtc']/1000+.1):
                        state = fixture.tick()
                self.assertEqual(state['phase'], 'stopped', state.get('reason'))
                self.restore(fixture)
                for _ in range(3):
                    fixture.tick()
                self.assertEqual([native.positions for native in fixture.natives], positions)
                for index, native in enumerate(fixture.natives):
                    self.assertFalse(native.orders)
                    self.assertTrue(all(row['action'] == native.TRADE_ACTION_REMOVE
                                        for row in native.sent[sent[index]:]))

    def test_changed_limit_protection_during_stale_wait_stops_remainder(self):
        fixture = self.fixture()
        fixture.entry(active=True)
        self.timestamp(fixture, fixture.natives[1], 2.)
        fixture.tick()
        fixture.natives[0].orders[0].sl += .1
        state = fixture.tick()
        self.assertIn(state['phase'], ('stopped', 'unknown'), state.get('reason'))
        self.assertFalse(any(native.orders for native in fixture.natives))
        self.assertTrue(all(sum(row.volume for row in native.positions) == .3 for native in fixture.natives))

    def test_explicit_cancel_of_initial_wait_prevents_late_fresh_quote_and_duplicate_arm(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                fixture = self.fixture(side)
                self.timestamp(fixture, fixture.natives[1], 2.)
                fixture.engine.arm(fixture.payload)
                stopped = fixture.engine.stop(dict(id='cancel-initial', entryId=fixture.payload['id'],
                                                   expiresAt=fixture.now * 1000 + 2000))
                self.assertEqual(stopped['phase'], 'stopped')
                self.restore(fixture)
                self.assertEqual(fixture.engine.arm(fixture.payload)['phase'], 'stopped')
                for _ in range(3):
                    self.assertEqual(fixture.tick()['phase'], 'stopped')
                self.assertEqual(self.sent_counts(fixture), [0, 0])

    def test_explicit_cancel_after_filled_stale_wait_keeps_positions_and_never_resumes_acquisition(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                with self.subTest(side=side, accounts=accounts):
                    fixture = self.fixture(side, accounts)
                    self.wait_after_first_fill(fixture)
                    positions = [copy.deepcopy(native.positions) for native in fixture.natives]
                    stopped = fixture.engine.stop(dict(id='cancel-filled', entryId=fixture.payload['id'],
                                                       expiresAt=fixture.now * 1000 + 2000))
                    self.assertEqual(stopped['phase'], 'paused')
                    self.assertEqual(stopped['quoteWait']['stage'], 'common-stop')
                    self.assertTrue(fixture.engine._load(fixture.payload['id'])[2]['hybridRemainderStop'])
                    before = self.sent_counts(fixture)
                    self.restore(fixture)
                    for _ in range(3):
                        self.assertEqual(fixture.tick()['phase'], 'stopped')
                    self.assertEqual(self.sent_counts(fixture), before)
                    self.assertEqual([native.positions for native in fixture.natives], positions)

    def test_deadline_ends_initial_wait_without_any_order_and_fresh_tick_cannot_resume(self):
        fixture = self.fixture()
        self.timestamp(fixture, fixture.natives[1], 2.)
        fixture.engine.arm(fixture.payload)
        with patch('time.time', return_value=fixture.payload['closeAtUtc'] / 1000 + 1):
            self.assertEqual(fixture.tick()['phase'], 'stopped')
        self.restore(fixture)
        self.assertEqual(fixture.tick()['phase'], 'stopped')
        self.assertEqual(self.sent_counts(fixture), [0, 0])

    def test_deadline_ends_filled_quote_wait_without_closing_or_resuming_positions(self):
        fixture = self.fixture('sell')
        self.wait_after_first_fill(fixture)
        before = self.sent_counts(fixture)
        positions = [copy.deepcopy(native.positions) for native in fixture.natives]
        with patch('time.time', return_value=fixture.payload['closeAtUtc'] / 1000 + 1):
            self.assertEqual(fixture.tick()['phase'], 'stopped')
        self.restore(fixture)
        self.assertEqual(fixture.tick()['phase'], 'stopped')
        self.assertEqual(self.sent_counts(fixture), before)
        self.assertEqual([native.positions for native in fixture.natives], positions)

    def test_missing_or_forged_common_stop_wait_proof_never_unlocks_uncertain_operation(self):
        corruptions = {
            'missing-proof': lambda result: result.pop('quoteWait'),
            'missing-mutation-proof': lambda result: result.pop('mutationStarted'),
            'mutation-started': lambda result: result.update(mutationStarted=True),
            'unknown-result': lambda result: result.update(status='unknown'),
            'different-account': lambda result: result.update(account='999'),
            'different-symbol': lambda result: result.update(symbol='USTEC'),
            'inconsistent-age': lambda result: result['quoteWait'].update(nativeAgeMs=9000.),
            'fresh-age': lambda result: result['quoteWait'].update(nativeAgeMs=0.),
            'nonfinite-age': lambda result: result['quoteWait'].update(nativeAgeMs=float('nan')),
        }
        for label, corrupt in corruptions.items():
            with self.subTest(proof=label):
                fixture = self.fixture()
                for native in fixture.natives:
                    native.after_market = lambda broker: self.timestamp(fixture, broker, 2.)
                original = fixture.primary.entry_common_stop

                def invalid_proof(payload):
                    result = copy.deepcopy(original(payload))
                    self.assertTrue(result['retryableQuoteWait'])
                    corrupt(result)
                    return result

                fixture.engine.arm(fixture.payload)
                with patch.object(fixture.primary, 'entry_common_stop', side_effect=invalid_proof):
                    state = fixture.tick()
                self.assertEqual(state['phase'], 'unknown', state.get('reason'))
                stored = fixture.engine._load(fixture.payload['id'])[2]
                self.assertTrue(stored['accounts']['900000002']['commonStopUncertain'])
                count = self.sent_counts(fixture)
                self.restore(fixture)
                self.assertEqual(fixture.tick()['phase'], 'unknown')
                self.assertEqual(self.sent_counts(fixture), count)

    def test_unknown_or_partial_native_common_stop_modification_remains_blocked(self):
        for side in ('buy', 'sell'):
            for accounts in (1, 2):
                for failure_at in (1, 2):
                    with self.subTest(side=side, accounts=accounts, failure_at=failure_at):
                        fixture = self.fixture(side, accounts)
                        fixture.natives[0].sl_failure = dict(at=failure_at, unknown=True)
                        fixture.engine.arm(fixture.payload)
                        state = fixture.tick()
                        self.assertEqual(state['phase'], 'unknown', state.get('reason'))
                        self.assertGreaterEqual(fixture.natives[0].protection_sends, failure_at)
                        before = self.sent_counts(fixture)
                        self.restore(fixture)
                        for _ in range(3):
                            self.assertEqual(fixture.tick()['phase'], 'unknown')
                        self.assertEqual(self.sent_counts(fixture), before)

    def test_limit_fill_winning_stale_cancellation_is_accounted_before_fresh_resume(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                fixture = self.fixture(side)
                for native in fixture.natives:
                    native.fill_on_cancel = True
                    native.after_market = lambda broker: self.timestamp(fixture, broker, 2.)
                fixture.engine.arm(fixture.payload)
                paused = fixture.tick()
                self.assertEqual(paused['phase'], 'paused', paused.get('reason'))
                for native in fixture.natives:
                    self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
                    self.assertFalse(native.orders)
                self.restore(fixture)
                self.finish(fixture)
                for native in fixture.natives:
                    self.assertEqual(sum(row['action'] == native.TRADE_ACTION_DEAL
                                         for row in native.sent), 1)


if __name__ == '__main__':
    unittest.main()
