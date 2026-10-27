"""Adversarial hybrid-entry simulations. No MT5 import, network or real orders."""
import copy
import json
import math
import pathlib
import random
import sys
import time
import unittest
from decimal import Decimal, ROUND_FLOOR

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_chunk_entry import Worker
from test_market_limit_entry import PendingBridge
from chunk_entry import ChunkEntries


class HybridBridge(PendingBridge):
    """Broker fake measuring live exposure at every individual state change."""
    def __init__(self, account='900000002'):
        super().__init__(account)
        self.target_volume = 1.
        self.risk_budget = 16.
        self.observations = []
        self.common_calls = []
        self.common_unknown = False
        self.before_common = None
        self.market_hook = None
        self.enforce_market_zone = False
        self.common_delay = 0.
        self.trade_delay = 0.

    def observe(self, operation):
        volume = sum(row['lots'] for row in self.rows)
        risk = sum(max(0., row['open_price'] - row['sl']) * row['lots'] * self.point
                   if row['type'] == 'buy' else
                   max(0., row['sl'] - row['open_price']) * row['lots'] * self.point
                   for row in self.rows)
        self.observations.append(dict(operation=operation, volume=volume, risk=risk))
        if volume > self.target_volume + 1e-8:
            raise AssertionError('Position plus live pending volume exceeds the captured target')
        if risk > self.risk_budget + 1e-8:
            raise AssertionError('Position plus live pending risk exceeds the captured risk')

    def _fill(self, payload):
        request = copy.deepcopy(payload)
        if request['type'] == 'limit' and request.get('entryGuard', {}).get('limitPrice') is not None:
            request['price'] = request['entryGuard']['limitPrice']
        if self.market_hook and request['type'] == 'market':
            self.market_hook(self, request)
        if self.enforce_market_zone and request['type'] == 'market':
            guard = request['entryGuard']
            price = self.price['ask' if request['side'] == 'buy' else 'bid']
            if price < guard['priceFloor'] - 1e-8 or price > guard['priceCeiling'] + 1e-8:
                return dict(status='rejected', reason='Market price moved outside the entry zone')
        if self.trade_delay:
            time.sleep(self.trade_delay)
        result = super()._fill(request)
        self.observe(request['type'])
        return result

    def execute(self, operation, payload):
        result = super().execute(operation, payload)
        if operation == 'cancel':
            if self.trade_delay:
                time.sleep(self.trade_delay)
            self.observe('cancel')
        return result

    def fill_limit(self, amount, price=None):
        pending = next(row for row in self.rows if not row['__isPosition'])
        position = next((row for row in self.rows if row['__isPosition'] and row['ticket'] == pending['ticket']), None)
        previous_volume = position['lots'] if position else 0.
        previous_price = position['open_price'] if position else pending['open_price']
        execution_price = pending['open_price'] if price is None else price
        super().fill_limit(amount)
        position = next(row for row in self.rows if row['__isPosition'] and row['ticket'] == pending['ticket'])
        position['open_price'] = (previous_price * previous_volume + execution_price * amount) / (previous_volume + amount)
        self.observe('limit-fill')

    def entry_common_stop(self, payload):
        self.common_calls.append(copy.deepcopy(payload))
        if self.before_common:
            self.before_common(self, payload)
        if self.common_unknown:
            return dict(status='unknown', reason='Simulated lost common-stop confirmation')
        positions = payload['positions']
        if len({str(row['ticket']) for row in positions}) != len(positions):
            raise AssertionError('Common-stop request repeats an owned position')
        actual = {str(row['ticket']): row for row in self.rows if row['__isPosition']}
        for position in positions:
            row = actual.get(str(position['ticket']))
            if row is None or abs(row['lots'] - position['volume']) > 1e-8:
                return dict(status='rejected', reason='Position closed or changed')
            if abs(row['sl'] - position['sl']) > 1e-8:
                return dict(status='rejected', reason='Position stop changed')
        volume = sum(row['volume'] for row in positions)
        average = sum(row['fillPrice'] * row['volume'] for row in positions) / volume
        direction = 1 if payload['side'] == 'buy' else -1
        stop = payload.get('sl', average - direction * payload.get('stopDistance', 0.))
        # The real broker rounds toward lower risk when the weighted mean is
        # between price ticks. Model the same direction with the fixture tick.
        stop = (math.ceil(stop / .1 - 1e-8) if direction == 1 else math.floor(stop / .1 + 1e-8)) * .1
        if self.common_delay:
            time.sleep(self.common_delay)
        # Tighten all positions before widening any: test intermediate risk.
        ordered = sorted(positions, key=lambda row: direction * (stop - actual[str(row['ticket'])]['sl']), reverse=True)
        for position in ordered:
            actual[str(position['ticket'])]['sl'] = round(stop, 8)
            self.observe('common-stop')
        result_positions = [dict(ticket=str(row['ticket']), volume=row['volume'], fillPrice=row['fillPrice'],
                                 sl=round(stop, 8), tp=row['tp']) for row in positions]
        risk = sum(direction * (row['fillPrice'] - stop) * row['volume'] * self.point for row in positions)
        return dict(status='ok', averagePrice=average, sl=round(stop, 8), riskUsed=round(risk, 8),
                    filledVolume=round(volume, 8), positions=result_positions)


def hybrid_fixture(side='buy', linked=True):
    primary = HybridBridge()
    follower = HybridBridge('900000001') if linked else None
    if follower:
        primary.linked['900000001'] = Worker(primary, follower)
    payload = dict(id='percent-hybrid', symbol='USTEC', side=side, trigger=100., level=100.,
                   sl=84. if side == 'buy' else 116., tp=0., riskUsd=16., volume=1., chunkVolume=.3,
                   chunkPercent=30., entryMode='side-zone', executionPolicy='hybrid-market-limit-v1',
                   zoneRadius=6., zoneMultiplier=1, tickSize=.1, stopOffsetPts=160., stopMode='entry',
                   maxGap=6., expiresAt=time.time()*1000+3000, closeAtUtc=time.time()*1000+3600000,
                   closeTimeZone='America/New_York', closeLocalDate='2026-10-21', closeLocalTime='15:00',
                   cancelPendingAtDeadline=True)
    if follower:
        payload['mirrorAccounts'] = [dict(account='900000001', multiplier=1.)]
    for bridge in (primary, follower):
        if bridge:
            bridge.price = dict(ask=102., bid=101.8) if side == 'buy' else dict(bid=98., ask=98.2)
    return primary, follower, payload


class PercentageHybridTests(unittest.TestCase):
    def setUp(self):
        self.primary, self.follower, self.payload = hybrid_fixture()
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)

    def bridges(self):
        return [bridge for bridge in (self.primary, self.follower) if bridge]

    def single_account(self):
        self.engine.close()
        self.primary, self.follower, self.payload = hybrid_fixture(linked=False)
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)

    def state(self):
        return self.engine.list()[0]

    def tick(self):
        self.engine.pending_poll_at.clear()
        self.engine.tick()

    def run_entry(self, limit=12):
        self.engine.arm(self.payload)
        for _ in range(limit):
            self.tick()
            if self.state()['phase'] in ('completed', 'stopped', 'unknown'):
                break
        return self.state()

    def capture_diagnostic_receipts(self):
        """Keep the original replies to detect accidental journal mutation."""
        originals = []
        def annotate(receipt, payload, operation):
            receipt.update(id=payload['id'], operation=operation,
                           timings=dict(server=dict(totalMs=7.5, readCounters={'position': 4})),
                           serverTiming=dict(totalMs=8.), rpcTiming=dict(totalMs=9.),
                           nativeProof=dict(opaque='future-proof-extension', sequence=[1, 2, 3]))
            if operation == 'cancel':
                receipt.update(ticket=payload['ticket'], retcode=10009,
                               reason=receipt.get('reason', 'Cancellation response retained'))
            originals.append(receipt)
            return receipt
        def decorate(bridge):
            fill, execute = bridge._fill, bridge.execute
            bridge._fill = lambda payload: annotate(fill(payload), payload, 'place')
            def send(operation, payload):
                response = execute(operation, payload)
                if operation == 'cancel':
                    annotate(response['result'], payload, operation)
                return response
            bridge.execute = send
        for bridge in self.bridges():
            decorate(bridge)
        group = self.primary.group_execute
        def grouped(operation, payload, **options):
            response = group(operation, payload, **options)
            annotate(response['result'], payload, operation)
            return response
        self.primary.group_execute = grouped
        return originals

    def assert_receipt_compacted(self, receipt):
        for diagnostic in ('timings', 'serverTiming', 'rpcTiming'):
            self.assertNotIn(diagnostic, receipt)
        self.assertEqual(receipt['nativeProof'], dict(opaque='future-proof-extension', sequence=[1, 2, 3]))
        self.assertTrue(receipt['id'])
        for outcome in receipt.get('accountResults', {}).values():
            self.assert_receipt_compacted(outcome)

    def assert_common_stop(self, bridge):
        positions = [row for row in bridge.rows if row['__isPosition']]
        self.assertTrue(positions)
        volume = sum(row['lots'] for row in positions)
        average = sum(row['lots'] * row['open_price'] for row in positions) / volume
        direction = 1 if self.payload['side'] == 'buy' else -1
        raw = average - direction * 16.
        expected = (math.ceil(raw / .1 - 1e-8) if direction == 1 else math.floor(raw / .1 + 1e-8)) * .1
        for row in positions:
            self.assertAlmostEqual(row['sl'], expected, places=7)
        self.assertTrue(all(row['volume'] <= bridge.target_volume + 1e-8 and row['risk'] <= bridge.risk_budget + 1e-8
                            for row in bridge.observations))

    def test_four_market_parts_and_support_remainders_keep_volume_and_risk_reserved(self):
        self.assertEqual(self.run_entry()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'limit'], [.7, .4, .1])
            self.assertTrue(all(abs(row['price'] - 101.8) < 1e-8 for row in bridge.sent if row['type'] == 'limit'))
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)

    def test_sell_is_mirrored_and_has_one_weighted_stop(self):
        self.payload.update(side='sell', sl=116.)
        for bridge in self.bridges():
            bridge.price = dict(bid=98., ask=98.2)
        self.assertEqual(self.run_entry()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assertTrue(all(abs(row['price'] - 98.2) < 1e-8 for row in bridge.sent if row['type'] == 'limit'))
            self.assert_common_stop(bridge)

    def test_buy_below_lt_waits_then_return_inside_uses_clamped_support_and_market(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=99., bid=98.8)
        self.engine.arm(self.payload)
        for _ in range(3):
            self.tick()
            self.assertEqual(self.state()['phase'], 'armed')
            for bridge in self.bridges():
                self.assertEqual(bridge.sent, [])
                self.assertEqual(bridge.rows, [])
        for bridge in self.bridges():
            bridge.price = dict(ask=100.1, bid=99.9)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'limit'], [.7, .4, .1])
            self.assertTrue(all(row['price'] == 100. for row in bridge.sent if row['type'] == 'limit'))
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assert_common_stop(bridge)

    def test_buy_jump_from_below_lt_to_above_play_places_only_full_outer_limit(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=99., bid=98.8)
        self.engine.arm(self.payload)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual(bridge.sent, [])
            bridge.price = dict(ask=107., bid=106.8)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual([(row['type'], row['side'], row['price'], row['volume']) for row in bridge.sent],
                             [('limit', 'buy', 106., 1.)])
        for _ in range(3):
            self.tick()
        for bridge in self.bridges():
            self.assertEqual(len(bridge.sent), 1)
            bridge.fill_limit(1.)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertFalse(any(row['type'] == 'market' for row in bridge.sent))
            self.assert_common_stop(bridge)
            self.assertTrue(all(row['sl'] == 90. for row in bridge.rows))

    def buy_partial_then_outer_limit(self, partial_support=0.):
        def move_after_first_fill(bridge, request):
            if not getattr(bridge, 'moved_above_play', False):
                bridge.moved_above_play = True
                bridge.price = dict(ask=107., bid=106.8)
        def fill_support_once(bridge, request):
            if not getattr(bridge, 'injected_support_fill', False):
                bridge.injected_support_fill = True
                bridge.fill_limit(partial_support)
        for bridge in self.bridges():
            bridge.before_common = move_after_first_fill
            if partial_support:
                bridge.market_hook = fill_support_once
            bridge.price = dict(ask=99., bid=98.8)
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.state()['phase'], 'armed')
        for bridge in self.bridges():
            self.assertEqual(bridge.sent, [])
            bridge.price = dict(ask=102., bid=101.8)
        self.tick()
        self.assertEqual(self.state()['phase'], 'running')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            pending = [row for row in bridge.rows if not row['__isPosition']]
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0]['open_price'], 106.)
            self.assertAlmostEqual(pending[0]['lots'], .7-partial_support)
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assert_common_stop(bridge)

    def test_buy_partial_then_above_play_limits_only_residual_and_shares_stop_after_fill(self):
        self.buy_partial_then_outer_limit()
        for bridge in self.bridges():
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows if row['__isPosition']), .3)
            self.assertTrue(all(row['sl'] == 86. for row in bridge.rows if row['__isPosition']))
            bridge.fill_limit(.7)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)
            self.assertTrue(all(abs(row['sl']-88.8) < 1e-8 for row in bridge.rows))

    def test_buy_return_from_above_play_cancels_outer_before_remaining_market(self):
        self.buy_partial_then_outer_limit()
        for bridge in self.bridges():
            bridge.price = dict(ask=104., bid=103.8)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)
            self.assertTrue(all(abs(row['sl']-87.4) < 1e-8 for row in bridge.rows))

    def test_buy_outer_limit_winning_return_cancel_race_blocks_unneeded_market(self):
        self.buy_partial_then_outer_limit()
        for bridge in self.bridges():
            bridge.price = dict(ask=104., bid=103.8)
            bridge.fill_on_cancel = True
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)

    def test_buy_market_and_partial_support_then_outer_limit_keep_exact_residual_and_common_stop(self):
        self.buy_partial_then_outer_limit(partial_support=.2)
        for bridge in self.bridges():
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows if row['__isPosition']), .5)
            bridge.fill_limit(.5)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)

    def test_buy_partial_then_below_lt_pauses_with_common_stop_until_return_inside(self):
        def move_below_after_fill(bridge, request):
            bridge.price = dict(ask=99., bid=98.8)
        for bridge in self.bridges():
            bridge.before_common = move_below_after_fill
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            bridge.before_common = None
            bridge.price = dict(ask=104., bid=103.8)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assert_common_stop(bridge)

    def test_buy_unknown_outer_cancel_keeps_reserved_volume_and_blocks_further_market(self):
        self.buy_partial_then_outer_limit()
        for bridge in self.bridges():
            bridge.price = dict(ask=104., bid=103.8)
            bridge.cancel_unknown = True
        self.tick()
        self.assertEqual(self.state()['phase'], 'unknown')
        attempts = [len(bridge.cancelled) for bridge in self.bridges()]
        for _ in range(3):
            self.tick()
        self.assertEqual([len(bridge.cancelled) for bridge in self.bridges()], attempts)
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertLessEqual(sum(row['lots'] for row in bridge.rows), 1.+1e-8)
            self.assert_common_stop(bridge)

    def test_buy_native_outer_gap_fill_below_lt_is_counted_with_common_stop_then_stops_remainder(self):
        self.buy_partial_then_outer_limit()
        for bridge in self.bridges():
            bridge.fill_limit(.7, price=99.)
            bridge.price = dict(ask=99., bid=98.8)
        self.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertIn('Actual hybrid fill left the captured zone', self.state()['reason'])
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)
            self.assertTrue(all(abs(row['sl']-83.9) < 1e-8 for row in bridge.rows))
            bridge.price = dict(ask=102., bid=101.8)
        self.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])

    def test_short_above_lt_waits_without_orders_then_returns_to_clamped_support_and_market(self):
        self.payload.update(side='sell', sl=116.)
        for bridge in self.bridges():
            bridge.price = dict(bid=117., ask=117.2)
        self.engine.arm(self.payload)
        for _ in range(3):
            self.tick()
            self.assertEqual(self.state()['phase'], 'armed')
            for bridge in self.bridges():
                self.assertEqual(bridge.sent, [])
                self.assertEqual(bridge.rows, [])
        for bridge in self.bridges():
            bridge.price = dict(bid=99.9, ask=100.1)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assertTrue(all(row['price'] == 100. for row in bridge.sent if row['type'] == 'limit'))
            self.assert_common_stop(bridge)

    def test_short_jump_from_above_lt_to_below_play_places_outer_sell_limit_without_market(self):
        self.payload.update(side='sell', sl=116.)
        for bridge in self.bridges():
            bridge.price = dict(bid=102., ask=102.2)
        self.engine.arm(self.payload)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual(bridge.sent, [])
            bridge.price = dict(bid=93.8, ask=94.)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual([(row['type'], row['side'], row['price'], row['volume']) for row in bridge.sent],
                             [('limit', 'sell', 94., 1.)])
            bridge.fill_limit(1.)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertFalse(any(row['type'] == 'market' for row in bridge.sent))
            self.assert_common_stop(bridge)

    def test_short_partial_fill_then_price_above_lt_pauses_with_shared_stop_and_resumes(self):
        self.payload.update(side='sell', sl=116.)
        for bridge in self.bridges():
            bridge.price = dict(bid=98., ask=98.2)
            def move_after_fill(current, request):
                current.price = dict(bid=101., ask=101.2)
            bridge.before_common = move_after_fill
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertTrue(bridge.cancelled)
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assert_common_stop(bridge)
            self.assertAlmostEqual(bridge.rows[0]['sl'], 114.)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            bridge.before_common = None
            bridge.price = dict(bid=98.2, ask=98.4)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3, .3, .3, .1])
            self.assert_common_stop(bridge)

    def test_short_partial_fill_reaching_sl_stops_entry_without_new_orders_or_position_close(self):
        self.payload.update(side='sell', sl=116.)
        for bridge in self.bridges():
            bridge.price = dict(bid=98., ask=98.2)
            def move_after_fill(current, request):
                current.price = dict(bid=114., ask=114.2)
            bridge.before_common = move_after_fill
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertIn('SL or TP reached', self.state()['reason'])
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), .3)
            self.assert_common_stop(bridge)
            bridge.before_common = None
            bridge.price = dict(bid=98., ask=98.2)
        self.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])

    def test_twenty_five_percent_rounding_finishes_in_one_tick_without_increasing_total(self):
        self.payload.update(chunkPercent=25., chunkVolume=.2)
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type']=='market'], [.2]*5)
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows), 1.)
            self.assert_common_stop(bridge)

    def test_any_positive_percent_through_one_hundred_respects_small_risk_sized_target(self):
        for percent in (.001, .01, .5, 1., 10., 20., 50., 100.):
            with self.subTest(percent=percent):
                self.single_account()
                units = (Decimal('.7') * Decimal(str(percent)) / Decimal('100') / Decimal('.1')).to_integral_value(rounding=ROUND_FLOOR)
                chunk = min(.7, max(.1, float(units * Decimal('.1'))))
                self.payload.update(volume=.7, riskUsd=12.5, chunkPercent=percent, chunkVolume=chunk)
                self.primary.target_volume, self.primary.risk_budget = .7, 12.5
                state = self.run_entry()
                self.assertEqual(state['phase'], 'completed')
                self.assertEqual(state['chunkPercent'], percent, 'The captured percentage must not be silently replaced')
                self.assertEqual(self.engine._load(self.payload['id'])[1]['chunkPercent'], percent)
                market_parts = [row['volume'] for row in self.primary.sent if row['type']=='market']
                self.assertTrue(all(0. < value <= chunk+1e-8 for value in market_parts))
                self.assertAlmostEqual(sum(market_parts), .7)
                self.assertAlmostEqual(sum(row['lots'] for row in self.primary.rows), .7)
                self.assertFalse(any(not row['__isPosition'] for row in self.primary.rows))
                self.assert_common_stop(self.primary)
                if percent == 100.:
                    self.assertEqual(market_parts, [.7])
                    self.assertFalse(any(row['type']=='limit' for row in self.primary.sent))

    def test_invalid_percentages_reject_before_admission_or_any_trade_command(self):
        for percent in (0., -.001, -1., 100.001, 101., float('nan'), float('inf'), True):
            with self.subTest(percent=percent):
                self.single_account()
                self.payload.update(volume=.7, riskUsd=12.5, chunkPercent=percent, chunkVolume=.1)
                with self.assertRaises(ValueError):
                    self.engine.arm(self.payload)
                self.assertEqual(self.engine.list(), [])
                self.assertEqual(self.primary.db.execute('SELECT COUNT(*) FROM chunk_entries').fetchone()[0], 0)
                self.assertEqual(self.primary.sent, [])

    def test_tiny_percentage_requiring_over_one_hundred_rounds_rejects_before_admission(self):
        self.single_account()
        self.payload.update(volume=10.1, riskUsd=161.6, chunkPercent=1., chunkVolume=.1)
        self.primary.target_volume, self.primary.risk_budget = 10.1, 161.6
        with self.assertRaisesRegex(ValueError, '100 tranches'):
            self.engine.arm(self.payload)
        self.assertEqual(self.payload['chunkPercent'], 1.)
        self.assertEqual(self.engine.list(), [])
        self.assertEqual(self.primary.db.execute('SELECT COUNT(*) FROM chunk_entries').fetchone()[0], 0)
        self.assertEqual(self.primary.sent, [])

    def test_one_percent_large_target_finishes_one_hundred_parts_with_complete_public_identities(self):
        self.single_account()
        self.payload.update(volume=10., riskUsd=160., chunkPercent=1., chunkVolume=.1)
        self.primary.target_volume, self.primary.risk_budget = 10., 160.
        state = self.run_entry()
        self.assertEqual(state['phase'], 'completed')
        self.assertEqual(state['chunkPercent'], 1.)
        self.assertEqual([row['volume'] for row in self.primary.sent if row['type']=='market'], [.1]*100)
        self.assertEqual(len(state['children']), 199)
        self.assertEqual(sum(bool(child['positions']) for child in state['children']), 100)
        self.assertEqual(len(self.engine.get(self.payload['id'])['children']), 199)
        self.assertAlmostEqual(sum(row['lots'] for row in self.primary.rows), 10.)
        self.assert_common_stop(self.primary)

    def test_two_hundred_owned_positions_block_new_openings_before_any_submission(self):
        self.single_account()
        self.payload.update(volume=30., riskUsd=480., chunkPercent=1., chunkVolume=.3)
        self.primary.target_volume, self.primary.risk_budget = 30., 480.
        self.engine.arm(self.payload)
        state = self.engine._load(self.payload['id'])[2]
        for index in range(200):
            ticket = str(1000+index)
            position = dict(ticket=ticket, openingTicket=ticket, volume=.1, fillPrice=102., sl=86., tp=0.)
            state['children'].append(dict(id=self.payload['id']+':h'+str(index+1), index=index+1,
                state='confirmed', type='limit', price=102., prices={'900000002':102.}, stops={'900000002':86.},
                volumes={'900000002':.1}, positions={'900000002':position},
                guards={'900000002':dict(anchor=102., maxGap=6., maxRiskPrice=106., priceFloor=100., priceCeiling=106.)}))
            self.primary.rows.append(dict(ticket=ticket, identifier=ticket, symbol='USTEC', type='buy', lots=.1,
                                          open_price=102., sl=86., tp=0., __isPosition=True))
        self.engine._hybrid_recount(self.payload, state)
        state['phase'] = 'running'
        self.engine._save(state)
        self.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertIn('position slot', self.state()['reason'])
        self.assertEqual(self.primary.sent, [])
        self.assertEqual(len(self.primary.rows), 200)
        self.assertTrue(all(row['__isPosition'] for row in self.primary.rows))

    def test_exact_parent_get_returns_all_five_hundred_twelve_children_without_writes(self):
        self.assertEqual(self.run_entry()['phase'], 'completed')
        state = self.engine._load(self.payload['id'])[2]
        for index in range(len(state['children']), 512):
            state['children'].append(dict(id=self.payload['id']+':h'+str(index+1), index=index+1,
                state='rejected', type='limit', price=102., volumes={'900000002':.1,'900000001':.1}, positions={},
                guards={}, result=dict(status='rejected', reason='Durable no-fill attempt fixture')))
        self.engine._save(state)
        queries = []
        self.primary.db.set_trace_callback(queries.append)
        before = self.primary.db.total_changes
        try:
            exact = self.engine.get(self.payload['id'])
        finally:
            self.primary.db.set_trace_callback(None)
        self.assertEqual(exact['phase'], 'completed')
        self.assertEqual(len(exact['children']), 512)
        self.assertEqual(exact['children'][-1]['id'], self.payload['id']+':h512')
        self.assertEqual(self.primary.db.total_changes, before)
        self.assertEqual(len([query for query in queries if query.lstrip().upper().startswith('SELECT')]), 1)
        self.assertEqual(len(self.primary.sent), 7)

    def test_submission_attempt_limit_cancels_its_last_support_without_an_unreserved_market(self):
        self.single_account()
        self.engine.arm(self.payload)
        state = self.engine._load(self.payload['id'])[2]
        state['children'] = [dict(id=self.payload['id']+':h'+str(index+1), index=index+1, state='rejected',
                                  type='market', price=102., volumes={'900000002':.3}, positions={}, guards={},
                                  result=dict(status='rejected', reason='Recorded fresh-price rejection'))
                             for index in range(511)]
        self.engine._save(state)
        self.tick()
        stopped = self.state()
        self.assertEqual(stopped['phase'], 'stopped')
        self.assertIn('submission limit', stopped['reason'])
        self.assertEqual(len(stopped['children']), 512)
        self.assertEqual([row['type'] for row in self.primary.sent], ['limit'])
        self.assertEqual(self.primary.rows, [])
        self.assertEqual(len(self.primary.cancelled), 1)
        self.tick()
        self.assertEqual(len(self.primary.sent), 1)

    def test_exact_parent_get_validates_identity_and_missing_or_precancelled_reads_never_mutate(self):
        before = self.primary.db.total_changes
        for identifier in ('', '../entry', 'a'*101, None, 123):
            with self.subTest(identifier=identifier), self.assertRaises(ValueError):
                self.engine.get(identifier)
        self.assertIsNone(self.engine.get('missing-parent'))
        self.assertEqual(self.primary.db.total_changes, before)
        self.assertEqual(self.primary.sent, [])
        stopped = self.engine.stop(dict(id='stop-before-arm', entryId='precancelled-parent', expiresAt=time.time()*1000+3000))
        before = self.primary.db.total_changes
        self.assertEqual(self.engine.get('precancelled-parent'), stopped)
        self.assertEqual(self.primary.db.total_changes, before)
        self.assertEqual(self.primary.sent, [])

    def test_terminal_guard_pruning_retains_price_bounds_and_actual_submission_snapshots(self):
        state = self.run_entry()
        self.assertEqual(state['phase'], 'completed')
        saved = self.engine._load(self.payload['id'])[2]
        for child in saved['children']:
            self.assertIn(child['state'], ('confirmed', 'cancelled', 'rejected'))
            for guard in child['guards'].values():
                self.assertNotIn('positions', guard)
                self.assertEqual((guard['priceFloor'],guard['priceCeiling']), (100.,106.))
        market_requests = [row for row in self.primary.sent if row['type']=='market']
        self.assertEqual(len(market_requests[-1]['entryGuard']['positions']), 3,
                         'The submitted final guard must still carry all three earlier positions')
        self.engine.close()
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        self.assertEqual(len([row for row in self.primary.sent if row['type']=='market']), 4)
        self.assert_common_stop(self.primary)

    def test_support_limit_is_clamped_to_lt_when_spread_crosses_level(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=100.1, bid=99.9)
        self.assertEqual(self.run_entry()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertTrue(any(row['type'] == 'limit' for row in bridge.sent))
            self.assertTrue(all(row['price'] == 100. for row in bridge.sent if row['type'] == 'limit'))

    def test_support_fill_can_win_cancel_race_without_extra_market(self):
        for bridge in self.bridges():
            bridge.fill_on_cancel = True
        state = self.run_entry()
        self.assertEqual(state['phase'], 'completed')
        for bridge in self.bridges():
            self.assertEqual([row['volume'] for row in bridge.sent if row['type'] == 'market'], [.3])
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows if row['__isPosition']), 1.)
            self.assert_common_stop(bridge)

    def test_favorable_limit_gap_records_actual_fill_before_blocking_remainder(self):
        self.single_account()
        def favorable_gap_once(bridge, request):
            if not getattr(bridge, 'injected_gap', False):
                bridge.injected_gap = True
                bridge.fill_limit(.2, price=99.)
        self.primary.market_hook = favorable_gap_once
        state = self.run_entry()
        self.assertIn(state['phase'], ('stopped', 'unknown'))
        self.assertAlmostEqual(sum(row['lots'] for row in self.primary.rows if row['__isPosition']), .5)
        self.assertAlmostEqual(state['accounts']['900000002']['filledVolume'], .5)
        self.assertEqual([row['volume'] for row in self.primary.sent if row['type']=='market'], [.3])
        self.assertFalse(any(not row['__isPosition'] for row in self.primary.rows))
        count = len(self.primary.sent)
        for _ in range(4):
            self.tick()
        self.assertEqual(len(self.primary.sent), count)

    def test_partial_support_fill_is_accounted_once_before_residual_market(self):
        def partial_once(bridge, request):
            if not getattr(bridge, 'injected_partial', False):
                bridge.injected_partial = True
                bridge.fill_limit(.2)
        for bridge in self.bridges():
            bridge.market_hook = partial_once
        self.assertEqual(self.run_entry()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertAlmostEqual(sum(row['volume'] for row in bridge.sent if row['type'] == 'market'), .8)
            self.assertAlmostEqual(sum(row['lots'] for row in bridge.rows if row['__isPosition']), 1.)
            self.assert_common_stop(bridge)

    def test_compacted_partial_fill_receipts_preserve_every_account_and_restart_position_identity(self):
        originals = self.capture_diagnostic_receipts()
        def partial_once(bridge, request):
            if not getattr(bridge, 'injected_partial', False):
                bridge.injected_partial = True
                bridge.fill_limit(.2)
        for bridge in self.bridges():
            bridge.market_hook = partial_once
            # Pause after the first .3 market + .2 limit fill, while retaining
            # real protected positions whose identity must survive restart.
            bridge.before_common = lambda current, request: setattr(current, 'price', dict(ask=99., bid=98.8))
        self.engine.arm(self.payload)
        self.tick()
        saved = self.engine._load(self.payload['id'])[2]
        self.assertEqual(saved['phase'], 'paused')
        self.assertEqual(len(saved['children']), 2)
        support, market = saved['children']
        self.assertEqual(support['state'], 'confirmed')
        self.assertEqual(set(support['result']['accountResults']), {'900000002', '900000001'})
        for account in ('900000002', '900000001'):
            outcome = support['result']['accountResults'][account]
            self.assertEqual(outcome['providerOrderId'], support['positions'][account]['openingTicket'])
            self.assertEqual(outcome['status'], 'ok')
            self.assertEqual(support['positions'][account]['volume'], .2)
            self.assertEqual(saved['accounts'][account]['filledVolume'], .5)
            cancelled = support['cancellations'][account]
            self.assert_receipt_compacted(cancelled)
            self.assertEqual(cancelled['operation'], 'cancel')
            self.assertEqual(cancelled['ticket'], outcome['providerOrderId'])
            self.assertEqual(cancelled['status'], 'ok')
        for child in (support, market):
            self.assert_receipt_compacted(child['result'])
        self.assertTrue(all('timings' in receipt and 'serverTiming' in receipt and 'rpcTiming' in receipt
                            for receipt in originals), 'Compaction must not mutate original broker replies')
        sent = [len(bridge.sent) for bridge in self.bridges()]
        common = [len(bridge.common_calls) for bridge in self.bridges()]
        self.engine.close()
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertEqual(self.engine._load(self.payload['id'])[2]['children'], saved['children'])
        for bridge in self.bridges():
            bridge.price = dict(ask=102., bid=101.8)
        self.tick()
        self.assertEqual([len(bridge.sent) for bridge in self.bridges()], sent)
        self.assertEqual([len(bridge.common_calls) for bridge in self.bridges()], common)
        for bridge in self.bridges():
            self.assert_common_stop(bridge)

    def test_compacted_unknown_cancellation_keeps_proof_and_never_replays_after_restart(self):
        originals = self.capture_diagnostic_receipts()
        self.primary.cancel_unknown = True
        self.assertEqual(self.run_entry()['phase'], 'unknown')
        saved = self.engine._load(self.payload['id'])[2]
        support = saved['children'][0]
        receipt = support['cancellations']['900000002']
        self.assert_receipt_compacted(receipt)
        self.assertEqual(receipt['status'], 'unknown')
        self.assertEqual(receipt['ticket'], support['result']['accountResults']['900000002']['providerOrderId'])
        self.assertEqual(receipt['operation'], 'cancel')
        self.assertTrue(receipt['reason'])
        self.assertTrue(all('timings' in original for original in originals))
        sent, cancellations = len(self.primary.sent), len(self.primary.cancelled)
        self.engine.close()
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.primary.cancel_unknown = False
        for _ in range(3):
            self.tick()
        self.assertEqual(self.state()['phase'], 'unknown')
        self.assertEqual((len(self.primary.sent), len(self.primary.cancelled)), (sent, cancellations))
        self.assertEqual(self.engine._load(self.payload['id'])[2]['children'], saved['children'])

    def test_legacy_receipts_keep_original_diagnostics(self):
        self.capture_diagnostic_receipts()
        self.payload.update(executionPolicy='market-limit', chunkVolume=1., chunkPercent=100.)
        self.assertEqual(self.run_entry()['phase'], 'completed')
        receipt = self.engine._load(self.payload['id'])[2]['children'][0]['result']
        for value in [receipt, *receipt['accountResults'].values()]:
            self.assertIn('timings', value)
            self.assertIn('serverTiming', value)
            self.assertIn('rpcTiming', value)

    def test_actual_native_group_journals_keep_full_diagnostics_and_partial_fill_management_proof(self):
        from test_hybrid_integration import HybridIntegrationTests, LifecycleMT5
        from mt5_bridge import Bridge
        native = HybridIntegrationTests()
        native.setUp()
        self.addCleanup(native.doCleanups)
        secondary = LifecycleMT5(native.now)
        secondary.account.login = 900000001
        follower = Bridge(secondary, dict(account=900000001, server='Example-MT5', symbols=['USTEC'], tradingEnabled=True),
                          pathlib.Path(native.temp.name)/'follower.sqlite')
        self.addCleanup(follower.db.close)
        self.addCleanup(native.bridge.close_group_executor)
        native.bridge.linked['900000001'] = Worker(native.bridge, follower)
        native.payload['mirrorAccounts'] = [dict(account='900000001', multiplier=1.)]
        def partial_before_first_market(terminal):
            send = terminal.order_send
            def injected(request):
                if request['action'] == terminal.TRADE_ACTION_DEAL and not getattr(terminal, 'partial_injected', False):
                    terminal.partial_injected = True
                    order = terminal.orders[0]
                    order.volume_current = round(order.volume_current-.2, 8)
                    terminal.positions.append(terminal.position_from(order.ticket,
                        dict(symbol=order.symbol, type=terminal.ORDER_TYPE_BUY, volume=.2,
                             price=order.price_open, sl=order.sl, tp=order.tp, comment=order.comment)))
                    terminal.observe('partial-pending-fill')
                return send(request)
            terminal.order_send = injected
        for terminal in (native.mt5, secondary):
            partial_before_first_market(terminal)
        state = native.run_entry()
        native.assert_protected_complete(state)
        saved = native.engine._load(native.payload['id'])[2]
        support = saved['children'][0]
        self.assertEqual({account: row['volume'] for account, row in support['positions'].items()},
                         {'900000002': .2, '900000001': .2})
        for child in saved['children']:
            self.assertNotIn('timings', child['result'])
            self.assertEqual(set(child['result']['accountResults']), {'900000002', '900000001'})
            for outcome in child['result']['accountResults'].values():
                self.assertNotIn('timings', outcome)
                self.assertEqual(outcome['status'], 'ok')
                self.assertTrue(outcome['providerOrderId'])
            for receipt in child.get('cancellations', {}).values():
                self.assertNotIn('timings', receipt)
                self.assertEqual(receipt['status'], 'ok')
        group_rows = native.bridge.db.execute('SELECT id,payload,result FROM groups ORDER BY id').fetchall()
        for _, _, text in group_rows:
            receipt = json.loads(text)
            self.assertIn('server', receipt['timings'])
            for outcome in receipt['accountResults'].values():
                self.assertIn('server', outcome['timings'])
                self.assertIn('mt5Ms', outcome['timings']['server'])
                self.assertIn('requestedVolume', outcome)
        native_rows = {}
        for account, bridge in (('900000002', native.bridge), ('900000001', follower)):
            native_rows[account] = bridge.db.execute('SELECT id,result FROM requests ORDER BY id').fetchall()
            self.assertTrue(native_rows[account])
            for _, text in native_rows[account]:
                receipt = json.loads(text)
                self.assertIn('mt5Ms', receipt['timings']['server'])
            for rule in bridge.time_rules():
                self.assertIn('timings', rule['result'])
        first_group = next((json.loads(payload), json.loads(receipt)) for identifier, payload, receipt in group_rows
                           if identifier == support['id'])
        states = {'900000002': native.bridge.local_management(), '900000001': follower.local_management()}
        self.assertEqual(native.bridge.hybrid_opened_volumes(*first_group, states), {'900000002': .2, '900000001': .2})
        self.assertFalse(any('Объёмы' in row.get('reason', '') for row in native.bridge.management()['alerts']))
        native.engine.close()
        native.engine = ChunkEntries(native.bridge)
        self.addCleanup(native.engine.close)
        native.engine.tick()
        self.assertEqual(native.engine.list()[0]['phase'], 'completed')
        self.assertEqual(native.bridge.db.execute('SELECT id,payload,result FROM groups ORDER BY id').fetchall(), group_rows)
        for account, bridge in (('900000002', native.bridge), ('900000001', follower)):
            self.assertEqual(bridge.db.execute('SELECT id,result FROM requests ORDER BY id').fetchall(), native_rows[account])

    def test_lost_cancel_confirmation_blocks_further_market_and_is_not_replayed(self):
        self.primary.cancel_unknown = True
        self.assertEqual(self.run_entry()['phase'], 'unknown')
        count = len(self.primary.cancelled)
        for _ in range(4):
            self.tick()
        self.assertEqual(len(self.primary.cancelled), count)
        self.assertLessEqual(sum(row['volume'] for row in self.primary.sent if row['type'] == 'market'), .3)
        self.assertTrue(all(row['volume'] <= 1. + 1e-8 for row in self.primary.observations))

    def test_common_stop_failure_blocks_next_market_and_leaves_no_owned_pending(self):
        self.follower.common_unknown = True
        self.assertEqual(self.run_entry()['phase'], 'unknown')
        for bridge in self.bridges():
            self.assertLessEqual(sum(row['volume'] for row in bridge.sent if row['type'] == 'market'), .3)
            self.assertFalse(any(not row['__isPosition'] for row in bridge.rows))

    def test_stale_follower_prevents_all_submissions(self):
        self.engine.arm(self.payload)
        self.follower.stale = True
        self.tick()
        self.assertEqual(self.primary.sent, [])
        self.assertEqual(self.follower.sent, [])

    def test_full_outer_limit_fill_completes_without_any_market(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=110., bid=109.8)
        self.engine.arm(self.payload)
        self.tick()
        for bridge in self.bridges():
            self.assertEqual(bridge.sent[0]['type'], 'limit')
            self.assertEqual(bridge.sent[0]['volume'], 1.)
            self.assertEqual(bridge.sent[0]['price'], 106.)
            bridge.fill_limit(1.)
        self.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        for bridge in self.bridges():
            self.assertFalse(any(row['type'] == 'market' for row in bridge.sent))
            self.assert_common_stop(bridge)

    def test_unchanged_outside_pending_ticks_do_not_write_the_durable_ledger(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=110., bid=109.8)
        self.engine.arm(self.payload)
        self.tick()
        changes = self.primary.db.total_changes
        for _ in range(12):
            # Force the bounded status read too, rather than measuring only
            # the shortcut before the next pending-status poll is due.
            self.tick()
        self.assertEqual(self.primary.db.total_changes, changes)
        self.assertEqual(self.state()['phase'], 'running')
        self.assertEqual(len(self.primary.sent), 1)
        self.assertEqual(len(self.follower.sent), 1)

    def test_unknown_support_placement_never_repeats_or_sends_market(self):
        self.engine.arm(self.payload)
        self.primary.raise_send = True
        self.tick()
        self.assertEqual(self.state()['phase'], 'unknown')
        self.primary.raise_send = False
        for _ in range(4):
            self.tick()
        self.assertEqual(self.primary.sent, [])

    def test_restart_does_not_resume_owned_outer_pending_order(self):
        for bridge in self.bridges():
            bridge.price = dict(ask=110., bid=109.8)
        self.engine.arm(self.payload)
        self.tick()
        self.engine.close()
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.assertEqual(self.state()['phase'], 'unknown')
        for bridge in self.bridges():
            bridge.price = dict(ask=102., bid=101.8)
        self.tick()
        self.assertEqual(len(self.primary.sent), 1)

    def test_single_account_varying_fills_share_actual_weighted_stop(self):
        self.single_account()
        def move_at_execution(bridge, request):
            bridge.price = dict(ask=round(bridge.price['ask'] + .1, 8), bid=round(bridge.price['bid'] + .1, 8))
        self.primary.market_hook = move_at_execution
        self.assertEqual(self.run_entry()['phase'], 'completed')
        self.assertGreater(len({row['open_price'] for row in self.primary.rows}), 1)
        self.assert_common_stop(self.primary)
        expected = sum(row['open_price']*row['lots'] for row in self.primary.rows)
        self.assertAlmostEqual(self.state()['accounts']['900000002']['averagePrice'], expected)

    def test_level_stop_stays_fixed_with_varying_market_prices(self):
        self.single_account()
        self.payload.update(stopMode='level', riskUsd=22.)
        self.primary.risk_budget = 22.
        def move_at_execution(bridge, request):
            bridge.price = dict(ask=round(bridge.price['ask'] + .1, 8), bid=round(bridge.price['bid'] + .1, 8))
        self.primary.market_hook = move_at_execution
        self.assertEqual(self.run_entry()['phase'], 'completed')
        self.assertTrue(all(row['sl'] == 84. for row in self.primary.rows))
        self.assertTrue(all(row['risk'] <= 22. + 1e-8 for row in self.primary.observations))

    def test_exact_lt_is_inclusive_for_market_without_invalid_support(self):
        self.single_account()
        self.primary.price = dict(ask=100., bid=99.8)
        self.assertEqual(self.run_entry()['phase'], 'completed')
        self.assertEqual([row['volume'] for row in self.primary.sent if row['type']=='market'], [.3, .3, .3, .1])
        self.assertFalse(any(row['type']=='limit' for row in self.primary.sent))
        self.assert_common_stop(self.primary)

    def test_server_rejects_full_size_chunk_forged_under_thirty_percent_policy(self):
        self.payload['chunkVolume'] = 1.
        with self.assertRaisesRegex(ValueError, '[Cc]hunk|[Pp]ercent|[Pp]art'):
            self.engine.arm(self.payload)
        self.assertEqual(self.primary.sent, [])

    def test_price_leaving_zone_between_support_and_market_switches_to_outer_limit_and_resumes(self):
        self.single_account()
        self.primary.enforce_market_zone = True
        def leave_zone(bridge, request):
            bridge.price = dict(ask=107., bid=106.8)
        self.primary.market_hook = leave_zone
        self.engine.arm(self.payload)
        self.tick()
        self.assertIn(self.state()['phase'], ('armed', 'paused', 'running'))
        self.assertFalse(any(row['type']=='market' for row in self.primary.sent))
        pending = [row for row in self.primary.rows if not row['__isPosition']]
        self.assertTrue(all(row['open_price'] == 106. and row['lots'] == 1. for row in pending))
        self.assertTrue(self.primary.cancelled)
        self.primary.market_hook = None
        self.primary.price = dict(ask=102., bid=101.8)
        for _ in range(4):
            self.tick()
            if self.state()['phase'] == 'completed':
                break
        self.assertEqual(self.state()['phase'], 'completed')
        self.assert_common_stop(self.primary)

    def test_external_close_blocks_entry_permanently_after_partial_execution(self):
        self.single_account()
        def close_before_common(bridge, request):
            if bridge.rows:
                bridge.rows.pop(0)
        self.primary.before_common = close_before_common
        self.run_entry()
        self.assertIn(self.state()['phase'], ('stopped', 'unknown'))
        sends = len(self.primary.sent)
        self.primary.before_common = None
        for _ in range(4):
            self.tick()
        self.assertEqual(len(self.primary.sent), sends)
        self.assertFalse(any(not row['__isPosition'] for row in self.primary.rows))

    def test_asymmetric_mirror_limit_fill_never_overfills_the_completed_account(self):
        self.primary.fill_on_cancel = True
        self.run_entry()
        self.assertAlmostEqual(sum(row['lots'] for row in self.primary.rows if row['__isPosition']), 1.)
        self.assertTrue(all(row['volume'] <= 1. + 1e-8 and row['risk'] <= 16. + 1e-8
                            for bridge in self.bridges() for row in bridge.observations))
        self.assertFalse(any(not row['__isPosition'] for bridge in self.bridges() for row in bridge.rows))
        sent = len(self.primary.sent)
        for _ in range(4):
            self.tick()
        self.assertEqual(len(self.primary.sent), sent)

    def test_varied_partial_fills_and_prices_preserve_budget_at_every_transition(self):
        randomizer = random.Random(20261021)
        for side in ('buy', 'sell'):
            for scenario in range(12):
                with self.subTest(side=side, scenario=scenario):
                    self.single_account()
                    self.payload.update(side=side, sl=84. if side=='buy' else 116.)
                    self.primary.price = dict(ask=102., bid=101.8) if side=='buy' else dict(bid=98., ask=98.2)
                    def fill_and_move(bridge, request):
                        pending = next((row for row in bridge.rows if not row['__isPosition']), None)
                        if pending and randomizer.choice((False, True)):
                            bridge.fill_limit(min(.1, pending['lots']))
                        move = randomizer.randint(2, 58) / 10.
                        quote = 100. + move if side=='buy' else 100. - move
                        bridge.price = dict(ask=quote, bid=round(quote-.2, 8)) if side=='buy' else dict(bid=quote, ask=round(quote+.2, 8))
                    self.primary.market_hook = fill_and_move
                    state = self.run_entry()
                    self.assertEqual(state['phase'], 'completed')
                    self.assertAlmostEqual(sum(row['lots'] for row in self.primary.rows), 1.)
                    self.assert_common_stop(self.primary)


if __name__ == '__main__':
    unittest.main()
