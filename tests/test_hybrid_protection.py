"""Real Bridge/coordinator protection contracts; in-memory MT5 trades only."""
import copy
import datetime
import http.client
import json
import math
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_hybrid_integration import LifecycleMT5
from mt5_bridge import Bridge, create_server


class ProtectionMT5(LifecycleMT5):
    """Native state with receipt loss, rejection and fill/cancel race hooks."""
    def __init__(self, now, account, side):
        super().__init__(now)
        self.account.login = account
        self.side = side
        self.next_ticket = account * 1000
        self.tick.bid, self.tick.ask = (25001.8, 25002.) if side == 'buy' else (24998., 24998.2)
        self.after_market = None
        self.sl_failure = None
        self.cancel_unknown = False
        self.partial_cancel_volume = None
        self.remap_pending_fill = False
        self.close_unknown = False
        self.close_apply_then_unknown = False
        self.before_close = None
        self.protection_sends = 0
        self.native_events = []
        self.group_budgets = {'cid:protection-one': (1., 16.)}

    def observe(self, action):
        """Each entry's pending reservation and positions share one risk cap."""
        groups = {}
        for comment, (target, budget) in self.group_budgets.items():
            positions = [row for row in self.positions if row.comment == comment]
            pending = [row for row in self.orders if row.comment == comment]
            volume = sum(row.volume for row in positions) + sum(row.volume_current for row in pending)
            risk = sum(max(0., -self.order_calc_profit(row.type, row.symbol, row.volume, row.price_open, row.sl))
                       for row in positions)
            risk += sum(max(0., (row.price_open-row.sl if row.type == self.ORDER_TYPE_BUY_LIMIT else row.sl-row.price_open)
                            * row.volume_current) for row in pending)
            if volume > target + 1e-8 or risk > budget + 1e-8:
                raise AssertionError('A native transition exceeded its entry target or risk budget')
            groups[comment] = dict(volume=volume, risk=risk)
        self.exposure.append(dict(action=action, groups=groups))
        self.native_events.append(dict(action=action, positions=copy.deepcopy(self.positions), orders=copy.deepcopy(self.orders)))

    def order_send(self, request):
        action = request['action']
        old_bid, old_ask = self.tick.bid, self.tick.ask
        if action == self.TRADE_ACTION_DEAL and 'position' in request:
            if self.before_close:
                self.before_close()
            self.sent.append(copy.deepcopy(request))
            position = next(row for row in self.positions if row.ticket == request['position'])
            if not 0 < request['volume'] <= position.volume + 1e-8:
                raise AssertionError('A simulated close exceeded the exact native remainder')
            if not self.close_unknown or self.close_apply_then_unknown:
                amount = request['volume']
                if abs(amount-position.volume) <= 1e-8:
                    self.positions.remove(position)
                else:
                    position.volume = round(position.volume-amount, 8)
                self.history_deals.append(N(symbol=position.symbol, position_id=position.identifier,
                    entry=1, volume=amount, reason=0))
            self.observe(action)
            if self.close_unknown:
                return None
            return N(retcode=self.TRADE_RETCODE_DONE, order=position.ticket,
                     volume=request['volume'], price=request['price'], comment='closed')
        if action == self.TRADE_ACTION_SLTP:
            self.protection_sends += 1
            failure = self.sl_failure
            if failure and self.protection_sends == failure['at']:
                if failure.get('apply'):
                    super().order_send(request)
                else:
                    self.sent.append(copy.deepcopy(request))
                    self.observe(action)
                if failure.get('unknown'):
                    return None
                return N(retcode=10016, order=request['position'], volume=0., price=0., comment='simulated protection rejection')
        if action == self.TRADE_ACTION_REMOVE and self.cancel_unknown:
            self.sent.append(copy.deepcopy(request))
            self.observe(action)
            return None
        if action == self.TRADE_ACTION_REMOVE and self.partial_cancel_volume is not None:
            order = next(row for row in self.orders if row.ticket == request['order'])
            amount, self.partial_cancel_volume = self.partial_cancel_volume, None
            if not 0 < amount < order.volume_current:
                raise AssertionError('The partial-cancel fixture must fill only part of the reservation')
            self.sent.append(copy.deepcopy(request))
            self.orders.remove(order)
            opening = dict(symbol=order.symbol,
                type=self.ORDER_TYPE_BUY if order.type == self.ORDER_TYPE_BUY_LIMIT else self.ORDER_TYPE_SELL,
                volume=amount, price=order.price_open, sl=order.sl, tp=order.tp, comment=order.comment)
            self.positions.append(self.position_from(order.ticket, opening))
            self.history_orders.append(N(ticket=order.ticket, symbol=order.symbol, position_id=order.ticket, state=4))
            self.history_deals.append(N(symbol=order.symbol, position_id=order.ticket, entry=0, volume=amount, reason=0))
            self.observe(action)
            return N(retcode=self.TRADE_RETCODE_DONE, order=order.ticket, volume=0., price=0., comment='partial fill then cancelled')
        result = super().order_send(request)
        if action == self.TRADE_ACTION_REMOVE and self.fill_on_cancel and self.remap_pending_fill:
            opening_ticket = request['order']
            position = next(row for row in self.positions if row.ticket == opening_ticket)
            position.ticket, position.identifier = opening_ticket+10000, opening_ticket+20000
            for order in self.history_orders:
                if order.ticket == opening_ticket:
                    order.position_id = position.identifier
            for deal in self.history_deals:
                if deal.position_id == opening_ticket:
                    deal.position_id = position.identifier
            self.remap_pending_fill = False
        if action == self.TRADE_ACTION_DEAL:
            if self.side == 'sell':
                self.tick.bid, self.tick.ask = round(old_bid-.2, 8), round(old_ask-.2, 8)
            if self.after_market:
                self.after_market(self)
        return result


class InProcessWorker:
    """Keep real account locking/group execution without native worker IPC."""
    def __init__(self, primary, linked):
        self.primary, self.linked = primary, linked

    def available(self):
        return True

    def call(self, method, *args):
        if self.primary.lock._is_owned():
            raise AssertionError('Primary MT5 lock held during a follower RPC')
        with self.linked.lock:
            return getattr(self.linked, method)(*args)


class HybridProtectionTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.datetime(2026, 10, 21, 14, tzinfo=datetime.timezone.utc).timestamp()
        clock = patch('time.time', return_value=self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.resources = []
        self.directories = []
        self.addCleanup(self.close_resources)

    def close_resources(self):
        # Observers run on the primary executor and may read a follower's
        # SQLite journal. Drain them before disposing either simulated account.
        for bridge in self.resources:
            bridge.chunk_entries.close()
            bridge.close_group_executor()
        for bridge in reversed(self.resources):
            bridge.db.close()
        for directory in self.directories:
            directory.cleanup()

    def make(self, side='buy', accounts=1):
        self.side = side
        directory = tempfile.TemporaryDirectory()
        self.directories.append(directory)
        self.natives, self.bridges, self.paths = [], [], []
        for account in (900000002, 900000001)[:accounts]:
            native = ProtectionMT5(self.now, account, side)
            path = pathlib.Path(directory.name) / (str(account) + '.sqlite')
            config = dict(account=account, server='Example-MT5', symbols=['USTEC'], tradingEnabled=True)
            bridge = Bridge(native, config, path)
            self.natives.append(native)
            self.bridges.append(bridge)
            self.paths.append(path)
            self.resources.append(bridge)
        self.primary = self.bridges[0]
        self.engine = self.primary.chunk_entries
        if accounts == 2:
            self.primary.linked['900000001'] = InProcessWorker(self.primary, self.bridges[1])
            self.primary.warm_group_executor()
        due = datetime.datetime(2026, 10, 21, 19, tzinfo=datetime.timezone.utc).timestamp()*1000
        self.payload = dict(id='protection-one', cid='protection-one', symbol='USTEC', side=side,
            trigger=25000., level=25000., sl=24984. if side == 'buy' else 25016., tp=0.,
            riskUsd=16., volume=1., chunkVolume=.3, chunkPercent=30., entryMode='side-zone',
            executionPolicy='hybrid-market-limit-v1', zoneRadius=6., zoneMultiplier=1,
            tickSize=.1, stopOffsetPts=160., stopMode='entry', maxGap=6.,
            expiresAt=self.now*1000+3000, closeAtUtc=due, closeTimeZone='America/New_York',
            closeLocalDate='2026-10-21', closeLocalTime='15:00', cancelPendingAtDeadline=True)
        if accounts == 2:
            self.payload['mirrorAccounts'] = [dict(account='900000001', multiplier=1.)]

    def state(self):
        return self.engine.get(self.payload['id'])

    def tick(self):
        self.engine.pending_poll_at.clear()
        self.engine.tick()
        return self.state()

    def quote(self, native, price):
        if self.side == 'buy':
            native.tick.bid, native.tick.ask = round(price-.2, 8), price
        else:
            native.tick.bid, native.tick.ask = price, round(price+.2, 8)

    def entry(self, active=False):
        if active:
            for native in self.natives:
                native.after_market = lambda broker: self.quote(broker, 25007. if self.side == 'buy' else 24993.)
        self.engine.arm(self.payload)
        state = self.tick()
        if active:
            self.assertIn(state['phase'], ('running', 'paused'))
            for native in self.natives:
                self.assertAlmostEqual(sum(row.volume for row in native.positions), .3)
                self.assertEqual(len(native.orders), 1)
                self.assertAlmostEqual(native.orders[0].volume_current, .7)
                native.after_market = None
        else:
            self.assertEqual(state['phase'], 'completed', state.get('reason'))
        return state

    def command(self, **extra):
        payload = dict(id='protect-1', entryId=self.payload['id'], symbol='USTEC', operation='modify',
                       account='900000002', expiresAt=self.now*1000+3000)
        payload.update(extra)
        return payload

    def protect(self, **extra):
        response = self.engine.protection(self.command(**extra))
        self.assertEqual(response['result']['entryId'], self.payload['id'])
        return response

    def assert_native_protection(self, native, sl=None, tp=None, count=None):
        positions = [row for row in native.positions if row.comment == 'cid:' + self.payload['cid']]
        self.assertTrue(positions)
        if count is not None:
            self.assertEqual(len(positions), count)
        for row in positions:
            if sl is not None:
                self.assertAlmostEqual(row.sl, sl, places=7)
            if tp is not None:
                self.assertAlmostEqual(row.tp, tp, places=7)
        return positions

    def markets(self, native):
        return [row for row in native.sent if row['action'] == native.TRADE_ACTION_DEAL and 'position' not in row]

    def closes(self, native):
        return [row for row in native.sent if row['action'] == native.TRADE_ACTION_DEAL and 'position' in row]

    def close_command(self, **extra):
        command = dict(id='close-card-1', entryId=self.payload['id'], symbol='USTEC',
                       expiresAt=self.now*1000+3000)
        command.update(extra)
        return command

    def close_card(self, **extra):
        return self.engine.close_position(self.close_command(**extra))

    def native_exit(self, native, position, volume):
        """A broker-proved external exit; it never sends a test-side trade."""
        if not 0 < volume <= position.volume + 1e-8:
            raise AssertionError('Invalid simulated native exit volume')
        if abs(position.volume-volume) < 1e-8:
            native.positions.remove(position)
        else:
            position.volume = round(position.volume-volume, 8)
        native.history_deals.append(N(symbol=position.symbol, position_id=position.identifier,
                                      entry=1, volume=volume, reason=0))

    def assert_weighted_breakeven(self, native):
        positions = [row for row in native.positions if row.comment == 'cid:' + self.payload['cid']]
        volume = sum(row.volume for row in positions)
        average = sum(row.volume*row.price_open for row in positions)/volume
        self.assertEqual(len({row.sl for row in positions}), 1)
        protective_rounding = (positions[0].sl-average)*(1 if self.side == 'buy' else -1)
        self.assertGreaterEqual(protective_rounding, -1e-7)
        self.assertLessEqual(protective_rounding, .1+1e-7)

    def start_http(self):
        self.primary.config['token'] = 'offline-protection-fixture'
        self.server = create_server(self.primary, ('127.0.0.1', 0))
        worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        worker.start()
        def stop():
            self.server.shutdown()
            self.server.server_close()
            worker.join(3)
        self.addCleanup(stop)

    def request_api(self, method, path, payload=None, authenticated=True):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        headers = {'Content-Type': 'application/json'}
        if authenticated:
            headers['Authorization'] = 'Bearer offline-protection-fixture'
        try:
            connection.request(method, path, body=None if payload is None else json.dumps(payload), headers=headers)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_completed_buy_and_sell_change_all_owned_parts_sl_and_tp(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry()
                sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
                response = self.protect(sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_native_protection(self.natives[0], sl, tp, 4)
                self.assertEqual(response['entry']['phase'], 'completed')
                self.assertAlmostEqual(response['entry']['accounts']['900000002']['sl'], sl)
                self.assertEqual(len(self.markets(self.natives[0])), 4)

    def test_active_buy_and_sell_manual_levels_persist_into_every_future_part(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry(active=True)
                sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
                response = self.protect(sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assertEqual(self.natives[0].orders, [])
                self.assert_native_protection(self.natives[0], sl, tp, 1)
                self.quote(self.natives[0], 25003. if side == 'buy' else 24997.)
                state = self.tick()
                self.assertEqual(state['phase'], 'completed', state.get('reason'))
                self.assert_native_protection(self.natives[0], sl, tp, 4)
                self.assertAlmostEqual(sum(row.volume for row in self.natives[0].positions), 1.)
                self.assertEqual([row['volume'] for row in self.markets(self.natives[0])], [.3, .3, .3, .1])

    def test_weighted_breakeven_is_individual_per_account_and_protectively_rounded(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side, accounts=2)
                self.quote(self.natives[1], 25003. if side == 'buy' else 24997.)
                self.entry()
                expected = []
                for native in self.natives:
                    average = sum(row.volume*row.price_open for row in native.positions)/sum(row.volume for row in native.positions)
                    expected.append((math.ceil(average/.1-1e-8) if side == 'buy' else math.floor(average/.1+1e-8))*.1)
                    self.quote(native, 25010. if side == 'buy' else 24990.)
                response = self.protect(operation='breakeven', mirrorAccounts=[dict(account='900000001')])
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assertNotAlmostEqual(expected[0], expected[1])
                for native, stop in zip(self.natives, expected):
                    self.assert_native_protection(native, stop, 0., 4)

    def test_active_breakeven_remains_frozen_after_new_prices_and_fills(self):
        self.make()
        self.entry(active=True)
        frozen = self.natives[0].positions[0].price_open
        response = self.protect(operation='breakeven', tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(self.natives[0], frozen, 25040., 1)
        self.quote(self.natives[0], 25004.)
        state = self.tick()
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        self.assert_native_protection(self.natives[0], frozen, 25040., 4)
        self.assertGreater(sum(row.volume*row.price_open for row in self.natives[0].positions), frozen)

    def test_reaching_manual_breakeven_stops_remainder_and_a_later_return_does_not_reenter(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry(active=True)
                native = self.natives[0]
                response = self.protect(operation='breakeven')
                self.assertEqual(response['result']['status'], 'ok', response)
                frozen = native.positions[0].sl
                count = len(native.sent)
                self.quote(native, frozen-.1 if side == 'buy' else frozen+.1)
                state = self.tick()
                self.assertEqual(state['phase'], 'stopped', state.get('reason'))
                self.quote(native, 25004. if side == 'buy' else 24996.)
                for _ in range(3):
                    self.tick()
                self.assertEqual(len(native.sent), count)
                self.assertEqual(len(self.markets(native)), 1)
                self.assert_native_protection(native, frozen, count=1)

    def test_selected_follower_only_does_not_touch_primary(self):
        self.make(accounts=2)
        self.entry()
        before = copy.deepcopy(self.natives[0].positions)
        primary_sent = len(self.natives[0].sent)
        response = self.protect(account='900000001', sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertEqual(set(response['result']['accountResults']), {'900000001'})
        self.assertEqual(self.natives[0].positions, before)
        self.assertEqual(len(self.natives[0].sent), primary_sent)
        self.assert_native_protection(self.natives[1], 24990., 25040., 4)

    def test_active_selected_follower_override_and_primary_default_both_survive_future_parts(self):
        self.make(accounts=2)
        self.entry(active=True)
        response = self.protect(account='900000001', sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(self.natives[0], 24986., 0., 1)
        self.assert_native_protection(self.natives[1], 24990., 25040., 1)
        for native in self.natives:
            self.quote(native, 25003.)
        state = self.tick()
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        primary = self.assert_native_protection(self.natives[0], tp=0., count=4)
        average = sum(row.volume*row.price_open for row in primary)
        self.assertEqual(len({row.sl for row in primary}), 1)
        self.assertTrue(15.9-1e-7 <= average-primary[0].sl <= 16.+1e-7)
        self.assert_native_protection(self.natives[1], 24990., 25040., 4)

    def test_explicit_captured_mirrors_change_all_and_other_account_is_rejected(self):
        self.make(accounts=2)
        self.entry()
        response = self.protect(sl=24990., tp=25040., mirrorAccounts=[dict(account='900000001')])
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertEqual(set(response['result']['accountResults']), {'900000002', '900000001'})
        for native in self.natives:
            self.assert_native_protection(native, 24990., 25040., 4)
        count = sum(len(native.sent) for native in self.natives)
        with self.assertRaises(ValueError):
            self.protect(id='foreign-account', account='999999', sl=24992.)
        self.assertEqual(sum(len(native.sent) for native in self.natives), count)

    def test_only_tp_preserves_common_stop_and_only_sl_preserves_tp(self):
        self.make()
        self.entry()
        stop = self.natives[0].positions[0].sl
        self.assertEqual(self.protect(tp=25040.)['result']['status'], 'ok')
        self.assert_native_protection(self.natives[0], stop, 25040.)
        self.assertEqual(self.protect(id='sl-only', sl=24990.)['result']['status'], 'ok')
        self.assert_native_protection(self.natives[0], 24990., 25040.)
        self.assertEqual(self.protect(id='tp-clear', tp=0.)['result']['status'], 'ok')
        self.assert_native_protection(self.natives[0], 24990., 0.)

    def test_active_tp_only_keeps_weighted_entry_stop_policy_on_future_parts(self):
        self.make()
        self.entry(active=True)
        response = self.protect(tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(self.natives[0], 24986., 25040., 1)
        self.quote(self.natives[0], 25003.)
        state = self.tick()
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        positions = self.assert_native_protection(self.natives[0], tp=25040., count=4)
        average = sum(row.volume*row.price_open for row in positions)
        self.assertEqual(len({row.sl for row in positions}), 1)
        self.assertTrue(15.9-1e-7 <= average-positions[0].sl <= 16.+1e-7)

    def test_all_selected_accounts_preflight_before_the_first_native_protection_change(self):
        self.make(accounts=2)
        self.entry()
        before = [copy.deepcopy(native.positions) for native in self.natives]
        counts = [len(native.sent) for native in self.natives]
        self.natives[1].tick.time = self.now-2.
        response = self.protect(sl=24990., tp=25040., mirrorAccounts=[dict(account='900000001')])
        self.assertEqual(response['result']['status'], 'rejected', response)
        self.assertEqual([len(native.sent) for native in self.natives], counts)
        self.assertEqual([native.positions for native in self.natives], before)

    def test_increased_native_position_or_forged_positions_are_not_adopted_as_owned(self):
        self.make()
        self.entry()
        native = self.natives[0]
        count = len(native.sent)
        original = native.positions[0].volume
        native.positions[0].volume = .4
        response = self.protect(sl=24990., tp=25040.)
        self.assertNotEqual(response['result']['status'], 'ok')
        self.assertEqual(len(native.sent), count)
        native.positions[0].volume = original
        with self.assertRaises(ValueError):
            self.engine.protection(self.command(id='forged-snapshot', sl=24990., positions=[dict(ticket='123')]))
        self.assertEqual(len(native.sent), count)

    def test_proven_closed_part_does_not_prevent_protection_or_breakeven_of_completed_remainder(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry()
                native = self.natives[0]
                closed = native.positions[0]
                self.native_exit(native, closed, closed.volume)
                sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
                response = self.protect(sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_native_protection(native, sl, tp, 3)
                self.assertFalse(any(row['action'] == native.TRADE_ACTION_SLTP and row['position'] == closed.ticket
                                     for row in native.sent if row.get('sl') == sl))
                self.quote(native, 25010. if side == 'buy' else 24990.)
                response = self.protect(id='remaining-be', operation='breakeven')
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_weighted_breakeven(native)
                self.assertAlmostEqual(sum(row.volume for row in native.positions), .7)
                self.assertEqual(len(self.markets(native)), 4)
                self.assertEqual(native.orders, [])
                self.assertEqual(self.close_card()['result']['status'], 'ok')
                self.assertAlmostEqual(sum(row['volume'] for row in self.closes(native)), .7)

    def test_proven_partial_remaining_volume_is_protected_and_weighted_without_reopening_it(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry()
                native = self.natives[0]
                self.native_exit(native, native.positions[0], .2)
                sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
                response = self.protect(sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_native_protection(native, sl, tp, 4)
                self.quote(native, 25010. if side == 'buy' else 24990.)
                response = self.protect(id='partial-remainder-be', operation='breakeven')
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_weighted_breakeven(native)
                for _ in range(3):
                    self.tick()
                self.assertAlmostEqual(sum(row.volume for row in native.positions), .8)
                self.assertEqual(len(self.markets(native)), 4)
                self.assertEqual(native.orders, [])

    def test_active_part_reduced_externally_can_be_protected_but_never_refilled(self):
        self.make()
        self.entry(active=True)
        native = self.natives[0]
        self.native_exit(native, native.positions[0], .1)
        response = self.protect(sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(native, 24990., 25040., 1)
        self.assertEqual(native.orders, [])
        self.quote(native, 25003.)
        for _ in range(3):
            state = self.tick()
        self.assertIn(state['phase'], ('paused', 'stopped'))
        self.assertEqual(len(self.markets(native)), 1)
        self.assertAlmostEqual(sum(row.volume for row in native.positions), .2)
        response = self.protect(id='reduced-update', tp=25050.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertIn(response['entry']['phase'], ('paused', 'stopped'))
        old = self.primary
        old.chunk_entries.close()
        restarted = Bridge(native, old.config, self.paths[0])
        self.resources.append(restarted)
        self.primary, self.engine = restarted, restarted.chunk_entries
        self.assertTrue(self.engine._load(self.payload['id'])[2]['entryReduced'])
        response = self.protect(id='reduced-after-restart', tp=25060.)
        self.assertEqual(response['result']['status'], 'ok', response)
        for _ in range(3):
            self.tick()
        self.assertEqual(len(self.markets(native)), 1)
        self.assert_native_protection(native, 24990., 25060., 1)
        self.assertAlmostEqual(sum(row.volume for row in native.positions), .2)

    def test_partial_reduction_without_matching_broker_exit_history_does_not_authorize_protection(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.positions[0].volume = .1
        count = len(native.sent)
        response = self.protect(sl=24990., tp=25040.)
        self.assertNotEqual(response['result']['status'], 'ok', response)
        self.assertEqual(len(native.sent), count)

    def indirect_limit_entry(self, side='buy'):
        self.make(side)
        self.entry(active=True)
        native = self.natives[0]
        native.fill_on_cancel = native.remap_pending_fill = True
        sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
        response = self.protect(sl=sl, tp=tp)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(native, sl, tp, 2)
        _, _, state = self.engine._load(self.payload['id'])
        child = next(row for row in state['children'] if row['type'] == 'limit' and row.get('positions'))
        captured = child['positions']['900000002']
        actual = next(row for row in native.positions if str(row.ticket) == captured['ticket'])
        self.assertNotEqual(captured['openingTicket'], str(actual.ticket))
        self.assertNotEqual(captured['openingTicket'], str(actual.identifier))
        self.assertNotEqual(actual.ticket, actual.identifier)
        self.assertEqual(self.tick()['phase'], 'completed')
        return native, captured, actual

    def test_indirect_pending_history_link_preserves_whole_sl_tp_breakeven_and_exact_native_close(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                native, _, _ = self.indirect_limit_entry(side)
                self.quote(native, 25010. if side == 'buy' else 24990.)
                response = self.protect(id='indirect-be', operation='breakeven')
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_weighted_breakeven(native)
                tickets = {row.ticket for row in native.positions}
                response = self.close_card()
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assertEqual({row['position'] for row in self.closes(native)}, tickets)
                self.assertAlmostEqual(sum(row['volume'] for row in self.closes(native)), 1.)
                self.assertEqual(native.positions, [])
                self.assertEqual(len(self.markets(native)), 1)

    def test_indirect_pending_partial_remainder_uses_exact_history_for_weighted_protection(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                native, _, position = self.indirect_limit_entry(side)
                self.native_exit(native, position, .2)
                sl, tp = (24992., 25050.) if side == 'buy' else (25008., 24950.)
                response = self.protect(id='indirect-remainder', sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_native_protection(native, sl, tp, 2)
                self.quote(native, 25010. if side == 'buy' else 24990.)
                response = self.protect(id='indirect-remainder-be', operation='breakeven')
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assert_weighted_breakeven(native)
                self.assertAlmostEqual(sum(row.volume for row in native.positions), .8)
                self.assertEqual(len(self.markets(native)), 1)
                self.assertEqual(self.close_card()['result']['status'], 'ok')
                self.assertAlmostEqual(sum(row['volume'] for row in self.closes(native)), .8)

    def test_wrong_ambiguous_or_missing_indirect_opening_history_never_authorizes_protection_or_close(self):
        for mutation in ('wrong', 'ambiguous', 'missing'):
            with self.subTest(mutation=mutation):
                native, captured, position = self.indirect_limit_entry()
                foreign = native.position_from(999000001, dict(symbol='USTEC', type=0, volume=position.volume,
                    price=position.price_open, sl=position.sl, tp=position.tp, comment='manual'))
                native.positions.append(foreign)
                opening = next(row for row in native.history_orders if str(row.ticket) == captured['openingTicket'])
                if mutation == 'wrong':
                    opening.position_id = foreign.identifier
                elif mutation == 'ambiguous':
                    native.history_orders.append(N(**{**vars(opening), 'position_id': foreign.identifier}))
                else:
                    native.history_orders.remove(opening)
                count = len(native.sent)
                response = self.protect(id='unproved-indirect', sl=24992., tp=25050.)
                self.assertNotEqual(response['result']['status'], 'ok', response)
                response = self.close_card()
                self.assertNotEqual(response['result']['status'], 'ok', response)
                self.assertEqual(len(native.sent), count)
                self.assertEqual(len(native.positions), 3)

    def test_missing_part_without_full_exit_proof_never_authorizes_remaining_protection(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.positions.pop()
        count = len(native.sent)
        response = self.protect(sl=24990., tp=25040.)
        self.assertNotEqual(response['result']['status'], 'ok', response)
        self.assertEqual(len(native.sent), count)

    def test_manual_ticket_and_different_completed_entry_are_not_modified(self):
        self.make()
        self.entry()
        native = self.natives[0]
        other = native.position_from(999000001, dict(symbol='USTEC', type=native.ORDER_TYPE_BUY,
            volume=.2, price=24980., sl=24950., tp=25100., comment='manual'))
        native.positions.append(other)
        other_payload = {**self.payload, 'id': 'protection-two', 'cid': 'protection-two'}
        native.group_budgets['cid:protection-two'] = (1., 16.)
        self.quote(native, 25002.)
        self.engine.arm(other_payload)
        self.tick()
        self.assertEqual(self.engine.get('protection-two')['phase'], 'completed')
        untouched = [copy.deepcopy(row) for row in native.positions if row.comment != 'cid:protection-one']
        response = self.protect(sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertEqual([row for row in native.positions if row.comment != 'cid:protection-one'], untouched)
        self.assert_native_protection(native, 24990., 25040., 4)

    def test_full_pending_fill_winning_protection_cancel_is_captured_without_more_market(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry(active=True)
                native = self.natives[0]
                native.fill_on_cancel = True
                sl, tp = (24990., 25040.) if side == 'buy' else (25010., 24960.)
                response = self.protect(sl=sl, tp=tp)
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assertAlmostEqual(response['entry']['accounts']['900000002']['filledVolume'], 1.)
                self.assertEqual(native.orders, [])
                self.assert_native_protection(native, sl, tp, 2)
                self.assertEqual(len(self.markets(native)), 1)
                self.quote(native, 25003. if side == 'buy' else 24997.)
                self.tick()
                self.assertEqual(len(self.markets(native)), 1)
                self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)

    def test_partial_fill_winning_cancel_is_protected_before_only_true_residual_is_acquired(self):
        self.make()
        self.entry(active=True)
        native = self.natives[0]
        native.partial_cancel_volume = .2
        response = self.protect(sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertAlmostEqual(response['entry']['accounts']['900000002']['filledVolume'], .5)
        self.assert_native_protection(native, 24990., 25040., 2)
        self.assertEqual(native.orders, [])
        self.quote(native, 25003.)
        state = self.tick()
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        self.assert_native_protection(native, 24990., 25040., 4)
        self.assertEqual([row['volume'] for row in self.markets(native)], [.3, .3, .2])
        self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)

    def test_unknown_pending_cancel_blocks_protection_and_does_not_retry_or_add_volume(self):
        self.make()
        self.entry(active=True)
        native = self.natives[0]
        native.cancel_unknown = True
        payload = self.command(sl=24990., tp=25040.)
        response = self.engine.protection(payload)
        self.assertEqual(response['result']['status'], 'unknown', response)
        sent = len(native.sent)
        native.cancel_unknown = False
        self.assertEqual(self.engine.protection(payload)['result'], response['result'])
        self.quote(native, 25003.)
        for _ in range(3):
            self.tick()
        self.assertEqual(len(native.sent), sent)
        self.assertEqual(len(self.markets(native)), 1)
        self.assertEqual(len(native.orders), 1)
        self.assertFalse(any(row['action'] == native.TRADE_ACTION_SLTP for row in native.sent[sent:]))

    def test_invalid_zero_widened_risk_nonfinite_and_wrong_side_tp_reject_before_send(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side)
                self.entry()
                invalid = ({'sl': 0.}, {'sl': -1.}, {'sl': float('nan')}, {'sl': float('inf')},
                           {'sl': 24900. if side == 'buy' else 25100.},
                           {'tp': 24900. if side == 'buy' else 25100.}, {'tp': -1.})
                for index, extra in enumerate(invalid):
                    before = copy.deepcopy(self.natives[0].positions)
                    sent = len(self.natives[0].sent)
                    try:
                        result = self.protect(id='invalid-' + str(index), **extra)['result']
                    except ValueError:
                        pass
                    else:
                        self.assertEqual(result['status'], 'rejected', (extra, result))
                    self.assertEqual(self.natives[0].positions, before)
                    self.assertEqual(len(self.natives[0].sent), sent)

    def test_active_successful_widening_that_cannot_fit_remaining_risk_pauses_remainder(self):
        self.make()
        self.entry(active=True)
        # .3 lots can carry a 40-point stop inside the original $16 cap,
        # but acquiring the remaining .7 lots at that stop cannot fit it.
        response = self.protect(sl=24962., tp=25040.)
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assert_native_protection(self.natives[0], 24962., 25040., 1)
        self.assertEqual(self.natives[0].orders, [])
        self.quote(self.natives[0], 25003.)
        for _ in range(3):
            state = self.tick()
        self.assertIn(state['phase'], ('paused', 'stopped'))
        self.assertEqual(len(self.markets(self.natives[0])), 1)
        self.assertAlmostEqual(sum(row.volume for row in self.natives[0].positions), .3)

    def test_exact_same_command_is_durable_idempotent_and_changed_reuse_rejects(self):
        self.make()
        self.entry()
        payload = self.command(sl=24990., tp=25040.)
        first = self.engine.protection(payload)
        self.assertEqual(first['result']['status'], 'ok', first)
        sent = len(self.natives[0].sent)
        self.assertEqual(self.engine.protection(payload)['result'], first['result'])
        self.assertEqual(len(self.natives[0].sent), sent)
        with patch('time.time', return_value=self.now+10.):
            self.assertEqual(self.engine.protection(payload)['result'], first['result'])
        self.assertEqual(len(self.natives[0].sent), sent)
        with self.assertRaises(ValueError):
            self.engine.protection({**payload, 'sl': 24992.})
        self.assertEqual(len(self.natives[0].sent), sent)

    def test_partial_native_rejection_does_not_resume_entry_or_repeat_original_command(self):
        self.make(accounts=2)
        self.entry(active=True)
        self.natives[1].sl_failure = dict(at=self.natives[1].protection_sends+1)
        payload = self.command(sl=24990., tp=25040., mirrorAccounts=[dict(account='900000001')])
        response = self.engine.protection(payload)
        self.assertIn(response['result']['status'], ('unknown', 'rejected'))
        self.assert_native_protection(self.natives[0], 24990., 25040., 1)
        counts = [len(native.sent) for native in self.natives]
        self.natives[1].sl_failure = None
        self.assertEqual(self.engine.protection(payload)['result'], response['result'])
        for native in self.natives:
            self.quote(native, 25003.)
        for _ in range(3):
            self.tick()
        self.assertEqual([len(native.sent) for native in self.natives], counts)
        self.assertEqual([len(self.markets(native)) for native in self.natives], [1, 1])

    def test_partial_same_account_stop_change_never_rolls_back_or_implicitly_retries(self):
        self.make()
        self.entry()
        native = self.natives[0]
        before = [row.sl for row in native.positions]
        native.sl_failure = dict(at=native.protection_sends+2)
        payload = self.command(sl=24990., tp=25040.)
        response = self.engine.protection(payload)
        self.assertIn(response['result']['status'], ('rejected', 'unknown'))
        self.assertEqual(native.positions[0].sl, 24990.)
        self.assertEqual([row.sl for row in native.positions[1:]], before[1:])
        sent = len(native.sent)
        native.sl_failure = None
        self.assertEqual(self.engine.protection(payload)['result'], response['result'])
        self.tick()
        self.assertEqual(len(native.sent), sent)
        self.assertTrue(all(row.sl > 0 for row in native.positions))

    def test_lost_applied_protection_receipt_stays_unknown_after_restart_and_never_resends(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.sl_failure = dict(at=native.protection_sends+1, apply=True, unknown=True)
        payload = self.command(sl=24990., tp=25040.)
        first = self.engine.protection(payload)
        self.assertEqual(first['result']['status'], 'unknown', first)
        sent = len(native.sent)
        native.sl_failure = None
        self.assertEqual(self.engine.protection(payload)['result'], first['result'])
        old = self.primary
        old.chunk_entries.close()
        restarted = Bridge(native, old.config, self.paths[0])
        self.resources.append(restarted)
        self.primary, self.engine = restarted, restarted.chunk_entries
        self.assertEqual(self.engine.protection(payload)['result'], first['result'])
        for _ in range(3):
            self.tick()
        self.assertEqual(len(native.sent), sent)
        self.assertNotEqual(self.state()['phase'], 'running')
        rows = restarted.db.execute('SELECT result FROM chunk_entry_protections WHERE id=?', (payload['id'],)).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])['status'], 'unknown')

    def test_completed_card_close_covers_all_captured_accounts_and_leaves_other_entries_and_manual_tickets(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side, accounts=2)
                self.entry()
                owned = [{row.ticket for row in native.positions} for native in self.natives]
                other_payload = {**self.payload, 'id': 'protection-two', 'cid': 'protection-two'}
                for native in self.natives:
                    native.group_budgets['cid:protection-two'] = (1., 16.)
                    self.quote(native, 25002. if side == 'buy' else 24998.)
                self.engine.arm(other_payload)
                self.tick()
                self.assertEqual(self.engine.get('protection-two')['phase'], 'completed')
                for native in self.natives:
                    native.positions.append(native.position_from(native.account.login*1000+999,
                        dict(symbol='USTEC', type=native.ORDER_TYPE_BUY, volume=.2, price=24980.,
                             sl=24950., tp=25100., comment='manual')))
                untouched = [[copy.deepcopy(row) for row in native.positions if row.ticket not in tickets]
                             for native, tickets in zip(self.natives, owned)]
                response = self.close_card()
                self.assertEqual(response['result']['status'], 'ok', response)
                self.assertEqual(set(response['result']['accountResults']), {'900000002', '900000001'})
                self.assertEqual(response['entry']['phase'], 'stopped')
                self.assertIs(response['entry']['hasOpenPositions'], False)
                self.assertIs(response['entry']['positionsConfirmed'], True)
                self.assertEqual(response['entry']['positionLifecycle'], 'closed')
                for native, tickets, remaining in zip(self.natives, owned, untouched):
                    self.assertEqual(native.positions, remaining)
                    self.assertEqual({row['position'] for row in self.closes(native)}, tickets)
                    self.assertAlmostEqual(sum(row['volume'] for row in self.closes(native)), 1.)
                    self.assertEqual(len(self.markets(native)), 8)
                counts = [len(native.sent) for native in self.natives]
                self.assertEqual(self.close_card()['result'], response['result'])
                with patch('time.time', return_value=self.now+10.):
                    self.assertEqual(self.close_card()['result'], response['result'])
                with self.assertRaises(ValueError):
                    self.close_card(entryId='protection-two')
                for _ in range(3):
                    self.tick()
                self.assertEqual([len(native.sent) for native in self.natives], counts)

    def test_active_card_cancels_all_pending_and_closes_fills_that_win_both_account_cancel_races(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                self.make(side, accounts=2)
                self.entry(active=True)
                for native in self.natives:
                    native.fill_on_cancel = True
                    native.before_close = lambda: self.assertTrue(all(not broker.orders for broker in self.natives))
                response = self.close_card()
                self.assertEqual(response['result']['status'], 'ok', response)
                for native in self.natives:
                    self.assertEqual(native.positions, [])
                    self.assertEqual(native.orders, [])
                    self.assertEqual(len(self.markets(native)), 1)
                    self.assertEqual(len(self.closes(native)), 2)
                    self.assertAlmostEqual(sum(row['volume'] for row in self.closes(native)), 1.)
                    actions = [row['action'] for row in native.sent]
                    first_close = next(index for index, row in enumerate(native.sent) if 'position' in row and row['action'] == native.TRADE_ACTION_DEAL)
                    self.assertIn(native.TRADE_ACTION_REMOVE, actions[:first_close])
                self.assertTrue(self.engine._load(self.payload['id'])[2]['positionsClosed'])

    def test_card_close_uses_only_actual_partial_remainders_and_proves_absent_ticket_closed(self):
        self.make()
        self.entry()
        native = self.natives[0]
        first, absent = native.positions[:2]
        first.volume = .1
        native.positions.remove(absent)
        for row, volume in ((first, .2), (absent, absent.volume)):
            native.history_deals.append(N(symbol=row.symbol, position_id=row.identifier, entry=1, volume=volume, reason=0))
        response = self.close_card()
        self.assertEqual(response['result']['status'], 'ok', response)
        self.assertEqual(native.positions, [])
        closes = self.closes(native)
        self.assertEqual(len(closes), 3)
        self.assertNotIn(absent.ticket, {row['position'] for row in closes})
        self.assertAlmostEqual(sum(row['volume'] for row in closes), .5)
        self.assertEqual(next(row['volume'] for row in closes if row['position'] == first.ticket), .1)

    def test_absent_position_without_closed_broker_proof_cannot_authorize_card_close(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.positions.pop()
        count = len(native.sent)
        response = self.close_card()
        self.assertNotEqual(response['result']['status'], 'ok', response)
        self.assertEqual(self.closes(native), [])
        self.assertEqual(len(native.sent), count)

    def test_card_close_cannot_be_narrowed_to_one_captured_account(self):
        self.make(accounts=2)
        self.entry()
        counts = [len(native.sent) for native in self.natives]
        with self.assertRaises(ValueError):
            self.close_card(account='900000002')
        self.assertEqual([len(native.sent) for native in self.natives], counts)

    def test_unknown_card_cancel_never_closes_or_retries_and_never_resumes_acquisition(self):
        self.make()
        self.entry(active=True)
        native = self.natives[0]
        native.cancel_unknown = True
        command = self.close_command()
        response = self.engine.close_position(command)
        self.assertEqual(response['result']['status'], 'unknown', response)
        self.assertIs(response['entry']['hasOpenPositions'], True)
        self.assertIs(response['entry']['positionsConfirmed'], False)
        count = len(native.sent)
        native.cancel_unknown = False
        self.assertEqual(self.engine.close_position(command)['result'], response['result'])
        self.quote(native, 25003.)
        for _ in range(3):
            self.tick()
        self.assertEqual(len(native.sent), count)
        self.assertEqual(self.closes(native), [])
        self.assertEqual(len(self.markets(native)), 1)
        with self.assertRaises(ValueError):
            self.protect(id='protect-after-close-unknown', sl=24990.)
        self.assertEqual(len(native.sent), count)

    def test_lost_applied_close_receipt_is_durable_mutually_blocks_protection_and_never_repeats_after_restart(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.close_unknown = native.close_apply_then_unknown = True
        command = self.close_command()
        response = self.engine.close_position(command)
        self.assertEqual(response['result']['status'], 'unknown', response)
        self.assertEqual(len(native.positions), 3)
        count = len(native.sent)
        native.close_unknown = False
        self.assertEqual(self.engine.close_position(command)['result'], response['result'])
        with self.assertRaises(ValueError):
            self.protect(sl=24990.)
        old = self.primary
        old.chunk_entries.close()
        restarted = Bridge(native, old.config, self.paths[0])
        self.resources.append(restarted)
        self.primary, self.engine = restarted, restarted.chunk_entries
        self.assertEqual(self.engine.close_position(command)['result'], response['result'])
        for _ in range(3):
            self.tick()
        with self.assertRaises(ValueError):
            self.engine.close_position(self.close_command(id='retry-another-id'))
        with self.assertRaises(ValueError):
            self.protect(sl=24990.)
        self.assertEqual(len(native.sent), count)
        saved = restarted.db.execute('SELECT result FROM chunk_entry_closures WHERE id=?', (command['id'],)).fetchone()
        self.assertEqual(json.loads(saved[0])['status'], 'unknown')

    def test_unknown_protection_blocks_whole_card_close_without_any_native_send(self):
        self.make()
        self.entry()
        native = self.natives[0]
        native.sl_failure = dict(at=native.protection_sends+1, apply=True, unknown=True)
        response = self.protect(sl=24990., tp=25040.)
        self.assertEqual(response['result']['status'], 'unknown')
        count = len(native.sent)
        with self.assertRaises(ValueError):
            self.close_card()
        self.assertEqual(len(native.sent), count)
        self.assertEqual(self.closes(native), [])

    def test_partial_mirror_close_failure_does_not_reopen_or_repeat_either_account(self):
        self.make(accounts=2)
        self.entry()
        self.natives[1].close_unknown = self.natives[1].close_apply_then_unknown = True
        command = self.close_command()
        response = self.engine.close_position(command)
        self.assertEqual(response['result']['status'], 'unknown', response)
        self.assertEqual(self.natives[0].positions, [])
        self.assertEqual(len(self.natives[1].positions), 3)
        self.assertTrue(all(row.sl > 0 for row in self.natives[1].positions))
        counts = [len(native.sent) for native in self.natives]
        self.natives[1].close_unknown = False
        self.assertEqual(self.engine.close_position(command)['result'], response['result'])
        for _ in range(3):
            self.tick()
        with self.assertRaises(ValueError):
            self.engine.close_position(self.close_command(id='fresh-retry'))
        self.assertEqual([len(native.sent) for native in self.natives], counts)
        self.assertEqual([len(self.markets(native)) for native in self.natives], [4, 4])

    def test_http_whole_parent_endpoints_receipts_and_management_metadata_share_exact_ownership(self):
        self.make(accounts=2)
        self.quote(self.natives[1], 25003.)
        self.entry()
        self.natives[0].positions.append(self.natives[0].position_from(999000001,
            dict(symbol='USTEC', type=0, volume=.2, price=24980., sl=24950., tp=25100., comment='manual')))
        self.start_http()
        status, health = self.request_api('GET', '/health')
        self.assertEqual(status, 200)
        self.assertIn('hybrid-position-protection-v1', health['capabilities'])
        status, management = self.request_api('GET', '/management')
        self.assertEqual(status, 200)
        for native in self.natives:
            view = management['accountStates'][str(native.account.login)]
            owned = [row for row in view['orders'] if row.get('entryPlan')]
            self.assertEqual(len(owned), 4)
            average = sum(row.price_open*row.volume for row in native.positions if row.comment == 'cid:protection-one')
            for row in owned:
                plan = row['entryPlan']
                self.assertEqual(plan['id'], self.payload['id'])
                self.assertEqual(set(plan['accounts']), {'900000002', '900000001'})
                self.assertTrue(plan['protectionReady'])
                self.assertAlmostEqual(plan['averagePrice'], average)
            self.assertTrue(all('entryPlan' not in row for row in view['orders'] if row['ticket'] == '999000001'))
        payload = self.command(sl=24990., tp=25040., mirrorAccounts=[dict(account='900000001')])
        status, applied = self.request_api('POST', '/entry-plan-protection', payload)
        self.assertEqual((status, applied['result']['status']), (200, 'ok'), applied)
        counts = [len(native.sent) for native in self.natives]
        status, receipt = self.request_api('GET', '/receipt?id=' + payload['id'])
        self.assertEqual(status, 200)
        self.assertEqual(receipt['result'], applied['result'])
        self.assertEqual(self.request_api('POST', '/entry-plan-protection', payload)[1]['result'], applied['result'])
        self.assertEqual([len(native.sent) for native in self.natives], counts)
        status, closed = self.request_api('POST', '/entry-plan-close', self.close_command())
        self.assertEqual((status, closed['result']['status']), (200, 'ok'), closed)
        self.assertIs(closed['entry']['positionsConfirmed'], True)
        self.assertIs(closed['entry']['hasOpenPositions'], False)
        self.assertEqual(closed['entry']['positionLifecycle'], 'closed')
        self.assertEqual([row.ticket for row in self.natives[0].positions], [999000001])
        self.assertEqual(self.natives[1].positions, [])
        self.assertEqual(self.request_api('GET', '/receipt?id=close-card-1')[1]['result'], closed['result'])

    def test_http_rejects_child_modify_linked_parent_route_and_binds_rejection_ids_without_native_send(self):
        self.make(accounts=2)
        self.entry()
        self.start_http()
        counts = [len(native.sent) for native in self.natives]
        for native, prefix in ((self.natives[0], ''), (self.natives[1], '/accounts/900000001')):
            ticket = str(native.positions[0].ticket)
            status, rejected = self.request_api('POST', prefix + '/modify',
                dict(id='child-modify-' + ticket, ticket=ticket, symbol='USTEC', target='position',
                     sl=24990., expiresAt=self.now*1000+3000))
            self.assertEqual(status, 400, rejected)
            self.assertIn('whole-parent', rejected['error'])
        status, _ = self.request_api('POST', '/accounts/900000001/entry-plan-protection', self.command(account='900000001', sl=24990.))
        self.assertEqual(status, 400)
        for route, command in (('/entry-plan-protection', self.command(symbol='SPX500', sl=24990.)),
                               ('/entry-plan-close', self.close_command(symbol='SPX500'))):
            status, response = self.request_api('POST', route, command)
            self.assertEqual(status, 200)
            self.assertEqual(response['result']['status'], 'rejected')
            self.assertEqual(response['result']['id'], command['id'])
            self.assertEqual(response['result']['entryId'], self.payload['id'])
        self.assertEqual(self.request_api('POST', '/entry-plan-protection', self.command(sl=24990.), authenticated=False)[0], 401)
        self.assertEqual([len(native.sent) for native in self.natives], counts)


if __name__ == '__main__':
    unittest.main()
