"""Real coordinator + real Bridge on an in-process MT5 simulation only."""
import copy
import datetime
import pathlib
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_hybrid_bridge import CommonStopMT5
from chunk_entry import ChunkEntries
from mt5_bridge import Bridge


class LifecycleMT5(CommonStopMT5):
    """Persist native pending, hedging positions, cancel history and SL sends."""
    def __init__(self, now):
        super().__init__()
        self.positions, self.orders, self.history_orders, self.history_deals = [], [], [], []
        self.tick = N(bid=25001.8, ask=25002., time=now)
        self.next_ticket = 100
        self.fill_on_cancel = False
        self.exposure = []

    def observe(self, action):
        volume = sum(row.volume for row in self.positions) + sum(row.volume_current for row in self.orders)
        risk = sum(max(0., -self.order_calc_profit(row.type, row.symbol, row.volume, row.price_open, row.sl))
                   for row in self.positions)
        risk += sum((row.price_open-row.sl if row.type==self.ORDER_TYPE_BUY_LIMIT else row.sl-row.price_open)
                    * row.volume_current for row in self.orders)
        self.exposure.append(dict(action=action, volume=volume, risk=risk))
        if volume > 1. + 1e-8 or risk > 16. + 1e-8:
            raise AssertionError('Native simulated transition exceeded the reserved target or risk')

    def position_from(self, ticket, request, price=None):
        return N(ticket=ticket, identifier=ticket, symbol=request['symbol'], type=request['type'],
                 volume=request['volume'], price_open=request['price'] if price is None else price,
                 sl=request['sl'], tp=request['tp'], price_current=request['price'], profit=0.,
                 comment=request.get('comment', 'cid:hybrid-integration'))

    def order_send(self, request):
        self.sent.append(copy.deepcopy(request))
        action = request['action']
        if action == self.TRADE_ACTION_PENDING:
            self.next_ticket += 1
            ticket = self.next_ticket
            self.orders.append(N(ticket=ticket, identifier=ticket, symbol=request['symbol'], type=request['type'],
                                 volume_current=request['volume'], price_open=request['price'], sl=request['sl'], tp=request['tp'],
                                 position_id=0, comment=request.get('comment', 'cid:hybrid-integration')))
            result = N(retcode=self.TRADE_RETCODE_PLACED, order=ticket, volume=0., price=0., comment='placed')
        elif action == self.TRADE_ACTION_DEAL:
            self.next_ticket += 1
            ticket = self.next_ticket
            self.positions.append(self.position_from(ticket, request))
            self.history_orders.append(N(ticket=ticket, symbol=request['symbol'], position_id=ticket, state=4))
            self.history_deals.append(N(symbol=request['symbol'], position_id=ticket, entry=0, volume=request['volume'], reason=0))
            result = N(retcode=self.TRADE_RETCODE_DONE, order=ticket, volume=request['volume'], price=request['price'], comment='filled')
            # Future rounds see new prices. The current deal matches the fresh
            # native guard price, so this tests weighting without slippage.
            self.tick.bid = round(self.tick.bid+.2, 8)
            self.tick.ask = round(self.tick.ask+.2, 8)
        elif action == self.TRADE_ACTION_REMOVE:
            ticket = request['order']
            order = next(row for row in self.orders if row.ticket == ticket)
            self.orders.remove(order)
            if self.fill_on_cancel:
                opening = dict(symbol=order.symbol, type=self.ORDER_TYPE_BUY if order.type==self.ORDER_TYPE_BUY_LIMIT else self.ORDER_TYPE_SELL,
                               volume=order.volume_current, price=order.price_open, sl=order.sl, tp=order.tp, comment=order.comment)
                self.positions.append(self.position_from(ticket, opening))
                self.history_orders.append(N(ticket=ticket, symbol=order.symbol, position_id=ticket, state=4))
                self.history_deals.append(N(symbol=order.symbol, position_id=ticket, entry=0, volume=order.volume_current, reason=0))
                result = N(retcode=10035, order=ticket, volume=0., price=0., comment='Already filled before cancellation')
            else:
                self.history_orders.append(N(ticket=ticket, symbol=order.symbol, position_id=0, state=2))
                result = N(retcode=self.TRADE_RETCODE_DONE, order=ticket, volume=0., price=0., comment='cancelled')
        elif action == self.TRADE_ACTION_SLTP:
            position = next(row for row in self.positions if row.ticket == request['position'])
            position.sl, position.tp = request['sl'], request['tp']
            result = N(retcode=self.TRADE_RETCODE_DONE, order=position.ticket, volume=0., price=0., comment='modified')
        else:
            raise AssertionError('Unexpected simulated MT5 action: ' + repr(request))
        self.observe(action)
        return result


class HybridIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.datetime(2026,10,21,14,tzinfo=datetime.timezone.utc).timestamp()
        clock = patch('time.time', return_value=self.now)
        clock.start(); self.addCleanup(clock.stop)
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.mt5 = LifecycleMT5(self.now)
        self.bridge = Bridge(self.mt5, dict(account=900000002,server='Example-MT5',symbols=['USTEC'],tradingEnabled=True),
                             pathlib.Path(self.temp.name)/'bridge.sqlite')
        self.addCleanup(self.bridge.db.close)
        self.engine = ChunkEntries(self.bridge); self.addCleanup(self.engine.close)
        due = datetime.datetime(2026,10,21,19,tzinfo=datetime.timezone.utc).timestamp()*1000
        self.payload = dict(id='native-hybrid', symbol='USTEC', side='buy', trigger=25000., level=25000.,
                            sl=24984., tp=0., riskUsd=16., volume=1., chunkVolume=.3, chunkPercent=30.,
                            entryMode='side-zone', executionPolicy='hybrid-market-limit-v1', zoneRadius=6.,
                            zoneMultiplier=1, tickSize=.1, stopOffsetPts=160., stopMode='entry', maxGap=6.,
                            expiresAt=self.now*1000+3000, closeAtUtc=due, closeTimeZone='America/New_York',
                            closeLocalDate='2026-10-21',closeLocalTime='15:00',cancelPendingAtDeadline=True)

    def run_entry(self):
        self.engine.arm(self.payload)
        for _ in range(8):
            self.engine.pending_poll_at.clear()
            self.engine.tick()
            state = self.engine.list()[0]
            if state['phase'] in ('completed', 'stopped', 'unknown'):
                return state
        return state

    def assert_protected_complete(self, state):
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        self.assertAlmostEqual(sum(row.volume for row in self.mt5.positions), 1.)
        self.assertEqual(self.mt5.orders, [])
        average = sum(row.volume*row.price_open for row in self.mt5.positions)
        self.assertAlmostEqual(state['accounts']['900000002']['averagePrice'], average)
        self.assertEqual(len({row.sl for row in self.mt5.positions}), 1)
        self.assertLessEqual(state['accounts']['900000002']['riskUsed'], 16.)
        self.assertTrue(all(row['volume'] <= 1.+1e-8 and row['risk'] <= 16.+1e-8 for row in self.mt5.exposure))
        self.assertTrue(all(rule['state']=='armed' for rule in self.bridge.time_rules()))

    def test_real_bridge_native_orders_deadlines_and_shared_stop_complete_varying_market_parts(self):
        state = self.run_entry()
        self.assert_protected_complete(state)
        markets = [row for row in self.mt5.sent if row['action']==self.mt5.TRADE_ACTION_DEAL]
        self.assertEqual([row['volume'] for row in markets], [.3,.3,.3,.1])
        self.assertGreater(len({row['price'] for row in markets}), 1)
        self.assertTrue(any(row['action']==self.mt5.TRADE_ACTION_SLTP for row in self.mt5.sent))
        self.assertEqual(len(self.bridge.time_rules()), 7)

    def test_native_limit_fill_winning_cancel_race_is_included_before_common_stop_and_completion(self):
        self.mt5.fill_on_cancel = True
        state = self.run_entry()
        self.assert_protected_complete(state)
        markets = [row for row in self.mt5.sent if row['action']==self.mt5.TRADE_ACTION_DEAL]
        self.assertEqual([row['volume'] for row in markets], [.3])
        self.assertEqual(len(self.mt5.positions), 2)
        self.assertTrue(any(row['action']==self.mt5.TRADE_ACTION_REMOVE for row in self.mt5.sent))


if __name__ == '__main__':
    unittest.main()
