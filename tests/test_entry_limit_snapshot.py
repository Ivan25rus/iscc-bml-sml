"""Native return-limit reads and cancellation proof using simulated MT5."""
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_hybrid_protection as fixture


class EntryLimitSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture.HybridProtectionTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.make(accounts=2)
        self.bridge, self.native = self.fixture.bridges[1], self.fixture.natives[1]
        self.expected = dict(side='buy', price=25001., sl=24985., tp=0., volume=.75)

    def test_no_send_cancel_reuses_exact_full_fill_snapshot(self):
        request = dict(id='already-filled', ticket='81', symbol='USTEC', expiresAt=self.fixture.now*1000+3000)
        watch = [dict(account='900000001', symbol='USTEC', ticket='81')]
        opening = dict(symbol='USTEC', type=self.native.ORDER_TYPE_BUY, price=25001., sl=24985., tp=0., volume=.75)
        self.native.positions.append(self.native.position_from(81, opening))
        self.native.history_orders.append(SimpleNamespace(ticket=81, symbol='USTEC', position_id=81, state=4))
        started = time.time()*1000
        with patch.object(self.bridge, '_orders', wraps=self.bridge._orders) as reads:
            reply = self.bridge.entry_cancel_limit(request, self.expected, watch)
        reads.assert_called_once()
        self.assertTrue(reply['receipt']['dispatchNotSent'])
        self.assertFalse(self.native.sent)
        self.assertEqual(reply['view']['positions'], reply['proof']['orders'])
        self.assertEqual(reply['proof']['openingStates']['81'], dict(phase='opened', ticket='81'))
        self.assertLessEqual(started, reply['proof']['checkedAt'])

    def test_absent_limit_reread_is_bounded_and_does_not_prove_cancellation(self):
        with patch.object(self.bridge, '_orders', wraps=self.bridge._orders) as reads:
            view = self.bridge.entry_limit_state('81', 'USTEC')
        self.assertEqual(reads.call_count, 2)
        self.assertFalse(view['pending'])
        self.assertFalse(view['positions'])
        self.assertEqual(view['terminal']['phase'], 'unknown')
        self.assertFalse(self.native.sent)

    def test_sent_cancel_still_confirms_with_a_new_inventory_read(self):
        pending = dict(symbol='USTEC', type=self.native.ORDER_TYPE_BUY_LIMIT, price=25001., sl=24985., tp=0., volume=.75)
        ticket = self.native.order_send(dict(pending, action=self.native.TRADE_ACTION_PENDING)).order
        request = dict(id='confirm-cancel', ticket=str(ticket), symbol='USTEC', expiresAt=self.fixture.now*1000+3000)
        with patch.object(self.bridge, '_orders', wraps=self.bridge._orders) as reads:
            reply = self.bridge.entry_cancel_limit(request, self.expected)
        self.assertEqual(reads.call_count, 2)
        self.assertEqual(reply['receipt']['status'], 'ok')
        self.assertEqual(reply['view']['terminal']['phase'], 'cancelled')
        self.assertFalse(reply['proof']['orders'])
        self.assertEqual([row['order'] for row in self.native.sent if row['action'] == self.native.TRADE_ACTION_REMOVE], [ticket])
