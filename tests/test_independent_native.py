"""Real Bridge/coordinator with native MT5 fakes; never connects to a terminal."""
import copy
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_bridge as guard
import test_hybrid_integration as lifecycle
import test_hybrid_protection as protection


class IndependentNativeTests(unittest.TestCase):
    def fixture(self, kind='guard'):
        f = guard.HybridBridgeTests() if kind == 'guard' else lifecycle.HybridIntegrationTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        return f

    def linked(self, side='buy'):
        f = protection.HybridProtectionTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.make(side, 2)
        f.payload['accountExecution'] = 'independent-v2'
        return f

    def test_linked_native_limit_refusal_does_not_block_either_market_ladder(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                f = self.linked(side)
                f.natives[1].info.trade_stops_level, f.natives[1].info.point = 100, .1
                f.engine.arm(f.payload)
                for _ in range(8):
                    state = f.tick()
                    if state['phase'] in ('stopped', 'unknown', 'completed'):
                        break
                self.assertEqual(state['phase'], 'completed', state.get('reason'))
                for native in f.natives:
                    self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
                    self.assertFalse(native.orders)
                    self.assertTrue(all(group['volume'] <= 1.+1e-8 and group['risk'] <= 16.+1e-8
                                        for row in native.exposure for group in row['groups'].values()))
                rejected = [r for r in state['children'] if r['state'] == 'rejected']
                self.assertTrue(rejected)
                saved = f.engine._load(f.payload['id'])[2]
                self.assertTrue(any(c.get('result', {}).get('entryDiagnostics') for c in saved['children'] if c['state']=='rejected'))

    def test_active_independent_return_limits_have_readable_management(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                f = self.linked(side)
                for native in f.natives:
                    native.tick.bid, native.tick.ask = (25010., 25010.2) if side == 'buy' else (24990., 24990.2)
                f.engine.arm(f.payload)
                state = f.tick()
                self.assertEqual(state['phase'], 'running', state.get('reason'))
                before = copy.deepcopy([native.sent for native in f.natives])
                for _ in range(2):
                    view = f.primary.management()
                    self.assertEqual(len(view['entryPlans']), 1)
                    plan = view['entryPlans'][0]
                    for account, details in plan['accounts'].items():
                        self.assertEqual(details['filledVolume'], 0)
                        self.assertEqual(details['pendingVolume'], details['targetVolume'])
                        self.assertEqual(details['freeVolume'], 0)
                        orders = view['accountStates'][account]['orders']
                        self.assertEqual(len(orders), 1)
                        self.assertFalse(orders[0]['__isPosition'])
                        self.assertEqual(orders[0]['entryPlan']['id'], plan['id'])
                    primary = view['accountStates']['900000002']['orders'][0]
                    self.assertEqual(primary['mirrorAccounts'], [])
                    self.assertNotIn('mirrorAccount', primary)
                self.assertEqual([native.sent for native in f.natives], before)

    def test_independent_ladder_positions_have_readable_management(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                f = self.linked(side)
                f.engine.arm(f.payload)
                for _ in range(8):
                    state = f.tick()
                    before = copy.deepcopy([native.sent for native in f.natives])
                    view = f.primary.management()
                    self.assertTrue(all(row['connected'] for row in view['accountStates'].values()))
                    self.assertEqual([native.sent for native in f.natives], before)
                    if state['phase'] in ('stopped', 'unknown', 'completed'):
                        break
                self.assertEqual(state['phase'], 'completed', state.get('reason'))

    def test_cancel_race_fill_reduces_residual_before_next_market(self):
        f = self.linked()
        f.natives[0].fill_on_cancel = True
        f.engine.arm(f.payload)
        for _ in range(8):
            state = f.tick()
            if state['phase'] in ('stopped', 'unknown', 'completed'):
                break
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        self.assertEqual(len(f.markets(f.natives[0])), 1)
        for native in f.natives:
            self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
            self.assertFalse(native.orders)

    def support(self, f, side='buy'):
        payload = f.support(side, supportReprice=True)
        payload['entryGuard']['limitPrice'] = 25006. if side == 'buy' else 24994.
        payload['entryGuard']['anchor'] = payload['entryGuard']['limitPrice']
        return payload

    def test_buy_return_limit_at_edge_and_broker_distance_inward(self):
        for ask, distance, expected in [(25010., 0, 25006.), (25006.2, 5, 25005.7)]:
            with self.subTest(ask=ask):
                f = self.fixture()
                f.mt5.tick.ask, f.mt5.tick.bid = ask, ask-.2
                f.mt5.info.trade_stops_level = distance
                f.mt5.info.point = .1
                result = f.bridge.execute('place', self.support(f))['result']
                self.assertEqual(result['status'], 'ok', result)
                self.assertAlmostEqual(f.mt5.sent[-1]['price'], expected)

    def test_sell_return_limit_mirrors_buy(self):
        f = self.fixture()
        f.mt5.tick.bid, f.mt5.tick.ask = 24990., 24990.2
        result = f.bridge.execute('place', self.support(f, 'sell'))['result']
        self.assertEqual(result['status'], 'ok', result)
        self.assertEqual(f.mt5.sent[-1]['price'], 24994.)

    def test_inclusive_edge_rounds_inward_to_broker_tick(self):
        f = self.fixture()
        f.mt5.tick.bid, f.mt5.tick.ask = 25010., 25010.2
        payload = self.support(f)
        payload['entryGuard']['priceCeiling'] = 25005.95
        result = f.bridge.execute('place', payload)['result']
        self.assertEqual(result['status'], 'ok', result)
        self.assertEqual(f.mt5.sent[-1]['price'], 25005.9)

    def test_impossible_distance_rejects_without_native_send_and_has_quote_details(self):
        f = self.fixture()
        f.mt5.tick.bid, f.mt5.tick.ask = 25000., 25000.2
        f.mt5.info.trade_stops_level, f.mt5.info.point = 100, .1
        result = f.bridge.execute('place', self.support(f))['result']
        self.assertEqual(result['status'], 'rejected', result)
        detail = result['entryDiagnostics']
        self.assertEqual((detail['bid'], detail['ask'], detail['minimumDistance']), (25000., 25000.2, 10.))
        self.assertIs(detail['mutationStarted'], False)
        self.assertEqual(f.mt5.sent, [])

    def test_final_after_intent_rejection_is_durable_with_quote_details(self):
        f = self.fixture()
        original = f.bridge.guard_chunk_request
        calls = []
        def check(payload, request):
            calls.append(True)
            if len(calls) == 2:
                f.mt5.tick.bid, f.mt5.tick.ask = 24999.8, 25000.
            return original(payload, request)
        f.bridge.guard_chunk_request = check
        payload = self.support(f)
        result = f.bridge.execute('place', payload)['result']
        self.assertEqual(result['status'], 'rejected', result)
        self.assertEqual(result['entryDiagnostics']['ask'], 25000.)
        saved = json.loads(f.bridge.db.execute('SELECT result FROM requests WHERE id=?', (payload['id'],)).fetchone()[0])
        self.assertEqual(saved['entryDiagnostics'], result['entryDiagnostics'])
        self.assertEqual(f.mt5.sent, [])

    def test_real_coordinator_keeps_native_return_reservation_without_churn(self):
        f = self.fixture('lifecycle')
        f.payload['accountExecution'] = 'independent-v2'
        original = f.mt5.order_send
        def move_outside(request):
            result = original(request)
            if request['action'] == f.mt5.TRADE_ACTION_DEAL:
                f.mt5.tick.bid, f.mt5.tick.ask = 25010., 25010.2
            return result
        f.mt5.order_send = move_outside
        f.engine.arm(f.payload)
        f.engine.tick()
        state = f.engine.list()[0]
        self.assertEqual(state['phase'], 'running', state.get('reason'))
        self.assertEqual(len(f.mt5.positions), 1)
        self.assertEqual(len(f.mt5.orders), 1)
        self.assertAlmostEqual(f.mt5.orders[0].volume_current, .7)
        self.assertAlmostEqual(state['accounts']['900000002']['pendingVolume'], .7)
        count = len(f.mt5.sent)
        for _ in range(6):
            f.engine.pending_poll_at.clear()
            f.engine.tick()
        self.assertEqual(len(f.mt5.sent), count)
        self.assertTrue(all(row['volume'] <= 1.+1e-8 and row['risk'] <= 16.+1e-8 for row in f.mt5.exposure))

    def test_lost_native_limit_reply_never_submits_market(self):
        f = self.fixture('lifecycle')
        f.payload['accountExecution'] = 'independent-v2'
        original = f.mt5.order_send
        def lost(request):
            original(request)
            return None
        f.mt5.order_send = lost
        f.engine.arm(f.payload)
        for _ in range(5):
            f.engine.tick()
        self.assertEqual(len(f.mt5.sent), 1)
        self.assertEqual(len(f.mt5.orders), 1)
        self.assertEqual(f.mt5.positions, [])

    def test_lost_native_market_reply_is_not_replayed(self):
        f = self.fixture('lifecycle')
        f.payload['accountExecution'] = 'independent-v2'
        original = f.mt5.order_send
        def lost(request):
            result = original(request)
            return None if request['action'] == f.mt5.TRADE_ACTION_DEAL else result
        f.mt5.order_send = lost
        f.engine.arm(f.payload)
        for _ in range(5):
            f.engine.tick()
        markets = [row for row in f.mt5.sent if row['action'] == f.mt5.TRADE_ACTION_DEAL]
        self.assertEqual(len(markets), 1)
        self.assertAlmostEqual(sum(row.volume for row in f.mt5.positions), .3)


if __name__ == '__main__':
    unittest.main()
