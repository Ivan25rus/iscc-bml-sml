"""Independent account policy simulations: no network, terminal or real trades."""
import copy
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_hybrid_entry import hybrid_fixture
from chunk_entry import ChunkEntries
from mt5_bridge import Bridge


class IndependentEntryTests(unittest.TestCase):
    def setUp(self):
        self.primary, self.follower, self.payload = hybrid_fixture()
        self.payload['accountExecution'] = 'independent-v2'
        # The older fixture always aggregates a rejection as unknown. Use the
        # production aggregator, which proves an all-rejected single leg final.
        group = self.primary.group_execute
        def execute(*args, **kwargs):
            response = group(*args, **kwargs)
            response['result'] = Bridge.group_result(self.primary, response['result']['accountResults'])
            return response
        self.primary.group_execute = execute
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)

    def tick(self):
        self.engine.pending_poll_at.clear()
        self.engine.tick()
        return self.engine.list()[0]

    def arm(self):
        self.engine.arm(self.payload)
        return self.tick()

    def arm_resting(self):
        for bridge in (self.primary, self.follower):
            original = bridge._fill
            def fill(request, bridge=bridge, original=original):
                result = original(request)
                if request['type'] == 'market':
                    bridge.price = dict(ask=110., bid=109.8)
                return result
            bridge._fill = fill
        return self.arm()

    def assert_reserved(self, state, account, filled, pending):
        row = state['accounts'][account]
        self.assertAlmostEqual(row['filledVolume'], filled)
        self.assertAlmostEqual(row['pendingVolume'], pending)
        self.assertAlmostEqual(row['freeVolume'], 1-filled-pending)

    def reject_limit(self, bridge, reason='Limit requires a fresh quote beyond its captured outer edge and broker distance'):
        original = bridge._fill
        def fill(request):
            if request['type'] == 'limit':
                return dict(status='rejected', reason=reason)
            return original(request)
        bridge._fill = fill

    def test_support_first_and_reserved_market_part_on_both_accounts(self):
        state = self.arm_resting()
        self.assertEqual(state['phase'], 'running')
        for bridge in (self.primary, self.follower):
            self.assertEqual([r['type'] for r in bridge.sent], ['limit', 'market'])
            self.assertEqual([r['volume'] for r in bridge.sent], [.7, .3])
            self.assert_reserved(state, str(bridge.login.login), .3, .7)
        for _ in range(5):
            self.tick()
        self.assertEqual(len(self.primary.sent), 2)
        self.assertEqual(len(self.follower.sent), 2)
        self.assertFalse(self.primary.cancelled or self.follower.cancelled)

    def test_definite_price_rejection_leaves_other_account_and_market_intact(self):
        self.reject_limit(self.follower)
        state = self.arm()
        self.assertEqual(state['phase'], 'running')
        self.assert_reserved(state, '900000002', .3, 0.)
        self.assert_reserved(state, '900000001', .3, 0.)
        for _ in range(4):
            state = self.tick()
        self.assert_reserved(state, '900000001', 1., 0.)
        self.assertEqual(sum(c['state']=='rejected' for c in state['children']), 3)
        self.assertEqual(len(self.primary.cancelled), 3)

    def test_changed_price_retry_never_repeats_reserved_volume(self):
        original = self.follower._fill
        self.reject_limit(self.follower)
        self.arm()
        self.follower._fill = original
        self.follower.price = dict(ask=110., bid=109.8)
        with patch('chunk_entry.time.time', return_value=__import__('time').time()+1):
            state = self.tick()
        self.assertEqual(state['phase'], 'running')
        self.assertEqual([r['type'] for r in self.follower.sent], ['market', 'limit'])
        self.assertEqual(self.follower.sent[-1]['price'], 106.)
        self.assertEqual(self.follower.sent[-1]['volume'], .7)
        self.assert_reserved(state, '900000001', .3, .7)

    def test_quote_moves_out_after_market_keeps_already_accepted_support(self):
        original = self.primary._fill
        def fill(request):
            result = original(request)
            if request['type'] == 'market':
                self.primary.price = dict(ask=110., bid=109.8)
            return result
        self.primary._fill = fill
        state = self.arm()
        self.assertEqual(self.primary.sent[0]['type'], 'limit')
        self.assertEqual(self.primary.sent[0]['price'], 101.8)
        self.assert_reserved(state, '900000002', .3, .7)
        self.tick()
        self.assertFalse(self.primary.cancelled)

    def test_buy_outside_on_click_places_only_return_limit_at_inclusive_edge(self):
        self.primary.price = dict(ask=110., bid=109.8)
        state = self.arm()
        self.assertEqual([r['type'] for r in self.primary.sent], ['limit'])
        self.assertEqual(self.primary.sent[0]['price'], 106.)
        self.assert_reserved(state, '900000002', 0., 1.)

    def test_sell_return_limit_at_lower_edge(self):
        self.engine.close()
        self.primary, self.follower, self.payload = hybrid_fixture(side='sell')
        self.payload['accountExecution'] = 'independent-v2'
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.primary.price = dict(bid=90., ask=90.2)
        state = self.arm()
        self.assertEqual([r['type'] for r in self.primary.sent], ['limit'])
        self.assertEqual(self.primary.sent[0]['price'], 94.)
        self.assert_reserved(state, '900000002', 0., 1.)

    def test_full_fill_finishes_common_stop_on_both_accounts(self):
        self.arm_resting()
        self.primary.fill_limit(.7)
        self.follower.fill_limit(.7)
        state = self.tick()
        self.assertEqual(state['phase'], 'completed')
        for bridge in (self.primary, self.follower):
            self.assertTrue(bridge.common_calls)
            self.assert_reserved(state, str(bridge.login.login), 1., 0.)
            self.assertEqual(len({r['sl'] for r in bridge.rows}), 1)

    def test_partial_pending_fill_cancel_proof_and_residual_do_not_overfill(self):
        self.arm_resting()
        self.primary.fill_limit(.2)
        state = self.tick()
        self.assertEqual(state['phase'], 'running')
        self.assertEqual(len(self.primary.cancelled), 1)
        self.assert_reserved(state, '900000002', .5, .5)
        self.assert_reserved(state, '900000001', .3, .7)

    def test_unknown_cancel_locks_residual_without_repeat(self):
        self.arm_resting()
        self.primary.fill_limit(.2)
        self.primary.cancel_unknown = True
        state = self.tick()
        self.assertIn(state['phase'], ('unknown', 'stopped', 'paused'))
        self.assertEqual(len(self.primary.sent), 2)
        self.tick()
        self.assertEqual(len(self.primary.sent), 2)

    def test_favorable_gap_fill_does_not_stop_remaining_entry(self):
        self.arm_resting()
        self.primary.fill_limit(.7, price=99.)
        state = self.tick()
        self.assertEqual(state['phase'], 'running')
        self.assert_reserved(state, '900000002', 1., 0.)
        self.assert_reserved(state, '900000001', .3, .7)
        self.assertFalse(any(row.get('fillGuardViolated') for row in state['children']))

    def test_inside_zone_support_reduces_75_50_25_percent_before_each_market_part(self):
        self.payload.update(chunkPercent=25., chunkVolume=.25)
        for bridge in (self.primary, self.follower):
            bridge.spec.update(quantityStep=.01, minQty=.01)
        self.arm()
        for _ in range(4):
            state = self.tick()
        self.assertEqual(state['phase'], 'completed', state.get('reason'))
        for bridge in (self.primary, self.follower):
            self.assertEqual([(r['type'], r['volume']) for r in bridge.sent],
                [('limit', .75), ('market', .25), ('limit', .5), ('market', .25),
                 ('limit', .25), ('market', .25), ('market', .25)])
            self.assertEqual(len(bridge.cancelled), 3)
            self.assert_reserved(state, str(bridge.login.login), 1., 0.)

    def test_hard_rejection_is_not_retried(self):
        self.reject_limit(self.follower, 'Insufficient margin')
        state = self.arm()
        self.assertEqual(state['phase'], 'stopped')
        count = len(state['children'])
        self.tick()
        self.assertEqual(len(self.engine.list()[0]['children']), count)

    def test_full_market_percent_has_no_limit(self):
        self.payload.update(chunkPercent=100., chunkVolume=1.)
        state = self.arm()
        self.assertEqual(state['phase'], 'completed')
        for bridge in (self.primary, self.follower):
            self.assertEqual([r['type'] for r in bridge.sent], ['market'])


if __name__ == '__main__':
    unittest.main()
