"""Unequal ticket counts must preserve exact parent ownership on both accounts."""
import copy
import time
import unittest
from types import SimpleNamespace

import test_entry_independence as fixture
from chunk_entry import IsolatedEntries


class AsymmetricParentProtectionTests(unittest.TestCase):
    def model(self, symbol, side, single_account):
        case = fixture.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f, root = case.model()
        f.payload.update(symbol=symbol, side=side, chunkPercent=50, chunkVolume=.5)
        if side == 'sell':
            f.payload.update(sl=25016.)
        root.config['symbols'] = [symbol]
        for native, bridge in zip(f.natives, f.bridges):
            bridge.config['symbols'] = [symbol]
            native.side = side
            native.tick.bid, native.tick.ask = ((25001.8, 25002.) if side == 'buy' else (24998., 24998.2))
            native.group_budgets = {}
            if str(native.account.login) == single_account:
                native.tick.bid, native.tick.ask = ((25007.8,25008.) if side == 'buy' else (24992.,24992.2))
        f.engine.arm(f.payload)
        single = next(n for n in f.natives if str(n.account.login) == single_account)
        other = next(n for n in f.natives if str(n.account.login) != single_account)
        fixture.support.IsolatedEngineTests.until(self, f,
            lambda state: len(single.orders) == 1 and abs(sum(p.volume for p in other.positions)-1.) < 1e-8)
        # One account fills its full support order while its peer fills two
        # market pieces. This is broker execution, not a guessed ticket merge.
        bridge = next(b for b in f.bridges if str(b.config['account']) == single_account)
        with bridge.lock:
            order = single.orders.pop()
            self.assertEqual(order.volume_current,1.)
            request = dict(symbol=symbol,type=single.ORDER_TYPE_BUY if side == 'buy' else single.ORDER_TYPE_SELL,
                           volume=order.volume_current,price=order.price_open,sl=order.sl,tp=order.tp,comment=order.comment)
            single.positions.append(single.position_from(order.ticket,request))
            single.history_orders.append(SimpleNamespace(ticket=order.ticket,symbol=symbol,position_id=order.ticket,state=4))
            single.history_deals.append(SimpleNamespace(symbol=symbol,position_id=order.ticket,entry=0,volume=1.,reason=0))
        fixture.support.IsolatedEngineTests.until(self, f, lambda state: state['phase'] == 'completed')
        counts = {str(n.account.login): len(n.positions) for n in f.natives}
        self.assertEqual(counts[single_account], 1)
        self.assertEqual(counts[next(a for a in counts if a != single_account)], 2)
        self.assertTrue(all(abs(sum(p.volume for p in n.positions)-1.) < 1e-8 for n in f.natives))
        for native in f.natives:
            native.tick.bid,native.tick.ask = ((25020.,25020.2) if side == 'buy' else (24980.,24980.2))
        return f, root

    def command(self, f, selected, identifier, **changes):
        return dict(id=identifier, entryId=f.payload['id'], symbol=f.payload['symbol'],
                    account=selected, mirrorAccounts=[dict(account=a) for a in ('900000002','900000001') if a != selected],
                    operation='modify', expiresAt=time.time()*1000+3000, **changes)

    def test_sl_tp_and_breakeven_reach_every_exact_part_with_two_positions_against_one(self):
        for symbol in ('USTEC', 'SPX500'):
            for side in ('buy', 'sell'):
                for single in ('900000002', '900000001'):
                    with self.subTest(symbol=symbol, side=side, one_position_account=single):
                        f, root = self.model(symbol, side, single)
                        original = {str(n.account.login): [(p.ticket,p.volume,p.price_open) for p in n.positions] for n in f.natives}
                        # A UI management read deliberately yields after 20 ms.
                        # Wait for that read to finish before asserting its data;
                        # an honest 'checking' response is not an empty position.
                        snapshots = {}
                        def management_ready(_state):
                            views = root.management()['accountStates']
                            if set(views)!=set(original) or not all(view.get('connected') is True and view.get('evidenceReady') is True
                                       for view in views.values()):return False
                            snapshots.update(views)
                            return True
                        fixture.support.IsolatedEngineTests.until(self, f, management_ready)
                        for account, view in snapshots.items():
                            own = [r for r in view['orders'] if r['__isPosition']]
                            self.assertTrue(own)
                            self.assertTrue(all(r['entryPlan']['id'] == f.payload['id']
                                                and r['entryPlan']['protectionReady'] is True
                                                and set(r['entryPlan']['accounts']) == {'900000002','900000001'} for r in own))
                        sl, tp = ((24990.,25040.) if side == 'buy' else (25010.,24960.))
                        # Half-step inputs from weighted entry prices must be
                        # normalized by the native planner before either send.
                        fraction = -.05 if side == 'buy' else .05
                        command = self.command(f, single, 'asymmetric-sl-tp', sl=sl+fraction, tp=tp+fraction)
                        response = f.engine.protection(command)['result']
                        self.assertEqual(response['status'], 'ok', response)
                        self.assertEqual(set(response['accountResults']), {'900000002','900000001'})
                        for native in f.natives:
                            account = str(native.account.login)
                            self.assertEqual(len(response['accountResults'][account]['positions']), len(native.positions))
                            self.assertTrue(all(p.sl == sl and p.tp == tp for p in native.positions))
                            self.assertEqual([(p.ticket,p.volume,p.price_open) for p in native.positions], original[account])
                        before = [len(n.sent) for n in f.natives]
                        self.assertEqual(f.engine.protection(command)['result'], response)
                        self.assertEqual([len(n.sent) for n in f.natives], before, 'A repeated command must not resend')
                        command = self.command(f, next(a for a in original if a != single), 'asymmetric-breakeven')
                        command['operation'] = 'breakeven'
                        response = f.engine.protection(command)['result']
                        self.assertEqual(response['status'], 'ok', response)
                        for native in f.natives:
                            account = str(native.account.login)
                            stop = response['accountResults'][account]['sl']
                            self.assertTrue(all(p.sl == stop and p.tp == tp for p in native.positions))
                            self.assertEqual([(p.ticket,p.volume,p.price_open) for p in native.positions], original[account])

    def test_same_symbol_unowned_position_is_not_included_in_asymmetric_parent(self):
        f, _ = self.model('USTEC', 'buy', '900000001')
        unrelated = copy.deepcopy(f.natives[0].positions[0])
        unrelated.ticket += 9000000
        unrelated.identifier = unrelated.ticket
        unrelated.comment = 'another-entry'
        f.natives[0].positions.append(unrelated)
        response = f.engine.protection(self.command(f, '900000002', 'asymmetric-owned-only', sl=24990.))['result']
        self.assertEqual(response['status'], 'ok', response)
        self.assertNotIn(str(unrelated.ticket), {p['ticket'] for p in response['accountResults']['900000002']['positions']})
        self.assertNotEqual(unrelated.sl, 24990.)
