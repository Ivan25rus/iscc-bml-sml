"""Independent planning ledgers, real state machine, synthetic MT5 only."""
import copy
import json
import pathlib
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as fixture
from chunk_entry import IsolatedEntries, ChunkEntries, SingleAccountLane, account_entry_sizes
from mt5_bridge import CoordinatorBridge,worker_result


class IsolatedEngineTests(unittest.TestCase):
    def make(self, side='buy'):
        f=fixture.HybridProtectionTests();f.setUp();self.addCleanup(f.doCleanups)
        f.make(side,2)
        f.engine.close()
        f.engine=IsolatedEntries(f.primary);f.primary.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        f.payload.update(accountExecution='independent-v5',chunkPercent=25,chunkVolume=.25,fillPriority='risk')
        for native in f.natives:native.info.volume_step=native.info.volume_min=.01
        return f

    def until(self,f,predicate,timeout=None):
        timeout=20 if sys.platform=='win32' else 6 if timeout is None else timeout
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            f.engine.tick();state=f.state()
            if predicate(state):return state
            time.sleep(.002)
        self.fail(repr(state))

    def test_both_sides_complete_with_distinct_ids_and_journals(self):
        for side in ('buy','sell'):
            f=self.make(side);f.engine.arm(f.payload)
            state=self.until(f,lambda s:s['phase']=='completed')
            self.assertEqual([x['filledVolume'] for x in state['accounts'].values()],[1,1])
            self.assertEqual(len({x['id'] for x in state['children']}),len(state['children']))
            paths=[engine.bridge.db.execute('PRAGMA database_list').fetchone()[2] for engine in f.engine.shards.values()]
            self.assertEqual(len(set(paths)),2)
            self.assertTrue(all(engine._load(f.payload['id'])[1]['initialSizingOnly'] for engine in f.engine.shards.values()))

    def test_frozen_return_limit_resumes_only_after_quote_returns_to_zone(self):
        f = self.make('sell')
        f.payload.update(chunkPercent=50, chunkVolume=.5, mirrorAccounts=[dict(account='900000001', multiplier=.7)])
        account = '900000001'
        native = f.natives[1]
        f.quote(native, 24993.9)
        send = native.order_send
        cancellations = []

        def freeze_once(request):
            if request['action'] == native.TRADE_ACTION_REMOVE:
                cancellations.append(request['order'])
                if len(cancellations) == 1:
                    native.sent.append(copy.deepcopy(request))
                    return SimpleNamespace(retcode=10029, order=request['order'], volume=0., price=0., comment='frozen')
            return send(request)

        def original_child():
            return f.engine.shards[account]._load(f.payload['id'])[2]['children'][0]

        native.order_send = freeze_once
        lane = f.engine._shard(account)
        send_batch = lane._parallel_batch

        def send_with_legacy_repricing(payload, state, kind, plans, **options):
            options.pop('fixed_limit_accounts', None)
            return send_batch(payload, state, kind, plans, **options)

        # Recover a working order repriced inside the zone by an older receiver.
        with patch.object(lane, '_parallel_batch', send_with_legacy_repricing):
            f.engine.arm(f.payload)
            self.until(f, lambda state: state['accounts'][account].get('pendingVolume') == .7
                       and any(child['state'] == 'pending' and account in child['volumes'] for child in state['children']))
        ticket = native.orders[0].ticket
        self.assertEqual(native.orders[0].price_open, 24994.1)
        f.quote(native, 24994.)
        self.until(f, lambda _: bool(original_child().get('frozenCancelQuotes')))
        first_cancel_id = original_child()['cancelIds'][account]
        self.assertEqual(cancellations, [ticket])
        self.assertFalse(f.markets(native))

        f.quote(native, 24993.)
        self.until(f, lambda state: state['accounts'][account].get('entryReason') ==
                   'Return limit reserved; market quote outside captured zone')
        self.assertEqual(cancellations, [ticket])
        self.assertEqual([order.ticket for order in native.orders], [ticket])
        self.assertFalse(f.markets(native))

        f.quote(native, 24995.)
        state = self.until(f, lambda state: state['phase'] == 'completed')
        self.assertEqual(cancellations[:2], [ticket, ticket])
        self.assertNotEqual(original_child()['cancelIds'][account], first_cancel_id)
        self.assertEqual(state['accounts'][account]['filledVolume'], .7)
        self.assertAlmostEqual(sum(position.volume for position in native.positions), .7)
        self.assertFalse(native.orders)

    def test_direct_edge_limit_survives_eligible_quote_until_explicit_stop(self):
        f = self.make('sell')
        native = f.natives[0]
        account = '900000002'
        f.quote(native, 24993.)
        f.engine.arm(f.payload)
        self.until(f, lambda s: s['accounts'][account].get('pendingVolume') == 1.
                   and any(child['state'] == 'pending' and account in child['volumes'] for child in s['children']))
        ticket = native.orders[0].ticket
        self.assertEqual(native.orders[0].price_open, 24994.)
        f.quote(native, 24995.)
        self.until(f, lambda s: s['accounts'][account].get('entryReason') ==
                   'Return limit reserved; waiting for exact pending result' or len(native.sent) > 1)
        self.assertEqual([row.ticket for row in native.orders], [ticket])
        self.assertEqual([request['action'] for request in native.sent], [native.TRADE_ACTION_PENDING])
        result = f.engine.stop(dict(id='stop-direct-edge', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
        self.assertEqual(result['lanePhases'][account], 'stopped')
        self.assertEqual([request['order'] for request in native.sent if request['action'] == native.TRADE_ACTION_REMOVE], [ticket])

    def test_full_return_limit_keeps_edge_when_spread_straddles_zone(self):
        for side, direction in (('buy', 1), ('sell', -1)):
            with self.subTest(side=side):
                f = self.make(side)
                account, native = '900000001', f.natives[1]
                native.info.volume_step = native.info.volume_min = .1
                f.payload.update(chunkPercent=50, chunkVolume=.5, mirrorAccounts=[dict(account=account, multiplier=.7)])
                edge = f.payload['trigger'] + direction*f.payload['zoneRadius']
                f.quote(native, edge + direction*.1)
                f.engine.arm(f.payload)
                self.until(f, lambda state: state['accounts'][account].get('pendingVolume') == .7
                           and any(child['state'] == 'pending' and account in child['volumes'] for child in state['children']))
                order = native.orders[0]
                self.assertEqual((order.price_open, order.volume_current), (edge, .7))

                f.quote(native, edge - direction*.1)
                self.until(f, lambda state: state['accounts'][account].get('entryReason') ==
                           'Return limit reserved; waiting for exact pending result')
                self.assertEqual([row.ticket for row in native.orders], [order.ticket])
                self.assertEqual([request['action'] for request in native.sent], [native.TRADE_ACTION_PENDING])

                with f.bridges[1].lock:
                    native.orders.remove(order)
                    opening = dict(symbol=order.symbol, type=native.ORDER_TYPE_BUY if side == 'buy' else native.ORDER_TYPE_SELL,
                                   volume=.7, price=edge, sl=order.sl, tp=order.tp, comment=order.comment)
                    native.positions.append(native.position_from(order.ticket, opening))
                    native.history_orders.append(SimpleNamespace(ticket=order.ticket, symbol=order.symbol,
                                                                 position_id=order.ticket, state=4))
                    native.history_deals.append(SimpleNamespace(symbol=order.symbol, position_id=order.ticket,
                                                                entry=0, volume=.7, reason=0))
                    native.observe('limit-filled')
                state = self.until(f, lambda state: state['accounts'][account].get('entryStatus') == 'completed')
                self.assertEqual(state['accounts'][account]['filledVolume'], .7)
                self.assertEqual(state['accounts'][account]['pendingVolume'], 0.)
                self.assertEqual([(row.ticket, row.volume) for row in native.positions], [(order.ticket, .7)])
                self.assertFalse(f.markets(native))

    def test_return_fill_between_native_reads_is_confirmed_even_with_stale_quote(self):
        for side, direction in (('buy', 1), ('sell', -1)):
            with self.subTest(side=side):
                f = self.make(side)
                account, native, bridge = '900000001', f.natives[1], f.bridges[1]
                f.payload.update(mirrorAccounts=[dict(account=account, multiplier=.7)])
                edge = f.payload['trigger'] + direction*f.payload['zoneRadius']
                f.quote(native, edge + direction*.2)
                f.engine.arm(f.payload)
                self.until(f, lambda state: state['accounts'][account].get('pendingVolume') == .7
                           and any(child['state'] == 'pending' and account in child['volumes'] for child in state['children']))
                order = native.orders[0]
                observations = []
                get_orders, limit_state = native.orders_get, bridge.entry_limit_state

                def fill_between_reads(*args, **kwargs):
                    if not observations:
                        native.orders.remove(order)
                        opening = dict(symbol=order.symbol, type=native.ORDER_TYPE_BUY if side == 'buy' else native.ORDER_TYPE_SELL,
                                       volume=.7, price=edge, sl=order.sl, tp=order.tp, comment=order.comment)
                        native.positions.append(native.position_from(order.ticket, opening))
                        native.history_orders.append(SimpleNamespace(ticket=order.ticket, symbol=order.symbol,
                                                                     position_id=order.ticket, state=4))
                        native.history_deals.append(SimpleNamespace(symbol=order.symbol, position_id=order.ticket,
                                                                    entry=0, volume=.7, reason=0))
                        native.observe('limit-filled')
                    observations.append('orders')
                    return get_orders(*args, **kwargs)

                def observe_limit(ticket, symbol):
                    with bridge.lock:
                        if observations:
                            return limit_state(ticket, symbol)
                        with patch.object(native, 'orders_get', fill_between_reads):
                            return limit_state(ticket, symbol)

                with bridge.lock:
                    native.tick.time = f.now - 2
                    bridge.entry_limit_state = observe_limit
                state = self.until(f, lambda state: state['accounts'][account]['filledVolume'] == .7)
                self.assertEqual(observations, ['orders', 'orders'])
                self.assertEqual(state['accounts'][account].get('pendingVolume'), 0.)
                f.quote(native, edge)
                native.tick.time = time.time()
                state = self.until(f, lambda state: state['accounts'][account].get('entryStatus') == 'completed')
                self.assertEqual(state['accounts'][account]['filledVolume'], .7)
                self.assertEqual([request['action'] for request in native.sent], [native.TRADE_ACTION_PENDING])

    def test_edge_limit_capacity_refusal_and_partial_fill_retain_working_remainder(self):
        for retcode in (10033, 10034):
            with self.subTest(retcode=retcode):
                f = self.make('sell')
                native = f.natives[0]
                account = '900000002'
                edge = 24994.
                f.quote(native, edge+.1)
                guard, send = f.primary.guard_chunk_request, native.order_send
                tickets, refusals = [], []
                guard_fill = []

                def partial_fill(order, amount):
                    order.volume_current = round(order.volume_current-amount, 8)
                    order.position_id = order.ticket
                    position = next((row for row in native.positions if row.ticket == order.ticket), None)
                    if position:
                        position.volume = round(position.volume+amount, 8)
                    else:
                        opening = dict(symbol=order.symbol, type=native.ORDER_TYPE_SELL, volume=amount,
                                       price=order.price_open, sl=order.sl, tp=order.tp, comment=order.comment)
                        native.positions.append(native.position_from(order.ticket, opening))
                    native.history_deals.append(SimpleNamespace(symbol=order.symbol, position_id=order.ticket,
                        entry=0, volume=amount, reason=0))
                    native.observe('partial-limit')

                def slip_before_support(payload, request):
                    if payload['type'] == 'limit' and not tickets:
                        f.quote(native, edge-.5)
                    elif (retcode == 10033 and payload['type'] == 'market' and payload['entryGuard']['positions']
                          and not guard_fill):
                        partial_fill(native.orders[0], .15)
                        guard_fill.append(True)
                    return guard(payload, request)

                def refuse_supplement(request):
                    if request['action'] != native.TRADE_ACTION_PENDING:
                        return send(request)
                    if tickets:
                        refusals.append(request.copy())
                        return SimpleNamespace(retcode=retcode, order=0, volume=0., price=0., comment='capacity')
                    result = send(request)
                    tickets.append(result.order)
                    return result

                f.primary.guard_chunk_request = slip_before_support
                native.order_send = refuse_supplement
                f.engine.arm(f.payload)
                self.until(f, lambda s: len(refusals) == 1 and 'Existing limit retained' in s['accounts'][account].get('entryReason', ''))
                with f.primary.lock:
                    order = native.orders[0]
                    partial_fill(order, .25)
                self.until(f, lambda s: s['accounts'][account].get('filledVolume') == .25)
                f.quote(native, edge+1.)
                with patch('time.time', return_value=f.now+.5):
                    if retcode == 10033:
                        self.until(f, lambda s: any(child.get('result', {}).get('reason') ==
                            'Previous entry position closed or changed; no further entry' for child in s['children']))
                        self.assertTrue(guard_fill)
                    else:
                        self.until(f, lambda s: s['accounts'][account].get('filledVolume') == .25)
                        with f.primary.lock:
                            partial_fill(order, .15)
                with patch('time.time', return_value=f.now+.8):
                    expected_fill = .65 if retcode == 10033 else .40
                    result = self.until(f, lambda s: s['accounts'][account].get('filledVolume') == expected_fill
                                        and s['accounts'][account].get('pendingVolume') == .35)
                    row = result['accounts'][account]
                    self.assertEqual(row['freeVolume'], .0 if retcode == 10033 else .25)
                    self.assertEqual([order.ticket for order in native.orders], tickets)
                    self.assertEqual(len(refusals), 1)
                    self.assertEqual([request['volume'] for request in f.markets(native)], [.25] if retcode == 10033 else [])
                    self.assertFalse(any(request['action'] == native.TRADE_ACTION_REMOVE for request in native.sent))
                    stopped = f.engine.stop(dict(id='stop-partial-edge', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
                    self.assertEqual(stopped['lanePhases'][account], 'stopped')
                    self.assertFalse(native.orders)

    def test_market_price_refusal_keeps_edge_limit_and_adds_only_free_volume(self):
        cases = (('sell', -1, 'fill'), ('buy', 1, 'fill-during-send'), ('sell', -1, 'return-before-top-up'),
                 ('sell', -1, 'stop'), ('sell', -1, 'lost-receipt'))
        for side, direction, outcome in cases:
            with self.subTest(side=side, outcome=outcome):
                f = self.make(side)
                account = '900000002'
                native = f.natives[0]
                native.info.volume_step = native.info.volume_min = .1
                native.group_budgets['cid:protection-one'] = (16.1, 257.6)
                f.payload.update(volume=16.1, riskUsd=257.6, chunkPercent=50, chunkVolume=8.,
                    tp=25000. + direction*102, mirrorAccounts=[dict(account='900000001', multiplier=.05)])
                edge = 25000. + direction*6
                f.quote(native, edge-direction*.1)
                guard, send = f.primary.guard_chunk_request, native.order_send
                tickets = []

                def move_quote_before_support(payload, request):
                    if payload['type'] == 'limit' and not tickets:
                        f.quote(native, edge+direction*.5)
                    elif (outcome == 'return-before-top-up' and payload['type'] == 'limit'
                          and not payload['entryGuard'].get('supportReprice')):
                        f.quote(native, edge-direction*2.6)
                    return guard(payload, request)

                def fill_limit(order):
                    native.orders.remove(order)
                    opening = dict(symbol=order.symbol, type=native.ORDER_TYPE_BUY if direction == 1 else native.ORDER_TYPE_SELL,
                        volume=order.volume_current, price=order.price_open, sl=order.sl, tp=order.tp, comment=order.comment)
                    native.positions.append(native.position_from(order.ticket, opening))
                    native.history_orders.append(SimpleNamespace(ticket=order.ticket, symbol=order.symbol, position_id=order.ticket, state=4))
                    native.history_deals.append(SimpleNamespace(symbol=order.symbol, position_id=order.ticket,
                        entry=0, volume=order.volume_current, reason=0))
                    native.observe('limit-filled')

                def record_limits(request):
                    pending = request['action'] == native.TRADE_ACTION_PENDING
                    if outcome == 'fill-during-send' and pending and len(tickets) == 1 and native.orders:
                        fill_limit(native.orders[0])
                    result = send(request)
                    if pending:
                        tickets.append(result.order)
                        if outcome == 'lost-receipt' and len(tickets) == 2:
                            return None
                    return result

                f.primary.guard_chunk_request = move_quote_before_support
                native.order_send = record_limits
                f.engine.arm(f.payload)
                # Only the tested lane needs the synthetic wall-clock jump.
                # Let the independent peer finish its native protection first.
                self.until(f, lambda s: s['accounts']['900000001'].get('entryStatus') == 'completed')
                if outcome == 'return-before-top-up':
                    self.until(f, lambda s: any(child['type'] == 'limit' and child['state'] == 'rejected'
                                              and account in child['volumes'] for child in s['children']))
                    with patch('time.time', return_value=f.now+.3):
                        self.until(f, lambda s: s['accounts'][account].get('filledVolume') == 8. or len(native.sent) > 2)
                        self.assertEqual([(request['action'], request['volume']) for request in native.sent],
                                         [(native.TRADE_ACTION_PENDING, 8.1), (native.TRADE_ACTION_DEAL, 8.)])
                        with f.primary.lock:
                            self.assertEqual([order.ticket for order in native.orders], tickets)
                            fill_limit(native.orders[0])
                        state = self.until(f, lambda s: s['phase'] == 'completed')
                        self.assertEqual(state['accounts'][account]['filledVolume'], 16.1)
                    continue
                self.until(f, lambda s: len(tickets) >= 2 and s['accounts'][account].get('freeVolume') == 0)
                self.assertEqual([request['action'] for request in native.sent], [native.TRADE_ACTION_PENDING]*2)
                self.assertEqual([(request['price'], request['volume']) for request in native.sent], [(edge, 8.1), (edge, 8.)])
                self.assertEqual({(request['sl'], request['tp']) for request in native.sent},
                                 {(edge-direction*16, f.payload['tp'])})
                children = f.engine.shards[account]._load(f.payload['id'])[2]['children']
                market = next(child for child in children if child['type'] == 'market')
                self.assertEqual(market['result']['entryDiagnostics']['rejectionStage'], 'native-entry-price-guard')
                outstanding = tickets[1:] if outcome == 'fill-during-send' else tickets
                self.assertEqual([order.ticket for order in native.orders], outstanding)

                if outcome == 'stop':
                    result = f.engine.stop(dict(id='stop-edge-limits', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
                    self.assertEqual(result['lanePhases'][account], 'stopped')
                    self.assertEqual([request['order'] for request in native.sent if request['action'] == native.TRADE_ACTION_REMOVE], tickets)
                    self.assertFalse(native.orders or native.positions)
                    continue
                if outcome == 'lost-receipt':
                    self.until(f, lambda s: s['lanePhases'].get(account) == 'paused')
                    f.quote(native, edge-direction)
                    for _ in range(3):
                        f.engine.tick()
                        time.sleep(.01)
                    self.assertEqual(len(native.sent), 2)
                    self.assertEqual([order.ticket for order in native.orders], outstanding)
                    continue

                if outcome == 'fill':
                    # The price returns before the broker exposes either fill.
                    # Both accepted tickets must still be there to catch it.
                    f.quote(native, edge-direction*2.6)
                    with patch('time.time', return_value=f.now+.3):
                        self.until(f, lambda s: len(native.sent) > 2 or s['accounts'][account].get('entryReason') ==
                                   'Return limit reserved; waiting for exact pending result')
                    self.assertEqual([order.ticket for order in native.orders], tickets)
                    self.assertEqual(len(native.sent), 2)
                with f.primary.lock:
                    for order in list(native.orders):
                        fill_limit(order)
                    f.quote(native, edge)
                state = self.until(f, lambda s: s['phase'] == 'completed')
                self.assertEqual(state['accounts'][account]['filledVolume'], 16.1)
                self.assertEqual(state['accounts'][account]['pendingVolume'], 0.)
                self.assertEqual([row.ticket for row in native.positions], tickets)

    def test_entry_take_tracks_each_accounts_weighted_market_and_limit_fills(self):
        for symbol, stop_mode in (('USTEC', 'entry'), ('SPX500', 'level')):
            for side, direction in (('buy', 1), ('sell', -1)):
                with self.subTest(symbol=symbol, side=side):
                    f = self.make(side)
                    distance = 15 if symbol == 'SPX500' else 102
                    f.payload.update(symbol=symbol, stopMode=stop_mode,
                                     tp=f.payload['trigger'] + direction * distance)
                    if stop_mode == 'level':
                        f.payload['riskUsd'] += f.payload['zoneRadius']
                    f.quote(f.natives[1], f.payload['trigger'] + direction * 4)
                    for bridge, native in zip(f.bridges, f.natives):
                        bridge.config['symbols'] = [symbol]
                        native.group_budgets['cid:protection-one'] = (1., f.payload['riskUsd'])
                        native.fill_on_cancel = True
                    f.engine.arm(f.payload)
                    state = self.until(f, lambda s: s['phase'] in ('completed', 'stopped'))
                    self.assertEqual(state['phase'], 'completed', state)
                    takes = []
                    for native in f.natives:
                        positions = native.positions
                        self.assertGreater(len({row.price_open for row in positions}), 1)
                        average = sum(row.volume * row.price_open for row in positions) / sum(row.volume for row in positions)
                        take = positions[0].tp
                        self.assertEqual({row.tp for row in positions}, {take})
                        self.assertAlmostEqual(state['accounts'][str(native.account.login)]['tp'], take)
                        self.assertGreaterEqual(direction * (take - average) + 1e-8, distance)
                        self.assertLessEqual(direction * (take - average), distance + f.payload['tickSize'] + 1e-8)
                        if stop_mode == 'level':
                            self.assertEqual({row.sl for row in positions}, {f.payload['sl']})
                        takes.append(take)
                    self.assertNotEqual(*takes)

    def test_zero_price_on_one_account_still_confirms_its_weighted_stop(self):
        for support_fills in (True, False):
            with self.subTest(support_fills=support_fills):
                f=self.make('sell')
                f.payload.update(chunkPercent=50,chunkVolume=.5,mirrorAccounts=[dict(account='900000001',multiplier=.7)])
                for native in f.natives:
                    native.info.volume_step=native.info.volume_min=.1
                follower=f.natives[1]
                follower.fill_on_cancel=support_fills
                send=follower.order_send
                positions_get=follower.positions_get
                first_market=[]
                proof_visible=threading.Event()
                if support_fills:
                    proof_visible.set()

                def complete_without_price(request):
                    result=send(request)
                    if request['action']==follower.TRADE_ACTION_DEAL and 'position' not in request and not first_market:
                        first_market.append(result.order)
                        result.price=0.
                    return result

                def visible_positions(*args,**kwargs):
                    rows=positions_get(*args,**kwargs)
                    return rows if proof_visible.is_set() else [row for row in rows if row.ticket not in first_market]

                follower.order_send=complete_without_price
                follower.positions_get=visible_positions
                f.engine.arm(f.payload)
                if not support_fills:
                    self.until(f,lambda s:s['lanePhases']['900000001']=='paused')
                    self.assertEqual([row['volume'] for row in f.markets(follower)],[.3])
                    self.assertEqual([row.volume_current for row in follower.orders],[.4])
                    proof_visible.set()
                state=self.until(f,lambda s:s['phase'] in ('completed','stopped'))
                self.assertEqual(state['phase'],'completed',state)
                self.assertEqual([state['accounts'][account]['filledVolume'] for account in ('900000002','900000001')],[1.,.7])
                expected_parts=[.3,.4] if support_fills else [.1,.3,.3]
                self.assertEqual(sorted(row.volume for row in follower.positions),expected_parts)
                self.assertEqual([row['volume'] for row in f.markets(follower)],[.3] if support_fills else [.3,.3,.1])
                for native in f.natives:
                    self.assertFalse(native.orders)
                    self.assertEqual(len({row.sl for row in native.positions}),1)
                    average=sum(row.volume*row.price_open for row in native.positions)/sum(row.volume for row in native.positions)
                    self.assertAlmostEqual(state['accounts'][str(native.account.login)]['sl'],native.positions[0].sl)
                    self.assertLessEqual(abs(native.positions[0].sl-(average+16.)),f.payload['tickSize']+1e-8)

    def test_fractional_multiplier_rounds_parts_to_native_step_and_scales_risk(self):
        for side in ('buy','sell'):
            with self.subTest(side=side):
                f=self.make(side)
                f.payload.update(volume=.5,chunkVolume=.2,chunkPercent=50,riskUsd=13,
                    stopOffsetPts=250,sl=24975. if side=='buy' else 25025.,
                    mirrorAccounts=[dict(account='900000001',multiplier=1.6)])
                f.primary.config['entryRiskLimitUsd']=13
                f.payload['entryRiskLimitUsd']=13
                for native in f.natives:
                    native.info.volume_step=native.info.volume_min=.1
                    native.group_budgets['cid:protection-one']=(.5 if native.account.login==900000002 else .8,
                        13 if native.account.login==900000002 else 20.8)
                f.engine.arm(f.payload)
                state=self.until(f,lambda s:s['phase'] not in ('armed','running','paused','sending'))
                self.assertEqual(state['phase'],'completed',state)
                for account,target,budget in [('900000002',.5,13),('900000001',.8,20.8)]:
                    details=state['accounts'][account]
                    self.assertEqual(details['targetVolume'],target)
                    self.assertEqual(details['filledVolume'],target)
                    self.assertEqual(details['riskUsd'],budget)
                    self.assertLessEqual(details['riskUsed'],budget+1e-8)
                    parts=f.engine.shards[account]._load(f.payload['id'])[2]['accounts'][account]['parts']
                    self.assertTrue(all(abs(v/.1-round(v/.1))<1e-8 for v in parts))
                self.assertGreater(state['accounts']['900000001']['riskUsed'],13)
                before=copy.deepcopy(state['children'])
                f.engine.arm(f.payload)
                self.assertEqual(f.state()['children'],before)
                closed=f.engine.close_position(dict(id='half-scaled-parent',entryId=f.payload['id'],
                    symbol='USTEC',closePercent=50,expiresAt=time.time()*1000+3000))['result']
                self.assertEqual(closed['status'],'ok',closed)
                self.assertEqual(closed['accountResults']['900000002']['filledVolume'],.2)
                self.assertEqual(closed['accountResults']['900000002']['remainingVolume'],.3)
                self.assertEqual(closed['accountResults']['900000001']['filledVolume'],.4)
                self.assertEqual(closed['accountResults']['900000001']['remainingVolume'],.4)

    def test_scaled_sizes_respect_account_step_minimum_percentage_and_original_intent(self):
        payload=dict(volume=.8,chunkVolume=.32,chunkPercent=50,initialSizingOnly=True,
            accountExecution='independent-v4',executionPolicy='hybrid-market-limit-v1')
        original=copy.deepcopy(payload)
        self.assertEqual(account_entry_sizes(payload,dict(quantityStep=.1,minQty=.1,maxQty=100)),(.8,.3))
        self.assertEqual(account_entry_sizes(dict(payload,volume=.625,chunkVolume=.25),
            dict(quantityStep=.1,minQty=.1,maxQty=100)),(.6,.2))
        self.assertEqual(account_entry_sizes(dict(payload,chunkPercent=25),
            dict(quantityStep=.1,minQty=.1,maxQty=100)),(.8,.2))
        self.assertEqual(account_entry_sizes(payload,dict(quantityStep=.01,minQty=.01,maxQty=100)),(.8,.32))
        self.assertEqual(account_entry_sizes(dict(payload,chunkVolume=.04),
            dict(quantityStep=.1,minQty=.1,maxQty=100)),(.8,.1))
        with self.assertRaisesRegex(ValueError,'smaller than the minimum'):
            account_entry_sizes(dict(payload,volume=.08),dict(quantityStep=.1,minQty=.1,maxQty=100))
        self.assertEqual(account_entry_sizes(dict(payload,initialSizingOnly=False),
            dict(quantityStep=.1,minQty=.1,maxQty=100)),(.8,.32))
        self.assertEqual(payload,original)

    def test_slow_admission_does_not_hold_peer_or_root_lock(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        original=f.primary.linked['900000001'].call
        def call(method,*args):
            if method=='entry_admission':entered.set();release.wait(3)
            return original(method,*args)
        f.primary.linked['900000001'].call=call
        started=time.monotonic();f.engine.arm(f.payload)
        self.assertLess(time.monotonic()-started,.5)
        try:
            state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']==1)
            self.assertTrue(entered.is_set());self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
        finally:release.set()
        self.until(f,lambda s:s['phase']=='completed')

    def test_stop_and_protection_keep_exact_parent_ownership(self):
        f=self.make();f.engine.arm(f.payload)
        self.until(f,lambda s:s['phase']=='completed')
        response=f.engine.protection(f.command(sl=24991.,mirrorAccounts=[dict(account='900000001')]))
        self.assertEqual(response['result']['status'],'ok',response)
        self.assertTrue(all(row.sl==24991. for native in f.natives for row in native.positions))
        result=f.engine.stop(dict(id='stop-isolated',entryId=f.payload['id'],expiresAt=time.time()*1000+3000))
        self.assertNotIn(result['phase'],('armed','running','sending','paused'))
        self.assertTrue(all(len(native.positions)==4 for native in f.natives))

    def test_new_protection_recovers_a_zero_price_fill_left_by_old_stopped_lane(self):
        f = self.make()
        native = f.natives[1]
        send = native.order_send
        confirm = ChunkEntries._confirmed_position
        zero_tickets = []

        def zero_final_fill(request):
            result = send(request)
            if request['action'] == native.TRADE_ACTION_DEAL and 'position' not in request and len(f.markets(native)) == 4:
                result.price = 0.
                zero_tickets.append(str(result.order))
            return result

        def old_confirmation(engine, payload, state, child, account, outcome, **options):
            if account == '900000001' and outcome.get('fillPrice') == 0:
                raise ValueError('Confirmed fill price must be positive')
            return confirm(engine, payload, state, child, account, outcome, **options)

        native.order_send = zero_final_fill
        with patch.object(ChunkEntries, '_confirmed_position', old_confirmation):
            f.engine.arm(f.payload)
            self.until(f, lambda state: state['lanePhases']['900000001'] == 'paused')
            f.engine.stop(dict(id='stop-legacy-zero', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
        before = f.state()
        self.assertEqual(before['lanePhases']['900000001'], 'stopped', before)
        self.assertEqual(before['accounts']['900000001']['filledVolume'], .75)
        self.assertEqual(len(zero_tickets), 1)
        self.assertFalse(native.orders)
        lane = f.engine.shards['900000001']
        call = lane._call

        def wrong_receipt(account, method, *args, **kwargs):
            reply = call(account, method, *args, **kwargs)
            if method == 'receipt':
                return dict(reply, result=dict(reply['result'], providerOrderId='1'))
            return reply

        sent = native.protection_sends
        rejected_command = f.command(id='legacy-zero-wrong-ticket', sl=24991., account='900000001')
        with patch.object(lane, '_call', wrong_receipt):
            rejected = f.engine.protection(rejected_command)
        self.assertEqual(rejected['result']['status'], 'rejected')
        self.assertEqual(native.protection_sends, sent)

        for index, position in enumerate(native.positions):
            position.sl = 24990. + index*.1
        manual_stops = {str(row.ticket):row.sl for row in native.positions}
        states = {'900000001':dict(f.bridges[1].entry_management(), connected=True, evidenceReady=True)}
        f.engine.decorate_management(states)
        self.assertEqual(f.state()['accounts']['900000001']['filledVolume'], 1.)
        self.assertEqual(f.state()['lanePhases']['900000001'], 'stopped')
        for row in states['900000001']['orders']:
            self.assertEqual(row['entryPlan']['sl'], manual_stops[row['ticket']])
            self.assertTrue(row['entryPlan']['protectionReady'])
        self.assertEqual({str(row.ticket):row.sl for row in native.positions}, manual_stops)
        self.assertEqual(native.protection_sends, sent)

        response = f.engine.protection(f.command(sl=24991., mirrorAccounts=[dict(account='900000001')]))
        self.assertEqual(response['result']['status'], 'ok', response['result']['accountResults'])
        self.assertEqual(f.state()['accounts']['900000001']['filledVolume'], 1.)
        self.assertEqual(f.state()['lanePhases']['900000001'], 'stopped')
        self.assertEqual({row.sl for row in native.positions}, {24991.})
        self.assertEqual(len(f.markets(native)), 4)
        self.assertFalse(f.closes(native))
        self.assertEqual(f.engine.protection(rejected_command)['result'], rejected['result'])

    def test_restart_does_not_rearm_and_keeps_exact_fills(self):
        f=self.make();f.engine.arm(f.payload)
        before=self.until(f,lambda s:s['phase']=='completed')
        f.engine.close();f.engine=IsolatedEntries(f.primary);f.primary.chunk_entries=f.engine
        self.addCleanup(f.engine.close)
        after=f.state()
        self.assertEqual(after['phase'],'completed')
        self.assertEqual(after['children'],before['children'])

    def test_restart_reconciles_passive_opening_with_stale_standalone_lane_phase(self):
        f = self.make()
        f.engine.arm(f.payload)
        self.until(f, lambda state: state['phase'] == 'completed')
        account, native = '900000002', f.natives[0]
        shard = f.engine.shards[account]
        identifier = f.payload['id']
        state = shard._load(identifier)[2]
        child = next(row for row in state['children'] if row['type'] == 'market')
        position = child['positions'].pop(account)
        receipt = f.primary.receipt(child['id'])['result']
        self.assertEqual(receipt['retcode'], 10009)
        receipt['fillPrice'] = 0.
        child.update(state='unknown', result=dict(receipt, accountResults={account: receipt}))
        state.update(phase='paused', observeAdmissionHold=True, lanePhases={account: 'stopped'},
            reconciliation=dict(status='checking', resumeAllowed=False, attempts=0),
            hybridRemainderStop='Previous entry stopped; waiting for exact broker outcome')
        shard._save(state)
        ticket = int(position['openingTicket'])
        order = next(row for row in native.history_orders if row.ticket == ticket)
        order.type, order.volume_initial = native.ORDER_TYPE_BUY, position['volume']
        order.sl, order.tp = receipt['sl'], position['tp']
        incoming = next(row for row in native.history_deals if row.position_id == ticket)
        incoming.ticket, incoming.order = 1, ticket
        incoming.type, incoming.price = native.ORDER_TYPE_BUY, position['fillPrice']
        native.history_deals.append(SimpleNamespace(ticket=2, order=ticket + 1, position_id=ticket,
            symbol=order.symbol, entry=1, type=native.ORDER_TYPE_SELL, volume=incoming.volume,
            price=incoming.price - 1., reason=4))
        native.positions.clear()
        f.primary.db.execute('UPDATE requests SET result=? WHERE id=?', (json.dumps(receipt), child['id']))
        f.primary.db.commit()
        send_counts = [len(broker.sent) for broker in f.natives]

        f.engine.close()
        f.engine = IsolatedEntries(f.primary)
        f.primary.chunk_entries = f.engine
        self.addCleanup(f.engine.close)
        result = self.until(f, lambda row: row['lanePhases'][account] == 'stopped')
        recovered = next(row for row in result['children'] if row['id'] == child['id'])
        self.assertEqual(recovered['state'], 'confirmed')
        self.assertEqual(recovered['positions'][account]['fillPrice'], position['fillPrice'])
        settled = f.engine.shards[account]._load(identifier)[2]
        self.assertEqual(settled['reconciliation']['status'], 'confirmed')
        self.assertFalse(settled['reconciliation']['resumeAllowed'])
        self.assertFalse(settled.get('observeAdmissionHold'))
        self.assertEqual([len(broker.sent) for broker in f.natives], send_counts)

    def test_peer_fills_while_other_account_journal_is_held(self):
        f=self.make();entered=threading.Event();release=threading.Event();self.addCleanup(release.set)
        original=ChunkEntries._save
        def save(engine,state,receipt=None):
            if getattr(engine,'account_id',None)=='900000001' and any(row['state']=='sending' for row in state['children']):
                entered.set();release.wait(3)
            return original(engine,state,receipt)
        with patch.object(ChunkEntries,'_save',save):
            f.engine.arm(f.payload)
            try:
                state=self.until(f,lambda s:s['accounts']['900000002']['filledVolume']==1)
                self.assertTrue(entered.is_set());self.assertEqual(state['accounts']['900000001']['filledVolume'],0)
            finally:release.set()
            self.until(f,lambda s:s['phase']=='completed')

    def test_no_monetary_reservation_limits_target_parts(self):
        f=self.make();f.engine.arm(f.payload)
        self.until(f,lambda s:bool(f.engine.account_states))
        engine=next(iter(f.engine.shards.values()))
        found=engine._load(f.payload['id'])
        while not found:
            time.sleep(.002);found=engine._load(f.payload['id'])
        payload,state=found[1:];account=engine.primary
        state['accounts'][account]['riskUsed']=1000
        self.assertEqual(engine._priority_volume(payload,state,account,'market',25002.,.25,{account:1000}),.25)
        guards=engine._hybrid_guards(payload,state,{account:25002.},{account:.25},{account:1000})
        self.assertGreater(guards[account]['riskBudget'],0)

    def test_stopping_during_admission_cannot_send_a_late_part(self):
        f=self.make();f.engine.arm(f.payload)
        f.engine.stop(dict(id='stop-before-tick',entryId=f.payload['id'],expiresAt=time.time()*1000+3000))
        for _ in range(5):f.engine.tick()
        self.assertFalse(any(native.sent for native in f.natives))

    def test_primary_connector_facade_and_independent_management(self):
        f=self.make();f.engine.close()
        class Worker:
            def call(self,method,*args):
                with f.primary.lock:
                    return worker_result(method,getattr(f.primary,method)(*args))
        coordinator=CoordinatorBridge(f.primary.config,f.paths[0],Worker())
        class QuoteClient(Worker):
            def __init__(self,config,path):pass
            def available(self):return True
            def rpc_snapshot(self):return {}
            def close(self):pass
        coordinator.quote_worker_factory=QuoteClient
        self.addCleanup(coordinator.db.close)
        self.addCleanup(coordinator.close_group_executor)
        self.addCleanup(coordinator.chunk_entries.close)
        coordinator.linked=f.primary.linked
        coordinator.start_rules()
        self.assertIsNone(coordinator.mt5)
        self.assertIn('independent-account-engines-v5',coordinator.health()['capabilities'])
        self.assertEqual(coordinator.snapshot('USTEC')['account'],900000002)
        coordinator.management_accounts()
        # A cold UI read intentionally yields after 20ms. Observe completion
        # of those exact reads instead of assuming Windows scheduled both in
        # that window; no extra native poll or relaxed freshness is needed.
        for _,job in list(coordinator.management_jobs.values()):job.result(timeout=2)
        views=coordinator.management_accounts()
        self.assertTrue(views['900000002']['connected']);self.assertTrue(views['900000001']['connected'])
        self.assertIn('independent-account-engines-v5',coordinator.management()['capabilities'])
        self.assertIsNone(coordinator.chunk_entries.progress_observer.snapshot())
        coordinator.frames=3
        self.assertEqual(coordinator.diagnostics()['quoteStream']['frames'],3)
        f.engine=coordinator.chunk_entries
        f.engine.arm(f.payload)
        self.until(f,lambda s:s['phase']=='completed')

    def test_native_protection_guard_rejects_wrong_side_and_broker_minimum_without_send(self):
        f=self.make();native=f.natives[0];bridge=f.primary
        payload=dict(id='native-sl',symbol='USTEC',side='buy',type='market',volume=.2,price=25002.,sl=25001.9,tp=0.,
            expiresAt=time.time()*1000+3000,entryGuard=dict(anchor=25002.,maxGap=1.,maxRiskPrice=25003.,positions=[]))
        result=bridge.execute('place',payload)['result']
        self.assertEqual(result['status'],'rejected');self.assertFalse(native.sent)
        native.info.trade_stops_level=200;native.info.point=.1
        result=bridge.execute('place',dict(payload,id='native-minimum',sl=24986.))['result']
        self.assertEqual(result['status'],'rejected');self.assertFalse(native.sent)

    def test_unknown_cancellation_keeps_its_reservation_and_does_not_block_peer(self):
        f=self.make();f.natives[0].cancel_unknown=True
        f.engine.arm(f.payload)
        state=self.until(f,lambda s:s['accounts']['900000001']['filledVolume']==1)
        self.assertEqual(state['accounts']['900000002']['filledVolume'],.25)
        self.assertGreater(state['accounts']['900000002']['pendingVolume'],0)
        self.assertEqual(len(f.natives[0].positions),1)

    def test_proven_empty_rejections_do_not_exhaust_ledger_or_reuse_ids(self):
        f=self.make();f.engine.arm(f.payload)
        for job in f.engine.admitting.values():job.result(timeout=5)
        engine=f.engine.shards['900000002'];_,payload,state=engine._load(f.payload['id'])
        lane=SingleAccountLane(engine,'900000002',0);ids=[]
        result=dict(result=dict(status='rejected',retcode=10004,reason='quote changed'))
        with patch('chunk_entry.HYBRID_MAX_SUBMISSIONS',16),patch.object(lane,'_call',return_value=result):
            for _ in range(32):
                guard=lane._hybrid_guards(payload,state,{'900000002':25002.},{'900000002':.25})
                lane._hybrid_send(payload,state,'market',{'900000002':25002.},{'900000002':.25},guard)
                ids.append(state['children'][-1]['id'])
        self.assertEqual(len(set(ids)),32);self.assertLessEqual(len(state['children']),9)
        self.assertEqual(engine._load(f.payload['id'])[2]['accounts']['900000002']['submissionSerial'],32)
        self.assertFalse(any(native.sent for native in f.natives))
