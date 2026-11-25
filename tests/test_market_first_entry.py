"""Exercise the VPS market-first policy with isolated simulated MT5 accounts."""
import unittest
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import test_isolated_entry_engines as isolated
from chunk_entry import IsolatedEntries, primary_balance_risk_sizing


class MarketFirstEntryTests(unittest.TestCase):
    make = isolated.IsolatedEngineTests.make
    until = isolated.IsolatedEngineTests.until



    def test_gold_primary_without_override_respects_broker_maximum_and_explicit_max_lot(self):
        native = SimpleNamespace(login=900000005, balance=203200.)
        terms = dict(symbol='XAUUSD', side='buy', entryMode='side-zone', trigger=3636.71,
                     zoneRadius=.5, sl=3635.21, tickSize=.01, stopMode='level', chunkPercent=100)
        for broker_maximum, max_lot, expected_chunk in ((1000, 0, 254), (200, 0, 200), (1000, 80, 80)):
            with self.subTest(broker_maximum=broker_maximum, max_lot=max_lot):
                metadata = dict(tickSize=.01, contractSize=1, quantityStep=1, minQty=1, maxQty=broker_maximum)
                request = dict(account='900000005', percent=.25, pointValue=1, maxLot=max_lot,
                               quantityStep=1, minQty=1, maxQty=broker_maximum)
                sizing = primary_balance_risk_sizing(dict(terms, primaryRiskRequest=request), native, metadata)
                self.assertEqual((sizing['volume'], sizing['chunkVolume']), (254, expected_chunk))

    def test_capped_return_limit_batches_quote_and_exact_order_for_any_index(self):
        for symbol, use_batch in (('USTEC', True), ('SPX500', True), ('SPX500', False)):
            with self.subTest(symbol=symbol, use_batch=use_batch):
                f = self.make()
                f.primary.config.update(symbols=[symbol], entryMarketFirst=True)
                native = f.natives[0]
                native.info.volume_max = .7
                f.payload.update(symbol=symbol, mirrorAccounts=[])
                f.quote(native, 25007.)
                context = f.primary.entry_context
                checks = []

                def read_context(symbol, watch=(), pending=(), exact=None):
                    if exact is not None:
                        checks.append((symbol, exact))
                    return context(symbol, watch, pending, exact if use_batch else None)

                with patch.object(f.primary, 'entry_context', side_effect=read_context), \
                        patch.object(f.primary, 'entry_limit_state', wraps=f.primary.entry_limit_state) as separate:
                    f.engine.arm(f.payload)
                    self.until(f, lambda _: len(native.orders) == 2)
                    f.engine._drain_lanes(f.payload['id'])
                first, second = native.orders
                self.assertEqual(checks, [(symbol, str(first.ticket))])
                self.assertEqual([first.volume_current, second.volume_current], [.7, .3])
                self.assertTrue(all(row.symbol == symbol and row.price_open == 25006. for row in native.orders))
                self.assertEqual(any(call.args[0] == str(first.ticket) for call in separate.call_args_list), not use_batch)
                self.assertFalse(f.markets(native))

    def test_four_accounts_read_each_new_intent_and_cancel_record_once(self):
        accounts = (900000003, 900000001, 900000004, 900000005)
        f = isolated.fixture.HybridProtectionTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.make(accounts=4, account_ids=accounts)
        f.engine.close()
        f.engine = IsolatedEntries(f.primary)
        f.primary.chunk_entries = f.engine
        self.addCleanup(f.engine.close)
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(accountExecution='independent-v5', chunkPercent=100, chunkVolume=1.)
        journal_reads = {}
        for account in map(str, accounts):
            engine = f.engine._shard(account)
            engine._enable_lane_storage()
            reads = journal_reads[account] = []

            def trace(sql, reads=reads):
                if threading.current_thread().name.startswith('iscc-admission'):
                    if sql.startswith('SELECT digest,payload,state FROM chunk_entries WHERE id='):
                        reads.append('entry')
                    if sql.startswith('SELECT stops.result FROM chunk_entry_cancels'):
                        reads.append('cancel')

            engine.storage.db.set_trace_callback(trace)

        f.engine.arm(f.payload)
        view = self.until(f, lambda row: row['phase'] == 'completed')
        for account, native in zip(map(str, accounts), f.natives):
            self.assertEqual(journal_reads[account], ['entry', 'cancel'])
            self.assertEqual(len(f.markets(native)), 1)
            self.assertEqual(view['accounts'][account]['filledVolume'], 1.)
            timing = view['accounts'][account]['entryTiming']['admission']
            for stage in ('legacyLockWaitMs', 'legacyQueryMs', 'legacyCheckMs', 'accountLockWaitMs',
                          'accountDirectionLockWaitMs', 'accountDirectionQueryMs', 'accountDirectionCheckMs'):
                self.assertGreaterEqual(timing[stage], 0.)

    def test_expired_admission_quote_reads_within_lane_and_rechecks_stop(self):
        for stopped in (False, True):
            with self.subTest(stopped=stopped):
                f = self.make()
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(chunkPercent=100, chunkVolume=1.)
                account = '900000001'
                bridge = f.bridges[1]
                admission, context = bridge.entry_admission, bridge.entry_context
                engine = f.engine._shard(account)
                executor = engine.bridge.warm_group_executor()
                queued_reads = []
                queued_admission = []
                read_entered, release_read = threading.Event(), threading.Event()
                self.addCleanup(release_read.set)

                def age_admission(*args):
                    reply = admission(*args)
                    reply['snapshot']['quote']['nativeCapturedAt'] = time.time()*1000-100.
                    return reply

                def read_context(*args):
                    if not read_entered.is_set():
                        read_entered.set()
                        if stopped and not release_read.wait(5):
                            raise AssertionError('First quote was not released')
                    return context(*args)

                submit = executor.submit

                def observe_queue(task, *args, **kwargs):
                    if task.__name__ == 'read' and not read_entered.is_set():
                        queued_reads.append(task)
                    return submit(task, *args, **kwargs)

                admit_submit = engine.admission_executor.submit

                def observe_admission(task, *args, **kwargs):
                    queued_admission.append(task.__name__)
                    return admit_submit(task, *args, **kwargs)

                with patch.object(bridge, 'entry_admission', side_effect=age_admission), \
                        patch.object(bridge, 'entry_context', side_effect=read_context), \
                        patch.object(engine.admission_executor, 'submit', side_effect=observe_admission), \
                        patch.object(executor, 'submit', side_effect=observe_queue):
                    f.engine.arm(f.payload)
                    if stopped:
                        try:
                            self.until(f, lambda _: read_entered.is_set() and bool(f.markets(f.natives[0])))
                            begin_stops = f.engine._begin_account_stops

                            def stop_and_release(*args):
                                release_read.set()
                                return begin_stops(*args)

                            with patch.object(f.engine, '_begin_account_stops', side_effect=stop_and_release):
                                result = f.engine.stop(dict(id='stop-first-inline-read', entryId=f.payload['id'],
                                                            expiresAt=f.now*1000+3000))
                            self.assertEqual(result['lanePhases'][account], 'stopped')
                            self.assertFalse(f.natives[1].sent)
                        finally:
                            release_read.set()
                    else:
                        result = self.until(f, lambda row: row['accounts'][account].get('filledVolume') == 1.)
                        self.assertEqual(len(f.markets(f.natives[1])), 1)
                        timing = result['accounts'][account]['entryTiming']
                        self.assertTrue(timing['firstInlineQuoteRead'])
                        for stage in ('queueMs', 'beforeReadMs', 'readMs', 'afterReadMs'):
                            self.assertGreaterEqual(timing['admission'][stage], 0.)
                self.assertEqual(queued_reads, [])
                self.assertEqual(queued_admission, [])

    def test_buy_at_upper_edge_is_market_and_above_it_is_buy_limit_at_edge(self):
        for ask in (206., 206.10):
            with self.subTest(ask=ask):
                f = self.make()
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(trigger=200., level=200., sl=184., chunkPercent=100, chunkVolume=1.)
                native = f.natives[0]
                for account_native in f.natives:
                    f.quote(account_native, ask)
                f.engine.arm(f.payload)
                self.until(f, lambda row: any(child['state'] in ('pending','confirmed')
                    and '900000002' in child['volumes'] for child in row['children']))
                opening = native.sent[0]
                expected = ((native.TRADE_ACTION_DEAL, native.ORDER_TYPE_BUY) if ask == 206.
                            else (native.TRADE_ACTION_PENDING, native.ORDER_TYPE_BUY_LIMIT))
                self.assertEqual((opening['action'], opening['type'], opening['price']), (*expected, 206.))

    def test_switches_order_type_on_exact_unsent_guard_tick_without_another_quote_round(self):
        for side, direction in (('buy', 1), ('sell', -1)):
            for original in ('market', 'limit'):
                with self.subTest(side=side, original=original):
                    f = self.make(side)
                    f.primary.config['entryMarketFirst'] = True
                    f.payload.update(chunkPercent=100, chunkVolume=1.)
                    native = f.natives[0]
                    inside, outside = (f.payload['trigger']+direction*distance for distance in (2., 7.))
                    f.quote(native, inside if original == 'market' else outside)
                    bridge = f.bridges[0]
                    guard = bridge.guard_chunk_request
                    snapshot = bridge.entry_context
                    reads_after_refusal = []
                    guards = []
                    writes = []
                    shard = f.engine._shard('900000002')
                    save = shard._save

                    def race(payload, request):
                        guards.append(payload['type'])
                        if len(guards) == 1:
                            f.quote(native, outside if original == 'market' else inside)
                            native.tick.time = time.time()
                        return guard(payload, request)

                    def observe(*args):
                        if len(guards) == 1:
                            reads_after_refusal.append(args)
                        return snapshot(*args)

                    def record(state, *args):
                        writes.append(tuple(child['state'] for child in state['children']))
                        return save(state, *args)

                    with patch.object(bridge, 'guard_chunk_request', side_effect=race), \
                            patch.object(bridge, 'entry_context', side_effect=observe), patch.object(shard, '_save', side_effect=record):
                        f.engine.arm(f.payload)
                        state = self.until(f, lambda row: any(child['state'] in ('pending','confirmed')
                            and '900000002' in child['volumes'] for child in row['children']))
                    self.assertEqual(guards, [original, 'limit' if original == 'market' else 'market'])
                    self.assertEqual(reads_after_refusal, [])
                    first = writes.index(('sending',))
                    second = writes.index(('rejected','sending'))
                    self.assertEqual(writes[first+1:second], [])
                    self.assertEqual(sum(row['action'] in (native.TRADE_ACTION_PENDING,native.TRADE_ACTION_DEAL)
                                         for row in native.sent), 1)
                    refused_id = next(child['id'] for child in state['children'] if child['state']=='rejected'
                                      and '900000002' in child['volumes'])
                    refusal = bridge.receipt(refused_id)['result']
                    self.assertIs(refusal['finalPreMutationRejection'], True)

    def test_next_market_claim_persists_previous_fill_once(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5)
        shard = f.engine._shard('900000002')
        save = shard._save
        writes = []

        def record(state, *args):
            writes.append((state['phase'], tuple(child['state'] for child in state['children'])))
            return save(state, *args)

        with patch.object(shard, '_save', side_effect=record):
            f.engine.arm(f.payload)
            self.until(f, lambda row: row['phase']=='completed')
        first = writes.index(('sending', ('sending',)))
        second = writes.index(('sending', ('confirmed','sending')))
        self.assertEqual(writes[first+1:second], [])

    def test_quote_diagnostics_read_only_and_keeps_previous_native_capture(self):
        f = self.make()
        bridge, native = f.bridges[0], f.natives[0]
        native.info.select = True
        native.info.subscription_delay = False
        native.tick.time = time.time()-2
        bridge.snapshot('USTEC')
        stale_tick = bridge.quote_freshness['USTEC']['lastStale']['nativeTickUtcMs']
        native.tick.time = time.time()
        with patch.object(native, 'symbol_select', side_effect=AssertionError('Changed Market Watch')):
            sample = bridge.quote_diagnostics()['symbols']['USTEC']
        self.assertEqual(sample['previous']['lastStale']['nativeTickUtcMs'], stale_tick)
        self.assertGreater(sample['tickUtcMs'], stale_tick)
        self.assertIs(sample['subscriptionDelayed'], False)
        self.assertIsNone(sample['synchronized'])
        self.assertFalse(native.sent)

    def test_wider_zone_accepts_market_only_on_configured_real_account(self):
        for side, direction in (('buy', 1), ('sell', -1)):
            with self.subTest(side=side):
                f = self.make(side)
                f.primary.config['entryMarketFirst'] = True
                real = f.natives[1]
                real.account.trade_mode = 2
                f.natives[0].account.trade_mode = 0
                f.payload.update(accountZoneRadii={'900000001':8.},chunkPercent=100,chunkVolume=1.)
                for native in f.natives:
                    f.quote(native, f.payload['trigger'] + direction*7.)
                result = f.engine.arm(f.payload)
                self.assertEqual(result['accounts']['900000001']['zoneRadius'], 8.)
                state = self.until(f, lambda row: row['accounts']['900000001']['filledVolume']==1.
                                   and row['accounts']['900000002']['pendingVolume']==1.)
                self.assertEqual(len(f.markets(real)), 1)
                self.assertEqual(state['zoneRadius'], 6.)
                self.assertNotIn('zoneRadius', state['accounts']['900000002'])
                self.assertAlmostEqual(f.natives[0].orders[0].price_open, f.payload['trigger'] + direction*6.)

    def test_wider_fixed_stop_zone_sizes_down_and_uses_its_own_limit_price(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.natives[1].account.trade_mode = 2
        f.payload.update(accountZoneRadii={'900000001':8.},stopMode='level',riskUsd=22.,chunkPercent=50,chunkVolume=.5)
        for native in f.natives:
            native.group_budgets['cid:protection-one'] = (1., 22.)
            f.quote(native, f.payload['trigger']+9.)
        f.engine.arm(f.payload)
        state = self.until(f, lambda row: all(account['pendingVolume']>0 for account in row['accounts'].values())
            and all(any(child['type']=='limit' and child['state']=='pending' and account in child['volumes']
                        for child in row['children']) for account in row['accounts']))
        self.assertAlmostEqual(state['accounts']['900000001']['targetVolume'], .91)
        self.assertAlmostEqual(f.natives[1].orders[0].price_open, f.payload['trigger']+8.)
        self.assertAlmostEqual(state['accounts']['900000002']['targetVolume'], 1.)

    def test_wider_zone_cannot_attach_to_a_demo_terminal_or_cross_take(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.natives[1].account.trade_mode = 0
        for zones in ({'unused':8.}, {'900000001':True}, {'900000001':6.}, {'900000001':8.01}):
            with self.subTest(zones=zones), self.assertRaises(ValueError):
                f.engine.arm(dict(f.payload,accountZoneRadii=zones,tp=f.payload['trigger']+8.))
        self.assertFalse(any(native.sent for native in f.natives))
        f.engine.arm(dict(f.payload,accountZoneRadii={'900000001':8.}))
        state = self.until(f, lambda row: row['lanePhases']['900000001']=='stopped')
        self.assertIn('real MT5 account',state['accounts']['900000001']['entryReason'])
        self.assertFalse(f.natives[1].sent)

    def test_account_market_parts_use_captured_percent_and_native_lot_caps(self):
        for percents, cap, expected in (({'900000002':30, '900000001':100}, 0., [[.3, .3, .3, .1], [1.]]),
                                        ({'900000001':100}, .4, [[.5, .5], [.4, .4, .2]])):
            with self.subTest(percents=percents, cap=cap):
                f = self.make()
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(chunkPercent=50, chunkVolume=.5, accountChunkPercents=percents, accountChunkMaxLot=cap)
                result = f.engine.arm(f.payload)
                self.assertEqual({account:a['chunkPercent'] for account,a in result['accounts'].items() if 'chunkPercent' in a}, percents)
                state = self.until(f, lambda row: row['phase'] == 'completed')
                for native, parts in zip(f.natives, expected):
                    self.assertEqual([row['volume'] for row in f.markets(native)], parts)
                    self.assertEqual(state['accounts'][str(native.account.login)]['filledVolume'], 1.)
                self.assertEqual(state['chunkPercent'], 50)

    def test_unselected_or_invalid_account_part_is_rejected_before_any_opening(self):
        f = self.make()
        for policies in ({'143123':50}, {'900000001':False}, {'900000001':101}):
            with self.subTest(policies=policies), self.assertRaises(ValueError):
                f.engine.arm(dict(f.payload, accountChunkPercents=policies))
        self.assertTrue(all(not native.sent for native in f.natives))

    def test_account_percent_applies_after_follower_balance_sizing(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5, accountChunkPercents={'900000001':100},
                         mirrorAccounts=[dict(account='900000001', multiplier=1, riskPercent=.2)])
        native = f.natives[1]
        native.account.balance = 8000
        native.info.volume_step = native.info.volume_min = .1
        f.engine.arm(f.payload)
        state = self.until(f, lambda row: row['phase'] == 'completed')
        self.assertEqual([row['volume'] for row in f.markets(native)], [1.])
        self.assertEqual(state['accounts']['900000001']['balanceRisk']['riskUsd'], 16.)

    @staticmethod
    def slip_first_markets(f, points):
        for native in f.natives:
            send = native.order_send
            slipped = []

            def send_with_slip(request, native=native, send=send, slipped=slipped):
                result = send(request)
                if request['action'] == native.TRADE_ACTION_DEAL and 'position' not in request and not slipped:
                    slipped.append(result.order)
                    result.price = round(result.price + points, 8)
                    native.positions[-1].price_open = result.price
                    # Only the simulator's assertion needs to allow the realized
                    # slippage. The captured entry budget is intentionally unchanged.
                    target, budget = native.group_budgets[request['comment']]
                    native.group_budgets[request['comment']] = (target, budget + abs(points)*request['volume'])
                return result

            native.order_send = send_with_slip

    def test_two_markets_keep_native_sl_and_defer_weighted_protection_after_a_point_of_slippage(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5, tp=25102.)
        self.slip_first_markets(f, 1.)
        f.engine.arm(f.payload)
        state = self.until(f, lambda row: row['phase'] == 'completed')
        for native in f.natives:
            requests = native.sent
            self.assertEqual([row['action'] for row in requests[:2]], [native.TRADE_ACTION_DEAL]*2)
            self.assertTrue(all(row['sl'] > 0 for row in requests[:2]))
            self.assertTrue(all(row['action'] == native.TRADE_ACTION_SLTP for row in requests[2:]))
            account = str(native.account.login)
            details = state['accounts'][account]
            self.assertEqual(details['entryTiming']['commonStop']['commandCalls'], 1)
            self.assertGreater(requests[0]['volume']*17 + requests[1]['volume']*16, details['riskUsd'])
            self.assertEqual({pos.sl for pos in native.positions}, {24986.6})
            self.assertEqual({pos.tp for pos in native.positions}, {25104.6})

    def test_second_market_refusal_leaves_first_fill_with_finalized_stop(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5, tp=25102.)
        self.slip_first_markets(f, 1.)
        for native in f.natives:
            send = native.order_send

            def refuse_second(request, native=native, send=send):
                if request['action'] == native.TRADE_ACTION_DEAL and len(f.markets(native)) == 1:
                    native.sent.append(request.copy())
                    return SimpleNamespace(retcode=10020, order=0, volume=0., price=0., comment='price changed')
                return send(request)

            native.order_send = refuse_second
        f.engine.arm(f.payload)
        state = self.until(f, lambda row: all(a.get('averagePrice') == 25003. for a in row['accounts'].values()))
        for native in f.natives:
            self.assertEqual([row['action'] for row in native.sent],
                [native.TRADE_ACTION_DEAL, native.TRADE_ACTION_DEAL, native.TRADE_ACTION_SLTP])
            self.assertEqual({pos.sl for pos in native.positions}, {24987.})
        self.assertTrue(all(a['filledVolume'] == .5 for a in state['accounts'].values()))

    def test_limit_transition_and_later_fill_finalize_protection_separately(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5, tp=25102.)
        self.slip_first_markets(f, 1.)
        for native in f.natives:
            native.after_market = lambda broker: f.quote(broker, 25007.)
        f.engine.arm(f.payload)
        self.wait_for_confirmed_limits(f, .5)
        for native, bridge in zip(f.natives, f.bridges):
            self.assertEqual([row['action'] for row in native.sent],
                [native.TRADE_ACTION_DEAL, native.TRADE_ACTION_SLTP, native.TRADE_ACTION_PENDING])
            with bridge.lock:
                order = native.orders.pop()
                native.positions.append(native.position_from(order.ticket, dict(symbol=order.symbol,
                    type=native.ORDER_TYPE_BUY, volume=order.volume_current, price=order.price_open,
                    sl=order.sl, tp=order.tp, comment=order.comment)))
                native.history_orders.append(SimpleNamespace(ticket=order.ticket, symbol=order.symbol,
                    position_id=order.ticket, state=4))
                native.history_deals.append(SimpleNamespace(symbol=order.symbol, position_id=order.ticket,
                    entry=0, volume=order.volume_current, reason=0))
                f.quote(native, 25005.8)
        state = self.until(f, lambda row: row['phase'] == 'completed')
        for native in f.natives:
            detail = state['accounts'][str(native.account.login)]
            self.assertEqual(detail['entryTiming']['commonStop']['commandCalls'], 2)
            self.assertEqual({pos.sl for pos in native.positions}, {24988.5})
            self.assertEqual({pos.tp for pos in native.positions}, {25106.5})

    def test_manual_stop_and_restart_finalize_saved_protection_without_another_opening(self):
        for restart in (False, True):
            with self.subTest(restart=restart):
                f = self.make()
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(chunkPercent=50, chunkVolume=.5, tp=25102.)
                self.slip_first_markets(f, 1.)
                f.engine.arm(f.payload)
                # Choose the exact cycle boundary after first fills. No other
                # trade request is outstanding when the stop or restart runs.
                read_quotes = isolated.SingleAccountLane._parallel_quotes
                def first_round_only(lane, payload, state):
                    if state['accounts'][lane.lane_account]['filledVolume']:
                        return {}, {}, {}
                    return read_quotes(lane, payload, state)
                with patch.object(isolated.SingleAccountLane, '_parallel_quotes', first_round_only):
                    self.until(f, lambda row: all(a['filledVolume'] == .5 for a in row['accounts'].values()))
                    for engine in f.engine.shards.values():
                        engine._drain_lanes(f.payload['id'])
                    if restart:
                        f.engine.close()
                        f.engine = isolated.IsolatedEntries(f.primary)
                        f.primary.chunk_entries = f.engine
                        self.addCleanup(f.engine.close)
                    else:
                        f.engine.stop(dict(id='stop-deferred-protection', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
                state = self.until(f, lambda row: row['phase'] == 'stopped')
                for native in f.natives:
                    self.assertEqual(len(f.markets(native)), 1)
                    self.assertEqual(native.protection_sends, 1)
                    self.assertEqual({pos.sl for pos in native.positions}, {24987.})
                self.assertTrue(all(a['filledVolume'] == .5 for a in state['accounts'].values()))

    def wait_for_confirmed_limits(self, f, volume):
        def confirmed(state):
            owners = {account for child in state['children'] if child['type'] == 'limit' and child['state'] == 'pending'
                      for account in child['volumes']}
            return owners == set(state['accounts']) and all(row['pendingVolume'] == volume for row in state['accounts'].values())

        return self.until(f, confirmed)

    def test_starts_with_market_and_has_no_support_limit_inside(self):
        for side in ('buy', 'sell'):
            with self.subTest(side=side):
                f = self.make(side)
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(chunkPercent=50, chunkVolume=.5)
                self.assertTrue(f.engine.arm(f.payload)['entryMarketFirst'])
                state = self.until(f, lambda row: row['phase'] == 'completed')
                for native in f.natives:
                    openings = [r for r in native.sent if r['action'] in (native.TRADE_ACTION_DEAL, native.TRADE_ACTION_PENDING)]
                    self.assertEqual([(r['action'], r['volume']) for r in openings], [(native.TRADE_ACTION_DEAL, .5)] * 2)
                self.assertTrue(all(row['filledVolume'] == 1 for row in state['accounts'].values()))

    def test_second_market_part_takes_one_rounding_step_without_exceeding_broker_max(self):
        for target, cap, expected in ((14.3, 50., [7.1, 7.2]), (1.1, 50., [.5, .6]), (1.1, .5, [.5, .5, .1])):
            with self.subTest(target=target, cap=cap):
                f = self.make('sell')
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(volume=target, riskUsd=round(target*16, 8), chunkPercent=50, chunkVolume=expected[0])
                for native in f.natives:
                    native.info.volume_step = native.info.volume_min = .1
                    native.info.volume_max = cap
                    native.group_budgets['cid:protection-one'] = (target, f.payload['riskUsd'])
                f.engine.arm(f.payload)
                state = self.until(f, lambda row: row['phase'] == 'completed')
                for native in f.natives:
                    self.assertEqual([row['volume'] for row in f.markets(native)], expected)
                self.assertTrue(all(row['filledVolume'] == target for row in state['accounts'].values()))

    def test_fresh_opening_proof_removes_duplicate_position_read_but_keeps_final_guard(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5)
        f.engine.arm(f.payload)
        lane = f.engine._shard(str(f.natives[0].account.login))
        call = lane._call
        contexts, actions = [], []
        native = f.natives[0]
        send, positions_get = native.order_send, native.positions_get

        def traced_call(account, method, *args, **kwargs):
            if method == 'entry_context':
                contexts.append((len(f.markets(native)), len(args[1])))
            return call(account, method, *args, **kwargs)

        def traced_positions(*args, **kwargs):
            actions.append('positions')
            return positions_get(*args, **kwargs)

        def traced_send(request):
            if request['action'] == native.TRADE_ACTION_DEAL:
                actions.append('market')
            return send(request)

        with patch.object(lane, '_call', traced_call), patch.object(native, 'positions_get', traced_positions), \
                patch.object(native, 'order_send', traced_send):
            self.until(f, lambda row: row['phase'] == 'completed')
        self.assertFalse(any(parts == 1 for parts, _ in contexts))
        first = actions.index('market')
        second = actions.index('market', first+1)
        self.assertIn('positions', actions[first+1:second])

    def test_only_confirmed_remainder_is_posted_at_the_edge(self):
        for side, adverse, edge in (('buy', 25007., 25006.), ('sell', 24993., 24994.)):
            with self.subTest(side=side):
                f = self.make(side)
                f.primary.config['entryMarketFirst'] = True
                f.payload.update(chunkPercent=50, chunkVolume=.5)
                for native in f.natives:
                    native.after_market = lambda mt5: f.quote(mt5, adverse)
                f.engine.arm(f.payload)
                state = self.wait_for_confirmed_limits(f, .5)
                self.assertTrue(all(a['filledVolume'] == .5 for a in state['accounts'].values()))
                for native in f.natives:
                    self.assertEqual([r['volume'] for r in f.markets(native)], [.5])
                    self.assertEqual([(row.price_open, row.volume_current) for row in native.orders], [(edge, .5)])

    def test_account_with_no_market_fill_can_reserve_full_remainder_outside_zone(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload.update(chunkPercent=50, chunkVolume=.5)
        for native in f.natives:
            f.quote(native, 25007.)
        f.engine.arm(f.payload)
        state = self.wait_for_confirmed_limits(f, 1.)
        self.assertTrue(all(a['filledVolume'] == 0 for a in state['accounts'].values()))
        for native in f.natives:
            self.assertFalse(f.markets(native))
            self.assertEqual([(row.price_open, row.volume_current) for row in native.orders], [(25006., 1.)])

    def test_server_policy_is_saved_once_and_ignores_client_override(self):
        f = self.make()
        f.primary.config['entryMarketFirst'] = True
        f.payload['entryMarketFirst'] = False
        self.assertTrue(f.engine.arm(f.payload)['entryMarketFirst'])
        f.primary.config['entryMarketFirst'] = False
        self.assertTrue(f.engine.arm(f.payload)['entryMarketFirst'])
        self.until(f, lambda row: row['phase'] == 'completed')
        for shard in f.engine.shards.values():
            self.assertIs(shard._load(f.payload['id'])[1]['entryMarketFirst'], True)
