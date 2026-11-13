"""Broker simulations only: exact recovery must never replay an opening."""
import copy
import json
import threading
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

import test_hybrid_protection as fixtures
import test_isolated_entry_engines as isolated
from mt5_bridge import Bridge
from chunk_entry import ChunkEntries


class NativeReconciliationTests(unittest.TestCase):
    def fixture(self, side='buy', accounts=2):
        fixture = fixtures.HybridProtectionTests()
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.make(side, accounts)
        return fixture

    def pump(self, fixture, predicate, timeout=3):
        # Executor scheduling varies with host load. Bound wall time rather
        # than assuming a native observer will finish within 100 tiny turns.
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            fixture.engine.reconcile_poll_at.clear()
            state = fixture.tick()
            if predicate(state):
                return state
            threading.Event().wait(.002)
        self.fail(repr(state))

    @staticmethod
    def lose_placement_reply(fixture, kind='limit', no_ticket=False):
        bridge = fixture.primary
        execute = bridge.group_execute
        def lose(operation, payload, **kwargs):
            reply = execute(operation, payload, **kwargs)
            if operation == 'place' and payload['type'] == kind:
                bridge.group_execute = execute
                result = copy.deepcopy(reply['result'])
                result['status'] = 'unknown'
                for account, item in result['accountResults'].items():
                    item['status'] = 'unknown'
                    if no_ticket:
                        item.pop('providerOrderId', None)
                        item.pop('filledVolume', None)
                        item.pop('fillPrice', None)
                        target = next(b for b in fixture.bridges if str(b.config['account']) == account)
                        identifier = payload['id'] if target is bridge else payload['id']+':'+account
                        target.db.execute('UPDATE requests SET result=? WHERE id=?', (json.dumps(item), identifier))
                        target.db.commit()
                reply = dict(reply, result=result)
            return reply
        bridge.group_execute = lose

    def test_late_durable_placement_proof_resumes_both_sides_without_duplicate_volume(self):
        for side in ('buy', 'sell'):
            for kind in ('limit', 'market'):
                with self.subTest(side=side, kind=kind):
                    f = self.fixture(side)
                    self.lose_placement_reply(f, kind)
                    f.engine.arm(f.payload)
                    held = f.tick()
                    self.assertEqual(held['reconciliation']['status'], 'checking')
                    final = self.pump(f, lambda s: s['phase'] in ('completed','stopped'))
                    self.assertEqual(final['phase'], 'completed', final.get('reason'))
                    for native in f.natives:
                        self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
                        self.assertFalse(native.orders)
                        self.assertEqual(len({row.sl for row in native.positions}), 1)
                        self.assertFalse(f.closes(native))

    def test_done_market_with_zero_price_waits_for_exact_broker_position_without_replay(self):
        for delay_proof in (False, True):
            with self.subTest(delay_proof=delay_proof):
                f = self.fixture('sell', accounts=1)
                f.payload.update(volume=.7, riskUsd=12.5, chunkPercent=50.)
                native = f.natives[0]
                native.fill_on_cancel = True
                send = native.order_send
                positions_get = native.positions_get
                hidden = set()

                def done_without_price(request):
                    result = send(request)
                    if request['action'] == native.TRADE_ACTION_DEAL and 'position' not in request:
                        result.price = 0.
                        if delay_proof:
                            hidden.add(result.order)
                    return result

                def positions(*args, **kwargs):
                    return [row for row in positions_get(*args, **kwargs) if row.ticket not in hidden]

                native.order_send = done_without_price
                native.positions_get = positions
                f.engine.arm(f.payload)
                if delay_proof:
                    held = self.pump(f, lambda s: s.get('reconciliation', {}).get('status') == 'checking')
                    self.assertEqual(held['children'][1]['state'], 'unknown')
                    sent = len(f.markets(native))
                    for _ in range(5):
                        f.engine.reconcile_poll_at.clear()
                        f.tick()
                    self.assertEqual(len(f.markets(native)), sent)
                    hidden.clear()
                    native.order_send = send
                final = self.pump(f, lambda s: s['phase'] in ('completed', 'stopped'))
                self.assertEqual(final['phase'], 'completed', final.get('reason'))
                self.assertAlmostEqual(final['accounts']['900000002']['filledVolume'], .7)
                self.assertAlmostEqual(sum(row.volume for row in native.positions), .7)
                self.assertEqual(len(f.markets(native)), 1)
                self.assertEqual(len({row.sl for row in native.positions}), 1)
                market = next(child for child in final['children'] if child['type'] == 'market')
                actual = market['positions']['900000002']
                self.assertEqual(actual['fillPrice'], next(row.price_open for row in native.positions
                                                          if str(row.ticket) == actual['ticket']))

    def test_missing_ticket_and_empty_native_rows_do_not_prove_rejection_or_allow_replay(self):
        f = self.fixture()
        self.lose_placement_reply(f, no_ticket=True)
        f.engine.arm(f.payload)
        held = f.tick()
        counts = [len(native.sent) for native in f.natives]
        for _ in range(20):
            f.engine.reconcile_poll_at.clear()
            held = f.tick()
            threading.Event().wait(.002)
        self.assertEqual(held['reconciliation']['status'], 'checking')
        self.assertEqual([len(native.sent) for native in f.natives], counts)
        self.assertTrue(all(len(native.orders) == 1 for native in f.natives))

    def test_cancel_during_pending_read_is_available_and_never_resumes_entry(self):
        f = self.fixture()
        self.lose_placement_reply(f)
        f.engine.arm(f.payload); f.tick()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        read = f.engine._read_reconciliation
        def delayed(account, items):
            entered.set()
            release.wait(2)
            return read(account, items)
        f.engine._read_reconciliation = delayed
        f.tick()
        self.assertTrue(entered.wait(1))
        start = time.perf_counter()
        f.tick()
        self.assertLess(time.perf_counter()-start, .1)
        f.engine.stop(dict(id='stop-reconcile', entryId=f.payload['id'], expiresAt=f.now*1000+3000))
        release.set()
        final = self.pump(f, lambda s: s['phase'] == 'stopped')
        self.assertFalse(final['reconciliation']['resumeAllowed'])
        self.assertTrue(all(not f.markets(native) and not f.closes(native) for native in f.natives))

    def test_restart_only_reconciles_and_never_restores_permission_to_acquire(self):
        f = self.fixture()
        self.lose_placement_reply(f)
        f.engine.arm(f.payload); f.tick()
        f.engine.close()
        f.engine = f.primary.chunk_entries = ChunkEntries(f.primary)
        state = f.state()
        self.assertEqual(state['phase'], 'paused')
        self.assertFalse(state['reconciliation']['resumeAllowed'])
        self.pump(f, lambda s: s['phase'] == 'stopped')
        self.assertTrue(all(not f.markets(native) and not f.closes(native) for native in f.natives))

    def test_known_frozen_cancel_retries_with_new_id_only_after_new_quote(self):
        f = self.fixture()
        for native in f.natives:
            send = native.order_send
            def freeze(request, native=native, send=send):
                if request['action'] == native.TRADE_ACTION_REMOVE:
                    native.sent.append(copy.deepcopy(request))
                    return N(retcode=10029, order=request['order'], volume=0., price=0., comment='frozen')
                return send(request)
            native.order_send = freeze
        f.engine.arm(f.payload)
        state = f.tick()
        self.assertEqual(state['reconciliation']['status'], 'checking')
        old_ids = copy.deepcopy(f.engine._load(f.payload['id'])[2]['children'][0]['cancelIds'])
        self.pump(f,lambda s:bool(f.engine._load(f.payload['id'])[2]['children'][0].get('frozenCancelQuotes')))
        self.assertEqual(f.state()['reconciliation']['status'], 'checking')
        counts = [len(native.sent) for native in f.natives]
        for native in f.natives:
            native.order_send = lambda request, native=native: super(type(native), native).order_send(request)
            # Keep the quote inside the captured entry zone.
            f.quote(native, 25003.)
        state = self.pump(f, lambda s: s['phase'] == 'completed')
        first = f.engine._load(f.payload['id'])[2]['children'][0]
        self.assertNotEqual(old_ids, first['cancelIds'])
        self.assertTrue(all(len(native.sent) > count for native,count in zip(f.natives,counts)))
        self.assertTrue(all(sum(row.volume for row in native.positions) <= 1.+1e-8 for native in f.natives))

    def test_unknown_cancel_is_not_retried_while_pending_is_still_live(self):
        f = self.fixture()
        for native in f.natives:
            native.cancel_unknown = True
        f.engine.arm(f.payload); f.tick()
        counts = [len(native.sent) for native in f.natives]
        for index in range(20):
            for native in f.natives:
                f.quote(native, 25002.+index*.01)
            f.engine.reconcile_poll_at.clear(); f.tick(); threading.Event().wait(.002)
        self.assertEqual([len(native.sent) for native in f.natives], counts)
        self.assertEqual(f.state()['reconciliation']['status'], 'checking')

    def test_final_native_guard_reprices_crossed_support_without_extra_quote_read(self):
        for side in ('buy','sell'):
            f = self.fixture(side)
            for bridge,native in zip(f.bridges,f.natives):
                guard = bridge.guard_chunk_request
                armed = [True]
                def moving(payload, request, guard=guard, native=native, armed=armed):
                    if payload['type']=='limit' and armed[0]:
                        armed[0]=False
                        f.quote(native,25001. if side=='buy' else 24999.)
                        read_tick=native.symbol_info_tick
                        count=[0]
                        def counted(*args):
                            count[0]+=1
                            return read_tick(*args)
                        native.symbol_info_tick=counted
                        try:
                            result=guard(payload,request)
                            self.assertEqual(count[0],1,'Repricing reuses the mandatory native quote')
                            return result
                        finally:
                            native.symbol_info_tick=read_tick
                    return guard(payload,request)
                bridge.guard_chunk_request=moving
            f.engine.arm(f.payload)
            final=self.pump(f,lambda s:s['phase'] in ('completed','stopped'))
            self.assertEqual(final['phase'],'completed',final.get('reason'))
            for native in f.natives:
                pending=next(row for row in native.sent if row['action']==native.TRADE_ACTION_PENDING)
                self.assertAlmostEqual(pending['price'],25000.8 if side=='buy' else 24999.2)
                self.assertAlmostEqual(sum(row.volume for row in native.positions),1.)
                self.assertEqual(len({row.sl for row in native.positions}),1)

    def test_native_timeout_with_known_opening_is_proved_and_original_deadline_restored_without_replay(self):
        for kind in ('limit','market'):
            with self.subTest(kind=kind):
                f=self.fixture()
                for native in f.natives:
                    send=native.order_send
                    once=[True]
                    def timeout(request,send=send,once=once,native=native):
                        result=send(request)
                        action=native.TRADE_ACTION_PENDING if kind=='limit' else native.TRADE_ACTION_DEAL
                        if once[0] and request['action']==action:
                            once[0]=False
                            result.retcode=native.TRADE_RETCODE_TIMEOUT
                        return result
                    native.order_send=timeout
                f.engine.arm(f.payload)
                self.assertEqual(f.tick()['reconciliation']['status'],'checking')
                final=self.pump(f,lambda s:s['phase']=='completed')
                for native,bridge in zip(f.natives,f.bridges):
                    self.assertAlmostEqual(sum(row.volume for row in native.positions),1.)
                    receipts=[json.loads(row[0]) for row in bridge.db.execute('SELECT result FROM requests')]
                    recovered=[row for row in receipts if row.get('nativeReconciled')]
                    self.assertEqual(len(recovered),1)
                    self.assertEqual(recovered[0]['status'],'ok')
                    self.assertEqual(recovered[0]['nativePreviousResult']['status'],'unknown')
                    self.assertFalse(any(row['state'] in ('unknown','opening') for row in bridge.time_rules()))
                    self.assertEqual(len({row.sl for row in native.positions}),1)
                groups=[json.loads(row[0]) for row in f.primary.db.execute('SELECT result FROM groups')]
                self.assertTrue(all(row['status']=='ok' for row in groups))

    def test_closed_market_before_first_position_read_is_accounted_and_never_reentered(self):
        f=self.fixture(accounts=1)
        native=f.natives[0]
        def exit_immediately(broker):
            position=broker.positions.pop()
            order=broker.history_orders[-1]
            order.type=position.type;order.volume_initial=position.volume;order.sl=position.sl;order.tp=position.tp
            incoming=broker.history_deals[-1]
            incoming.ticket=501;incoming.order=order.ticket;incoming.type=position.type;incoming.price=position.price_open
            broker.history_deals.append(N(ticket=502,order=999,symbol=position.symbol,position_id=position.identifier,
                entry=1,type=1-position.type,volume=position.volume,price=position.sl,reason=4))
        native.after_market=exit_immediately
        f.engine.arm(f.payload)
        final=self.pump(f,lambda s:s['phase']=='stopped')
        self.assertAlmostEqual(final['accounts']['900000002']['filledVolume'],.3)
        self.assertFalse(native.positions);self.assertFalse(native.orders)
        self.assertEqual(len(f.markets(native)),1)
        self.assertTrue(final['children'][1]['closedOpenings'])


class IsolatedNativeRecoveryTests(unittest.TestCase):
    def fixture(self):
        runner = isolated.IsolatedEngineTests()
        self.addCleanup(runner.doCleanups)
        return runner, runner.make()

    def test_lost_exact_price_refusal_resumes_once_with_fresh_price(self):
        runner, f = self.fixture()
        native, bridge = f.natives[0], f.primary
        account = '900000002'
        guard, execute = bridge.guard_chunk_request, bridge.execute
        checks, attempts, lost = [], [], []

        def move_quote(payload, request):
            if payload.get('type') == 'market' and not lost:
                identifier = payload['id']
                self.assertFalse(bridge.db.in_transaction)
                self.assertEqual(bridge._request_receipt(identifier)['status'], 'unknown')
                checks.append(identifier)
                f.quote(native, 25007.)
            return guard(payload, request)

        def drop_refusal(operation, payload, *args, **kwargs):
            if operation == 'place' and payload.get('type') == 'market':
                attempts.append(payload['id'])
            response = execute(operation, payload, *args, **kwargs)
            if payload['id'] in checks and not lost:
                refusal = response['result']
                self.assertEqual(refusal['status'], 'rejected')
                self.assertIs(refusal['finalPreMutationRejection'], True)
                self.assertEqual(refusal['entryDiagnostics']['rejectionStage'], 'native-entry-price-guard')
                self.assertEqual(bridge._request_receipt(payload['id']), refusal)
                lost.append(payload['id'])
                f.quote(native, 25003.)
                raise RuntimeError('Simulated loss of a native price refusal')
            return response

        bridge.guard_chunk_request = move_quote
        bridge.execute = drop_refusal
        f.engine.arm(f.payload)
        held = runner.until(f, lambda state: lost and state['lanePhases'].get(account) == 'paused')
        self.assertEqual(checks, lost)
        self.assertEqual(next(child['state'] for child in held['children'] if child['id'] == lost[0]), 'unknown')
        self.assertFalse(f.markets(native))
        with patch('time.time', return_value=f.now+.35):
            final = runner.until(f, lambda state: state['phase'] == 'completed')
        self.assertEqual(final['accounts'][account]['filledVolume'], 1.)
        rejected = next(child for child in final['children'] if child['id'] == lost[0])
        self.assertEqual((rejected['state'], rejected['result']['status']), ('rejected', 'rejected'))
        self.assertEqual(attempts.count(lost[0]), 1)
        self.assertEqual(f.markets(native)[0]['price'], 25003.)
        self.assertAlmostEqual(sum(row['volume'] for row in f.markets(native)), 1.)
        self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
        f.engine.close()

    def test_ioc_partial_waits_for_terminal_proof_then_acquires_only_residual(self):
        for closed in (False, True):
            with self.subTest(closed=closed):
                runner, f = self.fixture()
                native = f.natives[0]
                native.info.filling_mode = 2
                account = '900000002'
                ticket = []
                proof_ready = threading.Event()
                send, orders = native.order_send, native.history_orders_get

                def partial(request):
                    if request['action'] != native.TRADE_ACTION_DEAL or 'position' in request or ticket:
                        return send(request)
                    result = send(dict(request, volume=.10))
                    native.sent[-1] = copy.deepcopy(request)
                    ticket.append(result.order)
                    order, deal = native.history_orders[-1], native.history_deals[-1]
                    order.state = 2
                    order.type = request['type']
                    order.type_filling = native.ORDER_FILLING_IOC
                    order.volume_initial = request['volume']
                    order.volume_current = round(request['volume']-.1, 8)
                    order.sl, order.tp = request['sl'], request['tp']
                    deal.ticket, deal.order, deal.type, deal.price = 111, result.order, request['type'], request['price']
                    if closed:
                        position = native.positions.pop()
                        native.history_deals.append(N(ticket=112, order=222, symbol=position.symbol, position_id=position.identifier,
                            entry=1, type=1-position.type, volume=.1, price=position.sl, reason=4))
                    result.retcode = 10010
                    return result

                def visible_history(order=None, **kwargs):
                    order = kwargs.get('ticket', order)
                    if ticket and order == ticket[0] and not proof_ready.is_set():
                        return ()
                    return orders(ticket=order)

                native.order_send = partial
                native.history_orders_get = visible_history
                f.engine.arm(f.payload)
                runner.until(f, lambda state: ticket and state['lanePhases'].get(account) == 'paused')
                self.assertEqual(len(f.markets(native)), 1)
                proof_ready.set()
                final = runner.until(f, lambda state: state['phase'] == ('stopped' if closed else 'completed'))
                self.assertEqual(final['accounts'][account]['filledVolume'], .1 if closed else 1.)
                partial_child = next(child for child in final['children'] if child['result'].get('providerOrderId') == str(ticket[0])
                                     or any(row.get('ticket') == str(ticket[0]) for row in child['positions'].values()))
                self.assertEqual(partial_child['positions'][account]['volume'], .1)
                self.assertEqual(bool(partial_child.get('closedOpenings')), closed)
                self.assertFalse(native.orders)
                if closed:
                    self.assertFalse(native.positions)
                    self.assertEqual(len(f.markets(native)), 1)
                else:
                    self.assertAlmostEqual(sum(row.volume for row in native.positions), 1.)
                f.engine.close()


class ClosedOpeningProofTests(unittest.TestCase):
    def sample(self):
        order = N(ticket=71, symbol='USTEC', type=0, volume_initial=.3, sl=24984., tp=0.)
        deals = [N(ticket=81, order=71, symbol='USTEC', position_id=91, entry=0, type=0, volume=.1, price=25000.),
                 N(ticket=82, order=71, symbol='USTEC', position_id=91, entry=0, type=0, volume=.2, price=25003.),
                 N(ticket=83, order=72, symbol='USTEC', position_id=91, entry=1, type=1, volume=.3, price=24984.)]
        return order, deals

    def test_exact_balanced_history_proves_original_volume_and_weighted_entry(self):
        order,deals = self.sample()
        proof = Bridge._closed_opening_proof('71','USTEC','91',deals,order)
        self.assertAlmostEqual(proof['volume'], .3)
        self.assertAlmostEqual(proof['fillPrice'], 25002.)

    def test_foreign_duplicated_unbalanced_or_incomplete_history_cannot_supply_entry_proof(self):
        for field, value in [('ticket',81), ('order',99), ('position_id',99), ('symbol','SPX500'),
                             ('price',float('nan')), ('volume',.4), ('type',1)]:
            with self.subTest(field=field):
                order,deals = self.sample()
                setattr(deals[1],field,value)
                self.assertIsNone(Bridge._closed_opening_proof('71','USTEC','91',deals,order))
        order,deals = self.sample()
        self.assertIsNone(Bridge._closed_opening_proof('71','USTEC','91',deals[:-1],order))
