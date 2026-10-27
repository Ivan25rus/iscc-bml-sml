"""Broker simulations only: exact recovery must never replay an opening."""
import copy
import json
import threading
import time
import unittest
from types import SimpleNamespace as N

import test_hybrid_protection as fixtures
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
