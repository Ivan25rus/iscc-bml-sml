"""Exact parent close recovery using temporary journals and fake MT5."""
import copy
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_entry_independence as independent
import test_hybrid_protection as legacy
from chunk_entry import CLOSE_PROOF_RECHECK_SECONDS, native_parent_close_id


class CloseReconciliationTests(unittest.TestCase):
    def independent_parent(self):
        case = independent.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f, root = case.model()
        f.payload.update(chunkPercent=34, chunkVolume=.34)
        f.engine.arm(f.payload)
        independent.support.IsolatedEngineTests.until(self, f, lambda state: state['phase']=='completed')
        return f, root

    @staticmethod
    def complete_opening_history(native):
        for position in native.positions:
            order = next(row for row in native.history_orders if row.ticket==position.ticket)
            order.type = position.type
            order.volume_initial, order.sl, order.tp = position.volume, position.sl, position.tp
            deal = next(row for row in native.history_deals if row.position_id==position.identifier)
            deal.ticket, deal.order = position.ticket*100+1, order.ticket
            deal.type, deal.price = position.type, position.price_open

    @staticmethod
    def complete_exit_history(native, position, order):
        deal = native.history_deals[-1]
        deal.ticket, deal.order = position.ticket*100+2, order
        deal.type, deal.price = 1-position.type, position.sl

    def delay_close_history(self, native):
        self.complete_opening_history(native)
        send, history = native.order_send, native.history_deals_get
        ready = False

        def close_with_zero_reply_price(request):
            if request['action']!=native.TRADE_ACTION_DEAL or 'position' not in request:
                return send(request)
            position = next(row for row in native.positions if row.ticket==request['position'])
            reply = send(request)
            self.complete_exit_history(native, position, reply.order)
            reply.price = 0
            return reply

        def delayed_history(*args, **kwargs):
            return tuple(row for row in history(*args, **kwargs) if ready or row.entry==0)

        def release():
            nonlocal ready
            ready = True

        native.order_send, native.history_deals_get = close_with_zero_reply_price, delayed_history
        return release

    def stale_position_after_close(self, native, *, change_reply=None, hide_history=False, pending_ticket=None,
                                   stale_snapshots=1):
        release = self.delay_close_history(native)
        if not hide_history:
            release()
        previous = copy.deepcopy(native.positions if hide_history else native.positions[-1:])
        send, positions = native.order_send, native.positions_get
        closes, stale_reads = [], []

        def close_with_stale_snapshot(request):
            reply = send(request)
            if request['action'] == native.TRADE_ACTION_DEAL and 'position' in request:
                reply.order = 90000+len(closes)
                native.history_deals[-1].order = reply.order
                closes.append(request['position'])
                if change_reply is not None:
                    change_reply(reply)
                if pending_ticket is not None and len(closes)==3:
                    position = previous[-1]
                    native.orders.append(SimpleNamespace(ticket=pending_ticket,identifier=pending_ticket,
                        symbol=position.symbol,type=native.ORDER_TYPE_BUY_LIMIT,volume_current=.1,
                        price_open=position.price_open,sl=position.sl,tp=position.tp,position_id=0,comment=position.comment))
            return reply

        def positions_get(*args, **kwargs):
            if not args and not kwargs and len(closes)==3 and len(stale_reads)<stale_snapshots*len(previous):
                stale_reads.extend(row.ticket for row in previous)
                return tuple(previous)
            return positions(*args, **kwargs)

        native.order_send, native.positions_get = close_with_stale_snapshot, positions_get
        return closes, stale_reads

    def test_three_done_closes_survive_multiple_stale_native_position_snapshots_without_resending(self):
        f, root = self.independent_parent()
        account = '900000001'
        closes, stale_reads = self.stale_position_after_close(f.natives[1],stale_snapshots=2)
        command = f.close_command(id='stale-open-close')
        shard = f.engine.shards[account]
        with patch.object(shard,'_owned_close_view',wraps=shard._owned_close_view) as view:
            response = f.engine.close_position(command)
        result, entry = response['result'], response['entry']
        self.assertEqual(result['status'], 'ok', result)
        self.assertEqual([(row['status'],row['retcode'],row['fillPrice']) for row in result['accountResults'][account]['positions']],
                         [('ok',10009,0)]*3)
        self.assertEqual(stale_reads, [closes[-1]]*2)
        self.assertEqual(len(set(closes)), 3)
        self.assertEqual([call.kwargs.get('exact_exit',False) for call in view.call_args_list], [False,False,True,True,False])
        self.assertTrue(entry['positionsClosed'])
        self.assertNotIn('closurePending', entry['accounts'][account])
        counts = [len(native.sent) for native in f.natives]
        self.assertEqual(root.receipt(command['id'])['result'], result)
        self.assertEqual(f.engine.close_position(command)['result'], result)
        self.assertEqual([len(native.sent) for native in f.natives], counts)

    def test_stale_open_snapshot_cannot_be_rechecked_after_inexact_native_close_results(self):
        for changed in (dict(retcode=10025),dict(order=0),dict(order=90000),dict(volume=0.01)):
            with self.subTest(changed=changed):
                f, _ = self.independent_parent()
                account = '900000001'
                closes, stale_reads = self.stale_position_after_close(f.natives[1],
                    change_reply=lambda reply: vars(reply).update(changed))
                shard = f.engine.shards[account]
                with patch.object(shard,'_owned_close_view',wraps=shard._owned_close_view) as view:
                    result = f.engine.close_position(f.close_command(id='inexact-native-close'))['result']
                self.assertEqual(result['accountResults'][account]['status'], 'unknown', result)
                self.assertEqual(stale_reads, [closes[-1]])
                self.assertEqual(len(set(closes)), 3)
                self.assertEqual([call.kwargs.get('exact_exit',False) for call in view.call_args_list], [False,False])

    def test_stale_open_handoff_requires_exact_history_and_no_owned_pending(self):
        for scenario,reason in (('history','no definite broker full-exit proof'),('pending','Owned pending orders are not settled')):
            with self.subTest(scenario=scenario):
                f, _ = self.independent_parent()
                account = '900000001'
                shard = f.engine.shards[account]
                cancelled = next(child for child in shard._load(f.payload['id'])[2]['children'] if child['state']=='cancelled')
                closes, stale_reads = self.stale_position_after_close(f.natives[1],
                    hide_history=scenario=='history',pending_ticket=int(cancelled['result']['providerOrderId']) if scenario=='pending' else None)
                with patch.object(shard,'_owned_close_view',wraps=shard._owned_close_view) as view:
                    response = f.engine.close_position(f.close_command(id='stale-open-unproven-exit'))
                result, entry = response['result'], response['entry']
                self.assertEqual(result['accountResults'][account]['status'], 'unknown', result)
                self.assertIn(reason, result['accountResults'][account]['reason'])
                self.assertIn('closurePending', entry['accounts'][account])
                self.assertTrue(stale_reads)
                self.assertEqual(len(set(closes)), 3)
                rechecks = len(CLOSE_PROOF_RECHECK_SECONDS) if scenario=='history' else 1
                self.assertEqual([call.kwargs.get('exact_exit',False) for call in view.call_args_list], [False,False]+[True]*rechecks)

    def test_absent_position_can_gain_exact_exit_history_during_bounded_recheck(self):
        f, _ = self.independent_parent()
        native, account = f.natives[1], '900000001'
        release = self.delay_close_history(native)
        native_send = native.order_send
        closes = []

        def send_with_distinct_close_orders(request):
            reply = native_send(request)
            if request['action']==native.TRADE_ACTION_DEAL and 'position' in request:
                closes.append(request['position'])
                reply.order = 90000+len(closes)
                native.history_deals[-1].order = reply.order
            return reply

        native.order_send = send_with_distinct_close_orders
        shard = f.engine.shards[account]
        original = shard._call
        watches = []

        def read_after_close(account, method, *args):
            if method=='entry_close_reconciliation':
                watches.append(args[1])
                if len(watches)==2:
                    release()
            return original(account,method,*args)

        with patch.object(shard,'_call',side_effect=read_after_close):
            result = f.engine.close_position(f.close_command(id='history-arrives-during-close'))
        self.assertEqual(result['result']['status'],'ok',result)
        self.assertTrue(result['entry']['positionsClosed'])
        self.assertEqual(len(watches),2)
        self.assertEqual(len(set(closes)),3)
        for watch in watches:
            self.assertEqual({(row['account'],row['positionTicket']) for row in watch},
                             {(account,str(ticket)) for ticket in closes})

    def test_three_native_successes_with_zero_price_wait_for_exact_delayed_exit_history(self):
        f, root = self.independent_parent()
        native, account = f.natives[1], '900000001'
        release = self.delay_close_history(native)
        command = f.close_command(id='delayed-close')
        response = f.engine.close_position(command)
        receipt, state = response['result'], response['entry']
        affected = receipt['accountResults'][account]
        self.assertEqual(receipt['accountResults']['900000002']['status'], 'ok')
        self.assertEqual(affected['status'], 'unknown')
        self.assertIn('no definite broker full-exit proof', affected['reason'])
        self.assertEqual([(row['status'],row['retcode'],row['fillPrice']) for row in affected['positions']],
                         [('ok',10009,0)]*3)
        self.assertEqual(state['accounts'][account]['closurePending'], affected['controlId'])
        self.assertFalse(native.positions or native.orders)
        counts = [len(terminal.sent) for terminal in f.natives]

        release()
        closed = f.engine.get(command['entryId'])
        self.assertEqual(closed['positionLifecycle'], 'closed')
        self.assertFalse(closed['hasOpenPositions'])
        self.assertNotIn('closurePending', closed['accounts'][account])
        self.assertEqual(root.receipt(command['id'])['result'], receipt)
        self.assertEqual(f.engine.close_position(command)['result'], receipt)
        self.assertEqual([len(terminal.sent) for terminal in f.natives], counts)

    def test_own_native_unknown_can_prove_full_exits_without_weakening_other_guards(self):
        f, root = self.independent_parent()
        native, bridge, account = f.natives[1], f.bridges[1], '900000001'
        self.complete_opening_history(native)
        captures = {str(row.ticket):row for row in native.positions}
        native.close_unknown = native.close_apply_then_unknown = True
        command = f.close_command(id='native-unknown-close')
        result = f.engine.close_position(command)['result']
        lane = result['accountResults'][account]
        self.assertEqual(lane['status'], 'unknown')
        self.assertEqual(len(lane['positions']), 1)
        ticket = lane['positions'][0]['ticket']
        self.assertEqual(lane['positions'][0]['status'], 'unknown')
        close_id = native_parent_close_id(lane['controlId'], account, ticket)
        native_receipt = bridge._request_receipt(close_id)
        self.assertEqual(native_receipt['status'], 'unknown')
        self.complete_exit_history(native, captures[ticket], 91001)
        self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])
        for index, position in enumerate(list(native.positions), 2):
            f.native_exit(native, position, position.volume)
            self.complete_exit_history(native, position, 91000+index)
        counts = [len(terminal.sent) for terminal in f.natives]
        watch = [dict(account=account,ticket=ticket,symbol=command['symbol'])]
        self.assertEqual(bridge.entry_management(watch)['openingStates'][ticket]['phase'], 'unknown')
        with self.assertRaisesRegex(ValueError, 'Unconfirmed management command'):
            bridge.check_rule_unknown(dict(id='new-close',ticket=ticket,symbol=command['symbol']))

        shard = f.engine.shards[account]
        original_call = shard._call

        def mismatched_view(field, value):
            def call(account, method, *args):
                view = original_call(account, method, *args)
                if method == 'entry_close_reconciliation':
                    view = copy.deepcopy(view)
                    if field=='account':
                        view['account'] = value
                    else:
                        view['openingStates'][ticket]['openingProof'][field] = value
                return view
            return call

        for field,value in [('account','900000004'),('openingTicket','999'),('positionId','999'),
                            ('symbol','SPX500'),('side','sell'),('volume',.01),('fillPrice',1.)]:
            with self.subTest(field=field), patch.object(shard,'_call',side_effect=mismatched_view(field,value)):
                self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])

        pending = SimpleNamespace(ticket=91919,identifier=91919,symbol=command['symbol'],type=native.ORDER_TYPE_BUY_LIMIT,
            volume_current=.1,price_open=25000.,sl=24984.,tp=0.,position_id=0,comment='unsettled')
        native.orders.append(pending)
        self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])
        native.orders.clear()

        with bridge.lock:
            bridge.db.execute('INSERT INTO requests VALUES (?,?,?)',('foreign','digest',json.dumps(dict(status='unknown',operation='modify',ticket='999'))))
            bridge.db.execute('INSERT INTO request_targets VALUES (?,?,?)',('foreign','999',command['symbol']))
            bridge.db.commit()
        self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])
        with bridge.lock:
            bridge.db.execute('DELETE FROM requests WHERE id=?',('foreign',))
            bridge.db.execute('DELETE FROM request_targets WHERE id=?',('foreign',))
            bridge.db.execute('UPDATE request_targets SET ticket=? WHERE id=?',('999',close_id))
            bridge.db.commit()
        self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])
        with bridge.lock:
            bridge.db.execute('UPDATE request_targets SET ticket=?,symbol=? WHERE id=?',(ticket,'SPX500',close_id))
            bridge.db.commit()
        self.assertIn('closurePending', f.engine.get(command['entryId'])['accounts'][account])
        with bridge.lock:
            bridge.db.execute('UPDATE request_targets SET symbol=? WHERE id=?',(command['symbol'],close_id))
            bridge.db.commit()

        self.assertEqual(f.engine.get(command['entryId'])['positionLifecycle'], 'closed')
        self.assertEqual(root.receipt(command['id'])['result'], result)
        self.assertEqual(bridge._request_receipt(close_id), native_receipt)
        self.assertEqual(bridge.entry_management(watch)['openingStates'][ticket]['phase'], 'unknown')
        self.assertEqual([len(terminal.sent) for terminal in f.natives], counts)

    def test_legacy_get_confirms_full_exit_of_an_underfilled_parent(self):
        f = legacy.HybridProtectionTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        f.make()
        state = f.entry(active=True)
        account = '900000002'
        self.assertLess(state['accounts'][account]['filledVolume'], state['accounts'][account]['targetVolume'])
        native = f.natives[0]
        release = self.delay_close_history(native)
        command = f.close_command(id='legacy-underfilled-close')
        response = f.engine.close_position(command)
        self.assertEqual(response['result']['status'], 'unknown')
        self.assertFalse(native.positions or native.orders)
        count = len(native.sent)

        release()
        closed = f.engine.get(command['entryId'])
        self.assertEqual(closed['positionLifecycle'], 'closed')
        self.assertNotIn('closurePending', closed)
        self.assertEqual(f.engine._closure_receipt(command['id'])[1], response['result'])
        self.assertEqual(len(native.sent), count)


if __name__=='__main__':
    unittest.main()
