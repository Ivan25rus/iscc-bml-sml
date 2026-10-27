"""Native bridge guards for hybrid entries; no terminal or network required."""
import copy
import http.client
import json
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_mt5_bridge import FakeMT5
from mt5_bridge import Bridge, create_server


class CommonStopMT5(FakeMT5):
    def __init__(self):
        super().__init__()
        self.account.margin_mode = 2
        self.orders = []
        self.tick = N(bid=25006., ask=25006.2, time=time.time())
        self.positions = [self.position(71, 25000., 24984.), self.position(72, 25004., 24988.)]
        self.risks = []

    @staticmethod
    def position(ticket, price, sl, volume=1., side=0):
        return N(ticket=ticket, symbol='USTEC', type=side, volume=volume,
                 price_open=price, sl=sl, tp=0., price_current=25006., profit=0., comment='cid:hybrid')

    def order_calc_profit(self, kind, symbol, volume, opened, closed):
        return (closed-opened) * volume * (1 if kind == self.ORDER_TYPE_BUY else -1)

    def order_send(self, request):
        self.sent.append(dict(request))
        if request['action'] == self.TRADE_ACTION_SLTP:
            row = next(x for x in self.positions if x.ticket == request['position'])
            row.sl, row.tp = request['sl'], request['tp']
            self.risks.append(sum(max(0., -self.order_calc_profit(x.type, x.symbol, x.volume, x.price_open, x.sl)) for x in self.positions))
        return N(retcode=self.TRADE_RETCODE_DONE, order=request.get('position',42), volume=request.get('volume',0.),
                 price=request.get('price',0.), comment='done')


class HybridBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.mt5 = CommonStopMT5()
        self.bridge = Bridge(self.mt5, dict(account=900000002,server='Example-MT5',symbols=['USTEC'],tradingEnabled=True),
                             pathlib.Path(self.temp.name)/'db.sqlite')
        self.addCleanup(self.bridge.db.close)

    def payload(self, **extra):
        return dict(id='common-1', symbol='USTEC', side='buy', riskBudget=32., stopDistance=16., maxVolume=2.,
                    positions=[dict(ticket=str(x.ticket),volume=x.volume,fillPrice=x.price_open,sl=x.sl,tp=x.tp)
                               for x in self.mt5.positions], **extra)

    def test_weighted_stop_is_confirmed_and_tightening_precedes_widening(self):
        # Reverse the input order: widening must still happen second.
        payload = self.payload(); payload['positions'].reverse()
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual((result['account'],result['averagePrice'],result['sl'],result['riskUsed']),
                         ('900000002',25002.,24986.,32.))
        self.assertEqual([x['position'] for x in self.mt5.sent], [71,72])
        self.assertTrue(all(x <= 32. for x in self.mt5.risks))
        self.assertTrue(all(x['sl'] == 24986. for x in result['positions']))
        self.assertEqual(self.bridge.entry_common_stop(payload), result)
        self.assertEqual(len(self.mt5.sent), 2)
        self.assertEqual(self.bridge.db.execute('SELECT COUNT(*) FROM entry_common_stop_targets').fetchone()[0], 0)

    def test_weighting_and_buy_tick_rounding_never_add_risk(self):
        self.mt5.positions[0].volume = .3
        self.mt5.positions[1].volume = .7
        self.mt5.positions[1].price_open = 25000.1
        self.mt5.positions[1].sl = 24984.1
        payload = self.payload(); payload['riskBudget'] = 16.
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'], 'ok')
        self.assertAlmostEqual(result['averagePrice'], 25000.07)
        self.assertEqual(result['sl'], 24984.1)
        self.assertLessEqual(result['riskUsed'], 16.)

    def test_sell_tick_rounding_and_change_order_are_protective(self):
        self.mt5.positions = [self.mt5.position(71,25000.,25016.,.3,1),self.mt5.position(72,25000.1,25016.1,.7,1)]
        self.mt5.tick.bid, self.mt5.tick.ask = 24998.,24998.2
        payload = self.payload(); payload.update(side='sell', riskBudget=16.)
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['sl'], 25016.)
        self.assertLessEqual(result['riskUsed'], 16.)

    def test_absolute_level_stop_and_unchanged_protection_need_no_send(self):
        payload = self.payload(); payload.pop('stopDistance'); payload['sl'] = 24988.
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'], 'ok'); self.assertEqual(result['sl'],24988.)
        self.assertEqual(len(self.mt5.sent),1)

    def test_changed_or_closed_position_rejects_before_any_send(self):
        payload = self.payload(); self.mt5.positions[1].volume = .8
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'],'rejected'); self.assertEqual(self.mt5.sent,[])

    def test_risk_volume_distance_and_stale_quote_fail_before_changes(self):
        for key,value in (('riskBudget',31.),('maxVolume',1.9),('stopDistance',0.)):
            payload = self.payload(); payload[key] = value; payload['id'] += key
            self.assertEqual(self.bridge.entry_common_stop(payload)['status'],'rejected')
        self.mt5.tick.time = time.time()-2
        self.assertEqual(self.bridge.entry_common_stop(self.payload())['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_broker_stop_and_freeze_distances_are_checked_before_mutation(self):
        self.mt5.info.trade_stops_level = 2100
        self.assertEqual(self.bridge.entry_common_stop(self.payload())['status'],'rejected')
        self.mt5.info.trade_stops_level = 10
        self.mt5.info.trade_freeze_level = 2300
        payload = self.payload(); payload['id'] = 'freeze-check'
        self.assertEqual(self.bridge.entry_common_stop(payload)['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_reported_success_without_broker_change_is_unknown_and_never_retried(self):
        self.mt5.order_send = lambda request: N(retcode=self.mt5.TRADE_RETCODE_DONE,order=request['position'],volume=0.,price=0.,comment='done')
        payload = self.payload()
        result = self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(self.bridge.entry_common_stop(payload),result)
        later = copy.deepcopy(payload); later['id'] = 'different-command'
        self.assertEqual(self.bridge.entry_common_stop(later)['status'],'rejected')

    def test_position_race_between_preflight_and_modify_fails_closed(self):
        original = self.mt5.positions_get; reads=[]
        def racing(ticket=None):
            reads.append(ticket)
            if ticket == 71:
                self.mt5.positions[0].volume = .5
            return original(ticket)
        self.mt5.positions_get = racing
        result = self.bridge.entry_common_stop(self.payload())
        self.assertEqual(result['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_unknown_modify_receipt_blocks_another_stop_attempt(self):
        self.mt5.order_send = lambda request: None
        payload = self.payload()
        self.assertEqual(self.bridge.entry_common_stop(payload)['status'],'unknown')
        self.assertEqual(self.bridge.db.execute('SELECT COUNT(*) FROM entry_common_stop_targets').fetchone()[0], 2)
        payload['id'] = 'different-command'
        self.assertEqual(self.bridge.entry_common_stop(payload)['status'],'rejected')

    def test_completed_history_does_not_add_json_parsing_to_each_new_stop(self):
        historical = json.dumps(dict(status='ok', symbol='USTEC', positions=[dict(ticket='71')], note='historical'*100))
        self.bridge.db.executemany('INSERT INTO entry_common_stops VALUES (?,?,?)',
            [('history-'+str(i), 'fixture', historical) for i in range(2000)])
        self.bridge.db.commit()
        with patch('mt5_bridge.json.loads', wraps=json.loads) as loads:
            result = self.bridge.entry_common_stop(self.payload())
        self.assertEqual(result['status'], 'ok')
        self.assertLess(loads.call_count, 20, 'Historical stop JSON must never be parsed in the entry hot path')
        plan = self.bridge.db.execute('EXPLAIN QUERY PLAN SELECT 1 FROM entry_common_stop_targets WHERE symbol=? AND ticket IN (?,?) LIMIT 1',
                                      ('USTEC','71','72')).fetchall()
        self.assertTrue(any('entry_common_stop_target_lookup' in row[-1] for row in plan))

    def test_legacy_unknown_stop_targets_are_backfilled_once_before_entry_processing(self):
        self.bridge.db.execute('INSERT INTO entry_common_stops VALUES (?,?,?)',
            ('legacy-unknown','fixture',json.dumps(dict(status='unknown',symbol='USTEC',positions=[dict(ticket='71')]))))
        self.bridge.db.execute('DELETE FROM entry_common_stop_migrations WHERE id=?', ('targets-v1',))
        self.bridge.db.commit()
        restored = Bridge(self.mt5, dict(self.bridge.config), pathlib.Path(self.temp.name)/'db.sqlite')
        self.addCleanup(restored.db.close)
        self.assertEqual(restored.db.execute('SELECT command_id,symbol,ticket FROM entry_common_stop_targets').fetchall(),
                         [('legacy-unknown','USTEC','71')])
        self.assertEqual(restored.entry_common_stop(self.payload())['status'],'rejected')
        self.assertEqual(self.mt5.sent, [])
        # A subsequent startup must use the completed migration marker rather
        # than decoding the growing historical result ledger again.
        with patch('mt5_bridge.json.loads', wraps=json.loads) as loads:
            again = Bridge(self.mt5, dict(self.bridge.config), pathlib.Path(self.temp.name)/'db.sqlite')
        self.addCleanup(again.db.close)
        self.assertEqual(loads.call_count, 0)
        payload = self.payload(); payload['id'] = 'after-restart'
        self.assertEqual(again.entry_common_stop(payload)['status'],'rejected')

    def support(self, side='buy', **extra):
        buy = side == 'buy'
        return dict(id='support-'+side,symbol='USTEC',side=side,type='limit',volume=.3,price=25000.,
                    sl=24984. if buy else 25016.,tp=0.,expiresAt=time.time()*1000+3000,
                    entryGuard=dict(anchor=25000.,maxGap=6.,maxRiskPrice=25006. if buy else 24994.,positions=[],
                                    priceFloor=25000. if buy else 24994.,priceCeiling=25006. if buy else 25000.,
                                    limitPolicy='support',limitPrice=25002. if buy else 24998.,**extra))

    def test_support_limit_uses_per_account_pinned_price_and_survives_small_quote_drift(self):
        self.mt5.tick.bid,self.mt5.tick.ask = 25003.,25003.2
        result = self.bridge.execute('place',self.support())['result']
        self.assertEqual(result['status'],'ok')
        self.assertEqual(self.mt5.sent[-1]['price'],25002.)

    def test_support_limits_reject_outside_zone_and_wrong_side(self):
        self.mt5.tick.bid,self.mt5.tick.ask = 25000.,25000.2
        for price in (24999.9,25000.3):
            payload = self.support(); payload['id'] += str(price); payload['entryGuard']['limitPrice'] = price
            self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_clamped_support_at_level_is_valid_only_with_broker_distance(self):
        self.mt5.tick.bid,self.mt5.tick.ask = 24999.9,25000.2
        payload = self.support(); payload['entryGuard']['limitPrice'] = 25000.
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'ok')
        self.mt5.tick.ask = 25000.
        payload['id'] = 'at-ask'
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')

    def test_support_policy_cannot_override_market_or_outer_limit_price(self):
        payload=self.support();payload['type']='market'
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        payload=self.support();payload['entryGuard'].pop('limitPolicy')
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_hybrid_capability_is_advertised(self):
        self.assertIn('hybrid-market-limit-v1',self.bridge._health(self.mt5.account)['capabilities'])

    def test_common_stop_accepts_two_hundred_exact_positions_and_rejects_more(self):
        self.mt5.positions = [self.mt5.position(ticket,25000.,24984.,.1) for ticket in range(1,201)]
        payload=self.payload();payload.update(riskBudget=320.,maxVolume=20.)
        result=self.bridge.entry_common_stop(payload)
        self.assertEqual(result['status'],'ok');self.assertEqual(len(result['positions']),200)
        self.assertAlmostEqual(result['filledVolume'],20.)
        self.mt5.positions.append(self.mt5.position(201,25000.,24984.,.1))
        payload=self.payload();payload.update(id='too-many-owned',riskBudget=400.,maxVolume=25.)
        self.assertEqual(self.bridge.entry_common_stop(payload)['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_market_guard_checks_up_to_two_hundred_prior_positions_without_sending(self):
        self.mt5.positions = [self.mt5.position(ticket,25000.,24984.,.1) for ticket in range(1,201)]
        expected=[dict(ticket=str(row.ticket),volume=row.volume,sl=row.sl,tp=row.tp) for row in self.mt5.positions]
        payload=dict(symbol='USTEC',side='buy',type='market',volume=.1,price=25006.2,sl=24990.2,tp=0.,
                     entryGuard=dict(anchor=25006.2,maxGap=1.,maxRiskPrice=25007.,positions=expected))
        self.assertEqual(self.bridge.open_request(payload)['price'],25006.2)
        expected.append(dict(ticket='201',volume=.1,sl=24984.,tp=0.))
        with self.assertRaisesRegex(ValueError,'previous chunk positions'):
            self.bridge.open_request(payload)
        self.assertEqual(self.mt5.sent,[])


class HybridManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.terminals, self.bridges = [], []
        for account,ticket in ((900000002,71),(900000001,91)):
            terminal = CommonStopMT5(); terminal.account.login = account
            terminal.positions = [terminal.position(ticket,25000.,24984.,.2)]
            bridge = Bridge(terminal,dict(account=account,server='Example-MT5',symbols=['USTEC'],tradingEnabled=True),
                            pathlib.Path(self.temp.name)/(str(account)+'.sqlite'))
            self.terminals.append(terminal); self.bridges.append(bridge)
            self.addCleanup(bridge.db.close)
        self.main,self.linked = self.bridges
        linked = self.linked
        class Worker:
            def call(self,method,*args): return getattr(linked,method)(*args)
            def available(self): return True
        self.main.linked['900000001'] = Worker()
        self.addCleanup(self.main.close_group_executor)
        requests = {account:dict(id='settled-hybrid:h1'+('' if account=='900000002' else ':'+account),
                                symbol='USTEC',side='buy',type='limit',volume=.7,price=25000.,sl=24984.,tp=0.)
                    for account in ('900000002','900000001')}
        self.opening = dict(operation='place',payload={**requests['900000002'],'mirrorAccounts':[dict(account='900000001',multiplier=1.)]},requests=requests)
        self.opened = self.main.group_result({account:dict(status='ok',providerOrderId=ticket,requestedVolume=.7)
                                             for account,ticket in (('900000002','71'),('900000001','91'))})
        self.main.db.execute('INSERT INTO groups VALUES (?,?,?,?)',
                             ('settled-hybrid:h1','fixture',json.dumps(self.opening),json.dumps(self.opened)))
        policy = 'hybrid-market-limit-v1'
        self.parent = dict(id='settled-hybrid',symbol='USTEC',side='buy',executionPolicy=policy,phase='completed',
                           accounts={'900000002':{},'900000001':{}},children=[dict(id='settled-hybrid:h1',type='limit',state='confirmed',
                           volumes={'900000002':.7,'900000001':.7},result=self.opened,
                           cancellations={'900000002':dict(status='ok'),'900000001':dict(status='ok')},
                           positions={account:dict(ticket=ticket,openingTicket=ticket,volume=.2,fillPrice=25000.,sl=24984.,tp=0.)
                                      for account,ticket in (('900000002','71'),('900000001','91'))})])
        plan = dict(id='settled-hybrid',symbol='USTEC',side='buy',executionPolicy=policy)
        self.main.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',
                             ('settled-hybrid','fixture',json.dumps(plan),json.dumps(self.parent)))
        self.main.db.commit()

    def save_parent(self):
        self.main.db.execute('UPDATE chunk_entries SET state=? WHERE id=?',(json.dumps(self.parent),'settled-hybrid'))
        self.main.db.commit()

    def close(self, identifier='close-settled-hybrid', amount=.2, expected=.2):
        return self.main.group_execute('close',dict(id=identifier,symbol='USTEC',ticket='71',volume=amount,expectedVolume=expected,
                                                  mirrorAccounts=[dict(account='900000001')],expiresAt=time.time()*1000+2000))['result']

    def test_owned_partial_limit_fills_can_be_managed_and_closed_on_both_accounts(self):
        state = self.main.management()
        self.assertFalse(any('Объёмы' in row.get('reason','') for row in state['alerts']))
        result = self.close()
        self.assertEqual(result['status'],'ok')
        self.assertEqual([terminal.sent[-1]['position'] for terminal in self.terminals],[71,91])
        self.assertTrue(all(terminal.sent[-1]['volume']==.2 for terminal in self.terminals))

    def test_confirmed_group_partial_close_is_applied_after_the_captured_partial_fill(self):
        self.assertEqual(self.close('first-half',.1,.2)['status'],'ok')
        for terminal in self.terminals: terminal.positions[0].volume = .1
        self.assertEqual(self.close('second-half',.1,.1)['status'],'ok')
        self.assertTrue(all(len(terminal.sent)==2 for terminal in self.terminals))

    def test_external_volume_change_is_not_adopted_as_the_new_expected_volume(self):
        self.terminals[0].positions[0].volume = .3
        self.assertTrue(any('Объёмы' in row.get('reason','') for row in self.main.management()['alerts']))
        with self.assertRaisesRegex(ValueError,'volume differs'):
            self.close(amount=.3,expected=.3)
        self.assertTrue(all(not terminal.sent for terminal in self.terminals))

    def test_unconfirmed_cancel_and_different_opening_or_position_identity_cannot_authorize_management(self):
        child = self.parent['children'][0]
        original = copy.deepcopy(child)
        for mutation in ('unknown-cancel','different-opening','different-position','pending-child'):
            self.parent['children'][0] = copy.deepcopy(original); child = self.parent['children'][0]
            if mutation=='unknown-cancel': child['cancellations']['900000002']['status']='unknown'
            elif mutation=='different-opening': child['positions']['900000002']['openingTicket']='999'
            elif mutation=='different-position': child['positions']['900000002']['ticket']='999'
            else: child['state']='pending'
            self.save_parent()
            with self.assertRaisesRegex(ValueError,'volume differs'):
                self.close(identifier='blocked-'+mutation)
        self.assertTrue(all(not terminal.sent for terminal in self.terminals))


class HybridExactPlanHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.mt5=CommonStopMT5()
        self.bridge=Bridge(self.mt5,dict(account=900000002,server='Example-MT5',symbols=['USTEC'],tradingEnabled=True,token='fixture-token'),
                           pathlib.Path(self.temp.name)/'db.sqlite')
        self.addCleanup(self.bridge.db.close)
        state=dict(id='exact-owned-parent',symbol='USTEC',side='buy',phase='stopped',accounts={},children=[])
        self.bridge.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',('exact-owned-parent','fixture','{}',json.dumps(state)))
        self.bridge.db.commit()
        self.server=create_server(self.bridge,('127.0.0.1',0))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.addCleanup(self.stop_server)

    def stop_server(self):
        self.server.shutdown();self.server.server_close();self.thread.join(3)

    def get(self,path,authenticated=True):
        connection=http.client.HTTPConnection('127.0.0.1',self.server.server_port,timeout=3)
        try:
            headers={'Authorization':'Bearer fixture-token'} if authenticated else {}
            connection.request('GET',path,headers=headers)
            response=connection.getresponse()
            return response.status,json.loads(response.read())
        finally: connection.close()

    def test_exact_parent_get_is_authenticated_read_only_and_never_arms_or_submits(self):
        before=list(self.bridge.db.iterdump())
        status,_=self.get('/entry-plan?id=exact-owned-parent',authenticated=False)
        self.assertEqual(status,401)
        status,result=self.get('/entry-plan?id=exact-owned-parent')
        self.assertEqual(status,200);self.assertEqual(result['entry']['id'],'exact-owned-parent')
        self.assertEqual(list(self.bridge.db.iterdump()),before)
        self.assertEqual(self.mt5.sent,[])

    def test_missing_parent_returns_explicit_null_and_bad_ids_or_duplicate_keys_fail(self):
        status,result=self.get('/entry-plan?id=not-present')
        self.assertEqual(status,200);self.assertIsNone(result['entry'])
        for path in ('/entry-plan','/entry-plan?id=','/entry-plan?id=one&id=two','/entry-plan?id=one&extra=two','/entry-plan?id=bad%2Fid'):
            status,_=self.get(path)
            self.assertEqual(status,400)
        self.assertEqual(self.mt5.sent,[])

    def test_linked_account_route_cannot_read_or_start_primary_parent(self):
        class NoRPC:
            def call(self,*args):raise AssertionError('Exact parent lookup must never dispatch a linked RPC')
        self.bridge.linked['900000001']=NoRPC()
        status,_=self.get('/accounts/900000001/entry-plan?id=exact-owned-parent')
        self.assertEqual(status,400)
        self.assertEqual(self.mt5.sent,[])


if __name__ == '__main__':
    unittest.main()
