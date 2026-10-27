import pathlib
import sys
import tempfile
import time
import json
import http.client
import threading
import unittest
import statistics
import os
import subprocess
import ssl
import hashlib
from unittest.mock import patch
from types import SimpleNamespace as N

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'payload/vps'))
from mt5_bridge import Bridge, create_server, AccountWorker, server_tls_context, supervise, process_lock, ReadTimings



class FakeMT5:
    ORDER_TYPE_BUY, ORDER_TYPE_SELL = 0, 1
    ORDER_TYPE_BUY_LIMIT, ORDER_TYPE_SELL_LIMIT = 2, 3
    ORDER_TYPE_BUY_STOP, ORDER_TYPE_SELL_STOP = 4, 5
    TRADE_ACTION_DEAL, TRADE_ACTION_PENDING = 1, 5
    TRADE_ACTION_SLTP, TRADE_ACTION_MODIFY, TRADE_ACTION_REMOVE = 6, 7, 8
    ORDER_FILLING_IOC = 1
    ORDER_TIME_GTC, ORDER_FILLING_FOK, ORDER_FILLING_RETURN = 0, 0, 2
    TRADE_RETCODE_DONE, TRADE_RETCODE_PLACED = 10009, 10008
    TRADE_RETCODE_TIMEOUT, TRADE_RETCODE_DONE_PARTIAL, TRADE_RETCODE_CONNECTION = 10012, 10010, 10031

    def __init__(self):
        self.sent = []
        self.account = N(login=900000002, server='Example-MT5', currency='USD', trade_allowed=True, trade_expert=True)
        self.terminal = N(connected=True, trade_allowed=True, tradeapi_disabled=False)
        self.info = N(trade_tick_size=.1, volume_step=.1, volume_min=.1, volume_max=100,
                      trade_stops_level=10, point=.01, filling_mode=1)
        self.tick = N(bid=25000., ask=25000.2, time=time.time())
        self.result = N(retcode=self.TRADE_RETCODE_DONE, order=42, volume=.4, price=25000.2, comment='done')
        self.positions = [N(ticket=71, symbol='USTEC', type=0, volume=.4, price_open=25000., sl=24986., tp=25042.,
                            price_current=25000., profit=0., comment='cid:test')]
        self.orders = [N(ticket=81, symbol='USTEC', type=2, volume_current=.4, price_open=24900., sl=24886., tp=24942.,
                         type_time=2, time_expiration=1792000000, price_stoplimit=0., comment='cid:pending')]
        self.history_orders=[];self.history_deals=[]

    def account_info(self): return self.account
    def terminal_info(self): return self.terminal
    def symbol_select(self, *_): return True
    def symbol_info(self, *_): return self.info
    def symbol_info_tick(self, *_): return self.tick
    def order_calc_profit(self, *_): return 1.
    def positions_get(self, ticket=None, symbol=None): return tuple(x for x in self.positions if (ticket is None or x.ticket == ticket) and (symbol is None or x.symbol==symbol))
    def orders_get(self, ticket=None, symbol=None): return tuple(x for x in self.orders if (ticket is None or x.ticket == ticket) and (symbol is None or x.symbol==symbol))
    def history_orders_get(self, ticket=None): return tuple(x for x in self.history_orders if x.ticket==ticket)
    def history_deals_get(self, position=None): return tuple(x for x in self.history_deals if x.position_id==position)
    def order_check(self, request): return N(retcode=0)
    def order_send(self, request):
        self.sent.append(request)
        return self.result

class BridgeTests(unittest.TestCase):
    def timed_open(self, linked=False):
        now=1791813600. # 12 October 2026 14:00 UTC / 10:00 New York
        payload={**self.order,'closeAtUtc':1791831600000.,'closeTimeZone':'America/New_York',
                 'closeLocalDate':'2026-10-12','closeLocalTime':'15:00','cancelPendingAtDeadline':True,'expiresAt':now*1000+2000}
        self.mt5.result.order=71
        with patch('mt5_bridge.time.time',return_value=now):
            result=self.bridge.group_execute('place',{**payload,'mirrorAccount':'900000001'}) if linked else self.bridge.execute('place',payload)
        self.assertEqual(result['result']['status'],'ok')
        return payload

    def exit_history(self, terminal=None, ticket=71, volume=.4):
        terminal=terminal or self.mt5
        terminal.history_orders=[N(ticket=ticket,symbol='USTEC',position_id=ticket,state=4)]
        terminal.history_deals=[N(symbol='USTEC',position_id=ticket,entry=0,volume=volume,reason=0),
                                N(symbol='USTEC',position_id=ticket,entry=1,volume=volume,reason=4)]

    def due(self, bridge=None):
        bridge=bridge or self.bridge;bridge.time_rule_check_at=0
        with patch('mt5_bridge.time.time',return_value=1791831600.): bridge.evaluate_time_rules()
















    def condition(self, **changes):
        return dict(id='condition',ticket='71',symbol='USTEC',volume=.2,expectedVolume=.4,
                    targetPrice=25001.,comparison='gte',expiresAt=time.time()*1000+2000, **changes)






    def moscow_bridge(self):
        bridge=Bridge(self.mt5,{**self.config,'brokerUtcOffsetSeconds':10800},self.file)
        self.addCleanup(bridge.db.close)
        return bridge





    def test_price_condition_arms_without_a_trade_and_closes_exact_buy_ticket_once_at_bid(self):
        payload = self.condition()
        self.assertEqual(self.bridge.execute('schedule-close',payload)['result']['status'],'ok')
        self.bridge.execute('schedule-close',payload)
        self.assertEqual(self.mt5.sent,[])
        self.mt5.tick.bid,self.mt5.tick.ask = 25000.9,25001.1
        self.bridge.evaluate_rules()
        self.assertEqual(self.mt5.sent,[])  # Ask crossing is not a BUY trigger.
        self.mt5.tick.bid,self.mt5.tick.ask = 25001.,25001.2
        self.bridge.evaluate_rules()
        self.bridge.evaluate_rules()
        self.assertEqual(len(self.mt5.sent),1)
        self.assertEqual((self.mt5.sent[0]['position'],self.mt5.sent[0]['volume'],self.mt5.sent[0]['type']),(71,.2,1))
        self.assertEqual(self.bridge.close_rules()['rules'][0]['state'],'completed')




    def test_condition_cancel_is_durable_and_duplicate_arm_or_invalid_partial_is_rejected(self):
        payload=self.condition()
        self.bridge.execute('schedule-close',payload)
        self.assertEqual(self.bridge.execute('schedule-close',{**payload,'id':'another'})['result']['status'],'rejected')
        cancel=dict(id='cancel-condition',ruleId=payload['id'],ticket='71',symbol='USTEC',expiresAt=time.time()*1000+2000)
        self.assertEqual(self.bridge.execute('cancel-close',cancel)['result']['status'],'ok')
        self.assertEqual(self.bridge.execute('cancel-close',cancel)['result']['status'],'ok')
        self.mt5.tick.bid=25002.; self.mt5.tick.ask=25002.2
        self.bridge.evaluate_rules(); self.assertEqual(self.mt5.sent,[])
        for changes in ({'volume':.4},{'volume':.25},{'targetPrice':25000.05},{'comparison':'invalid'},{'expectedVolume':.3}):
            bad={**payload,'id':'bad',**changes}
            self.assertEqual(self.bridge.execute('schedule-close',bad)['result']['status'],'rejected')

    def test_unknown_price_close_is_never_retried_and_blocks_manual_management_after_restart(self):
        payload=self.condition(); payload['targetPrice']=25000.
        self.bridge.execute('schedule-close',payload)
        self.mt5.result=None
        self.bridge.evaluate_rules()
        restarted=Bridge(self.mt5,self.config,self.file); self.addCleanup(restarted.db.close)
        restarted.evaluate_rules()
        result=restarted.execute('close',dict(payload,id='manual',volume=.1))['result']
        self.assertEqual(result['status'],'rejected')
        self.assertEqual(len(self.mt5.sent),1)
        self.assertEqual(restarted.close_rules()['rules'][0]['state'],'unknown')


    def linked_bridge(self, account=900000001):
        mt5=FakeMT5(); mt5.account.login=account; mt5.result.order=71
        mt5.positions[0].ticket=91; mt5.positions[0].identifier=71
        linked=Bridge(mt5,{**self.config,'account':account},pathlib.Path(self.temp.name)/('secondary-'+str(account)+'.sqlite3'))
        self.addCleanup(linked.db.close)
        class LocalWorker:
            def call(_self,method,*args): return getattr(linked,method)(*args)
        self.bridge.linked[str(account)]=LocalWorker()
        return linked,mt5

    def test_scaled_three_account_open_uses_cached_specs_and_preserves_entry_protection_and_receipts(self):
        second,secondary=self.linked_bridge(); third,third_mt5=self.linked_bridge(152657)
        self.mt5.result.order=71
        for account,bridge in (('900000001',second),('152657',third)):
            self.bridge.linked[account].volume_specs=bridge.lot_specs()
            call=self.bridge.linked[account].call
            def cached(method,*args,_call=call):
                self.assertIn(method,('check_opening','execute'))
                return _call(method,*args)
            self.bridge.linked[account].call=cached
        copies=[dict(account='900000001',multiplier=.7),dict(account='152657',multiplier=1.5)]
        payload=dict(self.order,mirrorAccounts=copies)
        result=self.bridge.group_execute('place',payload)['result']
        self.assertEqual(result['status'],'ok'); self.assertEqual(len(result['accountResults']),3)
        self.assertEqual([terminal.sent[0]['volume'] for terminal in (self.mt5,secondary,third_mt5)],[.4,.2,.6])
        for terminal in (secondary,third_mt5):
            for key in ('price','type','sl','tp'): self.assertEqual(terminal.sent[0][key],self.mt5.sent[0][key])
        self.bridge.group_execute('place',payload)
        self.assertEqual([len(terminal.sent) for terminal in (self.mt5,secondary,third_mt5)],[1,1,1])

    def test_percent_price_close_uses_each_actual_volume_survives_restart_and_fires_once(self):
        second,secondary=self.linked_bridge(); third,third_mt5=self.linked_bridge(152657)
        self.mt5.result.order=71
        copies=[dict(account='900000001',multiplier=.7),dict(account='152657',multiplier=1.5)]
        self.bridge.group_execute('place',dict(self.order,mirrorAccounts=copies))
        secondary.positions[0].volume=.2; third_mt5.positions[0].volume=.6
        self.assertEqual(self.bridge.management()['alerts'],[])
        payload=dict(self.condition(),mirrorAccounts=[dict(account=row['account']) for row in copies],closePercent=50)
        self.assertEqual(self.bridge.group_execute('schedule-close',payload)['result']['status'],'ok')
        self.assertEqual([len(terminal.sent) for terminal in (self.mt5,secondary,third_mt5)],[1,1,1])
        restarted=Bridge(secondary,second.config,pathlib.Path(self.temp.name)/'secondary-900000001.sqlite3')
        self.addCleanup(restarted.db.close)
        for bridge,terminal,volume in ((self.bridge,self.mt5,.2),(restarted,secondary,.1),(third,third_mt5,.3)):
            rule=bridge.close_rules()['rules'][0]
            self.assertEqual(rule['volume'],volume); self.assertEqual(rule['closePercent'],50)
            bridge.evaluate_rules(); self.assertEqual(len(terminal.sent),1)
            terminal.tick.bid=25001.; terminal.tick.ask=25001.2
            bridge.evaluate_rules(); bridge.evaluate_rules()
            self.assertEqual(len(terminal.sent),2); self.assertEqual(terminal.sent[-1]['volume'],volume)
            terminal.positions[0].volume-=volume
        self.assertEqual(self.bridge.management()['alerts'],[])

    def test_too_small_scaled_open_or_percent_rejects_before_any_account_submission(self):
        second,secondary=self.linked_bridge(); self.mt5.result.order=71
        with self.assertRaisesRegex(ValueError,'lot limits'):
            self.bridge.group_execute('place',dict(self.order,mirrorAccounts=[dict(account='900000001',multiplier=.1)]))
        self.assertEqual(self.mt5.sent,[]); self.assertEqual(secondary.sent,[])
        self.bridge.group_execute('place',dict(self.order,mirrorAccounts=[dict(account='900000001',multiplier=.7)]))
        secondary.positions[0].volume=.2
        payload=dict(self.condition(),volume=.1,closePercent=25,mirrorAccounts=[dict(account='900000001')])
        with self.assertRaisesRegex(ValueError,'lot limits'): self.bridge.group_execute('schedule-close',payload)
        self.assertEqual(self.bridge.close_rules()['rules'],[]); self.assertEqual(second.close_rules()['rules'],[])
        self.assertEqual([len(self.mt5.sent),len(secondary.sent)],[1,1])

    def test_scaled_full_close_uses_own_remaining_volume_and_expected_volume_guard(self):
        second,secondary=self.linked_bridge(); self.mt5.result.order=71
        self.bridge.group_execute('place',dict(self.order,mirrorAccounts=[dict(account='900000001',multiplier=.7)]))
        secondary.positions[0].volume=.2
        payload=dict(self.condition(),id='full-scaled',volume=.4,mirrorAccounts=[dict(account='900000001')])
        self.assertEqual(self.bridge.group_execute('close',payload)['result']['status'],'ok')
        self.assertEqual(self.mt5.sent[-1]['volume'],.4); self.assertEqual(secondary.sent[-1]['volume'],.2)





























    def start_server(self):
        self.config['token'] = 'offline-fixture-token'
        server = create_server(self.bridge, ('127.0.0.1', 0))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_address[1]




    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.file = pathlib.Path(self.temp.name) / 'requests.db'
        self.mt5 = FakeMT5()
        self.config = {'account':900000002, 'server':'Example-MT5', 'symbols':['SPX500','USTEC'], 'tradingEnabled':True}
        self.bridge = Bridge(self.mt5, self.config, self.file)
        self.addCleanup(self.bridge.db.close)
        self.addCleanup(self.bridge.close_group_executor)
        self.order = {'id':'fixture', 'symbol':'USTEC','side':'buy','type':'market','volume':.4,
                      'price':25000.2,'sl':24986.2,'tp':25042.2,'cid':'test','expiresAt':time.time()*1000+2000}











    def control(self, operation, **changes):
        payload = {'id':'control', 'ticket':'71', 'symbol':'USTEC', 'expectedVolume':.4,
                   'expiresAt':time.time()*1000+2000, **changes}
        return self.bridge.execute(operation, payload)['result']

    def test_partial_hedge_close_binds_exact_ticket_and_opposite_side(self):
        self.mt5.positions.append(N(**{**vars(self.mt5.positions[0]), 'ticket':72, 'type':1}))
        self.assertEqual(self.control('close', volume=.1)['status'], 'ok')
        request = self.mt5.sent[-1]
        self.assertEqual((request['position'],request['type'],request['volume'],request['price']), (71,1,.1,25000.))
        self.assertNotIn('sl', request)
        self.assertNotIn('tp', request)
        self.assertEqual(self.control('close', id='sell',ticket='72',volume=.4)['status'],'ok')
        self.assertEqual((self.mt5.sent[-1]['position'],self.mt5.sent[-1]['type'],self.mt5.sent[-1]['price']), (72,0,25000.2))

    def test_close_volume_bounds_steps_remainder_and_changed_position(self):
        for values in ({'volume':.5},{'volume':.05},{'volume':.15},
                       {'volume':.1,'expectedVolume':.5},{'volume':0},{'volume':.1,'ticket':'999'},
                       {'volume':.1,'symbol':'SPX500'}):
            with self.subTest(values=values):
                self.assertEqual(self.control('close',**values)['status'],'rejected')
        self.mt5.info.volume_min = .2
        self.assertEqual(self.control('close',volume=.3)['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])
        with self.assertRaises(ValueError): self.control('close',volume=float('inf'))


    def test_cancel_never_closes_position(self):
        self.assertEqual(self.control('cancel',ticket='71')['status'],'rejected')
        self.assertEqual(self.control('cancel',ticket='81')['status'],'ok')
        self.assertEqual(self.mt5.sent,[{'action':8,'order':81,'symbol':'USTEC'}])


    def test_partial_fill_unknown_does_not_resend(self):
        self.mt5.result = N(retcode=10010,order=42)
        self.assertEqual(self.control('close',volume=.1)['status'],'unknown')
        self.assertEqual(self.control('close',volume=.1)['status'],'unknown')
        self.assertEqual(len(self.mt5.sent),1)




