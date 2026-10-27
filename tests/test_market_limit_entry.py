"""Native pending/market lifecycle simulations; no broker connection or trades."""
import copy
import sys
import pathlib
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_chunk_entry import FakeBridge, Worker
from chunk_entry import ChunkEntries
import test_chunk_bridge_guard as guard_fixture


class PendingBridge(FakeBridge):
    def __init__(self, account='900000002'):
        super().__init__(account)
        self.ended = set()
        self.cancelled = []
        self.cancel_unknown = False
        self.fill_on_cancel = False

    def _fill(self, payload):
        if payload['type'] != 'limit':
            return super()._fill(payload)
        self.ticket += 1
        ticket = str(self.ticket)
        self.sent.append(copy.deepcopy(payload))
        self.rows.append(dict(ticket=ticket, identifier=ticket, symbol=payload['symbol'], type=payload['side'],
            lots=payload['volume'], open_price=payload['price'], sl=payload['sl'], tp=payload['tp'], __isPosition=False))
        return dict(status='ok', providerOrderId=ticket, filledVolume=0., fillPrice=0.)

    def fill_limit(self, amount, price=None):
        pending = next(row for row in self.rows if not row['__isPosition'])
        position = next((row for row in self.rows if row['__isPosition'] and row['ticket']==pending['ticket']), None)
        if position is None:
            position = {**pending, '__isPosition':True, 'lots':0.}
            self.rows.append(position)
        position['lots'] = round(position['lots']+amount, 8)
        if price is not None:
            position['open_price'] = price
        pending['lots'] = round(pending['lots']-amount, 8)
        if pending['lots'] == 0:
            self.rows.remove(pending)

    def entry_limit_state(self, ticket, symbol):
        rows = [copy.deepcopy(row) for row in self.rows if str(row['ticket'])==ticket and row['symbol']==symbol]
        return dict(account=self.login.login, server=self.login.server,
            pending=[row for row in rows if not row['__isPosition']], positions=[row for row in rows if row['__isPosition']],
            terminal=dict(phase='cancelled' if ticket in self.ended else 'unknown'))

    def execute(self, operation, payload):
        if operation!='cancel':
            return super().execute(operation, payload)
        self.cancelled.append(copy.deepcopy(payload))
        if self.cancel_unknown:
            return {'result':{'status':'unknown'}}
        if self.fill_on_cancel:
            self.fill_limit(next(row['lots'] for row in self.rows if not row['__isPosition']))
            return {'result':{'status':'rejected','reason':'Already filled'}}
        self.rows = [row for row in self.rows if row['__isPosition'] or str(row['ticket'])!=payload['ticket']]
        self.ended.add(payload['ticket'])
        return {'result':{'status':'ok'}}


class HybridEntryTests(unittest.TestCase):
    def setUp(self):
        self.primary = PendingBridge()
        self.follower = PendingBridge('900000001')
        self.primary.linked['900000001'] = Worker(self.primary, self.follower)
        self.engine = ChunkEntries(self.primary)
        self.addCleanup(self.engine.close)
        self.payload = dict(id='hybrid', symbol='USTEC', side='buy', trigger=100., level=100.,
            sl=84., tp=0., riskUsd=12.5, volume=.7, chunkVolume=.7, chunkPercent=100,
            entryMode='side-zone', executionPolicy='market-limit', zoneRadius=6., zoneMultiplier=1,
            tickSize=.1, stopOffsetPts=160., stopMode='entry', maxGap=6.,
            mirrorAccounts=[dict(account='900000001',multiplier=1.)], expiresAt=time.time()*1000+3000,
            closeAtUtc=time.time()*1000+3600000, closeTimeZone='America/New_York',
            closeLocalDate='2026-10-20',closeLocalTime='15:00',cancelPendingAtDeadline=True)

    def price(self, value, other=None):
        for bridge, price in ((self.primary,value),(self.follower,value if other is None else other)):
            bridge.price = dict(ask=price,bid=price-.2) if self.payload['side']=='buy' else dict(bid=price,ask=price+.2)

    def tick(self):
        self.engine.pending_poll_at.clear()
        self.engine.tick()

    def state(self):
        return self.engine.list()[0]

    def arm_limit(self):
        direction=1 if self.payload['side']=='buy' else -1
        self.price(100.+direction*(self.payload['zoneRadius']+4.))
        self.engine.arm(self.payload)
        self.tick()
        self.assertEqual(self.primary.sent[0]['type'],'limit')

    def stop(self):
        return self.engine.stop(dict(id='stop-hybrid',entryId='hybrid',expiresAt=time.time()*1000+3000))

    def test_market_on_both_inclusive_edges_for_both_sides_and_multipliers(self):
        for side in ('buy','sell'):
            for multiplier in (1,2):
                for edge in (0,1):
                    with self.subTest(side=side,multiplier=multiplier,edge=edge):
                        self.setUp()
                        direction=1 if side=='buy' else -1
                        self.payload.update(side=side,sl=100.-direction*16.,zoneRadius=6.*multiplier,zoneMultiplier=multiplier,maxGap=6.*multiplier)
                        self.price(100.+direction*6.*multiplier*edge)
                        self.engine.arm(self.payload); self.tick()
                        self.assertEqual(self.state()['phase'],'completed')
                        self.assertEqual(self.primary.sent[0]['type'],'market')
                        self.assertEqual(self.follower.sent[0]['volume'],.7)

    def test_limit_on_outer_edge_and_no_market_while_it_is_pending(self):
        for side in ('buy','sell'):
            for multiplier in (1,2):
                with self.subTest(side=side,multiplier=multiplier):
                    direction=1 if side=='buy' else -1
                    self.setUp();self.payload.update(side=side,sl=100.-direction*16.,zoneRadius=6.*multiplier,zoneMultiplier=multiplier,maxGap=6.*multiplier)
                    self.arm_limit()
                    for bridge in (self.primary,self.follower):
                        self.assertEqual(bridge.sent[0]['price'],100.+direction*6.*multiplier)
                        self.assertEqual(bridge.sent[0]['sl'],100.+direction*(6.*multiplier-16.))
                    self.price(100.)
                    for _ in range(4): self.tick()
                    self.assertEqual(self.state()['phase'],'running')
                    self.assertEqual(len(self.primary.sent),1);self.assertEqual(len(self.follower.sent),1)

    def test_wrong_side_waits_then_price_jump_selects_limit(self):
        self.price(90.);self.engine.arm(self.payload);self.tick()
        self.assertEqual(self.primary.sent,[])
        self.price(110.);self.tick()
        self.assertEqual(self.primary.sent[0]['type'],'limit')

    def test_mixed_mirror_prices_wait_instead_of_invalid_or_duplicate_orders(self):
        self.price(110.,100.);self.engine.arm(self.payload);self.tick()
        self.assertEqual(self.primary.sent,[])
        self.price(110.,110.);self.tick()
        self.assertEqual(len(self.primary.sent),1)

    def test_partial_fill_and_remainder_then_full_fill_are_counted_once(self):
        self.arm_limit()
        for bridge in (self.primary,self.follower): bridge.fill_limit(.3)
        self.tick();self.tick()
        self.assertEqual(self.state()['accounts']['900000002']['filledVolume'],.3)
        for bridge in (self.primary,self.follower): bridge.fill_limit(.4)
        self.tick();self.tick()
        state=self.state();self.assertEqual(state['phase'],'completed')
        self.assertEqual(state['accounts']['900000002']['filledVolume'],.7)
        self.assertAlmostEqual(state['accounts']['900000002']['riskUsed'],11.2)
        self.assertEqual(len(self.primary.sent),1)

    def test_stop_cancels_both_captured_orders_without_new_entry_or_position_close(self):
        self.arm_limit();result=self.stop()
        self.assertEqual(result['phase'],'stopped')
        self.assertEqual(result['accounts']['900000002']['filledVolume'],0.)
        self.assertEqual(len(self.primary.cancelled),1);self.assertEqual(len(self.follower.cancelled),1)
        self.stop();self.tick()
        self.assertEqual(len(self.primary.cancelled),1);self.assertEqual(len(self.primary.sent),1)

    def test_cancel_keeps_partial_positions_and_accounts_for_fill_race(self):
        for race in (False,True):
            with self.subTest(race=race):
                self.setUp();self.arm_limit()
                for bridge in (self.primary,self.follower):
                    if race: bridge.fill_on_cancel=True
                    else: bridge.fill_limit(.3)
                result=self.stop()
                self.assertEqual(result['phase'],'stopped')
                self.assertEqual(result['accounts']['900000002']['filledVolume'],.7 if race else .3)
                self.assertTrue(self.primary.rows[0]['__isPosition'])

    def test_lost_cancel_response_preserves_unknown_lock_and_never_repeats_cancel(self):
        self.arm_limit();self.primary.cancel_unknown=True
        result=self.stop();self.assertEqual(result['phase'],'unknown')
        self.stop();self.tick()
        self.assertEqual(len(self.primary.cancelled),1);self.assertEqual(len(self.primary.sent),1)

    def test_lost_placement_response_never_repeats_order(self):
        self.price(110.);self.engine.arm(self.payload);self.primary.raise_send=True;self.tick()
        self.assertEqual(self.state()['phase'],'unknown')
        self.primary.raise_send=False
        for _ in range(4):self.tick()
        self.assertEqual(self.primary.sent,[])

    def test_restart_preserves_possible_pending_order_and_explicit_cancel_still_works(self):
        self.arm_limit();self.engine=ChunkEntries(self.primary);self.addCleanup(self.engine.close)
        self.assertEqual(self.state()['phase'],'unknown')
        self.price(100.);self.tick();self.assertEqual(len(self.primary.sent),1)
        self.assertEqual(self.stop()['phase'],'stopped')

    def test_deadline_cancels_pending_remainder(self):
        self.arm_limit()
        with patch('chunk_entry.time.time',return_value=self.payload['closeAtUtc']/1000+1):self.tick()
        self.assertEqual(self.state()['phase'],'stopped');self.assertEqual(len(self.primary.cancelled),1)

    def test_favorable_fill_beyond_level_stops_remainder_without_resubmission(self):
        self.arm_limit()
        for bridge in (self.primary,self.follower):bridge.fill_limit(.3,price=99.)
        self.tick()
        self.assertEqual(self.state()['phase'],'stopped');self.assertEqual(len(self.primary.cancelled),1)
        self.assertTrue(self.state()['children'][0]['fillGuardViolated'])

    def test_mirrored_multiplier_and_max_lot_keep_distinct_sequential_limits(self):
        self.payload['mirrorAccounts'][0]['multiplier']=2.
        self.payload['chunkVolume']=.4
        self.arm_limit()
        self.primary.fill_limit(.4);self.follower.fill_limit(.8);self.tick()
        self.tick()
        self.assertEqual(len(self.primary.sent),2)
        self.primary.fill_limit(.3);self.follower.fill_limit(.6);self.tick()
        state=self.state();self.assertEqual(state['phase'],'completed')
        self.assertEqual(state['accounts']['900000002']['filledVolume'],.7)
        self.assertEqual(state['accounts']['900000001']['filledVolume'],1.4)

    def test_mutated_partial_position_does_not_create_a_replacement(self):
        self.arm_limit();self.primary.fill_limit(.3);self.follower.fill_limit(.3);self.tick()
        self.primary.rows[-1]['lots']=.2;self.tick()
        self.assertEqual(self.state()['phase'],'unknown')
        self.tick();self.assertEqual(len(self.primary.sent),1)
        self.assertEqual(len(self.primary.cancelled),1)
        self.assertEqual(len(self.follower.cancelled),1)

    def test_changed_partial_stop_cancels_own_remainder_and_keeps_unknown_lock(self):
        self.arm_limit();self.primary.fill_limit(.3);self.follower.fill_limit(.3);self.tick()
        self.primary.rows[-1]['sl']=91.;self.tick()
        self.assertEqual(self.state()['phase'],'unknown')
        self.assertEqual(len(self.primary.cancelled),1)
        self.assertEqual(len(self.follower.cancelled),1)
        self.assertTrue(all(row['__isPosition'] for row in self.primary.rows))
        self.tick();self.assertEqual(len(self.primary.sent),1)

    def test_pending_protection_cannot_be_silently_changed(self):
        self.arm_limit();self.primary.rows[0]['sl']=89.
        self.tick()
        self.assertEqual(self.state()['phase'],'unknown')
        self.assertEqual(len(self.primary.sent),1)

    def test_stop_cancels_only_captured_ticket_and_preserves_an_unrelated_pending_order(self):
        self.arm_limit()
        unrelated={**self.primary.rows[0],'ticket':'999','identifier':'999'}
        self.primary.rows.append(unrelated)
        self.assertEqual(self.stop()['phase'],'stopped')
        self.assertEqual(self.primary.rows,[unrelated])
        self.assertEqual(self.primary.cancelled[0]['ticket'],'101')


class HybridFinalGuardTests(unittest.TestCase):
    def setUp(self):
        fixture=guard_fixture.ChunkGuardTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        self.bridge,self.mt5=fixture.bridge,fixture.mt5
        self.payload=copy.deepcopy(fixture.payload)
        self.payload.update(type='limit',price=25006.,sl=24990.,volume=.7)
        self.payload['entryGuard'].update(anchor=25006.,maxGap=6.,maxRiskPrice=25006.,priceFloor=25000.,priceCeiling=25006.,stopDistance=16.,riskBudget=12.5)
        self.mt5.tick=N(bid=25009.8,ask=25010.,time=time.time())

    def test_native_limit_keeps_boundary_and_return_filling_on_final_fresh_check(self):
        result=self.bridge.execute('place',self.payload)['result']
        self.assertEqual(result['status'],'ok')
        request=self.mt5.sent[0]
        self.assertEqual(request['action'],self.mt5.TRADE_ACTION_PENDING)
        self.assertEqual(request['price'],25006.);self.assertEqual(request['sl'],24990.)
        self.assertEqual(request['type_filling'],self.mt5.ORDER_FILLING_RETURN)

    def test_buy_uses_ask_and_sell_uses_bid_when_limit_is_inside_the_spread(self):
        for side,edge,bid,ask in (('buy',25006.,25005.9,25006.1),('sell',24994.,24993.9,24994.1)):
            with self.subTest(side=side):
                direction=1 if side=='buy' else -1
                payload=copy.deepcopy(self.payload)
                payload.update(id='spread-limit-'+side,side=side,price=edge,sl=edge-direction*16.)
                payload['entryGuard'].update(anchor=edge,maxRiskPrice=edge,
                    priceFloor=25000. if side=='buy' else edge,priceCeiling=edge if side=='buy' else 25000.)
                self.mt5.tick=N(bid=bid,ask=ask,time=time.time())
                self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'ok')
                self.assertEqual(self.mt5.sent[-1]['price'],edge)
                self.assertEqual(self.mt5.sent[-1]['type_filling'],self.mt5.ORDER_FILLING_RETURN)

    def test_price_race_into_band_is_rejected_without_sending_market_or_limit(self):
        ticks=[self.mt5.tick,N(bid=25004.8,ask=25005.,time=time.time())]
        self.mt5.symbol_info_tick=lambda *_:ticks.pop(0)
        self.assertEqual(self.bridge.execute('place',self.payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_bad_limit_price_stale_quote_and_excess_risk_are_rejected(self):
        for index,changes in enumerate(({'price':25005.},{'volume':1.})):
            payload={**self.payload,**changes,'id':'bad-limit-'+str(index)}
            self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        self.mt5.tick.time=time.time()-2
        self.assertEqual(self.bridge.execute('place',self.payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_pending_read_resolves_broker_position_identifier_after_partial_fill(self):
        position=self.mt5.positions[0]
        position.identifier=91
        order=N(ticket=52,symbol='USTEC',type=self.mt5.ORDER_TYPE_BUY_LIMIT,volume_current=.3,price_open=25006.,sl=24990.,tp=0.,comment='fixture')
        self.mt5.orders=[order]
        self.mt5.history_orders=[N(ticket=52,symbol='USTEC',position_id=91,state=1)]
        view=self.bridge.entry_limit_state('52','USTEC')
        self.assertEqual(view['pending'][0]['ticket'],'52')
        self.assertEqual(view['positions'][0]['ticket'],str(position.ticket))
        self.assertEqual(self.mt5.sent,[])

    def test_cancelled_ticket_requires_broker_history_and_unknown_history_is_not_empty_success(self):
        self.mt5.positions=[];self.mt5.orders=[]
        self.assertEqual(self.bridge.entry_limit_state('52','USTEC')['terminal']['phase'],'unknown')
        self.mt5.history_orders=[N(ticket=52,symbol='USTEC',position_id=0,state=2)]
        self.assertEqual(self.bridge.entry_limit_state('52','USTEC')['terminal']['phase'],'cancelled')
        self.assertEqual(self.mt5.sent,[])

    def test_live_partial_pending_position_id_is_resolved_before_any_history_exists(self):
        position=self.mt5.positions[0]
        position.identifier=91
        self.mt5.orders=[N(ticket=52,symbol='USTEC',type=self.mt5.ORDER_TYPE_BUY_LIMIT,
            position_id=91,volume_current=.3,price_open=25006.,sl=24990.,tp=0.,comment='fixture')]
        self.mt5.history_orders=[]
        def no_history(*args,**kwargs):
            raise AssertionError('Live pending position identity must not require history')
        self.mt5.history_orders_get=no_history
        view=self.bridge.entry_limit_state('52','USTEC')
        self.assertEqual(view['pending'][0]['positionId'],'91')
        self.assertEqual(view['positions'][0]['ticket'],str(position.ticket))
        self.assertEqual(self.mt5.sent,[])

    def test_live_position_id_cannot_attach_another_symbols_position(self):
        self.mt5.positions[0].symbol='SPX500'
        self.mt5.orders=[N(ticket=52,symbol='USTEC',type=self.mt5.ORDER_TYPE_BUY_LIMIT,
            position_id=self.mt5.positions[0].ticket,volume_current=.3,price_open=25006.,sl=24990.,tp=0.,comment='fixture')]
        self.mt5.history_orders=[]
        view=self.bridge.entry_limit_state('52','USTEC')
        self.assertEqual(view['positions'],[])
        self.assertEqual(self.mt5.sent,[])


if __name__=='__main__':unittest.main()
