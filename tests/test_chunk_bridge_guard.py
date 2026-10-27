import copy
import pathlib
import sys
import tempfile
import time
import unittest
import http.client
import json
import threading
import datetime
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from test_mt5_bridge import FakeMT5
from mt5_bridge import Bridge,create_server,new_york_zone


class ChunkGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.mt5=FakeMT5()
        self.bridge=Bridge(self.mt5,dict(account=900000002,server='Example-MT5',symbols=['USTEC'],tradingEnabled=True),pathlib.Path(self.temp.name)/'db.sqlite')
        self.addCleanup(self.bridge.db.close)
        self.payload=dict(id='guarded-1',symbol='USTEC',side='buy',type='market',volume=.2,price=25000.,sl=24990.,tp=0.,
                          expiresAt=time.time()*1000+3000,entryGuard=dict(anchor=25000.,maxGap=1.,maxRiskPrice=25000.8,positions=[]))

    def test_buy_uses_live_ask_and_preserves_absolute_stop(self):
        result=self.bridge.execute('place',self.payload)['result']
        self.assertEqual(result['status'],'ok');self.assertEqual(self.mt5.sent[0]['price'],25000.2)
        self.assertEqual(self.mt5.sent[0]['sl'],24990.);self.assertEqual(self.mt5.sent[0]['volume'],.2)
        self.bridge.execute('place',self.payload)
        self.assertEqual(len(self.mt5.sent),1)

    def test_second_check_after_durable_claim_rejects_price_race(self):
        ticks=[self.mt5.tick,N(bid=25001.,ask=25001.2,time=time.time())]
        self.mt5.symbol_info_tick=lambda *_:ticks.pop(0)
        result=self.bridge.execute('place',self.payload)['result']
        self.assertEqual(result['status'],'rejected');self.assertEqual(self.mt5.sent,[])
        self.assertEqual(self.bridge.receipt(self.payload['id'])['result']['status'],'rejected')

    def test_sell_checks_bid_and_risk_price_independently_of_gap(self):
        payload={**self.payload,'side':'sell','sl':25010.,'entryGuard':dict(anchor=25000.,maxGap=1.,maxRiskPrice=24999.9,positions=[])}
        self.mt5.tick=N(bid=24999.8,ask=25000.,time=time.time())
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_stale_tick_and_invalid_lot_fail_before_sending(self):
        self.mt5.tick.time=time.time()-2
        self.assertEqual(self.bridge.execute('place',self.payload)['result']['status'],'rejected')
        self.mt5.tick.time=time.time();payload={**self.payload,'id':'bad-lot','volume':.25}
        self.assertEqual(self.bridge.execute('place',payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_final_guard_cannot_renew_expired_command(self):
        now=time.time();self.payload['expiresAt']=now*1000+1000
        original=self.bridge.guard_chunk_request;calls=[]
        def delayed(payload,request):
            original(payload,request);calls.append(1)
            if len(calls)==2:self.payload['expiresAt']=now*1000-1
        # Simulate the wall clock advancing inside the second terminal read.
        def stamp():return now+2 if len(calls)>=2 else now
        self.bridge.guard_chunk_request=delayed
        with patch('mt5_bridge.time.time',side_effect=stamp):
            result=self.bridge.execute('place',self.payload)['result']
        self.assertEqual(result['status'],'rejected');self.assertEqual(self.mt5.sent,[])

    def test_zero_gap_allows_only_same_or_better_price(self):
        self.payload['entryGuard']['maxGap']=0
        self.mt5.tick.ask=25000.
        self.assertEqual(self.bridge.execute('place',self.payload)['result']['status'],'ok')

    def test_a_closed_or_changed_previous_part_blocks_further_entry_after_bounce(self):
        previous=self.mt5.positions[0]
        self.payload['entryGuard']['positions']=[dict(ticket=str(previous.ticket),volume=previous.volume,sl=previous.sl,tp=previous.tp)]
        original=self.bridge.guard_chunk_request;calls=[]
        def race(payload,request):
            original(payload,request);calls.append(1)
            if len(calls)==1:self.mt5.positions=[]
        self.bridge.guard_chunk_request=race
        self.assertEqual(self.bridge.execute('place',self.payload)['result']['status'],'rejected')
        self.assertEqual(self.mt5.sent,[])

    def test_authenticated_arm_list_and_stop_never_submit_before_trigger(self):
        self.mt5.account.margin_mode=2
        self.bridge.config['token']='validation-only'
        now=datetime.datetime.now(new_york_zone())
        deadline=now.replace(hour=15,minute=0,second=0,microsecond=0)
        if deadline<=now:deadline+=datetime.timedelta(days=1)
        payload=dict(id='http-arm',symbol='USTEC',side='buy',trigger=25001.,volume=1.,chunkVolume=.2,maxGap=1.,riskUsd=20.,sl=24990.,tp=0.,
                     expiresAt=time.time()*1000+3000,closeAtUtc=deadline.timestamp()*1000,closeTimeZone='America/New_York',
                     closeLocalDate=deadline.date().isoformat(),closeLocalTime='15:00',cancelPendingAtDeadline=True)
        server=create_server(self.bridge,('127.0.0.1',0));thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
        try:
            connection.request('GET','/entry-plans');reply=connection.getresponse();self.assertEqual(reply.status,401);reply.read();connection.close()
            headers={'Authorization':'Bearer validation-only'}
            connection.request('POST','/entry-plan',json.dumps(payload),headers);reply=connection.getresponse();entry=json.loads(reply.read())['entry']
            self.assertEqual(reply.status,200);self.assertEqual(entry['phase'],'armed')
            connection.request('GET','/entry-plans',headers=headers);reply=connection.getresponse();entries=json.loads(reply.read())['entries']
            self.assertEqual(entries[0]['id'],'http-arm');self.assertFalse(entries[0]['hasOpenPositions'])
            connection.request('POST','/entry-plan-stop',json.dumps(dict(id='stop-http-arm',entryId='http-arm',expiresAt=time.time()*1000+3000)),headers)
            reply=connection.getresponse();entry=json.loads(reply.read())['entry'];self.assertEqual(entry['phase'],'stopped')
            self.assertEqual(self.mt5.sent,[])
        finally:
            connection.close();server.shutdown();server.server_close();thread.join(3)

    def test_entry_readiness_is_authenticated_get_only_and_cannot_arm_or_send(self):
        self.mt5.account.margin_mode=2
        self.bridge.config['token']='validation-only'
        server=create_server(self.bridge,('127.0.0.1',0))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
        try:
            connection.request('GET','/entry-readiness');reply=connection.getresponse()
            self.assertEqual(reply.status,401);reply.read();connection.close()
            headers={'Authorization':'Bearer validation-only'}
            connection.request('GET','/entry-readiness',headers=headers);reply=connection.getresponse()
            self.assertEqual(reply.status,200)
            self.assertEqual(json.loads(reply.read())['entryReadiness'],dict(ready=True,accounts=['900000002']))
            connection.request('POST','/entry-readiness','{}',headers);reply=connection.getresponse()
            self.assertEqual(reply.status,400);reply.read();connection.close()
            for mode in ('zone','boundary'):
                connection.request('POST','/entry-plan',json.dumps({'entryMode':mode}),headers);reply=connection.getresponse()
                self.assertEqual(reply.status,200)
                self.assertEqual(json.loads(reply.read())['entryResult']['status'],'rejected')
            self.assertEqual(self.bridge.chunk_entries.list(),[]);self.assertEqual(self.mt5.sent,[])
        finally:
            connection.close();server.shutdown();server.server_close();thread.join(3)


if __name__=='__main__':unittest.main()
