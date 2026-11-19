"""Fast retry requires exact pre-send refusal and a newer native observation."""
import copy
import pathlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'payload/vps'))
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent))
from chunk_entry import ChunkEntries
import test_chunk_bridge_guard as native_fixture


class EntryPriceFastRetryTests(unittest.TestCase):
    def state(self):
        return dict(accounts={'900000001':dict(latestQuoteProof=dict(capturedAtMs=1002,tickUtcMs=1000),
            priceRejections=dict(market=dict(at=1000,retryMs=250,quote=[25000,25000.2],volume=.2,
                fastQuoteProof=dict(quote=[25001,25001.2],tickUtcMs=999,checkedAtMs=1001)))),
            '900000002':dict(priceRejections={})})

    def ready(self,state,quote=(25000,25000.2),kind='market'):
        with patch('chunk_entry.time.time',return_value=1.010):
            return ChunkEntries._parallel_retry_ready(state,'900000001',kind,list(quote),.2)

    def test_newer_native_quote_releases_only_the_affected_account_before_250ms(self):
        state=self.state();other=copy.deepcopy(state['accounts']['900000002'])
        self.assertTrue(self.ready(state))
        self.assertEqual(state['accounts']['900000002'],other)
        self.assertEqual(state['accounts']['900000001']['priceRejections']['market']['at'],1000)

    def test_old_capture_and_unchanged_native_tick_do_not_accelerate(self):
        for capture,tick,quote in ((1000,1005,[25000,25000.2]),
                                   (1001,1005,[25000,25000.2]),
                                   (1002,999,[25001,25001.2])):
            state=self.state();state['accounts']['900000001']['latestQuoteProof']=dict(capturedAtMs=capture,tickUtcMs=tick)
            self.assertFalse(self.ready(state,quote))

    def test_same_bid_ask_with_new_native_tick_is_a_new_observation(self):
        self.assertTrue(self.ready(self.state(),[25001,25001.2]))

    def test_missing_proof_and_broker_rate_backoff_keep_their_original_delay(self):
        state=self.state();last=state['accounts']['900000001']['priceRejections']['market']
        last.pop('fastQuoteProof')
        self.assertFalse(self.ready(state));last['retryMs']=500
        with patch('chunk_entry.time.time',return_value=1.300):
            self.assertFalse(ChunkEntries._parallel_retry_ready(state,'900000001','market',[25000,25000.2],.2))

    def batch(self,result,kind='market',**settings):
        engine=object.__new__(ChunkEntries);engine.entry_clocks={}
        engine._priority_volume=lambda payload,state,account,kind,price,volume,reserved:volume
        engine._hybrid_guards=lambda *args,**kwargs:{}
        engine._parallel_note=lambda *args,**kwargs:None
        engine._save=lambda state:None
        engine._hybrid_send=lambda payload,state,kind,*args,**kwargs:state['children'].append(dict(id='one',type=kind,state='rejected',result=result))
        engine._limit_results=lambda state,child:{'900000001':result}
        state=dict(id='entry',phase='running',accounts={'900000001':{}},children=[])
        payload=dict(symbol='USTEC',side='buy',initialSizingOnly=True)
        payload.update(settings)
        engine._parallel_batch(payload,state,kind,{'900000001':(25000.2,.2,25000,25000.2,0)})
        return state['accounts']['900000001'].get('priceRejections',{}).get(kind,{})

    def proof(self):
        return dict(status='rejected',entryDiagnostics=dict(account='900000001',symbol='USTEC',side='buy',kind='market',
            bid=25001,ask=25001.2,tickUtcMs=999,checkedAtMs=1001,
            rejectionStage='native-entry-price-guard',mutationStarted=False))

    def test_fast_marker_requires_matching_identity_and_unfilled_unsent_price_refusal(self):
        self.assertIn('fastQuoteProof',self.batch(self.proof()))
        for mutate in (lambda r:r.update(retcode=10024),lambda r:r.update(providerOrderId='9'),
                       lambda r:r.update(filledVolume=.2),
                       lambda r:r['entryDiagnostics'].update(account='900000002'),
                       lambda r:r['entryDiagnostics'].update(symbol='SPX500'),
                       lambda r:r['entryDiagnostics'].update(mutationStarted=True),
                       lambda r:r['entryDiagnostics'].update(rejectionStage='final-native-protection-guard')):
            row=self.proof();mutate(row)
            self.assertNotIn('fastQuoteProof',self.batch(row))

    def test_unknown_outcome_cannot_create_a_fast_retry_marker(self):
        row=self.proof();row['status']='unknown'
        with self.assertRaisesRegex(ValueError,'unknown'):self.batch(row)

    def test_stale_refusal_retries_both_order_types_on_a_new_fresh_tick(self):
        for kind in ('market', 'limit'):
            with self.subTest(kind=kind):
                row = self.proof()
                row.update(finalPreMutationRejection=True, mutationStarted=False)
                row['entryDiagnostics'].update(kind=kind, tickUtcMs=8900, checkedAtMs=10000,
                    rejectionStage='native-entry-stale-quote-guard')
                with patch('chunk_entry.time.time', return_value=10):
                    rejection = self.batch(row, kind)
                self.assertTrue(rejection['fastQuoteProof']['requireNewTick'])
                details = dict(priceRejections={kind: rejection})
                state = dict(accounts={'900000001': details})
                for captured, tick, expected in ((10000, 10000, False), (10010, 8900, False),
                        (10010, 9000, False), (10010, 10001, True)):
                    details['latestQuoteProof'] = dict(capturedAtMs=captured, tickUtcMs=tick)
                    with patch('chunk_entry.time.time', return_value=10.02):
                        ready = ChunkEntries._parallel_retry_ready(state, '900000001', kind, [25001, 25001.2], .2)
                    self.assertIs(ready, expected)
                with patch('chunk_entry.time.time', return_value=11.01):
                    self.assertFalse(ChunkEntries._parallel_retry_ready(state, '900000001', kind, [25001, 25001.2], .2))
                row['finalPreMutationRejection'] = False
                self.assertNotIn('fastQuoteProof', self.batch(row, kind))

    def test_fixed_edge_retries_on_the_first_usable_new_tick_in_both_directions(self):
        for side, inside, near, ready in (
                ('buy', [25005.7,25005.9], [25005.85,25006.05], [25005.91,25006.11]),
                ('sell', [25006.1,25006.3], [25005.95,25006.15], [25005.89,25006.09])):
            with self.subTest(side=side), patch('chunk_entry.time.time', return_value=10):
                row=dict(status='rejected', finalPreMutationRejection=True, mutationStarted=False,
                    entryDiagnostics=dict(account='900000001',symbol='USTEC',side=side,kind='limit',
                        bid=inside[0],ask=inside[1],tickUtcMs=9990,checkedAtMs=10000,
                        submittedPrice=25006., minimumDistance=.1, fixedEdgeQuoteRefusal=True,
                        rejectionStage='final-native-price-guard',mutationStarted=False))
                rejection=self.batch(row,'limit',side=side,entryMarketFirst=True)
                self.assertIn('fastQuoteProof',rejection)
                self.assertNotIn('fastQuoteProof',self.batch(row,'limit',side=side))
                state=dict(initialSizingOnly=True,accounts={'900000001':dict(priceRejections={'limit':rejection})})
                cases=((10060,10059,inside,False), (10060,10059,near,False),
                       (10000,10059,ready,False), (10060,9990,ready,False), (10060,10059,ready,True))
                with patch('chunk_entry.time.time', return_value=10.060):
                    for capture,tick,quote,expected in cases:
                        state['accounts']['900000001']['latestQuoteProof']=dict(capturedAtMs=capture,tickUtcMs=tick)
                        self.assertIs(ChunkEntries._parallel_retry_ready(state,'900000001','limit',quote,.2),expected)
                for change in (dict(fixedEdgeQuoteRefusal=False), dict(minimumDistance=float('nan')),
                               dict(account='900000002'),dict(kind='market')):
                    invalid=dict(row,entryDiagnostics=dict(row['entryDiagnostics'],**change))
                    self.assertNotIn('fastQuoteProof',self.batch(invalid,'limit',side=side,entryMarketFirst=True))
                throttled=dict(row,retcode=10024)
                self.assertNotIn('fastQuoteProof',self.batch(throttled,'limit',side=side,entryMarketFirst=True))
                for now, quote, expected in ((10.3,near,False), (10.7,near,False),
                                             (10.7,ready,True), (11.01,near,True)):
                    details=state['accounts']['900000001']
                    details['latestQuoteProof']=dict(capturedAtMs=now*1000,tickUtcMs=now*1000-3)
                    with patch('chunk_entry.time.time',return_value=now):
                        self.assertIs(ChunkEntries._parallel_retry_ready(state,'900000001','limit',quote,.2),expected)

    def test_native_stale_quote_records_exact_proof_without_sending(self):
        fixture=native_fixture.ChunkGuardTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.mt5.tick.time = fixture.payload['expiresAt']/1000-5
        result=fixture.bridge.execute('place',fixture.payload,None,True)['result']
        self.assertIs(result['finalPreMutationRejection'], True)
        self.assertEqual(result['entryDiagnostics']['rejectionStage'], 'native-entry-stale-quote-guard')
        self.assertEqual(result['entryDiagnostics']['tickUtcMs'], fixture.mt5.tick.time*1000)
        self.assertEqual(fixture.mt5.sent, [])
        self.assertEqual(fixture.bridge.receipt(fixture.payload['id'])['result'], result)

    def test_native_final_price_race_records_proof_and_sends_nothing(self):
        fixture=native_fixture.ChunkGuardTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        ticks=[fixture.mt5.tick,SimpleNamespace(bid=25001.,ask=25001.2,time=fixture.mt5.tick.time)]
        fixture.mt5.symbol_info_tick=lambda *_:ticks.pop(0)
        result=fixture.bridge.execute('place',fixture.payload)['result']
        self.assertEqual(result['status'],'rejected');self.assertEqual(fixture.mt5.sent,[])
        proof=result['entryDiagnostics']
        self.assertFalse(proof['mutationStarted'])
        self.assertEqual(proof['rejectionStage'],'native-entry-price-guard')
        self.assertEqual(proof['account'],'900000002');self.assertEqual(proof['ask'],25001.2)
        saved=fixture.bridge.receipt(fixture.payload['id'])['result']
        self.assertEqual(saved['entryDiagnostics'],proof)


if __name__=='__main__':unittest.main()
