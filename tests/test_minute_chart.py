import copy
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from mt5_bridge import Bridge, CoordinatorBridge, native_worker_method

class MinuteChartTests(unittest.TestCase):
    def model(self,count=10):
        bridge=object.__new__(Bridge)
        bridge.config={'symbols':['USTEC']};bridge.broker_clock_model='fixed-offset';bridge.broker_utc_offset_seconds=7200
        bridge.account=lambda:SimpleNamespace(login=900000002,server='test')
        now=1793766960
        rates=[dict(time=now+7200-60*i,open=25000.,high=25002.,low=24999.,close=25001.) for i in range(count-1,-1,-1)]
        calls=[]
        bridge.mt5=SimpleNamespace(TIMEFRAME_M1=1,copy_rates_from_pos=lambda *args:calls.append(args) or rates[-args[-1]:])
        return bridge,rates,calls,now

    def test_ten_real_bars_use_seasonal_converter_and_cache_only_the_current_minute(self):
        b,r,c,now=self.model()
        with patch('mt5_bridge.time.time',return_value=now+10),patch('candle_levels.available_symbol'):
            first=b.minute_bars('USTEC');again=b.minute_bars('USTEC')
            self.assertEqual(len(c),1);self.assertEqual(c[0],('USTEC',1,0,10))
            self.assertEqual(first['bars'][-1][0],now);self.assertEqual(first['basis'],'bid')
            first['bars'][0][1]=1;self.assertNotEqual(b.minute_bars('USTEC')['bars'][0][1],1)
        with patch('mt5_bridge.time.time',return_value=now+61),patch('candle_levels.available_symbol'):b.minute_bars('USTEC')
        self.assertEqual(len(c),2)

    def test_invalid_future_duplicate_and_non_ohlc_bars_are_never_drawn_as_history(self):
        for change in ('future','duplicate','invalid'):
            b,r,c,now=self.model()
            if change=='future':r[-1]['time']+=120
            elif change=='duplicate':r[-1]['time']=r[-2]['time']
            else:r[-1]['low']=25003
            with patch('mt5_bridge.time.time',return_value=now+10),patch('candle_levels.available_symbol'):
                with self.assertRaises(ValueError):b.minute_bars('USTEC')
        b,_,c,_=self.model()
        with self.assertRaises(ValueError):b.minute_bars('OTHER')
        self.assertEqual(c,[])

    def test_thirty_minutes_and_legacy_ten_have_separate_bounded_caches(self):
        b,_,calls,now=self.model(30)
        with patch('mt5_bridge.time.time',return_value=now+10),patch('candle_levels.available_symbol'):
            self.assertEqual(len(b.minute_bars('USTEC',30)['bars']),30)
            self.assertEqual(len(b.minute_bars('USTEC')['bars']),10)
            self.assertEqual(len(b.minute_bars('USTEC',30)['bars']),30)
        self.assertEqual([row[-1] for row in calls],[30,10])

    def test_arbitrary_history_sizes_are_rejected_before_any_native_read(self):
        b,_,calls,_=self.model()
        for count in (True,0,1,10.,'30',31,100000):
            with self.subTest(count=count),self.assertRaises(ValueError):b.minute_bars('USTEC',count)
        self.assertEqual(calls,[])

    def test_chart_read_is_routed_to_the_existing_separate_read_only_client(self):
        calls=[];b=object.__new__(CoordinatorBridge);b._candle_call=lambda *args:calls.append(args) or {'bars':[]}
        self.assertEqual(b.minute_bars('USTEC'),{'bars':[]})
        self.assertEqual(b.minute_bars('USTEC',30),{'bars':[]})
        self.assertEqual(calls,[('minute_bars','USTEC',10),('minute_bars','USTEC',30)])
        worker=SimpleNamespace(minute_bars=lambda symbol:{'symbol':symbol})
        self.assertEqual(native_worker_method(worker,'minute_bars',('USTEC',),candle_only=True),{'symbol':'USTEC'})
        with self.assertRaises(ValueError):native_worker_method(worker,'execute',(),candle_only=True)

if __name__=='__main__':unittest.main()
