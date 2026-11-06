import contextlib
import importlib.util
import json
import csv
import datetime as dt
import io
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'receiver'))
from candle_levels import CandleLevels, WindowHistory, read_window, candle_window_worker, window_offset

class Terminal:
    TIMEFRAME_M1 = 1
    def __init__(self, directory, stamp):
        self.directory, self.stamp, self.reads = directory, stamp, []
        self.high, self.low = 30380.25, 30342.5
    def terminal_info(self):
        return types.SimpleNamespace(data_path=self.directory)
    def symbol_info(self, symbol):
        return types.SimpleNamespace(name=symbol,digits=2,trade_tick_size=0.01) if symbol in ('USTEC', 'SPX500', 'XAUUSD') else None
    def copy_rates_range(self, symbol, timeframe, start, end):
        self.reads.append((symbol, timeframe, start.timestamp(), end.timestamp()))
        return [dict(time=self.stamp, high=self.high, low=self.low)]

class CandleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.stamp, self.offset = 1791981720, 10800
        self.now = self.stamp + 3600
        self.mt5 = Terminal(self.temporary.name, self.stamp + self.offset)
        self.book = CandleLevels(self.mt5, {'symbols': ['USTEC', 'SPX500'], 'brokerUtcOffsetSeconds': self.offset},
            pathlib.Path(self.temporary.name) / 'levels.sqlite3', clock=lambda: self.now)
        self.addCleanup(self.book.close)
        self.payload = dict(id='a'*24, revision=1, action='upsert', symbol='USTEC', timeframe='M1', time=self.stamp, side='high', tool='line')

    def ack(self, state, price=None, generation=None, complete=True, color=None, protocol=None):
        generation = generation or state['generation']; rows = [row for row in state['levels'] if row['status'] == 'drawing']
        protocol = protocol or (2 if any('colorRgb' in row for row in rows) else 1)
        file = pathlib.Path(self.temporary.name) / 'MQL5/Files/iscc-addon-levels-ack.csv'
        with file.open('w', newline='') as handle:
            writer = csv.writer(handle, delimiter='\t', lineterminator='\n')
            writer.writerow(['ISCC_CANDLE_LEVELS_ACK', protocol, generation, len(rows)])
            for row in rows:
                values = [row['id'], row['revision'], row['symbol'], 'ready', row['price'] if price is None else price]
                if protocol == 2:
                    values.append(row.get('colorRgb', 0xff6347 if row['side'] == 'high' else 0x00bfff) if color is None else color)
                writer.writerow(values)
            if complete: writer.writerow(['END', generation, len(rows)])
        os.utime(file, (self.now, self.now))
        return file

    def test_white_source_requires_red_display_readback_without_changing_source_or_price(self):
        for side in ('high', 'low'):
            with self.subTest(side=side):
                self.payload['revision'] += 1
                state = self.book.apply(dict(self.payload, side=side, colorRgb=0xffffff))
                row = state['levels'][0]
                price = self.mt5.high if side == 'high' else self.mt5.low
                self.assertEqual(row['colorRgb'], 0xffffff)
                self.assertEqual(row['price'], price)
                rows = list(csv.reader(io.StringIO((self.book.files()/'iscc-addon-levels.csv').read_text()), delimiter='\t'))
                self.assertEqual(int(rows[1][8]), 0xff0000)
                self.ack(state, color=0xffffff)
                self.assertEqual(self.book.state()['levels'][0]['status'], 'drawing')
                self.ack(state, color=0xff0000)
                ready = self.book.state()['levels'][0]
                self.assertEqual((ready['status'], ready['colorRgb'], ready['price']), ('ready', 0xffffff, price))

    def test_exact_mt5_high_and_broker_clock_are_used(self):
        state = self.book.apply(self.payload); row = state['levels'][0]
        self.assertEqual(row['price'], self.mt5.high); self.assertEqual(row['status'], 'drawing')
        self.assertEqual(self.mt5.reads[0][2:], (self.stamp+self.offset, self.stamp+self.offset))
        self.ack(state)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'ready')

    def test_status_uses_one_snapshot_for_both_file_and_ack_without_caching_readiness(self):
        pending=self.book.apply(self.payload)
        self.ack(pending)
        self.book.snapshot=Mock(wraps=self.book.snapshot)
        ready=self.book.state()
        self.assertEqual(self.book.snapshot.call_count,1)
        self.assertTrue(ready['snapshotApplied'])
        rows=list(csv.reader(io.StringIO((self.book.files()/'iscc-addon-levels.csv').read_text()),delimiter='\t'))
        self.assertEqual(rows[0][2],ready['generation'])
        self.assertEqual(rows[1][0],self.payload['id'])
        self.assertEqual(self.book.rows()[0][3]['status'],'drawing')
        self.now+=6
        expired=self.book.state()
        self.assertEqual(self.book.snapshot.call_count,2)
        self.assertFalse(expired['snapshotApplied'])
        self.assertEqual(expired['levels'][0]['status'],'drawing')

    def test_standalone_snapshot_write_still_reads_current_rows_once(self):
        self.book.apply(self.payload)
        self.book.snapshot=Mock(wraps=self.book.snapshot)
        self.book.write_snapshot()
        self.assertEqual(self.book.snapshot.call_count,1)

    def test_failed_snapshot_replace_is_retried_from_fresh_state(self):
        self.book.apply(self.payload)
        (self.book.files()/'iscc-addon-levels.csv').unlink()
        original=os.replace
        attempts=[]
        def replace(source,target):
            attempts.append((source,target))
            if len(attempts)==1:raise PermissionError('temporary file lock')
            return original(source,target)
        self.book.snapshot=Mock(wraps=self.book.snapshot)
        with patch('candle_levels.os.replace',side_effect=replace):
            with self.assertRaises(PermissionError):self.book.state()
            result=self.book.state()
        self.assertEqual(self.book.snapshot.call_count,2)
        self.assertEqual(len(attempts),2)
        self.assertFalse(result['snapshotApplied'])
        self.assertTrue((self.book.files()/'iscc-addon-levels.csv').exists())

    def test_arbitrary_tv_price_is_rejected_before_history_or_drawing(self):
        with self.assertRaises(ValueError):
            self.book.apply(dict(self.payload,mode='tv-price',side='price',price=30355.37))
        self.assertEqual(self.mt5.reads,[])
        self.assertEqual(self.book.state()['levels'],[])

    def test_red_source_is_purple_without_changing_candle_period_price_or_color_in_state(self):
        for source_color in (0xf23645,0xff0000,0x673ab7):
            for side in ('high','low'):
                self.payload['revision'] += 1
                state=self.book.apply(dict(self.payload,side=side,colorRgb=source_color))
                row=state['levels'][0]
                expected_price=self.mt5.high if side=='high' else self.mt5.low
                self.assertEqual((row['timeframe'],row['time'],row['side'],row['colorRgb'],row['price']),
                                 ('M1',self.stamp,side,source_color,expected_price))
                rows=list(csv.reader(io.StringIO((self.book.files()/'iscc-addon-levels.csv').read_text()),delimiter='\t'))
                self.assertEqual(int(rows[1][8]),0x673ab7)
                self.ack(state,color=0xf23645)
                self.assertEqual(self.book.state()['levels'][0]['status'],'drawing')
                self.ack(state,color=0x673ab7)
                self.assertEqual(self.book.state()['levels'][0]['status'],'ready')

    def test_low_is_a_single_level_and_line_and_ray_preserve_semantics(self):
        state = self.book.apply(dict(self.payload, side='low', tool='ray'))
        self.assertEqual(len(state['levels']), 1); self.assertEqual(state['levels'][0]['price'], self.mt5.low)
        self.assertEqual(state['levels'][0]['tool'], 'ray')

    def test_unmatched_candle_is_not_replaced_by_nearest(self):
        self.mt5.stamp -= 60
        row = self.book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'], 'waiting-history'); self.assertNotIn('price', row)
        for _ in range(7):
            self.now += 31
            row = self.book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'], 'error'); self.assertNotIn('price', row)

    def test_history_load_retries_are_bounded_and_can_recover_exact_bar(self):
        expected = self.mt5.stamp
        self.mt5.stamp -= 60
        first = self.book.apply(self.payload)
        self.book.apply(self.payload)
        self.assertEqual(len(self.mt5.reads), 1)
        self.assertNotIn('price', first['levels'][0])
        self.now += 3; self.mt5.stamp = expected
        row = self.book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'], 'drawing'); self.assertEqual(row['price'], self.mt5.high)

    def test_forming_candle_waits_without_history_read(self):
        self.now = self.stamp + 10
        row = self.book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'], 'waiting-close'); self.assertEqual(self.mt5.reads, [])
        self.now += 120
        self.assertEqual(self.book.apply(self.payload)['levels'][0]['status'], 'drawing')

    def test_partial_stale_wrong_generation_and_wrong_price_ack_are_rejected(self):
        state = self.book.apply(self.payload)
        for kwargs in [dict(complete=False), dict(generation='b'*64), dict(price=1)]:
            self.ack(state, **kwargs)
            self.assertEqual(self.book.state()['levels'][0]['status'], 'drawing')
        file = self.ack(state); self.now += 10
        self.assertEqual(self.book.state()['levels'][0]['status'], 'drawing')

    def test_revision_prevents_late_upsert_from_resurrecting_deleted_level(self):
        self.book.apply(self.payload)
        state = self.book.apply(dict(id='a'*24, revision=2, action='delete'))
        self.assertEqual(state['levels'], [])
        self.assertEqual(self.book.apply(self.payload)['levels'], [])
        with self.assertRaises(ValueError): self.book.apply(dict(self.payload, revision=2))

    def test_idempotent_resolved_request_does_not_requery_mt5(self):
        self.book.apply(self.payload); self.book.apply(self.payload)
        self.assertEqual(len(self.mt5.reads), 1)

    def test_invalid_symbols_nan_and_path_traversal_are_rejected(self):
        for patch in [dict(symbol='../other'), dict(id='../other'), dict(time=True), dict(revision=True), dict(timeframe='tick'), dict(side='both')]:
            with self.assertRaises(ValueError): self.book.apply(dict(self.payload, **patch))
        self.mt5.high = float('nan')
        self.assertEqual(self.book.apply(self.payload)['levels'][0]['status'], 'error')

    def test_source_color_reaches_file_and_requires_matching_indicator_color(self):
        state = self.book.apply(dict(self.payload, colorRgb=0x2962ff))
        self.assertEqual(state['levels'][0]['colorRgb'], 0x2962ff)
        file = self.book.files() / 'iscc-addon-levels.csv'
        rows = list(csv.reader(io.StringIO(file.read_text()), delimiter='\t'))
        self.assertEqual(rows[0][1], '2'); self.assertEqual(len(rows[1]), 9)
        self.assertEqual(rows[1][8], str(0x2962ff))
        self.ack(state, color=0xffffff)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'drawing')
        self.assertFalse(self.book.state()['snapshotApplied'])
        self.ack(state)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'ready')
        self.assertTrue(self.book.state()['snapshotApplied'])

    def test_old_indicator_cannot_acknowledge_a_colored_snapshot(self):
        state = self.book.apply(dict(self.payload, colorRgb=0x673ab7))
        self.ack(state, protocol=1)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'drawing')
        self.ack(state)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'ready')

    def test_color_only_edit_replaces_same_marker_and_rejects_old_generation(self):
        first = self.book.apply(dict(self.payload, colorRgb=0x2962ff))
        self.ack(first)
        second = self.book.apply(dict(self.payload, colorRgb=0xffffff, revision=2))
        self.assertNotEqual(first['generation'], second['generation'])
        self.assertEqual(second['levels'][0]['status'], 'drawing')
        self.assertEqual(len(self.book.rows()), 1)
        self.assertEqual(second['levels'][0]['price'], self.mt5.high)
        self.ack(second, color=0xff0000)
        self.assertEqual(self.book.state()['levels'][0]['status'], 'ready')

    def test_mixed_legacy_and_colored_markers_share_a_complete_version_two_snapshot(self):
        self.book.apply(self.payload)
        state = self.book.apply(dict(self.payload, id='b'*24, colorRgb=0x673ab7, side='low'))
        rows = list(csv.reader(io.StringIO((self.book.files()/'iscc-addon-levels.csv').read_text()), delimiter='\t'))
        self.assertEqual(rows[0][1], '2'); self.assertEqual(len(rows), 4)
        self.assertEqual([int(row[8]) for row in rows[1:-1]], [0xff6347, 0x673ab7])
        self.ack(state)
        self.assertTrue(all(row['status'] == 'ready' for row in self.book.state()['levels']))

    def test_invalid_color_never_reaches_the_terminal(self):
        for color in [-1, 0x1000000, True, 1.0, 'white', None]:
            with self.assertRaisesRegex(ValueError, 'color'):
                self.book.apply(dict(self.payload, colorRgb=color))
        self.assertEqual(self.mt5.reads, [])

    def test_exact_daily_hourly_and_five_minute_periods_are_not_downgraded(self):
        anchors = [('D1', dt.datetime(2026, 10, 13, 21, tzinfo=dt.timezone.utc)),
                   ('H1', dt.datetime(2026, 10, 14, 13, tzinfo=dt.timezone.utc)),
                   ('M5', dt.datetime(2026, 10, 14, 13, 35, tzinfo=dt.timezone.utc))]
        self.now = dt.datetime(2026, 10, 17, tzinfo=dt.timezone.utc).timestamp()
        for index, (tf, anchor) in enumerate(anchors):
            with self.subTest(timeframe=tf):
                value = index + 100
                setattr(self.mt5, 'TIMEFRAME_'+tf, value)
                timestamp = int(anchor.timestamp()); self.mt5.stamp = timestamp + self.offset
                row = self.book.apply(dict(self.payload, revision=index+1, timeframe=tf, time=timestamp, colorRgb=0xffffff))['levels'][0]
                self.assertEqual(row['status'], 'drawing'); self.assertEqual(row['timeframe'], tf)
                self.assertEqual(self.mt5.reads[-1][1:], (value, timestamp+self.offset, timestamp+self.offset))

    def test_an_adjacent_hour_or_shifted_daily_boundary_is_never_silently_used(self):
        self.now = self.stamp + 3*86400
        for index, tf in enumerate(['H1', 'D1']):
            setattr(self.mt5, 'TIMEFRAME_'+tf, index+10)
            self.mt5.stamp = self.stamp + self.offset - 3600
            row = self.book.apply(dict(self.payload, revision=index+1, timeframe=tf))['levels'][0]
            self.assertEqual(row['status'], 'waiting-history'); self.assertNotIn('price', row)

    def test_color_and_exact_anchor_survive_backend_restart(self):
        state = self.book.apply(dict(self.payload, colorRgb=0xf23645))
        self.book.close()
        self.book = CandleLevels(self.mt5, {'symbols':['USTEC','SPX500'], 'brokerUtcOffsetSeconds':self.offset},
            pathlib.Path(self.temporary.name)/'levels.sqlite3', clock=lambda:self.now)
        self.addCleanup(self.book.close)
        restored = self.book.state()['levels'][0]
        self.assertEqual(restored['time'], self.stamp); self.assertEqual(restored['colorRgb'], 0xf23645)
        self.assertEqual(restored['price'], state['levels'][0]['price'])

    def test_deletion_needs_fresh_ack_of_the_new_empty_snapshot(self):
        first = self.book.apply(dict(self.payload, colorRgb=0x2962ff))
        self.ack(first)
        self.assertTrue(self.book.state()['snapshotApplied'])
        removed = self.book.apply(dict(id=self.payload['id'], revision=2, action='delete'))
        self.assertEqual(removed['levels'], []); self.assertFalse(removed['snapshotApplied'])
        self.ack(removed, complete=False)
        self.assertFalse(self.book.state()['snapshotApplied'])
        self.ack(removed)
        self.assertTrue(self.book.state()['snapshotApplied'])
        self.now += 6
        self.assertFalse(self.book.state()['snapshotApplied'])

class WindowTests(unittest.TestCase):
    def setUp(self):
        self.start, self.offset = 1788184800, 10800
        self.payload = dict(id='c'*24, revision=1, action='upsert', symbol='USTEC',
            timeframe='H4', time=self.start, endTime=self.start+14400, side='low', tool='ray', colorRgb=0xffffff)
        self.minutes = [dict(time=self.start+self.offset+index*60, high=100+index/1000,
            low=90-index/1000, tick_volume=index+1) for index in range(240)]
        self.hours = []
        for hour in range(4):
            children=self.minutes[hour*60:(hour+1)*60]
            self.hours.append(dict(time=children[0]['time'],high=max(r['high'] for r in children),
                low=min(r['low'] for r in children),tick_volume=sum(r['tick_volume'] for r in children)))
        self.reads=[]
        def copy(symbol,tf,start,end):
            self.reads.append((symbol,tf,start.timestamp(),end.timestamp()))
            rows = self.minutes if tf==1 else self.hours if tf==60 else []
            return [r for r in rows if start.timestamp()<=r['time']<=end.timestamp()]
        self.mt5=types.SimpleNamespace(TIMEFRAME_M1=1,TIMEFRAME_M5=5,TIMEFRAME_H1=60,TIMEFRAME_H4=240,
            TIMEFRAME_D1=1440,copy_rates_range=copy)

    def test_gerchik_historical_offset_follows_us_rules_including_european_transition_gaps(self):
        from clock_fixture import NEW_YORK_TZIF_GZIP
        config={'server':'Example-MT5','brokerClockModel':'new-york-plus-hours','brokerClockHours':7,'brokerUtcOffsetSeconds':10800,'candleNewYorkTzif':NEW_YORK_TZIF_GZIP}
        for date,expected in [('2026-01-13',7200),('2026-03-09',10800),('2026-03-16',10800),
                              ('2026-10-26',10800),('2026-11-02',7200)]:
            stamp=int(dt.datetime.fromisoformat(date).replace(tzinfo=dt.timezone.utc).timestamp())
            self.assertEqual(window_offset(config,stamp,stamp+3600),expected,date)
        self.assertEqual(window_offset({'server':'another','brokerUtcOffsetSeconds':3600},self.start,self.start+3600),3600)

    def test_missing_timezone_rules_and_windows_crossing_a_clock_change_fail_closed(self):
        from clock_fixture import NEW_YORK_TZIF_GZIP
        with self.assertRaisesRegex(ValueError,'timezone'):
            window_offset({'server':'Example-MT5','brokerClockModel':'new-york-plus-hours','brokerClockHours':7},self.start,self.start+3600)
        start=int(dt.datetime(2026,3,7,tzinfo=dt.timezone.utc).timestamp())
        with self.assertRaisesRegex(ValueError,'clock change'):
            window_offset({'server':'Example-MT5','brokerClockModel':'new-york-plus-hours','brokerClockHours':7,'candleNewYorkTzif':NEW_YORK_TZIF_GZIP},start,start+3*86400)

    def test_unaligned_h4_uses_only_the_tv_window_and_retains_one_extreme(self):
        # These prices belong to the extra broker hour, outside the TV window.
        self.minutes.insert(0,dict(time=self.start+self.offset-60,high=999,low=1,tick_volume=100))
        self.minutes.append(dict(time=self.payload['endTime']+self.offset,high=888,low=2,tick_volume=100))
        result=read_window(self.mt5,self.payload,self.offset)
        self.assertEqual(result['method'],'mt5-m1-window');self.assertEqual(result['minuteCount'],240)
        self.assertEqual(result['high'],100.239);self.assertEqual(result['low'],89.761)
        self.assertEqual(result['brokerTime'],self.start+self.offset)
        self.assertEqual(self.reads[-1][2:],(self.start+self.offset,self.payload['endTime']+self.offset-1))

    def test_missing_zero_quote_minutes_are_allowed_only_with_matching_parent_evidence(self):
        missing=self.minutes.pop(0)
        self.hours[0]['tick_volume']-=missing['tick_volume']
        self.hours[0]['high']=max(r['high'] for r in self.minutes[:59])
        self.hours[0]['low']=min(r['low'] for r in self.minutes[:59])
        self.assertEqual(read_window(self.mt5,self.payload,self.offset)['minuteCount'],239)

    def test_partial_minute_history_cannot_pass_using_matching_overall_high_and_low(self):
        self.minutes.pop(10)  # Not an extreme: the tick volume still proves the gap.
        with self.assertRaisesRegex(ValueError,'tick volume'):
            read_window(self.mt5,self.payload,self.offset)

    def test_wrong_parent_prices_or_missing_boundary_hour_are_rejected(self):
        self.hours[0]['high']+=1
        with self.assertRaisesRegex(ValueError,'extremes'):read_window(self.mt5,self.payload,self.offset)
        self.hours.pop(0)
        with self.assertRaisesRegex(ValueError,'span'):read_window(self.mt5,self.payload,self.offset)

    def test_shortened_h4_window_does_not_read_a_full_broker_h4_bar(self):
        payload=dict(self.payload,endTime=self.start+10800)
        result=read_window(self.mt5,payload,self.offset)
        self.assertEqual(result['minuteCount'],180);self.assertEqual(result['high'],100.179)
        self.assertEqual([r[1] for r in self.reads],[60,5,1])

    def test_old_daily_window_uses_verified_m5_without_requiring_m1(self):
        start, offset, duration = 1777240800, 10800, 23*3600
        five = [dict(time=start+offset+i*300,high=27400+i/10,low=27300-i/10,tick_volume=i+1)
                for i in range(duration//300)]
        hours = [dict(time=five[i]['time'],high=max(r['high'] for r in five[i:i+12]),
                      low=min(r['low'] for r in five[i:i+12]),tick_volume=sum(r['tick_volume'] for r in five[i:i+12]))
                 for i in range(0,len(five),12)]
        calls=[]
        def read(symbol,tf,left,right):
            calls.append(tf)
            if tf==1:raise AssertionError('Verified M5 must not request old M1 history')
            return five if tf==5 else hours if tf==60 else []
        self.mt5.copy_rates_range=read
        payload=dict(self.payload,timeframe='D1',time=start,endTime=start+duration)
        result=read_window(self.mt5,payload,offset)
        self.assertEqual(result['method'],'mt5-m5-window')
        self.assertEqual(result['fiveMinuteCount'],276)
        self.assertEqual(result['high'],max(r['high'] for r in five))
        self.assertEqual(result['low'],min(r['low'] for r in five))
        self.assertEqual(calls,[60,5])

    def test_partial_m5_cannot_bypass_hourly_volume_proof(self):
        self.mt5.copy_rates_range=lambda symbol,tf,left,right: self.hours if tf==60 else ([
            dict(time=self.hours[0]['time'],high=999,low=1,tick_volume=1)] if tf in (5,1) else [])
        with self.assertRaisesRegex(ValueError,'tick volume|incomplete'):
            read_window(self.mt5,self.payload,self.offset)

    def test_aligned_native_m5_is_used_without_changing_its_period(self):
        calls=[]
        def read(symbol,tf,start,end):
            calls.append((tf,start.timestamp(),end.timestamp()))
            return [dict(time=self.start+self.offset,high=123.4,low=120)]
        self.mt5.copy_rates_range=read
        result=read_window(self.mt5,dict(self.payload,timeframe='M5',endTime=self.start+300),self.offset)
        self.assertEqual(result['method'],'mt5-candle');self.assertEqual(result['high'],123.4)
        self.assertEqual(calls,[(5,self.start+self.offset,self.start+self.offset)])

    def test_exact_minute_window_is_not_replaced_when_its_native_bar_is_absent(self):
        with self.assertRaisesRegex(ValueError,'exact native'):
            read_window(self.mt5,dict(self.payload,timeframe='M5',endTime=self.start+300),self.offset)

    def test_window_request_waits_asynchronously_and_requires_indicator_ack(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        history=Mock();history.get.side_effect=[{'pending':True},{'result':read_window(self.mt5,self.payload,self.offset)}]
        terminal=Terminal(temporary.name,self.start+self.offset)
        now=[self.payload['endTime']+1]
        book=CandleLevels(terminal,{'symbols':['USTEC'],'brokerUtcOffsetSeconds':self.offset},
            pathlib.Path(temporary.name)/'window.sqlite3',clock=lambda:now[0],window_history=history)
        self.addCleanup(book.close)
        row=book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'],'waiting-history');self.assertNotIn('price',row)
        now[0]+=2;row=book.apply(self.payload)['levels'][0]
        self.assertEqual(row['status'],'drawing');self.assertEqual(row['price'],89.761)
        self.assertEqual(row['timeframe'],'H4');self.assertEqual(row['endTime'],self.payload['endTime'])
        self.assertEqual(terminal.reads,[])

    def test_window_identity_rejects_seconds_and_excessive_duration_before_any_read(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        history=Mock()
        book=CandleLevels(Terminal(temporary.name,0),{'symbols':['USTEC']},pathlib.Path(temporary.name)/'window.sqlite3',window_history=history)
        self.addCleanup(book.close)
        for end in [self.start,self.start+1,self.start+14401,self.start+14460,True]:
            with self.assertRaises(ValueError):book.apply(dict(self.payload,endTime=end))
        history.get.assert_not_called()

    def test_a_late_window_worker_result_cannot_resurrect_a_deleted_revision(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        history=Mock();history.get.return_value={'pending':True}
        book=CandleLevels(Terminal(temporary.name,0),{'symbols':['USTEC']},pathlib.Path(temporary.name)/'window.sqlite3',
            clock=lambda:self.payload['endTime']+1,window_history=history)
        self.addCleanup(book.close)
        book.apply(self.payload)
        book.apply(dict(id=self.payload['id'],revision=2,action='delete'))
        history.get.return_value={'result':{'high':123,'low':99,'brokerTime':self.start+self.offset}}
        self.assertEqual(book.apply(self.payload)['levels'],[])
        self.assertEqual(history.get.call_count,1)

    def test_one_history_worker_never_blocks_or_starts_parallel_reads_and_has_a_deadline(self):
        context=Mock();parent,child=Mock(),Mock();context.Pipe.return_value=(parent,child)
        process=Mock();context.Process.return_value=process;process.is_alive.return_value=True
        parent.poll.return_value=False;now=[0]
        history=WindowHistory({'terminalPath':'known','account':900000002,'server':'known'},context=context,clock=lambda:now[0])
        self.addCleanup(history.close)
        self.assertEqual(history.get(self.payload),{'pending':True})
        self.assertEqual(history.get(dict(self.payload,time=self.start-3600)),{'pending':True})
        self.assertEqual(context.Process.call_count,1)
        now[0]=21
        self.assertIn('timed out',history.get(self.payload)['error']);process.terminate.assert_called_once()
        process.join.assert_called_with(timeout=0.05)

    def test_a_complete_closed_window_result_is_cached_for_the_other_extreme(self):
        context=Mock();parent,child=Mock(),Mock();context.Pipe.return_value=(parent,child)
        process=Mock();context.Process.return_value=process;process.is_alive.return_value=False
        parent.poll.return_value=False
        history=WindowHistory({},context=context,clock=lambda:0);self.addCleanup(history.close)
        history.get(self.payload);parent.poll.return_value=True
        result={'result':{'high':123,'low':99,'brokerTime':self.start+self.offset}}
        parent.recv.return_value=result
        self.assertEqual(history.get(self.payload),result)
        self.assertEqual(history.get(dict(self.payload,side='high',colorRgb=0x2962ff)),result)
        self.assertEqual(context.Process.call_count,1)

    def test_history_worker_checks_the_account_without_login_or_trading_calls(self):
        module=Mock();module.initialize.return_value=True
        module.account_info.return_value=types.SimpleNamespace(login=999,server='known')
        module.terminal_info.return_value=types.SimpleNamespace(connected=True)
        pipe=Mock()
        with unittest.mock.patch.dict(sys.modules,{'MetaTrader5':module}):
            candle_window_worker(pipe,{'terminalPath':'known','account':900000002,'server':'known'},self.payload)
        self.assertIn('account verification',pipe.send.call_args[0][0]['error'])
        module.copy_rates_range.assert_not_called();module.login.assert_not_called();module.order_send.assert_not_called()
        module.shutdown.assert_called_once()
