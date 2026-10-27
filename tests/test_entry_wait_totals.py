"""Long waits survive fast phase alternation without extending a trace ring."""
import copy
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/'payload/vps'))
from entry_wait_totals import EntryWaitTotals, record_common_stop_timing


class EntryWaitTotalsTests(unittest.TestCase):
    def test_common_stop_timings_keep_native_stages_without_protection_payload(self):
        timing={}
        result=dict(status='ok',elapsedMs=480,positions=[dict(ticket='private')],sl=12,
            protectionTiming=dict(positionReadMs=2,modifyMs=400,finalReadMs=3,private='secret'))
        record_common_stop_timing(timing,result,490)
        row=timing['commonStop']
        self.assertEqual(row['commandCalls'],1)
        self.assertEqual(row['lastCommand']['stages']['modifyMs'],400)
        self.assertEqual(row['lastCommand']['coordinatorElapsedMs'],490)
        self.assertNotIn('private',json.dumps(row));self.assertNotIn('positions',json.dumps(row))
        record_common_stop_timing(timing,result,4,receipt_only=True)
        self.assertEqual(timing['commonStop']['commandCalls'],1)
        self.assertEqual(timing['commonStop']['receiptChecks'],1)
        self.assertEqual(timing['commonStop']['lastCommand'],row['lastCommand'])

    def test_long_limit_wait_survives_thousands_of_ready_transitions(self):
        totals = EntryWaitTotals()
        for tick in range(1000):
            totals.observe('limit', 'Return limit reserved at broker', tick*10, 12)
            totals.observe('ready', 'Fresh quote checked', tick*10+9)
        totals.observe('completed', 'Target and common stop confirmed', 10000)
        self.assertEqual(totals.data['observedMs'], 10000)
        self.assertEqual(totals.data['stages']['limit']['durationMs'], 9000)
        self.assertEqual(totals.data['waits'][0]['durationMs'], 9000)
        self.assertEqual(totals.data['waits'][0]['segments'], 1000)
        self.assertEqual(totals.data['waits'][0]['quoteSamples'], 1000)
        self.assertEqual(len(totals.data['waits']), 1)
        self.assertLess(len(json.dumps(totals.data)), 1000)

    def test_distinct_stages_and_reasons_remain_bounded_with_totals_preserved(self):
        totals = EntryWaitTotals()
        for tick in range(1000):
            totals.observe('waiting-'+str(tick), 'Broker reason '+str(tick), tick)
        totals.observe('completed', 'Done', 1000)
        self.assertLessEqual(len(totals.data['stages']), totals.MAX_STAGES)
        self.assertLessEqual(len(totals.data['waits']), totals.MAX_REASONS)
        self.assertEqual(sum(x['durationMs'] for x in totals.data['stages'].values()), 1000)
        self.assertEqual(sum(x['durationMs'] for x in totals.data['waits']), 1000)
        self.assertEqual(totals.data['observedMs'], 1000)

    def test_repeated_same_wait_is_one_segment_and_collects_quote_age_range(self):
        totals = EntryWaitTotals()
        totals.observe('waiting-quote', 'Broker quote is stale', 0, 1100)
        totals.observe('waiting-quote', 'Broker quote is stale', 500, 1600)
        totals.observe('ready', 'Fresh quote checked', 600, 10)
        row = totals.data['waits'][0]
        self.assertEqual(row['segments'], 1)
        self.assertEqual(row['durationMs'], 600)
        self.assertEqual(row['minQuoteAgeMs'], 1100)
        self.assertEqual(row['maxQuoteAgeMs'], 1600)

    def test_accounts_have_independent_buffers(self):
        first, second = EntryWaitTotals(), EntryWaitTotals()
        first.observe('limit', 'Waiting for return', 0)
        second.observe('ready', 'Ready', 0)
        before = copy.deepcopy(first.data)
        second.observe('completed', 'Done', 100)
        self.assertEqual(first.data, before)
        first.observe('ready', 'Returned', 4000)
        self.assertEqual(first.data['waits'][0]['durationMs'], 4000)
        self.assertEqual(second.data['observedMs'], 100)

    def test_stopped_idle_time_is_not_charged_to_a_later_explicit_resume(self):
        totals = EntryWaitTotals()
        totals.observe('ready', 'Ready', 0)
        totals.observe('stopped', 'User stopped', 10)
        totals.observe('ready', 'Explicit resume', 10000)
        totals.observe('completed', 'Done', 10020)
        self.assertEqual(totals.data['observedMs'], 30)
        self.assertEqual(totals.data['stages']['stopped']['durationMs'], 0)

    def test_bad_diagnostic_input_does_not_corrupt_the_totals(self):
        totals = EntryWaitTotals(); totals.observe('limit', 'Waiting', 10)
        before = copy.deepcopy(totals.data)
        for phase, reason, at in ((None, 'x', 12), ('x', None, 12), ('x', 'x', float('nan')),
                                  ('x', 'x', True), ('x', 'x', 9), ('x', 'x', 10**1000)):
            totals.observe(phase, reason, at)
        self.assertEqual(totals.data, before)
        totals.observe('ready', 'Done', 20, float('inf'))
        json.dumps(totals.data, allow_nan=False)
        self.assertEqual(totals.data['observedMs'], 10)


if __name__ == '__main__':
    unittest.main()
