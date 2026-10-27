"""Exact broker evidence is optional metadata, never a trading dependency."""
import copy
import json
import pathlib
import sys
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]/'payload/vps'))
from broker_opening_evidence import opening_order_evidence


class BrokerOpeningEvidenceTests(unittest.TestCase):
    def order(self, **changes):
        fields = dict(ticket=33046869, symbol='SPX500', type=2, state=4,
                      price_open=7648.20, volume_initial=.2, volume_current=0.,
                      sl=7644.84, tp=7661.84, time_setup_msc=1793006016000,
                      time_done_msc=1793006028500, type_filling=2,
                      comment='private broker comment', external_id='private external id')
        fields.update(changes)
        return SimpleNamespace(**fields)

    def test_exact_record_distinguishes_request_accepted_order_and_fill(self):
        order = self.order(); before = copy.deepcopy(vars(order))
        result = opening_order_evidence(order, '33046869', 'SPX500',
                                        submitted_price=7648.08, fill_price=7648.32)
        self.assertEqual(result['orderType'], 2)
        self.assertEqual(result['acceptedOrderPrice'], 7648.20)
        self.assertEqual(result['submittedPrice'], 7648.08)
        self.assertEqual(result['requestToOrderPriceDifference'], .12)
        self.assertEqual(result['orderToFillPriceDifference'], .12)
        self.assertEqual(result['setupTimeMsc'], 1793006016000)
        self.assertEqual(result['doneTimeMsc'], 1793006028500)
        self.assertFalse(result['amendmentHistoryAvailable'])
        self.assertEqual(vars(order), before)
        self.assertLess(len(json.dumps(result, allow_nan=False)), 800)
        self.assertNotIn('private', json.dumps(result))

    def test_missing_or_mismatched_record_cannot_be_used_as_evidence(self):
        for order, ticket, symbol in ((None, '33046869', 'SPX500'),
                                      (self.order(ticket=1), '33046869', 'SPX500'),
                                      (self.order(), '33046869', 'USTEC'),
                                      (self.order(), True, 'SPX500'),
                                      (self.order(), '0', 'SPX500'),
                                      (self.order(), '33046869', 'x'*129)):
            with self.subTest(ticket=ticket, symbol=symbol):
                self.assertIsNone(opening_order_evidence(order, ticket, symbol))

    def test_invalid_required_native_fields_return_no_evidence(self):
        for changes in (dict(type=True), dict(type=-1), dict(type=1.5), dict(type=256),
                        dict(price_open=0), dict(price_open=float('nan')),
                        dict(price_open=float('inf')), dict(price_open=True),
                        dict(price_open=10**1000)):
            with self.subTest(changes=str(changes)[:80]):
                self.assertIsNone(opening_order_evidence(self.order(**changes), '33046869', 'SPX500'))

    def test_optional_bad_fields_are_omitted_without_affecting_valid_core(self):
        result = opening_order_evidence(self.order(time_done_msc=float('nan'),
            state=True, volume_current=-1, sl=float('inf')), '33046869', 'SPX500',
            submitted_price=float('inf'), fill_price=False)
        self.assertEqual(result['acceptedOrderPrice'], 7648.20)
        for name in ('doneTimeMsc', 'orderState', 'remainingVolume', 'sl', 'submittedPrice', 'fillPrice'):
            self.assertNotIn(name, result)
        json.dumps(result, allow_nan=False)

    def test_preserves_raw_order_type_without_inventing_broker_guarantees(self):
        for native_type in range(8):
            result = opening_order_evidence(self.order(type=native_type), '33046869', 'SPX500')
            self.assertEqual(result['orderType'], native_type)
            self.assertNotIn('priceGuaranteed', result)
            self.assertNotIn('brokerViolation', result)


if __name__ == '__main__':
    unittest.main()
