"""Known stale-quote refusal must not strand a protected parent or replay trades."""
import copy
import json
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import test_hybrid_protection as support
import test_entry_independence as independent


class PartialProtectionRecoveryTests(unittest.TestCase):
    def model(self):
        case = support.HybridProtectionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.make(accounts=2)
        case.entry()
        original = case.bridges[1].entry_common_stop
        def refused(request):
            case.natives[1].tick.time = case.now - 1.022
            return original(request)
        command = case.command(sl=24990., tp=25040., mirrorAccounts=[dict(account='900000001')])
        return case, command, refused

    def test_known_partial_refusal_releases_card_without_resending_and_retains_each_level(self):
        case, command, refused = self.model()
        with patch.object(case.bridges[1], 'entry_common_stop', side_effect=refused):
            response = case.engine.protection(command)
        self.assertEqual(response['result']['status'], 'rejected', response)
        self.assertTrue(response['result']['partialProtection'])
        self.assertNotIn('protectionPending', response['entry'])
        self.assertEqual(response['entry']['phase'], 'stopped')
        case.assert_native_protection(case.natives[0], 24990., 25040., 4)
        case.assert_native_protection(case.natives[1], tp=0., count=4)
        counts = [len(native.sent) for native in case.natives]
        self.assertEqual(case.engine.protection(command)['result'], response['result'])
        case.engine.reconcile_protection_rejection(command['id'])
        self.assertEqual([len(native.sent) for native in case.natives], counts)
        case.natives[1].tick.time = case.now
        self.assertEqual(case.close_card()['result']['status'], 'ok')

    def test_fresh_quote_after_pre_mutation_refusal_completes_both_accounts_once(self):
        case, command, _ = self.model()
        original = case.bridges[1].entry_common_stop
        calls = []
        def transient(request):
            calls.append(request['id'])
            case.natives[1].tick.time = case.now-1.022 if len(calls) == 1 else case.now
            return original(request)
        before = case.natives[1].protection_sends
        with patch.object(case.bridges[1], 'entry_common_stop', side_effect=transient):
            response = case.engine.protection(command)
        self.assertEqual(response['result']['status'], 'ok', response['result'])
        self.assertEqual(len(calls), 2)
        self.assertNotEqual(calls[0], calls[1])
        self.assertEqual(case.natives[1].protection_sends-before, 4)
        for native in case.natives:
            case.assert_native_protection(native, 24990., 25040., 4)

    def test_quote_wait_never_renews_expired_command_or_retries_unknown_send(self):
        case, command, _ = self.model()
        calls = []
        def expired(account, method, request):
            calls.append(request['id'])
            command['expiresAt'] = case.now*1000
            return dict(account=account, status='rejected', mutationStarted=False, retryableQuoteWait=True)
        request = dict(id='native', symbol='USTEC')
        with patch.object(case.engine, '_call', side_effect=expired):
            outcome, _ = case.engine._wait_parent_protection_quote('900000001', 'entry_common_stop', request,
                                                          command)
        self.assertEqual(calls, ['native'])
        self.assertEqual(outcome['status'], 'rejected')
        command['expiresAt'] = case.now*1000+3000
        with patch.object(case.engine, '_call', return_value=dict(status='unknown')) as call:
            outcome, _ = case.engine._wait_parent_protection_quote('900000001', 'entry_common_stop', request,
                                                          command)
        self.assertEqual(call.call_count, 1)
        self.assertEqual(outcome['status'], 'unknown')

    def test_legacy_unknown_receipt_reconciles_from_current_native_protection_without_sends(self):
        case, command, refused = self.model()
        with patch.object(case.engine, 'reconcile_protection_rejection'), \
                patch.object(case.bridges[1], 'entry_common_stop', side_effect=refused):
            response = case.engine.protection(command)
        self.assertEqual(response['result']['status'], 'unknown')
        # Observe a user's later manual adjustment; recovery never overwrites it.
        for position in case.natives[1].positions:
            position.sl, position.tp = 24992., 25045.
        counts = [len(native.sent) for native in case.natives]
        receipt = case.primary.receipt(command['id'])['result']
        self.assertEqual(receipt['status'], 'rejected')
        state = case.state()
        self.assertNotIn('protectionPending', state)
        self.assertEqual(state['accounts']['900000001']['sl'], 24992.)
        self.assertEqual(state['accounts']['900000001']['tp'], 25045.)
        self.assertEqual([len(native.sent) for native in case.natives], counts)

    def test_true_unknown_native_outcome_remains_locked(self):
        case, command, _ = self.model()
        case.natives[1].sl_failure = dict(at=case.natives[1].protection_sends + 1, apply=True, unknown=True)
        response = case.engine.protection(command)
        self.assertEqual(response['result']['status'], 'unknown')
        counts = [len(native.sent) for native in case.natives]
        self.assertEqual(case.primary.receipt(command['id'])['result']['status'], 'unknown')
        self.assertIn('protectionPending', case.state())
        self.assertEqual([len(native.sent) for native in case.natives], counts)

    def test_independent_v5_legacy_recovery_updates_both_shards_and_projection(self):
        case = independent.EntryIndependenceTests()
        self.addCleanup(case.doCleanups)
        f, root = case.model()
        f.engine.arm(f.payload)
        independent.support.IsolatedEngineTests.until(self, f, lambda state: state['phase'] == 'completed')
        original = f.bridges[1].entry_common_stop
        def refused(request):
            return dict(account='900000001', symbol=f.payload['symbol'], status='rejected',
                        retryableQuoteWait=True, mutationStarted=False, reason='Waiting for fresh quote')
        command = dict(id='partial-v5', entryId=f.payload['id'], symbol=f.payload['symbol'],
                       account='900000002', mirrorAccounts=[dict(account='900000001')],
                       operation='modify', sl=24990., tp=25040., expiresAt=independent.time.time()*1000+3000)
        with patch.object(f.engine, 'reconcile_protection_rejection'), \
                patch.object(f.bridges[1], 'entry_common_stop', side_effect=refused):
            # Reproduce a receipt written by the previous whole-parent
            # implementation, before independent account controls existed.
            response = independent.ChunkEntries.protection(f.engine, command)
        self.assertEqual(response['result']['status'], 'unknown', response)
        counts = [len(native.sent) for native in f.natives]
        self.assertEqual(root.receipt(command['id'])['result']['status'], 'rejected')
        for account in ('900000002', '900000001'):
            state = f.engine.shards[account]._load(f.payload['id'])[2]
            self.assertEqual(state['phase'], 'stopped')
            self.assertNotIn('protectionPending', state)
        f.engine._refresh_projections()
        state = f.engine.get(f.payload['id'])
        self.assertEqual(state['phase'], 'stopped')
        self.assertNotIn('protectionPending', state)
        self.assertEqual([len(native.sent) for native in f.natives], counts)

    def test_changed_owned_volume_or_unprotected_position_does_not_release_legacy_lock(self):
        for change in ('volume', 'sl'):
            case, command, refused = self.model()
            with patch.object(case.engine, 'reconcile_protection_rejection'), \
                    patch.object(case.bridges[1], 'entry_common_stop', side_effect=refused):
                case.engine.protection(command)
            position = case.natives[1].positions[0]
            if change == 'volume':
                position.volume -= .1
            else:
                position.sl = 0.
            counts = [len(native.sent) for native in case.natives]
            case.engine.reconcile_protection_rejection(command['id'])
            self.assertEqual(case.engine._protection_receipt(command['id'])[1]['status'], 'unknown')
            self.assertIn('protectionPending', case.state())
            self.assertEqual([len(native.sent) for native in case.natives], counts)


if __name__ == '__main__':
    unittest.main()
