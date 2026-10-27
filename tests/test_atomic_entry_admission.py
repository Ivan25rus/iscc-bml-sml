"""Authenticated local HTTP tests with fake MT5. Never connects to a broker."""
import copy
import hashlib
import pathlib
import unittest
import test_hybrid_protection as fixtures


class AtomicAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.f = fixtures.HybridProtectionTests()
        self.f.setUp()
        self.addCleanup(self.f.doCleanups)
        self.f.make(accounts=2)
        self.f.start_http()

    def payload(self):
        return dict(copy.deepcopy(self.f.payload), admissionContract='atomic-entry-admission-v1',
                    requiredCapabilities=['chunked-breakout','hybrid-market-limit-v1'])

    def test_single_post_checks_every_account_before_admitting_and_does_not_send(self):
        status, reply = self.f.request_api('POST','/entry-plan-admit',self.payload())
        self.assertEqual(status,200)
        self.assertEqual(reply['admissionContract'],'atomic-entry-admission-v1')
        self.assertEqual(reply['entry']['phase'],'armed')
        self.assertFalse(any(native.sent for native in self.f.natives))
        self.assertEqual(self.f.tick()['phase'],'completed')

    def test_bad_contract_capability_risk_or_follower_identity_has_no_opening_effect(self):
        for change in (dict(admissionContract='future-unsupported'),dict(requiredCapabilities=['missing-feature']),
                       dict(entryRiskLimitUsd=15.)):
            with self.subTest(change=change):
                status, reply = self.f.request_api('POST','/entry-plan-admit',{**self.payload(),**change})
                self.assertEqual(status,200)
                self.assertEqual(reply['entryResult']['status'],'rejected')
                self.assertFalse(any(native.sent for native in self.f.natives))
                self.assertIsNone(self.f.engine._load(self.f.payload['id']))
        self.f.natives[1].account.login=9999
        status, reply = self.f.request_api('POST','/entry-plan-admit',self.payload())
        self.assertEqual(reply['entryResult']['status'],'rejected')
        self.assertFalse(any(native.sent for native in self.f.natives))

    def test_runtime_hashes_identify_both_loaded_source_files(self):
        status, reply = self.f.request_api('GET','/health')
        self.assertEqual(status,200)
        root=pathlib.Path(__file__).resolve().parents[1]
        self.assertEqual(reply['build']['receiverSha256'],hashlib.sha256((root/'payload/vps/mt5_bridge.py').read_bytes()).hexdigest())
        self.assertEqual(reply['build']['entryModuleSha256'],hashlib.sha256((root/'payload/vps/chunk_entry.py').read_bytes()).hexdigest())
