"""Isolated chunk-entry tests; no MetaTrader5 import or live trading APIs."""
import copy
import json
import pathlib
import sqlite3
import sys
import threading
import time
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'payload/vps'))
from chunk_entry import ChunkEntries, tranche_plan


class Worker:
    def __init__(self, primary, bridge):
        self.primary, self.bridge = primary, bridge

    def call(self, method, *args):
        if self.primary.lock._is_owned():
            raise AssertionError('Worker RPC while the primary lock is held')
        return getattr(self.bridge, method)(*args)


class FakeBridge:
    def __init__(self, account='900000002'):
        self.lock = threading.RLock()
        self.db = sqlite3.connect(':memory:', check_same_thread=False)
        self.config = dict(account=account, symbols=['USTEC'], tradingEnabled=True)
        self.linked = {}
        self.login = N(login=int(account), currency='USD', server='Example-MT5', margin_mode=2,
                       trade_allowed=True, trade_expert=True)
        self.price = dict(bid=98.8, ask=99.)
        self.stale = False
        self.tick_offset = 0.
        self.point = 1.
        self.spec = dict(quantityStep=.1, minQty=.1, maxQty=100.)
        self.rows = []
        self.sent = []
        self.checks = []
        self.outcome = None
        self.raise_send = False
        self.ticket = 100
        self.deadline_checks = 0
        self.send_hook = None

    def account(self):
        if self.login.server != 'Example-MT5':
            raise ValueError('Account/server mismatch')
        return self.login

    def snapshot(self, symbol):
        self.account()
        return dict(account=self.login.login, server=self.login.server,
                    quote={**self.price, 'utcTime': time.time() + self.tick_offset - (2 if self.stale else 0)},
                    metadata={**self.spec, 'contractSize': self.point, 'tickSize': .1})

    def entry_snapshot(self, symbol):
        account = self.account()
        if str(account.login) != str(self.config['account']) or account.currency != 'USD':
            raise ValueError('Chunk entry account identity or currency mismatch')
        if account.margin_mode != 2:
            raise ValueError('Chunk entry requires a hedging account on every participating terminal')
        if account.trade_allowed is False or account.trade_expert is False:
            raise ValueError('Account does not permit algorithmic trading')
        return self.snapshot(symbol)

    def lot_specs(self):
        return {'USTEC': self.spec}

    def requested_followers(self, payload):
        result = {}
        for row in payload.get('mirrorAccounts', []):
            if row['account'] not in self.linked:
                raise ValueError('Unknown linked account')
            result[row['account']] = row['multiplier']
        return result

    def validate_deadline(self, payload):
        self.deadline_checks += 1
        if payload['closeAtUtc'] <= time.time() * 1000 or payload['cancelPendingAtDeadline'] is not True:
            raise ValueError('Invalid timed exit')

    def check_opening(self, payload):
        self.account()
        if not self.config['tradingEnabled']:
            raise ValueError('Read only')
        self.checks.append(copy.deepcopy(payload))
        return True

    def local_management(self, identifiers=(), watch=()):
        self.account()
        return dict(account=self.login.login, server=self.login.server,
                    orders=copy.deepcopy(self.rows), openingStates={})

    def entry_management(self, watch=()):
        return self.local_management((), watch)

    def _fill(self, payload):
        self.ticket += 1
        price = self.price['ask' if payload['side'] == 'buy' else 'bid']
        payload = copy.deepcopy(payload)
        distance = payload.get('entryGuard', {}).get('stopDistance')
        if distance is not None:
            payload['sl'] = round(price - (1 if payload['side'] == 'buy' else -1) * distance, 8)
        outcome = dict(status='ok', providerOrderId=str(self.ticket), filledVolume=payload['volume'], fillPrice=price)
        if distance is not None:
            outcome.update(submittedPrice=price, sl=payload['sl'])
        if self.outcome:
            outcome.update(self.outcome)
        self.sent.append(copy.deepcopy(payload))
        self.rows.append(dict(ticket=str(self.ticket), identifier=str(self.ticket), symbol=payload['symbol'],
                              type=payload['side'], lots=outcome['filledVolume'], open_price=outcome['fillPrice'],
                              sl=payload['sl'], tp=payload['tp'], __isPosition=True))
        return outcome

    def execute(self, operation, payload):
        self.check_opening(payload)
        if self.raise_send:
            raise OSError('Broker reply lost')
        if self.send_hook:
            self.send_hook(payload)
        return {'result': self._fill(payload)}

    def group_execute(self, operation, payload, volumes=None, guards=None):
        accounts = {str(self.config['account']): self, **{key: worker.bridge for key, worker in self.linked.items()}}
        requests = {account: {**copy.deepcopy(payload), 'volume': volumes[account], 'entryGuard': guards[account]}
                    for account in volumes}
        for account, request in requests.items():
            accounts[account].check_opening(request)
        if self.send_hook:
            self.send_hook(payload)
        if self.raise_send:
            raise OSError('Broker reply lost')
        results = {account: accounts[account]._fill(request) for account, request in requests.items()}
        status = 'ok' if all(row['status'] == 'ok' for row in results.values()) else 'unknown'
        return {'result': dict(status=status, primaryAccount=str(self.config['account']), accountResults=results)}


class CommitProbe:
    """Keep real SQLite transactions while observing/failing a chosen commit."""
    def __init__(self, db, callback):
        self.db, self.callback = db, callback

    def __getattr__(self, name):
        return getattr(self.db, name)

    def commit(self):
        self.callback(self.db)
        return self.db.commit()


class ChunkEntryTests(unittest.TestCase):
    def setUp(self):
        self.now = 1792209600.
        self.clock = patch('chunk_entry.time.time', return_value=self.now)
        self.clock.start()
        self.new_bridges()
        self.payload = dict(id='entry', symbol='USTEC', side='buy', trigger=100., volume=1., chunkVolume=.2,
                            maxGap=1., riskUsd=20., sl=90., tp=130., level=100., cid='test',
                            mirrorAccounts=[dict(account='900000001', multiplier=1.)], expiresAt=self.now * 1000 + 3000,
                            closeAtUtc=self.now * 1000 + 3600000, closeTimeZone='America/New_York',
                            closeLocalDate='2026-10-17', closeLocalTime='15:00', cancelPendingAtDeadline=True)

    def new_bridges(self):
        if getattr(self, 'engine', None):
            self.engine.close()
        self.bridge = FakeBridge()
        self.follower = FakeBridge('900000001')
        self.bridge.linked['900000001'] = Worker(self.bridge, self.follower)
        self.engine = ChunkEntries(self.bridge)

    def tearDown(self):
        self.engine.close()
        self.clock.stop()

    def state(self):
        return self.engine.list()[0]

    def internal(self):
        return self.engine._load('entry')[2]

    def prices(self, price, side='buy', follower=None):
        for bridge, value in ((self.bridge, price), (self.follower, price if follower is None else follower)):
            bridge.price = dict(bid=value - .2, ask=value) if side == 'buy' else dict(bid=value, ask=value + .2)

    def first(self):
        self.engine.arm(self.payload)
        self.prices(100.)
        self.engine.tick()

    def test_five_sequential_buy_chunks_use_actual_first_fill_and_fixed_protection(self):
        self.engine.arm(self.payload)
        self.engine.tick()
        self.assertEqual(self.bridge.sent, [])
        self.prices(100.4)
        for _ in range(5):
            self.engine.tick()
        state = self.state()
        self.assertEqual(state['phase'], 'completed')
        self.assertEqual(state['nextChunk'], 5)
        self.assertEqual([row['volume'] for row in self.bridge.sent], [.2] * 5)
        self.assertEqual([row['id'] for row in self.bridge.sent], ['entry:c' + str(i) for i in range(1, 6)])
        for details in state['accounts'].values():
            self.assertEqual(details['anchor'], 100.4)
            self.assertEqual(details['filledVolume'], 1.)
            self.assertAlmostEqual(details['riskUsed'], 10.4)
        for row in self.bridge.sent + self.follower.sent:
            self.assertEqual((row['sl'], row['tp']), (90., 130.))
            self.assertLessEqual(row['expiresAt'] - self.now * 1000, 3000)

    def test_sell_uses_bid_and_has_mirrored_pause_resume(self):
        self.payload.update(side='sell', sl=110., tp=70.)
        self.prices(101., 'sell')
        self.engine.arm(self.payload)
        self.prices(100., 'sell'); self.engine.tick()
        self.prices(98.8, 'sell'); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        self.prices(99.5, 'sell'); self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 2)
        self.assertEqual(self.state()['accounts']['900000002']['anchor'], 100.)

    def test_large_spread_alone_does_not_block_buy_when_ask_is_allowed(self):
        self.first()
        self.bridge.price = self.follower.price = dict(bid=95., ask=100.5)
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 2)

    def test_any_account_price_gap_pauses_every_account_and_resumes(self):
        self.first()
        self.prices(100.5, follower=101.2)
        self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))
        self.prices(100.4); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'running')
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (2, 2))

    def test_favorable_price_passes_and_gap_is_not_reset_per_tranche(self):
        self.first()
        self.prices(99.5); self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 2)
        self.prices(101.1); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        self.assertEqual(len(self.bridge.sent), 2)

    def test_price_more_than_two_initial_r_stops_permanently(self):
        self.first()
        self.prices(120.1); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.prices(100.); self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 1)

    def test_jump_more_than_two_r_before_first_fill_does_not_send(self):
        self.engine.arm(self.payload)
        self.prices(120.1); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertEqual(self.bridge.sent, [])

    def test_stale_tick_pauses_and_fresh_tick_can_resume(self):
        self.engine.arm(self.payload)
        self.prices(100.)
        self.follower.stale = True
        self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        self.assertEqual(self.bridge.sent, [])
        self.follower.stale = False
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 1)

    def test_budget_is_total_actual_risk_and_final_guard_has_remaining_price(self):
        self.payload['riskUsd'] = 3.
        self.first()
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 1)
        state = self.state()
        self.assertEqual(state['phase'], 'paused')
        self.assertEqual(state['accounts']['900000002']['riskUsed'], 2.)
        self.assertEqual(self.bridge.sent[0]['entryGuard']['maxRiskPrice'], 105.)
        self.prices(94.)
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 2)
        self.assertAlmostEqual(self.state()['accounts']['900000002']['riskUsed'], 2.8)

    def test_actual_fill_over_budget_halts_without_claiming_remaining_chunks(self):
        self.payload['riskUsd'] = 2.1
        self.bridge.outcome = dict(fillPrice=102.)
        self.first()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertEqual(len(self.bridge.sent), 1)

    def test_actual_fill_outside_price_guard_keeps_only_first_chunk(self):
        self.bridge.outcome = dict(fillPrice=101.2)
        self.first()
        state = self.state()
        self.assertEqual(state['phase'], 'stopped')
        self.assertTrue(state['children'][0]['fillGuardViolated'])
        self.assertEqual(state['accounts']['900000002']['anchor'], 101.2)
        self.assertEqual(state['accounts']['900000002']['filledVolume'], .2)
        self.assertEqual(state['children'][0]['positions']['900000002']['volume'], .2)
        self.assertEqual(self.internal()['children'][0]['guards']['900000002']['anchor'], 100.)
        self.engine.tick()
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))

    def test_unknown_or_partial_one_account_stops_both_and_is_not_repeated(self):
        self.follower.outcome = dict(status='unknown', filledVolume=.1)
        self.first()
        self.assertEqual(self.state()['phase'], 'unknown')
        for _ in range(3):
            self.engine.tick()
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))
        self.assertEqual(len(self.state()['children']), 1)

    def test_zero_fill_or_unseen_position_is_unknown_and_does_not_continue(self):
        self.follower.outcome = dict(filledVolume=0.)
        self.first()
        self.assertEqual(self.state()['phase'], 'unknown')
        self.assertEqual(self.state()['nextChunk'], 0)

    def test_durable_claim_exists_before_send_and_exception_never_retries(self):
        def check(_payload):
            self.assertEqual(self.state()['phase'], 'sending')
            self.assertEqual(self.state()['children'][0]['state'], 'sending')
        self.bridge.send_hook = check
        self.bridge.raise_send = True
        self.first()
        self.assertEqual(self.state()['phase'], 'unknown')
        self.engine.tick()
        self.assertEqual(len(self.state()['children']), 1)

    def test_same_parent_replay_after_expiry_returns_state_and_changed_payload_rejects(self):
        original = self.engine.arm(self.payload)
        replay = {**self.payload, 'expiresAt': 1.}
        self.assertEqual(self.engine.arm(replay), original)
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.engine.arm({**self.payload, 'chunkVolume': .5})

    def test_restart_never_resumes_armed_or_claimed_parents(self):
        self.engine.arm(self.payload)
        restored = ChunkEntries(self.bridge)
        self.assertEqual(restored.list()[0]['phase'], 'stopped')
        state = self.internal()
        state.update(phase='sending', children=[dict(id='entry:c1', state='sending')])
        self.engine._save(state)
        restored = ChunkEntries(self.bridge)
        self.assertEqual(restored.list()[0]['phase'], 'unknown')
        restored.tick()
        self.assertEqual(self.bridge.sent, [])

    def test_manual_close_partial_close_or_changed_sl_stops_without_reentry(self):
        for change in ('close', 'volume', 'sl'):
            with self.subTest(change=change):
                self.new_bridges()
                self.first()
                if change == 'close':
                    self.follower.rows.clear()
                elif change == 'volume':
                    self.follower.rows[0]['lots'] = .1
                else:
                    self.follower.rows[0]['sl'] = 91.
                self.engine.tick()
                self.assertEqual(self.state()['phase'], 'stopped')
                self.assertEqual(len(self.bridge.sent), 1)

    def test_stop_command_is_durable_idempotent_even_after_expiry(self):
        self.first()
        stop = dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000)
        stopped = self.engine.stop(stop)
        self.assertEqual(stopped['phase'], 'stopped')
        self.assertEqual(self.engine.stop({**stop, 'expiresAt': 1.}), stopped)
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent), 1)
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.engine.stop({**stop, 'entryId': 'other'})

    def test_deadline_and_touched_stop_or_take_do_not_allow_more_chunks(self):
        self.first()
        self.prices(90.); self.engine.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertEqual(len(self.bridge.sent), 1)

    def test_expiry_deadline_and_original_side_are_required_before_arming(self):
        for changes in ({'expiresAt': 1.}, {'closeAtUtc': 0.}, {'side': 'bad'}, {'chunkVolume': .25}, {'maxGap': -1.}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.engine.arm({**self.payload, **changes})
        self.prices(100.)
        with self.assertRaisesRegex(ValueError, 'already been crossed'):
            self.engine.arm(self.payload)
        self.assertEqual(self.bridge.sent, [])

    def test_both_accounts_must_be_usd_hedging_and_correct_identity(self):
        for changes in ({'currency': 'EUR'}, {'margin_mode': 0}, {'server': 'Wrong'}, {'login': 111}):
            previous = copy.deepcopy(self.follower.login)
            for key, value in changes.items():
                setattr(self.follower.login, key, value)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.engine.arm(self.payload)
            self.follower.login = previous
        self.assertEqual(self.bridge.sent, [])

    def test_cumulative_coefficient_rounding_preserves_point_seven_target(self):
        self.payload['mirrorAccounts'][0]['multiplier'] = .7
        self.engine.arm(self.payload)
        details = self.internal()['accounts']['900000001']
        self.assertEqual(details['targetVolume'], .7)
        self.assertEqual(details['parts'], [.1, .1, .2, .1, .2])
        self.prices(100.)
        for _ in range(5):
            self.engine.tick()
        self.assertEqual(self.state()['phase'], 'completed')
        self.assertAlmostEqual(sum(row['volume'] for row in self.follower.sent), .7)

    def test_impossible_zero_follower_tranches_rejected_upfront(self):
        self.payload['mirrorAccounts'][0]['multiplier'] = .3
        with self.assertRaisesRegex(ValueError, 'valid lot in every tranche'):
            self.engine.arm(self.payload)
        self.assertEqual(self.engine.list(), [])

    def test_single_account_uses_existing_execute_path_and_entry_guard(self):
        self.payload.pop('mirrorAccounts')
        self.first()
        self.assertEqual(len(self.bridge.sent), 1)
        self.assertEqual(self.follower.sent, [])
        self.assertIn('entryGuard', self.bridge.sent[0])

    def test_metadata_is_retained_for_card_recovery(self):
        self.payload.update(tickSize=.1, stopMode='level', stopOffsetPts=100)
        self.engine.arm(self.payload)
        state = self.state()
        self.assertEqual((state['tickSize'], state['stopMode'], state['stopOffsetPts']), (.1, 'level', 100))

    def test_different_request_id_cannot_duplicate_an_active_direction(self):
        self.engine.arm(self.payload)
        with self.assertRaisesRegex(ValueError, 'already active'):
            self.engine.arm({**self.payload, 'id': 'different', 'trigger': 101.})
        self.assertEqual(len(self.engine.list()), 1)

    def test_future_tick_over_250ms_pauses_before_sending(self):
        self.engine.arm(self.payload)
        self.prices(100.)
        self.follower.tick_offset = .3
        self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        self.assertEqual(self.bridge.sent, [])

    def test_requested_stop_during_broker_wait_blocks_next_tranche(self):
        stopper = []
        result = []
        def stop_while_waiting(_payload):
            thread = threading.Thread(target=lambda: result.append(self.engine.stop(
                dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000))))
            stopper.append(thread)
            thread.start()
            for _ in range(100):
                if self.engine._stopping('entry'):
                    break
                threading.Event().wait(.001)
            self.assertTrue(self.engine._stopping('entry'))
        self.bridge.send_hook = stop_while_waiting
        self.first()
        stopper[0].join(timeout=2)
        self.assertFalse(stopper[0].is_alive())
        self.assertEqual(result[0]['phase'], 'stopped')
        self.engine.tick()
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))

    def test_no_database_write_on_unchanged_waiting_or_paused_cycles(self):
        self.engine.arm(self.payload)
        writes = self.bridge.db.total_changes
        for _ in range(10):
            self.engine.tick()
        self.assertEqual(self.bridge.db.total_changes, writes)
        self.prices(102.)
        self.engine.tick()
        self.assertEqual(self.state()['phase'], 'paused')
        writes = self.bridge.db.total_changes
        self.engine.tick()
        self.assertEqual(self.bridge.db.total_changes, writes)

    def test_final_guards_include_all_previous_position_tickets_and_protection(self):
        self.first()
        self.assertEqual(self.bridge.sent[0]['entryGuard']['positions'], [])
        self.engine.tick()
        prior = self.bridge.sent[1]['entryGuard']['positions']
        self.assertEqual(prior, [dict(ticket='101', volume=.2, sl=90., tp=130.)])
        self.engine.tick()
        self.assertEqual(len(self.bridge.sent[2]['entryGuard']['positions']), 2)

    def test_expired_or_invalid_stop_does_not_raise_in_memory_stop_flag(self):
        self.engine.arm(self.payload)
        for changes in ({'expiresAt': 1.}, {'id': 'bad id'}, {'entryId': 'bad id'}):
            command = dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000)
            command.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.engine.stop(command)
        self.assertFalse(self.engine._stopping('entry'))
        self.assertEqual(self.state()['phase'], 'armed')

    def test_autoexit_deadline_stops_partial_entry_without_sending_more(self):
        self.first()
        with patch('chunk_entry.time.time', return_value=self.now + 3601):
            self.engine.tick()
        self.assertEqual(self.state()['phase'], 'stopped')
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))

    def test_public_responses_strip_large_internal_timings_but_keep_durable_guards(self):
        self.first()
        state = self.internal()
        state['children'][0]['result']['timings'] = {'large': 'x' * 1200000}
        state['children'][0]['result']['accountResults']['900000002']['timings'] = {'large': 'x' * 1000}
        self.engine._save(state)
        self.assertGreater(len(json.dumps(self.internal())), 1024 * 1024)
        for public in (self.engine.list()[0], self.engine.arm(self.payload), self.engine.stop(
                dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000))):
            self.assertLess(len(json.dumps(public)), 10000)
            self.assertNotIn('previousPrice', public)
            self.assertNotIn('triggered', public)
            self.assertNotIn('parts', public['accounts']['900000002'])
            self.assertNotIn('spec', public['accounts']['900000002'])
            self.assertNotIn('guards', public['children'][0])
            self.assertEqual(set(public['children'][0]['result']), {'status', 'primaryAccount'})
            position = public['children'][0]['positions']['900000002']
            self.assertEqual((position['ticket'], position['volume'], position['fillPrice']), ('101', .2, 100.))
            self.assertNotIn('openingTicket', position)
        durable = self.internal()
        self.assertEqual(durable['children'][0]['guards']['900000002']['anchor'], 100.)
        self.assertEqual(len(durable['children'][0]['result']['timings']['large']), 1200000)
        self.assertEqual(durable['accounts']['900000002']['parts'], [.2] * 5)

    def test_unknown_public_receipts_keep_account_outcomes_without_timings(self):
        self.follower.outcome = dict(status='unknown', filledVolume=.1)
        self.first()
        state = self.internal()
        state['children'][0]['result']['accountResults']['900000001']['timings'] = {'large': 'x' * 100000}
        self.engine._save(state)
        receipt = self.state()['children'][0]['result']['accountResults']['900000001']
        self.assertEqual((receipt['status'], receipt['providerOrderId'], receipt['filledVolume']), ('unknown', '101', .1))
        self.assertNotIn('timings', receipt)
        self.assertIn('timings', self.internal()['children'][0]['result']['accountResults']['900000001'])

    def fabricated(self, identifier, phase, account_count=2):
        accounts = [str(1234567890123456 + index) for index in range(account_count)]
        outcome = dict(status='ok', providerOrderId='12345678901234567890', filledVolume=.2, fillPrice=100.12345678)
        return dict(id=identifier, symbol='USTEC', side='buy', phase=phase, reason='Entry complete',
                    trigger=100., volume=10., chunkVolume=.2, maxGap=1., riskUsd=100., sl=90., tp=130., level=100.,
                    closeAtUtc=self.now * 1000 + 3600000, createdAt=self.now * 1000, updatedAt=self.now * 1000,
                    nextChunk=50, childCount=50, accounts={account: dict(multiplier=1., targetVolume=10., filledVolume=10.,
                    anchor=100., riskUsd=100., riskUsed=100., parts=[.2] * 50, spec={}) for account in accounts},
                    children=[dict(id=identifier + ':c' + str(index + 1), index=index + 1, state='confirmed',
                                   volumes={account: .2 for account in accounts},
                                   positions={account: dict(ticket='12345678901234567890', openingTicket='12345678901234567890',
                                                           volume=.2, fillPrice=100.12345678) for account in accounts},
                                   result={**outcome, 'primaryAccount': accounts[0], 'accountResults': {account: outcome for account in accounts}},
                                   guards={account: {'anchor': 100., 'maxGap': 1.} for account in accounts}) for index in range(50)])

    def test_public_list_prioritizes_older_active_and_unknown_with_byte_budget(self):
        for index in range(35):
            identifier = str(index).zfill(100)
            phase = 'running' if index < 4 else 'unknown' if index == 4 else 'stopped'
            self.engine._remember(self.fabricated(identifier, phase))
        records = self.engine.list()
        self.assertEqual([row['phase'] for row in records[:5]], ['running'] * 4 + ['unknown'])
        self.assertEqual({row['id'] for row in records[:4]}, {str(index).zfill(100) for index in range(4)})
        self.assertLessEqual(len(records), 30)
        self.assertLessEqual(len(json.dumps(records, separators=(',', ':')).encode()), 512 * 1024)
        self.assertLess(len(records), 30)  # The byte budget also bounds large histories.

    def test_all_four_active_seventeen_account_plans_fit_maximum_public_allowance(self):
        for index in range(4):
            self.engine._remember(self.fabricated(str(index).zfill(100), 'running', account_count=17))
        records = self.engine.list()
        self.assertEqual(len(records), 4)
        self.assertLessEqual(len(json.dumps(records, separators=(',', ':')).encode()), 768 * 1024)

    def test_stop_before_delayed_arm_persists_cancellation_and_sends_nothing(self):
        stop = dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000)
        cancelled = self.engine.stop(stop)
        self.assertEqual((cancelled['id'], cancelled['phase']), ('entry', 'stopped'))
        self.assertEqual(cancelled['accounts'], {})
        self.assertEqual(self.engine.stop({**stop, 'expiresAt': 1.}), cancelled)
        self.assertEqual(self.engine.arm(self.payload), cancelled)
        self.prices(100.)
        self.engine.tick()
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))
        self.assertIsNone(self.engine._load('entry'))
        self.assertEqual(self.bridge.db.execute('SELECT entry_id,stop_id FROM chunk_entry_cancels').fetchall(), [('entry', 'stop-entry')])

    def test_cancellation_survives_restart_and_blocks_expired_late_arm(self):
        stop = dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000)
        cancelled = self.engine.stop(stop)
        restored = ChunkEntries(self.bridge)
        self.assertEqual(restored.arm({**self.payload, 'expiresAt': 1.}), cancelled)
        self.assertEqual(restored.stop({**stop, 'expiresAt': 1.}), cancelled)
        self.assertEqual(self.bridge.checks, [])
        self.assertEqual(self.follower.checks, [])

    def test_expired_stop_cannot_create_a_cancellation_tombstone(self):
        with self.assertRaisesRegex(ValueError, 'expired'):
            self.engine.stop(dict(id='stop-entry', entryId='entry', expiresAt=1.))
        self.assertEqual(self.bridge.db.execute('SELECT COUNT(*) FROM chunk_entry_cancels').fetchone()[0], 0)
        self.assertEqual(self.bridge.db.execute('SELECT COUNT(*) FROM chunk_entry_stops').fetchone()[0], 0)
        self.assertEqual(self.engine.arm(self.payload)['phase'], 'armed')

    def test_active_index_and_public_arm_appear_only_after_durable_commit(self):
        observed = []
        def during_commit(db):
            observed.append((set(self.engine.active_ids), self.engine.list()))
            self.assertEqual(json.loads(db.execute('SELECT state FROM chunk_entries').fetchone()[0])['phase'], 'armed')
        self.bridge.db = CommitProbe(self.bridge.db, during_commit)
        self.engine.arm(self.payload)
        self.assertEqual(observed, [(set(), [])])
        self.assertEqual(self.engine.active_ids, {'entry'})
        self.assertEqual(self.state()['phase'], 'armed')

    def test_failed_arm_commit_does_not_index_publish_or_leave_uncommitted_parent(self):
        def fail(_db):
            raise sqlite3.OperationalError('Simulated durable write failure')
        self.bridge.db = CommitProbe(self.bridge.db, fail)
        with self.assertRaisesRegex(sqlite3.OperationalError, 'durable write failure'):
            self.engine.arm(self.payload)
        self.assertEqual(self.engine.active_ids, set())
        self.assertEqual(self.engine.list(), [])
        self.assertIsNone(self.engine._load('entry'))
        self.assertFalse(self.bridge.db.in_transaction)
        self.engine.tick()
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))

    def test_failed_child_claim_does_not_publish_or_send_and_parent_remains_evaluated(self):
        self.engine.arm(self.payload)
        db = self.bridge.db
        def fail(_db):
            self.assertEqual(self.engine.active_ids, {'entry'})
            self.assertEqual(self.state()['phase'], 'armed')
            raise sqlite3.OperationalError('Claim commit failed')
        self.bridge.db = CommitProbe(db, fail)
        self.prices(100.)
        with self.assertRaisesRegex(sqlite3.OperationalError, 'Claim commit failed'):
            self.engine.tick()
        self.assertEqual(self.internal()['phase'], 'armed')
        self.assertEqual(self.internal()['children'], [])
        self.assertEqual(self.engine.active_ids, {'entry'})
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))
        self.bridge.db = db
        self.engine.tick()
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))

    def test_failed_confirm_save_becomes_unknown_and_is_not_evaluated_again(self):
        self.engine.arm(self.payload)
        failed = []
        def fail_confirmation(db):
            state = json.loads(db.execute('SELECT state FROM chunk_entries WHERE id=?', ('entry',)).fetchone()[0])
            if state['phase'] == 'running' and not failed:
                failed.append(True)
                raise sqlite3.OperationalError('Confirmation commit failed')
        self.bridge.db = CommitProbe(self.bridge.db, fail_confirmation)
        self.prices(100.)
        self.engine.tick()
        self.assertEqual(self.state()['phase'], 'unknown')
        self.assertEqual(self.engine.active_ids, set())
        self.assertEqual(self.internal()['children'][0]['state'], 'unknown')
        self.engine.tick()
        self.assertEqual((len(self.bridge.sent), len(self.follower.sent)), (1, 1))

    def test_failed_stop_commit_retains_prior_index_until_successful_explicit_retry(self):
        self.engine.arm(self.payload)
        db = self.bridge.db
        def fail(_db):
            self.assertEqual(self.engine.active_ids, {'entry'})
            self.assertEqual(self.state()['phase'], 'armed')
            raise sqlite3.OperationalError('Stop commit failed')
        self.bridge.db = CommitProbe(db, fail)
        command = dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000)
        with self.assertRaisesRegex(sqlite3.OperationalError, 'Stop commit failed'):
            self.engine.stop(command)
        self.assertEqual(self.engine.active_ids, {'entry'})
        self.assertEqual(self.internal()['phase'], 'armed')
        self.assertFalse(self.bridge.db.in_transaction)
        self.bridge.db = db
        self.assertEqual(self.engine.stop(command)['phase'], 'stopped')
        self.assertEqual(self.engine.active_ids, set())
        self.engine.tick()
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))

    def test_terminal_phase_transitions_remove_only_committed_active_parent(self):
        for phase in ('completed', 'stopped', 'unknown'):
            with self.subTest(phase=phase):
                self.new_bridges()
                self.engine.arm(self.payload)
                self.engine._phase(self.internal(), phase, 'Fixture terminal transition')
                self.assertEqual(self.engine.active_ids, set())
                self.assertEqual(self.internal()['phase'], phase)
                with patch.object(self.engine, '_tick_entry') as evaluate:
                    self.engine.tick()
                evaluate.assert_not_called()
                # Unknown receipts and existing-ID dedup remain in the ledger.
                self.assertEqual(self.engine.arm({**self.payload, 'expiresAt': 1.})['phase'], phase)

    def test_running_paused_and_sending_remain_in_active_index(self):
        self.engine.arm(self.payload)
        for phase in ('running', 'paused', 'sending', 'armed'):
            self.engine._phase(self.internal(), phase, 'Fixture active transition')
            self.assertEqual(self.engine.active_ids, {'entry'})
            with patch.object(self.engine, '_tick_entry') as evaluate:
                self.engine.tick()
            self.assertEqual([args[0][1]['id'] for args in evaluate.call_args_list], ['entry'])

    def test_restart_commits_stop_or_unknown_before_empty_active_index(self):
        for phase, claimed in (('armed', False), ('paused', False), ('running', False), ('sending', True)):
            with self.subTest(phase=phase):
                self.new_bridges()
                self.engine.arm(self.payload)
                state = self.internal()
                state.update(phase=phase, children=[dict(id='entry:c1', state='sending')] if claimed else [])
                self.engine._save(state)
                restored = ChunkEntries(self.bridge)
                self.assertEqual(restored.active_ids, set())
                expected = 'unknown' if claimed else 'stopped'
                self.assertEqual(restored._load('entry')[2]['phase'], expected)
                self.assertEqual(restored.arm({**self.payload, 'expiresAt': 1.})['phase'], expected)
                with patch.object(restored, '_tick_entry') as evaluate:
                    restored.tick()
                evaluate.assert_not_called()

    def test_cancellation_tombstone_does_not_add_an_active_parent(self):
        self.engine.stop(dict(id='stop-entry', entryId='entry', expiresAt=self.now * 1000 + 2000))
        self.assertEqual(self.engine.active_ids, set())
        self.assertEqual(self.engine.arm(self.payload)['phase'], 'stopped')
        self.assertEqual(self.engine.active_ids, set())
        queries = []
        self.bridge.db.set_trace_callback(queries.append)
        self.engine.tick()
        self.assertEqual(queries, [])

    def test_thousand_completed_parents_do_not_change_active_evaluations_or_order(self):
        history = [(f'old-{index}', '', '{}', json.dumps(dict(id=f'old-{index}', phase='completed', children=[], accounts={})))
                   for index in range(1000)]
        self.bridge.db.executemany('INSERT INTO chunk_entries VALUES (?,?,?,?)', history)
        self.bridge.db.commit()
        self.bridge.config['symbols'].append('SPX500')
        for bridge in (self.bridge, self.follower):
            bridge.lot_specs = lambda: dict(USTEC=self.bridge.spec, SPX500=self.bridge.spec)
        identifiers = ['z-first', 'a-second', 'm-third', 'b-fourth']
        for identifier, symbol, side in zip(identifiers, ('USTEC', 'USTEC', 'SPX500', 'SPX500'), ('buy', 'sell', 'buy', 'sell')):
            payload = {**self.payload, 'id': identifier, 'symbol': symbol, 'side': side}
            if side == 'sell':
                payload.update(trigger=98., sl=110., tp=70.)
            self.engine.arm(payload)
        queries = []
        self.bridge.db.set_trace_callback(queries.append)
        decode = json.loads
        with patch('chunk_entry.json.loads', wraps=decode) as loads, patch.object(self.engine, '_tick_entry') as evaluate:
            self.engine.tick()
        self.bridge.db.set_trace_callback(None)
        self.assertEqual([args[0][1]['id'] for args in evaluate.call_args_list], identifiers)
        self.assertEqual(loads.call_count, 8)  # Payload + state only for each active parent.
        self.assertEqual(len(queries), 1)
        self.assertIn('WHERE id IN', queries[0])
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))

    def test_missing_indexed_parent_fails_closed_instead_of_skipping_validation(self):
        self.engine.arm(self.payload)
        self.bridge.db.execute('DELETE FROM chunk_entries WHERE id=?', ('entry',)); self.bridge.db.commit()
        with self.assertRaisesRegex(ValueError, 'missing from the durable ledger'):
            self.engine.tick()
        with self.assertRaisesRegex(ValueError, 'missing from the durable ledger'):
            self.engine.arm({**self.payload, 'id': 'other'})
        self.assertEqual((self.bridge.sent, self.follower.sent), ([], []))

    def test_optional_chunk_percent_is_immutable_display_metadata_and_survives_restart(self):
        payload = {**self.payload, 'chunkPercent': 20.}
        entry = self.engine.arm(payload)
        self.assertEqual((entry['chunkPercent'], entry['chunkVolume'], entry['volume']), (20., .2, 1.))
        self.assertEqual(self.internal()['accounts']['900000002']['parts'], [.2] * 5)
        with self.assertRaisesRegex(ValueError, 'different parameters'):
            self.engine.arm({**payload, 'chunkPercent': 25.})
        restored = ChunkEntries(self.bridge)
        self.assertEqual(restored.arm({**payload, 'expiresAt': 1.})['chunkPercent'], 20.)
        self.assertEqual(restored.list()[0]['chunkPercent'], 20.)

    def test_chunk_percent_is_optional_and_invalid_display_metadata_rejects_before_admission(self):
        for value in (1., 101., 0., True, '20', None, float('nan'), float('inf')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.engine.arm({**self.payload, 'chunkPercent': value})
        self.assertEqual(self.engine.active_ids, set())
        self.assertNotIn('chunkPercent', self.engine.arm(self.payload))


if __name__ == '__main__':
    unittest.main()
