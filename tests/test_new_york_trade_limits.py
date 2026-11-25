from datetime import datetime
import json
import sqlite3
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import test_isolated_entry_engines as fixtures
from mt5_bridge import Bridge, CoordinatorBridge, native_worker_method, new_york_zone
from new_york_trade_limits import SessionEntries, check_daily_budget, daily_pnl, risk_day_start, session_bounds


def ny_stamp(year, month, day, hour, minute):
    return datetime(year, month, day, hour, minute, tzinfo=new_york_zone()).timestamp()


class NewYorkTradeLimitTests(unittest.TestCase):
    def fixture(self):
        f = fixtures.IsolatedEngineTests.make(self)
        f.engine.close()
        f.primary.config.update(newYorkTradeLimits=True, entryMarketFirst=True)
        terminals = {str(bridge.config['account']): bridge.mt5 for bridge in f.bridges}

        class ExecutionWorker:
            def __init__(self, bridge):
                self.bridge, self.config = bridge, bridge.config

            def available(self):
                return True

            def call(self, method, *args):
                with self.bridge.lock:
                    return native_worker_method(self.bridge, method, args)

        class RiskReader:
            def __init__(self, config, path):
                assert config['_riskOnly'] and not config['tradingEnabled'] and not config['linkedAccounts']
                self.bridge = Bridge(terminals[str(config['account'])], dict(config, _nativeWorker=True), path)

            def available(self):
                return True

            def call(self, method, *args):
                return native_worker_method(self.bridge, method, args, risk_only=True)

            def close(self):
                self.bridge.chunk_entries.close()
                self.bridge.close_group_executor()
                self.bridge.db.close()

        root = CoordinatorBridge(f.primary.config, f.paths[0], ExecutionWorker(f.primary))
        root.linked = {str(bridge.config['account']): ExecutionWorker(bridge) for bridge in f.bridges[1:]}
        root.risk_client_factory = RiskReader
        f.engine = root.chunk_entries
        self.addCleanup(root.db.close)
        self.addCleanup(root.close_group_executor)
        self.addCleanup(f.engine.close)
        return f

    def test_boundaries_follow_new_york_in_both_seasons(self):
        for month, day in ((7, 20), (11, 20)):
            now = lambda hour, minute: ny_stamp(2026, month, day, hour, minute)
            self.assertIsNone(session_bounds(now(9, 29), new_york_zone()))
            self.assertEqual(session_bounds(now(9, 30), new_york_zone()), (now(9, 30)*1000, now(16, 0)*1000))
            self.assertIsNone(session_bounds(now(16, 0), new_york_zone()))
        self.assertIsNone(session_bounds(ny_stamp(2026, 11, 21, 10, 0), new_york_zone()))

    def test_daily_window_resets_at_close_including_dst_changes(self):
        zone = new_york_zone()
        for month, day, elapsed_hours in ((3, 8, 23), (7, 20, 24), (11, 1, 25), (11, 20, 24)):
            close = ny_stamp(2026, month, day, 16, 0)
            with self.subTest(month=month, day=day):
                self.assertEqual(risk_day_start(close - .001, zone), close - 3600 * elapsed_hours)
                self.assertEqual(risk_day_start(close, zone), close)
                self.assertEqual(risk_day_start(close + 8 * 3600, zone), close)

    def test_daily_risk_excludes_old_losses_at_close_and_rejects_pre_close_snapshot(self):
        f = self.fixture()
        bridge, native = f.bridges[0], f.natives[0]
        close = ny_stamp(2026, 11, 20, 16, 0)
        offset = bridge.broker_utc_offset_seconds
        native.history_deals_get = lambda *args: (SimpleNamespace(time_msc=(close - 30 + offset) * 1000, type=1, profit=-1900.),
                                                 SimpleNamespace(time_msc=(close + offset) * 1000, type=0, profit=-10.))
        native.account.balance = 98100.
        with patch('time.time', return_value=close - .001):
            before = bridge.daily_entry_risk()
            self.assertEqual((before['startBalance'], before['realized']), (100000., -1900.))
            with self.assertRaisesRegex(ValueError, '1,8%'):
                bridge._check_daily_entry_risk(native.account, (before, 0.), 250.)
        with patch('time.time', return_value=close):
            with self.assertRaisesRegex(ValueError, 'торговый день изменились'):
                bridge._check_daily_entry_risk(native.account, (before, 0.), 250.)
            native.account.balance = 98090.
            after = bridge.daily_entry_risk()
            self.assertEqual((after['windowStart'], after['startBalance'], after['realized']), (close, 98100., -10.))
            bridge._check_daily_entry_risk(native.account, (after, 0.), 250.)

    def test_health_reports_limit_and_reset_without_reading_history_or_sending_orders(self):
        f = self.fixture()
        for native in f.natives:
            native.account.balance = 100000.
            native.history_deals_get = lambda *args, **kwargs: self.fail('A health request read transaction history')
        health = f.engine.bridge.health()
        self.assertIn('new-york-trade-limits-v1', health['capabilities'])
        self.assertEqual(health['entryLimits'], dict(timeZone='America/New_York', sessionStart='09:30', sessionEnd='16:00',
                                                    maxSessionEntries=3, dailyRiskPercent=1.8, dailyRiskReset='16:00',
                                                    offSessionRiskPercent=1.8, dailyRiskPercentByAccount={}))
        self.assertFalse(any(native.sent for native in f.natives))

    def test_slots_survive_reload_and_count_roots_once(self):
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        db.execute('CREATE TABLE chunk_entries (id TEXT PRIMARY KEY, digest TEXT, payload TEXT, state TEXT)')
        started = ny_stamp(2026, 11, 20, 10, 0)
        states = [dict(phase='stopped', accounts={'main': {'filledVolume': count}, 'copy': {'filledVolume': count}},
                       children=[dict(state='confirmed')] if count else [], createdAt=started*1000) for count in (4, 0, 5)]
        for index, state in enumerate(states):
            db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)', (str(index), '', '', json.dumps(state)))
        storage = SimpleNamespace(lock=threading.RLock(), db=db)
        limits = SessionEntries(storage, new_york_zone(), lambda _: None)
        self.assertTrue(limits.reserve_direct('direct', started))
        self.assertFalse(limits.reserve_direct('direct', started))
        reloaded = SessionEntries(storage, new_york_zone(), lambda _: None)
        with self.assertRaisesRegex(ValueError, 'трёх входов'):
            reloaded.check(started)
        self.assertTrue(reloaded.check(ny_stamp(2026, 11, 23, 10, 0)))
        self.assertFalse(reloaded.check(ny_stamp(2026, 11, 20, 16, 0)))
        reloaded.release_direct('direct')
        self.assertTrue(reloaded.check(started))
        self.assertTrue(reloaded.reserve_direct('earlier', ny_stamp(2026, 11, 20, 9, 29)))
        self.assertFalse(reloaded.reserve_direct('earlier', started))
        self.assertTrue(reloaded.check(started))

    def test_realized_profit_restores_budget_and_cash_flow_does_not(self):
        trades = [SimpleNamespace(type=0, profit=-1195, commission=-5), SimpleNamespace(type=1, profit=1000),
                  SimpleNamespace(type=2, profit=10000), SimpleNamespace(type=3, profit=2500)]
        initial, pnl = daily_pnl(SimpleNamespace(), trades, 109800)
        self.assertEqual((initial, pnl), (100000, -200))
        check_daily_budget(initial, pnl, 0, 250)
        check_daily_budget(initial, -1550, 0, 250)
        with self.assertRaisesRegex(ValueError, '1,8%'):
            check_daily_budget(initial, -1800, 0, 250)
        with self.assertRaisesRegex(ValueError, '1,8%'):
            check_daily_budget(initial, -1500, 60, 250)

    def test_same_remaining_budget_before_and_after_session_open(self):
        f = self.fixture()
        bridge, native = f.bridges[0], f.natives[0]
        opens = ny_stamp(2026, 11, 20, 9, 30)
        native.account.balance = 98500.
        loss_at = opens - 60 + bridge.broker_utc_offset_seconds
        native.history_deals_get = lambda *args: (SimpleNamespace(time=loss_at, type=1, profit=-1500.),)
        for stamp in (opens - .001, opens):
            with patch('time.time', return_value=stamp):
                snapshot = bridge.daily_entry_risk()
                bridge._check_daily_entry_risk(native.account, (snapshot, 0.), 300.)
                with self.assertRaisesRegex(ValueError, '1,8%: доступно 300.00'):
                    bridge._check_daily_entry_risk(native.account, (snapshot, 0.), 300.01)
                with self.assertRaisesRegex(ValueError, '1,8%: доступно 50.00'):
                    bridge._check_daily_entry_risk(native.account, (snapshot, 250.), 250.)

    def test_history_is_read_once_per_account_while_all_parts_execute(self):
        f = self.fixture()
        history_reads = []
        for native in f.natives:
            native.account.balance = 100000.
            exact_history = native.history_deals_get
            def history(*bounds, method=exact_history, account=native.account.login, **kwargs):
                if len(bounds) == 2:
                    history_reads.append(account)
                    return ()
                return method(*bounds, **kwargs)
            native.history_deals_get = history

        f.engine.arm(f.payload)
        entry = fixtures.IsolatedEngineTests.until(self, f, lambda row: row['phase'] == 'completed')
        self.assertGreater(len(entry['children']), 2)
        self.assertCountEqual(history_reads, [native.account.login for native in f.natives])

    def test_fourth_entry_is_rejected_before_terminal_admission(self):
        f = self.fixture()
        for index in range(3):
            entry = dict(phase='completed', accounts={}, children=[], createdAt=f.now*1000)
            f.engine.storage.db.execute('INSERT INTO chunk_entries VALUES (?,?,?,?)',
                                        (f'previous-{index}', '', '{}', json.dumps(entry)))
            f.engine.trade_limits.record(f'previous-{index}', f.now)
        with self.assertRaisesRegex(ValueError, 'трёх входов'):
            f.engine.arm(f.payload)
        self.assertFalse(f.engine.admitting)
        self.assertFalse(any(native.sent for native in f.natives))

    def test_realized_loss_rejects_before_any_market_or_limit_order(self):
        f = self.fixture()
        account_limits = {'900000002': (1800., '1,8%'), '900000001': (1800., '1,8%')}
        for bridge, native in zip(f.bridges, f.natives):
            loss, _ = account_limits[str(native.account.login)]
            native.account.balance = 100000. - loss
            loss_at = f.now + bridge.broker_utc_offset_seconds - 60
            native.history_deals_get = lambda *args, at=loss_at, loss=loss: (SimpleNamespace(time=at, type=1, profit=-loss),)
        f.engine.arm(f.payload)
        entry = fixtures.IsolatedEngineTests.until(self, f, lambda row: row['phase'] == 'stopped')
        for account, (_, percent) in account_limits.items():
            self.assertIn(percent, entry['accounts'][account]['entryReason'])
        self.assertTrue(f.engine.trade_limits.check(f.now))
        self.assertFalse(any(native.sent for native in f.natives))


    def test_stalled_history_of_new_entry_does_not_block_parts_on_same_account(self):
        f = self.fixture()
        f.payload.pop('mirrorAccounts')
        native = f.natives[0]
        native.account.balance = 100000.
        second_part, risk_read = threading.Event(), threading.Event()
        release_part, release_risk = threading.Event(), threading.Event()
        send, history = native.order_send, native.history_deals_get
        parts, reads = [], []

        def observe_part(request):
            if request['action'] == native.TRADE_ACTION_DEAL and 'position' not in request:
                parts.append(request['volume'])
                if len(parts) == 2:
                    second_part.set()
                    self.assertTrue(release_part.wait(10))
            return send(request)

        def observe_history(*bounds, **kwargs):
            if len(bounds) != 2:
                return history(*bounds, **kwargs)
            reads.append(bounds)
            if len(reads) == 2:
                risk_read.set()
                self.assertTrue(release_risk.wait(10))
                return (SimpleNamespace(time=f.now - 60, type=1, profit=-2000.),)
            return ()

        native.order_send, native.history_deals_get = observe_part, observe_history
        f.engine.start()
        f.engine.arm(f.payload)
        second = dict(f.payload, id='second-opposite-entry', cid='second-opposite-entry', side='sell', sl=25016.)
        try:
            self.assertTrue(second_part.wait(6), 'First plan never started sending its parts')
            f.engine.arm(second)
            self.assertTrue(risk_read.wait(6), 'Second plan could not read while the first was sending')
            release_part.set()
            first = fixtures.IsolatedEngineTests.until(self, f, lambda row: row['phase'] == 'completed')
            self.assertFalse(release_risk.is_set())
            self.assertEqual(first['accounts']['900000002']['filledVolume'], 1.)
            self.assertEqual(parts, [.25] * 4)
            self.assertEqual(f.engine.get(second['id'])['accounts']['900000002']['filledVolume'], 0)
        finally:
            release_part.set()
            release_risk.set()
        f.engine._drain_lanes(second['id'])
        self.assertIn('1,8%', f.engine.get(second['id'])['accounts']['900000002']['entryReason'])

    def test_daily_risk_rpc_cannot_reach_execution_worker_or_send_orders(self):
        class Forbidden:
            def __getattr__(self, name):
                raise AssertionError(f'Unexpected native dispatch: {name}')

        with self.assertRaisesRegex(ValueError, 'dedicated read-only'):
            native_worker_method(Forbidden(), 'daily_entry_risk', ())
        with self.assertRaisesRegex(ValueError, 'dedicated read-only'):
            native_worker_method(Forbidden(), 'execute', (), risk_only=True)


if __name__ == '__main__':
    unittest.main()
