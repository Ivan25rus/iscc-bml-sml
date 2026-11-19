"""Exercise the real entry round with frequent quotes and a controlled clock.

Only native reads/sends are replaced. No terminal, network or live journal.
"""
import pathlib
import sys
import unittest
from types import SimpleNamespace as N
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'payload/vps'))
from chunk_entry import ChunkEntries


class PendingPollDeadlineTests(unittest.TestCase):
    def model(self):
        account = '900000002'
        details = dict(targetVolume=1., filledVolume=0., entryStatus='limit')
        child = dict(id='entry:h1', type='limit', state='pending', volumes={account: 1.},
                     edgePlaced={account: 106.})
        state = dict(id='entry', phase='running', accounts={account: details}, children=[child])
        payload = dict(side='buy', symbol='USTEC', trigger=100., zoneRadius=6.,
                       entryMode='side-zone', closeAtUtc=10**15)
        clock = [100.]
        polls, cancellations = [], []
        quote = [110., 110.2]
        engine = N(pending_poll_at={child['id']: 100.25}, snapshot_jobs={}, entry_clocks={}, entry_diagnostics=None,
                   bridge=N(config=dict(server='mock')), _stopping=lambda _: False,
                   _protecting=lambda _: False, _positions=lambda *a, **kw: None,
                   _hybrid_fill_violation=lambda _: False, _independent_sync_stop=lambda *a: True,
                   _parallel_retry_ready=lambda *a: True, _remember=lambda _: None)
        engine._parallel_quotes = lambda *a: ({account: tuple(quote)}, {account: None}, {})
        engine._parallel_pending = lambda s, a: ChunkEntries._parallel_pending(engine, s, a)
        engine._parallel_note = lambda s, a, phase, reason: s['accounts'][a].update(entryStatus=phase)
        engine._phase = lambda s, phase, reason: s.update(phase=phase, reason=reason)
        engine._parallel_cancel = lambda *a: cancellations.append(clock[0])
        engine._parallel_batch = lambda p, s, kind, plans, **options: False
        engine._hybrid_limit = lambda *a, **kw: polls.append(clock[0])
        # An unexpected exception must fail the simulation, not look like a
        # successful protective stop through the production error handler.
        engine._hybrid_cancel_limits = lambda *a: self.fail(state.get('reason', 'Unexpected entry error'))

        def tick(at):
            clock[0] = at
            with patch('chunk_entry.time.monotonic', return_value=at):
                ChunkEntries._tick_parallel_hybrid(engine, payload, state)
            self.assertEqual(state.get('reason'), 'Target and protection confirmed' if state['phase'] == 'completed'
                             else 'Parallel account entry is active')
        return engine, state, child, details, quote, polls, cancellations, tick

    def test_frequent_quotes_do_not_postpone_an_already_scheduled_poll(self):
        engine, state, child, _, _, polls, _, tick = self.model()
        for step in range(1, 5):
            tick(100. + step * .05)
            self.assertEqual(engine.pending_poll_at[child['id']], 100.25)
            self.assertFalse(polls)
        tick(100.25)
        self.assertEqual(polls, [100.25])
        self.assertEqual(engine.pending_poll_at[child['id']], 100.5)
        for step in range(6, 21):
            tick(100. + step * .05)
        self.assertEqual(polls, [100.25, 100.5, 100.75, 101.])

    def test_filled_limit_is_consumed_at_deadline_even_while_quotes_remain_outside(self):
        engine, state, child, details, _, polls, _, tick = self.model()
        def fill(*args, **kwargs):
            polls.append(100.25)
            child.update(state='confirmed', positions={'900000002': dict(ticket='71', sl=90., tp=0.)})
            details['filledVolume'] = 1.
        engine._hybrid_limit = fill
        for step in range(1, 6):
            tick(100. + step * .05)
        self.assertEqual(polls, [100.25])
        self.assertEqual(state['phase'], 'completed')

    def test_price_return_cancels_pending_without_waiting_for_poll_deadline(self):
        _, _, _, _, quote, polls, cancellations, tick = self.model()
        quote[:] = [103.8, 104.]
        tick(100.05)
        self.assertFalse(polls)
        self.assertEqual(cancellations, [100.05])


if __name__ == '__main__':
    unittest.main()
