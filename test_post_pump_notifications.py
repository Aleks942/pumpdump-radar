import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import post_pump_notifications as notify
import post_pump_short_strategy as strategy

NOW = 1800000000.0

class NotificationsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name)/'watch.db')
        with sqlite3.connect(self.path) as db:
            strategy.setup(db)
            db.execute('''INSERT INTO post_pump_short_strategy
                (symbol,episode_at,last_seen_at,phase,phase_at,peak,rise_max,band,reference_open)
                VALUES ('TESTUSDT',?,?, 'WATCH',?,150,50,'50_TO_99',100)''', (NOW,NOW,NOW))
        self.sender = Mock(return_value=True)
        self.run_delivery(NOW)

    def event(self, phase, at=NOW+1, reason='TEST', **overrides):
        ev = {'order_flow': {'ready': True, 'status': 'CONFIRMED',
                            'source': 'PUMPDUMP_LOCAL_TAKER_FLOW'}}
        data = dict(entry=100,stop=104,tp1=92,tp2=88,evidence_json=json.dumps(ev))
        data.update(overrides)
        with sqlite3.connect(self.path) as db:
            db.execute('''INSERT INTO post_pump_short_strategy_audit
                (symbol,episode_at,at,phase,reason,rise_pct,retrace_pct,reference_price,
                 entry,stop,tp1,tp2,evidence_json) VALUES ('TESTUSDT',?,?,?,?,50,5,100,?,?,?,?,?)''',
                (NOW,at,phase,reason,data['entry'],data['stop'],data['tp1'],data['tp2'],data['evidence_json']))
            db.execute('UPDATE post_pump_short_strategy SET phase=?,phase_at=? WHERE symbol=?',
                       (phase,at,'TESTUSDT'))

    def run_delivery(self, at, price=100):
        return notify.deliver(self.sender,[{'instId':'TEST-USDT-SWAP','last':str(price)}],
                              now=at,db_path=self.path)

    def test_research_metrics_are_labeled_as_partial_context(self):
        event={'kind':'WEAKENING','symbol':'TESTUSDT','at':NOW,
               'rise_pct':35.0,'retrace_pct':8.0,
               'evidence_json':json.dumps({
                   'capital_state':{'state':'NEW_SHORT_BUILDUP'},
                   'bounded_cvd_5m':{'state':'SELLERS_DOMINANT'},
                   'liquidation_context':{'available':True,
                       'long_usdt':12000,'short_usdt':3000},
                   'funding_context':{'available':True,'funding_pct':0.015}})}
        msg=notify.message(event)
        self.assertIn('OI / капитал',msg)
        self.assertIn('не полный CVD',msg)
        self.assertIn('Ликвидации (частично)',msg)
        self.assertIn('Funding: +0.0150%',msg)
        event['evidence_json']='{invalid'
        self.assertIn('недоступны',notify.message(event))

    def test_full_lifecycle_and_restart_dedup(self):
        for i,phase in enumerate(('DISCOVERED','EARLY_WEAKENING','TREND_SHIFT','RETEST','READY_RESEARCH')):
            self.event(phase,NOW+1+i)
            self.run_delivery(NOW+1+i)
            self.run_delivery(NOW+1+i)
        self.assertEqual(self.sender.call_count,5)
        self.assertIn('ТЕСТОВЫЙ ПЛАН',self.sender.call_args.args[0])
        self.event('CANCELLED',NOW+10,'BREAK_INVALIDATED')
        self.run_delivery(NOW+10)
        self.assertEqual(self.sender.call_count,6)
        self.assertIn('СЦЕНАРИЙ ОТМЕНЁН',self.sender.call_args.args[0])

    def test_later_plan_can_be_cancelled_after_earlier_warning_cancel(self):
        self.event('EARLY_WEAKENING')
        self.run_delivery(NOW+1)
        self.event('WATCH',NOW+2,'BUYERS_RECOVERED_EARLY_WEAKENING')
        self.run_delivery(NOW+2)
        self.event('WATCH',NOW+3,'BUYERS_RECOVERED_EARLY_WEAKENING')
        self.run_delivery(NOW+3)
        self.assertEqual(self.sender.call_count,2)
        self.event('READY_RESEARCH',NOW+4)
        self.run_delivery(NOW+4)
        self.event('CANCELLED',NOW+5,'BREAK_INVALIDATED')
        self.run_delivery(NOW+5)
        self.assertEqual(self.sender.call_count,4)

    def test_false_retries_but_unknown_does_not(self):
        self.event('RETEST')
        self.sender.return_value = False
        self.run_delivery(NOW+1)
        self.run_delivery(NOW+2)
        self.assertEqual(self.sender.call_count,1)
        self.sender.return_value = None
        self.run_delivery(NOW+62)
        self.sender.return_value = True
        self.run_delivery(NOW+130)
        self.assertEqual(self.sender.call_count,2)

    def test_expired_or_moved_plan_is_not_sent(self):
        self.event('READY_RESEARCH')
        self.run_delivery(NOW+2,price=101)
        self.assertEqual(self.sender.call_count,0)

    def test_old_plan_is_not_sent(self):
        self.event('READY_RESEARCH')
        self.run_delivery(NOW+122)
        self.assertEqual(self.sender.call_count,0)

    def test_unconfirmed_plan_is_not_sent(self):
        self.event('READY_RESEARCH',evidence_json='{}')
        self.run_delivery(NOW+2)
        self.assertEqual(self.sender.call_count,0)

    def test_superseded_stages_skipped(self):
        self.event('RETEST')
        self.event('READY_RESEARCH',NOW+2)
        self.run_delivery(NOW+2)
        self.assertEqual(self.sender.call_count,1)
        self.assertIn('ТЕСТОВЫЙ SHORT',self.sender.call_args.args[0])

    def test_no_cancellation_without_prior_notification(self):
        self.event('CANCELLED',reason='BREAK_INVALIDATED')
        self.run_delivery(NOW+1)
        self.assertEqual(self.sender.call_count,0)

    def test_activation_skips_history(self):
        self.event('READY_RESEARCH')
        with sqlite3.connect(self.path) as db:
            db.execute('DELETE FROM post_pump_notify_meta')
        self.run_delivery(NOW+1)
        self.assertEqual(self.sender.call_count,0)

    def test_interrupted_send_is_not_repeated(self):
        self.event('RETEST')
        self.sender.return_value=False
        self.run_delivery(NOW+1)
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE post_pump_notify_outbox SET status='SENDING',retry_at=?",(NOW+2,))
        self.run_delivery(NOW+200)
        self.assertEqual(self.sender.call_count,1)

    def test_plan_expiry_without_quote_or_active_slot(self):
        self.event('READY_RESEARCH')
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            db.execute("UPDATE post_pump_short_strategy SET last_price=100,last_retrace_pct=5")
            strategy._cancel_published_plans(db, {}, NOW+3602)
            row=db.execute("SELECT phase,watch_reason FROM post_pump_short_strategy").fetchone()
            self.assertEqual(tuple(row), ('CANCELLED','PAPER_PLAN_EXPIRED'))

    def test_plan_cancelled_even_without_candle_data(self):
        self.event('READY_RESEARCH')
        with sqlite3.connect(self.path) as db:
            db.row_factory=sqlite3.Row
            st=dict(db.execute('SELECT * FROM post_pump_short_strategy').fetchone())
            st.update(last_price=100,last_retrace_pct=5,last_spot_poll_at=0,
                      last_context_at=0,break_level=102,break_candle_ts=NOW-300)
            profile=dict(last=105,high=150,rise=50,band='50_TO_99')
            with patch.object(strategy,'verified_spot_stream',return_value={}):
                strategy._advance(db,st,profile,NOW+2,lambda s:{},lambda s:[],lambda s,t:{},None,self.path)
            self.assertEqual(st['phase'],'CANCELLED')
            self.assertEqual(st['watch_reason'],'PAPER_STOP_REACHED')

if __name__=='__main__':
    unittest.main()
