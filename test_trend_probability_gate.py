"""No-network tests: conservative HTF trend and first-touch gating."""
import math
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import trend_probability_gate as gate
import pump_peak_watch as watch
import pump_watch_forward as forward

NOW=1800000000.0
PATTERN="NEW_LONG_BUILDUP"


def history(now, seconds, direction=1, stale=False):
    last=int(now//seconds)*seconds-seconds
    if stale:
        last-=5*seconds
    bars=[]
    for k in range(100):
        at=last-(99-k)*seconds
        close=100*(1.001**(k*direction))
        bars.append([str(at*1000),str(close),str(close*1.001),
                     str(close*.999),str(close),
                     "1","1","1","1"])
    return list(reversed(bars))


def sample(price=100):
    return {"symbol":"TESTUSDT","decision":{
        "pattern":PATTERN,"direction":"UP"},"price":price}


def quote(p):
    return {"instId":"TEST-USDT-SWAP", "last":str(p),
        "open24h":"100", "high24h":"112", "volCcy24h":"30000000"}


class TrendStudyTests(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        for obj,key,val in (
            (gate,"DB_PATH",os.path.join(temp.name,"trend.sqlite3")),
            (gate,"_trend_cache",gate.OrderedDict()),
            (watch,"DB_PATH",os.path.join(temp.name,"watch.sqlite3")),
            (watch,"_observed",{})):
            p=patch.object(obj,key,val);p.start();self.addCleanup(p.stop)

    def test_htf_needs_both_fresh_confirmed_directions(self):
        with patch.object(gate,"_okx",side_effect=lambda symbol,bar,limit:
                          history(NOW,3600 if bar=="1H" else 14400)):
            self.assertEqual(gate.trend_status("TESTUSDT",NOW),"UP")
        gate._trend_cache.clear()
        with patch.object(gate,"_okx",side_effect=lambda symbol,bar,limit:
                          history(NOW,3600 if bar=="1H" else 14400,
                                  direction=1 if bar=="1H" else -1)):
            self.assertEqual(gate.trend_status("TESTUSDT",NOW),"UNKNOWN")
        gate._trend_cache.clear()
        with patch.object(gate,"_okx",side_effect=lambda symbol,bar,limit:
                          history(NOW,3600 if bar=="1H" else 14400,stale=True)):
            self.assertEqual(gate.trend_status("TESTUSDT",NOW),"UNKNOWN")
        self.assertEqual(gate._side_from_candles([],3600,NOW),"UNKNOWN")

    def test_h1_with_flat_h4_stays_separate_and_requires_proven_edge(self):
        self.assertEqual((gate.MIN_N, gate.MIN_DAYS, gate.MIN_SYMBOLS),
                         (80, 5, 15))
        with patch.object(gate, "_okx", side_effect=lambda symbol,bar,limit:
                          history(NOW, 3600 if bar == "1H" else 14400,
                                  direction=1 if bar == "1H" else 0)):
            self.assertEqual(gate.trend_status("TESTUSDT", NOW), "UP_H4_FLAT")

        with patch.object(gate, "trend_status", return_value="UP_H4_FLAT"):
            self.assertEqual(gate.consider(sample(), NOW),
                             (False, "EDGE_UNPROVEN"))
        with gate._db() as db:
            self.assertEqual(db.execute(
                "SELECT trend FROM candidates").fetchone()[0], "UP_H4_FLAT")
            # Even perfect STRICT-mode historical wins cannot qualify the
            # new flat-H4 cohort without its own out-of-sample observations.
            for k in range(90):
                ts = NOW - 15*86400 + k*3*3600
                db.execute("""INSERT INTO candidates
                    (ts,symbol,pattern,side,trend,status,verdict,net_pct,evaluated_ts)
                    VALUES (?,?,?,?,?,'DONE','TP_FIRST',1.28,?)""",
                    (ts, "COIN"+str(k%25)+"USDT",
                     PATTERN, "LONG", "UP", ts+1800))
            db.commit()
            self.assertTrue(gate._edge(db, PATTERN, "LONG", "UP", NOW)["approved"])
            self.assertFalse(gate._edge(
                db, PATTERN, "LONG", "UP_H4_FLAT", NOW)["approved"])
        gate._trend_cache.clear()
        with patch.object(gate, "_okx", side_effect=lambda symbol,bar,limit:
                          history(NOW, 3600 if bar == "1H" else 14400,
                                  direction=1 if bar == "1H" else -1)):
            self.assertEqual(gate.trend_status("TESTUSDT", NOW), "UNKNOWN")

    def test_first_touch_stop_prioritized_over_ambiguous_target(self):
        start=math.floor(NOW/60)*60+60
        path=[]
        for k in range(30):
            hi=102 if k==0 else 100.2
            lo=98 if k==0 else 99.8
            path.append([str((start+k*60)*1000),"100",str(hi),
                         str(lo),"100","1","1","1","1"])
        rows=list(reversed(path))
        candles=gate._evaluate_rows(rows,NOW)
        self.assertEqual(len(candles),30)
        self.assertEqual(gate._outcome(candles,"LONG"),
                         ("SL_FIRST",-1.22))
        self.assertIsNone(gate._evaluate_rows(rows[:-1],NOW))
        # Confirmation data from the candidate's own minute cannot be used.
        self.assertIsNone(gate._evaluate_rows(
            [[str((start-60)*1000),"100","100","100","100",
              "1","1","1","1"]]+rows[:-1],NOW))

    def test_insufficient_evidence_stays_silent_and_keeps_one_sample(self):
        with patch.object(gate,"trend_status",return_value="UP"):
            self.assertEqual(gate.consider(sample(),NOW),
                             (False,"EDGE_UNPROVEN"))
            self.assertEqual(gate.consider(sample(),NOW+10),
                             (False,"EDGE_UNPROVEN"))
        with gate._db() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM candidates"
                                        ).fetchone()[0],1)
        with patch.object(gate,"trend_status",return_value="DOWN"):
            self.assertEqual(gate.consider(sample(),NOW+60),
                             (False,"TREND_NOT_ALIGNED"))

    def test_estimated_edge_requires_n_days_symbols_and_lower_bound(self):
        with gate._db() as db:
            for k in range(150):
                stamp=NOW-20*86400+k*3*3600
                db.execute("""INSERT INTO candidates
                    (ts,symbol,pattern,side,trend,status,verdict,net_pct,evaluated_ts)
                    VALUES (?,?,?,?,?,'DONE','TP_FIRST',1.28,?)""",
                    (stamp,"COIN"+str(k%30)+"USDT",
                     PATTERN,"LONG","UP",stamp+40*60))
            db.commit()
            edge=gate._edge(db,PATTERN,"LONG","UP",NOW)
            self.assertTrue(edge["approved"])
            self.assertGreater(edge["lower_win_probability"],
                               edge["breakeven_probability"])
            db.execute("""UPDATE candidates SET verdict='SL_FIRST',
                net_pct=-1.22 WHERE id%2=0""")
            db.commit()
            failed=gate._edge(db,PATTERN,"LONG","UP",NOW)
            self.assertFalse(failed["approved"])

    def test_silent_watch_still_persists_and_isolated_from_delivered(self):
        event=watch.observe([quote(110)],now=NOW)[0]
        watch.acknowledge(event,False,now=NOW,research_only=True)
        self.assertEqual(watch.observe([quote(110)],now=NOW+10),[])
        with watch._db() as db:
            row=db.execute("""SELECT delivered,entry_price
                FROM pump_watch_alerts""").fetchone()
            self.assertEqual(row,(0,110))
        forward.observe([quote(106)],now=NOW+300)
        reports=forward.summary(now=NOW+300)
        self.assertEqual(reports[0]["cohort"],"DELIVERED")
        self.assertEqual(reports[0]["n"],0)
        silent=next(r for r in reports if r["cohort"]=="SILENT"
                    and r["level"]==1 and r["horizon_min"]==5)
        self.assertEqual(silent["n"],1)

    def test_small_bounded_batch_forward_research(self):
        first=int(NOW//60)*60+60
        rows=[]
        for k in range(30):
            begin=first+k*60
            o=100
            high=102 if k==3 else 100.1
            rows.append([str(begin*1000),str(o),str(high),
                         "99.9","100","1","1","1","1"])
        with patch.object(gate,"trend_status",return_value="UP"):
            gate.consider(sample(),NOW)
        with patch.object(gate,"_okx",return_value=list(reversed(rows))):
            gate.advance(NOW+34*60)
        with gate._db() as db:
            status,verdict,net=db.execute("""SELECT status,verdict,net_pct
                FROM candidates""").fetchone()
            self.assertEqual((status,verdict),("DONE","TP_FIRST"))
            self.assertAlmostEqual(net,1.28)


if __name__=="__main__":
    unittest.main()
