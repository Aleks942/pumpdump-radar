import unittest

import swing_backtest as b


def hourly_series(n=1000,start=1_700_000_000):
    rows=[]
    price=1.0
    # Align start to exact hour.
    start=(start//3600)*3600
    for i in range(n):
        o=price
        c=o*1.001
        h=max(o,c)*1.002
        l=min(o,c)*0.998
        rows.append({"ts":float(start+i*3600),"o":o,"h":h,"l":l,"c":c,"v":1000+i})
        price=c
    return rows


def oi_series(candles):
    out=[]
    value=1_000_000.0
    for row in candles:
        value*=1.0005
        out.append((row["ts"],value))
    return out


class SwingBacktestTests(unittest.TestCase):
    def test_resampled_bar_not_visible_before_close(self):
        c1=hourly_series(12,start=1_700_006_400)
        c4=b._resample(c1,4)
        self.assertTrue(c4)
        first=c4[0]
        before=first["end_ts"]-3600
        at_close=first["end_ts"]
        self.assertEqual(b._slice_before(c4,before),[])
        self.assertEqual(len(b._slice_before(c4,at_close)),1)

    def test_future_markout_long_and_short(self):
        c1=hourly_series(20)
        long=b._future_markout(c1,5,4,"LONG")
        short=b._future_markout(c1,5,4,"SHORT")
        self.assertIsNotNone(long)
        self.assertGreater(long["return_pct"],0)
        self.assertLess(short["return_pct"],0)

    def test_walk_forward_generates_medium_term_long_signals(self):
        c1=hourly_series(1100)
        oi=oi_series(c1)
        events=b.build_signals("TESTUSDT",c1,oi)
        self.assertTrue(events)
        self.assertTrue(any(e["direction"]=="LONG" for e in events))
        self.assertTrue(all("168" in e["outcomes"] for e in events))

    def test_future_prices_do_not_change_existing_signal_score(self):
        c1=hourly_series(1100)
        oi=oi_series(c1)
        events=b.build_signals("TESTUSDT",c1,oi)
        self.assertTrue(events)
        anchor=events[len(events)//2]
        ts=anchor["ts"]

        changed=[]
        for row in c1:
            x=dict(row)
            if x["ts"]>=ts:
                # Destroy the future after the decision timestamp.
                x["o"]*=0.5
                x["h"]*=0.5
                x["l"]*=0.5
                x["c"]*=0.5
            changed.append(x)

        events2=b.build_signals("TESTUSDT",changed,oi)
        same=[e for e in events2 if e["ts"]==ts]
        self.assertEqual(len(same),1)
        self.assertEqual(same[0]["score"],anchor["score"])
        self.assertEqual(same[0]["direction"],anchor["direction"])

    def test_repeated_same_direction_signals_are_spaced(self):
        c1=hourly_series(1100)
        oi=oi_series(c1)
        events=b.build_signals("TESTUSDT",c1,oi,cooldown_hours=12)
        longs=[e for e in events if e["direction"]=="LONG"]
        self.assertTrue(longs)
        self.assertTrue(all(
            b2["ts"]-a["ts"]>=12*3600
            for a,b2 in zip(longs,longs[1:])
        ))

    def test_summary_separates_score_and_horizon(self):
        c1=hourly_series(1100)
        oi=oi_series(c1)
        events=b.build_signals("TESTUSDT",c1,oi)
        summary=b.summarize(events)
        self.assertTrue(summary)
        self.assertTrue(all(x["horizon_h"] in b.HORIZONS_H for x in summary))
        self.assertTrue(all("win_pct_net" in x for x in summary))


if __name__=="__main__":
    unittest.main()
