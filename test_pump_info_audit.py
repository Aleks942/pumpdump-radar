"""Network-free integration tests of actual Telegram-delivered PUMP/DUMP audits."""
import ast
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pump_info_audit as audit


def event(symbol="APTUSDT",kind="PUMP",window="10m",exchange="Bybit",price=100):
    return {"symbol":symbol,"type":kind,
            "decision":{"direction":"UP" if kind=="PUMP" else "DOWN"},
            "window":window,"exchange":exchange,
            "price":price,"change":6 if kind=="PUMP" else -6}


def quotes(exchange,price,symbol="APTUSDT"):
    if exchange=="Bybit":
        return [{"symbol":symbol,"lastPrice":str(price)}]
    return [{"instId":symbol[:-4]+"-USDT-SWAP","last":str(price)}]


class InfoAuditTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tempdir=tmp.name
        db=Path(tmp.name)/"info.sqlite3"
        p=patch.object(audit,"DB_PATH",str(db))
        p.start()
        self.addCleanup(p.stop)

    def test_four_windows_and_two_directions_are_separate(self):
        at=1_800_000_000.0
        for j,window in enumerate(audit.WINDOWS):
            self.assertTrue(audit.record_delivered(
                event(symbol=f"EX{j}USDT",window=window), "PRICE_THRESHOLD",at+j))
        self.assertTrue(audit.record_delivered(
            event("DUMPUSDT","DUMP","20m","OKX"),"PRICE_THRESHOLD",at))
        self.assertFalse(audit.record_delivered(
            event("DUMPUSDT","DUMP","20m","OKX"),"PRICE_THRESHOLD",at))
        self.assertEqual(sum(v["total"] for (w,k,h),v in audit.summary().items()
                             if h==30),5)
        # Some bybit symbols continue; dump on OKX falls further (correct).
        for j,window in enumerate(audit.WINDOWS):
            self.assertEqual(audit.observe_quotes(
                quotes("Bybit",102,f"EX{j}USDT"),"Bybit",at+30*60+60),4)
        self.assertEqual(audit.observe_quotes(
            quotes("OKX",97,"DUMPUSDT"),"OKX",at+30*60+60),4)
        out=audit.summary()
        for window in audit.WINDOWS:
            self.assertEqual(out[(window,"PUMP",30)]["continued"],1)
        self.assertEqual(out[("20m","DUMP",30)]["continued"],1)

    def test_directional_reversal_and_small_flats(self):
        base=1800000000
        audit.record_delivered(event(),"PRICE_THRESHOLD",base)
        self.assertEqual(audit.observe_quotes(
            quotes("Bybit",99),"Bybit",base+300),1)
        a=audit.summary()[("10m","PUMP",5)]
        self.assertEqual(a["reversed"],1)
        self.assertAlmostEqual(a["sum_direction"],-1)
        audit.record_delivered(event("ETHUSDT","DUMP","5m","OKX"),
                               "PRICE_CONTINUATION",base)
        audit.observe_quotes(quotes("OKX",99.8,"ETHUSDT"),"OKX",base+300)
        self.assertEqual(audit.summary()[("5m","DUMP",5)]["flat"],1)

    def test_same_exchange_only_and_no_fabricated_late_outcome(self):
        at=1800000000
        audit.record_delivered(event(),"PRICE_THRESHOLD",at)
        # A mismatched OKX price is not evidence for a Bybit alert.
        self.assertEqual(audit.observe_quotes(quotes("OKX",120),"OKX",
                                              at+5*60),0)
        self.assertEqual(audit.summary()[("10m","PUMP",5)]["pending"],1)
        self.assertEqual(audit.observe_quotes(quotes("Bybit",120),"Bybit",
                                              at+5*60+241),0)
        stats=audit.summary()[("10m","PUMP",5)]
        self.assertEqual(stats["missing"],1)
        self.assertEqual(stats["priced"],0)
        # Delayed observation cannot overwrite MISSING retroactively.
        self.assertEqual(audit.observe_quotes(quotes("Bybit",105),"Bybit",
                                              at+5*60+242),0)

    def test_missing_quote_retry_within_240_seconds(self):
        at=1800000000
        audit.record_delivered(event(),"PRICE_THRESHOLD",at)
        self.assertEqual(audit.observe_quotes([],"Bybit",at+5*60+10),0)
        self.assertEqual(audit.summary()[("10m","PUMP",5)]["pending"],1)
        self.assertEqual(audit.observe_quotes(quotes("Bybit",100.2),"Bybit",
                                              at+5*60+200),1)
        self.assertEqual(audit.summary()[("10m","PUMP",5)]["flat"],1)

    def test_invalid_or_unacknowledged_alerts_do_not_enter_journal(self):
        at=1800000000
        invalid=event()
        invalid["price"]=float("nan")
        self.assertFalse(audit.record_delivered(invalid,"PRICE_THRESHOLD",at))
        bad=event()
        bad["decision"]={"direction":"DOWN"}
        self.assertFalse(audit.record_delivered(bad,"PRICE_THRESHOLD",at))
        self.assertFalse(audit.record_delivered(event(), "BAD_REASON",at))
        self.assertEqual(audit.summary(),{})

    def test_daily_digest_only_once_after_ack(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        at=datetime(2026,10,15,11,0,tzinfo=ZoneInfo("Europe/Warsaw")).timestamp()
        audit.record_delivered(event(),"PRICE_THRESHOLD",at)
        when=datetime(2026,10,16,10,0,tzinfo=ZoneInfo("Europe/Warsaw")).timestamp()
        messages=[]
        self.assertFalse(audit.daily_digest(lambda text:False,when))
        self.assertTrue(audit.daily_digest(
            lambda text:messages.append(text) or True,when))
        self.assertFalse(audit.daily_digest(lambda text:True,when))
        self.assertIn("не сделки",messages[0])
        self.assertEqual(len(messages),1)

    def test_main_sender_journals_only_after_telegram_ack(self):
        source=Path(__file__).with_name("main.py").read_text(encoding="utf8")
        tree=ast.parse(source)
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)
                and n.name=="maybe_send_market_watch")
        sent=[]
        ns={
            "time":__import__("time"),"math":__import__("math"),
            "_MARKET_WATCH_SPACING_SEC":0,
            "_MARKET_WATCH_SYMBOL_SEC":600,
            "_market_watch_delivery":{"day":None,"count":0,"last_sent":None,
                                     "symbols":{},"retry_after":0},
            "build_market_watch_message":lambda event,reason:"NOTICE",
            "send_telegram":lambda text,symbol="SYSTEM":False,
            "record_pump_info_event":lambda *args,**kwargs:sent.append(args) or True,
            "schedule_coinglass_context":lambda *args,**kwargs:None,
            "print":lambda *args,**kwargs:None
        }
        exec(compile(ast.Module(body=[fn],type_ignores=[]),
                     "<market-watch-test>","exec"),ns)
        send=ns["maybe_send_market_watch"]
        e=event()
        self.assertFalse(send(e,"PRICE_THRESHOLD",now=1000))
        self.assertEqual(sent,[])
        ns["send_telegram"]=lambda text,symbol="SYSTEM":True
        self.assertTrue(send(e,"PRICE_THRESHOLD",now=1400))
        self.assertEqual(len(sent),1)
        self.assertIs(sent[0][0],e)


if __name__=="__main__":
    unittest.main()
