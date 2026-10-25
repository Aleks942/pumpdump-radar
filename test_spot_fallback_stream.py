from collections import deque
from pathlib import Path
import ast
import threading
import time
import unittest


class SpotFallbackStreamTests(unittest.TestCase):
    def namespace(self):
        source = Path(__file__).with_name("spot_fallback_stream.py").read_text(
            encoding="utf-8")
        tree = ast.parse(source)
        nodes = [
            node for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name in ("_norm", "get_flow")
        ]
        ns = {
            "time": time,
            "_lock": threading.RLock(),
            "_source": {},
            "_watch_at": {},
            "_connected": {"OKX": False, "Binance": False},
            "_events": {},
            "WINDOW_SEC": 300,
            "SOURCE_TTL_SEC": 45,
        }
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=nodes, type_ignores=[])),
            "spot_fallback_subset", "exec"), ns)
        return ns

    def test_ready_window_uses_real_accumulated_trades(self):
        ns = self.namespace()
        symbol = "TESTUSDT"
        now = 10_000.0
        ns["_watch_at"][symbol] = now - 300
        ns["_source"][symbol] = "OKX"
        ns["_connected"]["OKX"] = True
        ns["_events"][symbol] = deque(
            (now - 250 + i * (249 / 29), 100.0 if i < 20 else -100.0)
            for i in range(30)
        )
        result = ns["get_flow"](symbol, now=now)
        self.assertTrue(result["ready"])
        self.assertEqual(result["quality"], "READY")
        self.assertEqual(result["source"], "OKX_SPOT_WS")
        self.assertGreaterEqual(result["coverage_sec"], 240)
        self.assertGreater(result["imbalance_pct"], 0)

    def test_since_excludes_old_opposite_sequence(self):
        ns = self.namespace()
        symbol = "TESTUSDT"
        now = 10_000.0
        ns["_watch_at"][symbol] = now - 300
        ns["_source"][symbol] = "OKX"
        ns["_connected"]["OKX"] = True
        rows = deque()
        for i in range(20):
            rows.append((now - 250 + i * 5, 100.0))
        for i in range(20):
            rows.append((now - 100 + i * 5, -100.0))
        ns["_events"][symbol] = rows
        result = ns["get_flow"](symbol, now=now, since=now - 105)
        self.assertLess(result["imbalance_pct"], 0)
        self.assertLessEqual(result["coverage_sec"], 105)

    def test_unlisted_market_stays_unsupported(self):
        ns = self.namespace()
        symbol = "NOPEUSDT"
        ns["_watch_at"][symbol] = 1000
        ns["_source"][symbol] = "UNSUPPORTED"
        result = ns["get_flow"](symbol, now=1100)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"], "UNSUPPORTED")

    def test_source_contains_okx_and_binance_fallbacks(self):
        code = Path(__file__).with_name("spot_fallback_stream.py").read_text(
            encoding="utf-8")
        self.assertIn("wss://ws.okx.com:8443/ws/v5/public", code)
        self.assertIn("wss://stream.binance.com:9443/ws", code)
        self.assertIn('source = "OKX"', code)
        self.assertIn('source = "Binance"', code)
        self.assertIn("coverage >= 240.0", code)


if __name__ == "__main__":
    unittest.main()
