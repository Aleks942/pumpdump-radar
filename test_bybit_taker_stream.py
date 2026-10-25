from collections import deque
from pathlib import Path
import ast
import threading
import time
import unittest


class BybitTakerStreamTests(unittest.TestCase):
    def namespace(self):
        source = Path(__file__).with_name("bybit_taker_stream.py").read_text(
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
            "_spot_support": {},
            "_connected": {"linear": True, "spot": True},
            "_watch_at": {},
            "_events": {},
            "WINDOW_SEC": 300,
            "SOURCE_TTL_SEC": 45,
        }
        exec(compile(ast.fix_missing_locations(
            ast.Module(body=nodes, type_ignores=[])),
            "bybit_taker_subset", "exec"), ns)
        return ns

    def test_since_excludes_pre_trigger_trades(self):
        ns = self.namespace()
        symbol = "TESTUSDT"
        now = 10_000.0
        ns["_watch_at"][symbol] = now - 300
        rows = deque()
        for i in range(20):
            rows.append((now - 250 + i * 5, 100.0))
        for i in range(20):
            rows.append((now - 100 + i * 5, -100.0))
        ns["_events"][("linear", symbol)] = rows

        result = ns["get_flow"](
            symbol,
            category="linear",
            now=now,
            since=now - 105,
        )
        self.assertLess(result["imbalance_pct"], 0)
        self.assertLessEqual(result["coverage_sec"], 105)

    def test_no_since_keeps_normal_rolling_window(self):
        ns = self.namespace()
        symbol = "TESTUSDT"
        now = 10_000.0
        ns["_watch_at"][symbol] = now - 300
        ns["_events"][("linear", symbol)] = deque(
            (now - 250 + i * (249 / 29), 100.0 if i < 20 else -100.0)
            for i in range(30)
        )
        result = ns["get_flow"](symbol, category="linear", now=now)
        self.assertTrue(result["ready"])
        self.assertGreaterEqual(result["coverage_sec"], 240)


if __name__ == "__main__":
    unittest.main()
