from pathlib import Path
import unittest


class BybitMoveVerifierTests(unittest.TestCase):
    def test_main_wires_bybit_watch_into_verifier(self):
        code = Path(__file__).with_name("main.py").read_text(encoding="utf-8")
        self.assertIn("schedule_bybit_move_verification", code)
        self.assertIn('str(signal.get("exchange") or "").upper() == "BYBIT"', code)

    def test_verifier_checks_required_context_and_fails_closed(self):
        code = Path(__file__).with_name("bybit_move_verifier.py").read_text(
            encoding="utf-8")
        for required in (
            "/v5/market/open-interest",
            "/v5/market/recent-trade",
            "/v5/market/tickers",
            "/v5/market/kline",
            "watch_bybit_symbol",
            "get_liquidation_summary",
            "get_aggregated_oi",
            "detect_pattern",
            '"quality": "PARTIAL"',
            '"quality": "UNAVAILABLE"',
            '"trade_approved": False',
            '"research_only": True',
            "[BYBIT_VERIFY_SENT]",
        ):
            self.assertIn(required, code)

    def test_followup_message_reports_all_layers(self):
        code = Path(__file__).with_name("bybit_move_verifier.py").read_text(
            encoding="utf-8")
        for label in (
            "OI Bybit",
            "Aggregated OI",
            "Futures CVD",
            "Spot CVD",
            "Funding Bybit",
            "Тренд закрытых 5м свечей",
            "Тренд закрытых H1 свечей",
            "Ликвидации 5м",
            "не команда на сделку",
        ):
            self.assertIn(label, code)


if __name__ == "__main__":
    unittest.main()
