from pathlib import Path
import unittest


class MoveVerifierRouterTests(unittest.TestCase):
    def test_router_and_main_are_exchange_neutral(self):
        router = Path(__file__).with_name("move_verifier.py").read_text(
            encoding="utf-8"
        )
        main = Path(__file__).with_name("main.py").read_text(encoding="utf-8")

        self.assertIn('exchange == "BYBIT"', router)
        self.assertIn('exchange == "OKX"', router)
        self.assertIn("schedule_move_verification", main)
        self.assertNotIn(
            'if str(signal.get("exchange") or "").upper() == "BYBIT":',
            main,
        )


if __name__ == "__main__":
    unittest.main()
