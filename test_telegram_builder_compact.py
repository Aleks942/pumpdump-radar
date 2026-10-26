import unittest

from telegram_builder import build_short_message


class CompactTelegramCardTests(unittest.TestCase):
    def test_long_card_is_short_clear_and_color_coded(self):
        signal = {
            "symbol": "ORDERUSDT",
            "price": 0.04838,
            "change": 4.2,
            "oi_change": 1.4,
            "decision": {
                "pattern": "NEW_LONG_BUILDUP",
                "direction": "UP",
            },
            "aggregated_oi": {
                "windows": {
                    "5m": {
                        "ready": True,
                        "change_pct": 1.1,
                        "confidence": "HIGH",
                        "consensus_mode": "3/3",
                    }
                }
            },
            "futures_flow": {
                "window_ready": True,
                "imbalance_pct": 22.0,
            },
            "spot_cvd": {
                "available": True,
                "cvd_percent": 8.0,
            },
            "liquidations": {},
        }

        text = build_short_message(signal)

        for expected in (
            "🟢 <b>LONG — ORDERUSDT</b>",
            "СИГНАЛ ПОДТВЕРЖДЁН",
            "сила: 🟢 ВЫСОКАЯ",
            "Новые лонги заходят в рынок",
            "OI 5м: +1.40%",
            "OI рынка: +1.10% · 3/3 биржи",
            "Фьючерсы: +22.00% · подтверждает",
            "Спот: +8.00% · подтверждает",
            "Сейчас:",
            "Не догонять свечу",
            "Отмена идеи",
        ):
            self.assertIn(expected, text)

        for noise in (
            "PUMPDUMP SIGNAL",
            "READY",
            "PARTIAL",
            "Дельта объёма",
            "Источники:",
            "Ликвидации лонгов",
            "Ликвидации шортов",
        ):
            self.assertNotIn(noise, text)

        self.assertLessEqual(len(text.splitlines()), 13)

    def test_short_card_uses_red_and_plain_russian(self):
        signal = {
            "symbol": "APRUSDT",
            "price": 0.04113,
            "change": -4.5,
            "oi_change": 2.2,
            "decision": {
                "pattern": "NEW_SHORT_BUILDUP",
                "direction": "DOWN",
            },
            "aggregated_oi": {
                "windows": {
                    "5m": {
                        "ready": True,
                        "change_pct": 1.7,
                        "confidence": "MEDIUM",
                        "consensus_mode": "2/2_SUPPORTED",
                    }
                }
            },
            "futures_flow": {
                "window_ready": True,
                "imbalance_pct": -18.0,
            },
            "spot_cvd": {
                "available": True,
                "cvd_percent": -6.0,
            },
            "liquidations": {},
        }

        text = build_short_message(signal)

        self.assertIn("🔴 <b>SHORT — APRUSDT</b>", text)
        self.assertIn("сила: 🟡 СРЕДНЯЯ", text)
        self.assertIn("2/2 доступных биржи", text)
        self.assertIn("Продавцы контролируют движение", text)


if __name__ == "__main__":
    unittest.main()
