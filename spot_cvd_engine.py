import requests


OKX_SPOT_TRADES_URL = "https://www.okx.com/api/v5/market/trades"


def swap_to_spot_symbol(raw_symbol):
    """
    BTC-USDT-SWAP -> BTC-USDT
    """

    if not raw_symbol:
        return None

    if raw_symbol.endswith("-USDT-SWAP"):
        return raw_symbol.replace("-USDT-SWAP", "-USDT")

    return None


def classify_spot_cvd(cvd_percent):
    """
    Классификация давления на спотовом рынке.
    """

    if cvd_percent >= 30:
        return {
            "state": "STRONG_SPOT_BUY",
            "vote": "CONTINUE",
            "weight": 4,
            "text": "Спот активно покупают"
        }

    if cvd_percent >= 10:
        return {
            "state": "SPOT_BUY",
            "vote": "CONTINUE",
            "weight": 2,
            "text": "Спотовые покупки сильнее"
        }

    if cvd_percent <= -30:
        return {
            "state": "STRONG_SPOT_SELL",
            "vote": "EXHAUSTION",
            "weight": 4,
            "text": "На споте активно продают"
        }

    if cvd_percent <= -10:
        return {
            "state": "SPOT_SELL",
            "vote": "EXHAUSTION",
            "weight": 2,
            "text": "Спотовые продажи сильнее"
        }

    return {
        "state": "SPOT_BALANCE",
        "vote": "NEUTRAL",
        "weight": 1,
        "text": "На споте баланс"
    }


def get_spot_cvd(raw_symbol, limit=100):
    """
    Получает последние сделки OKX Spot и считает:

    buy_quote_volume  — объём агрессивных покупок в USDT
    sell_quote_volume — объём агрессивных продаж в USDT
    delta             — покупки минус продажи
    cvd_percent       — нормализованный перевес в процентах
    """

    spot_symbol = swap_to_spot_symbol(raw_symbol)

    if not spot_symbol:
        return {
            "available": False,
            "spot_symbol": None,
            "state": "NO_SPOT_SYMBOL",
            "vote": "UNKNOWN",
            "weight": 0,
            "text": "Спотовая пара не определена"
        }

    params = {
        "instId": spot_symbol,
        "limit": str(limit)
    }

    try:
        response = requests.get(
            OKX_SPOT_TRADES_URL,
            params=params,
            timeout=15
        )

        if response.status_code != 200:
            print(
                "[SPOT_CVD_HTTP_ERROR]",
                spot_symbol,
                response.status_code,
                flush=True
            )

            return {
                "available": False,
                "spot_symbol": spot_symbol,
                "state": "HTTP_ERROR",
                "vote": "UNKNOWN",
                "weight": 0,
                "text": "Нет данных Spot CVD"
            }

        payload = response.json()

        if payload.get("code") != "0":
            print(
                "[SPOT_CVD_API_ERROR]",
                spot_symbol,
                payload,
                flush=True
            )

            return {
                "available": False,
                "spot_symbol": spot_symbol,
                "state": "API_ERROR",
                "vote": "UNKNOWN",
                "weight": 0,
                "text": "Спотовая пара недоступна"
            }

        trades = payload.get("data", [])

        if not trades:
            return {
                "available": False,
                "spot_symbol": spot_symbol,
                "state": "NO_TRADES",
                "vote": "UNKNOWN",
                "weight": 0,
                "text": "Нет спотовых сделок"
            }

        buy_quote_volume = 0.0
        sell_quote_volume = 0.0
        valid_trades = 0

        for trade in trades:

            try:
                price = float(trade.get("px", 0))
                size = float(trade.get("sz", 0))
                side = trade.get("side", "").lower()

                if price <= 0 or size <= 0:
                    continue

                quote_volume = price * size

                if side == "buy":
                    buy_quote_volume += quote_volume

                elif side == "sell":
                    sell_quote_volume += quote_volume

                else:
                    continue

                valid_trades += 1

            except (TypeError, ValueError):
                continue

        total_quote_volume = (
            buy_quote_volume
            + sell_quote_volume
        )

        if total_quote_volume <= 0:
            return {
                "available": False,
                "spot_symbol": spot_symbol,
                "state": "EMPTY_VOLUME",
                "vote": "UNKNOWN",
                "weight": 0,
                "text": "Недостаточно Spot CVD данных"
            }

        delta = (
            buy_quote_volume
            - sell_quote_volume
        )

        cvd_percent = (
            delta / total_quote_volume
        ) * 100

        classification = classify_spot_cvd(
            cvd_percent
        )

        result = {
            "available": True,
            "spot_symbol": spot_symbol,

            "trade_count": valid_trades,

            "buy_quote_volume": buy_quote_volume,
            "sell_quote_volume": sell_quote_volume,
            "total_quote_volume": total_quote_volume,

            "delta": delta,
            "cvd_percent": cvd_percent,

            "state": classification["state"],
            "vote": classification["vote"],
            "weight": classification["weight"],
            "text": classification["text"],
        }

        print(
            "[SPOT_CVD]",
            spot_symbol,
            "trades=",
            valid_trades,
            "buy_usdt=",
            round(buy_quote_volume, 2),
            "sell_usdt=",
            round(sell_quote_volume, 2),
            "delta=",
            round(delta, 2),
            "cvd_percent=",
            round(cvd_percent, 2),
            "state=",
            classification["state"],
            flush=True
        )

        return result

    except Exception as error:

        print(
            "[SPOT_CVD_EXCEPTION]",
            spot_symbol,
            error,
            flush=True
        )

        return {
            "available": False,
            "spot_symbol": spot_symbol,
            "state": "EXCEPTION",
            "vote": "UNKNOWN",
            "weight": 0,
            "text": "Ошибка Spot CVD"
        }
