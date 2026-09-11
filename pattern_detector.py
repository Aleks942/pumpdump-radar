def detect_pattern(
    price_change,
    oi_change,
    futures_cvd,
    spot_cvd,
    delta,
    long_liquidations=0,
    short_liquidations=0,
):
    """
    Возвращает:
    {
        "pattern": str,
        "direction": "UP" | "DOWN" | "NONE",
        "reason": str
    }
    """

    # 1. NEW LONG BUILDUP
    if (
        price_change > 0
        and oi_change > 0
        and futures_cvd > 0
        and spot_cvd > 0
        and delta > 0
    ):
        return {
            "pattern": "NEW_LONG_BUILDUP",
            "direction": "UP",
            "reason": "Price↑ + OI↑ + Futures CVD↑ + Spot CVD↑ + Delta↑",
        }

    # 2. NEW SHORT BUILDUP
    if (
        price_change < 0
        and oi_change > 0
        and futures_cvd < 0
        and spot_cvd < 0
        and delta < 0
    ):
        return {
            "pattern": "NEW_SHORT_BUILDUP",
            "direction": "DOWN",
            "reason": "Price↓ + OI↑ + Futures CVD↓ + Spot CVD↓ + Delta↓",
        }

    # 3. SHORT SQUEEZE
    if (
        price_change > 0
        and oi_change < 0
        and futures_cvd > 0
        and short_liquidations > 0
    ):
        return {
            "pattern": "SHORT_SQUEEZE",
            "direction": "UP",
            "reason": "Price↑ + OI↓ + Futures buying + Short liquidations",
        }

    # 4. LONG LIQUIDATION
    if (
        price_change < 0
        and oi_change < 0
        and futures_cvd < 0
        and long_liquidations > 0
    ):
        return {
            "pattern": "LONG_LIQUIDATION",
            "direction": "DOWN",
            "reason": "Price↓ + OI↓ + Futures selling + Long liquidations",
        }

    # 5. SELL ABSORPTION
    if (
        futures_cvd < 0
        and spot_cvd < 0
        and delta < 0
        and price_change >= -0.3
    ):
        return {
            "pattern": "SELL_ABSORPTION",
            "direction": "UP",
            "reason": "Strong selling but price is not falling",
        }

    # 6. BUY ABSORPTION
    if (
        futures_cvd > 0
        and spot_cvd > 0
        and delta > 0
        and price_change <= 0.3
    ):
        return {
            "pattern": "BUY_ABSORPTION",
            "direction": "DOWN",
            "reason": "Strong buying but price is not rising",
        }

    # 7. FAILED HIGH
    if (
        price_change < 0
        and futures_cvd < 0
        and spot_cvd < 0
        and delta < 0
    ):
        return {
            "pattern": "FAILED_HIGH",
            "direction": "DOWN",
            "reason": "Price rejected higher levels and flow turned bearish",
        }

    # 8. FAILED LOW
    if (
        price_change > 0
        and futures_cvd > 0
        and spot_cvd > 0
        and delta > 0
    ):
        return {
            "pattern": "FAILED_LOW",
            "direction": "UP",
            "reason": "Price rejected lower levels and flow turned bullish",
        }

    return {
        "pattern": "NONE",
        "direction": "NONE",
        "reason": "No complete pattern",
    }
