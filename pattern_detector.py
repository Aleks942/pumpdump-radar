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

    if oi_change is None:
        return {
            "pattern": "NONE",
            "direction": "NONE",
            "reason": "OI data unavailable",
        }

    spot_up = spot_cvd is not None and spot_cvd > 0
    spot_down = spot_cvd is not None and spot_cvd < 0

    # 1. NEW LONG BUILDUP
    if (
        price_change > 0
        and oi_change > 0
        and futures_cvd > 0
        and spot_cvd is not None
        and spot_cvd >= 5.0
        and delta > 0
    ):
        return {
            "pattern": "NEW_LONG_BUILDUP",
            "direction": "UP",
            "reason": "Цена↑ + OI↑ + покупки во фьючерсах + Spot CVD заметно↑",
        }

    # 2. NEW SHORT BUILDUP
    if (
        price_change < 0
        and oi_change > 0
        and futures_cvd < 0
        and spot_cvd is not None
        and spot_cvd <= -5.0
        and delta < 0
    ):
        return {
            "pattern": "NEW_SHORT_BUILDUP",
            "direction": "DOWN",
            "reason": "Цена↓ + OI↑ + продажи во фьючерсах + Spot CVD заметно↓",
        }

    # 3. SHORT SQUEEZE
    if (
        price_change > 0
        and oi_change <= -0.10
        and futures_cvd > 0
        and short_liquidations > long_liquidations
    ):
        return {
            "pattern": "SHORT_SQUEEZE",
            "direction": "UP",
            "reason": "Цена↑ + OI заметно↓ + покупки во фьючерсах + ликвидации шортов преобладают",
        }

    # 4. LONG LIQUIDATION
    if (
        price_change < 0
        and oi_change <= -0.10
        and futures_cvd < 0
        and long_liquidations > short_liquidations
    ):
        return {
            "pattern": "LONG_LIQUIDATION",
            "direction": "DOWN",
            "reason": "Цена↓ + OI заметно↓ + продажи во фьючерсах + ликвидации лонгов преобладают",
        }

    
   

    return {
        "pattern": "NONE",
        "direction": "NONE",
        "reason": "No complete pattern",
    }
