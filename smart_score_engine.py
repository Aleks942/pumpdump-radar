"""
SMART SCORE ENGINE V1

Единая оценка качества сигнала.

Все остальные модули
используют именно этот результат.
"""


def calculate_smart_score(signal):

    score = 0

    reasons = []

    # =========================
    # OI
    # =========================

    oi = signal.get("oi_change")

    if oi is not None:

        if oi >= 10:

            score += 20
            reasons.append("Очень сильный рост OI")

        elif oi >= 5:

            score += 15
            reasons.append("Растёт OI")

        elif oi >= 2:

            score += 8
            reasons.append("OI усиливается")

        elif oi <= -10:

            score += 20
            reasons.append("Сильный выход денег")

        elif oi <= -5:

            score += 15
            reasons.append("Деньги выходят")

    # =========================
    # Давление
    # =========================

    pressure = signal.get("pressure_state", "")

    if "BUY+++" in pressure:

        score += 15
        reasons.append("Сильное давление покупателей")

    elif "SELL+++" in pressure:

        score += 15
        reasons.append("Сильное давление продавцов")

    elif "BUY" in pressure:

        score += 8
        reasons.append("Покупатели сильнее")

    elif "SELL" in pressure:

        score += 8
        reasons.append("Продавцы сильнее")

    # =========================
    # Spot
    # =========================

    spot = signal.get("spot_cvd") or {}

    if spot.get("available"):

        cvd = abs(
            spot.get("cvd_percent", 0)
        )

        if cvd >= 15:

            score += 15
            reasons.append("Сильный Spot CVD")

        elif cvd >= 5:

            score += 8
            reasons.append("Spot поддерживает движение")

    # =========================
    # Ликвидации
    # =========================

    liq = signal.get("liquidations") or {}

    total_liq = (
        liq.get("long_liq", 0)
        +
        liq.get("short_liq", 0)
    )

    if total_liq >= 500000:

        score += 15
        reasons.append("Большая ликвидация")

    elif total_liq >= 100000:

        score += 8
        reasons.append("Есть ликвидации")

    # =========================
    # Размер движения
    # =========================

    move = abs(
        signal.get("change", 0)
    )

    if 5 <= move <= 8:

        score += 10
        reasons.append("Импульс только начинается")

    elif move > 12:

        score -= 10
        reasons.append("Импульс уже далеко")

    # =====================================
    # Ограничение
    # =====================================
    
    score = max(
        0,
        min(score, 100)
    )
    
    # =====================================
    # Рейтинг
    # =====================================
    
    if score >= 90:
    
        rating = "A+"
        stars = "⭐⭐⭐⭐⭐"
        quality = "ELITE"
        risk = "LOW"
    
    elif score >= 75:
    
        rating = "A"
        stars = "⭐⭐⭐⭐"
        quality = "STRONG"
        risk = "LOW"
    
    elif score >= 60:
    
        rating = "B"
        stars = "⭐⭐⭐"
        quality = "GOOD"
        risk = "MEDIUM"
    
    elif score >= 40:
    
        rating = "C"
        stars = "⭐⭐"
        quality = "AVERAGE"
        risk = "HIGH"
    
    else:
    
        rating = "D"
        stars = "⭐"
        quality = "WEAK"
        risk = "VERY_HIGH"
    
    # =====================================
    # RETURN
    # =====================================
    
    return {
    
        "score": score,
    
        "rating": rating,
    
        "stars": stars,
    
        "quality": quality,
    
        "risk": risk,
    
        "reasons": reasons,
    
    }
