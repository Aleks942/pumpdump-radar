"""
SMART MONEY ENGINE V1

Главная задача:
найти самые ранние признаки,
что крупные игроки начинают
набирать позицию.

Пока не принимает решений.
Он только оценивает состояние рынка.
"""

from typing import Dict, Any


def detect_smart_money(signal: Dict[str, Any]) -> Dict[str, Any]:

    oi_change = signal.get("oi_change")
    oi_slope = signal.get("oi_slope") or {}

    pressure = signal.get("pressure_state", "")

    change = abs(signal.get("change", 0))

    score = 0
    reasons = []

    # ==========================
    # Цена ещё спокойная
    # ==========================

    if change <= 2:

        score += 20
        reasons.append("Цена ещё стоит")

    # ==========================
    # OI растёт
    # ==========================

    if oi_change is not None:

        if oi_change >= 8:

            score += 30
            reasons.append("Сильный рост OI")

        elif oi_change >= 5:

            score += 20
            reasons.append("OI растёт")

        elif oi_change >= 2:

            score += 10
            reasons.append("OI начинает расти")

    # ==========================
    # Ускорение OI
    # ==========================

    acceleration = oi_slope.get(
        "acceleration",
        0
    )

    if acceleration > 0:

        score += 15
        reasons.append("Рост OI ускоряется")

    # ==========================
    # Давление покупателей
    # ==========================

    if "BUY" in pressure:

        score += 20
        reasons.append("Покупатели усиливаются")

    elif "SELL" in pressure:

        score += 20
        reasons.append("Продавцы усиливаются")

    # ==========================
    # Ограничение
    # ==========================

    score = max(
        0,
        min(score, 100)
    )

    # ==========================
    # Классификация
    # ==========================

    if score >= 80:

        state = "SMART_ACCUMULATION"

    elif score >= 60:

        state = "BUILDING"

    elif score >= 40:

        state = "WATCH"

    else:

        state = "NONE"

    return {

        "score": score,

        "state": state,

        "reasons": reasons

    }
