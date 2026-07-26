"""
SMART SCORE ENGINE V1

Единая оценка качества сигнала.

Все остальные модули
используют именно этот результат.
"""


def calculate_smart_score(signal):

    decision = signal.get("decision", {})
    context = decision.get("context", {})

    score = 0
    reasons = []

    # =====================================
    # MARKET HEALTH (0-25)
    # =====================================

    health = context.get("market_health", 0)

    score += round(health * 0.25)

    if health >= 80:
        reasons.append("Рынок находится в здоровом состоянии")

    elif health >= 60:
        reasons.append("Рынок остаётся стабильным")

    # =====================================
    # ENTRY SCORE (0-25)
    # =====================================

    entry = context.get("entry_score", 0)

    score += round(entry * 0.25)

    if entry >= 90:
        reasons.append("Очень сильная точка входа")

    elif entry >= 70:
        reasons.append("Есть хорошие условия для сделки")

    # =====================================
    # CONSENSUS (0-20)
    # =====================================

    consensus = context.get("consensus", 0)

    score += round(consensus * 0.20)

    if consensus >= 80:
        reasons.append("Большинство аналитиков согласны")

    elif consensus >= 60:
        reasons.append("Аналитики в основном поддерживают сценарий")

    # =====================================
    # MOVE ENERGY (0-15)
    # =====================================

    energy = context.get("move_energy", 0)

    score += round(energy * 0.15)

    if energy >= 80:
        reasons.append("Импульс сохраняет высокую энергию")

    elif energy >= 60:
        reasons.append("Движение ещё не ослабло")

    # =====================================
    # DATA QUALITY (0-15)
    # =====================================

    quality = context.get("data_quality", 0)

    score += round(quality * 0.15)

    if quality >= 90:
        reasons.append("Все ключевые данные доступны")

    elif quality >= 70:
        reasons.append("Данных достаточно для анализа")

    # =====================================
    # Бонусы Rule Engine
    # =====================================

    trade = decision.get("trade_state", "IGNORE")

    if trade == "ENTRY":
        score += 10
        reasons.append("Rule Engine разрешает вход")

    elif trade == "SETUP":
        score += 5
        reasons.append("Формируется качественная возможность")

    elif trade == "WATCH":
        score -= 5

    elif trade == "IGNORE":
        score -= 15

    # =====================================
    # Штраф за риск
    # =====================================

    risk = context.get("entry_risk", "HIGH")

    if risk == "LOW":
        pass

    elif risk == "MEDIUM":
        score -= 5

    elif risk == "HIGH":
        score -= 15

    # =====================================
    # Ограничение
    # =====================================

    score = max(0, min(score, 100))

    # =====================================
    # Рейтинг
    # =====================================

    if score >= 90:

        rating = "🟢 Отличный сигнал"
        stars = "⭐⭐⭐⭐⭐"
        quality = "Очень сильный"
        advice = "Можно искать вход"

    elif score >= 75:

        rating = "🟢 Сильный сигнал"
        stars = "⭐⭐⭐⭐"
        quality = "Сильный"
        advice = "Сделка выглядит качественно"

    elif score >= 60:

        rating = "🟡 Хороший сигнал"
        stars = "⭐⭐⭐"
        quality = "Хороший"
        advice = "Желательно дождаться подтверждения"

    elif score >= 40:

        rating = "🟠 Слабый сигнал"
        stars = "⭐⭐"
        quality = "Слабый"
        advice = "Лучше не торопиться"

    else:

        rating = "🔴 Очень слабый"
        stars = "⭐"
        quality = "Очень слабый"
        advice = "Лучше пропустить"

    return {

        "score": score,
        "rating": rating,
        "stars": stars,
        "quality": quality,
        "risk": risk,
        "advice": advice,
        "reasons": reasons[:5],

    }
