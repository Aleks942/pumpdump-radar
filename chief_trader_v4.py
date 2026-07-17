from typing import Any, Dict, Optional


ENGINE_VERSION = "V4.0"


def _to_float(value: Any, default: float = 0.0) -> float:
    """
    Безопасно преобразует значение в число.
    """

    try:
        if value is None:
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def _get_pressure(signal: Dict[str, Any]) -> str:
    """
    Получает давление покупателей/продавцов.
    """

    money = signal.get("money") or {}

    return str(
        money.get("pressure")
        or ""
    )


def detect_early_move(
    signal: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Ищет зарождение движения ДО пампа или дампа.

    Важно:
    цена ещё должна стоять относительно спокойно,
    а OI и давление уже должны усиливаться.
    """

    price_change = _to_float(
        signal.get("change")
    )

    oi_change_raw = signal.get("oi_change")

    if oi_change_raw is None:
        return None

    oi_change = _to_float(oi_change_raw)

    oi_slope = signal.get("oi_slope") or {}

    oi_acceleration = _to_float(
        oi_slope.get("acceleration")
    )

    pressure = _get_pressure(signal)

    # Для раннего движения цена ещё не должна
    # пройти полноценный памп или дамп.
    price_is_quiet = abs(price_change) <= 2.0

    oi_is_building = oi_change >= 3.0

    oi_is_accelerating = oi_acceleration > 0

    buyers_active = pressure in (
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
        "BUYERS_DOMINATE",
    )

    sellers_active = pressure in (
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
        "SELLERS_DOMINATE",
    )

    # =====================================
    # РАННЕЕ НАКОПЛЕНИЕ ПЕРЕД РОСТОМ
    # =====================================

    if (
        price_is_quiet
        and oi_is_building
        and oi_is_accelerating
        and buyers_active
    ):
        strength = 50

        strength += min(
            25,
            int(oi_change * 2)
        )

        strength += min(
            15,
            int(abs(oi_acceleration) * 5)
        )

        strength = max(
            0,
            min(95, strength)
        )

        return {
            "detected": True,
            "state": "SMART_ACCUMULATION",
            "direction": "LONG",
            "title": "🟢 Зарождается движение вверх",
            "market_summary": (
                "Цена пока почти стоит, но новые позиции "
                "уже набираются."
            ),
            "stronger_summary": (
                "🟢 Покупатели постепенно усиливаются"
            ),
            "action_summary": (
                "👀 Добавить монету в наблюдение. "
                "Вход искать только после первого подтверждения роста."
            ),
            "next_move_summary": (
                "🟢 Возможен ранний импульс вверх"
            ),
            "probability": strength,
            "reasons": [
                f"OI вырос на {oi_change:.2f}%",
                "Цена ещё не ушла далеко вверх",
                "Покупательское давление усиливается",
                "Рост OI ускоряется",
            ],
        }

    # =====================================
    # РАННИЙ НАБОР ШОРТОВ ПЕРЕД ПАДЕНИЕМ
    # =====================================

    if (
        price_is_quiet
        and oi_is_building
        and oi_is_accelerating
        and sellers_active
    ):
        strength = 50

        strength += min(
            25,
            int(oi_change * 2)
        )

        strength += min(
            15,
            int(abs(oi_acceleration) * 5)
        )

        strength = max(
            0,
            min(95, strength)
        )

        return {
            "detected": True,
            "state": "SMART_DISTRIBUTION",
            "direction": "SHORT",
            "title": "🔴 Зарождается движение вниз",
            "market_summary": (
                "Цена пока почти стоит, но новые позиции "
                "уже набираются в сторону снижения."
            ),
            "stronger_summary": (
                "🔴 Продавцы постепенно усиливаются"
            ),
            "action_summary": (
                "👀 Добавить монету в наблюдение. "
                "Шорт искать только после подтверждения падения."
            ),
            "next_move_summary": (
                "🔴 Возможен ранний импульс вниз"
            ),
            "probability": strength,
            "reasons": [
                f"OI вырос на {oi_change:.2f}%",
                "Цена ещё не ушла далеко вниз",
                "Давление продавцов усиливается",
                "Рост OI ускоряется",
            ],
        }

    return None


def build_pump_dump_ui(
    signal: Dict[str, Any],
    legacy_decision: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Единая логика для пампа и дампа.

    Главная задача:
    определить, сохраняется ли импульс,
    начинает ли он ослабевать,
    или откат уже становится вероятным.

    Все тексты формируются из одного итогового состояния,
    поэтому противоречий быть не должно.
    """

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    change = _to_float(
        signal.get("change")
    )

    oi_raw = signal.get("oi_change")
    oi_available = oi_raw is not None
    oi_change = _to_float(oi_raw)

    pressure = _get_pressure(signal)

    legacy_reversal_score = _to_float(
        legacy_decision.get("reversal_score")
    )

    legacy_confidence = _to_float(
        legacy_decision.get("confidence"),
        50.0
    )

    buyers_active = pressure in (
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
        "BUYERS_DOMINATE",
        "BUY",
        "BUY+++",
    )

    sellers_active = pressure in (
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
        "SELLERS_DOMINATE",
        "SELL",
        "SELL+++",
    )

    # =====================================
    # 1. ОЦЕНКА ПРОДОЛЖЕНИЯ И ОСЛАБЛЕНИЯ
    # =====================================

    continuation_score = 0
    weakening_score = 0

    continuation_reasons = []
    weakening_reasons = []

    # =====================================
    # ПАМП
    # =====================================

    if move_type == "PUMP":

        # Покупатели поддерживают текущее движение
        if buyers_active:
            continuation_score += 30
            continuation_reasons.append(
                "Покупатели поддерживают рост"
            )

        # Продавцы работают против пампа
        if sellers_active:
            weakening_score += 30
            weakening_reasons.append(
                "Продавцы усиливаются против роста"
            )

        # Рост OI подтверждает новые позиции
        if oi_available and oi_change >= 5:
            continuation_score += 30
            continuation_reasons.append(
                f"OI растёт (+{oi_change:.2f}%)"
            )

        elif oi_available and oi_change >= 2:
            continuation_score += 15
            continuation_reasons.append(
                f"OI умеренно растёт (+{oi_change:.2f}%)"
            )

        # Падение OI во время роста — позиции закрываются
        elif oi_available and oi_change <= -5:
            weakening_score += 35
            weakening_reasons.append(
                f"OI падает ({oi_change:.2f}%)"
            )

        elif oi_available and oi_change <= -2:
            weakening_score += 20
            weakening_reasons.append(
                f"OI постепенно снижается ({oi_change:.2f}%)"
            )

    # =====================================
    # ДАМП
    # =====================================

    elif move_type == "DUMP":

        # Продавцы поддерживают текущее движение
        if sellers_active:
            continuation_score += 30
            continuation_reasons.append(
                "Продавцы поддерживают падение"
            )

        # Покупатели работают против дампа
        if buyers_active:
            weakening_score += 30
            weakening_reasons.append(
                "Покупатели усиливаются против падения"
            )

        # Рост OI подтверждает новые позиции
        if oi_available and oi_change >= 5:
            continuation_score += 30
            continuation_reasons.append(
                f"OI растёт (+{oi_change:.2f}%)"
            )

        elif oi_available and oi_change >= 2:
            continuation_score += 15
            continuation_reasons.append(
                f"OI умеренно растёт (+{oi_change:.2f}%)"
            )

        # Падение OI во время дампа — позиции закрываются
        elif oi_available and oi_change <= -5:
            weakening_score += 35
            weakening_reasons.append(
                f"OI падает ({oi_change:.2f}%)"
            )

        elif oi_available and oi_change <= -2:
            weakening_score += 20
            weakening_reasons.append(
                f"OI постепенно снижается ({oi_change:.2f}%)"
            )

    # =====================================
    # 2. УЧИТЫВАЕМ СТАРУЮ ОЦЕНКУ ОТКАТА
    # Но она больше не принимает решение одна.
    # =====================================

    if legacy_reversal_score >= 70:
        weakening_score += 30
        weakening_reasons.append(
            "Старый анализ видит сильные признаки отката"
        )

    elif legacy_reversal_score >= 50:
        weakening_score += 20
        weakening_reasons.append(
            "Есть заметные признаки отката"
        )

    elif legacy_reversal_score >= 30:
        weakening_score += 10
        weakening_reasons.append(
            "Появились первые признаки ослабления"
        )

    else:
        continuation_score += 10

    # =====================================
    # 3. ПЕРЕГРЕВ
    # Большое движение повышает риск отката,
    # но само по себе не подтверждает разворот.
    # =====================================

    if abs(change) >= 15:
        weakening_score += 20
        weakening_reasons.append(
            "Цена уже прошла очень большое расстояние"
        )

    elif abs(change) >= 10:
        weakening_score += 12
        weakening_reasons.append(
            "Движение заметно перегрето"
        )

    elif abs(change) >= 7:
        weakening_score += 5

    # =====================================
    # 4. ИТОГОВАЯ ВЕРОЯТНОСТЬ ОТКАТА
    # =====================================

    total_score = continuation_score + weakening_score

    if total_score > 0:
        reversal_probability = round(
            weakening_score
            / total_score
            * 100
        )
    else:
        reversal_probability = 50

    reversal_probability = max(
        10,
        min(90, reversal_probability)
    )

    continuation_probability = (
        100 - reversal_probability
    )

    # =====================================
    # 5. ОДНО ИТОГОВОЕ СОСТОЯНИЕ
    # =====================================

    if reversal_probability >= 70:
        state = "REVERSAL_RISK"

    elif reversal_probability >= 45:
        state = "WEAKENING"

    else:
        state = "MOVEMENT_STRONG"

    # =====================================
    # 6. СОГЛАСОВАННЫЙ ТЕКСТ ДЛЯ ПАМПА
    # =====================================

    if move_type == "PUMP":

        if state == "MOVEMENT_STRONG":

            market_summary = (
                "🟢 Памп пока сохраняет силу"
            )

            stronger_summary = (
                "🟢 Покупатели удерживают преимущество"
            )

            action_summary = (
                "⛔ Не шортить против сильного движения"
            )

            next_move_summary = (
                "🟢 Вероятнее продолжение роста"
            )

            scenario_probability = (
                continuation_probability
            )

            reasons = continuation_reasons

        elif state == "WEAKENING":

            market_summary = (
                "🟠 Памп начинает ослабевать"
            )

            stronger_summary = (
                "🟠 Покупатели ещё удерживают цену, "
                "но продавцы усиливаются"
            )

            action_summary = (
                "👀 Готовиться к откату, "
                "но шорт пока не подтверждён"
            )

            next_move_summary = (
                "🟡 Рост может замедлиться"
            )

            scenario_probability = (
                reversal_probability
            )

            reasons = weakening_reasons

        else:

            market_summary = (
                "🔴 Памп теряет поддержку"
            )

            stronger_summary = (
                "🔴 Продавцы перехватывают преимущество"
            )

            action_summary = (
                "🎯 Искать подтверждение разворота "
                "для входа в шорт"
            )

            next_move_summary = (
                "🔴 Вероятен откат вниз"
            )

            scenario_probability = (
                reversal_probability
            )

            reasons = weakening_reasons

    # =====================================
    # 7. СОГЛАСОВАННЫЙ ТЕКСТ ДЛЯ ДАМПА
    # =====================================

    else:

        if state == "MOVEMENT_STRONG":

            market_summary = (
                "🔴 Дамп пока сохраняет силу"
            )

            stronger_summary = (
                "🔴 Продавцы удерживают преимущество"
            )

            action_summary = (
                "⛔ Не покупать против сильного падения"
            )

            next_move_summary = (
                "🔴 Вероятнее продолжение падения"
            )

            scenario_probability = (
                continuation_probability
            )

            reasons = continuation_reasons

        elif state == "WEAKENING":

            market_summary = (
                "🟠 Дамп начинает ослабевать"
            )

            stronger_summary = (
                "🟠 Продавцы ещё удерживают движение, "
                "но покупатели усиливаются"
            )

            action_summary = (
                "👀 Готовиться к отскоку, "
                "но лонг пока не подтверждён"
            )

            next_move_summary = (
                "🟡 Падение может замедлиться"
            )

            scenario_probability = (
                reversal_probability
            )

            reasons = weakening_reasons

        else:

            market_summary = (
                "🟢 Дамп теряет поддержку"
            )

            stronger_summary = (
                "🟢 Покупатели перехватывают преимущество"
            )

            action_summary = (
                "🎯 Искать подтверждение разворота "
                "для входа в лонг"
            )

            next_move_summary = (
                "🟢 Вероятен отскок вверх"
            )

            scenario_probability = (
                reversal_probability
            )

            reasons = weakening_reasons

    # =====================================
    # 8. НАДЁЖНОСТЬ ДАННЫХ
    # =====================================

    confidence = legacy_confidence

    if not oi_available:
        confidence = min(
            confidence,
            60
        )

    if not buyers_active and not sellers_active:
        confidence = min(
            confidence,
            70
        )

    confidence = max(
        50,
        min(95, int(confidence))
    )

    if not reasons:
        reasons = [
            "Недостаточно сильных подтверждений"
        ]

    return {
        "mode": "PUMP_DUMP",

        "state": state,

        "market_summary": market_summary,
        "stronger_summary": stronger_summary,
        "action_summary": action_summary,
        "next_move_summary": next_move_summary,

        "scenario_probability": int(
            scenario_probability
        ),

        "reversal_probability": int(
            reversal_probability
        ),

        "continuation_probability": int(
            continuation_probability
        ),

        "confidence": confidence,

        "continuation_score": continuation_score,
        "weakening_score": weakening_score,

        "reasons": reasons[:3],
    }

def chief_trader_v4(
    signal: Dict[str, Any],
    legacy_decision: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Chief Trader V4 для текущего PumpDump Radar.

    Этот бот работает только с уже обнаруженными
    пампами и дампами и оценивает:

    1. движение ещё сильное;
    2. движение начинает ослабевать;
    3. появляется высокий риск отката.

    Раннее движение будет реализовано
    в отдельном проекте.
    """

    legacy_decision = legacy_decision or {}

    pump_dump_ui = build_pump_dump_ui(
        signal,
        legacy_decision,
    )

    return {
        "engine_version": ENGINE_VERSION,
        "mode": "PUMP_DUMP",
        "legacy_decision": legacy_decision,
        "early_move": None,
        "ui": pump_dump_ui,
    }
