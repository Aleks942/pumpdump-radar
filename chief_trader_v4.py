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
    Формирует понятный вывод для уже обнаруженного
    пампа или дампа.

    Пока использует готовые данные старого Chief Trader,
    поэтому существующая логика не теряется.
    """

    move_type = signal.get("type")

    reversal_score = _to_float(
        legacy_decision.get("reversal_score")
    )

    confidence = _to_float(
        legacy_decision.get("confidence"),
        50.0
    )

    pressure = _get_pressure(signal)

    oi_change = signal.get("oi_change")

    # =====================================
    # КТО СИЛЬНЕЕ
    # =====================================

    if pressure in (
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
        "BUYERS_DOMINATE",
    ):
        stronger_summary = "🟢 Покупатели сильнее"

    elif pressure in (
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
        "SELLERS_DOMINATE",
    ):
        stronger_summary = "🔴 Продавцы сильнее"

    else:
        stronger_summary = "⚪ Явного преимущества нет"

    # =====================================
    # ПАМП
    # =====================================

    if move_type == "PUMP":

        if oi_change is not None and _to_float(oi_change) >= 3:
            market_summary = (
                "🟢 Рост поддерживается новыми позициями"
            )

        elif oi_change is not None and _to_float(oi_change) <= -3:
            market_summary = (
                "🟠 Цена растёт, но открытые позиции закрываются"
            )

        else:
            market_summary = (
                "🟡 Рост продолжается без сильного подтверждения OI"
            )

        if reversal_score >= 70:
            action_summary = (
                "⛔ Не покупать. Шорт искать только "
                "после подтверждения разворота."
            )
            next_move_summary = (
                "🔴 Вероятна коррекция вниз"
            )

        elif reversal_score >= 50:
            action_summary = (
                "👀 Не покупать по текущей цене. "
                "Дождаться коррекции."
            )
            next_move_summary = (
                "🟠 Возможна коррекция вниз"
            )

        elif reversal_score >= 30:
            action_summary = (
                "⚠️ Новый вход рискован. "
                "Наблюдать за ослаблением роста."
            )
            next_move_summary = (
                "🟡 Рост может замедлиться"
            )

        else:
            action_summary = (
                "🟢 Не шортить. Вход искать после "
                "небольшого отката и удержания уровня."
            )
            next_move_summary = (
                "🟢 Вероятнее продолжение роста"
            )

    # =====================================
    # ДАМП
    # =====================================

    else:

        if oi_change is not None and _to_float(oi_change) >= 3:
            market_summary = (
                "🔴 Падение поддерживается новыми позициями"
            )

        elif oi_change is not None and _to_float(oi_change) <= -3:
            market_summary = (
                "🟠 Цена падает, но открытые позиции закрываются"
            )

        else:
            market_summary = (
                "🟡 Падение продолжается без сильного подтверждения OI"
            )

        if reversal_score >= 70:
            action_summary = (
                "🟢 Не открывать новый шорт. "
                "Лонг искать только после подтверждения разворота."
            )
            next_move_summary = (
                "🟢 Вероятен сильный отскок вверх"
            )

        elif reversal_score >= 50:
            action_summary = (
                "👀 Не открывать новый шорт. "
                "Дождаться отскока."
            )
            next_move_summary = (
                "🟢 Возможен отскок вверх"
            )

        elif reversal_score >= 30:
            action_summary = (
                "⚠️ Новый шорт рискован. "
                "Наблюдать за ослаблением падения."
            )
            next_move_summary = (
                "🟡 Падение может замедлиться"
            )

        else:
            action_summary = (
                "⛔ Пока не покупать. "
                "Падение может продолжиться."
            )
            next_move_summary = (
                "🔴 Вероятнее продолжение падения"
            )

    return {
        "mode": "PUMP_DUMP",
        "market_summary": market_summary,
        "stronger_summary": stronger_summary,
        "action_summary": action_summary,
        "next_move_summary": next_move_summary,
        "scenario_probability": reversal_score,
        "confidence": confidence,
    }


def chief_trader_v4(
    signal: Dict[str, Any],
    legacy_decision: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Новый Chief Trader V4.

    Пока работает параллельно со старым движком:
    - не заменяет старое решение;
    - не ломает Pump/Dump;
    - готовит отдельный режим раннего движения.
    """

    legacy_decision = legacy_decision or {}

    early_move = detect_early_move(signal)

    if early_move:
        return {
            "engine_version": ENGINE_VERSION,
            "mode": "EARLY_MOVE",
            "early_move": early_move,
            "ui": {
                "market_summary": early_move["market_summary"],
                "stronger_summary": early_move["stronger_summary"],
                "action_summary": early_move["action_summary"],
                "next_move_summary": early_move["next_move_summary"],
                "scenario_probability": early_move["probability"],
                "confidence": early_move["probability"],
            },
        }

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
