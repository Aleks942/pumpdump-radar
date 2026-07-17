from typing import Any, Dict, Optional

from chief_trader_v4 import chief_trader_v4


ENGINE_VERSION = "MASTER_TRADER_V1"


def _to_float(
    value: Any,
    default: float = 0.0,
) -> float:
    """
    Безопасно преобразует значение в число.
    """

    try:
        if value is None:
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def _get_pressure(
    signal: Dict[str, Any],
) -> str:
    """
    Получает техническое состояние давления.
    """

    money = signal.get("money") or {}

    return str(
        money.get("pressure")
        or ""
    ).upper()


def _get_spot_state(
    signal: Dict[str, Any],
) -> str:
    """
    Получает состояние Spot CVD.
    """

    spot = (
        signal.get("spot")
        or signal.get("spot_cvd")
        or {}
    )

    return str(
        spot.get("state")
        or ""
    ).upper()


def _get_money_state(
    signal: Dict[str, Any],
) -> str:
    """
    Получает состояние денежного потока.
    """

    money = signal.get("money") or {}

    return str(
        money.get("money_state")
        or ""
    ).upper()


def _collect_facts(
    signal: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Собирает факты от всех датчиков.

    Эта функция ничего не решает.
    Она только приводит данные к единому виду.
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

    oi_change = (
        _to_float(oi_raw)
        if oi_available
        else None
    )

    oi_slope = signal.get("oi_slope") or {}

    oi_total = _to_float(
        oi_slope.get("total_change")
    )

    oi_acceleration = _to_float(
        oi_slope.get("acceleration")
    )

    liquidations = signal.get("liquidations") or {}

    long_liquidations = _to_float(
        liquidations.get("long_liq")
    )

    short_liquidations = _to_float(
        liquidations.get("short_liq")
    )

    return {
        "symbol": signal.get("symbol"),

        "move_type": move_type,
        "change": change,
        "window": signal.get("window"),

        "oi_available": oi_available,
        "oi_change": oi_change,
        "oi_total": oi_total,
        "oi_acceleration": oi_acceleration,

        "pressure": _get_pressure(signal),

        "spot_state": _get_spot_state(signal),

        "money_state": _get_money_state(signal),

        "long_liquidations": long_liquidations,
        "short_liquidations": short_liquidations,
    }


def _build_final_ui(
    move_type: str,
    state: str,
    probability: int,
) -> Dict[str, str]:
    """
    Создаёт согласованный Telegram-текст
    только из одного финального состояния.
    """

    if move_type == "PUMP":

        if state == "MOVEMENT_STRONG":

            return {
                "market_summary":
                    "🟢 Памп пока сохраняет силу",

                "stronger_summary":
                    "🟢 Покупатели удерживают преимущество",

                "action_summary":
                    "⛔ Не шортить против сильного движения",

                "next_move_summary":
                    "🟢 Вероятнее продолжение роста",
            }

        if state == "WEAKENING":

            return {
                "market_summary":
                    "🟠 Памп начинает ослабевать",

                "stronger_summary":
                    "🟠 Продавцы усиливаются, "
                    "но разворот ещё не подтверждён",

                "action_summary":
                    "👀 Готовиться к откату. "
                    "Шорт открывать только после подтверждения",

                "next_move_summary":
                    "🟡 Рост может замедлиться",
            }

        if state == "REVERSAL_READY":

            return {
                "market_summary":
                    "🔴 Памп теряет поддержку",

                "stronger_summary":
                    "🔴 Продавцы перехватывают преимущество",

                "action_summary":
                    "🎯 Искать подтверждённый вход в шорт",

                "next_move_summary":
                    "🔴 Вероятен откат вниз",
            }

    if move_type == "DUMP":

        if state == "MOVEMENT_STRONG":

            return {
                "market_summary":
                    "🔴 Дамп пока сохраняет силу",

                "stronger_summary":
                    "🔴 Продавцы удерживают преимущество",

                "action_summary":
                    "⛔ Не покупать против сильного падения",

                "next_move_summary":
                    "🔴 Вероятнее продолжение падения",
            }

        if state == "WEAKENING":

            return {
                "market_summary":
                    "🟠 Дамп начинает ослабевать",

                "stronger_summary":
                    "🟠 Покупатели усиливаются, "
                    "но разворот ещё не подтверждён",

                "action_summary":
                    "👀 Готовиться к отскоку. "
                    "Лонг открывать только после подтверждения",

                "next_move_summary":
                    "🟡 Падение может замедлиться",
            }

        if state == "REVERSAL_READY":

            return {
                "market_summary":
                    "🟢 Дамп теряет поддержку",

                "stronger_summary":
                    "🟢 Покупатели перехватывают преимущество",

                "action_summary":
                    "🎯 Искать подтверждённый вход в лонг",

                "next_move_summary":
                    "🟢 Вероятен отскок вверх",
            }

    return {
        "market_summary":
            "⚪ Ситуация пока не определена",

        "stronger_summary":
            "⚪ Явного преимущества нет",

        "action_summary":
            "👀 Пропустить и ждать новых данных",

        "next_move_summary":
            "⚪ Направление не подтверждено",
    }


def master_trader(
    signal: Dict[str, Any],
    legacy_decision: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Главный финальный судья PumpDump Radar.

    Датчики и анализаторы передают ему данные.

    Только Master Trader имеет право сформировать:

    - итоговое состояние;
    - кто получает преимущество;
    - что вероятнее;
    - что делать.
    """

    legacy_decision = legacy_decision or {}

    facts = _collect_facts(signal)

    continuation_engine = chief_trader_v4(
        signal,
        legacy_decision,
    )

    continuation_ui = (
        continuation_engine.get("ui")
        or {}
    )

    continuation_state = str(
        continuation_ui.get("state")
        or "UNKNOWN"
    ).upper()

    reversal_probability = int(
        _to_float(
            continuation_ui.get(
                "reversal_probability"
            ),
            50,
        )
    )

    continuation_probability = int(
        _to_float(
            continuation_ui.get(
                "continuation_probability"
            ),
            50,
        )
    )

    weakening_score = _to_float(
        continuation_ui.get("weakening_score")
    )

    continuation_score = _to_float(
        continuation_ui.get("continuation_score")
    )

    reasons = list(
        continuation_ui.get("reasons")
        or []
    )

    pressure = facts["pressure"]

    buyers_active = pressure in (
        "BUY",
        "BUY+++",
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
        "BUYERS_DOMINATE",
    )

    sellers_active = pressure in (
        "SELL",
        "SELL+++",
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
        "SELLERS_DOMINATE",
    )

    move_type = facts["move_type"]

    # =====================================
    # Финальное состояние
    # =====================================

    if reversal_probability >= 70:

        # Одного высокого расчётного риска мало.
        # Нужна противоположная сторона движения.
        reversal_confirmed = (
            move_type == "PUMP"
            and sellers_active
        ) or (
            move_type == "DUMP"
            and buyers_active
        )

        if reversal_confirmed:
            final_state = "REVERSAL_READY"
        else:
            final_state = "WEAKENING"

    elif reversal_probability >= 45:

        final_state = "WEAKENING"

    else:

        final_state = "MOVEMENT_STRONG"

    # =====================================
    # Вероятность именно выбранного сценария
    # =====================================

    if final_state == "MOVEMENT_STRONG":
        scenario_probability = continuation_probability

    else:
        scenario_probability = reversal_probability

    scenario_probability = max(
        10,
        min(90, scenario_probability)
    )

    # =====================================
    # Надёжность данных
    # =====================================

    confidence = 50

    if facts["oi_available"]:
        confidence += 15

    if buyers_active or sellers_active:
        confidence += 15

    if facts["spot_state"]:
        confidence += 10

    if (
        facts["long_liquidations"] > 0
        or facts["short_liquidations"] > 0
    ):
        confidence += 5

    score_gap = abs(
        continuation_score
        - weakening_score
    )

    confidence += min(
        10,
        int(score_gap / 5)
    )

    confidence = max(
        50,
        min(95, confidence)
    )

    ui = _build_final_ui(
        move_type,
        final_state,
        scenario_probability,
    )

    return {
        "engine_version": ENGINE_VERSION,

        "state": final_state,

        "scenario_probability":
            scenario_probability,

        "confidence":
            confidence,

        "facts":
            facts,

        "continuation_engine": {
            "state":
                continuation_state,

            "continuation_probability":
                continuation_probability,

            "reversal_probability":
                reversal_probability,

            "continuation_score":
                continuation_score,

            "weakening_score":
                weakening_score,
        },

        "reasons":
            reasons[:3],

        "ui":
            ui,
    }
