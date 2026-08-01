from dataclasses import dataclass, field
from typing import Any
from adaptive_learning import load_weights

# ADAPTIVE_WEIGHTS = load_weights()

# ============================================
# CHIEF TRADER V7
# ============================================
@dataclass
class TradeDecision:
    """
    Единый результат работы Chief Trader V7.

    Все данные для Telegram и торгового решения
    находятся в одном объекте.
    """

    # Главное решение
    trade_state: str = "IGNORE"
    direction: str = "NONE"
    stage: str = "UNCERTAIN"

    # Числовые оценки
    confidence: int = 0
    buyers_power: int = 50
    sellers_power: int = 50
    move_energy: int = 0
    quality_score: int = 0

    # Состояние рынка
    consensus: int = 0
    data_quality: int = 0
    market_health: int = 0

    # Риск
    risk: str = "HIGH"
    can_trade: bool = False

    # Понятные тексты
    market_summary: str = "Недостаточно данных"
    control_summary: str = "Явного преимущества нет"
    action_summary: str = "Пропустить сигнал"
    next_move_summary: str = "Сценарий не определён"

    # Объяснение
    confirmations: list[str] = field(default_factory=list)
    obstacles: list[str] = field(default_factory=list)
    data_problems: list[str] = field(default_factory=list)

    # Технические данные
    votes: list[dict[str, Any]] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """
        Превращает объект в обычный словарь,
        чтобы его мог использовать main.py.
        """

        return {
            "trade_state": self.trade_state,
            "direction": self.direction,
            "stage": self.stage,

            "confidence": self.confidence,
            "buyers_power": self.buyers_power,
            "sellers_power": self.sellers_power,
            "move_energy": self.move_energy,
            "quality_score": self.quality_score,

            "consensus": self.consensus,
            "data_quality": self.data_quality,
            "market_health": self.market_health,

            "risk": self.risk,
            "can_trade": self.can_trade,

            "market_summary": self.market_summary,
            "control_summary": self.control_summary,
            "action_summary": self.action_summary,
            "next_move_summary": self.next_move_summary,

            "confirmations": self.confirmations,
            "obstacles": self.obstacles,
            "data_problems": self.data_problems,

            "votes": self.votes,
            "context": self.context,
        }

def safe_float(value, default=0.0):
    """
    Безопасное преобразование в float.
    """

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def safe_int(value, default=0):
    """
    Безопасное преобразование в int.
    """

    try:
        return int(value)

    except (TypeError, ValueError):
        return default


def normalize_vote(vote):

    vote = str(vote).upper()

    if vote in (
        "CONTINUE",
        "EXHAUSTION",
        "NEUTRAL",
        "UNKNOWN"
    ):
        return vote

    return "UNKNOWN"


def normalize_weight(weight):

    weight = safe_float(weight)

    if weight < 0:
        return 0

    if weight > 10:
        return 10

    return weight

def oi_vote(signal):

    oi = signal.get("oi_change")

    move_type = signal.get("type")

    if oi is None:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "OI_NO_DATA",
            "text": "OI нет данных"
        }

    abs_oi = abs(oi)

    if abs_oi >= 10:
        weight = 6

    elif abs_oi >= 7:
        weight = 5

    elif abs_oi >= 5:
        weight = 4

    elif abs_oi >= 3:
        weight = 3

    else:
        weight = 1

    # =====================================
    # PUMP
    # =====================================

    if move_type == "PUMP":

        if oi >= 3:

            return {
                "vote": "CONTINUE",
                "weight": weight,
                "reason": "LONGS_OPENING",
                "text": f"Во время роста OI увеличивается (+{oi:.2f}%). Покупатели продолжают открывать новые позиции."
            }

        if oi <= -3:

            return {
                "vote": "EXHAUSTION",
                "weight": weight,
                "reason": "LONGS_CLOSING",
                "text": f"Во время роста OI уменьшается ({oi:.2f}%). Покупатели закрывают позиции, импульс начинает слабеть."
            }

    # =====================================
    # DUMP
    # =====================================

    if move_type == "DUMP":

        if oi >= 3:

            return {
                "vote": "CONTINUE",
                "weight": weight,
                "reason": "SHORTS_OPENING",
                "text": f"Во время падения OI увеличивается (+{oi:.2f}%). Продавцы продолжают открывать новые шорты."
            }

        if oi <= -3:

            return {
                "vote": "EXHAUSTION",
                "weight": weight,
                "reason": "SHORTS_CLOSING",
                "text": f"Во время падения OI уменьшается ({oi:.2f}%). Продавцы закрывают шорты, давление ослабевает."
            }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "OI_NEUTRAL",
        "text": f"OI почти без изменений ({oi:.2f}%)"
    }

def trend_vote(signal):

    trend = signal.get("trend_strength", {})
    score = trend.get("score", 0)

    # =========================
    # Очень сильный импульс
    # =========================

    if score >= 3:

        return {
            "vote": "CONTINUE",
            "weight": 3,
            "reason": "TREND_STRONG",
            "text": "Цена движется очень уверенно"
        }

    # =========================
    # Нормальный импульс
    # =========================

    if score >= 2:

        return {
            "vote": "CONTINUE",
            "weight": 2,
            "reason": "TREND_GOOD",
            "text": "Цена сохраняет направление"
        }

    # =========================
    # Слабый импульс
    # =========================

    if score <= 1:

        return {
            "vote": "EXHAUSTION",
            "weight": 2,
            "reason": "TREND_WEAK",
            "text": "Цена теряет импульс"
        }

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": "TREND_NEUTRAL",

        "text": "Импульс неоднозначный"

    }

def money_vote(signal):

    money = signal.get("money")

    if not money:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_MONEY_DATA",
            "text": "Нет данных по деньгам"
        }

    score = money.get("money_score", 0)

    state = money.get("money_state", "")

    # ==========================
    # VERY STRONG MONEY
    # ==========================

    if score >= 5:

        return {
            "vote": "CONTINUE",
            "weight": 4,
            "reason": "STRONG_NEW_MONEY",
            "text": "Заходят деньги"
        }

    # ==========================
    # BUILDING MONEY
    # ==========================

    if score >= 3:

        return {
            "vote": "CONTINUE",
            "weight": 2,
            "reason": "BUILDING_MONEY",
            "text": "Деньги постепенно заходят"
        }

    # ==========================
    # WEAK FLOW
    # ==========================

    if (
        "WEAK" in state
        or
        "NO_CLEAR" in state
    ):

        return {
            "vote": "EXHAUSTION",
            "weight": 2,
            "reason": "WEAK_MONEY_FLOW",
            "text": "Деньги выходят"
        }

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": "MONEY_NEUTRAL",

        "text": "Поток денег нейтральный"

    }


def pressure_vote(signal):

    money = signal.get("money")

    if not money:
        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "PRESSURE_NO_DATA",
            "text": "Нет данных о давлении"
        }

    move = signal.get("type")
    pressure = money.get("pressure", "")

    # =========================
    # PUMP
    # =========================

    if move == "PUMP":

        if pressure == "STRONG_BUY_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 3,
                "reason": "PRESSURE_CONTINUE",
                "text": "Покупатели полностью контролируют движение"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE",
                "text": "Покупатели сильнее продавцов"
            }

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Продавцы усиливаются"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Продавцы усиливаются"
            }

    # =========================
    # DUMP
    # =========================

    if move == "DUMP":

        if pressure == "STRONG_SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 3,
                "reason": "PRESSURE_CONTINUE",
                "text": "Продавцы полностью контролируют движение"
            }

        if pressure == "SELL_PRESSURE":
            return {
                "vote": "CONTINUE",
                "weight": 2,
                "reason": "PRESSURE_CONTINUE",
                "text": "Продавцы сильнее покупателей"
            }

        if pressure == "STRONG_BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 3,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Покупатели усиливаются"
            }

        if pressure == "BUY_PRESSURE":
            return {
                "vote": "EXHAUSTION",
                "weight": 2,
                "reason": "PRESSURE_EXHAUSTION",
                "text": "Покупатели усиливаются"
            }

    return {
        "vote": "NEUTRAL",
        "weight": 1,
        "reason": "PRESSURE_NEUTRAL",
        "text": "Давление покупателей и продавцов почти одинаковое"
    }

def liquidation_vote(signal):

    liq = signal.get("liquidations")

    if not liq:
        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_LIQUIDATIONS",
            "text": "Нет данных по ликвидациям"
        }

    long_liq = liq.get("long_liq", 0)
    short_liq = liq.get("short_liq", 0)

    move = signal.get("type")

    # =========================
    # PUMP
    # =========================

    if move == "PUMP":

        if short_liq >= 100000 and short_liq > long_liq * 2:

            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "SHORT_SQUEEZE",
                "text": "Вынос шортов"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_SQUEEZE",
            "text": "Массового шорт-сквиза нет"
        }

    # =========================
    # DUMP
    # =========================

    if move == "DUMP":

        if long_liq >= 100000 and long_liq > short_liq * 2:

            return {
                "vote": "EXHAUSTION",
                "weight": 4,
                "reason": "LONG_CAPITULATION",
                "text": "Вынос лонгов"
            }

        return {
            "vote": "NEUTRAL",
            "weight": 1,
            "reason": "NO_CAPITULATION",
            "text": "Массовой капитуляции нет"
        }

    return {
        "vote": "UNKNOWN",
        "weight": 0,
        "reason": "UNKNOWN",
        "text": ""
    }

def spot_vote(signal):

    spot = signal.get("spot_cvd")

    if not spot or not spot.get("available"):

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "SPOT_NO_DATA",
            "text": "Нет данных Spot"
        }

    return {

        # Пока Spot только наблюдает
        "vote": "NEUTRAL",

        # На решение не влияет
        "weight": 0,

        # Чтобы Chief Trader видел состояние
        "reason": spot.get("state", "SPOT_UNKNOWN"),

        # Будет выводиться в разделе "Почему"
        "text": spot.get("text", "")

    }


def scenario_vote(signal):

    scenario = signal.get("money_scenario")

    if not scenario:

        return {
            "vote": "UNKNOWN",
            "weight": 0,
            "reason": "NO_SCENARIO",
            "text": "Сценарий не определён"
        }

    bias = scenario.get("bias", "WAIT")
    name = scenario.get("name", "UNKNOWN")
    title = scenario.get("title", "")

    # ===========================
    # Продолжение движения
    # ===========================

    if bias == "CONTINUE":

        return {
            "vote": "CONTINUE",
            "weight": 3,
            "reason": name,
            "text": f"Сценарий: {title}"
        }

    # ===========================
    # Вероятна коррекция
    # ===========================

    if bias == "CORRECTION":

        return {
            "vote": "EXHAUSTION",
            "weight": 3,
            "reason": name,
            "text": f"Сценарий: {title}"
        }

    # ===========================
    # Нет явного сценария
    # ===========================

    return {

        "vote": "NEUTRAL",

        "weight": 1,

        "reason": name,

        "text": f"Сценарий: {title}"

    }
def analyze_quality(signal):

    """
    Оценивает качество текущего движения.

    Возвращает:
    score: 0–100
    level: текстовая оценка
    reasons: причины оценки

    Эта функция НЕ решает:
    - входить или нет;
    - поздний ли вход;
    - будет ли разворот.

    Она оценивает только подтверждение движения.
    """

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    score = 0
    reasons = []

    # =====================================
    # 1. СИЛА ЦЕНОВОГО ДВИЖЕНИЯ
    # =====================================

    try:
        change = abs(
            float(signal.get("change") or 0)
        )
    except (TypeError, ValueError):
        change = 0

    if change >= 10:
        score += 20
        reasons.append("Цена показывает очень сильное движение")

    elif change >= 7:
        score += 17
        reasons.append("Цена показывает сильное движение")

    elif change >= 5:
        score += 14
        reasons.append("Цена уверенно движется")

    elif change >= 3:
        score += 10
        reasons.append("Минимальный импульс подтверждён")

    # =====================================
    # 2. OPEN INTEREST
    # =====================================

    oi_change = signal.get("oi_change")

    if oi_change is not None:

        try:
            oi_change = float(oi_change)
        except (TypeError, ValueError):
            oi_change = None

    if oi_change is not None:

        abs_oi = abs(oi_change)

        if abs_oi >= 10:
            oi_points = 25

        elif abs_oi >= 7:
            oi_points = 21

        elif abs_oi >= 5:
            oi_points = 18

        elif abs_oi >= 3:
            oi_points = 14

        elif abs_oi >= 1:
            oi_points = 7

        else:
            oi_points = 2

        # Рост OI подтверждает и PUMP, и DUMP:
        # при PUMP открываются новые позиции вверх;
        # при DUMP могут открываться новые шорты.
        if oi_change >= 3:

            score += oi_points

            if move_type == "PUMP":
                reasons.append(
                    "Рост поддерживается увеличением OI"
                )

            elif move_type == "DUMP":
                reasons.append(
                    "Падение поддерживается увеличением OI"
                )

        # Снижение OI не подтверждает качество движения.
        elif oi_change <= -3:

            score += max(
                0,
                oi_points // 3
            )

            reasons.append(
                "OI снижается — часть движения идёт через закрытие позиций"
            )

        else:

            score += oi_points

    # =====================================
    # 3. ДАВЛЕНИЕ
    # =====================================

    money = signal.get("money") or {}

    pressure = str(
        money.get("pressure")
        or ""
    ).upper()

    if move_type == "PUMP":

        if pressure == "STRONG_BUY_PRESSURE":
            score += 20
            reasons.append(
                "Покупатели полностью контролируют давление"
            )

        elif pressure == "BUY_PRESSURE":
            score += 14
            reasons.append(
                "Покупатели сохраняют преимущество"
            )

        elif pressure == "SELL_PRESSURE":
            score += 4
            reasons.append(
                "Продавцы мешают продолжению роста"
            )

        elif pressure == "STRONG_SELL_PRESSURE":
            reasons.append(
                "Сильное давление продавцов против роста"
            )

    elif move_type == "DUMP":

        if pressure == "STRONG_SELL_PRESSURE":
            score += 20
            reasons.append(
                "Продавцы полностью контролируют давление"
            )

        elif pressure == "SELL_PRESSURE":
            score += 14
            reasons.append(
                "Продавцы сохраняют преимущество"
            )

        elif pressure == "BUY_PRESSURE":
            score += 4
            reasons.append(
                "Покупатели мешают продолжению падения"
            )

        elif pressure == "STRONG_BUY_PRESSURE":
            reasons.append(
                "Сильное давление покупателей против падения"
            )

    # =====================================
    # 4. СИЛА ТРЕНДА
    # =====================================

    trend = signal.get("trend_strength") or {}

    try:
        trend_score = float(
            trend.get("score")
            or 0
        )
    except (TypeError, ValueError):
        trend_score = 0

    if trend_score >= 6:
        score += 15
        reasons.append(
            "Тренд подтверждает направление"
        )

    elif trend_score >= 3:
        score += 10
        reasons.append(
            "Тренд умеренно поддерживает движение"
        )

    elif trend_score > 0:
        score += 5

    # =====================================
    # 5. ДЕНЕЖНЫЙ СЦЕНАРИЙ
    # =====================================

    scenario = signal.get("money_scenario") or {}

    scenario_name = str(
        scenario.get("name")
        or ""
    ).upper()

    continuation_scenarios = {
        "HEALTHY_PUMP",
        "HEALTHY_DUMP",
        "SPOT_SUPPORTED_RISE",
        "SPOT_SUPPORTED_FALL",
        "NEW_LONGS",
        "NEW_SHORTS",
    }

    exhaustion_scenarios = {
        "OVERHEATED_PUMP",
        "OVERHEATED_DUMP",
        "WEAK_MONEY_FLOW",
        "MIXED_MONEY_FLOW",
    }

    if scenario_name in continuation_scenarios:
        score += 12
        reasons.append(
            "Денежный сценарий поддерживает движение"
        )

    elif scenario_name in exhaustion_scenarios:
        score += 3
        reasons.append(
            "Денежный сценарий даёт слабое подтверждение"
        )

    # =====================================
    # 6. ФИНАЛЬНАЯ ОЦЕНКА
    # =====================================

    score = max(
        0,
        min(100, round(score))
    )

    if score >= 85:
        level = "🔥 Очень сильное движение"

    elif score >= 70:
        level = "🟢 Сильное движение"

    elif score >= 55:
        level = "🟡 Умеренно подтверждённое движение"

    elif score >= 40:
        level = "🟠 Слабое подтверждение"

    else:
        level = "🔴 Движение плохо подтверждено"

    return {
        "score": score,
        "level": level,
        "reasons": reasons[:3],
    }

def _chief_safe_float(value, default=0.0):

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def _chief_collect_votes(signal):

    """
    Собирает независимые оценки аналитических модулей.

    Каждый модуль должен вернуть:

    {
        "vote": "CONTINUE" | "EXHAUSTION" | "NEUTRAL" | "UNKNOWN",
        "weight": число,
        "reason": техническая причина,
        "text": понятное объяснение
    }
    """

    vote_functions = [

        ("OI", oi_vote),

        ("TREND", trend_vote),

        ("MONEY", money_vote),

        ("PRESSURE", pressure_vote),

        ("LIQUIDATIONS", liquidation_vote),

        ("SPOT", spot_vote),

        ("SCENARIO", scenario_vote),

    ]

    votes = []

    for module_name, vote_function in vote_functions:

        try:

            result = vote_function(signal)

            if not isinstance(result, dict):

                raise TypeError(
                    f"{module_name} returned non-dict"
                )

        except Exception as error:

           

            result = {

                "vote": "UNKNOWN",

                "weight": 0,

                "reason": f"{module_name}_ERROR",

                "text": (
                    f"{module_name}: данные получить не удалось"
                ),
            }

        vote = str(
            result.get("vote")
            or "UNKNOWN"
        ).upper()

        if vote not in {

            "CONTINUE",

            "EXHAUSTION",

            "NEUTRAL",

            "UNKNOWN",

        }:

            vote = "UNKNOWN"

        weight = _chief_safe_float(
            result.get("weight"),
            0
        )
        
        adaptive = ADAPTIVE_WEIGHTS.get(
            module_name,
            1.0
        )
        
        weight *= adaptive
        
        weight = max(
            0.0,
            min(10.0, weight)
        )

        votes.append({

            "module": module_name,

            "vote": vote,

            "weight": weight,

            "reason": str(
                result.get("reason")
                or f"{module_name}_UNKNOWN"
            ),

            "text": str(
                result.get("text")
                or ""
            ).strip(),

        })

    return votes


def _chief_build_context(signal, votes):

    """
    Создаёт единый Decision Context.

    Здесь нет торгового решения.
    Только факты, голоса и качество данных.
    """

    continue_votes = 0
    exhaustion_votes = 0
    neutral_votes = 0
    unknown_votes = 0

    continue_weight = 0.0
    exhaustion_weight = 0.0

    for vote_data in votes:

        vote = vote_data["vote"]

        weight = vote_data["weight"]

        if vote == "CONTINUE":

            continue_votes += 1

            continue_weight += weight

        elif vote == "EXHAUSTION":

            exhaustion_votes += 1

            exhaustion_weight += weight

        elif vote == "NEUTRAL":

            neutral_votes += 1

        else:

            unknown_votes += 1

    active_votes = (
        continue_votes
        + exhaustion_votes
    )

    active_weight = (
        continue_weight
        + exhaustion_weight
    )

    if active_votes > 0:

        continue_vote_share = (
            continue_votes
            / active_votes
        )

        exhaustion_vote_share = (
            exhaustion_votes
            / active_votes
        )

        consensus = max(
            continue_vote_share,
            exhaustion_vote_share
        )

    else:

        continue_vote_share = 0.5

        exhaustion_vote_share = 0.5

        consensus = 0.0

    if active_weight > 0:

        continue_weight_share = (
            continue_weight
            / active_weight
        )

        exhaustion_weight_share = (
            exhaustion_weight
            / active_weight
        )

    else:

        continue_weight_share = 0.5

        exhaustion_weight_share = 0.5

    # Сила сценария:
    # 60% — вес голосов;
    # 40% — количество независимых подтверждений.

    continue_strength = (
        continue_weight_share * 0.60
        + continue_vote_share * 0.40
    )

    exhaustion_strength = (
        exhaustion_weight_share * 0.60
        + exhaustion_vote_share * 0.40
    )

    if continue_strength > exhaustion_strength:

        dominant_side = "CONTINUE"

        dominant_strength = continue_strength

        dominant_votes = continue_votes

    elif exhaustion_strength > continue_strength:

        dominant_side = "EXHAUSTION"

        dominant_strength = exhaustion_strength

        dominant_votes = exhaustion_votes

    else:

        dominant_side = "UNCERTAIN"

        dominant_strength = 0.5

        dominant_votes = 0

    money = signal.get("money") or {}

    pressure = str(
        money.get("pressure")
        or ""
    ).upper()

    spot = signal.get("spot_cvd") or {}

    spot_state = str(
        spot.get("state")
        or ""
    ).upper()

    spot_cvd_percent = _chief_safe_float(
        spot.get("cvd_percent"),
        0
    )

    trend = signal.get("trend_strength") or {}

    trend_score = _chief_safe_float(
        trend.get("score"),
        0
    )

    scenario = signal.get("money_scenario") or {}

    scenario_name = str(
        scenario.get("name")
        or ""
    ).upper()

    liquidations = (
        signal.get("liquidations")
        or {}
    )

    long_liq = _chief_safe_float(
        liquidations.get("long_liq"),
        0
    )

    short_liq = _chief_safe_float(
        liquidations.get("short_liq"),
        0
    )

    oi_change = signal.get("oi_change")

    if oi_change is not None:

        oi_change = _chief_safe_float(
            oi_change,
            None
        )

    oi_slope = signal.get("oi_slope") or {}

    oi_total = _chief_safe_float(
        oi_slope.get("total_change"),
        0
    )

    oi_acceleration = _chief_safe_float(
        oi_slope.get("acceleration"),
        0
    )

    # =====================================
    # ПОКРЫТИЕ ДАННЫХ
    # =====================================

    available_sources = 0

    core_sources = 0

    if oi_change is not None:

        available_sources += 1

        core_sources += 1

    if pressure:

        available_sources += 1

        core_sources += 1

    if spot_state or abs(spot_cvd_percent) > 0:

        available_sources += 1

        core_sources += 1

    if trend:

        available_sources += 1

    if scenario_name:

        available_sources += 1

    if long_liq > 0 or short_liq > 0:

        available_sources += 1

    if money:

        available_sources += 1

    total_sources = 7

    data_coverage = round(
        available_sources
        / total_sources
        * 100
    )

    return {

        "votes": votes,

        "continue_votes": continue_votes,

        "exhaustion_votes": exhaustion_votes,

        "neutral_votes": neutral_votes,

        "unknown_votes": unknown_votes,

        "continue_weight": round(
            continue_weight,
            2
        ),

        "exhaustion_weight": round(
            exhaustion_weight,
            2
        ),

        "active_votes": active_votes,

        "consensus": round(
            consensus * 100
        ),

        "continue_strength": round(
            continue_strength * 100
        ),

        "exhaustion_strength": round(
            exhaustion_strength * 100
        ),

        "dominant_side": dominant_side,

        "dominant_strength": round(
            dominant_strength * 100
        ),

        "dominant_votes": dominant_votes,

        "available_sources": available_sources,

        "core_sources": core_sources,

        "data_coverage": data_coverage,

        "oi_change": oi_change,

        "oi_total": oi_total,

        "oi_acceleration": oi_acceleration,

        "pressure": pressure,

        "spot_state": spot_state,

        "spot_cvd_percent": spot_cvd_percent,

        "trend_score": trend_score,

        "scenario_name": scenario_name,

        "long_liq": long_liq,

        "short_liq": short_liq,

    }


def _chief_classify_stage(signal, context):

    """
    Определяет стадию текущего движения.

    START       — движение только началось.
    EXPANSION   — импульс развивается.
    CLIMAX      — движение уже сильно растянуто.
    EXHAUSTION  — движение теряет поддержку.
    UNCERTAIN   — данных недостаточно.
    """

    change = abs(
        _chief_safe_float(
            signal.get("change"),
            0
        )
    )

    window = str(
        signal.get("window")
        or ""
    ).lower()

    dominant_side = context["dominant_side"]

    dominant_strength = context["dominant_strength"]

    consensus = context["consensus"]

    oi_change = context["oi_change"]

    oi_acceleration = context["oi_acceleration"]

    # Явное ослабление движения.

    if (
        dominant_side == "EXHAUSTION"
        and dominant_strength >= 65
        and consensus >= 60
    ):

        return "EXHAUSTION"

    # Слишком большое движение уже может быть поздним.

    if change >= 10:

        return "CLIMAX"

    if change >= 7 and window in {

        "20m",

        "30m",

    }:

        return "CLIMAX"

    # Раннее движение.

    if (
        change <= 4.5
        and window in {
            "5m",
            "10m",
            "20m",
        }
        and dominant_side == "CONTINUE"
        and dominant_strength >= 65
    ):

        return "START"

    # Развивающийся импульс.

    if (
        dominant_side == "CONTINUE"
        and dominant_strength >= 60
        and change < 8
    ):

        return "EXPANSION"

    # OI начал резко замедляться.

    if (
        oi_change is not None
        and oi_change < 0
        and oi_acceleration < -1
    ):

        return "EXHAUSTION"

    return "UNCERTAIN"


def _chief_validate_market(signal, context):

    """
    Первый фильтр V6.

    Решает, достоин ли сигнал дальнейшего анализа.
    """

    problems = []

    if context["available_sources"] < 3:

        problems.append(
            "Недостаточно независимых источников данных"
        )

    if context["core_sources"] < 1:

        problems.append(
            "Нет данных OI, давления или Spot CVD"
        )

    if context["active_votes"] < 2:

        problems.append(
            "Слишком мало активных голосов"
        )

    if context["data_coverage"] < 35:

        problems.append(
            "Низкое покрытие рыночных данных"
        )

    is_valid = len(problems) == 0

    return {

        "valid": is_valid,

        "problems": problems,

    }


def _chief_make_decision(
    signal,
    context,
    quality_engine,
    market_stage,
    validation
):

    """
    Главное торговое решение V6.

    IGNORE — сигнал не заслуживает внимания.
    WATCH  — наблюдать.
    SETUP  — идея формируется.
    ENTRY  — можно искать подтверждённый вход.
    EXIT   — текущее движение заканчивается.
    """

    move_type = str(
        signal.get("type")
        or ""
    ).upper()

    change = abs(
        _chief_safe_float(
            signal.get("change"),
            0
        )
    )

    window = str(
        signal.get("window")
        or ""
    ).lower()

    quality_score = round(
        _chief_safe_float(
            quality_engine.get("score"),
            0
        )
    )

    dominant_side = context["dominant_side"]

    dominant_strength = context["dominant_strength"]

    consensus = context["consensus"]

    active_votes = context["active_votes"]

    data_coverage = context["data_coverage"]

    core_sources = context["core_sources"]

    pressure = context["pressure"]

    # =====================================
    # 1. IGNORE
    # =====================================

    if not validation["valid"]:

        return {

            "trade_state": "IGNORE",

            "market_stage": "UNCERTAIN",

            "direction": "NONE",

            "title": "🚫 Пропустить сигнал",

            "action": (
                "Недостаточно данных для безопасного решения"
            ),

            "reason": (
                validation["problems"][0]
                if validation["problems"]
                else "Данных недостаточно"
            ),

        }

    # =====================================
    # 2. РАЗВОРОТ / ЗАВЕРШЕНИЕ ДВИЖЕНИЯ
    # =====================================

    if dominant_side == "EXHAUSTION":

        if (
            dominant_strength >= 78
            and consensus >= 70
            and active_votes >= 3
            and data_coverage >= 55
        ):

            if move_type == "PUMP":

                direction = "SHORT"

                action = (
                    "Ждать подтверждения разворота цены "
                    "и только потом искать шорт"
                )

            else:

                direction = "LONG"

                action = (
                    "Ждать подтверждения разворота цены "
                    "и только потом искать лонг"
                )

            return {

                "trade_state": "SETUP",

                "market_stage": "EXHAUSTION",

                "direction": direction,

                "title": "🎯 Формируется разворотный сетап",

                "action": action,

                "reason": (
                    "Несколько независимых модулей "
                    "подтверждают ослабление движения"
                ),

            }

        return {

            "trade_state": "EXIT",

            "market_stage": "EXHAUSTION",

            "direction": "NONE",

            "title": "🟠 Движение начинает выдыхаться",

            "action": (
                "Не входить против движения без "
                "подтверждения разворота"
            ),

            "reason": (
                "Есть признаки ослабления, "
                "но разворот ещё не подтверждён"
            ),

        }

    # =====================================
    # 3. ПРОДОЛЖЕНИЕ ДВИЖЕНИЯ
    # =====================================

    if dominant_side == "CONTINUE":

        # ENTRY разрешаем только для раннего движения,
        # хорошего качества и нескольких подтверждений.

        if (
            market_stage == "START"
            and dominant_strength >= 82
            and consensus >= 75
            and active_votes >= 4
            and quality_score >= 75
            and core_sources >= 2
            and data_coverage >= 55
            and change <= 5.5
            and window in {
                "5m",
                "10m",
                "20m",
            }
        ):

            if move_type == "PUMP":

                direction = "LONG"

                action = (
                    "Можно искать вход в лонг "
                    "после небольшого отката или ретеста"
                )

            else:

                direction = "SHORT"

                action = (
                    "Можно искать вход в шорт "
                    "после небольшого отката или ретеста"
                )

            return {

                "trade_state": "ENTRY",

                "market_stage": "START",

                "direction": direction,

                "title": "✅ Есть ранняя возможность входа",

                "action": action,

                "reason": (
                    "Движение раннее и подтверждено "
                    "несколькими независимыми источниками"
                ),

            }

        # SETUP — хороший сценарий,
        # но вход ещё нужно дождаться.

        if (
            dominant_strength >= 68
            and consensus >= 60
            and active_votes >= 3
            and quality_score >= 55
            and data_coverage >= 45
            and market_stage in {
                "START",
                "EXPANSION",
            }
        ):

            if move_type == "PUMP":

                direction = "LONG"

                action = (
                    "Наблюдать за откатом и искать "
                    "подтверждение продолжения роста"
                )

            else:

                direction = "SHORT"

                action = (
                    "Наблюдать за откатом и искать "
                    "подтверждение продолжения падения"
                )

            return {

                "trade_state": "SETUP",

                "market_stage": market_stage,

                "direction": direction,

                "title": "🎯 Формируется торговый сетап",

                "action": action,

                "reason": (
                    "Направление подтверждено, "
                    "но точка входа ещё не сформирована"
                ),

            }

        # Сильное, но позднее движение.

        if market_stage == "CLIMAX":

            return {

                "trade_state": "WATCH",

                "market_stage": "CLIMAX",

                "direction": "NONE",

                "title": "🔥 Сильное, но позднее движение",

                "action": (
                    "Не догонять цену. "
                    "Ждать откат или новый сетап"
                ),

                "reason": (
                    "Импульс подтверждён, "
                    "но цена уже прошла слишком много"
                ),

            }

    # =====================================
    # 4. WATCH
    # =====================================

    return {

        "trade_state": "WATCH",

        "market_stage": market_stage,

        "direction": "NONE",

        "title": "👀 Пока только наблюдать",

        "action": (
            "Пропустить вход и дождаться "
            "более ясного подтверждения"
        ),

        "reason": (
            "Эксперты пока не дают достаточно "
            "сильного и согласованного сигнала"
        ),

    }


def _chief_build_explanation(
    votes,
    dominant_side
):

    """
    Разделяет подтверждения и препятствия.
    """

    confirmations = []

    obstacles = []

    sorted_votes = sorted(

        votes,

        key=lambda item: item.get(
            "weight",
            0
        ),

        reverse=True
    )

    for vote_data in sorted_votes:

        vote = vote_data["vote"]

        text = vote_data.get("text", "")

        if not text:
            continue

        text = text.replace(
            "• ",
            ""
        ).strip()

        if vote == dominant_side:

            confirmations.append(text)

        elif vote in {

            "CONTINUE",

            "EXHAUSTION",

        }:

            obstacles.append(text)

    return {

        "confirmations": confirmations[:3],

        "obstacles": obstacles[:2],

    }



# ============================================
# VOTE COLLECTOR
# Собирает мнения всех аналитиков
# ============================================

def collect_votes(signal):

    modules = [

        ("OI", oi_vote),

        ("TREND", trend_vote),

        ("PRESSURE", pressure_vote),

        ("MONEY", money_vote),

        ("SPOT", spot_vote),

        ("LIQUIDATIONS", liquidation_vote),

        ("SCENARIO", scenario_vote),

    ]

    votes = []

    for name, func in modules:

        try:

            result = func(signal)

        except Exception as e:

           
            result = {

                "vote": "UNKNOWN",

                "weight": 0,

                "reason": f"{name}_ERROR",

                "text": f"{name}: ошибка"

            }

        vote = normalize_vote(

            result.get("vote")

        )

        weight = normalize_weight(

            result.get("weight")

        )

        votes.append({

            "module": name,

            "vote": vote,

            "weight": weight,

            "reason": result.get("reason", ""),

            "text": result.get("text", "")

        })

    return votes

# ============================================
# MARKET CONTEXT ENGINE
# Собирает голоса и качество доступных данных
# ============================================

def build_market_context(signal, votes):

    """
    Market Context не принимает торговое решение.

    Он только:
    - считает голоса модулей;
    - складывает силу голосов;
    - определяет согласие аналитиков;
    - оценивает полноту данных;
    - сохраняет сырые рыночные показатели.

    Buyers Power, Sellers Power, Move Energy
    и Market Health рассчитываются отдельно.
    """

    context = {
        # Тип найденного движения
        "move_type": str(
            signal.get("type")
            or ""
        ).upper(),

        "symbol": str(
            signal.get("symbol")
            or "UNKNOWN"
        ),

        "window": str(
            signal.get("window")
            or ""
        ),

        "change": safe_float(
            signal.get("change"),
            0.0
        ),

        # Количество голосов
        "continue_votes": 0,
        "exhaustion_votes": 0,
        "neutral_votes": 0,
        "unknown_votes": 0,
        "active_votes": 0,

        # Суммарная сила голосов
        "continue_weight": 0.0,
        "exhaustion_weight": 0.0,
        "active_weight": 0.0,

        # Согласие аналитиков
        "dominant_side": "UNCERTAIN",
        "consensus": 0,
        "vote_difference": 0,
        "weight_difference": 0.0,

        # Качество данных модулей
        "total_modules": len(votes),
        "available_modules": 0,
        "data_quality": 0,

        # Наличие ключевых данных
        "has_oi": False,
        "has_pressure": False,
        "has_spot_cvd": False,
        "has_trend": False,
        "has_liquidations": False,
        "has_money": False,
        "has_scenario": False,
        "core_data_count": 0,

        # Сырые рыночные показатели
        "oi_change": None,
        "oi_total": 0.0,
        "oi_acceleration": 0.0,

        "pressure": "",
        "money_state": "",

        "spot_state": "",
        "spot_cvd_percent": 0.0,

        "trend_score": 0.0,

        "long_liq": 0.0,
        "short_liq": 0.0,

        "scenario_name": "",

        # Голоса всех аналитиков
        "votes": votes,
    }

    # ============================================
    # 1. ПОДСЧЁТ ГОЛОСОВ
    # ============================================

    for vote_data in votes:

        vote_type = normalize_vote(
            vote_data.get("vote")
        )

        weight = normalize_weight(
            vote_data.get("weight")
        )

        if vote_type == "CONTINUE":

            context["continue_votes"] += 1
            context["continue_weight"] += weight

        elif vote_type == "EXHAUSTION":

            context["exhaustion_votes"] += 1
            context["exhaustion_weight"] += weight

        elif vote_type == "NEUTRAL":

            context["neutral_votes"] += 1

        else:

            context["unknown_votes"] += 1

    context["active_votes"] = (
        context["continue_votes"]
        + context["exhaustion_votes"]
    )

    context["active_weight"] = (
        context["continue_weight"]
        + context["exhaustion_weight"]
    )

    context["vote_difference"] = (
        context["continue_votes"]
        - context["exhaustion_votes"]
    )

    context["weight_difference"] = round(
        context["continue_weight"]
        - context["exhaustion_weight"],
        2
    )

    # ============================================
    # 2. ДОМИНИРУЮЩАЯ СТОРОНА
    # ============================================

    if (
        context["continue_votes"]
        > context["exhaustion_votes"]
    ):

        context["dominant_side"] = "CONTINUE"

    elif (
        context["exhaustion_votes"]
        > context["continue_votes"]
    ):

        context["dominant_side"] = "EXHAUSTION"

    elif (
        context["continue_weight"]
        > context["exhaustion_weight"]
    ):

        context["dominant_side"] = "CONTINUE"

    elif (
        context["exhaustion_weight"]
        > context["continue_weight"]
    ):

        context["dominant_side"] = "EXHAUSTION"

    # ============================================
    # 3. СОГЛАСИЕ АНАЛИТИКОВ
    # ============================================

    if context["active_votes"] > 0:

        strongest_vote_count = max(
            context["continue_votes"],
            context["exhaustion_votes"]
        )

        context["consensus"] = round(
            strongest_vote_count
            / context["active_votes"]
            * 100
        )

    # ============================================
    # 4. КАЧЕСТВО ДАННЫХ МОДУЛЕЙ
    # ============================================

    context["available_modules"] = sum(
        1
        for vote_data in votes
        if normalize_vote(
            vote_data.get("vote")
        ) != "UNKNOWN"
    )

    if context["total_modules"] > 0:

        context["data_quality"] = round(
            context["available_modules"]
            / context["total_modules"]
            * 100
        )

    # ============================================
    # 5. OPEN INTEREST
    # ============================================

    oi_change = signal.get("oi_change")

    if oi_change is not None:

        try:
            context["oi_change"] = float(
                oi_change
            )

            context["has_oi"] = True

        except (TypeError, ValueError):
            context["oi_change"] = None

    oi_slope = (
        signal.get("oi_slope")
        or {}
    )

    context["oi_total"] = safe_float(
        oi_slope.get("total_change"),
        0.0
    )

    context["oi_acceleration"] = safe_float(
        oi_slope.get("acceleration"),
        0.0
    )

    # ============================================
    # 6. MONEY FLOW И PRESSURE
    # ============================================

    money = (
        signal.get("money")
        or {}
    )

    context["money_state"] = str(
        money.get("money_state")
        or ""
    ).upper()

    context["pressure"] = str(
        money.get("pressure")
        or ""
    ).upper()

    context["has_money"] = bool(
        context["money_state"]
        or context["pressure"]
    )

    context["has_pressure"] = bool(
        context["pressure"]
    )

    # ============================================
    # 7. SPOT CVD
    # ============================================

    spot_cvd = (
        signal.get("spot_cvd")
        or {}
    )

    context["spot_state"] = str(
        spot_cvd.get("state")
        or ""
    ).upper()

    context["spot_cvd_percent"] = safe_float(
        spot_cvd.get("cvd_percent"),
        0.0
    )

    context["has_spot_cvd"] = bool(
        context["spot_state"]
        or abs(
            context["spot_cvd_percent"]
        ) > 0
    )

    # ============================================
    # 8. TREND
    # ============================================

    trend = (
        signal.get("trend_strength")
        or {}
    )

    context["trend_score"] = safe_float(
        trend.get("score"),
        0.0
    )

    context["has_trend"] = bool(trend)

    # ============================================
    # 9. LIQUIDATIONS
    # ============================================

    liquidations = (
        signal.get("liquidations")
        or {}
    )

    context["long_liq"] = safe_float(
        liquidations.get("long_liq"),
        0.0
    )

    context["short_liq"] = safe_float(
        liquidations.get("short_liq"),
        0.0
    )

    context["has_liquidations"] = (
        context["long_liq"] > 0
        or context["short_liq"] > 0
    )

    # ============================================
    # 10. MONEY SCENARIO
    # ============================================

    scenario = (
        signal.get("money_scenario")
        or {}
    )

    context["scenario_name"] = str(
        scenario.get("name")
        or ""
    ).upper()

    context["has_scenario"] = bool(
        context["scenario_name"]
    )

    # ============================================
    # 11. КЛЮЧЕВЫЕ ИСТОЧНИКИ
    # ============================================

    context["core_data_count"] = sum([
        context["has_oi"],
        context["has_pressure"],
        context["has_spot_cvd"],
    ])

    # Округляем накопленные веса.
    context["continue_weight"] = round(
        context["continue_weight"],
        2
    )

    context["exhaustion_weight"] = round(
        context["exhaustion_weight"],
        2
    )

    context["active_weight"] = round(
        context["active_weight"],
        2
    )

    return context

# ============================================
# MARKET HEALTH ENGINE
# Рассчитывает реальную силу рынка
# ============================================

def calculate_market_health(signal, context):

    buyers = 0
    sellers = 0

    # ========================================
    # OI
    # ========================================

    oi = context["oi_change"]

    move = context["move_type"]

    if oi is not None:

        if move == "PUMP":

            if oi >= 8:
                buyers += 25

            elif oi >= 5:
                buyers += 20

            elif oi >= 3:
                buyers += 15

            elif oi <= -5:
                sellers += 20

            elif oi <= -3:
                sellers += 15

        elif move == "DUMP":

            if oi >= 8:
                sellers += 25

            elif oi >= 5:
                sellers += 20

            elif oi >= 3:
                sellers += 15

            elif oi <= -5:
                buyers += 20

            elif oi <= -3:
                buyers += 15

    # ========================================
    # PRESSURE
    # ========================================

    pressure = context["pressure"]

    if pressure == "STRONG_BUY_PRESSURE":
        buyers += 20

    elif pressure == "BUY_PRESSURE":
        buyers += 12

    elif pressure == "STRONG_SELL_PRESSURE":
        sellers += 20

    elif pressure == "SELL_PRESSURE":
        sellers += 12

    # ========================================
    # SPOT CVD
    # ========================================

    if context["spot_state"] == "BUY":
        buyers += 18

    elif context["spot_state"] == "SELL":
        sellers += 18

    # ========================================
    # MONEY FLOW
    # ========================================

    money = context["money_state"]

    if money == "STRONG_NEW_MONEY":
        buyers += 15

    elif money == "BUILDING_MONEY":
        buyers += 10

    elif money == "EXITING_MONEY":
        sellers += 12

    # ========================================
    # TREND
    # ========================================

    trend = context["trend_score"]

    if trend >= 8:

        if move == "PUMP":
            buyers += 12
        else:
            sellers += 12

    elif trend >= 5:

        if move == "PUMP":
            buyers += 8
        else:
            sellers += 8

    # ========================================
    # LIQUIDATIONS
    # ========================================

    long_liq = context["long_liq"]
    short_liq = context["short_liq"]

    if short_liq > long_liq * 1.5:
        buyers += 10

    elif long_liq > short_liq * 1.5:
        sellers += 10

    # ========================================
    # НОРМАЛИЗАЦИЯ
    # ========================================

    total = buyers + sellers

    if total == 0:

        buyers_power = 50
        sellers_power = 50

    else:

        buyers_power = round(
            buyers / total * 100
        )

        sellers_power = 100 - buyers_power

    # ========================================
    # ЭНЕРГИЯ ДВИЖЕНИЯ
    # ========================================

    energy = round(

        (
            context["consensus"]
            * 0.4
        )

        +

        (
            context["data_quality"]
            * 0.3
        )

        +

        (
            max(
                buyers_power,
                sellers_power
            )
            * 0.3
        )

    )

    # ========================================
    # MARKET HEALTH
    # ========================================

    health = round(

        (
            energy
            * 0.6
        )

        +

        (
            context["data_quality"]
            * 0.4
        )

    )

    context["buyers_power"] = buyers_power
    context["sellers_power"] = sellers_power

    context["move_energy"] = energy
    context["market_health"] = health

    return context

# ============================================
# MARKET STAGE ENGINE
# Определяет стадию текущего пампа или дампа
# ============================================

def classify_market_stage(signal, context):

    """
    Определяет стадию уже найденного движения.

    START:
        движение раннее и поддерживается.

    EXPANSION:
        импульс развивается и остаётся здоровым.

    CLIMAX:
        цена уже прошла слишком много;
        вход по направлению движения становится поздним.

    WEAKENING:
        движение теряет энергию и встречает сопротивление.

    REVERSAL:
        противоположная сторона получила сильное
        и согласованное преимущество.

    UNCERTAIN:
        данных недостаточно или сигналы конфликтуют.
    """

    move_type = str(
        context.get("move_type")
        or signal.get("type")
        or ""
    ).upper()

    window = str(
        context.get("window")
        or signal.get("window")
        or ""
    ).lower()

    change = abs(
        safe_float(
            context.get(
                "change",
                signal.get("change")
            ),
            0.0
        )
    )

    buyers_power = safe_int(
        context.get("buyers_power"),
        50
    )

    sellers_power = safe_int(
        context.get("sellers_power"),
        50
    )

    move_energy = safe_int(
        context.get("move_energy"),
        0
    )

    market_health = safe_int(
        context.get("market_health"),
        0
    )

    consensus = safe_int(
        context.get("consensus"),
        0
    )

    data_quality = safe_int(
        context.get("data_quality"),
        0
    )

    oi_change = context.get("oi_change")

    oi_acceleration = safe_float(
        context.get("oi_acceleration"),
        0.0
    )

    # ========================================
    # 1. КТО ПОДДЕРЖИВАЕТ ТЕКУЩЕЕ ДВИЖЕНИЕ
    # ========================================

    if move_type == "PUMP":

        movement_power = buyers_power
        opposite_power = sellers_power

    elif move_type == "DUMP":

        movement_power = sellers_power
        opposite_power = buyers_power

    else:

        context["market_stage"] = "UNCERTAIN"
        context["stage_reason"] = (
            "Тип движения не определён"
        )

        return context

    power_difference = (
        movement_power
        - opposite_power
    )

    context["movement_power"] = movement_power
    context["opposite_power"] = opposite_power
    context["power_difference"] = power_difference

    # ========================================
    # 2. ПРОВЕРКА ДОСТАТОЧНОСТИ ДАННЫХ
    # ========================================

    if (
        data_quality < 35
        or context.get("available_modules", 0) < 3
    ):

        context["market_stage"] = "UNCERTAIN"

        context["stage_reason"] = (
            "Недостаточно данных для определения стадии"
        )

        return context

    # ========================================
    # 3. REVERSAL
    #
    # Противоположная сторона явно сильнее,
    # энергия движения низкая,
    # аналитики достаточно согласны.
    # ========================================

    if (
        opposite_power >= 72
        and power_difference <= -44
        and move_energy <= 48
        and consensus >= 65
    ):

        context["market_stage"] = "REVERSAL"

        context["stage_reason"] = (
            "Противоположная сторона получила "
            "сильное подтверждённое преимущество"
        )

        return context

    # ========================================
    # 4. WEAKENING
    #
    # Движение ещё не развернулось,
    # но уже теряет здоровье и энергию.
    # ========================================

    weakening_conditions = 0

    if opposite_power >= 58:
        weakening_conditions += 1

    if move_energy < 55:
        weakening_conditions += 1

    if market_health < 58:
        weakening_conditions += 1

    if power_difference < 15:
        weakening_conditions += 1

    if (
        oi_change is not None
        and safe_float(oi_change, 0) < -2
    ):
        weakening_conditions += 1

    if oi_acceleration < -1:
        weakening_conditions += 1

    if weakening_conditions >= 3:

        context["market_stage"] = "WEAKENING"

        context["stage_reason"] = (
            "Движение теряет энергию "
            "и встречает усиливающееся сопротивление"
        )

        return context

    # ========================================
    # 5. CLIMAX
    #
    # Движение сильное, но уже слишком растянуто.
    # Это не разворот, но вход по движению поздний.
    # ========================================

    climax_by_change = False

    if window == "5m" and change >= 7:
        climax_by_change = True

    elif window == "10m" and change >= 8:
        climax_by_change = True

    elif window == "20m" and change >= 9:
        climax_by_change = True

    elif window == "30m" and change >= 10:
        climax_by_change = True

    if (
        climax_by_change
        and movement_power >= 60
    ):

        context["market_stage"] = "CLIMAX"

        context["stage_reason"] = (
            "Импульс остаётся сильным, "
            "но цена уже прошла слишком большое расстояние"
        )

        return context

    # ========================================
    # 6. START
    #
    # Раннее движение:
    # цена ещё не ушла далеко,
    # сторона движения сильнее,
    # энергия и здоровье хорошие.
    # ========================================

    early_change_limit = {
        "5m": 4.5,
        "10m": 5.0,
        "20m": 5.5,
        "30m": 6.0,
    }.get(
        window,
        5.0
    )

    if (
        change <= early_change_limit
        and movement_power >= 68
        and power_difference >= 36
        and move_energy >= 65
        and market_health >= 62
        and consensus >= 60
    ):

        context["market_stage"] = "START"

        context["stage_reason"] = (
            "Движение ещё раннее и подтверждено "
            "несколькими рыночными факторами"
        )

        return context

    # ========================================
    # 7. EXPANSION
    #
    # Движение развивается:
    # энергия сохраняется,
    # доминирующая сторона удерживает контроль.
    # ========================================

    if (
        movement_power >= 60
        and power_difference >= 20
        and move_energy >= 58
        and market_health >= 58
    ):

        context["market_stage"] = "EXPANSION"

        context["stage_reason"] = (
            "Импульс развивается, "
            "доминирующая сторона сохраняет контроль"
        )

        return context

    # ========================================
    # 8. UNCERTAIN
    # ========================================

    context["market_stage"] = "UNCERTAIN"

    context["stage_reason"] = (
        "Рыночные показатели пока не формируют "
        "ясную стадию движения"
    )

    return context

# ============================================
# ENTRY RISK ENGINE
# Оценивает риск входа и готовность сделки
# ============================================

def calculate_entry_risk(signal, context):

    """
    Оценивает не направление рынка,
    а безопасность входа прямо сейчас.

    Возвращает в context:

    entry_score:
        0–100 — качество потенциальной точки входа.

    entry_risk:
        LOW
        MEDIUM
        HIGH
        EXTREME

    entry_ready:
        True — условия достаточно хорошие,
        чтобы искать вход после подтверждения.

    entry_trigger:
        Что именно нужно дождаться.

    entry_reason:
        Понятное объяснение решения.
    """

    move_type = str(
        context.get("move_type")
        or signal.get("type")
        or ""
    ).upper()

    window = str(
        context.get("window")
        or signal.get("window")
        or ""
    ).lower()

    change = abs(
        safe_float(
            context.get(
                "change",
                signal.get("change")
            ),
            0.0
        )
    )

    stage = str(
        context.get("market_stage")
        or "UNCERTAIN"
    ).upper()

    buyers_power = safe_int(
        context.get("buyers_power"),
        50
    )

    sellers_power = safe_int(
        context.get("sellers_power"),
        50
    )

    move_energy = safe_int(
        context.get("move_energy"),
        0
    )

    market_health = safe_int(
        context.get("market_health"),
        0
    )

    consensus = safe_int(
        context.get("consensus"),
        0
    )

    data_quality = safe_int(
        context.get("data_quality"),
        0
    )

    available_modules = safe_int(
        context.get("available_modules"),
        0
    )

    core_data_count = safe_int(
        context.get("core_data_count"),
        0
    )

    oi_change = context.get("oi_change")

    pressure = str(
        context.get("pressure")
        or ""
    ).upper()

    # ========================================
    # 1. СИЛА СТОРОНЫ ТЕКУЩЕГО ДВИЖЕНИЯ
    # ========================================

    if move_type == "PUMP":

        movement_power = buyers_power
        opposite_power = sellers_power

        continuation_direction = "LONG"
        reversal_direction = "SHORT"

    elif move_type == "DUMP":

        movement_power = sellers_power
        opposite_power = buyers_power

        continuation_direction = "SHORT"
        reversal_direction = "LONG"

    else:

        context["entry_score"] = 0
        context["entry_risk"] = "EXTREME"
        context["entry_ready"] = False
        context["entry_direction"] = "NONE"
        context["entry_trigger"] = (
            "Тип движения не определён"
        )
        context["entry_reason"] = (
            "Невозможно оценить риск без типа движения"
        )
        context["late_entry"] = True

        return context

    # ========================================
    # 2. БАЗОВАЯ ОЦЕНКА ВХОДА
    # ========================================

    entry_score = 50

    positive_factors = []
    risk_factors = []

    # Качество данных.
    if data_quality >= 80:

        entry_score += 12

        positive_factors.append(
            "Высокое качество данных"
        )

    elif data_quality >= 60:

        entry_score += 7

        positive_factors.append(
            "Данных достаточно для анализа"
        )

    elif data_quality < 40:

        entry_score -= 20

        risk_factors.append(
            "Недостаточно рыночных данных"
        )

    else:

        entry_score -= 8

        risk_factors.append(
            "Часть важных данных отсутствует"
        )

    # Согласие модулей.
    if consensus >= 85:

        entry_score += 12

        positive_factors.append(
            "Аналитические модули почти единогласны"
        )

    elif consensus >= 70:

        entry_score += 7

        positive_factors.append(
            "Большинство модулей подтверждает сценарий"
        )

    elif consensus < 55:

        entry_score -= 15

        risk_factors.append(
            "Модули дают противоречивые сигналы"
        )

    # Энергия движения.
    if move_energy >= 80:

        entry_score += 10

        positive_factors.append(
            "Энергия движения высокая"
        )

    elif move_energy >= 65:

        entry_score += 6

        positive_factors.append(
            "Движение сохраняет энергию"
        )

    elif move_energy < 45:

        entry_score -= 15

        risk_factors.append(
            "Движение теряет энергию"
        )

    # Здоровье движения.
    if market_health >= 75:

        entry_score += 10

        positive_factors.append(
            "Движение остаётся здоровым"
        )

    elif market_health >= 60:

        entry_score += 5

    elif market_health < 45:

        entry_score -= 15

        risk_factors.append(
            "Структура движения слабая"
        )

    # ========================================
    # 3. ШТРАФ ЗА ПОЗДНИЙ ВХОД
    # ========================================

    late_entry_limit = {
        "5m": 5.5,
        "10m": 6.5,
        "20m": 7.5,
        "30m": 8.5,
    }.get(
        window,
        7.0
    )

    very_late_limit = {
        "5m": 8.0,
        "10m": 9.0,
        "20m": 10.0,
        "30m": 12.0,
    }.get(
        window,
        10.0
    )

    late_entry = False

    if change >= very_late_limit:

        entry_score -= 35
        late_entry = True

        risk_factors.append(
            "Цена уже прошла слишком большое расстояние"
        )

    elif change >= late_entry_limit:

        entry_score -= 20
        late_entry = True

        risk_factors.append(
            "Вход по текущей цене уже поздний"
        )

    elif change <= 4.5:

        entry_score += 8

        positive_factors.append(
            "Цена ещё не слишком далеко от начала импульса"
        )

    # ========================================
    # 4. ВЛИЯНИЕ СТАДИИ
    # ========================================

    if stage == "START":

        entry_score += 18

        positive_factors.append(
            "Движение находится на ранней стадии"
        )

    elif stage == "EXPANSION":

        entry_score += 8

        positive_factors.append(
            "Импульс продолжает развиваться"
        )

    elif stage == "CLIMAX":

        entry_score -= 30
        late_entry = True

        risk_factors.append(
            "Движение находится в фазе перегрева"
        )

    elif stage == "WEAKENING":

        entry_score -= 18

        risk_factors.append(
            "Текущее движение начинает ослабевать"
        )

    elif stage == "REVERSAL":

        entry_score -= 10

        risk_factors.append(
            "Разворот ещё требует подтверждения цены"
        )

    else:

        entry_score -= 12

        risk_factors.append(
            "Стадия рынка пока не определена"
        )

    # ========================================
    # 5. КЛЮЧЕВЫЕ ДАННЫЕ
    # ========================================

    if core_data_count >= 3:

        entry_score += 10

        positive_factors.append(
            "OI, давление и Spot CVD доступны"
        )

    elif core_data_count == 2:

        entry_score += 4

    elif core_data_count == 1:

        entry_score -= 10

        risk_factors.append(
            "Доступен только один ключевой источник"
        )

    else:

        entry_score -= 25

        risk_factors.append(
            "Нет ключевых данных OI, давления и Spot CVD"
        )

    if oi_change is None:

        entry_score -= 10

        risk_factors.append(
            "Нет данных Open Interest"
        )

    # ========================================
    # 6. ПРОТИВОРЕЧИЕ ДАВЛЕНИЯ И ДВИЖЕНИЯ
    # ========================================

    pressure_against_move = False

    if move_type == "PUMP" and pressure in {
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
    }:

        pressure_against_move = True

    elif move_type == "DUMP" and pressure in {
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
    }:

        pressure_against_move = True

    if pressure_against_move:

        entry_score -= 15

        risk_factors.append(
            "Давление идёт против текущего движения"
        )

    # ========================================
    # 7. НОРМАЛИЗАЦИЯ SCORE
    # ========================================

    entry_score = max(
        0,
        min(100, round(entry_score))
    )

    # ========================================
    # 8. РИСК
    # ========================================

    if (
        entry_score >= 80
        and not late_entry
        and data_quality >= 60
        and core_data_count >= 2
    ):

        entry_risk = "LOW"

    elif (
        entry_score >= 65
        and not late_entry
    ):

        entry_risk = "MEDIUM"

    elif entry_score >= 40:

        entry_risk = "HIGH"

    else:

        entry_risk = "EXTREME"

    # ========================================
    # 9. НАПРАВЛЕНИЕ И ГОТОВНОСТЬ ВХОДА
    # ========================================

    entry_ready = False
    entry_direction = "NONE"

    if stage in {
        "START",
        "EXPANSION",
    }:

        entry_direction = continuation_direction

        if (
            entry_score >= 75
            and movement_power >= 65
            and consensus >= 65
            and data_quality >= 50
            and available_modules >= 3
            and not late_entry
        ):

            entry_ready = True

    elif stage in {
        "WEAKENING",
        "REVERSAL",
    }:

        entry_direction = reversal_direction

        # Разворотный вход всегда требует
        # более строгих условий.
        if (
            stage == "REVERSAL"
            and entry_score >= 65
            and opposite_power >= 68
            and consensus >= 65
            and available_modules >= 3
        ):

            entry_ready = True

    # ========================================
    # 10. ЧТО НУЖНО ДОЖДАТЬСЯ
    # ========================================

    if stage == "START":

        if entry_ready:

            entry_trigger = (
                "Ждать небольшой откат или ретест "
                "и подтверждение продолжения"
            )

        else:

            entry_trigger = (
                "Ждать усиления подтверждений "
                "и первого безопасного отката"
            )

    elif stage == "EXPANSION":

        entry_trigger = (
            "Не догонять цену. Ждать ретест "
            "или локальный откат"
        )

    elif stage == "CLIMAX":

        entry_trigger = (
            "Не входить по движению. "
            "Ждать охлаждение рынка"
        )

    elif stage == "WEAKENING":

        entry_trigger = (
            "Ждать слом локальной структуры "
            "и подтверждение разворота"
        )

    elif stage == "REVERSAL":

        entry_trigger = (
            "Ждать ретест уровня после "
            "подтверждённого разворота"
        )

    else:

        entry_trigger = (
            "Ждать более ясной структуры рынка"
        )

    # ========================================
    # 11. ОБЪЯСНЕНИЕ
    # ========================================

    if entry_ready:

        entry_reason = (
            "Условия достаточно сильные, "
            "чтобы искать подтверждённый вход"
        )

    elif late_entry:

        entry_reason = (
            "Направление может быть правильным, "
            "но текущая цена уже не даёт безопасного входа"
        )

    elif entry_risk == "EXTREME":

        entry_reason = (
            "Риск слишком высокий, сделку лучше пропустить"
        )

    elif entry_risk == "HIGH":

        entry_reason = (
            "Подтверждений недостаточно для качественного входа"
        )

    else:

        entry_reason = (
            "Сценарий интересный, "
            "но точка входа ещё не подтверждена"
        )

    # ========================================
    # 12. СОХРАНЯЕМ В CONTEXT
    # ========================================

    context["entry_score"] = entry_score
    context["entry_risk"] = entry_risk
    context["entry_ready"] = entry_ready
    context["entry_direction"] = entry_direction
    context["entry_trigger"] = entry_trigger
    context["entry_reason"] = entry_reason
    context["late_entry"] = late_entry

    context["entry_positive_factors"] = (
        positive_factors[:4]
    )

    context["entry_risk_factors"] = (
        risk_factors[:4]
    )

    return context

# ============================================
# DECISION MATRIX V7
# Единственное место,
# где хранится торговая стратегия
# ============================================

# ============================================================
# RULEBOOK V7
# Универсальная книга торговых правил
# ============================================================

RULEBOOK = {

    # ========================================================
    # РАННЯЯ СТАДИЯ ДВИЖЕНИЯ
    # ========================================================

    "START": [

        {
            "name": "START_ENTRY",

            "trade_state": "ENTRY",

            # Направление будет определено автоматически:
            # PUMP -> LONG
            # DUMP -> SHORT
            "direction_mode": "CONTINUATION",

            "priority": 100,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 80,
                },

                {
                    "field": "movement_power",
                    "op": ">=",
                    "value": 70,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 70,
                },

                {
                    "field": "market_health",
                    "op": ">=",
                    "value": 65,
                },

                {
                    "field": "data_quality",
                    "op": ">=",
                    "value": 55,
                },

                {
                    "field": "late_entry",
                    "op": "==",
                    "value": False,
                },

                {
                    "field": "entry_ready",
                    "op": "==",
                    "value": True,
                },

            ],

            "market_summary": (
                "Движение находится на ранней стадии "
                "и имеет сильную поддержку"
            ),

            "action_summary": (
                "Искать вход только после небольшого отката "
                "или подтверждённого ретеста"
            ),

            "next_move_summary": (
                "Текущее движение сохраняет потенциал продолжения"
            ),

        },

        {
            "name": "START_SETUP",

            "trade_state": "SETUP",

            "direction_mode": "CONTINUATION",

            "priority": 80,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 65,
                },

                {
                    "field": "movement_power",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "market_health",
                    "op": ">=",
                    "value": 55,
                },

                {
                    "field": "late_entry",
                    "op": "==",
                    "value": False,
                },

            ],

            "market_summary": (
                "Ранний импульс формируется, "
                "но точка входа ещё не подтверждена"
            ),

            "action_summary": (
                "Наблюдать и ждать безопасный откат или ретест"
            ),

            "next_move_summary": (
                "Сценарий интересный, но пока требует подтверждения"
            ),

        },

        {
            "name": "START_WATCH",

            "trade_state": "WATCH",

            "direction_mode": "NONE",

            "priority": 40,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Движение раннее, но подтверждений пока недостаточно"
            ),

            "action_summary": (
                "Не входить. Продолжать наблюдение"
            ),

            "next_move_summary": (
                "Нужно дождаться усиления рыночных данных"
            ),

        },

    ],

    # ========================================================
    # РАЗВИТИЕ ИМПУЛЬСА
    # ========================================================

    "EXPANSION": [

        {
            "name": "EXPANSION_ENTRY",

            "trade_state": "ENTRY",

            "direction_mode": "CONTINUATION",

            "priority": 100,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 78,
                },

                {
                    "field": "movement_power",
                    "op": ">=",
                    "value": 68,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 65,
                },

                {
                    "field": "market_health",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "late_entry",
                    "op": "==",
                    "value": False,
                },

                {
                    "field": "entry_ready",
                    "op": "==",
                    "value": True,
                },

            ],

            "market_summary": (
                "Импульс развивается и сохраняет поддержку"
            ),

            "action_summary": (
                "Искать вход только после локального отката "
                "или ретеста"
            ),

            "next_move_summary": (
                "Продолжение движения остаётся рабочим сценарием"
            ),

        },

        {
            "name": "EXPANSION_SETUP",

            "trade_state": "SETUP",

            "direction_mode": "CONTINUATION",

            "priority": 80,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "movement_power",
                    "op": ">=",
                    "value": 58,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 55,
                },

                {
                    "field": "market_health",
                    "op": ">=",
                    "value": 55,
                },

                {
                    "field": "late_entry",
                    "op": "==",
                    "value": False,
                },

            ],

            "market_summary": (
                "Направление сохраняется, "
                "но текущая точка входа неидеальна"
            ),

            "action_summary": (
                "Не догонять цену. Ждать откат или ретест"
            ),

            "next_move_summary": (
                "Сценарий продолжения остаётся под наблюдением"
            ),

        },

        {
            "name": "EXPANSION_WATCH",

            "trade_state": "WATCH",

            "direction_mode": "NONE",

            "priority": 40,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Импульс развивается, но вход сейчас рискованный"
            ),

            "action_summary": (
                "Не догонять движение"
            ),

            "next_move_summary": (
                "Ждать более выгодную точку"
            ),

        },

    ],

    # ========================================================
    # ПЕРЕГРЕВ
    # ========================================================

    "CLIMAX": [

        {
            "name": "CLIMAX_WATCH",

            "trade_state": "WATCH",

            "direction_mode": "NONE",

            "priority": 100,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Движение сильное, но цена уже перегрета"
            ),

            "action_summary": (
                "Не входить по направлению импульса. "
                "Ждать охлаждение или новый сетап"
            ),

            "next_move_summary": (
                "Вероятность резкого отката повышена"
            ),

        },

    ],

    # ========================================================
    # ОСЛАБЛЕНИЕ ТЕКУЩЕГО ДВИЖЕНИЯ
    # ========================================================

    "WEAKENING": [

        {
            "name": "WEAKENING_SETUP",

            "trade_state": "SETUP",

            # Для пампа — SHORT.
            # Для дампа — LONG.
            "direction_mode": "REVERSAL",

            "priority": 90,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "opposite_power",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "data_quality",
                    "op": ">=",
                    "value": 45,
                },

            ],

            "market_summary": (
                "Текущее движение теряет энергию"
            ),

            "action_summary": (
                "Готовить разворотный сценарий, "
                "но входить только после слома структуры"
            ),

            "next_move_summary": (
                "Разворот ещё не подтверждён окончательно"
            ),

        },

        {
            "name": "WEAKENING_WATCH",

            "trade_state": "WATCH",

            "direction_mode": "NONE",

            "priority": 40,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Движение ослабевает, "
                "но разворот пока не готов"
            ),

            "action_summary": (
                "Не входить. Ждать подтверждение смены направления"
            ),

            "next_move_summary": (
                "Рынок находится в переходной фазе"
            ),

        },

    ],

    # ========================================================
    # ПОДТВЕРЖДЁННЫЙ РАЗВОРОТ
    # ========================================================

    "REVERSAL": [

        {
            "name": "REVERSAL_ENTRY",

            "trade_state": "ENTRY",

            "direction_mode": "REVERSAL",

            "priority": 100,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 75,
                },

                {
                    "field": "opposite_power",
                    "op": ">=",
                    "value": 70,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 70,
                },

                {
                    "field": "market_health",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "entry_ready",
                    "op": "==",
                    "value": True,
                },

            ],

            "market_summary": (
                "Противоположная сторона получила "
                "подтверждённое преимущество"
            ),

            "action_summary": (
                "Искать вход после ретеста сломанного уровня"
            ),

            "next_move_summary": (
                "Разворотный сценарий подтверждён рыночными данными"
            ),

        },

        {
            "name": "REVERSAL_SETUP",

            "trade_state": "SETUP",

            "direction_mode": "REVERSAL",

            "priority": 80,

            "logic": "ALL",

            "conditions": [

                {
                    "field": "entry_score",
                    "op": ">=",
                    "value": 60,
                },

                {
                    "field": "opposite_power",
                    "op": ">=",
                    "value": 65,
                },

                {
                    "field": "consensus",
                    "op": ">=",
                    "value": 60,
                },

            ],

            "market_summary": (
                "Разворотный сценарий формируется"
            ),

            "action_summary": (
                "Ждать подтверждение цены и ретест уровня"
            ),

            "next_move_summary": (
                "Направление разворота пока требует подтверждения"
            ),

        },

        {
            "name": "REVERSAL_WATCH",

            "trade_state": "WATCH",

            "direction_mode": "NONE",

            "priority": 40,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Есть признаки разворота, "
                "но качественного входа пока нет"
            ),

            "action_summary": (
                "Продолжать наблюдение"
            ),

            "next_move_summary": (
                "Вход без подтверждения остаётся рискованным"
            ),

        },

    ],

    # ========================================================
    # НЕОПРЕДЕЛЁННЫЙ РЫНОК
    # ========================================================

    "UNCERTAIN": [

        {
            "name": "UNCERTAIN_IGNORE",

            "trade_state": "IGNORE",

            "direction_mode": "NONE",

            "priority": 100,

            "logic": "ALWAYS",

            "conditions": [],

            "market_summary": (
                "Рыночные данные не дают ясного сценария"
            ),

            "action_summary": (
                "Пропустить сигнал"
            ),

            "next_move_summary": (
                "Ждать новой структуры и более согласованных данных"
            ),

        },

    ],

}

# ============================================================
# UNIVERSAL RULE ENGINE V7
# Проверяет RULEBOOK и формирует TradeDecision
# ============================================================

def get_context_value(context, field, default=None):
    """
    Безопасно получает значение из context.

    Поддерживает обычные поля:
        entry_score

    И вложенные пути:
        metrics.entry_score
    """

    if not field:
        return default

    current = context

    for part in str(field).split("."):

        if not isinstance(current, dict):
            return default

        if part not in current:
            return default

        current = current[part]

    return current


def compare_rule_values(actual, operator, expected):
    """
    Универсальное сравнение значений.

    Поддерживает:
        ==
        !=
        >
        >=
        <
        <=
        IN
        NOT_IN
        BETWEEN
        EXISTS
        NOT_EXISTS
        CONTAINS
        NOT_CONTAINS
    """

    operator = str(
        operator
        or "=="
    ).upper()

    # ========================================
    # EXISTS / NOT_EXISTS
    # ========================================

    if operator == "EXISTS":

        return (
            actual is not None
            and actual != ""
        )

    if operator == "NOT_EXISTS":

        return (
            actual is None
            or actual == ""
        )

    # ========================================
    # IN / NOT_IN
    # ========================================

    if operator == "IN":

        if not isinstance(
            expected,
            (list, tuple, set)
        ):
            expected = [expected]

        return actual in expected

    if operator == "NOT_IN":

        if not isinstance(
            expected,
            (list, tuple, set)
        ):
            expected = [expected]

        return actual not in expected

    # ========================================
    # BETWEEN
    # ========================================

    if operator == "BETWEEN":

        if not isinstance(
            expected,
            (list, tuple)
        ):

            return False

        if len(expected) != 2:

            return False

        try:

            actual_number = float(actual)

            lower = float(expected[0])

            upper = float(expected[1])

            return (
                lower
                <= actual_number
                <= upper
            )

        except (TypeError, ValueError):

            return False

    # ========================================
    # CONTAINS / NOT_CONTAINS
    # ========================================

    if operator == "CONTAINS":

        try:
            return expected in actual

        except TypeError:
            return False

    if operator == "NOT_CONTAINS":

        try:
            return expected not in actual

        except TypeError:
            return True

    # ========================================
    # РАВЕНСТВО
    # ========================================

    if operator == "==":

        return actual == expected

    if operator == "!=":

        return actual != expected

    # ========================================
    # ЧИСЛОВЫЕ СРАВНЕНИЯ
    # ========================================

    try:

        actual_number = float(actual)

        expected_number = float(expected)

    except (TypeError, ValueError):

        return False

    if operator == ">":

        return (
            actual_number
            > expected_number
        )

    if operator == ">=":

        return (
            actual_number
            >= expected_number
        )

    if operator == "<":

        return (
            actual_number
            < expected_number
        )

    if operator == "<=":

        return (
            actual_number
            <= expected_number
        )

    return False


def check_rule_condition(context, condition):
    """
    Проверяет одно условие RULEBOOK.

    Пример:

    {
        "field": "entry_score",
        "op": ">=",
        "value": 80
    }
    """

    if not isinstance(condition, dict):

        return {
            "passed": False,
            "field": "",
            "actual": None,
            "operator": "",
            "expected": None,
            "error": "Condition is not dict",
        }

    field = condition.get("field")

    operator = condition.get(
        "op",
        condition.get(
            "operator",
            "=="
        )
    )

    expected = condition.get("value")

    actual = get_context_value(
        context,
        field,
        None
    )

    passed = compare_rule_values(
        actual,
        operator,
        expected
    )

    return {
        "passed": passed,
        "field": field,
        "actual": actual,
        "operator": operator,
        "expected": expected,
        "error": None,
    }


def validate_rule(context, rule):
    """
    Проверяет всё правило.

    Поддерживаемая логика:

        ALWAYS
        ALL
        ANY
        2_OF_3
        3_OF_5
        N_OF_M
    """

    if not isinstance(rule, dict):

        return {
            "passed": False,
            "logic": "INVALID",
            "passed_count": 0,
            "total_count": 0,
            "condition_results": [],
        }

    logic = str(
        rule.get("logic")
        or "ALL"
    ).upper()

    conditions = (
        rule.get("conditions")
        or []
    )

    # Правило-заглушка подходит всегда.
    if logic == "ALWAYS":

        return {
            "passed": True,
            "logic": logic,
            "passed_count": 0,
            "total_count": 0,
            "condition_results": [],
        }

    condition_results = [
        check_rule_condition(
            context,
            condition
        )
        for condition in conditions
    ]

    total_count = len(
        condition_results
    )

    passed_count = sum(
        1
        for result in condition_results
        if result["passed"]
    )

    # Пустое ALL-правило не должно
    # случайно давать положительное решение.
    if total_count == 0:

        return {
            "passed": False,
            "logic": logic,
            "passed_count": 0,
            "total_count": 0,
            "condition_results": [],
        }

    if logic == "ALL":

        passed = (
            passed_count
            == total_count
        )

    elif logic == "ANY":

        passed = (
            passed_count
            >= 1
        )

    elif "_OF_" in logic:

        try:

            required_text, total_text = (
                logic.split(
                    "_OF_",
                    1
                )
            )

            required_count = int(
                required_text
            )

            declared_total = int(
                total_text
            )

            # Если в названии написано 3_OF_5,
            # а условий фактически не 5,
            # используем реальное количество условий.
            if declared_total != total_count:

                declared_total = total_count

            passed = (
                passed_count
                >= required_count
            )

        except (
            TypeError,
            ValueError
        ):

            passed = False

    else:

        passed = False

    return {
        "passed": passed,
        "logic": logic,
        "passed_count": passed_count,
        "total_count": total_count,
        "condition_results": condition_results,
    }


def resolve_trade_direction(
    context,
    direction_mode
):
    """
    Определяет направление сделки.

    CONTINUATION:
        PUMP -> LONG
        DUMP -> SHORT

    REVERSAL:
        PUMP -> SHORT
        DUMP -> LONG

    LONG / SHORT:
        фиксированное направление

    NONE:
        направления нет
    """

    direction_mode = str(
        direction_mode
        or "NONE"
    ).upper()

    move_type = str(
        context.get("move_type")
        or ""
    ).upper()

    if direction_mode == "CONTINUATION":

        if move_type == "PUMP":
            return "LONG"

        if move_type == "DUMP":
            return "SHORT"

    elif direction_mode == "REVERSAL":

        if move_type == "PUMP":
            return "SHORT"

        if move_type == "DUMP":
            return "LONG"

    elif direction_mode in {
        "LONG",
        "SHORT",
    }:

        return direction_mode

    return "NONE"


def build_rule_explanation(
    context,
    validation
):
    """
    Строит понятные подтверждения и препятствия
    на основе реально проверенных условий.
    """

    confirmations = []

    obstacles = []

    for result in validation.get(
        "condition_results",
        []
    ):

        field = result.get("field")

        actual = result.get("actual")

        operator = result.get(
            "operator"
        )

        expected = result.get(
            "expected"
        )

        if result.get("passed"):

            confirmations.append(
                f"{field}: {actual} "
                f"{operator} {expected}"
            )

        else:

            obstacles.append(
                f"{field}: {actual}, "
                f"нужно {operator} {expected}"
            )

    # Добавляем понятные факторы Entry Engine.
    for text in context.get(
        "entry_positive_factors",
        []
    ):

        if (
            text
            and text not in confirmations
        ):

            confirmations.append(text)

    for text in context.get(
        "entry_risk_factors",
        []
    ):

        if (
            text
            and text not in obstacles
        ):

            obstacles.append(text)

    return (
        confirmations[:5],
        obstacles[:5]
    )


def create_default_decision(
    context,
    reason
):
    """
    Безопасное решение на случай,
    если стадия отсутствует в RULEBOOK
    или ни одно правило не подошло.
    """

    decision = TradeDecision()

    decision.trade_state = "IGNORE"

    decision.direction = "NONE"

    decision.stage = str(
        context.get("market_stage")
        or "UNCERTAIN"
    ).upper()

    decision.confidence = safe_int(
        context.get("entry_score"),
        0
    )

    decision.buyers_power = safe_int(
        context.get("buyers_power"),
        50
    )

    decision.sellers_power = safe_int(
        context.get("sellers_power"),
        50
    )

    decision.move_energy = safe_int(
        context.get("move_energy"),
        0
    )

    decision.quality_score = safe_int(
        context.get("market_health"),
        0
    )

    decision.consensus = safe_int(
        context.get("consensus"),
        0
    )

    decision.data_quality = safe_int(
        context.get("data_quality"),
        0
    )

    decision.market_health = safe_int(
        context.get("market_health"),
        0
    )

    decision.risk = str(
        context.get("entry_risk")
        or "EXTREME"
    ).upper()

    decision.can_trade = False

    decision.market_summary = reason

    decision.control_summary = (
        "Явного торгового преимущества нет"
    )

    decision.action_summary = (
        "Пропустить сигнал"
    )

    decision.next_move_summary = (
        "Ждать более ясной рыночной структуры"
    )

    decision.confirmations = []

    decision.obstacles = list(
        context.get(
            "entry_risk_factors",
            []
        )
    )[:5]

    decision.data_problems = []

    if not context.get("has_oi"):

        decision.data_problems.append(
            "Нет данных Open Interest"
        )

    if not context.get(
        "has_spot_cvd"
    ):

        decision.data_problems.append(
            "Нет данных Spot CVD"
        )

    if not context.get(
        "has_pressure"
    ):

        decision.data_problems.append(
            "Нет данных о рыночном давлении"
        )

    decision.votes = list(
        context.get(
            "votes",
            []
        )
    )

    decision.context = context

    result = decision.to_dict()

    result["matched_rule"] = None

    result["rule_validation"] = None

    return result


def apply_rule_to_decision(
    context,
    rule,
    validation
):
    """
    Создаёт TradeDecision из правила,
    которое успешно прошло проверку.
    """

    decision = TradeDecision()

    stage = str(
        context.get("market_stage")
        or "UNCERTAIN"
    ).upper()

    trade_state = str(
        rule.get("trade_state")
        or "IGNORE"
    ).upper()

    direction = resolve_trade_direction(
        context,
        rule.get("direction_mode")
    )

    confirmations, obstacles = (
        build_rule_explanation(
            context,
            validation
        )
    )

    decision.trade_state = trade_state

    decision.direction = direction

    decision.stage = stage

    decision.confidence = safe_int(
        context.get("entry_score"),
        0
    )

    decision.buyers_power = safe_int(
        context.get("buyers_power"),
        50
    )

    decision.sellers_power = safe_int(
        context.get("sellers_power"),
        50
    )

    decision.move_energy = safe_int(
        context.get("move_energy"),
        0
    )

    decision.quality_score = safe_int(
        context.get("market_health"),
        0
    )

    decision.consensus = safe_int(
        context.get("consensus"),
        0
    )

    decision.data_quality = safe_int(
        context.get("data_quality"),
        0
    )

    decision.market_health = safe_int(
        context.get("market_health"),
        0
    )

    decision.risk = str(
        context.get("entry_risk")
        or "HIGH"
    ).upper()

    decision.can_trade = (
        trade_state
        in {
            "ENTRY",
            "SETUP",
        }
    )

    decision.market_summary = str(
        rule.get("market_summary")
        or context.get("stage_reason")
        or "Состояние рынка не определено"
    )

    if direction == "LONG":

        decision.control_summary = (
            "Приоритет покупателей"
        )

    elif direction == "SHORT":

        decision.control_summary = (
            "Приоритет продавцов"
        )

    else:

        movement_power = safe_int(
            context.get("movement_power"),
            50
        )

        opposite_power = safe_int(
            context.get("opposite_power"),
            50
        )

        if movement_power > opposite_power:

            decision.control_summary = (
                "Сторона текущего движения "
                "сохраняет преимущество"
            )

        elif opposite_power > movement_power:

            decision.control_summary = (
                "Противоположная сторона усиливается"
            )

        else:

            decision.control_summary = (
                "Явного преимущества нет"
            )

    decision.action_summary = str(
        rule.get("action_summary")
        or context.get("entry_trigger")
        or "Ждать подтверждения"
    )

    decision.next_move_summary = str(
        rule.get("next_move_summary")
        or context.get("entry_reason")
        or "Сценарий пока не определён"
    )

    decision.confirmations = confirmations

    decision.obstacles = obstacles

    decision.data_problems = []

    if not context.get("has_oi"):

        decision.data_problems.append(
            "Нет данных Open Interest"
        )

    if not context.get(
        "has_spot_cvd"
    ):

        decision.data_problems.append(
            "Нет данных Spot CVD"
        )

    if not context.get(
        "has_pressure"
    ):

        decision.data_problems.append(
            "Нет данных о рыночном давлении"
        )

    decision.votes = list(
        context.get(
            "votes",
            []
        )
    )

    decision.context = context

    result = decision.to_dict()

    result["matched_rule"] = (
        rule.get("name")
    )

    result["rule_priority"] = safe_int(
        rule.get("priority"),
        0
    )

    result["rule_validation"] = validation

    return result


def make_trade_decision(context):
    """
    Главная функция универсального Rule Engine.

    1. Определяет текущую стадию.
    2. Берёт правила этой стадии из RULEBOOK.
    3. Сортирует правила по priority.
    4. Проверяет каждое правило.
    5. Возвращает первое подходящее решение.
    """

    if not isinstance(context, dict):

        return create_default_decision(
            {},
            "Chief получил некорректный контекст"
        )

    stage = str(
        context.get("market_stage")
        or "UNCERTAIN"
    ).upper()

    rules = RULEBOOK.get(stage)

    if not rules:

        rules = RULEBOOK.get(
            "UNCERTAIN",
            []
        )

    if not rules:

        return create_default_decision(
            context,
            (
                f"Для стадии {stage} "
                "не найдено торговых правил"
            )
        )

    sorted_rules = sorted(
        rules,
        key=lambda rule: safe_int(
            rule.get("priority"),
            0
        ),
        reverse=True
    )

    checked_rules = []

    for rule in sorted_rules:

        validation = validate_rule(
            context,
            rule
        )

        checked_rules.append({
            "name": rule.get("name"),
            "trade_state": rule.get(
                "trade_state"
            ),
            "priority": rule.get(
                "priority",
                0
            ),
            "passed": validation.get(
                "passed",
                False
            ),
            "passed_count": validation.get(
                "passed_count",
                0
            ),
            "total_count": validation.get(
                "total_count",
                0
            ),
        })

        if validation["passed"]:

            result = apply_rule_to_decision(
                context,
                rule,
                validation
            )

            result["checked_rules"] = (
            result["votes"] = context.get("votes", [])   
            )

            print(
                "[RULE_ENGINE_V7]",
                context.get("symbol"),
                f"stage={stage}",
                f"rule={rule.get('name')}",
                f"trade={result.get('trade_state')}",
                f"direction={result.get('direction')}",
                f"entry={context.get('entry_score')}",
                f"risk={context.get('entry_risk')}",
                f"energy={context.get('move_energy')}",
                f"health={context.get('market_health')}",
                f"consensus={context.get('consensus')}",
                flush=True
            )

            return result

    result = create_default_decision(
        context,
        (
            f"Ни одно правило стадии "
            f"{stage} не выполнилось"
        )
    )

    result["checked_rules"] = checked_rules
    result["votes"] = context.get("votes", [])

    print(
        "[RULE_ENGINE_V7_NO_MATCH]",
        context.get("symbol"),
        f"stage={stage}",
        checked_rules,
        flush=True
    )

    return result

 
    
# ============================================================
# CHIEF TRADER V7
# Главный мозг системы
# ============================================================

def chief_trader_v7(signal):

    """
    Полный цикл принятия решения.

    Никакой аналитики здесь нет.

    Только последовательный вызов модулей.
    """

    # ============================================
    # 1. Голоса экспертов
    # ============================================

    votes = collect_votes(signal)

    # ============================================
    # 2. Контекст рынка
    # ============================================

    context = build_market_context(
        signal,
        votes
    )

    # ============================================
    # 3. Здоровье рынка
    # ============================================

    context = calculate_market_health(
        signal,
        context
    )

    # ============================================
    # 4. Стадия движения
    # ============================================

    context = classify_market_stage(
        signal,
        context
    )

    # ============================================
    # 5. Риск входа
    # ============================================

    context = calculate_entry_risk(
        signal,
        context
    )

    # ============================================
    # DEBUG CONTEXT
    # ============================================
    
    print("\n")
    print("=" * 70)
    print("[CHIEF V7 CONTEXT]")
    print("=" * 70)
    
    for key in sorted(context.keys()):
    
        print(
            f"{key:<25} : {context[key]}"
        )
    
    print("=" * 70)
    print("\n")
    # ============================================
    # 6. Итоговое решение
    # ============================================

    decision = make_trade_decision(
        context
    )

    return decision


