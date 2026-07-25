Фfrom dataclasses import dataclass, field
from typing import Any

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

            print(

                "[VOTE ERROR]",

                name,

                signal.get("symbol"),

                e,

                flush=True

            )

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


