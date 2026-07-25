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


