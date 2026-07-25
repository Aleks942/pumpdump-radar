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
