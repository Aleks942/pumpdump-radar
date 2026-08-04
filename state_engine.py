from dataclasses import dataclass


# ============================================
# STATE ENGINE V8
# ============================================

@dataclass
class MarketState:

    name: str

    priority: int

    description: str

    allow_entry: bool = False

    allow_hold: bool = False

    allow_exit: bool = False


# ============================================
# Все возможные стадии рынка
# ============================================

MARKET_BUILDING = MarketState(
    "MARKET_BUILDING",
    10,
    "Деньги начинают заходить"
)

ACCUMULATION = MarketState(
    "ACCUMULATION",
    20,
    "Идет накопление позиции"
)

PREMOVE = MarketState(
    "PREMOVE",
    30,
    "Подготовка к импульсу"
)

READY = MarketState(
    "READY",
    40,
    "Практически готов ко входу",
    allow_entry=True
)

ENTRY = MarketState(
    "ENTRY",
    50,
    "Оптимальная точка входа",
    allow_entry=True
)

ACTIVE = MarketState(
    "ACTIVE",
    60,
    "Сделка активна",
    allow_hold=True
)

EXPANSION = MarketState(
    "EXPANSION",
    70,
    "Импульс развивается",
    allow_hold=True
)

EXHAUSTION = MarketState(
    "EXHAUSTION",
    80,
    "Импульс заканчивается",
    allow_exit=True
)

EXIT = MarketState(
    "EXIT",
    90,
    "Закрытие сделки",
    allow_exit=True
)


STATE_MAP = {

    MARKET_BUILDING.name: MARKET_BUILDING,

    ACCUMULATION.name: ACCUMULATION,

    PREMOVE.name: PREMOVE,

    READY.name: READY,

    ENTRY.name: ENTRY,

    ACTIVE.name: ACTIVE,

    EXPANSION.name: EXPANSION,

    EXHAUSTION.name: EXHAUSTION,

    EXIT.name: EXIT,

}

def detect_market_state(context):
    """
    Определяет новое состояние рынка.
    """

    energy = context.get("move_energy", 0)
    health = context.get("market_health", 0)
    entry = context.get("entry_score", 0)

    continue_power = context.get("buyers_power", 50)
    exhaustion_power = context.get("sellers_power", 50)

    if entry >= 95 and energy >= 80:
        return ENTRY

    if entry >= 85:
        return READY

    if energy >= 85 and health >= 80:
        return EXPANSION

    if exhaustion_power >= 75:
        return EXHAUSTION

    if continue_power >= 70:
        return ACTIVE

    if energy >= 60:
        return PREMOVE

    return ACCUMULATION
