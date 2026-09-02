from dataclasses import dataclass
import time


# ============================================
# STATE ENGINE V9
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
# СОСТОЯНИЯ РЫНКА
# ============================================

MARKET_BUILDING = MarketState(
    "MARKET_BUILDING",
    10,
    "Деньги начинают заходить"
)

ACCUMULATION = MarketState(
    "ACCUMULATION",
    20,
    "Рынок пока не сформировал подтвержденное движение"
)

PREMOVE = MarketState(
    "PREMOVE",
    30,
    "Импульс обнаружен, требуется подтверждение"
)

READY = MarketState(
    "READY",
    40,
    "Движение подтверждается повторно, формируется точка входа"
)

ENTRY = MarketState(
    "ENTRY",
    50,
    "Импульс подтвержден несколькими последовательными проверками",
    allow_entry=True
)

ACTIVE = MarketState(
    "ACTIVE",
    60,
    "Подтвержденное движение продолжается",
    allow_hold=True
)

EXPANSION = MarketState(
    "EXPANSION",
    70,
    "Импульс уже развивается",
    allow_hold=True
)

EXHAUSTION = MarketState(
    "EXHAUSTION",
    80,
    "Текущее движение теряет поддержку",
    allow_exit=True
)

EXIT = MarketState(
    "EXIT",
    90,
    "Движение сломано или подтвержден выход",
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


# ============================================
# ПАМЯТЬ ПО МОНЕТАМ
# ============================================

STATE_MEMORY = {}


# Максимальное время жизни старого состояния.
# Если монета долго не появлялась — начинаем анализ заново.
STATE_TTL_SEC = 15 * 60


# ============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================

def safe_float(value, default=0.0):

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def normalize_direction(move_type):

    move_type = str(
        move_type or ""
    ).upper()

    if move_type == "PUMP":
        return "LONG"

    if move_type == "DUMP":
        return "SHORT"

    return "NONE"


def get_direction_power(context):
    """
    Определяет силу движения и силу противоположной стороны.

    PUMP:
        движение = BUYERS
        против движения = SELLERS

    DUMP:
        движение = SELLERS
        против движения = BUYERS
    """

    move_type = str(
        context.get("move_type")
        or ""
    ).upper()

    buyers = safe_float(
        context.get("buyers_power"),
        50
    )

    sellers = safe_float(
        context.get("sellers_power"),
        50
    )

    if move_type == "PUMP":

        return {
            "direction": "LONG",
            "movement_power": buyers,
            "opposite_power": sellers,
        }

    if move_type == "DUMP":

        return {
            "direction": "SHORT",
            "movement_power": sellers,
            "opposite_power": buyers,
        }

    return {
        "direction": "NONE",
        "movement_power": 50,
        "opposite_power": 50,
    }


def spot_supports_direction(context, direction):
    """
    Проверяет, подтверждает ли Spot направление.
    """

    spot_state = str(
        context.get("spot_state")
        or ""
    ).upper()

    if not spot_state:
        return None

    if spot_state in {
        "HTTP_ERROR",
        "API_ERROR",
        "NO_TRADES",
        "NO_SPOT_SYMBOL",
        "EMPTY_VOLUME",
        "EXCEPTION",
    }:
        return None

    if direction == "LONG":

        if spot_state in {
            "SPOT_BUY",
            "STRONG_SPOT_BUY",
        }:
            return True

        if spot_state in {
            "SPOT_SELL",
            "STRONG_SPOT_SELL",
        }:
            return False

    if direction == "SHORT":

        if spot_state in {
            "SPOT_SELL",
            "STRONG_SPOT_SELL",
        }:
            return True

        if spot_state in {
            "SPOT_BUY",
            "STRONG_SPOT_BUY",
        }:
            return False

    return None


def pressure_supports_direction(context, direction):
    """
    Проверяет направление последних агрессивных сделок.
    """

    pressure = str(
        context.get("pressure")
        or ""
    ).upper()

    if direction == "LONG":

        if pressure in {
            "BUY_PRESSURE",
            "STRONG_BUY_PRESSURE",
        }:
            return True

        if pressure in {
            "SELL_PRESSURE",
            "STRONG_SELL_PRESSURE",
        }:
            return False

    if direction == "SHORT":

        if pressure in {
            "SELL_PRESSURE",
            "STRONG_SELL_PRESSURE",
        }:
            return True

        if pressure in {
            "BUY_PRESSURE",
            "STRONG_BUY_PRESSURE",
        }:
            return False

    return None


def oi_supports_direction(context):
    """
    Для продолжения PUMP/DUMP рост OI является подтверждением.

    Сильное падение OI означает, что движение может быть
    вызвано закрытием/ликвидацией уже существующих позиций.
    """

    oi = context.get("oi_change")

    if oi is None:
        return None

    oi = safe_float(
        oi,
        0
    )

    if oi >= 3:
        return True

    if oi <= -3:
        return False

    return None


def cleanup_old_states():
    """
    Удаляет слишком старые состояния.
    """

    now = time.time()

    expired = []

    for symbol, data in STATE_MEMORY.items():

        last_seen = data.get(
            "last_seen",
            0
        )

        if now - last_seen > STATE_TTL_SEC:
            expired.append(symbol)

    for symbol in expired:
        STATE_MEMORY.pop(
            symbol,
            None
        )


# ============================================
# STATE ENGINE
# ============================================

def detect_market_state(context):
    """
    STATE ENGINE V9.

    Главная идея:

        первый сильный snapshot
            ->
        PREMOVE

        повторное подтверждение
            ->
        READY

        еще одно подтверждение
            ->
        ENTRY

    То есть один сильный импульс больше
    не может сам по себе создать ENTRY.
    """

    cleanup_old_states()

    symbol = str(
        context.get("symbol")
        or "UNKNOWN"
    ).upper()

    move_type = str(
        context.get("move_type")
        or ""
    ).upper()

    direction = normalize_direction(
        move_type
    )

    # ============================================
    # НЕТ НАПРАВЛЕНИЯ
    # ============================================

    if direction == "NONE":

        STATE_MEMORY.pop(
            symbol,
            None
        )

        return ACCUMULATION

    # ============================================
    # ОСНОВНЫЕ ПОКАЗАТЕЛИ
    # ============================================

    energy = safe_float(
        context.get("move_energy"),
        0
    )

    health = safe_float(
        context.get("market_health"),
        0
    )

    consensus = safe_float(
        context.get("consensus"),
        0
    )

    data_quality = safe_float(
        context.get("data_quality"),
        0
    )

    stage = str(
        context.get("market_stage")
        or "UNCERTAIN"
    ).upper()

    power = get_direction_power(
        context
    )

    movement_power = power[
        "movement_power"
    ]

    opposite_power = power[
        "opposite_power"
    ]

    spot_confirmation = (
        spot_supports_direction(
            context,
            direction
        )
    )

    pressure_confirmation = (
        pressure_supports_direction(
            context,
            direction
        )
    )

    oi_confirmation = (
        oi_supports_direction(
            context
        )
    )

    # ============================================
    # ПОДТВЕРЖДЕНИЯ
    # ============================================

    confirmation_score = 0

    contradiction_score = 0

    # --------------------------------------------
    # Сила основной стороны
    # --------------------------------------------

    if movement_power >= 75:
        confirmation_score += 2

    elif movement_power >= 65:
        confirmation_score += 1

    # --------------------------------------------
    # Spot
    # --------------------------------------------

    if spot_confirmation is True:
        confirmation_score += 2

    elif spot_confirmation is False:
        contradiction_score += 2

    # --------------------------------------------
    # Pressure
    # --------------------------------------------

    if pressure_confirmation is True:
        confirmation_score += 2

    elif pressure_confirmation is False:
        contradiction_score += 2

    # --------------------------------------------
    # Open Interest
    # --------------------------------------------

    if oi_confirmation is True:
        confirmation_score += 2

    elif oi_confirmation is False:
        contradiction_score += 2

    # --------------------------------------------
    # Consensus
    # --------------------------------------------

    if consensus >= 75:
        confirmation_score += 2

    elif consensus >= 60:
        confirmation_score += 1

    # --------------------------------------------
    # Market Health
    # --------------------------------------------

    if health >= 70:
        confirmation_score += 2

    elif health >= 58:
        confirmation_score += 1

    # --------------------------------------------
    # Energy
    # --------------------------------------------

    if energy >= 70:
        confirmation_score += 2

    elif energy >= 58:
        confirmation_score += 1

    # ============================================
    # ЯВНАЯ ПРОТИВОПОЛОЖНАЯ СИЛА
    # ============================================

    if opposite_power >= 75:

        contradiction_score += 3

    elif opposite_power >= 65:

        contradiction_score += 1

    # ============================================
    # OI ПАДАЕТ
    # ============================================

    oi_change = context.get(
        "oi_change"
    )

    if oi_change is not None:

        oi_change = safe_float(
            oi_change,
            0
        )

        if oi_change <= -5:
            contradiction_score += 2

    print(
        "[STATE_DEBUG]",
        symbol,
        "dir=", direction,
        "confirm_score=", confirmation_score,
        "contradictions=", contradiction_score,
        "power=", round(movement_power, 1),
        "consensus=", round(consensus, 1),
        "health=", round(health, 1),
        "energy=", round(energy, 1),
        "spot=", spot_confirmation,
        "pressure=", pressure_confirmation,
        "oi=", oi_confirmation,
        flush=True,
    )

    # ============================================
    # ПОЛУЧАЕМ ПРЕДЫДУЩЕЕ СОСТОЯНИЕ
    # ============================================

    previous = STATE_MEMORY.get(
        symbol
    )

    now = time.time()

    # ============================================
    # ЕСЛИ НАПРАВЛЕНИЕ СМЕНИЛОСЬ
    # ============================================

    if previous:

        previous_direction = previous.get(
            "direction"
        )

        if (
            previous_direction
            and previous_direction != direction
        ):

            STATE_MEMORY[symbol] = {
                "direction": direction,
                "confirmations": 0,
                "failures": 1,
                "last_seen": now,
                "state": "ACCUMULATION",
            }

            return EXHAUSTION

    # ============================================
    # СОЗДАЕМ ПАМЯТЬ ДЛЯ НОВОЙ МОНЕТЫ
    # ============================================

    if not previous:

        previous = {
            "direction": direction,
            "confirmations": 0,
            "failures": 0,
            "last_seen": now,
            "state": "ACCUMULATION",
        }

    confirmations = int(
        previous.get(
            "confirmations",
            0
        )
    )

    failures = int(
        previous.get(
            "failures",
            0
        )
    )

    # ============================================
    # СИЛЬНОЕ ПРОТИВОРЕЧИЕ
    # ============================================

    if contradiction_score >= 4:

        failures += 1

        confirmations = max(
            0,
            confirmations - 2
        )

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": failures,
            "last_seen": now,
            "state": "EXHAUSTION",
        }

        return EXHAUSTION

    # ============================================
    # СЛАБОЕ / НЕКАЧЕСТВЕННОЕ ДВИЖЕНИЕ
    # ============================================

    if (
        confirmation_score < 4
        or movement_power < 58
        or consensus < 55
    ):

        confirmations = max(
            0,
            confirmations - 1
        )

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": failures,
            "last_seen": now,
            "state": "ACCUMULATION",
        }

        return ACCUMULATION

    # ============================================
    # CLIMAX / ПОЗДНЕЕ ДВИЖЕНИЕ
    # ============================================

    if stage == "CLIMAX":

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": failures,
            "last_seen": now,
            "state": "EXPANSION",
        }

        return EXPANSION

    # ============================================
    # ПЕРВОЕ ПОДТВЕРЖДЕНИЕ
    # ============================================

    if confirmations <= 0:

        confirmations = 1

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": 0,
            "last_seen": now,
            "state": "PREMOVE",
        }

        return PREMOVE

    # ============================================
    # ВТОРОЕ ПОДТВЕРЖДЕНИЕ
    # ============================================

    if confirmations == 1:

        if (
            confirmation_score >= 6
            and contradiction_score <= 1
            and movement_power >= 62
        ):

            confirmations = 2

            STATE_MEMORY[symbol] = {
                "direction": direction,
                "confirmations": confirmations,
                "failures": 0,
                "last_seen": now,
                "state": "READY",
            }

            return READY

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": failures,
            "last_seen": now,
            "state": "PREMOVE",
        }

        return PREMOVE

    # ============================================
    # ТРЕТЬЕ ПОДТВЕРЖДЕНИЕ -> ENTRY
    # ============================================

    if confirmations == 2:

        if (
            confirmation_score >= 7
            and contradiction_score == 0
            and movement_power >= 65
            and consensus >= 65
            and health >= 58
        ):

            confirmations = 3

            STATE_MEMORY[symbol] = {
                "direction": direction,
                "confirmations": confirmations,
                "failures": 0,
                "last_seen": now,
                "state": "ENTRY",
            }

            return ENTRY

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": failures,
            "last_seen": now,
            "state": "READY",
        }

        return READY

    # ============================================
    # УЖЕ БЫЛ ENTRY
    # ============================================

    if confirmations >= 3:

        if contradiction_score >= 3:

            STATE_MEMORY[symbol] = {
                "direction": direction,
                "confirmations": 0,
                "failures": failures + 1,
                "last_seen": now,
                "state": "EXHAUSTION",
            }

            return EXHAUSTION

        if (
            stage == "EXPANSION"
            or energy >= 75
        ):

            STATE_MEMORY[symbol] = {
                "direction": direction,
                "confirmations": confirmations,
                "failures": 0,
                "last_seen": now,
                "state": "EXPANSION",
            }

            return EXPANSION

        STATE_MEMORY[symbol] = {
            "direction": direction,
            "confirmations": confirmations,
            "failures": 0,
            "last_seen": now,
            "state": "ACTIVE",
        }

        return ACTIVE

    # ============================================
    # FALLBACK
    # ============================================

    return ACCUMULATION
