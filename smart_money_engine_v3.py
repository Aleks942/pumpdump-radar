# smart_money_engine_v3.py
# PumpDump Radar V3
#
# Простая интерпретация Smart Money.
#
# Задача:
# получить MarketSnapshot и определить:
#
# - кто доминирует на SPOT
# - кто доминирует на FUTURES
# - растёт / падает OI
# - как реагирует цена
# - есть ли готовый рыночный паттерн
#
# Здесь НЕТ:
# - score
# - процентов "BUY 100%"
# - Chief
# - Telegram
# - автоматической сделки


# ============================================================
# INITIAL THRESHOLDS
#
# Это стартовые значения.
# Позже будем проверять их на реальных сигналах.
# ============================================================

FLOW_BUY_THRESHOLD = 15.0
FLOW_SELL_THRESHOLD = -15.0

STRONG_FLOW_BUY = 35.0
STRONG_FLOW_SELL = -35.0

OI_RISING_THRESHOLD = 0.10
OI_FALLING_THRESHOLD = -0.10

# Небольшая нейтральная зона цены.
# Чтобы +0.001% не назывался полноценным ростом.
PRICE_FLAT_THRESHOLD = 0.03


# ============================================================
# HELPERS
# ============================================================

def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_flow_imbalance(flow_row):
    """
    Возвращает реальный imbalance потока в %.

    Например:
        +46.7 = покупатели доминируют
        -38.2 = продавцы доминируют

    None = данных ещё нет / окно не готово
    """

    if not flow_row:
        return None

    if not flow_row.get("window_ready"):
        return None

    return _safe_float(
        flow_row.get("imbalance_pct")
    )

def classify_flow(flow_row):
    """
    Классифицирует Spot/Futures flow.

    BUYING
    SELLING
    NEUTRAL
    NOT_READY
    """

    if not flow_row:
        return "NOT_READY"

    if not flow_row.get("window_ready"):
        return "NOT_READY"

    imbalance = _safe_float(
        flow_row.get("imbalance_pct")
    )

    if imbalance is None:
        return "NOT_READY"

    if imbalance >= FLOW_BUY_THRESHOLD:
        return "BUYING"

    if imbalance <= FLOW_SELL_THRESHOLD:
        return "SELLING"

    return "NEUTRAL"


def classify_oi(oi_row):
    """
    OI:

    RISING
    FALLING
    FLAT
    NOT_READY
    """

    if not oi_row:
        return "NOT_READY"

    change = _safe_float(
        oi_row.get("change_pct")
    )

    if change is None:
        return "NOT_READY"

    if change >= OI_RISING_THRESHOLD:
        return "RISING"

    if change <= OI_FALLING_THRESHOLD:
        return "FALLING"

    return "FLAT"

def get_oi_change_value(oi_row):
    """
    Возвращает реальное изменение OI в %.
    """

    if not oi_row:
        return None

    return _safe_float(
        oi_row.get("change_pct")
    )


def classify_price(price_change):
    """
    Направление цены:

    UP
    DOWN
    FLAT
    NOT_READY
    """

    value = _safe_float(
        price_change
    )

    if value is None:
        return "NOT_READY"

    if value >= PRICE_FLAT_THRESHOLD:
        return "UP"

    if value <= -PRICE_FLAT_THRESHOLD:
        return "DOWN"

    return "FLAT"


def flow_is_opposite(
    flow_1m,
    required_side,
):
    """
    Проверяем, не развернулась ли
    последняя минута ПРОТИВ сигнала.

    required_side:
        BUYING
        SELLING
    """

    if flow_1m == "NOT_READY":
        return True

    if required_side == "BUYING":
        return flow_1m == "SELLING"

    if required_side == "SELLING":
        return flow_1m == "BUYING"

    return False


# ============================================================
# SMART MONEY INTERPRETER
# ============================================================

def analyze_smart_money(snapshot):
    """
    Главная функция V3.

    Возвращает простое описание рынка.

    ВАЖНО:
    Эта функция ещё НЕ принимает окончательное
    торговое решение LONG / SHORT.

    Structure Engine будет вторым подтверждением.
    """

    if not snapshot:
        return {
            "ready": False,
            "pattern": "WAIT",
            "reason": "NO_SNAPSHOT",
        }

    symbol = snapshot.get(
        "symbol",
        "UNKNOWN",
    )

    # --------------------------------------------------------
    # DATA
    # --------------------------------------------------------

    price_change = (
        snapshot.get("price_change")
        or {}
    )

    oi_change = (
        snapshot.get("oi_change")
        or {}
    )

    spot_flow = (
        snapshot.get("spot_flow")
        or {}
    )

    futures_flow = (
        snapshot.get("futures_flow")
        or {}
    )

    # --------------------------------------------------------
    # 5m = основной рынок
    # --------------------------------------------------------

    price_5m_raw = price_change.get("5m")

    spot_5m_row = spot_flow.get("5m")
    futures_5m_row = futures_flow.get("5m")
    oi_5m_row = oi_change.get("5m")

    price_5m = classify_price(
        price_5m_raw
    )

    spot_5m = classify_flow(
        spot_5m_row
    )

    futures_5m = classify_flow(
        futures_5m_row
    )

    oi_5m = classify_oi(
        oi_5m_row
    )

    # --------------------------------------------------------
    # RAW VALUES
    # Сохраняем реальные значения, а не только состояния.
    # Они понадобятся для absorption / weakening / acceleration.
    # --------------------------------------------------------
    
    spot_5m_imbalance = get_flow_imbalance(
        spot_5m_row
    )
    
    futures_5m_imbalance = get_flow_imbalance(
        futures_5m_row
    )
    
    oi_5m_change = get_oi_change_value(
        oi_5m_row
    )
    
    price_5m_change = _safe_float(
        price_5m_raw
    )

    # --------------------------------------------------------
    # 1m = что происходит прямо сейчас
    # --------------------------------------------------------

    spot_1m = classify_flow(
        spot_flow.get("1m")
    )

    futures_1m = classify_flow(
        futures_flow.get("1m")
    )

    price_1m = classify_price(
        price_change.get("1m")
    )

    spot_1m_imbalance = get_flow_imbalance(
        spot_flow.get("1m")
    )
    
    futures_1m_imbalance = get_flow_imbalance(
        futures_flow.get("1m")
    )
    
    price_1m_change = _safe_float(
        price_change.get("1m")
    )


    raw_values = {
        "price_5m_change": price_5m_change,
        "price_1m_change": price_1m_change,
    
        "spot_5m_imbalance": spot_5m_imbalance,
        "spot_1m_imbalance": spot_1m_imbalance,
    
        "futures_5m_imbalance": futures_5m_imbalance,
        "futures_1m_imbalance": futures_1m_imbalance,
    
        "oi_5m_change": oi_5m_change,
    }
    def build_result(
        ready,
        pattern,
        dominant_side,
        long_forbidden,
        short_forbidden,
        reasons,
    ):
        """
        Единый формат результата Smart Money V3.
        Все паттерны получают одинаковый набор данных.
        """
    
        return {
            "ready": ready,
            "symbol": symbol,
    
            "pattern": pattern,
            "dominant_side": dominant_side,
    
            # Состояния рынка
            "price_5m": price_5m,
            "price_1m": price_1m,
    
            "spot_5m": spot_5m,
            "spot_1m": spot_1m,
    
            "futures_5m": futures_5m,
            "futures_1m": futures_1m,
    
            "oi_5m": oi_5m,
    
            # Реальные значения
            "raw": raw_values,
    
            # Защита
            "long_forbidden": long_forbidden,
            "short_forbidden": short_forbidden,
    
            "reasons": reasons,
        }
    
    # --------------------------------------------------------
    # READY CHECK
    # --------------------------------------------------------

    critical_states = (
        price_5m,
        spot_5m,
        futures_5m,
        oi_5m,
        spot_1m,
        futures_1m,
    )

    if "NOT_READY" in critical_states:
        return build_result(
            ready=False,
            pattern="WAIT",
            dominant_side="NONE",
            long_forbidden=True,
            short_forbidden=True,
            reasons=[
                "5m market data not fully ready"
            ],
        )

    reasons = []

    long_forbidden = False
    short_forbidden = False

    dominant_side = "NONE"
    pattern = "WAIT"

   
    # ========================================================
    # SIMPLE V3 PATTERN MATCHER
    #
    # Логика:
    # 1. BLOCK LONG
    # 2. BLOCK SHORT
    # 3. LONG
    # 4. SHORT
    # 5. WAIT
    # ========================================================


    # --------------------------------------------------------
    # 1. BLOCK LONG
    #
    # Покупатели давят сильно,
    # но цена НЕ растёт.
    # Значит LONG опасен.
    # --------------------------------------------------------

    block_long = (
        spot_5m_imbalance is not None
        and futures_5m_imbalance is not None
        and (
            spot_5m_imbalance >= STRONG_FLOW_BUY
            or futures_5m_imbalance >= STRONG_FLOW_BUY
        )
        and price_5m in ("FLAT", "DOWN")
    )

    if block_long:
        reasons.append(
            "Strong buying exists, but price does not rise"
        )

        return build_result(
            ready=True,
            pattern="BLOCK_LONG",
            dominant_side="BUYER",
            long_forbidden=True,
            short_forbidden=False,
            reasons=reasons,
        )


    # --------------------------------------------------------
    # 2. BLOCK SHORT
    #
    # Продавцы давят сильно,
    # но цена НЕ падает.
    # Значит SHORT опасен.
    # --------------------------------------------------------

    block_short = (
        spot_5m_imbalance is not None
        and futures_5m_imbalance is not None
        and (
            spot_5m_imbalance <= STRONG_FLOW_SELL
            or futures_5m_imbalance <= STRONG_FLOW_SELL
        )
        and price_5m in ("FLAT", "UP")
    )

    if block_short:
        reasons.append(
            "Strong selling exists, but price does not fall"
        )

        return build_result(
            ready=True,
            pattern="BLOCK_SHORT",
            dominant_side="SELLER",
            long_forbidden=False,
            short_forbidden=True,
            reasons=reasons,
        )


    # --------------------------------------------------------
    # 3. LONG PATTERN
    #
    # Цена растёт
    # Spot сильно покупает
    # Futures сильно покупает
    # OI растёт
    # 1m не развернулся против LONG
    # --------------------------------------------------------

    long_pattern = (
        price_5m == "UP"
        and spot_5m_imbalance is not None
        and futures_5m_imbalance is not None
        and spot_5m_imbalance >= STRONG_FLOW_BUY
        and futures_5m_imbalance >= STRONG_FLOW_BUY
        and oi_5m == "RISING"
        and not flow_is_opposite(
            spot_1m,
            "BUYING",
        )
        and not flow_is_opposite(
            futures_1m,
            "BUYING",
        )
    )

    if long_pattern:
        reasons.extend([
            "Price rising",
            "Strong Spot buying",
            "Strong Futures buying",
            "Open Interest rising",
            "1m flow not reversed against LONG",
        ])

        return build_result(
            ready=True,
            pattern="LONG",
            dominant_side="BUYER",
            long_forbidden=False,
            short_forbidden=True,
            reasons=reasons,
        )


    # --------------------------------------------------------
    # 4. SHORT PATTERN
    #
    # Цена падает
    # Spot сильно продаёт
    # Futures сильно продаёт
    # OI растёт
    # 1m не развернулся против SHORT
    # --------------------------------------------------------

    short_pattern = (
        price_5m == "DOWN"
        and spot_5m_imbalance is not None
        and futures_5m_imbalance is not None
        and spot_5m_imbalance <= STRONG_FLOW_SELL
        and futures_5m_imbalance <= STRONG_FLOW_SELL
        and oi_5m == "RISING"
        and not flow_is_opposite(
            spot_1m,
            "SELLING",
        )
        and not flow_is_opposite(
            futures_1m,
            "SELLING",
        )
    )

    if short_pattern:
        reasons.extend([
            "Price falling",
            "Strong Spot selling",
            "Strong Futures selling",
            "Open Interest rising",
            "1m flow not reversed against SHORT",
        ])

        return build_result(
            ready=True,
            pattern="SHORT",
            dominant_side="SELLER",
            long_forbidden=True,
            short_forbidden=False,
            reasons=reasons,
        )


    # --------------------------------------------------------
    # 5. WAIT
    # --------------------------------------------------------

    reasons.append(
        "No complete V3 pattern"
    )

    return build_result(
        ready=True,
        pattern="WAIT",
        dominant_side="NONE",
        long_forbidden=False,
        short_forbidden=False,
        reasons=reasons,
    )
