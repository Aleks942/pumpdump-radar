import math
from html import escape


PATTERN_NAMES = {
    "NEW_LONG_BUILDUP": "Признаки набора новых лонгов",
    "NEW_SHORT_BUILDUP": "Признаки набора новых шортов",
    "SHORT_SQUEEZE": "Признаки выноса шортов",
    "LONG_LIQUIDATION": "Признаки ликвидации лонгов",
    "NONE": "Нет полного подтверждения паттерна",
}


def number(value, signed=False, suffix=""):
    if value is None:
        return "нет данных"

    try:
        value = float(value)
    except (TypeError, ValueError):
        return "нет данных"

    if not math.isfinite(value):
        return "нет данных"

    text = f"{value:+.2f}" if signed else f"{value:.2f}"
    return text + suffix


def build_short_message(signal):
    decision = signal.get("decision") or {}
    spot = signal.get("spot_cvd") or {}
    futures = signal.get("futures_flow") or {}
    ready = futures.get("window_ready")
    futures_status = {
        True: "готово",
        False: "не готово",
    }.get(ready, "нет данных")
    liquidations = signal.get("liquidations") or {}

    pattern = decision.get("pattern") or "NONE"
    direction = decision.get("direction") or "NONE"

    direction_text = {
        "UP": "⬆️ Вверх",
        "DOWN": "⬇️ Вниз",
        "NONE": "⚪ Не определено",
    }.get(direction, "⚪ Не определено")

    symbol = escape(str(signal.get("symbol") or "UNKNOWN"))
    window = escape(str(signal.get("window") or "—"))
    pattern_text = escape(str(pattern))
    description = PATTERN_NAMES.get(
        pattern, "Неизвестный паттерн"
    )

    parts = [
        "📡 PumpDump Radar",
        "",
        f"🪙 {symbol} | окно движения: {window}",
        f"🧩 {pattern_text}",
        description,
        f"Направление: {direction_text}",
        "",
        "━━━━━━━━━━━━",
        "",
        "Изменение цены: "
        + number(signal.get("change"), signed=True, suffix="%"),
        "Изменение OI: "
        + number(signal.get("oi_change"), signed=True, suffix="%"),
        "Spot CVD: "
        + number(spot.get("cvd_percent"), signed=True, suffix="%"),
        "",
        "Ликвидации лонгов: "
        + number(liquidations.get("long_liq")),
        "Ликвидации шортов: "
        + number(liquidations.get("short_liq")),
        "",
        "━━━━━━━━━━━━",
        "",
        "Паттерн описывает наблюдаемое движение.",
        "Продолжение проверяем через 5 / 10 / 20 / 30 минут.",
    ]

    return "\n".join(parts)
