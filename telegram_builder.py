import math
from html import escape
from aggregated_oi import format_aggregated_oi


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
    liquidations = signal.get("liquidations") or {}

    ready = futures.get("window_ready")

    futures_status = {
        True: "готово",
        False: "не готово",
    }.get(ready, "нет данных")

    liq_available = liquidations.get("available", True)

    liq_note = (
        "Ликвидации — 5м, доступная неполная выборка (USDT):"
        if liq_available
        else "Ликвидации — нет свежих доступных данных"
    )

    pattern = decision.get("pattern") or "NONE"
    direction = decision.get("direction") or "NONE"

    direction_text = {
        "UP": "⬆️ Вверх",
        "DOWN": "⬇️ Вниз",
        "NONE": "⚪ Не определено",
    }.get(direction, "⚪ Не определено")

    alert_side = {
        "UP": "UP",
        "DOWN": "DOWN",
        "NONE": "SIGNAL",
    }.get(direction, "SIGNAL")

    alert_icon = {
        "UP": "🟢",
        "DOWN": "🔴",
        "NONE": "🚨",
    }.get(direction, "🚨")

    symbol = escape(
        str(signal.get("symbol") or "UNKNOWN")
    )

    window = escape(
        str(signal.get("window") or "—")
    )

    pattern_text = escape(str(pattern))

    description = PATTERN_NAMES.get(
        pattern,
        "Неизвестный паттерн",
    )

    parts = [
        f"🚨🚨🚨 PUMPDUMP SIGNAL — {alert_side}",
        f"{alert_icon} ВНИМАНИЕ: ПОДТВЕРЖДЁННЫЙ СИГНАЛ",
        f"🪙 {symbol}",
        "✅ Полный паттерн прошёл фильтры",
        "",
        f"Окно движения: {window}",
        f"🧩 {pattern_text}",
        description,
        f"Направление: {direction_text}",
        "",
        "━━━━━━━━━━━━",
        "",
        "Изменение цены: "
        + number(
            signal.get("change"),
            signed=True,
            suffix="%",
        ),
        "OI 5м (основной расчёт): "
        + number(
            signal.get("oi_change"),
            signed=True,
            suffix="%",
        ),
        "",
        "📊 " + format_aggregated_oi(signal.get("aggregated_oi")),
        "",
        f"Фьючерсы — окно 5м: {futures_status}",
        "Дельта объёма: "
        + number(
            futures.get("delta_quote"),
            signed=True,
        ),
        "Дисбаланс покупок/продаж: "
        + number(
            futures.get("imbalance_pct"),
            signed=True,
            suffix="%",
        ),
        "",
        "Spot CVD: "
        + number(
            spot.get("cvd_percent"),
            signed=True,
            suffix="%",
        ),
        "",
        liq_note,
        "Ликвидации лонгов: "
        + number(
            liquidations.get("long_liq")
            if liq_available
            else None
        ),
        "Ликвидации шортов: "
        + number(
            liquidations.get("short_liq")
            if liq_available
            else None
        ),
        "",
        "Источники: "
        + escape(
            str(
                liquidations.get(
                    "exchanges",
                    "нет данных",
                )
            )
        ),
        "━━━━━━━━━━━━",
        "",
        "Паттерн описывает наблюдаемое движение.",
        "Продолжение проверяем через 5 / 10 / 20 / 30 минут.",
    ]

    message = "\n".join(parts)

    aggregated = signal.get("aggregated_oi") or {}
    windows = aggregated.get("windows") or {}
    oi_5m = windows.get("5m") or {}
    oi_30m = windows.get("30m") or {}

    print(
        "[TG_OI_BLOCK]",
        signal.get("symbol"),
        "agg_5m_ready=", oi_5m.get("ready"),
        "agg_5m_pct=", oi_5m.get("change_pct"),
        "agg_30m_ready=", oi_30m.get("ready"),
        "agg_30m_pct=", oi_30m.get("change_pct"),
        flush=True,
    )

    return message


def build_market_watch_message(signal, reason):
    """Inform about a real observed PUMP/DUMP; never imply an approved entry."""
    decision = signal.get("decision") or {}
    direction = decision.get("direction")
    move_type = signal.get("type")
    if (move_type, direction) == ("PUMP", "UP"):
        heading = "🚀 ПАМП — НАБЛЮДЕНИЕ"
    elif (move_type, direction) == ("DUMP", "DOWN"):
        heading = "🔻 ДАМП — НАБЛЮДЕНИЕ"
    else:
        raise ValueError("Inconsistent pump/dump direction")

    def price_text(value):
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            return "нет данных"
        if not math.isfinite(value) or value <= 0:
            return "нет данных"
        return f"{value:.8f}".rstrip("0").rstrip(".")

    if reason == "PRICE_CONTINUATION":
        symbol = escape(str(signal.get("symbol") or "UNKNOWN"))
        elapsed = float(signal.get("confirmation_seconds"))
        follow = float(signal.get("followthrough_pct"))
        trigger = float(signal.get("trigger_change"))
        if (not all(math.isfinite(x) for x in (elapsed, follow, trigger))
                or not 60 <= elapsed <= 210 or follow < 0.2):
            raise ValueError("Invalid continuation evidence")
        direction_word = "Рост" if move_type == "PUMP" else "Падение"
        return "\n".join((
            f"{heading} — ПРОДОЛЖЕНИЕ",
            f"🪙 {symbol} | {escape(str(signal.get('exchange') or 'OKX'))} SWAP",
            f"Текущая цена: {price_text(signal.get('price'))} USDT",
            f"Первый импульс (~5м): {number(trigger, signed=True, suffix='%')}",
            f"За следующие {elapsed/60:.1f} мин: движение продолжилось ещё на {follow:.2f}%",
            f"{direction_word} проверено по нескольким последовательным котировкам.",
            "",
            "✅ Устойчивость краткосрочного движения подтверждена наблюдениями.",
            "⚠️ НЕ сигнал для входа. Продолжение не гарантирует дальнейшего движения.",
            "OI, CVD и статистическое преимущество сделки здесь не подтверждались.",
        ))

    if reason == "PRICE_THRESHOLD":
        symbol = escape(str(signal.get("symbol") or "UNKNOWN"))
        age_sec = float(signal.get("sample_age_sec"))
        window = str(signal.get("window") or "~5m").lstrip("~")
        bounds = {
            "5m": (240, 420), "10m": (480, 750),
            "20m": (1050, 1350), "30m": (1650, 1950),
        }
        if (window not in bounds or not math.isfinite(age_sec)
                or not bounds[window][0] <= age_sec <= bounds[window][1]):
            raise ValueError("Invalid quote sampling age or window")
        minutes = int(window[:-1])
        return "\n".join((
            heading,
            f"🪙 {symbol} | {escape(str(signal.get('exchange') or 'OKX'))} SWAP",
            f"Цена: {price_text(signal.get('price'))} USDT",
            f"Движение: {number(signal.get('change'), signed=True, suffix='%')}",
            f"Период наблюдения: около {minutes} минут ({age_sec/60:.1f} мин)",
            "",
            "ℹ️ Порог цены из Railway Variables достигнут.",
            ("⚠️ НЕ сигнал для входа. 🔬 Автопроверка OI/CVD/ликвидаций/funding/тренда запускается сразу." if str(signal.get("exchange") or "").upper() == "BYBIT" else "⚠️ НЕ сигнал для входа. OI, CVD и тренд в этом первичном сообщении не проверялись."),
            "Резкое движение может продолжиться или развернуться.",
        ))

    reason_text = {
        "EDGE_UNPROVEN": "Статистическое преимущество входа пока не доказано",
        "TREND_NOT_ALIGNED": "Старший тренд не подтверждает направление",
        "RESEARCH_UNAVAILABLE": "Проверка качества входа сейчас недоступна",
    }.get(reason, "Торговый вход не подтверждён")

    futures = signal.get("futures_flow") or {}
    spot = signal.get("spot_cvd") or {}
    pattern = decision.get("pattern") or "NONE"
    pattern_name = escape(PATTERN_NAMES.get(pattern, "Наблюдаемое движение"))
    symbol = escape(str(signal.get("symbol") or "UNKNOWN"))
    timeframe = escape(str(signal.get("window") or "5m"))

    return "\n".join((
        heading,
        f"🪙 {symbol} | {escape(str(signal.get('exchange') or 'OKX'))} SWAP",
        f"Цена: {price_text(signal.get('price'))} USDT",
        f"Движение за {timeframe}: {number(signal.get('change'), signed=True, suffix='%')}",
        "",
        f"🧩 {pattern_name}",
        f"OI 5м: {number(signal.get('oi_change'), signed=True, suffix='%')}",
        f"Фьючерсный дисбаланс: {number(futures.get('imbalance_pct'), signed=True, suffix='%')}",
        f"Spot CVD: {number(spot.get('cvd_percent'), signed=True, suffix='%')}",
        "",
        f"⚠️ {escape(reason_text)}.",
        "Это оповещение о движении рынка, НЕ сигнал для входа.",
        "Рост или падение могут быстро смениться откатом.",
    ))
