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


def _price_text(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return "нет данных"

    if not math.isfinite(value) or value <= 0:
        return "нет данных"

    return f"{value:.8f}".rstrip("0").rstrip(".")


def _signed_value(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _traffic(value, expected):
    value = _signed_value(value)
    if value is None:
        return "🟡", "нет данных"
    aligned = value > 0 if expected == "UP" else value < 0
    return ("🟢", "подтверждает") if aligned else ("🔴", "против сигнала")


def build_short_message(signal):
    """Compact professional Telegram card for a confirmed PumpDump signal."""
    decision = signal.get("decision") or {}
    direction = decision.get("direction") or "NONE"
    pattern = decision.get("pattern") or "NONE"
    symbol = escape(str(signal.get("symbol") or "UNKNOWN"))
    price = _price_text(signal.get("price"))
    move = number(signal.get("change"), signed=True, suffix="%")

    if direction == "UP":
        side = "LONG"
        icon = "🟢"
        expected = "UP"
        action = "Покупатели контролируют движение."
        cancel = "Отмена идеи: 5м поток и OI разворачиваются вниз."
    elif direction == "DOWN":
        side = "SHORT"
        icon = "🔴"
        expected = "DOWN"
        action = "Продавцы контролируют движение."
        cancel = "Отмена идеи: 5м поток и OI разворачиваются вверх."
    else:
        side = "ЖДАТЬ"
        icon = "🟡"
        expected = "UP"
        action = "Направление пока не подтверждено."
        cancel = "Ждём нового подтверждения."

    pattern_text = {
        "NEW_LONG_BUILDUP": "Новые лонги заходят в рынок",
        "NEW_SHORT_BUILDUP": "Новые шорты заходят в рынок",
        "SHORT_SQUEEZE": "Шорты закрываются и толкают цену вверх",
        "LONG_LIQUIDATION": "Лонги закрываются и усиливают падение",
        "NONE": "Чёткого паттерна пока нет",
    }.get(pattern, "Паттерн подтверждён фильтрами")

    aggregated = signal.get("aggregated_oi") or {}
    agg_5m = (aggregated.get("windows") or {}).get("5m") or {}
    agg_change = agg_5m.get("change_pct")
    confidence = agg_5m.get("confidence")
    confidence_text = {
        "HIGH": "🟢 ВЫСОКАЯ",
        "MEDIUM": "🟡 СРЕДНЯЯ",
    }.get(confidence, "🟡 СРЕДНЯЯ")

    futures = signal.get("futures_flow") or {}
    spot = signal.get("spot_cvd") or {}

    futures_value = futures.get("imbalance_pct")
    spot_value = spot.get("cvd_percent")
    local_oi = signal.get("oi_change")

    futures_icon, futures_note = _traffic(futures_value, expected)
    spot_icon, spot_note = _traffic(spot_value, expected)

    oi_value = _signed_value(local_oi)
    agg_value = _signed_value(agg_change)
    requires_expansion = pattern in ("NEW_LONG_BUILDUP", "NEW_SHORT_BUILDUP")
    requires_contraction = pattern in ("SHORT_SQUEEZE", "LONG_LIQUIDATION")

    def oi_line_icon(value):
        if value is None:
            return "🟡"
        if requires_expansion:
            return "🟢" if value > 0 else "🔴"
        if requires_contraction:
            return "🟢" if value < 0 else "🔴"
        return "🟡"

    oi_icon = oi_line_icon(oi_value)
    agg_icon = oi_line_icon(agg_value)

    if confidence == "HIGH":
        agg_source = "3/3 биржи"
    elif agg_5m.get("consensus_mode") == "2/2_SUPPORTED":
        agg_source = "2/2 доступных биржи"
    else:
        agg_source = "консенсус бирж"

    return "\n".join((
        f"{icon} <b>{side} — {symbol}</b>",
        f"✅ <b>СИГНАЛ ПОДТВЕРЖДЁН</b> · сила: {confidence_text}",
        f"💰 Цена: <b>{price}</b> · импульс {move}",
        "",
        f"📌 {escape(pattern_text)}",
        f"{oi_icon} OI 5м: {number(local_oi, signed=True, suffix='%')}",
        f"{agg_icon} OI рынка: {number(agg_change, signed=True, suffix='%')} · {agg_source}",
        f"{futures_icon} Фьючерсы: {number(futures_value, signed=True, suffix='%')} · {futures_note}",
        f"{spot_icon} Спот: {number(spot_value, signed=True, suffix='%')} · {spot_note}",
        "",
        f"🎯 <b>Сейчас:</b> {escape(action)}",
        "⚠️ Не догонять свечу — лучше дождаться отката/ретеста.",
        f"❌ {escape(cancel)}",
    ))


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
        if move_type == "PUMP":
            move_label = "ПАМП"
            move_icon = "📈"
        else:
            move_label = "ДАМП"
            move_icon = "📉"

        return "\n".join((
            f"🟡 <b>{move_label} ОБНАРУЖЕН — {symbol}</b>",
            f"{move_icon} Движение: <b>{number(signal.get('change'), signed=True, suffix='%')}</b> за ~{minutes} мин",
            f"💰 Цена: <b>{price_text(signal.get('price'))}</b>",
            "",
            "🔎 Бот проверяет OI · фьючерсы · спот · ликвидации",
            "⚠️ Это факт движения рынка, а не вход.",
            "➡️ Если сценарий подтвердится — придёт отдельный 🟢 LONG или 🔴 SHORT.",
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
