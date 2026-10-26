"""Bounded signal-maturity layer for post-move reversals.

This module does NOT place orders.  It converts already collected market data
into a transparent lifecycle:
WATCH -> PRE_SHORT/PRE_LONG -> CONFIRMED or CANCELLED/EXPIRED.

The layer is deliberately separate from the existing continuation verifier:
a PUMP continuation can still be LONG, while this module watches for a later
PUMP -> SHORT correction.  DUMP is mirrored into a possible LONG reversal.
"""
import math
from html import escape

FLOW_CONFIRM_PCT = 10.0
SPOT_CONFIRM_PCT = 5.0
EARLY_FLOW_MIN_COVERAGE_SEC = 60.0
EARLY_FLOW_MIN_TRADES = 15
EARLY_FLOW_MAX_STALE_SEC = 45.0
OI_MIN_ABS_PCT = 0.15
STRUCTURE_HINT_PCT = 0.60
STRUCTURE_CONFIRM_PCT = 1.50
EXTREME_RECLAIM_PCT = 0.15
FOLLOWUP_INTERVAL_SEC = 60.0
FOLLOWUP_TTL_SEC = 15 * 60.0


def finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def reversal_side(kind):
    kind = str(kind or "").upper()
    if kind == "PUMP":
        return "SHORT", "DOWN"
    if kind == "DUMP":
        return "LONG", "UP"
    return None, None


def update_extreme(kind, current_price, previous_extreme, pre_active=False):
    """Track only prices actually observed by this verifier.

    Returns (new_extreme, reclaimed_after_pre).
    A tiny print beyond the old extreme is ignored for cancellation; a reclaim
    must exceed EXTREME_RECLAIM_PCT.
    """
    kind = str(kind or "").upper()
    current = finite(current_price)
    extreme = finite(previous_extreme)
    if current is None or current <= 0:
        return extreme, False
    if extreme is None or extreme <= 0:
        return current, False

    reclaimed = False
    if kind == "PUMP" and current > extreme:
        if pre_active:
            reclaimed = bool(
                current >= extreme * (1.0 + EXTREME_RECLAIM_PCT / 100.0)
            )
            # Freeze the PRE anchor through tiny probes. Otherwise a staircase
            # of +0.10% prints could keep moving the anchor and never trigger
            # the intended +0.15% cancellation threshold.
            if reclaimed:
                extreme = current
        else:
            extreme = current
    elif kind == "DUMP" and current < extreme:
        if pre_active:
            reclaimed = bool(
                current <= extreme * (1.0 - EXTREME_RECLAIM_PCT / 100.0)
            )
            if reclaimed:
                extreme = current
        else:
            extreme = current
    return extreme, reclaimed


def reversal_from_extreme_pct(kind, current_price, extreme_price):
    current = finite(current_price)
    extreme = finite(extreme_price)
    if current is None or extreme is None or current <= 0 or extreme <= 0:
        return None
    # PUMP correction becomes negative; DUMP rebound becomes positive.
    return (current / extreme - 1.0) * 100.0


def _flow_state(data, direction, threshold):
    data = data if isinstance(data, dict) else {}
    value = finite(data.get("imbalance_pct"))
    if value is None:
        return {"strong": False, "early": False, "opposite": False, "value": None}

    aligned = value <= -threshold if direction == "DOWN" else value >= threshold
    opposite = value >= threshold if direction == "DOWN" else value <= -threshold

    strong = data.get("ready") is True and aligned
    quality = str(data.get("quality") or "").upper()
    trades = int(data.get("trade_count") or 0)
    coverage = finite(data.get("coverage_sec"))
    stale = finite(data.get("last_trade_age_sec"))
    partial_good = (
        quality == "PARTIAL"
        and trades >= EARLY_FLOW_MIN_TRADES
        and coverage is not None
        and coverage >= EARLY_FLOW_MIN_COVERAGE_SEC
        and stale is not None
        and 0 <= stale <= EARLY_FLOW_MAX_STALE_SEC
    )
    early = strong or (partial_good and aligned)
    strong_opposite = data.get("ready") is True and opposite
    return {
        "strong": bool(strong),
        "early": bool(early),
        "opposite": bool(strong_opposite),
        "value": value,
    }


def evaluate(result):
    """Return transparent reversal maturity from independent evidence groups."""
    kind = str((result or {}).get("type") or "").upper()
    side, direction = reversal_side(kind)
    if side is None:
        return {
            "code": "WATCH",
            "side": None,
            "direction": None,
            "early_score": 0,
            "strong_score": 0,
            "continue_watch": False,
            "missing": [],
            "signs": {},
        }

    reverse = finite((result or {}).get("reversal_from_extreme_pct"))
    futures = _flow_state((result or {}).get("futures"), direction, FLOW_CONFIRM_PCT)
    spot = _flow_state((result or {}).get("spot"), direction, SPOT_CONFIRM_PCT)
    agg = (result or {}).get("agg_5m") or {}
    agg_change = finite(agg.get("change_pct"))
    trend5 = str(((result or {}).get("trend_5m") or {}).get("trend") or "UNKNOWN")

    reverse_aligned = (
        reverse is not None
        and ((direction == "DOWN" and reverse < 0) or (direction == "UP" and reverse > 0))
    )
    structure_hint = bool(reverse_aligned and abs(reverse) >= STRUCTURE_HINT_PCT)
    structure_strong = bool(reverse_aligned and abs(reverse) >= STRUCTURE_CONFIRM_PCT)
    trend_aligned = trend5 == direction

    # OI alone is not directional.  Count it only after price has actually
    # started reversing from the observed extreme.  Both expansion (new
    # positions) and contraction (liquidation/covering) can reinforce the move.
    oi_strong = bool(
        agg.get("ready") is True
        and agg_change is not None
        and abs(agg_change) >= OI_MIN_ABS_PCT
        and structure_hint
    )

    signs = {
        "futures": {
            "label": "Futures flow/Delta",
            "early": futures["early"],
            "strong": futures["strong"],
            "value": futures["value"],
        },
        "spot": {
            "label": "Spot flow",
            "early": spot["early"],
            "strong": spot["strong"],
            "value": spot["value"],
        },
        "oi": {
            "label": "OI рынка",
            "early": oi_strong,
            "strong": oi_strong,
            "value": agg_change,
        },
        "structure": {
            "label": "Откат от экстремума",
            "early": structure_hint,
            "strong": structure_strong,
            "value": reverse,
        },
        "trend5": {
            "label": "Структура 5м",
            "early": trend_aligned,
            "strong": trend_aligned,
            "value": trend5,
        },
    }

    early_score = sum(1 for row in signs.values() if row["early"])
    strong_score = sum(1 for row in signs.values() if row["strong"])
    flow_early = futures["early"] or spot["early"]
    hard_opposition = futures["opposite"] and spot["opposite"]

    if hard_opposition and not structure_hint:
        code = "BROKEN"
    elif (
        futures["strong"]
        and structure_strong
        and strong_score >= 4
    ):
        code = "CONFIRMED"
    elif (
        early_score >= 3
        and flow_early
        and structure_hint
    ):
        code = "PRE"
    else:
        code = "WATCH"

    missing = [
        row["label"]
        for row in signs.values()
        if not row["strong"]
    ]
    continue_watch = bool(
        code == "PRE"
        or (
            code == "WATCH"
            and early_score >= 2
            and (structure_hint or flow_early)
        )
    )
    return {
        "code": code,
        "side": side,
        "direction": direction,
        "early_score": early_score,
        "strong_score": strong_score,
        "continue_watch": continue_watch,
        "missing": missing,
        "signs": signs,
        "reversal_pct": reverse,
    }


def _fmt(value, suffix=""):
    value = finite(value)
    if value is None:
        return "нет данных"
    return f"{value:+.2f}{suffix}"


def _sign_line(sign):
    icon = "✅" if sign.get("strong") else "🟠" if sign.get("early") else "▫️"
    value = sign.get("value")
    if isinstance(value, str):
        tail = value
    elif value is None:
        tail = "нет данных"
    else:
        suffix = "%" if sign.get("label") != "Структура 5м" else ""
        tail = _fmt(value, suffix)
    return f"{icon} {escape(str(sign.get('label') or 'Признак'))}: {escape(str(tail))}"


def build_pre_message(result, maturity, checkpoint):
    side = maturity["side"]
    symbol = escape(str(result.get("symbol") or ""))
    title = "PRE-SHORT" if side == "SHORT" else "PRE-LONG"
    lines = [
        f"🟠 <b>{title} — {symbol}</b>",
        "<b>Сценарий созревает, но вход ещё НЕ подтверждён.</b>",
        f"Контроль: {escape(str(checkpoint))}",
        f"Ранние признаки: <b>{maturity['early_score']}/5</b> · строгие: <b>{maturity['strong_score']}/5</b>",
        f"От экстремума: <b>{_fmt(maturity.get('reversal_pct'), '%')}</b>",
        "",
    ]
    lines.extend(_sign_line(maturity["signs"][key]) for key in (
        "futures", "spot", "oi", "structure", "trend5"
    ))
    if maturity.get("missing"):
        lines += [
            "",
            "🔎 <b>Ждём:</b> " + escape(", ".join(maturity["missing"][:3])),
            "⏱ Наблюдение продолжится только пока признаки сохраняются, максимум 15 минут.",
        ]
    return "\n".join(lines)


def build_confirmed_message(result, maturity, checkpoint):
    side = maturity["side"]
    symbol = escape(str(result.get("symbol") or ""))
    icon = "🔴" if side == "SHORT" else "🟢"
    lines = [
        f"{icon} <b>{side} CONFIRMED — {symbol}</b>",
        "✅ <b>Разворот после сильного движения подтверждён</b>",
        f"Контроль: {escape(str(checkpoint))}",
        f"Строгие признаки: <b>{maturity['strong_score']}/5</b>",
        f"От наблюдаемого экстремума: <b>{_fmt(maturity.get('reversal_pct'), '%')}</b>",
        "",
    ]
    lines.extend(_sign_line(maturity["signs"][key]) for key in (
        "futures", "spot", "oi", "structure", "trend5"
    ))
    lines += [
        "",
        "⚠️ Это подтверждение структуры, не автоматический ордер.",
        "Не догонять уже ушедшее движение; вход оценивать от локального ретеста/риска.",
    ]
    return "\n".join(lines)


def build_cancel_message(result, maturity, reason):
    side = (maturity or {}).get("side") or ("SHORT" if result.get("type") == "PUMP" else "LONG")
    symbol = escape(str(result.get("symbol") or ""))
    reason_text = {
        "NEW_EXTREME": "цена снова обновила исходный экстремум",
        "EVIDENCE_FADED": "ранние признаки ослабли и перестали складываться",
        "BROKEN": "потоки развернулись против сценария",
        "TTL_EXPIRED": "за 15 минут полного подтверждения не появилось",
    }.get(reason, "сценарий перестал быть актуальным")
    return "\n".join((
        f"⚫ <b>PRE-{escape(side)} ОТМЕНЁН — {symbol}</b>",
        escape(reason_text) + ".",
        "Новый вход по старому сценарию не рассматриваем.",
    ))
