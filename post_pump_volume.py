"""Volume-behaviour analyzer for POST-PUMP research.

Uses only the already-collected 1m/5m taker-flow windows. It makes no HTTP
requests, opens no websocket and does not change trading gates. The result is
descriptive research evidence that can later be audited against outcomes.
"""
import math


def _num(value):
    try:
        x = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def _window(window):
    if not isinstance(window, dict):
        return None
    total = _num(window.get("total_quote"))
    imbalance = _num(window.get("imbalance_pct"))
    trades = _num(window.get("trade_count"))
    if (
        window.get("window_ready") is not True
        or str(window.get("quality")) != "READY"
        or total is None
        or total < 0
        or imbalance is None
        or not -100 <= imbalance <= 100
        or trades is None
        or trades < 0
    ):
        return None
    return {
        "total_quote": total,
        "imbalance_pct": imbalance,
        "trade_count": int(trades),
    }


def analyze_volume_windows(futures_windows, spot_windows=None):
    """Describe short-term volume expansion/fade and directional pressure.

    The 1m window is compared with the preceding four minutes implied by the
    5m aggregate: (5m_total - 1m_total) / 4. This avoids extra storage/API
    calls while keeping the metric causal.
    """
    fw = futures_windows if isinstance(futures_windows, dict) else {}
    sw = spot_windows if isinstance(spot_windows, dict) else {}

    f1 = _window(fw.get("1m"))
    f5 = _window(fw.get("5m"))
    s5 = _window(sw.get("5m"))

    if f1 is None or f5 is None:
        return {
            "ready": False,
            "state": "INSUFFICIENT_VOLUME_DATA",
            "reason": "FUTURES_1M_5M_NOT_READY",
            "research_only": True,
        }

    current_1m = f1["total_quote"]
    prior_4m_total = max(0.0, f5["total_quote"] - current_1m)
    prior_1m_avg = prior_4m_total / 4.0

    if prior_1m_avg > 0:
        activity_ratio = current_1m / prior_1m_avg
    elif current_1m > 0:
        activity_ratio = 9.99
    else:
        activity_ratio = 0.0

    f1_imb = f1["imbalance_pct"]
    f5_imb = f5["imbalance_pct"]
    spot_imb = s5["imbalance_pct"] if s5 is not None else None

    expansion = activity_ratio >= 1.50
    fade = activity_ratio <= 0.65
    seller_shift = f1_imb <= -15 and f5_imb <= -10
    buyer_shift = f1_imb >= 15 and f5_imb >= 10
    spot_sell = spot_imb is not None and spot_imb <= -10
    spot_buy = spot_imb is not None and spot_imb >= 10

    if expansion and seller_shift and spot_sell:
        state = "SELL_VOLUME_EXPANSION"
        reason = "VOLUME_EXPANDS_WITH_SELLERS_ON_FUTURES_AND_SPOT"
    elif expansion and seller_shift:
        state = "FUTURES_SELL_EXPANSION"
        reason = "VOLUME_EXPANDS_WITH_FUTURES_SELLERS"
    elif fade and seller_shift:
        state = "SELL_PRESSURE_ON_FADING_VOLUME"
        reason = "SELLERS_LEAD_BUT_ACTIVITY_IS_FADING"
    elif fade:
        state = "VOLUME_FADE"
        reason = "CURRENT_ACTIVITY_BELOW_PREVIOUS_4M_AVERAGE"
    elif expansion and buyer_shift and spot_buy:
        state = "BUY_VOLUME_EXPANSION"
        reason = "VOLUME_EXPANDS_WITH_BUYERS_ON_FUTURES_AND_SPOT"
    elif buyer_shift:
        state = "BUYER_PRESSURE"
        reason = "BUYERS_LEAD_CURRENT_FLOW"
    else:
        state = "VOLUME_MIXED"
        reason = "NO_CLEAR_VOLUME_EDGE"

    return {
        "ready": True,
        "state": state,
        "reason": reason,
        "activity_ratio": round(activity_ratio, 3),
        "current_1m_quote": round(current_1m, 2),
        "previous_4m_avg_quote_per_min": round(prior_1m_avg, 2),
        "futures_1m_imbalance_pct": round(f1_imb, 2),
        "futures_5m_imbalance_pct": round(f5_imb, 2),
        "spot_5m_imbalance_pct": round(spot_imb, 2) if spot_imb is not None else None,
        "volume_expansion": expansion,
        "volume_fade": fade,
        "seller_shift": seller_shift,
        "buyer_shift": buyer_shift,
        "research_only": True,
    }
