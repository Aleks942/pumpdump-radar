# money_flow_engine.py
import requests
import time

OKX = "https://www.okx.com"

def okx_get(path, params=None):
    try:
        r = requests.get(OKX + path, params=params or {}, timeout=10)
        data = r.json()
        if data.get("code") != "0":
            return None
        return data.get("data", [])
    except Exception as e:
        print("[OKX_ERROR]", path, e)
        return None


def get_candles(inst_id, bar="5m", limit=20):
    data = okx_get("/api/v5/market/candles", {
        "instId": inst_id,
        "bar": bar,
        "limit": limit
    })
    if not data:
        return []

    candles = []
    for c in data:
        candles.append({
            "ts": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[5]),
        })
    return list(reversed(candles))


def get_open_interest(inst_id):
    data = okx_get("/api/v5/public/open-interest", {
        "instType": "SWAP",
        "instId": inst_id
    })
    if not data:
        return None
    return float(data[0].get("oi", 0))


def get_funding(inst_id):
    data = okx_get("/api/v5/public/funding-rate", {
        "instId": inst_id
    })
    if not data:
        return None
    return float(data[0].get("fundingRate", 0)) * 100


def get_market_buy_sell(inst_id, limit=100):
    data = okx_get("/api/v5/market/trades", {
        "instId": inst_id,
        "limit": limit
    })
    
    if not data:
        return {
            "market_buy_qty": 0,
            "market_sell_qty": 0,
            "delta": 0,
            "delta_ratio": 0,
            "pressure": "NO_DATA"
        }

    buy_qty = 0
    sell_qty = 0

    for t in data:
        qty = float(t.get("sz", 0))
        side = t.get("side")

        if side == "buy":
            buy_qty += qty
        elif side == "sell":
            sell_qty += qty

    delta = buy_qty - sell_qty
    total = buy_qty + sell_qty

    if total == 0:
        pressure = "NO_DATA"
    else:
        ratio = delta / total

        if ratio > 0.20:
            pressure = "STRONG_BUY_PRESSURE"
        elif ratio > 0.05:
            pressure = "BUY_PRESSURE"
        elif ratio < -0.20:
            pressure = "STRONG_SELL_PRESSURE"
        elif ratio < -0.05:
            pressure = "SELL_PRESSURE"
        else:
            pressure = "BALANCED"

    return {
        "market_buy_qty": buy_qty,
        "market_sell_qty": sell_qty,
        "delta": delta,
        "delta_ratio": round(ratio * 100, 2) if total > 0 else 0,
        "pressure": pressure
    }


OI_MEMORY = {}

def analyze_new_money(inst_id):
    candles = get_candles(inst_id, "5m", 20)
    if len(candles) < 5:
        return None

    last = candles[-1]
    prev = candles[-2]

    price_now = last["close"]
    price_prev = prev["close"]

    price_change_pct = ((price_now - price_prev) / price_prev) * 100

    volume_now = last["volume"]
    avg_volume = sum(c["volume"] for c in candles[-10:-1]) / 9
    volume_ratio = volume_now / avg_volume if avg_volume > 0 else 0

    oi_now = get_open_interest(inst_id)
    funding = get_funding(inst_id)
    flow = get_market_buy_sell(inst_id)

    oi_prev = OI_MEMORY.get(inst_id)
    OI_MEMORY[inst_id] = oi_now

    if oi_now is None or oi_prev is None:
        oi_change_pct = 0
    else:
        oi_change_pct = ((oi_now - oi_prev) / oi_prev) * 100 if oi_prev > 0 else 0

    score = 0
    reasons = []

    if price_change_pct > 0.3 and oi_change_pct > 0.2:
        score += 3
        reasons.append("цена растёт + OI растёт = заходят новые деньги в LONG")

    if price_change_pct < -0.3 and oi_change_pct > 0.2:
        score += 3
        reasons.append("цена падает + OI растёт = заходят новые деньги в SHORT")

    if volume_ratio >= 1.5:
        score += 2
        reasons.append("объём выше среднего")

    if flow["pressure"] in ["STRONG_BUY_PRESSURE", "BUY_PRESSURE"]:
        score += 2
        reasons.append("по рынку активно покупают")

    if flow["pressure"] in ["STRONG_SELL_PRESSURE", "SELL_PRESSURE"]:
        score += 2
        reasons.append("по рынку активно продают")

    if price_change_pct > 0.3 and oi_change_pct < -0.2:
        score -= 2
        reasons.append("цена растёт, но OI падает = возможно шорты закрываются, не новые деньги")

    if price_change_pct < -0.3 and oi_change_pct < -0.2:
        score -= 2
        reasons.append("цена падает, но OI падает = возможно лонги закрываются")

    if score >= 6:
        state = "STRONG_NEW_MONEY"
    elif score >= 4:
        state = "BUILDING_MONEY"
    elif score >= 2:
        state = "WEAK_FLOW"
    else:
        state = "NO_CLEAR_MONEY"

    return {
        "symbol": inst_id.replace("-USDT-SWAP", "USDT"),
        "price": price_now,
        "price_change_pct": round(price_change_pct, 2),
        "volume_ratio": round(volume_ratio, 2),
        "oi": oi_now,
        "oi_change_pct": round(oi_change_pct, 2),
        "funding": round(funding, 4) if funding is not None else None,
        "market_buy_qty": round(flow["market_buy_qty"], 2),
        "market_sell_qty": round(flow["market_sell_qty"], 2),
        "delta": round(flow["delta"], 2),
        "pressure": flow["pressure"],
        "money_state": state,
        "money_score": score,
        "reasons": reasons
    }
