"""Dynamic Bybit public-trade streams for mover verification.

Collects real event-time trades after a Bybit mover is detected so the
1m/3m/5m follow-up checks use an actually observed window instead of a
single REST page limited to the latest 1000 trades.
"""
import json
import math
import threading
import time
from collections import deque

import requests
import websocket


LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"
SPOT_WS = "wss://stream.bybit.com/v5/public/spot"
WINDOW_SEC = 300
SOURCE_TTL_SEC = 45
MAX_EVENTS_PER_SYMBOL = 120000

_lock = threading.RLock()
_started = False
_ws = {"linear": None, "spot": None}
_connected = {"linear": False, "spot": False}
_subscribed = {"linear": set(), "spot": set()}
_watch_ref = {}
_watch_at = {}
_events = {}
_spot_support = {}


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _norm(symbol):
    return str(symbol or "").strip().upper().replace("-", "").replace("/", "")


def _prune(category, symbol, now):
    key = (category, symbol)
    q = _events.get(key)
    if not q:
        return
    while q and now - q[0][0] > WINDOW_SEC + 15:
        q.popleft()


def _subscribe(category, symbols):
    sock = _ws.get(category)
    if sock is None:
        return False
    wanted = []
    with _lock:
        for symbol in symbols:
            symbol = _norm(symbol)
            if symbol.endswith("USDT") and symbol not in _subscribed[category]:
                wanted.append(symbol)
    if not wanted:
        return True
    try:
        for i in range(0, len(wanted), 10):
            batch = wanted[i:i + 10]
            sock.send(json.dumps({
                "op": "subscribe",
                "args": ["publicTrade." + symbol for symbol in batch],
            }))
        with _lock:
            _subscribed[category].update(wanted)
        print("[BYBIT_FLOW_SUBSCRIBE]", category, ",".join(wanted), flush=True)
        return True
    except Exception as exc:
        print("[BYBIT_FLOW_SUBSCRIBE_ERROR]", category, type(exc).__name__, flush=True)
        return False


def _spot_exists(symbol):
    symbol = _norm(symbol)
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "spot", "symbol": symbol},
            timeout=6,
        )
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return False
        rows = ((data.get("result") or {}).get("list") or [])
        return any(
            isinstance(row, dict)
            and row.get("symbol") == symbol
            and str(row.get("status") or "").lower() == "trading"
            for row in rows
        )
    except Exception:
        return None


def _resolve_spot(symbol):
    supported = _spot_exists(symbol)
    with _lock:
        _spot_support[symbol] = supported
    if supported:
        _subscribe("spot", [symbol])
    print("[BYBIT_SPOT_SUPPORT]", symbol, supported, flush=True)


def watch_symbol(symbol, started_at=None):
    global _started
    symbol = _norm(symbol)
    if not symbol.endswith("USDT"):
        return False
    at = time.time() if started_at is None else float(started_at)
    with _lock:
        _watch_ref[symbol] = _watch_ref.get(symbol, 0) + 1
        _watch_at.setdefault(symbol, at)
        _events.setdefault(("linear", symbol), deque(maxlen=MAX_EVENTS_PER_SYMBOL))
        _events.setdefault(("spot", symbol), deque(maxlen=MAX_EVENTS_PER_SYMBOL))
        need_spot_check = symbol not in _spot_support
    _ensure_started()
    _subscribe("linear", [symbol])
    if need_spot_check:
        threading.Thread(
            target=_resolve_spot,
            args=(symbol,),
            name="bybit-spot-check-" + symbol,
            daemon=True,
        ).start()
    elif _spot_support.get(symbol) is True:
        _subscribe("spot", [symbol])
    return True


def unwatch_symbol(symbol):
    symbol = _norm(symbol)
    with _lock:
        count = _watch_ref.get(symbol, 0)
        if count <= 1:
            _watch_ref.pop(symbol, None)
            _watch_at.pop(symbol, None)
        else:
            _watch_ref[symbol] = count - 1


def _on_open(category, sock):
    with _lock:
        _ws[category] = sock
        _connected[category] = True
        _subscribed[category].clear()
        symbols = tuple(sorted(_watch_ref))
        if category == "spot":
            symbols = tuple(
                symbol for symbol in symbols
                if _spot_support.get(symbol) is True
            )
    _subscribe(category, symbols)


def _on_close(category):
    with _lock:
        _connected[category] = False
        _ws[category] = None
        _subscribed[category].clear()


def _on_message(category, sock, message):
    try:
        payload = json.loads(message)
        if payload.get("success") is False:
            print("[BYBIT_FLOW_SUB_REJECTED]", category,
                  str(payload.get("ret_msg") or "")[:120], flush=True)
            return
        topic = str(payload.get("topic") or "")
        if not topic.startswith("publicTrade."):
            return
        symbol = _norm(topic.split(".", 1)[1])
        rows = payload.get("data")
        if not isinstance(rows, list):
            return
        now = time.time()
        with _lock:
            if symbol not in _watch_ref:
                return
            q = _events.setdefault(
                (category, symbol),
                deque(maxlen=MAX_EVENTS_PER_SYMBOL),
            )
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if _norm(row.get("s")) != symbol:
                    continue
                ts = _finite(row.get("T"))
                price = _finite(row.get("p"))
                size = _finite(row.get("v"))
                side = str(row.get("S") or "").upper()
                if ts is None or price is None or size is None:
                    continue
                if ts > 100_000_000_000:
                    ts /= 1000.0
                if side not in ("BUY", "SELL") or price <= 0 or size <= 0:
                    continue
                if not 0 <= now - ts <= 30:
                    continue
                signed_quote = price * size * (1.0 if side == "BUY" else -1.0)
                q.append((ts, signed_quote))
            _prune(category, symbol, now)
    except Exception as exc:
        print("[BYBIT_FLOW_MESSAGE_ERROR]", category, type(exc).__name__, flush=True)


def _run(category, url):
    while True:
        try:
            app = websocket.WebSocketApp(
                url,
                on_open=lambda sock: _on_open(category, sock),
                on_message=lambda sock, msg: _on_message(category, sock, msg),
                on_error=lambda sock, err: _on_close(category),
                on_close=lambda sock, code, msg: _on_close(category),
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print("[BYBIT_FLOW_WS_ERROR]", category, type(exc).__name__, flush=True)
        finally:
            _on_close(category)
        time.sleep(5)


def _ensure_started():
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_run, args=("linear", LINEAR_WS),
                     name="bybit-flow-linear", daemon=True).start()
    threading.Thread(target=_run, args=("spot", SPOT_WS),
                     name="bybit-flow-spot", daemon=True).start()
    print("[BYBIT_FLOW_STREAM_STARTED]", flush=True)


def get_flow(symbol, category="linear", now=None, since=None):
    symbol = _norm(symbol)
    now = time.time() if now is None else float(now)
    if category not in ("linear", "spot"):
        return {"quality": "INVALID", "ready": False}
    with _lock:
        if category == "spot":
            support = _spot_support.get(symbol)
            if support is False:
                return {"quality": "UNSUPPORTED", "ready": False}
            if support is None:
                return {"quality": "CHECKING", "ready": False}
        connected = _connected.get(category, False)
        watch_at = _watch_at.get(symbol)
        rows = list(_events.get((category, symbol), ()))
    if watch_at is None:
        return {"quality": "NOT_WATCHED", "ready": False}
    if not connected:
        return {"quality": "DISCONNECTED", "ready": False}
    start_at = watch_at
    if since is not None:
        try:
            start_at = max(start_at, float(since))
        except (TypeError, ValueError, OverflowError):
            pass
    rows = [
        row for row in rows
        if 0 <= now - row[0] <= WINDOW_SEC and row[0] >= start_at
    ]
    if not rows:
        return {
            "quality": "WARMING",
            "ready": False,
            "trade_count": 0,
            "coverage_sec": max(0.0, min(WINDOW_SEC, now - start_at)),
        }
    oldest = min(row[0] for row in rows)
    newest = max(row[0] for row in rows)
    observed_from = max(start_at, oldest)
    coverage = max(0.0, min(WINDOW_SEC, newest - observed_from))
    stale = max(0.0, now - newest)
    buy = sum(value for _, value in rows if value > 0)
    sell = -sum(value for _, value in rows if value < 0)
    total = buy + sell
    delta = buy - sell
    imbalance = 100.0 * delta / total if total > 0 else 0.0
    ready = (
        coverage >= 240.0
        and stale <= SOURCE_TTL_SEC
        and len(rows) >= 15
        and total > 0
    )
    return {
        "quality": "READY" if ready else "PARTIAL",
        "ready": ready,
        "trade_count": len(rows),
        "coverage_sec": coverage,
        "last_trade_age_sec": stale,
        "buy_quote": buy,
        "sell_quote": sell,
        "delta_quote": delta,
        "imbalance_pct": imbalance,
        "source": "BYBIT_PUBLIC_TRADE_WS",
    }
