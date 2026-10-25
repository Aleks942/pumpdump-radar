"""Cross-exchange Spot taker-flow fallback for Bybit mover verification.

Preference is OKX Spot, then Binance Spot. A symbol is observed only while an
active Bybit mover verification needs it. Real WebSocket trades accumulate for
up to five minutes; missing/unlisted markets stay UNKNOWN/UNSUPPORTED and never
count as directional evidence.
"""
import json
import math
import threading
import time
from collections import deque

import requests
import websocket


OKX_WS = "wss://ws.okx.com:8443/ws/v5/public"
BINANCE_WS = "wss://stream.binance.com:9443/ws"
OKX_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments"
BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
WINDOW_SEC = 300
SOURCE_TTL_SEC = 45
MAX_EVENTS_PER_SYMBOL = 120000
OKX_UNIVERSE_TTL_SEC = 30 * 60

_lock = threading.RLock()
_started = False
_ws = {"OKX": None, "Binance": None}
_connected = {"OKX": False, "Binance": False}
_subscribed = {"OKX": set(), "Binance": set()}
_watch_ref = {}
_watch_at = {}
_source = {}
_events = {}
_resolving = set()
_okx_universe = set()
_okx_universe_at = 0.0
_okx_universe_status = "UNKNOWN"
_binance_req_id = 0


def _finite(value):
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _norm(symbol):
    return str(symbol or "").strip().upper().replace("-", "").replace("/", "")


def _okx_pair(symbol):
    symbol = _norm(symbol)
    return symbol[:-4] + "-USDT" if symbol.endswith("USDT") and len(symbol) > 4 else ""


def _refresh_okx_universe(now=None):
    global _okx_universe, _okx_universe_at, _okx_universe_status
    now = time.time() if now is None else float(now)
    with _lock:
        if (
            _okx_universe
            and 0 <= now - _okx_universe_at <= OKX_UNIVERSE_TTL_SEC
        ):
            return set(_okx_universe), _okx_universe_status
    try:
        r = requests.get(
            OKX_INSTRUMENTS,
            params={"instType": "SPOT"},
            timeout=6,
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data") if isinstance(payload, dict) and payload.get("code") == "0" else None
        if not isinstance(rows, list):
            raise ValueError("BAD_OKX_SPOT_SCHEMA")
        universe = {
            str(row.get("instId"))
            for row in rows
            if isinstance(row, dict)
            and row.get("state", "live") == "live"
            and isinstance(row.get("instId"), str)
            and row.get("instId").endswith("-USDT")
        }
        if not universe:
            raise ValueError("EMPTY_OKX_SPOT_UNIVERSE")
        with _lock:
            _okx_universe = universe
            _okx_universe_at = now
            _okx_universe_status = "READY"
        return set(universe), "READY"
    except Exception as exc:
        with _lock:
            if _okx_universe:
                _okx_universe_status = "STALE_CACHE"
                print("[SPOT_FALLBACK_OKX_UNIVERSE_STALE]", type(exc).__name__, flush=True)
                return set(_okx_universe), "STALE_CACHE"
            _okx_universe_status = "DATA_ERROR"
        print("[SPOT_FALLBACK_OKX_UNIVERSE_ERROR]", type(exc).__name__, flush=True)
        return set(), "DATA_ERROR"


def _binance_support(symbol):
    try:
        r = requests.get(
            BINANCE_EXCHANGE_INFO,
            params={"symbol": symbol},
            timeout=6,
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("symbols") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ValueError("BAD_BINANCE_SPOT_SCHEMA")
        row = next(
            (
                item for item in rows
                if isinstance(item, dict) and item.get("symbol") == symbol
            ),
            None,
        )
        if row is None:
            return False
        return (
            row.get("status") == "TRADING"
            and row.get("quoteAsset") == "USDT"
        )
    except Exception as exc:
        print("[SPOT_FALLBACK_BINANCE_CHECK_ERROR]", symbol,
              type(exc).__name__, flush=True)
        return None


def _resolve(symbol):
    symbol = _norm(symbol)
    try:
        pair = _okx_pair(symbol)
        universe, okx_status = _refresh_okx_universe()
        source = None
        if pair and pair in universe:
            source = "OKX"
        else:
            binance = _binance_support(symbol)
            if binance is True:
                source = "Binance"
            elif binance is False and okx_status != "DATA_ERROR":
                source = "UNSUPPORTED"
            else:
                source = "DATA_ERROR"

        with _lock:
            if symbol not in _watch_ref:
                return
            _source[symbol] = source
            _events.setdefault(symbol, deque(maxlen=MAX_EVENTS_PER_SYMBOL))
        if source in ("OKX", "Binance"):
            _subscribe(source, [symbol])
        print("[SPOT_FALLBACK_SOURCE]", symbol, source, flush=True)
    finally:
        with _lock:
            _resolving.discard(symbol)


def watch_symbol(symbol, started_at=None):
    symbol = _norm(symbol)
    if not symbol.endswith("USDT") or len(symbol) <= 4:
        return False
    at = time.time() if started_at is None else float(started_at)
    with _lock:
        _watch_ref[symbol] = _watch_ref.get(symbol, 0) + 1
        _watch_at.setdefault(symbol, at)
        _events.setdefault(symbol, deque(maxlen=MAX_EVENTS_PER_SYMBOL))
        source = _source.get(symbol)
        need_resolve = source not in ("OKX", "Binance", "UNSUPPORTED", "DATA_ERROR")
        if need_resolve and symbol not in _resolving:
            _resolving.add(symbol)
        else:
            need_resolve = False
    _ensure_started()
    if source in ("OKX", "Binance"):
        _subscribe(source, [symbol])
    elif need_resolve:
        threading.Thread(
            target=_resolve,
            args=(symbol,),
            name="spot-fallback-resolve-" + symbol,
            daemon=True,
        ).start()
    return True


def unwatch_symbol(symbol):
    symbol = _norm(symbol)
    source = None
    should_unsubscribe = False
    with _lock:
        count = _watch_ref.get(symbol, 0)
        if count <= 1:
            _watch_ref.pop(symbol, None)
            _watch_at.pop(symbol, None)
            source = _source.get(symbol)
            should_unsubscribe = source in ("OKX", "Binance")
        else:
            _watch_ref[symbol] = count - 1
    if should_unsubscribe:
        _unsubscribe(source, [symbol])


def _next_binance_id():
    global _binance_req_id
    with _lock:
        _binance_req_id += 1
        return _binance_req_id


def _subscribe(exchange, symbols):
    sock = _ws.get(exchange)
    if sock is None:
        return False
    with _lock:
        wanted = [
            _norm(symbol) for symbol in symbols
            if _norm(symbol) in _watch_ref
            and _source.get(_norm(symbol)) == exchange
            and _norm(symbol) not in _subscribed[exchange]
        ]
    if not wanted:
        return True
    try:
        if exchange == "OKX":
            sock.send(json.dumps({
                "op": "subscribe",
                "args": [
                    {"channel": "trades", "instId": _okx_pair(symbol)}
                    for symbol in wanted
                ],
            }))
        else:
            sock.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": [symbol.lower() + "@trade" for symbol in wanted],
                "id": _next_binance_id(),
            }))
        with _lock:
            _subscribed[exchange].update(wanted)
        print("[SPOT_FALLBACK_SUBSCRIBE]", exchange, ",".join(wanted), flush=True)
        return True
    except Exception as exc:
        print("[SPOT_FALLBACK_SUBSCRIBE_ERROR]", exchange,
              type(exc).__name__, flush=True)
        return False


def _unsubscribe(exchange, symbols):
    sock = _ws.get(exchange)
    if sock is None:
        return False
    with _lock:
        existing = [
            _norm(symbol) for symbol in symbols
            if _norm(symbol) in _subscribed[exchange]
        ]
    if not existing:
        return True
    try:
        if exchange == "OKX":
            sock.send(json.dumps({
                "op": "unsubscribe",
                "args": [
                    {"channel": "trades", "instId": _okx_pair(symbol)}
                    for symbol in existing
                ],
            }))
        else:
            sock.send(json.dumps({
                "method": "UNSUBSCRIBE",
                "params": [symbol.lower() + "@trade" for symbol in existing],
                "id": _next_binance_id(),
            }))
        with _lock:
            for symbol in existing:
                _subscribed[exchange].discard(symbol)
        return True
    except Exception:
        return False


def _save(symbol, exchange, ts, side, price, size):
    symbol = _norm(symbol)
    ts = _finite(ts)
    price = _finite(price)
    size = _finite(size)
    if ts is None or price is None or size is None:
        return
    if ts > 100_000_000_000:
        ts /= 1000.0
    side = str(side or "").upper()
    now = time.time()
    if (
        side not in ("BUY", "SELL")
        or price <= 0
        or size <= 0
        or not 0 <= now - ts <= 30
    ):
        return
    with _lock:
        if symbol not in _watch_ref or _source.get(symbol) != exchange:
            return
        q = _events.setdefault(symbol, deque(maxlen=MAX_EVENTS_PER_SYMBOL))
        signed_quote = price * size * (1.0 if side == "BUY" else -1.0)
        q.append((ts, signed_quote))
        while q and now - q[0][0] > WINDOW_SEC + 15:
            q.popleft()


def _okx_message(message):
    try:
        payload = json.loads(message)
        if not isinstance(payload, dict) or "event" in payload:
            return
        arg = payload.get("arg") or {}
        if arg.get("channel") != "trades":
            return
        pair = str(arg.get("instId") or "")
        symbol = _norm(pair)
        for row in payload.get("data") or []:
            if not isinstance(row, dict):
                continue
            _save(
                symbol,
                "OKX",
                row.get("ts"),
                row.get("side"),
                row.get("px"),
                row.get("sz"),
            )
    except Exception as exc:
        print("[SPOT_FALLBACK_MESSAGE_ERROR] OKX",
              type(exc).__name__, flush=True)


def _binance_message(message):
    try:
        row = json.loads(message)
        if not isinstance(row, dict) or row.get("e") != "trade":
            return
        side = "SELL" if row.get("m") is True else "BUY"
        _save(
            row.get("s"),
            "Binance",
            row.get("T"),
            side,
            row.get("p"),
            row.get("q"),
        )
    except Exception as exc:
        print("[SPOT_FALLBACK_MESSAGE_ERROR] Binance",
              type(exc).__name__, flush=True)


def _on_open(exchange, sock):
    with _lock:
        _ws[exchange] = sock
        _connected[exchange] = True
        _subscribed[exchange].clear()
        symbols = tuple(
            symbol for symbol in _watch_ref
            if _source.get(symbol) == exchange
        )
    _subscribe(exchange, symbols)


def _on_close(exchange):
    with _lock:
        _connected[exchange] = False
        _ws[exchange] = None
        _subscribed[exchange].clear()


def _run(exchange, url):
    handler = _okx_message if exchange == "OKX" else _binance_message
    while True:
        try:
            app = websocket.WebSocketApp(
                url,
                on_open=lambda sock: _on_open(exchange, sock),
                on_message=lambda sock, msg: handler(msg),
                on_error=lambda sock, err: _on_close(exchange),
                on_close=lambda sock, code, msg: _on_close(exchange),
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            print("[SPOT_FALLBACK_WS_ERROR]", exchange,
                  type(exc).__name__, flush=True)
        finally:
            _on_close(exchange)
        time.sleep(5)


def _ensure_started():
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(
        target=_run,
        args=("OKX", OKX_WS),
        name="spot-fallback-okx",
        daemon=True,
    ).start()
    threading.Thread(
        target=_run,
        args=("Binance", BINANCE_WS),
        name="spot-fallback-binance",
        daemon=True,
    ).start()
    print("[SPOT_FALLBACK_STREAM_STARTED]", flush=True)


def get_flow(symbol, now=None):
    symbol = _norm(symbol)
    now = time.time() if now is None else float(now)
    with _lock:
        source = _source.get(symbol)
        watch_at = _watch_at.get(symbol)
        connected = _connected.get(source, False)
        rows = list(_events.get(symbol, ()))
    if watch_at is None:
        return {"quality": "NOT_WATCHED", "ready": False}
    if source is None:
        return {"quality": "CHECKING", "ready": False}
    if source == "UNSUPPORTED":
        return {"quality": "UNSUPPORTED", "ready": False}
    if source == "DATA_ERROR":
        return {"quality": "DATA_ERROR", "ready": False}
    if source not in ("OKX", "Binance"):
        return {"quality": "UNAVAILABLE", "ready": False}
    if not connected:
        return {
            "quality": "DISCONNECTED",
            "ready": False,
            "source": source + "_SPOT_WS",
        }

    rows = [row for row in rows if 0 <= now - row[0] <= WINDOW_SEC]
    if not rows:
        return {
            "quality": "WARMING",
            "ready": False,
            "trade_count": 0,
            "coverage_sec": max(0.0, min(WINDOW_SEC, now - watch_at)),
            "source": source + "_SPOT_WS",
        }

    oldest = min(row[0] for row in rows)
    newest = max(row[0] for row in rows)
    observed_from = max(watch_at, oldest)
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
        and total >= 1000.0
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
        "source": source + "_SPOT_WS",
    }
