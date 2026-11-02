"""Independent Binance Spot trade stream for post-pump fallback.

Used only when the primary OKX Spot stream is not ready. Keeps up to ten
candidate symbols warm in parallel so a quiet OKX Spot market does not block
strict post-pump SHORT confirmation when Binance has sufficient real trading.

Fail-closed: readiness requires a continuous warmed window, fresh trades,
minimum trade count and minimum quote notional. No orders are placed.
"""
import json
import math
import threading
import time
from collections import deque

import requests
import websocket

WS="wss://stream.binance.com:9443/ws"
EXCHANGE_INFO="https://api.binance.com/api/v3/exchangeInfo"
WINDOW_SEC=300
READY_COVERAGE_SEC=240
MAX_WATCHED_SPOT=10
MIN_TRADES=15
MIN_NOTIONAL=1000.0
MAX_FRESH_AGE_SEC=60
MAX_SNAPSHOT_AGE_SEC=300
UNIVERSE_TTL_SEC=30*60
MIN_STREAM_TENURE_SEC=15*60
MAX_EVENTS_PER_SYMBOL=120000

_LOCK=threading.RLock()
_watched=()
_subscribed=set()
_socket=None
_socket_ready=False
_thread=None
_lease_at=0.0
_symbol_started={}
_events={}
_universe=set()
_universe_at=0.0
_universe_status="UNKNOWN"
_req_id=0


def _norm(symbol):
    return str(symbol or "").strip().upper().replace("-","").replace("/","")


def _refresh_universe(now=None, request_get=None):
    global _universe,_universe_at,_universe_status
    now=time.time() if now is None else float(now)
    with _LOCK:
        if _universe and 0<=now-_universe_at<=UNIVERSE_TTL_SEC:
            return set(_universe),_universe_status
    if request_get is None:
        request_get=requests.get
    try:
        r=request_get(EXCHANGE_INFO,timeout=6)
        r.raise_for_status()
        payload=r.json()
        rows=payload.get("symbols") if isinstance(payload,dict) else None
        if not isinstance(rows,list):
            raise ValueError("BAD_BINANCE_SPOT_SCHEMA")
        universe={
            str(x.get("symbol"))
            for x in rows if isinstance(x,dict)
            and x.get("status")=="TRADING"
            and x.get("quoteAsset")=="USDT"
            and isinstance(x.get("symbol"),str)
        }
        if not universe:
            raise ValueError("EMPTY_BINANCE_SPOT_UNIVERSE")
        with _LOCK:
            _universe=universe
            _universe_at=now
            _universe_status="READY"
        return set(universe),"READY"
    except Exception as exc:
        with _LOCK:
            if _universe:
                _universe_status="STALE_CACHE"
                print("[POST_PUMP_BINANCE_UNIVERSE_STALE]",
                      type(exc).__name__,flush=True)
                return set(_universe),"STALE_CACHE"
            _universe_status="DATA_ERROR"
        print("[POST_PUMP_BINANCE_UNIVERSE_ERROR]",
              type(exc).__name__,flush=True)
        return set(),"DATA_ERROR"


def _next_id():
    global _req_id
    with _LOCK:
        _req_id+=1
        return _req_id


def _sync_subscriptions(ws):
    target=set(_watched)
    removed=_subscribed-target
    added=target-_subscribed
    if removed:
        ws.send(json.dumps({
            "method":"UNSUBSCRIBE",
            "params":[p.lower()+"@trade" for p in sorted(removed)],
            "id":_next_id(),
        }))
    if added:
        ws.send(json.dumps({
            "method":"SUBSCRIBE",
            "params":[p.lower()+"@trade" for p in sorted(added)],
            "id":_next_id(),
        }))
    now=time.time()
    for symbol in removed:
        _events.pop(symbol,None)
        _symbol_started.pop(symbol,None)
    for symbol in added:
        _events[symbol]=deque(maxlen=MAX_EVENTS_PER_SYMBOL)
        _symbol_started[symbol]=now
    _subscribed.clear()
    _subscribed.update(target)
    if added or removed:
        print("[POST_PUMP_BINANCE_SUB_CHANGE]",
              "added=",",".join(sorted(added)) or "NONE",
              "removed=",",".join(sorted(removed)) or "NONE",
              "retained=",",".join(sorted(target-added)) or "NONE",
              flush=True)


def watch(symbols, urgent_symbols=()):
    global _watched,_thread,_lease_at
    raw=tuple(dict.fromkeys(
        _norm(s) for s in (symbols or ())
        if _norm(s).endswith("USDT") and len(_norm(s))>4
    ))
    urgent={_norm(s) for s in (urgent_symbols or ())}
    now=time.time()
    universe,status=_refresh_universe(now=now)
    candidates=tuple(s for s in raw if s in universe)
    with _LOCK:
        old=_watched
        if not universe and status=="DATA_ERROR" and old:
            candidates=old
        retained=old
        if old and now-_lease_at<MIN_STREAM_TENURE_SEC:
            retained=tuple(s for s in old if not universe or s in universe)
            urgent_ranked=tuple(s for s in candidates if s in urgent)
            urgent_missing=tuple(s for s in urgent_ranked if s not in retained)
            if urgent_missing:
                ordered=[]
                for s in urgent_ranked+retained+candidates:
                    if s not in ordered:
                        ordered.append(s)
                    if len(ordered)>=MAX_WATCHED_SPOT:
                        break
                target=tuple(ordered)
                print("[POST_PUMP_BINANCE_PRIORITY]",
                      "urgent=",",".join(urgent_missing),
                      "old=",",".join(old) or "NONE",
                      "new=",",".join(target) or "NONE",flush=True)
            else:
                additions=tuple(s for s in candidates if s not in retained)
                target=(retained+additions)[:MAX_WATCHED_SPOT]
        else:
            target=tuple(candidates[:MAX_WATCHED_SPOT])
        if target!=old:
            _watched=target
            _lease_at=now
        if _thread is None or not _thread.is_alive():
            _thread=threading.Thread(target=_worker,daemon=True,
                                     name="post-pump-binance-spot")
            _thread.start()
        if _socket is not None and _socket_ready:
            try:
                _sync_subscriptions(_socket)
            except Exception:
                try:
                    _socket.close()
                except Exception:
                    pass
    return target


def _message(message):
    try:
        row=json.loads(message)
    except (TypeError,ValueError):
        return
    if not isinstance(row,dict) or row.get("e")!="trade":
        return
    symbol=_norm(row.get("s"))
    with _LOCK:
        if symbol not in _watched:
            return
    try:
        ts=float(row.get("T"))/1000
        px=float(row.get("p"))
        qty=float(row.get("q"))
        if not all(math.isfinite(x) and x>0 for x in (ts,px,qty)):
            return
        side="SELL" if row.get("m") is True else "BUY"
        signed=px*qty*(1.0 if side=="BUY" else -1.0)
        now=time.time()
        if not 0<=now-ts<=30:
            return
        with _LOCK:
            q=_events.setdefault(symbol,deque(maxlen=MAX_EVENTS_PER_SYMBOL))
            q.append((ts,signed))
            while q and now-q[0][0]>WINDOW_SEC+15:
                q.popleft()
    except (TypeError,ValueError,OverflowError):
        return


def _worker():
    global _socket,_socket_ready
    while True:
        with _LOCK:
            pairs=_watched
        if not pairs:
            time.sleep(15)
            continue
        def on_open(ws):
            global _socket_ready
            with _LOCK:
                _socket_ready=True
                _subscribed.clear()
                for s in _watched:
                    _events[s]=deque(maxlen=MAX_EVENTS_PER_SYMBOL)
                    _symbol_started[s]=time.time()
                _sync_subscriptions(ws)
            print("[POST_PUMP_BINANCE_WS_SUB]",len(_subscribed),flush=True)
        def on_close(ws,code,msg):
            global _socket_ready
            with _LOCK:
                _socket_ready=False
                _subscribed.clear()
                for s in _watched:
                    _events[s]=deque(maxlen=MAX_EVENTS_PER_SYMBOL)
                    _symbol_started[s]=time.time()
        try:
            ws=websocket.WebSocketApp(
                WS,on_open=on_open,
                on_message=lambda ws,msg:_message(msg),
                on_close=on_close,
                on_error=lambda ws,e:print(
                    "[POST_PUMP_BINANCE_WS_ERROR]",type(e).__name__,flush=True))
            with _LOCK:
                _socket=ws
                _socket_ready=False
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as exc:
            print("[POST_PUMP_BINANCE_WS_ERROR]",type(exc).__name__,flush=True)
        finally:
            with _LOCK:
                if _socket is ws:
                    _socket=None
                    _socket_ready=False
        time.sleep(5)


def _flow(symbol,max_age):
    symbol=_norm(symbol)
    now=time.time()
    with _LOCK:
        if symbol not in _watched:
            return {"ready":False,"reason":"BINANCE_NOT_WATCHED"}
        if not _socket_ready:
            return {"ready":False,"reason":"BINANCE_DISCONNECTED"}
        started=_symbol_started.get(symbol)
        rows=list(_events.get(symbol,()))
    if started is None or now-started<READY_COVERAGE_SEC:
        return {"ready":False,"reason":"BINANCE_WARMING"}
    rows=[r for r in rows if 0<=now-r[0]<=WINDOW_SEC]
    if not rows:
        return {"ready":False,"reason":"BINANCE_NO_TRADES"}
    oldest=min(x[0] for x in rows)
    newest=max(x[0] for x in rows)
    coverage=max(0.0,min(WINDOW_SEC,newest-max(started,oldest)))
    age=max(0.0,now-newest)
    buy=sum(v for _,v in rows if v>0)
    sell=-sum(v for _,v in rows if v<0)
    total=buy+sell
    if coverage<READY_COVERAGE_SEC:
        return {"ready":False,"reason":"BINANCE_WARMING",
                "coverage_sec":coverage}
    if age>max_age:
        return {"ready":False,"reason":"BINANCE_TRADES_STALE",
                "last_trade_age_sec":age}
    if len(rows)<MIN_TRADES:
        return {"ready":False,"reason":"BINANCE_TOO_FEW_TRADES",
                "count":len(rows)}
    if total<MIN_NOTIONAL:
        return {"ready":False,"reason":"BINANCE_TOO_LITTLE_NOTIONAL",
                "total_quote":total}
    imbalance=100*(buy-sell)/total
    if not math.isfinite(imbalance):
        return {"ready":False,"reason":"BINANCE_BAD_IMBALANCE"}
    return {
        "ready":True,
        "reason":"BINANCE_SPOT_WS_5M" if max_age<=MAX_FRESH_AGE_SEC
                 else "BINANCE_SPOT_WS_5M_SNAPSHOT",
        "source":"BINANCE_SPOT_WS",
        "imbalance_pct":imbalance,
        "delta_quote":buy-sell,
        "count":len(rows),
        "last_trade_age_sec":age,
        "coverage_sec":coverage,
        "total_quote":total,
    }


def verified(symbol):
    return _flow(symbol,MAX_FRESH_AGE_SEC)


def snapshot_fallback(symbol):
    return _flow(symbol,MAX_SNAPSHOT_AGE_SEC)
