"""Targeted OKX SPOT trades WebSocket for POST-PUMP SHORT candidates.

A single low-volume companion thread within the existing PumpDump process;
NO new Railway service, order, API key, score or Telegram. Maintains only up to
3 selected real spot markets in the existing trade_flow_collector_v3 cache.
Readiness requires 5m unbroken connection and real traded quote, not the most
recent 100 REST trades. Spot contracts not listed/supported fail closed.
"""
import json
import math
import threading
import time

from trade_flow_collector_v3 import (
    save_trade, mark_stream_connected, mark_stream_disconnected,
    mark_stream_activity, get_spot_windows, reset_symbol,
)

_LOCK=threading.RLock()
_watched=()
_socket=None
_socket_ready=False
_subscribed=set()
_thread=None
_lease_at=0.0
MIN_STREAM_TENURE_SEC=15*60
_WS="wss://ws.okx.com:8443/ws/v5/public"
_SPOT_INSTRUMENTS_URL="https://www.okx.com/api/v5/public/instruments"
SPOT_UNIVERSE_TTL_SEC=30*60
_SPOT_UNIVERSE=set()
_SPOT_UNIVERSE_AT=0.0
_SPOT_UNIVERSE_STATUS="UNKNOWN"
_RECONNECT_BASE_SEC=5
_RECONNECT_MAX_SEC=60
_SPOT_DIAG_INTERVAL_SEC=300
_SPOT_DIAG_LAST={}


def _reconnect_delay(failures):
    """Back off repeated short sessions without slowing healthy reconnects."""
    return min(_RECONNECT_MAX_SEC,
               _RECONNECT_BASE_SEC * (2 ** min(max(0,int(failures)-1),4)))


def _spot_gap_reason(item):
    """Distinguish network problems from insufficient real trades."""
    if not item or item.get("stream_healthy") is not True:
        return "STREAM_DISCONNECTED_OR_STALE"
    age=item.get("stream_stale_seconds")
    if age is None or not 0<=age<=30:
        return "STREAM_DISCONNECTED_OR_STALE"
    if item.get("window_ready") is not True or item.get("quality")!="READY":
        return "WARMING_AFTER_CONNECTION_OR_SUBSCRIPTION"
    age=item.get("last_trade_age_sec")
    if age is None or not 0<=age<=60:
        return "SYMBOL_TRADES_MISSING_OR_STALE"
    count=item.get("trade_count")
    if count is None or count<15:
        return "TOO_FEW_REAL_TRADES"
    total=item.get("total_quote")
    if total is None or total<1000:
        return "TOO_LITTLE_TRADED_NOTIONAL"
    try:
        if not math.isfinite(float(item.get("imbalance_pct"))):
            return "BAD_IMBALANCE_DATA"
    except (TypeError,ValueError,OverflowError):
        return "BAD_IMBALANCE_DATA"
    return "UNKNOWN_NOT_READY"


def _log_spot_gap(pair,cause):
    """Rate-limit diagnostic log lines without changing SHORT gates."""
    now=time.monotonic()
    with _LOCK:
        key=(pair,cause)
        if now-_SPOT_DIAG_LAST.get(key,-1e20)<_SPOT_DIAG_INTERVAL_SEC:
            return
        _SPOT_DIAG_LAST[key]=now
        if len(_SPOT_DIAG_LAST)>120:
            cutoff=now-_SPOT_DIAG_INTERVAL_SEC
            for k,at in list(_SPOT_DIAG_LAST.items()):
                if at<cutoff:
                    _SPOT_DIAG_LAST.pop(k,None)
    print("[POST_PUMP_SPOT_NOT_READY]","pair=",pair,"cause=",cause,
          "no_short_approval=true",flush=True)


def _refresh_spot_universe(now=None,request_get=None):
    """Return (available_pairs,status) using a bounded cached OKX instrument list.

    Missing contracts are SPOT_UNAVAILABLE. A transport/schema failure is
    SPOT_DATA_ERROR and must never be converted into a bearish/neutral vote.
    """
    global _SPOT_UNIVERSE,_SPOT_UNIVERSE_AT,_SPOT_UNIVERSE_STATUS
    now=time.time() if now is None else float(now)
    with _LOCK:
        if _SPOT_UNIVERSE and 0<=now-_SPOT_UNIVERSE_AT<=SPOT_UNIVERSE_TTL_SEC:
            return set(_SPOT_UNIVERSE),_SPOT_UNIVERSE_STATUS
    if request_get is None:
        try:
            import requests
            request_get=requests.get
        except ImportError:
            request_get=None
    if request_get is None:
        with _LOCK:
            if _SPOT_UNIVERSE:
                _SPOT_UNIVERSE_STATUS="STALE_CACHE"
                return set(_SPOT_UNIVERSE),_SPOT_UNIVERSE_STATUS
            _SPOT_UNIVERSE_STATUS="SPOT_DATA_ERROR"
            return set(),_SPOT_UNIVERSE_STATUS
    try:
        response=request_get(_SPOT_INSTRUMENTS_URL,
                             params={"instType":"SPOT"},timeout=6)
        if getattr(response,"status_code",None)!=200:
            raise RuntimeError("HTTP_"+str(getattr(response,"status_code","?")))
        raw=response.json()
        rows=raw.get("data") if isinstance(raw,dict) and raw.get("code")=="0" else None
        if not isinstance(rows,list):
            raise ValueError("BAD_SPOT_INSTRUMENT_SCHEMA")
        universe={str(x.get("instId")) for x in rows if isinstance(x,dict)
                  and x.get("state","live")=="live"
                  and isinstance(x.get("instId"),str)
                  and x.get("instId").endswith("-USDT")}
        if not universe:
            raise ValueError("EMPTY_SPOT_UNIVERSE")
        with _LOCK:
            _SPOT_UNIVERSE=universe
            _SPOT_UNIVERSE_AT=now
            _SPOT_UNIVERSE_STATUS="READY"
        return set(universe),"READY"
    except Exception as exc:
        with _LOCK:
            if _SPOT_UNIVERSE:
                _SPOT_UNIVERSE_STATUS="STALE_CACHE"
                print("[POST_PUMP_SPOT_UNIVERSE_STALE]",
                      type(exc).__name__,flush=True)
                return set(_SPOT_UNIVERSE),"STALE_CACHE"
            _SPOT_UNIVERSE_STATUS="SPOT_DATA_ERROR"
        print("[POST_PUMP_SPOT_UNIVERSE_ERROR]",type(exc).__name__,flush=True)
        return set(),"SPOT_DATA_ERROR"


def spot_availability(pair,now=None,request_get=None):
    universe,status=_refresh_spot_universe(now=now,request_get=request_get)
    if not universe and status=="SPOT_DATA_ERROR":
        return {"available":False,"reason":"SPOT_DATA_ERROR"}
    if pair not in universe:
        return {"available":False,"reason":"SPOT_UNAVAILABLE"}
    return {"available":True,"reason":"SPOT_AVAILABLE",
            "universe_status":status}


def watch(symbols):
    global _watched,_thread,_lease_at
    # Preserve continuity: rotating three subscriptions every 60s would
    # prevent ANY 5-minute flow window from ever warming up.
    raw_candidates=tuple(dict.fromkeys(
        s[:-4]+"-USDT" for s in (symbols or ())
        if isinstance(s,str) and s.endswith("USDT") and len(s)>4))
    now=time.time()
    universe,universe_status=_refresh_spot_universe(now=now)
    candidates=tuple(x for x in raw_candidates if x in universe)
    with _LOCK:
        old=_watched
        available=set(candidates)
        if not universe and universe_status=="SPOT_DATA_ERROR" and old:
            # A temporary instrument-list failure must not rotate a healthy,
            # already validated subscription. Keep the old set until refresh.
            candidates=old
            available=set(old)
        # Keep current valid subscriptions for their full lease even when
        # candidate ranking changes. A prematurely rotated feed cannot
        # accumulate the required continuous 5m Spot trade window.
        # Drop only symbols that the verified instrument universe no longer
        # lists; never continue ingesting a delisted pair.
        if old and now-_lease_at<MIN_STREAM_TENURE_SEC:
            retained=(old if universe_status=='SPOT_DATA_ERROR' and not universe
                      else tuple(p for p in old if p in universe))
            additions=tuple(p for p in candidates if p not in retained)
            target=(retained+additions)[:3]
        else:
            target=tuple(candidates[:3])
        if target!=old:
            _watched=target
            _lease_at=now
        if _thread is None or not _thread.is_alive():
            _thread=threading.Thread(target=_worker,daemon=True,
                                     name="post-pump-spot-flow")
            _thread.start()
        if _socket is not None and _socket_ready:
            try:
                _sync_subscriptions(_socket)
            except Exception:
                # Uncertain subscription state: fail closed and reconnect.
                mark_stream_disconnected("spot")
                _socket.close()
    return target


def _sync_subscriptions(ws):
    """Caller holds _LOCK. Retained instruments keep their accumulated trades."""
    target=set(_watched)
    removed=_subscribed-target
    added=target-_subscribed
    for pair in removed | added:
        reset_symbol("spot", pair)
    for op, pairs in (("unsubscribe",removed),("subscribe",added)):
        if pairs:
            ws.send(json.dumps({"op":op,"args":[
                {"channel":"trades","instId":p} for p in sorted(pairs)]}))
    _subscribed.clear()
    _subscribed.update(target)
    if added or removed:
        print("[POST_PUMP_SPOT_SUB_CHANGE]",
              "added=", ",".join(sorted(added)) or "NONE",
              "removed=", ",".join(sorted(removed)) or "NONE",
              "retained=", ",".join(sorted(target-added)) or "NONE",flush=True)


def _message(message):
    try:
        data=json.loads(message)
    except (ValueError,TypeError):
        return
    if not isinstance(data,dict):
        return
    if "event" in data:
        if data.get("event")=="error":
            print("[POST_PUMP_SPOT_WS_EVENT]",str(data.get("msg",""))[:90],
                  flush=True)
        return
    arg=data.get("arg") or {}
    pair=arg.get("instId")
    if not pair or arg.get("channel")!="trades":
        return
    with _LOCK:
        if pair not in _watched:
            return
    mark_stream_activity("spot")
    for item in data.get("data",[]):
        try:
            ts=float(item.get("ts"))/1000
            px=float(item.get("px"))
            size=float(item.get("sz"))
            side=item.get("side","").upper()
            if (not all(math.isfinite(x) and x>0 for x in (ts,px,size))
                    or side not in ("BUY","SELL")):
                continue
            save_trade(symbol=pair,market="spot",side=side,
                       price=px,size=size,event_ts=ts,
                       quote_value=px*size)
        except (ValueError,TypeError,OverflowError):
            continue


def _worker():
    global _socket, _socket_ready
    try:
        import websocket
    except ImportError:
        print("[POST_PUMP_SPOT_STREAM] websocket_missing",flush=True)
        return
    failures=0
    while True:
        with _LOCK:
            pairs=_watched
        if not pairs:
            mark_stream_disconnected("spot")
            failures=0
            time.sleep(15)
            continue
        session_started=None
        close_info={"code":None,"reason":""}
        def on_open(ws):
            global _socket_ready
            nonlocal session_started
            with _LOCK:
                mark_stream_connected("spot")
                _subscribed.clear()
                _sync_subscriptions(ws)
                _socket_ready=True
                session_started=time.monotonic()
            print("[POST_PUMP_SPOT_WS_SUB]",len(_subscribed),flush=True)
        def on_message(ws,msg):
            with _LOCK:
                if ws is _socket:
                    _message(msg)
        def on_close(ws,code,msg):
            close_info["code"]=code
            close_info["reason"]=str(msg or "")[:120]
            mark_stream_disconnected("spot")
        try:
            ws=websocket.WebSocketApp(
                _WS,on_open=on_open,on_message=on_message,on_close=on_close,
                on_error=lambda ws,e:print(
                    "[POST_PUMP_SPOT_WS_ERROR]",type(e).__name__,flush=True))
            with _LOCK:
                _socket=ws
                _socket_ready=False
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as exc:
            print("[POST_PUMP_SPOT_WS_ERROR]",type(exc).__name__,flush=True)
        finally:
            with _LOCK:
                if _socket is ws:
                    _socket=None
                    _socket_ready=False
                    _subscribed.clear()
            mark_stream_disconnected("spot")
        uptime=(max(0.0,time.monotonic()-session_started)
                if session_started is not None else 0.0)
        # Real disconnections reset the 5m windows; short repeated failures
        # are rate-limited, never mistaken for completed market observations.
        failures=0 if uptime>=300 else min(failures+1,5)
        delay=_reconnect_delay(failures)
        print("[POST_PUMP_SPOT_WS_RECONNECT]",
              "uptime_sec=",round(uptime,1),
              "close_code=",close_info["code"],
              "close_reason=",repr(close_info["reason"]),
              "consecutive_short_sessions=",failures,
              "delay_sec=",delay,flush=True)
        time.sleep(delay)


def verified(symbol):
    """5-minute *streamed* taker imbalance, not an unsupported CVD assertion."""
    pair=(symbol[:-4]+"-USDT" if isinstance(symbol,str) and symbol.endswith("USDT")
          else str(symbol or ""))
    availability=spot_availability(pair)
    if not availability.get("available"):
        return {"ready":False,"reason":availability.get("reason","SPOT_DATA_ERROR")}
    try:
        item=(get_spot_windows(symbol) or {}).get("5m") or {}
        age=item.get("stream_stale_seconds")
        count=item.get("trade_count")
        total=item.get("total_quote")
        symbol_age=item.get("last_trade_age_sec")
        value=item.get("imbalance_pct")
        if (item.get("quality")!="READY"
            or item.get("window_ready") is not True
            or item.get("stream_healthy") is not True
            or age is None or not 0<=age<=30
            or symbol_age is None or not 0<=symbol_age<=60
            or count is None or count<15
            or total is None or total<1000
            or value is None or not math.isfinite(float(value))):
            cause=_spot_gap_reason(item)
            _log_spot_gap(pair,cause)
            return {"ready":False,"reason":"SPOT_WS_NOT_READY",
                    "diagnostic":cause}
        return {"ready":True,"reason":"OKX_SPOT_WS_5M",
                "imbalance_pct":float(value),
                "delta_quote":float(item.get("delta_quote") or 0),
                "count":count}
    except (ValueError,TypeError,OverflowError):
        return {"ready":False,"reason":"SPOT_WS_BAD_DATA"}
