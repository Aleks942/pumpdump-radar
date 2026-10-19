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
    mark_stream_activity, get_spot_windows,
)

_LOCK=threading.RLock()
_watched=()
_socket=None
_thread=None
_WS="wss://ws.okx.com:8443/ws/v5/public"


def watch(symbols):
    global _watched,_thread
    target=tuple(sorted({s[:-4]+"-USDT" for s in (symbols or ())
                         if isinstance(s,str) and s.endswith("USDT")
                         and len(s)>4}))[:3]
    with _LOCK:
        old=_watched
        _watched=target
        if _thread is None or not _thread.is_alive():
            _thread=threading.Thread(target=_worker,daemon=True,
                                     name="post-pump-spot-flow")
            _thread.start()
        if old!=target and _socket is not None:
            try:
                _socket.close()
            except Exception:
                pass
    return target


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
    global _socket
    try:
        import websocket
    except ImportError:
        print("[POST_PUMP_SPOT_STREAM] websocket_missing",flush=True)
        return
    while True:
        with _LOCK:
            pairs=_watched
        if not pairs:
            mark_stream_disconnected("spot")
            time.sleep(15)
            continue
        def on_open(ws):
            mark_stream_connected("spot")
            ws.send(json.dumps({"op":"subscribe","args":[
                {"channel":"trades","instId":s} for s in pairs]}))
            print("[POST_PUMP_SPOT_WS_SUB]",len(pairs),flush=True)
        def on_message(ws,msg):
            _message(msg)
        def on_close(ws,code,msg):
            mark_stream_disconnected("spot")
        try:
            ws=websocket.WebSocketApp(
                _WS,on_open=on_open,on_message=on_message,on_close=on_close,
                on_error=lambda ws,e:print(
                    "[POST_PUMP_SPOT_WS_ERROR]",type(e).__name__,flush=True))
            with _LOCK:
                _socket=ws
            ws.run_forever(ping_interval=20,ping_timeout=10)
        except Exception as exc:
            print("[POST_PUMP_SPOT_WS_ERROR]",type(exc).__name__,flush=True)
        finally:
            with _LOCK:
                if _socket is ws:
                    _socket=None
            mark_stream_disconnected("spot")
        time.sleep(5)


def verified(symbol):
    """5-minute *streamed* taker imbalance, not an unsupported CVD assertion."""
    try:
        item=(get_spot_windows(symbol) or {}).get("5m") or {}
        age=item.get("stream_stale_seconds")
        count=item.get("trade_count")
        total=item.get("total_quote")
        value=item.get("imbalance_pct")
        if (item.get("quality")!="READY"
            or item.get("window_ready") is not True
            or item.get("stream_healthy") is not True
            or age is None or not 0<=age<=30
            or count is None or count<15
            or total is None or total<1000
            or value is None or not math.isfinite(float(value))):
            return {"ready":False,"reason":"SPOT_WS_NOT_READY"}
        return {"ready":True,"reason":"OKX_SPOT_WS_5M",
                "imbalance_pct":float(value),
                "delta_quote":float(item.get("delta_quote") or 0),
                "count":count}
    except (ValueError,TypeError,OverflowError):
        return {"ready":False,"reason":"SPOT_WS_BAD_DATA"}
