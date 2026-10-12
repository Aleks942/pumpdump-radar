"""Bounded background bridge: send one confirmed PumpDump alert only."""
import os
import time
import threading
import requests

_slots = threading.BoundedSemaphore(2)


def send_confirmed(signal, send_message, format_message):
    """Fail closed; no Telegram message unless Flow confirms within TTL."""
    if not _slots.acquire(blocking=False):
        print("[PUMP_FLOW] BUSY", signal.get("symbol"), flush=True)
        return
    try:
        base = os.getenv("PUMP_FLOW_URL", "").strip()
        token = os.getenv("PUMP_FLOW_SHARED_TOKEN", "")
        if not base.startswith("https://") or not token or not base.endswith("/api/pump-candidate"):
            print("[PUMP_FLOW] NOT_CONFIGURED", flush=True)
            return
        symbol = str(signal.get("symbol") or "").upper()
        side = {"UP": "LONG", "DOWN": "SHORT"}.get(
            str((signal.get("decision") or {}).get("direction") or ""))
        if not side or not symbol.endswith("USDT"):
            return
        now = time.time()
        payload = {"schema": 1, "source": "pumpdump-radar",
                   "symbol": symbol, "side": side,
                   "price": float(signal["price"]), "observed_at": now,
                   "expires_at": now + 90}
        headers = {"X-Pump-Flow-Token": token}
        response = requests.post(base, json=payload, headers=headers, timeout=3)
        if response.status_code != 202:
            print("[PUMP_FLOW] REJECTED", symbol, response.status_code, flush=True)
            return
        result_url = base.rsplit("/", 1)[0] + "/pump-result"
        while time.time() - now < 70:
            time.sleep(3)
            result = requests.get(result_url, headers=headers,
                                  params={"symbol": symbol, "observed_at": now},
                                  timeout=3)
            if result.status_code != 200:
                return
            status = result.json().get("status")
            if status == "PENDING":
                continue
            if status == "CONFIRMED":
                text = format_message(signal)
                text += "\n\n🟢 Order Flow: подтверждено (Bybit, поток сделок)."
                send_message(text, symbol=symbol)
            else:
                print("[PUMP_FLOW] NO_ALERT", symbol, status, flush=True)
            return
        print("[PUMP_FLOW] TIMEOUT", symbol, flush=True)
    except Exception as exc:
        print("[PUMP_FLOW] ERROR", type(exc).__name__, flush=True)
    finally:
        _slots.release()


def dispatch(signal, send_message, format_message):
    threading.Thread(target=send_confirmed,
                     args=(dict(signal), send_message, format_message),
                     daemon=True).start()
