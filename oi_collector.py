import math
import threading
import time
from collections import deque

import requests


history = {}
lock = threading.Lock()
start_lock = threading.Lock()
started = False


def store(symbol, ts, oi, now):
    if not all(math.isfinite(v) for v in (ts, oi)):
        return

    if oi <= 0 or not -5 <= now - ts <= 60:
        return

    with lock:
        points = history.setdefault(symbol, deque(maxlen=40))

        if points and ts <= points[-1][0]:
            return

        if points and ts - points[-1][0] > 75:
            points.clear()

        points.append((ts, oi))


def read(symbol, now):
    with lock:
        points = list(history.get(symbol, ()))

    result = {
        "ready": False,
        "quality": "WARMING",
        "oi": None,
        "change_pct": None,
        "period_sec": None,
        "age_sec": None,
    }

    if not points:
        return result

    end_ts, current = points[-1]
    age = now - end_ts
    result.update(oi=current, age_sec=age)

    if not -5 <= age <= 60:
        result["quality"] = "STALE"
        return result

    baseline = next(
        (
            p for p in reversed(points)
            if p[0] <= end_ts - 300
        ),
        None,
    )

    if baseline is None:
        return result

    start_ts, previous = baseline
    period = end_ts - start_ts
    result["period_sec"] = period

    if period > 335:
        result["quality"] = "WINDOW_MISMATCH"
        return result

    result.update(
        ready=True,
        quality="READY",
        change_pct=(current / previous - 1) * 100,
    )
    return result


def collect_once(session):
    response = session.get(
        "https://www.okx.com/api/v5/public/open-interest",
        params={"instType": "SWAP"},
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    if data.get("code") != "0":
        raise RuntimeError(
            f"OKX code={data.get('code')} msg={data.get('msg')}"
        )

    rows = data.get("data")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("Empty or invalid OI response")

    now = time.time()

    for row in rows:
        raw_symbol = row.get("instId", "")
        if not raw_symbol.endswith("-USDT-SWAP"):
            continue

        try:
            ts = float(row["ts"]) / 1000
            oi = float(row["oi"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue

        symbol = raw_symbol.replace("-USDT-SWAP", "USDT")
        store(symbol, ts, oi, now)

    with lock:
        fresh_symbols = sum(
            1 for points in history.values()
            if points and -5 <= now - points[-1][0] <= 60
        )

    print(
        "[OI_COLLECTOR]",
        "fresh_symbols=", fresh_symbols,
        flush=True,
    )


def worker():
    with requests.Session() as session:
        while True:
            cycle_started = time.monotonic()

            try:
                collect_once(session)
            except Exception as error:
                print(
                    "[OI_COLLECTOR_ERROR]",
                    type(error).__name__,
                    str(error),
                    flush=True,
                )

            elapsed = time.monotonic() - cycle_started
            time.sleep(max(1.0, 30.0 - elapsed))


def get_oi_5m(symbol):
    global started

    with start_lock:
        if not started:
            thread = threading.Thread(
                target=worker,
                name="oi-collector",
                daemon=True,
            )
            thread.start()
            started = True

    return read(symbol, time.time())
