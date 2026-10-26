import math
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests


VERSION = "2026-09-30-v1"
EXCHANGES = ("OKX", "Binance", "Bybit")

MAX_AGE = 120
MAX_GAP = 150
WINDOW_TOLERANCE = 35
MAX_SKEW = 45

_lock = threading.RLock()
_start_lock = threading.Lock()
_rate_lock = threading.Lock()
_local = threading.local()

_started = False
_watched = set()
_maps = {}
_meta_at = {}
_history = {}
_errors = {}
_blocked = {}
_next_request = 0.0


def _number(value):
    value = float(value)

    if not math.isfinite(value) or value < 0:
        raise ValueError("invalid nonnegative number")

    return value


def _json(exchange, url, params=None):
    global _next_request

    with _lock:
        if time.monotonic() < _blocked.get(exchange, 0):
            raise RuntimeError("API_BACKOFF")

    if exchange == "Binance":
        with _rate_lock:
            time.sleep(max(0, _next_request - time.monotonic()))
            _next_request = time.monotonic() + 0.15

    if not hasattr(_local, "session"):
        _local.session = requests.Session()

    response = _local.session.get(
        url,
        params=params,
        timeout=(5, 10),
    )

    if response.status_code in (403, 418, 429, 451):
        try:
            delay = max(
                300,
                float(response.headers.get("Retry-After", 300)),
            )
        except (TypeError, ValueError):
            delay = 300

        with _lock:
            _blocked[exchange] = time.monotonic() + delay

        raise RuntimeError("HTTP_" + str(response.status_code))

    response.raise_for_status()
    data = response.json()

    if not isinstance(data, dict):
        raise ValueError("invalid API object")

    if exchange == "OKX" and str(data.get("code")) != "0":
        raise ValueError("OKX code=" + str(data.get("code")))

    if exchange == "Bybit" and data.get("retCode") != 0:
        raise ValueError("Bybit code=" + str(data.get("retCode")))

    if exchange == "Binance" and "code" in data:
        raise ValueError("Binance code=" + str(data["code"]))

    return data


def _load_map(exchange):
    result = {}

    if exchange == "OKX":
        data = _json(
            exchange,
            "https://www.okx.com/api/v5/public/instruments",
            {"instType": "SWAP"},
        )

        for row in data.get("data", []):
            raw = row.get("instId", "")
            base = raw.removesuffix("-USDT-SWAP")

            if (
                raw.endswith("-USDT-SWAP")
                and row.get("state") == "live"
                and row.get("ctType") == "linear"
                and row.get("settleCcy") == "USDT"
                and row.get("ctValCcy") == base
            ):
                result[base + "USDT"] = raw

    elif exchange == "Binance":
        data = _json(
            exchange,
            "https://fapi.binance.com/fapi/v1/exchangeInfo",
        )

        for row in data.get("symbols", []):
            symbol = row.get("symbol", "")

            if (
                row.get("contractType") == "PERPETUAL"
                and row.get("status") == "TRADING"
                and row.get("quoteAsset") == "USDT"
                and row.get("marginAsset") == "USDT"
                and symbol == str(row.get("baseAsset")) + "USDT"
            ):
                result[symbol] = symbol

    else:
        cursor = ""
        seen = set()

        for _ in range(100):
            data = _json(
                exchange,
                "https://api.bybit.com/v5/market/instruments-info",
                {
                    "category": "linear",
                    "limit": 1000,
                    "cursor": cursor,
                },
            )

            page = data.get("result") or {}

            for row in page.get("list", []):
                symbol = row.get("symbol", "")

                if (
                    row.get("contractType") == "LinearPerpetual"
                    and row.get("status") == "Trading"
                    and row.get("quoteCoin") == "USDT"
                    and row.get("settleCoin") == "USDT"
                    and symbol == str(row.get("baseCoin")) + "USDT"
                ):
                    result[symbol] = symbol

            cursor = page.get("nextPageCursor") or ""

            if not cursor:
                break

            if cursor in seen:
                raise ValueError("repeated instrument cursor")

            seen.add(cursor)

        else:
            raise ValueError("too many instrument pages")

    if not result:
        raise ValueError("empty instrument map")

    return result


def _store(exchange, symbol, ts, value):
    now = time.time()
    ts = _number(ts)
    value = _number(value)

    if not -5 <= now - ts <= MAX_AGE:
        raise ValueError("STALE_TIMESTAMP")

    with _lock:
        points = _history.setdefault(
            (exchange, symbol),
            deque(maxlen=160),
        )

        if points and ts < points[-1][0]:
            raise ValueError("OUT_OF_ORDER")

        if points and ts == points[-1][0]:
            if value != points[-1][1]:
                raise ValueError("CONFLICTING_TIMESTAMP")

            _errors.pop((exchange, symbol), None)
            return

        if points and ts - points[-1][0] > MAX_GAP:
            points.clear()

        points.append((ts, value))
        _errors.pop((exchange, symbol), None)


def _bybit_value(row):
    single = _number(row["singleOpenInterest"])
    both = _number(row["openInterest"])

    if not math.isclose(
        both,
        2 * single,
        rel_tol=0.001,
        abs_tol=0.01,
    ):
        raise ValueError("BYBIT_COUNTING_MISMATCH")

    return single


def _failure(exchange, symbol, error):
    with _lock:
        _errors[(exchange, symbol)] = (
            type(error).__name__ + ": " + str(error)
        )[:160]


def _binance_one(symbol):
    try:
        data = _json(
            "Binance",
            "https://fapi.binance.com/fapi/v1/openInterest",
            {"symbol": symbol},
        )

        if data.get("symbol") != symbol:
            raise ValueError("SYMBOL_MISMATCH")

        _store(
            "Binance",
            symbol,
            float(data["time"]) / 1000,
            data["openInterest"],
        )

        return True

    except Exception as error:
        _failure("Binance", symbol, error)
        return False


def _collect(exchange, mapping, symbols, pool):
    if exchange == "Binance":
        return sum(pool.map(_binance_one, symbols))

    if exchange == "OKX":
        data = _json(
            exchange,
            "https://www.okx.com/api/v5/public/open-interest",
            {"instType": "SWAP"},
        )

        rows = {
            row["instId"]: row
            for row in data.get("data", [])
        }

    else:
        data = _json(
            exchange,
            "https://api.bybit.com/v5/market/tickers",
            {"category": "linear"},
        )

        rows = {
            row["symbol"]: row
            for row in (data.get("result") or {}).get("list", [])
        }

    saved = 0

    for symbol in symbols:
        try:
            row = rows[mapping[symbol]]

            if exchange == "OKX":
                ts = float(row["ts"]) / 1000
                value = row["oiCcy"]
            else:
                ts = float(data["time"]) / 1000
                value = _bybit_value(row)

            _store(exchange, symbol, ts, value)
            saved += 1

        except Exception as error:
            _failure(exchange, symbol, error)

    return saved


def _worker(exchange):
    with ThreadPoolExecutor(max_workers=4) as pool:
        while True:
            began = time.monotonic()

            try:
                with _lock:
                    refresh = (
                        time.time() - _meta_at.get(exchange, 0) > 3600
                    )

                if refresh:
                    mapping = _load_map(exchange)

                    with _lock:
                        old = _maps.get(exchange, {})

                        for symbol in set(old) | set(mapping):
                            if old.get(symbol) != mapping.get(symbol):
                                _history.pop((exchange, symbol), None)

                        _maps[exchange] = mapping
                        _meta_at[exchange] = time.time()

                with _lock:
                    mapping = dict(_maps[exchange])

                    # Collect every watched symbol that exists on this
                    # exchange. Consensus accepts 3/3, or 2/2 only when the
                    # third exchange is confirmed UNSUPPORTED. Errors,
                    # warming/stale data and disagreement still fail closed.
                    symbols = sorted(
                        _watched
                        & mapping.keys()
                    )

                saved = (
                    _collect(exchange, mapping, symbols, pool)
                    if symbols
                    else 0
                )

                print(
                    "[AGG_OI_CYCLE]",
                    exchange,
                    "saved=", saved,
                    "requested=", len(symbols),
                    flush=True,
                )

            except Exception as error:
                with _lock:
                    for symbol in _watched:
                        _failure(exchange, symbol, error)

                print(
                    "[AGG_OI_ERROR]",
                    exchange,
                    str(error),
                    flush=True,
                )

            interval = 60 if exchange == "Binance" else 30

            time.sleep(
                max(1, interval - (time.monotonic() - began))
            )


def watch_symbol(symbol):
    global _started

    if not isinstance(symbol, str) or not symbol.endswith("USDT"):
        return

    with _lock:
        _watched.add(symbol)

    with _start_lock:
        if not _started:
            for exchange in EXCHANGES:
                threading.Thread(
                    target=_worker,
                    args=(exchange,),
                    name="agg-oi-" + exchange,
                    daemon=True,
                ).start()

            _started = True

            print(
                "[AGG_OI_STARTED]",
                VERSION,
                flush=True,
            )


def _source_window(points, seconds, now):
    if not points:
        return {"quality": "WARMING"}

    end_ts, current = points[-1]

    result = {
        "quality": "WARMING",
        "oi_coin": current,
        "end_ts": end_ts,
        "age_sec": now - end_ts,
    }

    if not -5 <= now - end_ts <= MAX_AGE:
        result["quality"] = "STALE"
        return result

    target = end_ts - seconds

    baseline = min(
        points,
        key=lambda point: abs(point[0] - target),
    )

    start_ts, previous = baseline

    if (
        points[0][0] > target
        or abs(start_ts - target) > WINDOW_TOLERANCE
    ):
        return result

    if previous <= 0:
        result["quality"] = "ZERO_BASELINE"
        return result

    result.update(
        quality="READY",
        start_ts=start_ts,
        previous_coin=previous,
        period_sec=end_ts - start_ts,
        change_pct=(current / previous - 1) * 100,
    )

    return result


def get_aggregated_oi(symbol):
    now = time.time()

    result = {
        "version": VERSION,
        "unit": symbol[:-4],
        "observed_ts": now,
        "scope": "USDT perpetual; exact base symbols",
        "windows": {},
    }

    with _lock:
        for label, seconds in (("5m", 300), ("30m", 1800)):
            sources = {}

            for exchange in EXCHANGES:
                if now - _meta_at.get(exchange, 0) > 7200:
                    item = {
                        "quality": "METADATA_UNAVAILABLE",
                    }

                elif symbol not in _maps.get(exchange, {}):
                    item = {
                        "quality": "UNSUPPORTED",
                    }

                elif (exchange, symbol) in _errors:
                    item = {
                        "quality": "ERROR",
                        "error": _errors[(exchange, symbol)],
                    }

                else:
                    item = _source_window(
                        list(
                            _history.get(
                                (exchange, symbol),
                                (),
                            )
                        ),
                        seconds,
                        now,
                    )

                sources[exchange] = item

            ready = [
                value
                for value in sources.values()
                if value["quality"] == "READY"
            ]

            window = {
                "ready": False,
                "quality": "INCOMPLETE",
                "sources": sources,
                "ready_sources": len(ready),
                "change_pct": None,
            }

            if len(ready) == 3:
                end_skew = (
                    max(value["end_ts"] for value in ready)
                    - min(value["end_ts"] for value in ready)
                )

                start_skew = (
                    max(value["start_ts"] for value in ready)
                    - min(value["start_ts"] for value in ready)
                )

                window.update(
                    end_skew_sec=end_skew,
                    start_skew_sec=start_skew,
                )

                if max(end_skew, start_skew) <= MAX_SKEW:
                    current = math.fsum(
                        value["oi_coin"]
                        for value in ready
                    )

                    previous = math.fsum(
                        value["previous_coin"]
                        for value in ready
                    )

                    window.update(
                        ready=True,
                        quality="READY",
                        oi_coin=current,
                        previous_coin=previous,
                        change_pct=(current / previous - 1) * 100,
                    )

                else:
                    window["quality"] = "TIME_MISMATCH"

            result["windows"][label] = window

    return result


def format_aggregated_oi(data):
    if not isinstance(data, dict):
        return "OI трёх бирж: нет данных"

    lines = [
        "OI OKX + Binance + Bybit (USDT-контракты):"
    ]

    statuses = {
        "WARMING": "накопление",
        "STALE": "устарело",
        "ERROR": "ошибка API",
        "UNSUPPORTED": "нет сопоставимого контракта",
        "METADATA_UNAVAILABLE": "список контрактов недоступен",
        "ZERO_BASELINE": "нулевая база",
    }

    for key, title in (("5m", "≈5м"), ("30m", "≈30м")):
        window = data.get("windows", {}).get(key, {})

        if window.get("ready"):
            lines.append(
                f"{title}: {window['change_pct']:+.2f}% · 3/3 биржи"
            )

        else:
            if window.get("quality") == "TIME_MISMATCH":
                note = "время замеров не совпало"
            else:
                note = (
                    f"готово {window.get('ready_sources', 0)}/3 бирж"
                )

            lines.append(
                f"{title}: нет общего расчёта · {note}"
            )

        details = []

        for exchange, item in window.get("sources", {}).items():
            if item.get("quality") == "READY":
                value = f"{item['change_pct']:+.2f}%"

            else:
                value = statuses.get(
                    item.get("quality"),
                    "нет данных",
                )

            details.append(exchange + " " + value)

        if details:
            lines.append(" / ".join(details))

    return "\n".join(lines)

# OI consensus/alignment extension.
VERSION = "2026-10-26-consensus-v3"
_get_latest_oi_v1 = get_aggregated_oi


def _contract_sets(symbol, now):
    known = []
    supported = []
    unsupported = []

    for exchange in EXCHANGES:
        if now - _meta_at.get(exchange, 0) > 7200:
            continue

        known.append(exchange)
        if symbol in _maps.get(exchange, {}):
            supported.append(exchange)
        else:
            unsupported.append(exchange)

    return known, supported, unsupported


def _direction_consensus(items):
    changes = []

    for item in items:
        if item.get("quality") != "READY":
            return "UNKNOWN"

        try:
            value = float(item.get("change_pct"))
        except (TypeError, ValueError, OverflowError):
            return "UNKNOWN"

        if not math.isfinite(value):
            return "UNKNOWN"

        changes.append(value)

    if not changes:
        return "UNKNOWN"

    if all(value > 0 for value in changes):
        return "EXPANSION"

    if all(value < 0 for value in changes):
        return "CONTRACTION"

    return "MIXED"


def _select_consensus_sources(symbol, exchanges, seconds, now, latest):
    selected = {
        exchange: latest.get(exchange, {})
        for exchange in exchanges
    }

    if not all(
        item.get("quality") == "READY"
        for item in selected.values()
    ):
        return None, "INCOMPLETE", "LATEST"

    ends = [item["end_ts"] for item in selected.values()]
    starts = [item["start_ts"] for item in selected.values()]
    end_skew = max(ends) - min(ends)
    start_skew = max(starts) - min(starts)

    if max(end_skew, start_skew) <= MAX_SKEW:
        return selected, None, "LATEST"

    anchor = min(ends)
    aligned = {}

    for exchange in exchanges:
        points = [
            point
            for point in _history.get((exchange, symbol), ())
            if point[0] <= anchor
        ]
        aligned[exchange] = _source_window(points, seconds, now)

    if not all(
        item.get("quality") == "READY"
        for item in aligned.values()
    ):
        return None, "TIME_MISMATCH", "COMMON_PAST"

    ends = [item["end_ts"] for item in aligned.values()]
    starts = [item["start_ts"] for item in aligned.values()]

    if max(max(ends) - min(ends), max(starts) - min(starts)) > MAX_SKEW:
        return None, "TIME_MISMATCH", "COMMON_PAST"

    return aligned, None, "COMMON_PAST"


def get_aggregated_oi(symbol):
    with _lock:
        result = _get_latest_oi_v1(symbol)
        now = result["observed_ts"]

        for label, seconds in (("5m", 300), ("30m", 1800)):
            window = result["windows"][label]
            latest_sources = dict(window.get("sources") or {})
            known, supported, unsupported = _contract_sets(symbol, now)

            ready_supported = sum(
                1
                for exchange in supported
                if latest_sources.get(exchange, {}).get("quality") == "READY"
            )

            window.update(
                ready=False,
                quality="INCOMPLETE",
                change_pct=None,
                confidence="BLOCK",
                consensus_mode="BLOCK",
                direction_consensus="UNKNOWN",
                metadata_known_sources=len(known),
                supported_sources=len(supported),
                supported_exchanges=list(supported),
                unsupported_exchanges=list(unsupported),
                ready_sources=ready_supported,
                alignment_method="LATEST",
            )

            # We must know the contract universe on all three exchanges.
            # Unknown metadata is different from a confirmed absent contract.
            if len(known) != len(EXCHANGES):
                window["quality"] = "METADATA_UNAVAILABLE"
                continue

            # One exchange is never enough for a cross-exchange confirmation.
            if len(supported) < 2:
                window["quality"] = "INSUFFICIENT_SOURCES"
                continue

            selected, error, method = _select_consensus_sources(
                symbol,
                supported,
                seconds,
                now,
                latest_sources,
            )

            if selected is None:
                window["quality"] = error or "INCOMPLETE"
                window["alignment_method"] = method
                continue

            direction = _direction_consensus(selected.values())

            # Opposite OI direction on any supported exchange is a veto.
            if direction not in ("EXPANSION", "CONTRACTION"):
                window["quality"] = "DIRECTION_MISMATCH"
                window["direction_consensus"] = direction
                continue

            current = math.fsum(
                item["oi_coin"]
                for item in selected.values()
            )
            previous = math.fsum(
                item["previous_coin"]
                for item in selected.values()
            )

            if previous <= 0:
                window["quality"] = "ZERO_BASELINE"
                continue

            merged_sources = dict(latest_sources)
            merged_sources.update(selected)

            if len(supported) == 3:
                confidence = "HIGH"
                consensus_mode = "3/3"
            elif len(supported) == 2 and len(unsupported) == 1:
                confidence = "MEDIUM"
                consensus_mode = "2/2_SUPPORTED"
            else:
                # Defensive fail-closed branch for unexpected universe state.
                window["quality"] = "INCOMPLETE"
                continue

            ends = [item["end_ts"] for item in selected.values()]
            starts = [item["start_ts"] for item in selected.values()]

            window.update(
                ready=True,
                quality="READY",
                sources=merged_sources,
                latest_sources=latest_sources,
                ready_sources=len(selected),
                confidence=confidence,
                consensus_mode=consensus_mode,
                direction_consensus=direction,
                alignment_method=method,
                end_skew_sec=max(ends) - min(ends),
                start_skew_sec=max(starts) - min(starts),
                oldest_end_ts=min(ends),
                newest_end_ts=max(ends),
                age_sec=max(0.0, now - min(ends)),
                oi_coin=current,
                previous_coin=previous,
                change_pct=(current / previous - 1) * 100,
            )

        result["version"] = VERSION
        return result


def format_aggregated_oi(data):
    if not isinstance(data, dict):
        return "OI OKX + Binance + Bybit: нет данных"

    lines = ["OI OKX + Binance + Bybit (USDT-контракты):"]

    statuses = {
        "WARMING": "накопление",
        "STALE": "устарело",
        "ERROR": "ошибка API",
        "UNSUPPORTED": "контракта нет",
        "METADATA_UNAVAILABLE": "метаданные недоступны",
        "ZERO_BASELINE": "нулевая база",
        "TIME_MISMATCH": "время замеров не совпало",
        "INCOMPLETE": "неполные данные",
        "INSUFFICIENT_SOURCES": "меньше двух поддерживаемых бирж",
        "DIRECTION_MISMATCH": "биржи расходятся по направлению OI",
    }

    for key, title in (("5m", "≈5м"), ("30m", "≈30м")):
        window = data.get("windows", {}).get(key, {})

        if window.get("ready"):
            mode = window.get("consensus_mode") or "CONSENSUS"
            confidence = window.get("confidence") or "UNKNOWN"
            if mode == "2/2_SUPPORTED":
                mode_text = "2/2 доступных"
            else:
                mode_text = mode

            lines.append(
                f"{title}: {window['change_pct']:+.2f}% · "
                f"{confidence} · {mode_text}"
            )
        else:
            quality = window.get("quality") or "INCOMPLETE"
            note = statuses.get(quality, quality)
            ready = int(window.get("ready_sources") or 0)
            supported = int(window.get("supported_sources") or 0)
            lines.append(
                f"{title}: нет консенсуса · {note} · "
                f"готово {ready}/{supported} поддерживаемых"
            )

        details = []
        for exchange in EXCHANGES:
            item = (window.get("sources") or {}).get(exchange, {})
            if item.get("quality") == "READY":
                value = f"{item['change_pct']:+.2f}%"
            else:
                value = statuses.get(
                    item.get("quality"),
                    item.get("quality") or "нет данных",
                )
            details.append(exchange + " " + value)

        lines.append(" / ".join(details))

        age = window.get("age_sec")
        if window.get("ready") and age is not None:
            method = (
                "общее прошлое время"
                if window.get("alignment_method") == "COMMON_PAST"
                else "последние синхронные замеры"
            )
            lines.append(
                f"OI {key}: возраст до {math.ceil(age)} с; {method}"
            )

    return "\n".join(lines)
