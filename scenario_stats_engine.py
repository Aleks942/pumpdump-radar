import json
import os
import time
import threading


STATS_FILE = os.getenv(
    "SCENARIO_STATS_FILE",
    "scenario_stats.json"
)

CHECKPOINTS = {
    "15m": 15 * 60,
    "30m": 30 * 60,
    "60m": 60 * 60,
}

FLAT_THRESHOLD_PCT = float(
    os.getenv("STATS_FLAT_THRESHOLD_PCT", 0.25)
)

_stats_lock = threading.Lock()


def _load_records():
    if not os.path.exists(STATS_FILE):
        return []

    try:
        with open(STATS_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list):
            return data

    except Exception as error:
        print(
            "[SCENARIO_STATS_LOAD_ERROR]",
            error,
            flush=True
        )

    return []


def _save_records(records):
    temp_file = f"{STATS_FILE}.tmp"

    try:
        with open(temp_file, "w", encoding="utf-8") as file:
            json.dump(
                records,
                file,
                ensure_ascii=False,
                indent=2
            )

        os.replace(temp_file, STATS_FILE)

    except Exception as error:
        print(
            "[SCENARIO_STATS_SAVE_ERROR]",
            error,
            flush=True
        )


def _expected_direction(move_type, scenario_bias):
    """
    Возвращает ожидаемое направление цены.

    +1 = ожидается рост
    -1 = ожидается падение
     0 = сценарий WAIT, направления нет
    """

    if scenario_bias == "WAIT":
        return 0

    if scenario_bias == "CONTINUE":
        return 1 if move_type == "PUMP" else -1

    if scenario_bias == "CORRECTION":
        return -1 if move_type == "PUMP" else 1

    return 0


def _classify_result(expected_move_pct):
    if expected_move_pct >= FLAT_THRESHOLD_PCT:
        return "WIN"

    if expected_move_pct <= -FLAT_THRESHOLD_PCT:
        return "LOSS"

    return "FLAT"


def register_scenario_signal(signal):
    """
    Регистрирует новый сигнал для проверки через 15/30/60 минут.
    """

    scenario = signal.get("money_scenario") or {}

    symbol = signal.get("symbol")
    entry_price = signal.get("price")
    move_type = signal.get("type")

    if not symbol or not entry_price or not move_type:
        print(
            "[SCENARIO_STATS_SKIP]",
            "signal data incomplete",
            symbol,
            flush=True
        )
        return None

    try:
        entry_price = float(entry_price)
    except (TypeError, ValueError):
        return None

    if entry_price <= 0:
        return None

    timestamp = time.time()

    record_id = (
        f"{symbol}_"
        f"{move_type}_"
        f"{int(timestamp * 1000)}"
    )

    scenario_bias = scenario.get("bias", "WAIT")

    record = {
        "id": record_id,
        "symbol": symbol,
        "created_at": timestamp,

        "entry_price": entry_price,
        "trigger_change_pct": signal.get("change", 0),
        "window": signal.get("window"),

        "move_type": move_type,

        "scenario_name": scenario.get(
            "name",
            "UNKNOWN"
        ),
        "scenario_title": scenario.get(
            "title",
            "Неизвестный сценарий"
        ),
        "scenario_bias": scenario_bias,
        "scenario_strength": scenario.get(
            "strength",
            0
        ),

        "expected_direction": _expected_direction(
            move_type,
            scenario_bias
        ),

        "decision_stage": (
            signal.get("decision") or {}
        ).get("stage"),

        "decision_action": (
            signal.get("decision") or {}
        ).get("action"),

        "decision_confidence": (
            signal.get("decision") or {}
        ).get("confidence"),

        "oi_change": signal.get("oi_change"),
        "funding": signal.get("funding"),

        "spot_state": (
            signal.get("spot_cvd") or {}
        ).get("state"),

        "spot_cvd_percent": (
            signal.get("spot_cvd") or {}
        ).get("cvd_percent"),

        "pressure": (
            signal.get("money") or {}
        ).get("pressure"),

        "votes": (
            signal.get("decision") or {}
        ).get("votes", []),

        "max_expected_profit_pct": 0.0,
        "max_expected_drawdown_pct": 0.0,

        "checks": {
            checkpoint: None
            for checkpoint in CHECKPOINTS
        },

        "completed": False,
    }

    with _stats_lock:
        records = _load_records()
        records.append(record)
        _save_records(records)

    print(
        "[SCENARIO_STATS_REGISTER]",
        symbol,
        "scenario=",
        record["scenario_name"],
        "bias=",
        scenario_bias,
        "entry=",
        entry_price,
        flush=True
    )

    return record_id


def update_scenario_results(price_map):
    """
    price_map должен выглядеть так:

    {
        "BTCUSDT": 65000.0,
        "ETHUSDT": 3500.0
    }
    """

    if not price_map:
        return

    now = time.time()
    updated = False

    with _stats_lock:
        records = _load_records()

        for record in records:

            if record.get("completed"):
                continue

            symbol = record.get("symbol")
            current_price = price_map.get(symbol)

            if current_price is None:
                continue

            try:
                current_price = float(current_price)
                entry_price = float(record["entry_price"])
            except (TypeError, ValueError, KeyError):
                continue

            if current_price <= 0 or entry_price <= 0:
                continue

            raw_move_pct = (
                (current_price - entry_price)
                / entry_price
            ) * 100

            expected_direction = record.get(
                "expected_direction",
                0
            )

            expected_move_pct = (
                raw_move_pct
                * expected_direction
            )

            if expected_direction != 0:

                record["max_expected_profit_pct"] = max(
                    record.get(
                        "max_expected_profit_pct",
                        0
                    ),
                    expected_move_pct
                )

                record["max_expected_drawdown_pct"] = min(
                    record.get(
                        "max_expected_drawdown_pct",
                        0
                    ),
                    expected_move_pct
                )

            age_seconds = (
                now
                - record.get("created_at", now)
            )

            checks = record.get("checks", {})

            for checkpoint, delay in CHECKPOINTS.items():

                if checks.get(checkpoint) is not None:
                    continue

                if age_seconds < delay:
                    continue

                result = {
                    "checked_at": now,
                    "price": current_price,
                    "raw_move_pct": round(
                        raw_move_pct,
                        4
                    ),
                    "expected_move_pct": round(
                        expected_move_pct,
                        4
                    ),
                    "result": (
                        _classify_result(
                            expected_move_pct
                        )
                        if expected_direction != 0
                        else "NO_DIRECTION"
                    ),
                    "max_profit_pct": round(
                        record.get(
                            "max_expected_profit_pct",
                            0
                        ),
                        4
                    ),
                    "max_drawdown_pct": round(
                        record.get(
                            "max_expected_drawdown_pct",
                            0
                        ),
                        4
                    ),
                }

                checks[checkpoint] = result
                updated = True

                print(
                    "[SCENARIO_STATS_CHECK]",
                    symbol,
                    checkpoint,
                    "scenario=",
                    record.get("scenario_name"),
                    "result=",
                    result["result"],
                    "move=",
                    result["expected_move_pct"],
                    flush=True
                )

            record["checks"] = checks

            if all(
                checks.get(checkpoint) is not None
                for checkpoint in CHECKPOINTS
            ):
                record["completed"] = True
                updated = True

        if updated:
            _save_records(records)
