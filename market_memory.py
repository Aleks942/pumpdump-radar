import os
import sqlite3
import threading
import time
from contextlib import closing


DB_FILE = os.getenv(
    "MARKET_MEMORY_DB",
    "market_memory.db"
)

_db_lock = threading.Lock()


def get_connection():
    connection = sqlite3.connect(
        DB_FILE,
        timeout=30,
        check_same_thread=False
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA journal_mode=WAL"
    )

    connection.execute(
        "PRAGMA synchronous=NORMAL"
    )

    connection.execute(
        "PRAGMA busy_timeout=30000"
    )

    return connection


def initialize_market_memory():
    """
    Создаёт SQLite-базу и таблицу сигналов.
    Функцию можно безопасно вызывать при каждом запуске.
    """

    with _db_lock:

        try:

            with closing(get_connection()) as connection:

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS market_signals (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,

                        signal_key TEXT NOT NULL UNIQUE,

                        created_at REAL NOT NULL,
                        created_at_utc TEXT,

                        symbol TEXT NOT NULL,
                        move_type TEXT NOT NULL,
                        window_name TEXT,

                        entry_price REAL NOT NULL,
                        trigger_change_pct REAL,

                        scenario_name TEXT,
                        scenario_title TEXT,
                        scenario_bias TEXT,
                        scenario_strength INTEGER,

                        decision_stage TEXT,
                        decision_action TEXT,
                        decision_confidence REAL,

                        chief_score REAL,
                        continue_score REAL,
                        exhaustion_score REAL,

                        oi_change REAL,
                        funding REAL,

                        spot_available INTEGER DEFAULT 0,
                        spot_state TEXT,
                        spot_cvd_percent REAL,
                        spot_buy_volume REAL,
                        spot_sell_volume REAL,

                        money_state TEXT,
                        money_score REAL,
                        pressure TEXT,

                        long_liquidations REAL,
                        short_liquidations REAL,

                        trend_score REAL,

                        applied_rules TEXT,
                        explanation TEXT,

                        price_15m REAL,
                        move_15m_pct REAL,
                        expected_15m_pct REAL,
                        result_15m TEXT,
                        checked_15m_at REAL,

                        price_30m REAL,
                        move_30m_pct REAL,
                        expected_30m_pct REAL,
                        result_30m TEXT,
                        checked_30m_at REAL,

                        price_60m REAL,
                        move_60m_pct REAL,
                        expected_60m_pct REAL,
                        result_60m TEXT,
                        checked_60m_at REAL,

                        max_profit_pct REAL DEFAULT 0,
                        max_drawdown_pct REAL DEFAULT 0,

                        completed INTEGER DEFAULT 0
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_market_signals_symbol
                    ON market_signals(symbol)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_market_signals_scenario
                    ON market_signals(scenario_name)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_market_signals_created_at
                    ON market_signals(created_at)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_market_signals_completed
                    ON market_signals(completed)
                    """
                )

                connection.commit()

            print(
                "[MARKET_MEMORY_READY]",
                DB_FILE,
                flush=True
            )

            return True

        except Exception as error:

            print(
                "[MARKET_MEMORY_INIT_ERROR]",
                error,
                flush=True
            )

            return False


def market_memory_healthcheck():
    """
    Проверяет, что база открывается и таблица существует.
    """

    try:

        with closing(get_connection()) as connection:

            row = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                AND name = 'market_signals'
                """
            ).fetchone()

        if row:

            print(
                "[MARKET_MEMORY_HEALTH] OK",
                flush=True
            )

            return True

        print(
            "[MARKET_MEMORY_HEALTH] TABLE_NOT_FOUND",
            flush=True
        )

        return False

    except Exception as error:

        print(
            "[MARKET_MEMORY_HEALTH_ERROR]",
            error,
            flush=True
        )

        return False


def get_memory_status():
    """
    Возвращает краткую информацию о базе.
    Пока используется только для проверки.
    """

    try:

        with closing(get_connection()) as connection:

            total = connection.execute(
                """
                SELECT COUNT(*) AS total
                FROM market_signals
                """
            ).fetchone()["total"]

            pending = connection.execute(
                """
                SELECT COUNT(*) AS pending
                FROM market_signals
                WHERE completed = 0
                """
            ).fetchone()["pending"]

            completed = connection.execute(
                """
                SELECT COUNT(*) AS completed
                FROM market_signals
                WHERE completed = 1
                """
            ).fetchone()["completed"]

        return {
            "database": DB_FILE,
            "total": total,
            "pending": pending,
            "completed": completed,
        }

    except Exception as error:

        print(
            "[MARKET_MEMORY_STATUS_ERROR]",
            error,
            flush=True
        )

        return {
            "database": DB_FILE,
            "total": 0,
            "pending": 0,
            "completed": 0,
        }

def save_market_signal(signal):
    """
    Сохраняет отправленный Telegram-сигнал в SQLite.
    Повторный signal_key не записывается.
    """

    if not signal:
        return None

    symbol = signal.get("symbol")
    move_type = signal.get("type")
    entry_price = signal.get("price")
    window_name = signal.get("window")

    if not symbol or not move_type or entry_price is None:
        print(
            "[MARKET_MEMORY_SAVE_SKIP]",
            "incomplete signal",
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

    created_at = time.time()

    signal_key = (
        f"{symbol}_"
        f"{move_type}_"
        f"{window_name}_"
        f"{int(created_at * 1000)}"
    )

    scenario = signal.get("money_scenario") or {}
    decision = signal.get("decision") or {}
    spot = signal.get("spot_cvd") or {}
    money = signal.get("money") or {}
    liquidations = signal.get("liquidations") or {}
    trend = signal.get("trend_strength") or {}

    applied_rules = decision.get("applied_rules") or []
    explanation = decision.get("explanation") or []

    try:
        import json

        applied_rules_json = json.dumps(
            applied_rules,
            ensure_ascii=False
        )

        explanation_json = json.dumps(
            explanation,
            ensure_ascii=False
        )

    except Exception:
        applied_rules_json = "[]"
        explanation_json = "[]"

    with _db_lock:

        try:

            with closing(get_connection()) as connection:

                cursor = connection.execute(
                    """
                    INSERT INTO market_signals (
                        signal_key,
                        created_at,
                        created_at_utc,

                        symbol,
                        move_type,
                        window_name,

                        entry_price,
                        trigger_change_pct,

                        scenario_name,
                        scenario_title,
                        scenario_bias,
                        scenario_strength,

                        decision_stage,
                        decision_action,
                        decision_confidence,

                        chief_score,
                        continue_score,
                        exhaustion_score,

                        oi_change,
                        funding,

                        spot_available,
                        spot_state,
                        spot_cvd_percent,
                        spot_buy_volume,
                        spot_sell_volume,

                        money_state,
                        money_score,
                        pressure,

                        long_liquidations,
                        short_liquidations,

                        trend_score,

                        applied_rules,
                        explanation
                    )
                    VALUES (
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?,
                        ?, ?,
                        ?,
                        ?, ?
                    )
                    """,
                    (
                        signal_key,
                        created_at,
                        time.strftime(
                            "%Y-%m-%d %H:%M:%S",
                            time.gmtime(created_at)
                        ),

                        symbol,
                        move_type,
                        window_name,

                        entry_price,
                        signal.get("change"),

                        scenario.get("name"),
                        scenario.get("title"),
                        scenario.get("bias"),
                        scenario.get("strength"),

                        decision.get("stage"),
                        decision.get("action"),
                        decision.get("confidence"),

                        decision.get("score"),
                        decision.get("continue_score"),
                        decision.get("exhaustion_score"),

                        signal.get("oi_change"),
                        signal.get("funding"),

                        1 if spot.get("available") else 0,
                        spot.get("state"),
                        spot.get("cvd_percent"),
                        spot.get("buy_quote_volume"),
                        spot.get("sell_quote_volume"),

                        money.get("money_state"),
                        money.get("money_score"),
                        money.get("pressure"),

                        liquidations.get("long_liq", 0),
                        liquidations.get("short_liq", 0),

                        trend.get("score"),

                        applied_rules_json,
                        explanation_json,
                    )
                )

                connection.commit()

                record_id = cursor.lastrowid

            print(
                "[MARKET_MEMORY_SAVED]",
                symbol,
                "id=",
                record_id,
                "scenario=",
                scenario.get("name"),
                "action=",
                decision.get("action"),
                flush=True
            )

            return record_id

        except sqlite3.IntegrityError:

            print(
                "[MARKET_MEMORY_DUPLICATE]",
                signal_key,
                flush=True
            )

            return None

        except Exception as error:

            print(
                "[MARKET_MEMORY_SAVE_ERROR]",
                symbol,
                error,
                flush=True
            )

            return None

def update_market_memory(current_prices: dict):

    """
    current_prices:

    {
        "BTCUSDT": 108522.4,
        "ETHUSDT": 2654.8,
        ...
    }
    """

    now = time.time()

    with _db_lock:

        try:

            with closing(get_connection()) as connection:

                rows = connection.execute(
                    """
                    SELECT
                        id,
                        symbol,
                        entry_price,

                        created_at,

                        checked_15m_at,
                        checked_30m_at,
                        checked_60m_at,

                        completed

                    FROM market_signals

                    WHERE completed=0
                    """
                ).fetchall()

                print(
                    f"[MARKET_MEMORY_PENDING] {len(rows)}",
                    flush=True
                )

                for row in rows:

                    signal_id = row["id"]

                    symbol = row["symbol"]

                    entry_price = row["entry_price"]

                    created = row["created_at"]

                    age = now - created

                    current_price = current_prices.get(symbol)

                    if current_price is None:
                        continue

                    # ===========================
                    # 15 MIN
                    # ===========================

                    if (
                        age >= 15 * 60
                        and row["checked_15m_at"] is None
                    ):

                        move = (
                            (current_price - entry_price)
                            / entry_price
                        ) * 100

                        connection.execute(
                            """
                            UPDATE market_signals

                            SET

                                price_15m=?,
                                move_15m_pct=?,
                                checked_15m_at=?

                            WHERE id=?
                            """,
                            (
                                current_price,
                                move,
                                now,
                                signal_id
                            )
                        )

                        print(
                            f"[15M] {symbol} {move:.2f}%",
                            flush=True
                        )

                connection.commit()

        except Exception as e:

            print(
                "[MARKET_MEMORY_UPDATE_ERROR]",
                e,
                flush=True
            )
