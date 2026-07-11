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


if __name__ == "__main__":

    initialize_market_memory()

    market_memory_healthcheck()

    status = get_memory_status()

    print(
        "[MARKET_MEMORY_STATUS]",
        status,
        flush=True
    )
