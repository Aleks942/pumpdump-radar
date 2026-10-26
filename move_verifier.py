"""Route a detected market move to the verifier matching its source exchange."""
from bybit_move_verifier import schedule as schedule_bybit
from okx_move_verifier import schedule as schedule_okx


def schedule(signal, sender, now=None):
    exchange = str((signal or {}).get("exchange") or "").upper()
    if exchange == "BYBIT":
        return schedule_bybit(signal, sender, now=now)
    if exchange == "OKX":
        return schedule_okx(signal, sender, now=now)
    print(
        "[MOVE_VERIFY_UNSUPPORTED_SOURCE]",
        (signal or {}).get("symbol"),
        "exchange=", exchange or "UNKNOWN",
        flush=True,
    )
    return False
