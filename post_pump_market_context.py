"""Bounded background liquidation refresh for the three active candidates."""
import threading
import time

_lock = threading.Lock()
_targets = ()
_seen = 0.0
_thread = None


def collect(symbols):
    from liquidation_engine import fetch_okx_liquidations, watch_bybit_symbol
    from post_pump_context_v2 import funding_context
    for symbol in symbols[:3]:
        try:
            bybit = watch_bybit_symbol(symbol)
            ok = fetch_okx_liquidations(symbol)
            funding_context(symbol)
            print('[POST_PUMP_CONTEXT_REFRESH]', symbol,
                  'okx_liquidations=', ok,
                  'bybit_liquidations=', bybit, flush=True)
        except Exception as exc:
            print('[POST_PUMP_CONTEXT_ERROR]', symbol, type(exc).__name__, flush=True)


def _worker():
    while True:
        started = time.monotonic()
        with _lock:
            symbols = _targets if time.monotonic()-_seen < 600 else ()
        collect(symbols)
        time.sleep(max(1, 30-(time.monotonic()-started)))


def watch(symbols):
    global _targets, _seen, _thread
    targets = tuple(dict.fromkeys(s for s in symbols
                    if isinstance(s, str) and s.endswith('USDT')))[:3]
    with _lock:
        _targets = targets
        _seen = time.monotonic()
        if targets and (_thread is None or not _thread.is_alive()):
            _thread = threading.Thread(target=_worker, daemon=True,
                                       name='post-pump-market-context')
            _thread.start()
