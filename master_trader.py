from typing import Any, Dict, Optional


ENGINE_VERSION = "MASTER_TRADER_V1"


def _to_float(
    value: Any,
    default: float = 0.0,
) -> float:
    """
    Безопасное преобразование значения в число.
    """

    try:
        if value is None:
            return default

        return float(value)

    except (TypeError, ValueError):
        return default


def _get_continuation_ui(
    continuation_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Достаёт результат Continuation Engine.

    Сейчас этим движком является chief_trader_v4.py.
    """

    continuation_result = continuation_result or {}

    ui = continuation_result.get("ui")

    if isinstance(ui, dict):
        return ui

    return continuation_result


def master_trader(
    signal: Dict[str
