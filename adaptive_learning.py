import json
import os
from typing import Dict


WEIGHTS_FILE = "adaptive_weights.json"


DEFAULT_WEIGHTS = {
    "OI": 1.0,
    "TREND": 1.0,
    "PRESSURE": 1.0,
    "MONEY": 1.0,
    "SPOT": 1.0,
    "LIQUIDATIONS": 1.0,
    "SCENARIO": 1.0,
}


def save_weights(weights: Dict[str, float]) -> None:
    """
    Сохраняет адаптивные веса в JSON-файл.
    """

    try:
        with open(WEIGHTS_FILE, "w", encoding="utf-8") as file:
            json.dump(
                weights,
                file,
                ensure_ascii=False,
                indent=4,
            )

    except Exception as error:
        print(
            "[ADAPTIVE_WEIGHTS_SAVE_ERROR]",
            error,
            flush=True,
        )


def load_weights() -> Dict[str, float]:
    """
    Загружает веса из JSON-файла.

    Если файла ещё нет или он повреждён,
    создаёт стандартный набор весов.
    """

    if not os.path.exists(WEIGHTS_FILE):
        save_weights(DEFAULT_WEIGHTS.copy())
        return DEFAULT_WEIGHTS.copy()

    try:
        with open(WEIGHTS_FILE, "r", encoding="utf-8") as file:
            loaded_weights = json.load(file)

        if not isinstance(loaded_weights, dict):
            raise ValueError("Weights file must contain a dictionary")

        final_weights = DEFAULT_WEIGHTS.copy()

        for key, value in loaded_weights.items():
            if key in final_weights:
                final_weights[key] = float(value)

        return final_weights

    except Exception as error:
        print(
            "[ADAPTIVE_WEIGHTS_LOAD_ERROR]",
            error,
            flush=True,
        )

        save_weights(DEFAULT_WEIGHTS.copy())

        return DEFAULT_WEIGHTS.copy()


def get_weight(
    module_name: str,
    weights: Dict[str, float],
) -> float:
    """
    Возвращает вес конкретного модуля.
    """

    return float(
        weights.get(
            module_name,
            DEFAULT_WEIGHTS.get(module_name, 1.0),
        )
    )
def update_module_statistics(votes, result):
    """
    Обновляет статистику модулей.

    votes - список голосов Chief Trader
    result - WIN / LOSS / FLAT
    """

    if result == "FLAT":
        return

    weights = load_weights()

    changed = False

    for vote in votes:

        module = vote.get("module")

        if module not in weights:
            continue

        direction = vote.get("vote")

        if result == "WIN":

            if direction == "CONTINUE":
                weights[module] += 0.01

            elif direction == "EXHAUSTION":
                weights[module] -= 0.01

        elif result == "LOSS":

            if direction == "CONTINUE":
                weights[module] -= 0.01

            elif direction == "EXHAUSTION":
                weights[module] += 0.01

        weights[module] = max(
            0.5,
            min(
                2.0,
                round(weights[module], 3)
            )
        )

        changed = True

    if changed:
        save_weights(weights)

        print(
            "[ADAPTIVE_FILE]",
            WEIGHTS_FILE,
            flush=True
        )

        print(
            "[ADAPTIVE_UPDATE]",
            weights,
            flush=True
        )
