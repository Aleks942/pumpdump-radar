# ============================================
# CHIEF EXPLAINER V7
# Переводит решение Chief Trader
# на человеческий язык.
# ============================================


def build_verdict(signal, decision):

    trade = decision.get(
        "trade_state",
        "IGNORE"
    )

    direction = decision.get(
        "direction",
        "NONE"
    )

    buyers = decision.get(
        "buyers_power",
        50
    )

    sellers = decision.get(
        "sellers_power",
        50
    )

    reasons = []

    # =====================================
    # ENTRY
    # =====================================

    if trade == "ENTRY":

        reasons.append(
            "Рынок сформировал хорошую точку входа."
        )

        if direction == "LONG":

            reasons.append(
                "Покупатели сохраняют полный контроль."
            )

        elif direction == "SHORT":

            reasons.append(
                "Продавцы сохраняют полный контроль."
            )

        reasons.append(
            "Вероятность продолжения движения высокая."
        )

        return reasons

    # =====================================
    # SETUP
    # =====================================

    if trade == "SETUP":

        reasons.append(
            "Импульс выглядит сильным."
        )

        if buyers > sellers:

            reasons.append(
                "Покупатели пока сильнее."
            )

        elif sellers > buyers:

            reasons.append(
                "Продавцы пока сильнее."
            )

        reasons.append(
            "Лучше дождаться подтверждения входа."
        )

        return reasons

    # =====================================
    # WATCH
    # =====================================

    if trade == "WATCH":

        reasons.append(
            "Движение заслуживает внимания."
        )

        reasons.append(
            "Точка входа пока не сформирована."
        )

        return reasons

    # =====================================
    # IGNORE
    # =====================================

    reasons.append(
        "Сигнал недостаточно подтверждён."
    )

    reasons.append(
        "Лучше дождаться более сильного сценария."
    )

    return reasons
