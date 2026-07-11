def detect_money_scenario(signal):

    move_type = signal.get("type")
    oi_change = signal.get("oi_change")
    funding = signal.get("funding")

    money = signal.get("money") or {}
    pressure = money.get("pressure", "")

    spot = signal.get("spot_cvd") or {}
    spot_state = spot.get("state", "SPOT_NO_DATA")

    liquidations = signal.get("liquidations") or {}
    long_liq = liquidations.get("long_liq", 0)
    short_liq = liquidations.get("short_liq", 0)

    oi_up = oi_change is not None and oi_change >= 3
    oi_down = oi_change is not None and oi_change <= -3

    spot_buy = spot_state in {
        "SPOT_BUY",
        "STRONG_SPOT_BUY",
    }

    spot_sell = spot_state in {
        "SPOT_SELL",
        "STRONG_SPOT_SELL",
    }

    buy_pressure = pressure in {
        "BUY_PRESSURE",
        "STRONG_BUY_PRESSURE",
    }

    sell_pressure = pressure in {
        "SELL_PRESSURE",
        "STRONG_SELL_PRESSURE",
    }

    strong_short_liq = (
        short_liq >= 100000
        and short_liq > long_liq * 2
    )

    strong_long_liq = (
        long_liq >= 100000
        and long_liq > short_liq * 2
    )

    funding_positive = (
        funding is not None
        and funding >= 0.01
    )

    funding_negative = (
        funding is not None
        and funding <= -0.01
    )

    # =====================================
    # PUMP SCENARIOS
    # =====================================

    if move_type == "PUMP":

        # 1. Здоровый рост
        if oi_up and spot_buy and buy_pressure:

            return {
                "name": "HEALTHY_PUMP",
                "title": "Здоровый памп",
                "bias": "CONTINUE",
                "strength": 5,
                "text": "Спот покупает, OI растёт, покупатели контролируют движение",
            }

        # 2. Рост поддерживает спот,
        # но фьючерсные позиции не растут
        if spot_buy and not oi_up and buy_pressure:

            return {
                "name": "SPOT_LED_PUMP",
                "title": "Рост поддерживает спот",
                "bias": "CONTINUE",
                "strength": 3,
                "text": "Монету покупают на споте, но новых фьючерсных позиций пока мало",
            }

        # 3. Short squeeze
        if oi_down and strong_short_liq:

            return {
                "name": "SHORT_SQUEEZE",
                "title": "Вынос шортов",
                "bias": "CORRECTION",
                "strength": 4,
                "text": "OI падает, шорты закрываются через ликвидации",
            }

        # 4. Слабый или ложный памп
        if spot_sell and sell_pressure:

            return {
                "name": "WEAK_PUMP",
                "title": "Рост без поддержки денег",
                "bias": "CORRECTION",
                "strength": 4,
                "text": "На споте продают, продавцы усиливаются",
            }

        # 5. Перегретый рост
        if funding_positive and not spot_buy:

            return {
                "name": "OVERHEATED_PUMP",
                "title": "Перегретый памп",
                "bias": "CORRECTION",
                "strength": 3,
                "text": "Funding положительный, а сильной поддержки спота нет",
            }

    # =====================================
    # DUMP SCENARIOS
    # =====================================

    if move_type == "DUMP":

        # 6. Здоровое продолжение падения
        if oi_up and spot_sell and sell_pressure:

            return {
                "name": "HEALTHY_DUMP",
                "title": "Сильный дамп",
                "bias": "CONTINUE",
                "strength": 5,
                "text": "Спот продаёт, OI растёт, продавцы контролируют движение",
            }

        # 7. Продажи идут со спота
        if spot_sell and not oi_up and sell_pressure:

            return {
                "name": "SPOT_LED_DUMP",
                "title": "Падение поддерживает спот",
                "bias": "CONTINUE",
                "strength": 3,
                "text": "Реальные продажи сохраняются, но новых шортов пока мало",
            }

        # 8. Капитуляция лонгов
        if oi_down and strong_long_liq:

            return {
                "name": "LONG_CAPITULATION",
                "title": "Капитуляция лонгов",
                "bias": "CORRECTION",
                "strength": 4,
                "text": "OI падает, лонги закрываются через ликвидации",
            }

        # 9. Дамп встречает спотовые покупки
        if spot_buy and buy_pressure:

            return {
                "name": "DUMP_ABSORPTION",
                "title": "Падение выкупают",
                "bias": "CORRECTION",
                "strength": 4,
                "text": "На споте покупают, покупатели перехватывают давление",
            }

        # 10. Перегретый дамп
        if funding_negative and not spot_sell:

            return {
                "name": "OVERHEATED_DUMP",
                "title": "Перегретый дамп",
                "bias": "CORRECTION",
                "strength": 3,
                "text": "Funding отрицательный, а сильных продаж на споте нет",
            }

    # =====================================
    # NO CLEAR SCENARIO
    # =====================================

    return {
        "name": "MIXED_MONEY_FLOW",
        "title": "Смешанный поток денег",
        "bias": "WAIT",
        "strength": 1,
        "text": "Денежные показатели пока не дают единого направления",
    }
