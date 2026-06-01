import os
import time
import requests
import random
from datetime import datetime, UTC

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
CMC_API_KEY = os.getenv("CMC_API_KEY")

PUMP_THRESHOLD_5M = float(os.getenv("PUMP_THRESHOLD_5M", 3))
DUMP_THRESHOLD_5M = float(os.getenv("DUMP_THRESHOLD_5M", -3))
PUMP_THRESHOLD_10M = float(os.getenv("PUMP_THRESHOLD_10M", 4))
DUMP_THRESHOLD_10M = float(os.getenv("DUMP_THRESHOLD_10M", -4))
PUMP_THRESHOLD_15M = float(os.getenv("PUMP_THRESHOLD_15M", 5))
DUMP_THRESHOLD_15M = float(os.getenv("DUMP_THRESHOLD_15M", -5))
PUMP_THRESHOLD_20M = float(os.getenv("PUMP_THRESHOLD_20M", 6))
DUMP_THRESHOLD_20M = float(os.getenv("DUMP_THRESHOLD_20M", -6))
PUMP_THRESHOLD_30M = float(os.getenv("PUMP_THRESHOLD_30M", 8))
DUMP_THRESHOLD_30M = float(os.getenv("DUMP_THRESHOLD_30M", -8))
PUMP_THRESHOLD_60M = float(os.getenv("PUMP_THRESHOLD_60M", 12))
DUMP_THRESHOLD_60M = float(os.getenv("DUMP_THRESHOLD_60M", -12))

MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", 10000000))
ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN", 900))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 120))
SCAN_SLEEP = int(os.getenv("SCAN_SLEEP", 60))
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", 1000))
REPEAT_IMPROVE_PCT = float(os.getenv("REPEAT_IMPROVE_PCT", 2.0))
CMC_CACHE_TTL = int(os.getenv("CMC_CACHE_TTL", 1800))

symbol_states = {}
signal_24h_count = {}
oi_memory = {}
cmc_cache = {}
rotation_index = 0

TIME_WINDOWS = {
    "5m": {"bar": "5m", "candles": 2, "pump": PUMP_THRESHOLD_5M, "dump": DUMP_THRESHOLD_5M},
    "10m": {"bar": "5m", "candles": 3, "pump": PUMP_THRESHOLD_10M, "dump": DUMP_THRESHOLD_10M},
    "15m": {"bar": "5m", "candles": 4, "pump": PUMP_THRESHOLD_15M, "dump": DUMP_THRESHOLD_15M},
    "20m": {"bar": "5m", "candles": 5, "pump": PUMP_THRESHOLD_20M, "dump": DUMP_THRESHOLD_20M},
    "30m": {"bar": "5m", "candles": 7, "pump": PUMP_THRESHOLD_30M, "dump": DUMP_THRESHOLD_30M},
    "60m": {"bar": "5m", "candles": 13, "pump": PUMP_THRESHOLD_60M, "dump": DUMP_THRESHOLD_60M},
}


def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("[TG DISABLED] BOT_TOKEN or CHAT_ID missing")
        print(text)
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        r = requests.post(url, json=payload, timeout=10)
        print("[TG STATUS]", r.status_code)
        if r.status_code != 200:
            print("[TG BODY]", r.text[:300])
    except Exception as e:
        print("[TG ERROR]", e)


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def fmt_money(value):
    if value is None:
        return "нет данных"
    try:
        value = float(value)
        if value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"${value / 1_000:.2f}K"
        return f"${value:.2f}"
    except Exception:
        return "нет данных"


def clean_base_symbol(symbol):
    return (
        symbol
        .replace("USDT", "")
        .replace("USD", "")
        .replace("1000", "")
    )


def get_market_tickers():
    url = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"

    try:
        r = requests.get(url, timeout=20)
        print("[OKX STATUS]", r.status_code)
        data = r.json()

        if data.get("code") != "0":
            print("[OKX ERROR]", data)
            return []

        tickers = data.get("data", [])
        tickers = [t for t in tickers if "USDT-SWAP" in t.get("instId", "")]

        random.shuffle(tickers)
        return tickers[:MAX_SYMBOLS]

    except Exception as e:
        print("[OKX EXCEPTION]", e)
        return []


def get_rotation_chunk(tickers):
    global rotation_index

    total = len(tickers)
    if total == 0:
        return []

    start = rotation_index * CHUNK_SIZE
    end = start + CHUNK_SIZE
    current_chunk = tickers[start:end]

    if not current_chunk:
        rotation_index = 0
        start = 0
        end = CHUNK_SIZE
        current_chunk = tickers[start:end]

    if end >= total:
        rotation_index = 0
    else:
        rotation_index += 1

    print("[ROTATION]", start, "-", min(end, total), "of", total)
    return current_chunk


def get_funding_rate(raw_symbol):
    url = "https://www.okx.com/api/v5/public/funding-rate"
    params = {"instId": raw_symbol}

    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()

        if data.get("code") != "0":
            return None

        rows = data.get("data", [])
        if not rows:
            return None

        return float(rows[0].get("fundingRate", 0)) * 100

    except Exception as e:
        print("[FUNDING EXCEPTION]", raw_symbol, e)
        return None


def get_open_interest(raw_symbol):
    url = "https://www.okx.com/api/v5/public/open-interest"
    params = {"instId": raw_symbol}

    try:
        r = requests.get(url, params=params, timeout=15)
        data = r.json()

        if data.get("code") != "0":
            return None

        rows = data.get("data", [])
        if not rows:
            return None

        return float(rows[0].get("oi", 0))

    except Exception as e:
        print("[OI EXCEPTION]", raw_symbol, e)
        return None


def get_window_move(raw_symbol, bar, candles_count):
    url = "https://www.okx.com/api/v5/market/candles"
    params = {
        "instId": raw_symbol,
        "bar": bar,
        "limit": str(candles_count)
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        if data.get("code") != "0":
            return None

        candles = data.get("data", [])
        if len(candles) < candles_count:
            return None

        newest = candles[0]
        oldest = candles[-1]

        start_price = float(oldest[1])
        end_price = float(newest[4])

        if start_price == 0:
            return None

        change = ((end_price - start_price) / start_price) * 100

        return {
            "start_price": start_price,
            "end_price": end_price,
            "change": change
        }

    except Exception as e:
        print("[CANDLES EXCEPTION]", raw_symbol, e)
        return None


def get_cmc_data(symbol):
    if not CMC_API_KEY:
        return None

    base = clean_base_symbol(symbol)
    now = time.time()

    cached = cmc_cache.get(base)
    if cached and now - cached["time"] < CMC_CACHE_TTL:
        return cached["data"]

    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest"

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    params = {
        "symbol": base,
        "convert": "USD"
    }

    try:
        r = requests.get(url, headers=headers, params=params, timeout=15)
        print("[CMC QUOTE]", base, r.status_code)

        if r.status_code != 200:
            print("[CMC ERROR]", r.text[:300])
            return None

        data = r.json()
        rows = data.get("data", {}).get(base)

        if not rows:
            return None

        if isinstance(rows, list):
            coin = rows[0]
        else:
            coin = rows

        quote = coin.get("quote", {}).get("USD", {})

        result = {
            "name": coin.get("name"),
            "symbol": coin.get("symbol"),
            "rank": coin.get("cmc_rank"),
            "market_cap": safe_float(quote.get("market_cap")),
            "volume_24h": safe_float(quote.get("volume_24h")),
            "percent_change_24h": safe_float(quote.get("percent_change_24h")),
        }

        cmc_cache[base] = {
            "time": now,
            "data": result
        }

        return result

    except Exception as e:
        print("[CMC EXCEPTION]", base, e)
        return None


def explain_market_cap(market_cap):
    if market_cap is None:
        return "Капитализация неизвестна. Сложнее понять, насколько легко монету двигать."

    if market_cap < 50_000_000:
        return "Очень маленькая капитализация. Монету могут легко пампить и резко сливать. Риск манипуляции высокий."

    if market_cap < 300_000_000:
        return "Небольшая капитализация. Монету ещё достаточно легко двигать, возможны резкие пампы и дампы."

    if market_cap < 1_000_000_000:
        return "Средняя капитализация. Движение уже серьёзнее, но манипуляции всё ещё возможны."

    if market_cap < 10_000_000_000:
        return "Крупная монета. Двигать её сложнее, сигнал обычно надёжнее, чем у микрокапов."

    return "Очень крупная монета. Резкое движение обычно означает сильное рыночное событие или мощный поток денег."


def explain_rank(rank):
    if rank is None:
        return "Ранг неизвестен. Нужно осторожнее относиться к сигналу."

    try:
        rank = int(rank)
    except Exception:
        return "Ранг неизвестен. Нужно осторожнее относиться к сигналу."

    if rank <= 50:
        return "Топовая монета. Манипулировать сложнее, движение обычно более серьёзное."

    if rank <= 200:
        return "Достаточно популярная монета. Сигнал стоит внимания."

    if rank <= 500:
        return "Средняя монета. Риск резких движений и ловушек выше."

    return "Дальняя монета по рейтингу. Высокий риск манипуляций, резких шпилек и плохой ликвидности."


def explain_cmc_volume(volume_24h, market_cap):
    if volume_24h is None:
        return "Объём CMC неизвестен. Сложнее оценить качество движения."

    if market_cap and market_cap > 0:
        ratio = volume_24h / market_cap

        if ratio > 0.8:
            return "Объём очень высокий относительно капитализации. Монета перегрета, возможны резкие развороты."

        if ratio > 0.3:
            return "Объём высокий. В монете сильная активность, движение не случайное."

        if ratio > 0.08:
            return "Объём нормальный. Движение имеет рыночную поддержку."

        return "Объём слабый относительно капитализации. Движение может быть менее надёжным."

    if volume_24h >= 500_000_000:
        return "Очень большой суточный объём. Монета активно торгуется."

    if volume_24h >= 100_000_000:
        return "Хороший суточный объём. Ликвидность нормальная."

    if volume_24h >= 20_000_000:
        return "Средний объём. Нужно осторожно смотреть на шпильки."

    return "Низкий объём. Высокий риск манипуляций и резких движений."


def explain_funding(funding):
    if funding is None:
        return "Funding неизвестен. Нельзя точно понять, где сейчас толпа."

    if funding <= -0.20:
        return "Funding сильно отрицательный. Толпа агрессивно шортит. Рынок может резко вынести шортистов вверх."

    if funding <= -0.03:
        return "Funding отрицательный. Шортов больше, чем лонгов. Это может стать топливом для пампа."

    if funding >= 0.20:
        return "Funding сильно положительный. Толпа агрессивно лонгует. Риск выноса лонгистов вниз высокий."

    if funding >= 0.03:
        return "Funding положительный. Лонгов больше, чем шортов. После пампа это может быть опасно."

    return "Funding около нуля. Явного перекоса толпы пока нет."


def explain_oi(oi_change, move_type):
    if oi_change is None:
        return "OI неизвестен. Нельзя точно понять, новые позиции заходят или старые закрываются."

    if move_type == "PUMP":
        if oi_change > 2:
            return "OI растёт вместе с ценой. В рынок заходят новые позиции, движение поддерживается деньгами."
        if oi_change < -2:
            return "OI падает при росте цены. Это похоже на закрытие шортов и вынос шортистов."
        return "OI почти не меняется. Рост есть, но сильного подтверждения новыми позициями пока нет."

    if move_type == "DUMP":
        if oi_change > 2:
            return "OI растёт при падении цены. Новые шорты давят рынок вниз."
        if oi_change < -2:
            return "OI падает при снижении цены. Позиции закрываются, возможна капитуляция или фиксация."
        return "OI почти не меняется. Падение есть, но сильного подтверждения новыми позициями пока нет."

    return "OI без явной интерпретации."


def clean_old_signal_counts():
    now = time.time()

    for symbol in list(signal_24h_count.keys()):
        signal_24h_count[symbol] = [
            t for t in signal_24h_count[symbol]
            if now - t < 86400
        ]

        if not signal_24h_count[symbol]:
            del signal_24h_count[symbol]


def add_signal_count(symbol):
    now = time.time()

    if symbol not in signal_24h_count:
        signal_24h_count[symbol] = []

    signal_24h_count[symbol].append(now)
    clean_old_signal_counts()

    return len(signal_24h_count.get(symbol, []))


def can_send(symbol, move_type, window, change):
    now = time.time()
    key = f"{symbol}_{move_type}"

    state = symbol_states.get(key)

    if state is None:
        symbol_states[key] = {
            "last_alert": now,
            "max_change": change,
            "window": window
        }
        return True

    last_alert = state.get("last_alert", 0)
    old_change = state.get("max_change", change)

    if now - last_alert < ALERT_COOLDOWN:
        if move_type == "PUMP" and change < old_change + REPEAT_IMPROVE_PCT:
            return False

        if move_type == "DUMP" and change > old_change - REPEAT_IMPROVE_PCT:
            return False

    symbol_states[key] = {
        "last_alert": now,
        "max_change": change,
        "window": window
    }

    return True


def calc_smart_score(move_type, change, window_name, volume_24h, funding, oi_change, cmc_data):
    score = 0
    abs_change = abs(change)

    # =========================
    # EARLY MOVE PRIORITY
    # =========================
    # Нам важнее раннее движение, а не уже поздний памп
    if abs_change >= 15:
        score += 12      # было слишком много
    elif abs_change >= 10:
        score += 16
    elif abs_change >= 7:
        score += 20
    elif abs_change >= 5:
        score += 24
    elif abs_change >= 2:
        score += 22
    elif abs_change >= 1:
        score += 16
    else:
        score += 8

    # =========================
    # FAST WINDOW BONUS
    # =========================
    if window_name in ("5m", "10m"):
        score += 20
    elif window_name in ("15m", "20m"):
        score += 14
    elif window_name == "30m":
        score += 7
    else:
        score += 3

    # =========================
    # VOLUME FILTER
    # =========================
    if volume_24h >= 1_000_000_000:
        score += 10
    elif volume_24h >= 300_000_000:
        score += 10
    elif volume_24h >= 100_000_000:
        score += 8
    elif volume_24h >= 30_000_000:
        score += 5

    # =========================
    # FUNDING TRAP
    # =========================
    if funding is not None:
        if move_type == "PUMP" and funding < -0.03:
            score += 18   # шорты могут быть топливом для пампа
        elif move_type == "DUMP" and funding > 0.03:
            score += 18   # лонги могут быть топливом для дампа
        elif abs(funding) >= 0.10:
            score += 8
        elif abs(funding) >= 0.03:
            score += 5

    # =========================
    # OI LOGIC
    # =========================
    if oi_change is not None:
        if oi_change >= 2:
            score += 18   # новые позиции заходят
        elif oi_change >= 0.5:
            score += 10
        elif oi_change < -2:
            score -= 8    # движение может быть уже squeeze/exhaustion

    # =========================
    # LATE MOVE PENALTY
    # =========================
    if abs_change >= 10 and oi_change is not None and oi_change < 0:
        score -= 20

    if abs_change >= 15:
        score -= 15

    # =========================
    # CMC QUALITY
    # =========================
    if cmc_data:
        rank = cmc_data.get("rank")
        market_cap = cmc_data.get("market_cap")

        if rank is not None:
            try:
                rank = int(rank)
                if rank <= 100:
                    score += 5
                elif rank <= 300:
                    score += 4
            except Exception:
                pass

        if market_cap is not None:
            if market_cap < 50_000_000:
                score -= 8
            elif market_cap > 300_000_000:
                score += 4

    return max(0, min(100, score))

def score_label(score):
    if score >= 85:
        return "🔥 Очень сильный сигнал"
    if score >= 70:
        return "🟠 Сильный сигнал"
    if score >= 50:
        return "🟡 Средний сигнал"
    return "⚪ Слабый / шумный сигнал"


def classify_phase(move_type, change, window_name, funding, oi_change):
    abs_change = abs(change)

    if move_type == "PUMP":

        if funding is not None and funding < -0.03 and oi_change is not None and oi_change < -0.5:
            return (
                "🟠 Вынос шортистов",
                "Цена резко растёт против отрицательного funding. Это значит, что многие стояли в шортах, но рынок пошёл против них. OI падает — часть позиций закрывается, рост может идти за счёт принудительного закрытия шортов.",
                "Движение может продолжиться рывком вверх, но это уже опасная зона для позднего входа. После выноса часто бывает резкий откат.",
                "Не догонять зелёную свечу. Ждать откат, слабый повторный тест или признаки усталости покупателей."
            )

        if funding is not None and funding < -0.03 and oi_change is not None and oi_change >= 0.5:
            return (
                "🟡 Импульс развивается",
                "Цена растёт, funding отрицательный, а OI растёт. В рынок заходят новые позиции, но толпа всё ещё пытается шортить рост. Это может дать продолжение выноса шортистов.",
                "Пока движение живое, но нужно следить за перегревом: если funding начнёт выходить к плюсу, а цена станет рваной — риск отката вырастет.",
                "Можно наблюдать продолжение, но вход лучше искать не в свечу, а после отката."
            )

        if funding is not None and funding > 0.03 and oi_change is not None and oi_change > 0.5:
            return (
                "🔴 Поздний перегретый памп",
                "Цена растёт, funding уже положительный, OI растёт. Это значит, что толпа начинает лонговать уже после сильного движения. Такая структура часто становится ловушкой для поздних покупателей.",
                "Риск резкого отката повышен. Если цена перестанет идти выше, поздних лонгистов могут начать выбивать вниз.",
                "Не покупать поздно. Смотреть на признаки истощения и возможный откат."
            )

        if oi_change is not None and oi_change < -0.5:
            return (
                "🟠 Рост на закрытии позиций",
                "Цена растёт, но OI падает. Это больше похоже на закрытие шортов или снятие плечевых позиций, а не на полноценный новый тренд.",
                "Если после такого пампа нет нового спроса, движение может быстро выдохнуться.",
                "Ждать откат. Не заходить без подтверждения продолжения."
            )

        if abs_change >= 10:
            return (
                "🔴 Сильный памп, возможен перегрев",
                "Монета уже прошла большое движение вверх. Даже если рост ещё продолжается, риск отката становится выше просто из-за скорости движения.",
                "После таких пампов часто бывает откат, боковик или резкий разворот.",
                "Искать не догоняющий вход, а откат / слабость / повторный тест."
            )

        return (
            "🟡 Обычный памп",
            "Цена быстро выросла, но по OI и funding нет сильной однозначной картины. Движение есть, но пока не ясно, это начало сильного импульса или просто короткий рывок.",
            "Без подтверждения OI/funding такой сигнал может быть шумным.",
            "Смотреть продолжение: удержит ли цену и появится ли новый объём."
        )

    if move_type == "DUMP":

        if funding is not None and funding > 0.03 and oi_change is not None and oi_change < -0.5:
            return (
                "🔴 Вынос лонгистов",
                "Цена резко падает против положительного funding. Это значит, что многие были в лонгах, а рынок пошёл против них. OI падает — позиции закрываются, возможна ликвидационная волна вниз.",
                "После резкого слива возможен сильный отскок, потому что часть продавцов будет фиксировать прибыль.",
                "Не шортить поздно в красную свечу. Ждать отскок или слабый повторный тест."
            )

        if funding is not None and funding > 0.03 and oi_change is not None and oi_change >= 0.5:
            return (
                "🔻 Продавцы усиливают давление",
                "Цена падает, funding положительный, OI растёт. Поздние лонгисты под давлением, а новые позиции могут усиливать падение.",
                "Если цена продолжит обновлять минимумы, возможна вторая волна слива.",
                "Шорт логичнее искать после слабого отскока, а не в самом низу свечи."
            )

        if oi_change is not None and oi_change > 0.5:
            return (
                "🔻 Новые шорты давят цену",
                "Цена падает, а OI растёт. Это значит, что в рынок заходят новые позиции на падение. Продавец пока активен.",
                "Опасность в том, что если цена перестанет падать при высоком OI, шортистов могут резко выдавить вверх.",
                "Не опаздывать с шортом. Смотреть, не появляется ли откуп и удержание цены."
            )

        if oi_change is not None and oi_change < -0.5:
            return (
                "🟣 Капитуляция / закрытие позиций",
                "Цена падает, и OI тоже падает. Позиции закрываются. Это может быть ликвидация лонгов или фиксация прибыли шортистами.",
                "После такого часто бывает отскок, особенно если funding остаётся отрицательным.",
                "Не шортить бездумно внизу. Смотреть реакцию: появляется ли покупатель."
            )

        if abs_change >= 10:
            return (
                "🔴 Сильный дамп, возможен отскок",
                "Монета уже прошла большое движение вниз. Продавец сильный, но после резких дампов часто приходит технический отскок.",
                "Опасность — поздний шорт прямо перед откупом.",
                "Лучше ждать слабый отскок и только потом оценивать продолжение вниз."
            )

        return (
            "🔻 Обычный дамп",
            "Цена быстро упала, но по OI и funding нет сильной однозначной картины. Движение есть, но пока не ясно, это начало слива или короткий прокол.",
            "Без подтверждения OI/funding такой сигнал может быть шумным.",
            "Смотреть продолжение: будет ли новый продавец и обновление минимумов."
        )

    return (
        "⚪ Неясная стадия",
        "Недостаточно данных для оценки.",
        "Ждать больше подтверждений.",
        "Не принимать решение только по одному сигналу."
    )


def analyze(ticker):
    try:
        raw_symbol = ticker["instId"]
        symbol = raw_symbol.replace("-USDT-SWAP", "USDT")
        price = float(ticker["last"])

        if price < 0.01:
            return None

        volume_24h = float(ticker["volCcy24h"])

        if volume_24h < MIN_VOLUME_24H:
            return None

    except Exception as e:
        print("[ANALYZE TICKER ERROR]", e)
        return None

    best_signal = None

    for window_name, cfg in TIME_WINDOWS.items():
        move = get_window_move(raw_symbol, cfg["bar"], cfg["candles"])

        if move is None:
            continue

        change = move["change"]
        move_type = None

        if change >= cfg["pump"]:
            move_type = "PUMP"
        elif change <= cfg["dump"]:
            move_type = "DUMP"
        else:
            continue

        if best_signal is not None and abs(change) <= abs(best_signal["change"]):
            continue

        funding = get_funding_rate(raw_symbol)
        oi = get_open_interest(raw_symbol)

        oi_change = None
        old_oi = oi_memory.get(symbol)

        if oi is not None and old_oi is not None and old_oi > 0:
            oi_change = ((oi - old_oi) / old_oi) * 100

        if oi is not None:
            oi_memory[symbol] = oi

        cmc_data = get_cmc_data(symbol)

        phase, explanation, risk, action = classify_phase(
            move_type=move_type,
            change=change,
            window_name=window_name,
            funding=funding,
            oi_change=oi_change
        )

        score = calc_smart_score(
            move_type=move_type,
            change=change,
            window_name=window_name,
            volume_24h=volume_24h,
            funding=funding,
            oi_change=oi_change,
            cmc_data=cmc_data
        )

        best_signal = {
            "symbol": symbol,
            "type": move_type,
            "window": window_name,
            "change": change,
            "start_price": move["start_price"],
            "end_price": move["end_price"],
            "price": price,
            "volume": volume_24h,
            "funding": funding,
            "oi": oi,
            "oi_change": oi_change,
            "phase": phase,
            "explanation": explanation,
            "risk": risk,
            "action": action,
            "score": score,
            "cmc": cmc_data,
        }

    if best_signal is None:
        return None

    if not can_send(
        best_signal["symbol"],
        best_signal["type"],
        best_signal["window"],
        best_signal["change"]
    ):
        return None

    signal_count = add_signal_count(best_signal["symbol"])
    best_signal["signal_24h"] = signal_count

    return best_signal


def build_message(signal):
    emoji = "🚀" if signal["type"] == "PUMP" else "🔻"
    side_text = "ПАМП" if signal["type"] == "PUMP" else "ДАМП"

    funding = signal.get("funding")
    oi_change = signal.get("oi_change")
    score = signal.get("score", 0)
    cmc = signal.get("cmc") or {}

    funding_text = "нет данных" if funding is None else f"{funding:.3f}%"
    oi_text = "нет данных" if oi_change is None else f"{oi_change:.2f}%"

    market_cap = cmc.get("market_cap")
    cmc_volume = cmc.get("volume_24h")
    rank = cmc.get("rank")
    name = cmc.get("name") or signal["symbol"]

    rank_text = "нет данных" if rank is None else f"#{rank}"

    return f"""
{emoji} <b>{signal["symbol"]}</b> | <b>{side_text}</b>
🏷 Монета: <b>{name}</b>

⏱ Период: <b>{signal["window"]}</b>
📈 Движение: <b>{signal["change"]:.2f}%</b>
Цена резко изменилась за короткое время. Это аномальное движение, которое нужно разобрать по деньгам.

💰 Цена:
<code>{signal["start_price"]} → {signal["end_price"]}</code>

📊 Объём OKX 24ч:
<b>{signal["volume"]:,.0f}</b>
Показывает, насколько активно монету торговали на OKX за сутки.

🏦 Market Cap:
<b>{fmt_money(market_cap)}</b>
{explain_market_cap(market_cap)}

📊 CMC Rank:
<b>{rank_text}</b>
{explain_rank(rank)}

💧 Объём CMC 24ч:
<b>{fmt_money(cmc_volume)}</b>
{explain_cmc_volume(cmc_volume, market_cap)}

💸 Funding:
<b>{funding_text}</b>
{explain_funding(funding)}

📦 OI:
<b>{oi_text}</b>
{explain_oi(oi_change, signal["type"])}

📌 Стадия:
<b>{signal["phase"]}</b>

⭐ Оценка:
<b>{score}/100</b> — {score_label(score)}

🧠 Что происходит:
{signal["explanation"]}

⚠️ Риск:
{signal["risk"]}

🔎 Что делать:
{signal["action"]}

🔁 Сигналов за 24ч:
<b>{signal["signal_24h"]}</b>

🕒 {datetime.now(UTC).strftime("%H:%M UTC")}
"""


def test_cmc():
    if not CMC_API_KEY:
        print("[CMC] API key missing")
        return

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}

    try:
        r = requests.get(url, headers=headers, timeout=15)
        print("[CMC STATUS]", r.status_code)

        if r.status_code == 200:
            print("[CMC] API WORKING")
        else:
            print("[CMC ERROR]", r.text[:300])

    except Exception as e:
        print("[CMC EXCEPTION]", e)


print("🚀 PumpDump Radar V4 started")
send_telegram("🚀 PumpDump Radar V4 ONLINE")
test_cmc()

while True:
    print("[SCAN] scanning market...")

    tickers = get_market_tickers()
    print(f"[TICKERS] {len(tickers)}")

    current_chunk = get_rotation_chunk(tickers)
    print(f"[CHUNK] {len(current_chunk)}")

    for ticker in current_chunk:
        signal = analyze(ticker)

        if not signal:
            continue

        send_telegram(build_message(signal))

        print(
            "[SIGNAL]",
            signal["window"],
            signal["symbol"],
            signal["type"],
            round(signal["change"], 2),
            "score=",
            signal["score"]
        )

    time.sleep(SCAN_SLEEP)
