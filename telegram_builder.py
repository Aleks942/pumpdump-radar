# ============================================
# TELEGRAM BUILDER V7
# ============================================

from datetime import datetime
from chief_explainer import build_verdict



def build_short_message(signal):
    """
    Основное короткое сообщение Telegram.
    """

    decision = signal.get("decision", {})
    smart = signal.get("smart_score", {})

    parts = []

    parts.append(
        build_header(signal, decision)
    )

    parts.append(
        build_score_block(smart)
    )

    parts.append(
        build_power_block(decision)
    )

    parts.append(
        build_reason_block(decision)
    )

    parts.append(
        build_footer()
    )

    return "\n".join(
        x for x in parts if x
    )

# ============================================
# HEADER
# ============================================

def build_header(signal, decision):

    trade = decision.get(
        "trade_state",
        "WATCH"
    )

    direction = decision.get(
        "direction",
        "NONE"
    )

    symbol = signal.get(
        "symbol",
        "UNKNOWN"
    )

    window = signal.get(
        "window",
        ""
    )

    change = signal.get(
        "change",
        0
    )

    if trade == "ENTRY":
        icon = "🟢"

    elif trade == "SETUP":
        icon = "🟡"

    elif trade == "WATCH":
        icon = "🟠"

    else:
        icon = "⚪"

    return (
        f"{icon} {trade} {direction}\n\n"
        f"🪙 {symbol} | {window}\n"
        f"📈 {change:+.2f}%"
    )

# ============================================
# SMART SCORE
# ============================================

def build_score_block(smart):

    return (
        "\n━━━━━━━━━━━━\n\n"
        f"{smart.get('stars','⭐')}"
        f" {smart.get('score',0)}/100\n\n"
        f"{smart.get('rating','')}\n"
        f"⚠️ {smart.get('risk','')}"
    )

# ============================================
# BUYERS / SELLERS
# ============================================

def build_power_block(decision):

    buyers = decision.get(
        "buyers_power",
        50
    )

    sellers = decision.get(
        "sellers_power",
        50
    )

    return (
        "\n━━━━━━━━━━━━\n\n"
        f"👥 BUY {buyers}% • SELL {sellers}%"
    )

# ============================================
# CHIEF VERDICT
# ============================================

def build_reason_block(signal, decision):

    reasons = build_verdict(
        signal,
        decision
    )

    if not reasons:
        return ""

    txt = "\n━━━━━━━━━━━━\n"

    txt += "\n🎯 Вердикт Chief\n"

    for r in reasons:

        txt += f"\n\n✔ {r}"

    return txt
