"""Persistent Telegram lifecycle for research plans; never opens orders.

Only new committed strategy events are eligible. On first activation history is
skipped. Ambiguous Telegram responses are held UNKNOWN, never blindly retried.
"""
import json
import math
import os
import sqlite3
import time
from contextlib import closing
from html import escape
from datetime import datetime
from zoneinfo import ZoneInfo

DB_PATH = os.getenv('PUMP_WATCH_DB', '/data/pump_watch.sqlite3')
TITLES = {
    'DISCOVERED': '🛰 СИЛЬНЫЙ ПАМП — НАБЛЮДАЕМ',
    'EARLY_WEAKENING': '🟡 РАННЕЕ ОСЛАБЛЕНИЕ — ПОКУПАТЕЛЬ СЛАБЕЕТ',
    'WEAKENING': '⚠️ ПАМП ОСЛАБЕВАЕТ — СЛЕДИМ ЗА ШОРТОМ',
    'TREND_SHIFT': '📉 СТРУКТУРА СЛОМАНА ВНИЗ — ЖДЁМ ПОДТВЕРЖДЕНИЕ',
    'PRE_SHORT': '🟠 РАННИЙ СИГНАЛ НА ШОРТ — 4 ИЗ 5',
    'SHORT_CONFIRMED': '🔴 ШОРТ ПОДТВЕРЖДЁН — ЖДЁМ ПОВТОРНУЮ ПРОВЕРКУ УРОВНЯ',
    'RETEST': '🔎 ПОВТОРНАЯ ПРОВЕРКА УРОВНЯ — СМОТРИМ ПРОДАВЦОВ',
    'ENTRY_ZONE': '🎯 ЗОНА ВХОДА В ШОРТ',
    'READY_RESEARCH': '🧪 ТЕСТОВЫЙ ПЛАН НА ШОРТ',
    'CANCELLED': '⛔ СЦЕНАРИЙ ОТМЕНЁН',
}
CANCEL_REASONS = {
    'BUYERS_RECLAIMED_NEW_HIGH': 'Покупатели обновили максимум.',
    'BUYERS_RECOVERED_EARLY_WEAKENING': 'Цена восстановилась после ослабления.',
    'BREAK_INVALIDATED': 'Цена вернулась выше пробитого уровня.',
    'TREND_NOT_ALIGNED': 'Структура снижения больше не подтверждена.',
    'PAPER_STOP_REACHED': 'Цена достигла стопа тестового плана.',
    'PAPER_PLAN_EXPIRED': 'Срок наблюдения тестового плана истёк.',
}


def _num(x):
    try:
        n = float(x)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError, OverflowError):
        return None


def _setup(db, now):
    db.execute('''CREATE TABLE IF NOT EXISTS post_pump_notify_meta (
        key TEXT PRIMARY KEY, value INTEGER NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS post_pump_notify_outbox (
        audit_id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
        episode_at REAL NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING', retry_at REAL NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0, sent_at REAL)''')
    db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_post_pump_notify_stage
        ON post_pump_notify_outbox(symbol,episode_at,kind) WHERE kind!='CANCELLED' ''')
    if db.execute("SELECT 1 FROM post_pump_notify_meta WHERE key='cursor'").fetchone() is None:
        last = db.execute('SELECT COALESCE(MAX(id),0) FROM post_pump_short_strategy_audit').fetchone()[0]
        db.execute("INSERT INTO post_pump_notify_meta VALUES ('cursor',?)", (last,))
        print('[POST_PUMP_NOTIFY_STARTED] history_skipped_through=', last, flush=True)
    # A process interrupted after send may already have delivered the message.
    db.execute("UPDATE post_pump_notify_outbox SET status='UNKNOWN' WHERE status='SENDING' AND retry_at<?", (now,))



def _research_lines(event):
    """Describe cached evidence, not an approval to trade."""
    try:
        d=json.loads(event.get('evidence_json') or '{}')
    except (ValueError,TypeError):
        return ['Дополнительные данные: недоступны']
    if not isinstance(d,dict):
        return ['Дополнительные данные: недоступны']
    def val(value, suffix=''):
        n=_num(value)
        return f'{n:+,.2f}{suffix}' if n is not None else 'нет свежих данных'
    lines=['Проверки данных:']
    capital=d.get('capital_state') or {}
    if isinstance(capital,dict) and capital:
        lines.append('OI / капитал: '+escape(str(capital.get('state') or 'НЕТ ДАННЫХ')[:70]))
        lines.append('OI 5м: '+val(capital.get('oi_5m_pct'), '%')+' · '+escape(str(capital.get('oi_source') or 'нет источника')))
    else:
        agg_oi=_num(d.get('agg_oi_5m_pct'))
        local_oi=_num(d.get('local_oi_5m_pct'))
        raw_oi=agg_oi if agg_oi is not None else local_oi
        raw_source='AGGREGATED' if agg_oi is not None else 'LOCAL' if local_oi is not None else 'нет источника'
        lines.append('OI / капитал: '+(
            'OI уже есть, структура ещё не подтверждена'
            if raw_oi is not None else 'НЕТ ДАННЫХ'))
        lines.append('OI 5м: '+val(raw_oi, '%')+' · '+raw_source)
    cvd=d.get('bounded_cvd_5m') or {}
    if isinstance(cvd,dict) and cvd:
        lines.append('Дельта сделок 5м: '+escape(str(cvd.get('state') or 'НЕТ ДАННЫХ')[:70])+
                     ' (не полный CVD)')
        lines += ['OKX фьючерсы, Δ 5м: '+val(cvd.get('futures_delta_quote'), ' USDT')+
                  ' · дисбаланс '+val(cvd.get('futures_imbalance_pct'), '%'),
                  'OKX спот, Δ 5м: '+val(cvd.get('spot_delta_quote'), ' USDT')+
                  ' · дисбаланс '+val(cvd.get('spot_imbalance_pct'), '%')]
    else:
        f_delta=_num(d.get('futures_taker_5m_delta_quote'))
        f_imb=_num(d.get('futures_taker_5m_imbalance_pct'))
        s_delta=_num(d.get('spot_taker_5m_delta_quote'))
        s_imb=_num(d.get('spot_taker_5m_imbalance_pct'))
        raw_flow=any(x is not None for x in (f_delta,f_imb,s_delta,s_imb))
        lines.append('Дельта сделок 5м: '+(
            'свежий кэш, ждём полного подтверждения'
            if raw_flow else 'НЕТ ДАННЫХ')+' (не полный CVD)')
        lines += ['OKX фьючерсы, Δ 5м: '+val(f_delta, ' USDT')+
                  ' · дисбаланс '+val(f_imb, '%'),
                  'OKX спот, Δ 5м: '+val(s_delta, ' USDT')+
                  ' · дисбаланс '+val(s_imb, '%')]
    liq=d.get('liquidation_context') or {}
    if isinstance(liq,dict):
        lo=_num(liq.get('long_usdt'))
        sh=_num(liq.get('short_usdt'))
        if liq.get('available') is True and lo is not None and sh is not None:
            lines.append(f'Ликвидации (частично), 5м: LONG ${lo:,.0f} / SHORT ${sh:,.0f}')
        else:
            lines.append('Ликвидации: данных недостаточно')
        sources=liq.get('sources') or {}
        if isinstance(sources,dict):
            active=[str(k) for k,v in sources.items() if v=='OBSERVED_PARTIAL']
            if active:
                lines.append('Источники ликвидаций: '+escape(', '.join(active)))
        if liq.get('available') and lo==0 and sh==0:
            lines.append('В доступной выборке событий нет; это не весь рынок.')
    funding=d.get('funding_context') or {}
    if isinstance(funding,dict):
        pct=_num(funding.get('funding_pct'))
        lines.append(f'Funding: {pct:+.4f}%' if pct is not None and funding.get('available') is True
                     else 'Funding: нет данных')
        if funding.get('available') is True:
            age=_num(funding.get('age_sec'))
            lines.append('Funding: OKX, за период биржи'+(f' · возраст {age:.0f} с' if age is not None else ''))
            when=_num(funding.get('funding_at'))
            if when is not None:
                lines.append('Расчёт funding: '+datetime.fromtimestamp(when,ZoneInfo('Europe/Warsaw')).strftime('%d.%m %H:%M')+' (Польша)')
    return lines or ['Дополнительные данные: пока недоступны']


def message(event):
    kind = event['kind']
    at = datetime.fromtimestamp(event['at'], ZoneInfo('Europe/Warsaw')).strftime('%d.%m %H:%M:%S')
    lines = [TITLES[kind], '<b>' + escape(event['symbol']) + '</b>',
             'Наблюдение: ' + at + ' (Польша)',
             f"Рост от открытия 24ч до максимума: +{event['rise_pct']:.2f}%",
             f"Откат от максимума: {event['retrace_pct']:.2f}%"]
    if kind == 'CANCELLED':
        lines.append(CANCEL_REASONS[event['reason']])
        lines.append('Предыдущий сценарий больше не актуален. Это не исполнение сделки.')
    elif kind == 'PRE_SHORT':
        lines += ['⚠️ Ранний SHORT-сценарий: подтверждены Futures + OI + структура + продавец.',
                  'Спотовые данные пока не готовы — это не отменяет сценарий, но полного подтверждения ещё нет.',
                  'Риск выше, чем у подтверждённого шорта. Не догоняем движение; ждём спотовое подтверждение или повторную проверку уровня.']
    elif kind == 'SHORT_CONFIRMED':
        lines += ['✅ Продавцы подтверждены: Futures + Spot + OI + структура.',
                  '⏳ Вход не догоняем. Ждём отдельный закрытый 5м ретест.',
                  'Шорт подтверждён как сценарий, но зоны входа ещё нет.']
    elif kind in ('ENTRY_ZONE','READY_RESEARCH'):
        try:
            d=json.loads(event.get('evidence_json') or '{}')
        except (ValueError,TypeError):
            d={}
        z=d.get('entry_zone') if isinstance(d,dict) else {}
        low=_num((z or {}).get('zone_low'))
        high=_num((z or {}).get('zone_high'))
        zone=(f'{low:.8g} — {high:.8g}'
              if low is not None and high is not None
              else f"{event['entry']:.8g}")
        lines += ['✅ Пробой подтверждён, закрытый 5м ретест отклонён продавцами.',
                  '✅ Futures + Spot + OI повторно подтверждают давление вниз.',
                  f"🎯 Зона входа SHORT: {zone}",
                  f"Текущая расчётная цена: {event['entry']:.8g}",
                  f"🛑 Стоп / отмена: {event['stop']:.8g}",
                  f"Цель 1: {event['tp1']:.8g} | цель 2: {event['tp2']:.8g}",
                  'Бот ордер не выставляет. Это зона входа по модели, не гарантия падения.']
    else:
        lines.append('Наблюдение, вход SHORT ещё не подтверждён.')
    if kind != 'CANCELLED':
        lines.extend(_research_lines(event))
    return '\n'.join(lines)


def _valid_plan(e):
    try:
        values = [_num(e[k]) for k in ('entry', 'stop', 'tp1', 'tp2')]
        entry, stop, tp1, tp2 = values
        ev = json.loads(e.get('evidence_json') or '{}')
        flow = ev.get('order_flow') or {}
        return (all(v is not None and v > 0 for v in values)
                and tp2 < tp1 < entry < stop
                and flow.get('ready') is True and flow.get('status') == 'CONFIRMED'
                and flow.get('source') == 'PUMPDUMP_LOCAL_TAKER_FLOW')
    except (ValueError, TypeError, KeyError, AttributeError):
        return False


def deliver(sender, tickers, now=None, db_path=None):
    """Consume latest valid stages; ACK only True. At most three sends per scan."""
    now = time.time() if now is None else float(now)
    path = db_path or DB_PATH
    prices = {}
    for t in tickers or ():
        inst = str(t.get('instId') or '')
        px = _num(t.get('last'))
        if inst.endswith('-USDT-SWAP') and px is not None and px > 0:
            prices[inst[:-10]+'USDT'] = px
    with closing(sqlite3.connect(path, timeout=5)) as db:
        db.row_factory = sqlite3.Row
        _setup(db, now)
        db.commit()
        cursor = db.execute("SELECT value FROM post_pump_notify_meta WHERE key='cursor'").fetchone()[0]
        events = db.execute('SELECT * FROM post_pump_short_strategy_audit WHERE id>? ORDER BY id LIMIT 500', (cursor,)).fetchall()
        for row in events:
            e = dict(row)
            kind = 'CANCELLED' if e['reason'] in CANCEL_REASONS else e['phase']
            if kind in TITLES and 0 <= now-e['at'] <= 600:
                e['kind'] = kind
                if kind not in ('ENTRY_ZONE','READY_RESEARCH') or _valid_plan(e):
                    db.execute('''INSERT OR IGNORE INTO post_pump_notify_outbox
                        (audit_id,symbol,episode_at,kind,payload) VALUES (?,?,?,?,?)''',
                        (e['id'],e['symbol'],e['episode_at'],kind,json.dumps(e)))
            cursor = e['id']
        db.execute("UPDATE post_pump_notify_meta SET value=? WHERE key='cursor'", (cursor,))
        db.commit()
        pending = db.execute("SELECT * FROM post_pump_notify_outbox WHERE status='PENDING' ORDER BY audit_id").fetchall()
        sent = 0
        for row in pending:
            e = json.loads(row['payload'])
            latest = db.execute('''SELECT id FROM post_pump_short_strategy_audit
                WHERE symbol=? AND episode_at=? ORDER BY id DESC LIMIT 1''',
                (row['symbol'],row['episode_at'])).fetchone()
            state = db.execute('SELECT * FROM post_pump_short_strategy WHERE symbol=?', (row['symbol'],)).fetchone()
            ttl = 180 if row['kind'] == 'ENTRY_ZONE' else 120 if row['kind'] == 'READY_RESEARCH' else 600
            valid = (latest is not None and latest['id'] == row['audit_id']
                     and state is not None and state['episode_at'] == row['episode_at']
                     and 0 <= now-e['at'] <= ttl)
            if row['kind'] in ('ENTRY_ZONE','READY_RESEARCH'):
                px = prices.get(row['symbol'])
                phase_ok=(state['phase']==row['kind'])
                zone_ok=False
                if px is not None:
                    try:
                        evidence=json.loads(e.get('evidence_json') or '{}')
                    except (ValueError,TypeError):
                        evidence={}
                    zone=(evidence.get('entry_zone') or {}
                          if isinstance(evidence,dict) else {})
                    low=_num(zone.get('zone_low'))
                    high=_num(zone.get('zone_high'))
                    zone_ok=(low is not None and high is not None
                             and low<=px<=high)
                    if row['kind']=='READY_RESEARCH' and low is None:
                        zone_ok=abs(px/e['entry']-1)<=.003
                valid=(valid and phase_ok and px is not None and zone_ok
                       and e['tp1']<px<e['stop'])
            if row['kind'] == 'CANCELLED':
                # Never notify cancellation of a scenario the user has not seen.
                prior = db.execute('''SELECT 1 FROM post_pump_notify_outbox
                    WHERE symbol=? AND episode_at=? AND status IN ('SENT','UNKNOWN')
                    AND kind!='CANCELLED' AND audit_id > COALESCE((
                        SELECT MAX(audit_id) FROM post_pump_notify_outbox
                        WHERE symbol=? AND episode_at=? AND kind='CANCELLED'
                        AND status IN ('SENT','UNKNOWN')),0) LIMIT 1''',
                    (row['symbol'],row['episode_at'],row['symbol'],row['episode_at'])).fetchone()
                valid = valid and prior is not None
            if not valid:
                db.execute("UPDATE post_pump_notify_outbox SET status='EXPIRED' WHERE audit_id=?", (row['audit_id'],))
                db.commit()
                continue
            if row['retry_at'] > now or sent >= 3:
                continue
            # Commit a reservation before network IO. A second worker can't send it.
            claimed = db.execute("""UPDATE post_pump_notify_outbox SET status='SENDING',
                retry_at=?,attempts=attempts+1 WHERE audit_id=? AND status='PENDING'""",
                (now+120,row['audit_id'])).rowcount
            db.commit()
            if not claimed:
                continue
            try:
                result = sender(message(e), symbol=row['symbol'])
            except Exception:
                result = None
            status = 'SENT' if result is True else 'PENDING' if result is False else 'UNKNOWN'
            db.execute('''UPDATE post_pump_notify_outbox SET status=?,retry_at=?,sent_at=?
                WHERE audit_id=?''', (status,now+60,now if result is True else None,row['audit_id']))
            db.commit()
            sent += 1
            print('[POST_PUMP_NOTIFY]',row['symbol'],'stage=',row['kind'],
                  'status=',status,'audit_id=',row['audit_id'],flush=True)
        return sent
