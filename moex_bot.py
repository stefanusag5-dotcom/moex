import os
import json
import math
import asyncio
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

import pandas as pd
import numpy as np
import pandas_ta as ta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

try:
    from groq import Groq
except ImportError:
    Groq = None

load_dotenv()

# ══════════════════════════════════════════════
# ENV & CONFIG
# ══════════════════════════════════════════════
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TINKOFF_TOKEN = os.getenv("TINKOFF_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
PORT = int(os.getenv("PORT", 8080))

groq_client = Groq(api_key=GROQ_API_KEY) if Groq and GROQ_API_KEY else None
GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("moex_railway_bot")

TINKOFF_API = "https://invest-public-api.tinkoff.ru/rest"

# ══════════════════════════════════════════════
# STOCKS & SECTORS
# ══════════════════════════════════════════════
MOEX_STOCKS = {
    # Нефтегаз
    "GAZP":  ("BBG004730RP0", "Газпром",               "нефтегаз"),
    "LKOH":  ("BBG004731032", "Лукойл",                "нефтегаз"),
    "ROSN":  ("BBG004731354", "Роснефть",              "нефтегаз"),
    "NVTK":  ("BBG00475KKY8", "Новатэк",               "нефтегаз"),
    "TATN":  ("BBG004731642", "Татнефть",              "нефтегаз"),
    "TATNP": ("BBG004731706", "Татнефть п.",            "нефтегаз"),
    "SNGS":  ("BBG004730JJ5", "Сургутнефтегаз",        "нефтегаз"),
    "SNGSP": ("BBG0047315Y7", "Сургутнефтегаз п.",     "нефтегаз"),
    "TRNFP": ("BBG00475KHX6", "Транснефть п.",          "нефтегаз"),
    "BANE":  ("BBG004S68758", "Башнефть",              "нефтегаз"),
    "BANEP": ("BBG004S687B4", "Башнефть п.",            "нефтегаз"),
    # Банки и финансы
    "SBER":  ("BBG004730N88", "Сбербанк",              "банки"),
    "SBERP": ("BBG004730N96", "Сбербанк п.",            "банки"),
    "VTBR":  ("BBG004730ZJ9", "ВТБ",                   "банки"),
    "BSPB":  ("BBG0029SNB14", "БСП",                   "банки"),
    "CBOM":  ("BBG009GSYN76", "МКБ",                   "банки"),
    "MOEX":  ("BBG004730JJ5", "МосБиржа",              "финансы"),
    "TCSG":  ("BBG00QPYJ5H0", "Т-Банк (TCS)",          "банки"),
    "SVCB":  ("BBG00F9XX7H4", "Совкомбанк",            "банки"),
    # Металлы и горнодобыча
    "GMKN":  ("BBG004731489", "Норникель",             "металлы"),
    "CHMF":  ("BBG00475JZZ6", "Северсталь",            "металлы"),
    "NLMK":  ("BBG004S68BH6", "НЛМК",                 "металлы"),
    "MAGN":  ("BBG004S68507", "ММК",                   "металлы"),
    "RUAL":  ("BBG008F2T3T2", "РусАл",                 "металлы"),
    "ENPG":  ("BBG00F6NK0J8", "Эн+ Груп",              "металлы"),
    "ALRS":  ("BBG004S68B31", "Алроса",                "горнодобыча"),
    "POLY":  ("BBG004PYF2N3", "Полюс",                 "золото"),
    "PLZL":  ("BBG000R607Y3", "Полюс Золото",          "золото"),
    "RASP":  ("BBG00475M5R0", "Распадская",            "уголь"),
    "MTLR":  ("BBG004S68598", "Мечел",                 "уголь"),
    "MTLRP": ("BBG004S68716", "Мечел п.",               "уголь"),
    # IT и телеком
    "YNDX":  ("BBG006L8G4H1", "Яндекс",               "IT"),
    "VKCO":  ("BBG00178PGX3", "ВКонтакте",             "IT"),
    "POSI":  ("BBG01FD18M82", "Позитив",               "кибербезопасность"),
    "ASTR":  ("BBG016S3QJ60", "Астра",                 "IT"),
    "HEAD":  ("BBG00DHTYPH4", "HeadHunter",            "IT"),
    "CIAN":  ("BBG009S39JX6", "ЦИАН",                  "IT"),
    "MTSS":  ("BBG004S68473", "МТС",                   "телеком"),
    "RTKM":  ("BBG004S681B4", "Ростелеком",            "телеком"),
    "RTKMP": ("BBG004S68696", "Ростелеком п.",          "телеком"),
    # Ритейл и потребительский
    "MGNT":  ("BBG004RVFCY3", "Магнит",                "ритейл"),
    "FIVE":  ("BBG00JXPFBN0", "X5 Group",              "ритейл"),
    "OZON":  ("BBG00Y91R9T3", "Ozon",                  "e-commerce"),
    "LENT":  ("BBG00264RNXT", "Лента",                 "ритейл"),
    "MDMG":  ("BBG001M2SC01", "MD Medical (Мать и дитя)",  "медицина"),
    "FIXP":  ("BBG00ZHCX1X2", "Fix Price",             "ритейл"),
    # Энергетика
    "FEES":  ("BBG00475K6C3", "ФСК ЕЭС",              "энергетика"),
    "HYDR":  ("BBG00475K2X9", "РусГидро",              "энергетика"),
    "IRAO":  ("BBG004S68829", "Интер РАО",             "энергетика"),
    "OGKB":  ("BBG004S686G4", "ОГК-2",                "энергетика"),
    "MSNG":  ("BBG004S686W0", "Мосэнерго",             "энергетика"),
    "TGKA":  ("BBG004S68C23", "ТГК-1",                "энергетика"),
    # Транспорт и инфраструктура
    "AFLT":  ("BBG004S683W7", "Аэрофлот",              "транспорт"),
    "FLOT":  ("BBG000R04X57", "Совкомфлот",            "транспорт"),
    "GLTR":  ("BBG000VFX6Y4", "Globaltrans",           "транспорт"),
    # Девелопмент
    "PIKK":  ("BBG004S68BF0", "ПИК",                   "девелопмент"),
    "SMLT":  ("BBG005D1WCQ1", "Самолёт",               "девелопмент"),
    "LSRG":  ("BBG0040F7B78", "ЛСР",                   "девелопмент"),
    "ETLN":  ("BBG00475JZY3", "Эталон",                "девелопмент"),
    # Удобрения и химия
    "PHOR":  ("BBG004S689R0", "ФосАгро",               "химия"),
    "KAZT":  ("BBG004S68614", "КуйбышевАзот",          "химия"),
    "NKNC":  ("BBG004S681N6", "Нижнекамскнефтехим",    "химия"),
    # Прочее — 2 эшелон
    "SGZH":  ("BBG0100R9963", "Сегежа",                "лесопромышленность"),
    "UWGN":  ("BBG008HD3V85", "ОВК",                   "машиностроение"),
    "GCHE":  ("BBG000RP9B63", "Черкизово",             "АПК"),
    "AGRO":  ("BBG005Y6BNR6", "РусАгро",               "АПК"),
    "SFIN":  ("BBG00HGDC7N7", "ЭсЭфАй",               "финансы"),
    "MFGS":  ("BBG004S68BR5", "Мегафон",               "телеком"),
}

# ══════════════════════════════════════════════
# ФЬЮЧЕРСЫ FORTS (срочный рынок MOEX)
# ══════════════════════════════════════════════
# Формат: code -> (figi, name, category, tick_size, lot_value)
# FIGI актуализируются через /update_figi
# Ближний контракт — обновляй раз в месяц при экспирации
# FUTURES: FIGI обновляются через /update_figi после деплоя.
# Тикеры — стандартные коды MOEX FORTS ближнего контракта.
# При экспирации (раз в квартал) обновляй тикеры вручную или через /update_figi.
FUTURES = {
    # ── Валюта ────────────────────────────────────────────────────────────
    "SiM6":  ("FUTSI0626000", "Доллар/Рубль (Si)",    "валюта",   1.0,    1000),
    "EuM6":  ("FUTEU0626000", "Евро/Рубль (Eu)",      "валюта",   1.0,    1000),
    # ── Индексы ───────────────────────────────────────────────────────────
    "RIM6":  ("FUTRI0626000", "Индекс РТС (Ri)",      "индекс",   10.0,   1),
    "MXM6":  ("FUTMX0626000", "Индекс MOEX (MIX)",    "индекс",   0.25,   1),
    # ── Товары ────────────────────────────────────────────────────────────
    "BRM6":  ("FUTBR0626000", "Нефть Brent (BR)",     "товар",    0.01,   10),
    "GDM6":  ("FUTGD0626000", "Золото (Gold)",         "товар",    0.1,    1),
    "NGM6":  ("FUTNG0626000", "Природный газ (NG)",    "товар",    0.001,  1),
    "SRM6":  ("FUTSR0626000", "Серебро (Silver)",      "товар",    0.01,   1),
    # ── Акционные фьючерсы ────────────────────────────────────────────────
    "SBM6":  ("FUTSB0626000", "Сбербанк (SBER)",      "акция",    1.0,    100),
    "GZM6":  ("FUTGZ0626000", "Газпром (GAZR)",        "акция",    1.0,    100),
    "LKM6":  ("FUTLK0626000", "Лукойл (LKOH)",         "акция",    1.0,    10),
    "GKM6":  ("FUTGK0626000", "Норникель (GMKN)",      "акция",    1.0,    10),
    "RNM6":  ("FUTRN0626000", "Роснефть (ROSN)",       "акция",    1.0,    100),
    "NKM6":  ("FUTNK0626000", "Новатэк (NVTK)",        "акция",    1.0,    10),
    "VBM6":  ("FUTVB0626000", "ВТБ (VTBR)",            "акция",    0.5,    10000),
    "MNM6":  ("FUTMN0626000", "МТС (MTSS)",            "акция",    1.0,    100),
    "AFM6":  ("FUTAF0626000", "Аэрофлот (AFLT)",       "акция",    1.0,    1000),
}

# Watchlist фьючерсов — отдельный файл
FUTURES_WATCHLIST_FILE = Path("futures_watchlist.json")

def load_futures_watchlist() -> list[str]:
    try:
        if FUTURES_WATCHLIST_FILE.exists():
            data = json.loads(FUTURES_WATCHLIST_FILE.read_text())
            return [t for t in data if t in FUTURES]
    except Exception:
        pass
    # Дефолт — самые ликвидные
    return ["SiZ5", "RIZ5", "BRZ5", "GDZ5", "SBZ5", "GZZ5"]

def save_futures_watchlist(tickers: list[str]):
    FUTURES_WATCHLIST_FILE.write_text(json.dumps(tickers, ensure_ascii=False))

SECTOR_KEYWORDS = {
    "нефтегаз":        ["нефть", "газ", "brent", "urals", "опек", "opec", "экспорт нефти",
                        "пошлина нефть", "санкции нефть", "нефтяные"],
    "металлы":         ["сталь", "алюминий", "никель", "медь", "metal", "экспорт металл",
                        "пошлина металл", "санкции металл"],
    "банки":           ["ключевая ставка", "цб рф", "ставка", "ипотека", "резервы", "прибыль банк"],
    "IT":              ["it", "технологии", "санкции it", "импортозамещение"],
    "золото":          ["золото", "gold", "драгметалл"],
    "энергетика":      ["электроэнергия", "тариф", "энергосбыт"],
    "девелопмент":     ["ипотека", "льготная ипотека", "недвижимость", "строительство"],
    "ритейл":          ["ритейл", "торговля", "потребительский"],
    "telecom":         ["связь", "телеком", "5g", "тариф связь"],
    "транспорт":       ["транспорт", "авиация", "санкции авиа", "фрахт"],
    "горнодобыча":     ["алмазы", "добыча", "санкции добыча"],
    "уголь":           ["уголь", "coal", "экспорт уголь"],
    "лесопромышленность": ["лес", "целлюлоза", "бумага", "лесозаготовка"],
    "e-commerce":      ["электронная торговля", "маркетплейс", "онлайн торговля"],
    "финансы":         ["биржа", "торги", "ликвидность рынок"],
}

MARKET_KEYWORDS = [
    "цб рф", "ключевая ставка", "минфин", "минэкономразвития",
    "санкции", "нефть", "газ", "курс рубля", "рубль",
    "дивиденды", "байбек", "buyback", "допэмиссия", "сделка слияние",
    "ввп россия", "инфляция россия",
]

TF_MAP = {
    "5m":  ("CANDLE_INTERVAL_5_MIN",      5,  300),
    "15m": ("CANDLE_INTERVAL_15_MIN",    15,  300),
    "1h":  ("CANDLE_INTERVAL_HOUR",      60,  200),
    "4h":  ("CANDLE_INTERVAL_4_HOUR",   240,  200),
    "1d":  ("CANDLE_INTERVAL_DAY",     1440,  300),
    "1w":  ("CANDLE_INTERVAL_WEEK",   10080,  100),
}
DEFAULT_TF = "15m"
INTRADAY_TFS = {"5m", "15m", "1h"}

TRADE_MODES = {
    "low": {
        "label": "🟢 LOW (консерватив)",
        "rsi_oversold": 30, "rsi_overbought": 70,
        "min_score": 80,
        "min_vol_ratio": 1.8,
        "min_news_score": 5,
        "personality": "Консервативный трейдер. Только чёткие сигналы.",
    },
    "mid": {
        "label": "🟡 MID (баланс)",
        "rsi_oversold": 35, "rsi_overbought": 65,
        "min_score": 70,
        "min_vol_ratio": 1.3,
        "min_news_score": 6,
        "personality": "Сбалансированный трейдер на MOEX.",
    },
    "hard": {
        "label": "🔴 HARD (агрессив)",
        "rsi_oversold": 40, "rsi_overbought": 60,
        "min_score": 60,
        "min_vol_ratio": 1.1,
        "min_news_score": 5,
        "personality": "Агрессивный трейдер. Больше сигналов.",
    },
}

_cache: dict = {}
_news_cache: dict = {}

TRADES_FILE = Path("open_trades.json")
SCANNER_FILE = Path("scanner_state.json")
WATCHLIST_FILE = Path("watchlist.json")


def esc(text: str) -> str:
    """Экранирует спецсимволы HTML для Telegram parse_mode=HTML."""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


# ══════════════════════════════════════════════
# TRADE MANAGER (из trade_manager.py)
# ══════════════════════════════════════════════
def load_trades() -> dict:
    try:
        return json.loads(TRADES_FILE.read_text()) if TRADES_FILE.exists() else {}
    except Exception:
        return {}


def save_trades(d: dict):
    TRADES_FILE.write_text(json.dumps(d, indent=2, ensure_ascii=False))


def open_trade(ticker: str, direction: str, entry: float,
               sl: float, tp1: float, tp2: float, tp3: float,
               chat_id: int, tf: str = "15m") -> str:
    """Открывает сделку, возвращает trade_id."""
    trades = load_trades()
    trade_id = f"{ticker}_{int(time.time())}"
    trades[trade_id] = {
        "ticker":          ticker,
        "direction":       direction.upper(),
        "entry":           entry,
        "sl":              sl,
        "sl_original":     sl,
        "tp1":             tp1,
        "tp2":             tp2,
        "tp3":             tp3,
        "status":          "open",
        "chat_id":         chat_id,
        "opened_at":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "tf":              tf,
        "sl_moved_to_be":  False,
        "sl_moved_to_tp1": False,
        "high_price":      entry,
        "low_price":       entry,
    }
    save_trades(trades)
    return trade_id


def close_trade(trade_id: str, reason: str, price: float) -> dict | None:
    trades = load_trades()
    if trade_id not in trades:
        return None
    t = trades[trade_id]
    t["status"] = "closed"
    t["close_reason"] = reason
    t["close_price"] = price
    t["closed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    if t["direction"] == "LONG":
        pnl_pct = (price - t["entry"]) / t["entry"] * 100
    else:
        pnl_pct = (t["entry"] - price) / t["entry"] * 100
    t["pnl_pct"] = round(pnl_pct, 2)

    save_trades(trades)
    return t


def format_trade_status(t: dict) -> str:
    """Форматирует текущий статус сделки для Telegram."""
    direction_e = "🟩 LONG" if t["direction"] == "LONG" else "🟥 SHORT"
    status_map = {
        "open":      "🔵 Открыта",
        "tp1_hit":   "🟡 TP1 достигнут",
        "tp2_hit":   "🟠 TP2 достигнут",
        "closed":    "⚫ Закрыта",
    }
    status = status_map.get(t["status"], t["status"])
    sl_note = ""
    if t.get("sl_moved_to_tp1"):
        sl_note = " (SL на TP1)"
    elif t.get("sl_moved_to_be"):
        sl_note = " (SL в б/у)"

    lines = [
        f"<b>{t['ticker']}</b> {direction_e} | {status}",
        f"Вход: {t['entry']:,.2f} ₽  |  TF: {t['tf']}",
        f"SL: {t['sl']:,.2f} ₽{sl_note}",
        f"TP1: {t['tp1']:,.2f} ₽  TP2: {t['tp2']:,.2f} ₽  TP3: {t['tp3']:,.2f} ₽",
        f"Открыта: {t['opened_at'][:16].replace('T', ' ')}",
    ]
    if t.get("close_price"):
        pnl = t.get("pnl_pct", 0)
        pnl_e = "📈" if pnl >= 0 else "📉"
        lines.append(f"Закрыта по {t['close_price']:,.2f} ₽  {pnl_e} {pnl:+.2f}%")
    return "\n".join(lines)


async def _check_trade(trade_id: str, t: dict, price: float) -> str | None:
    """Проверяет уровни для одной сделки. Обновляет SL. Возвращает alert или None."""
    trades = load_trades()
    if trade_id not in trades:
        return None

    direction = t["direction"]
    entry = t["entry"]
    sl = t["sl"]
    tp1 = t["tp1"]
    tp2 = t["tp2"]
    tp3 = t["tp3"]
    status = t["status"]
    is_long = direction == "LONG"
    changed = False

    # Обновляем high/low
    if is_long:
        trades[trade_id]["high_price"] = max(t.get("high_price", entry), price)
    else:
        trades[trade_id]["low_price"] = min(t.get("low_price", entry), price)

    # ── Проверка SL ───────────────────────────────────────
    sl_hit = (is_long and price <= sl) or (not is_long and price >= sl)
    if sl_hit:
        closed = close_trade(trade_id, "SL", price)
        pnl = closed.get("pnl_pct", 0) if closed else 0
        pnl_e = "📈" if pnl >= 0 else "📉"
        sl_type = "безубыток" if t.get("sl_moved_to_be") else ("TP1" if t.get("sl_moved_to_tp1") else "стоп")
        return (
            f"🛑 <b>SL сработал — {t['ticker']} {direction}</b>\n"
            f"Цена: {price:,.2f} ₽ | SL ({sl_type}): {sl:,.2f} ₽\n"
            f"{pnl_e} Результат: {pnl:+.2f}%\n"
            f"<i>Сделка закрыта.</i>"
        )

    # ── Проверка TP3 ──────────────────────────────────────
    tp3_hit = (is_long and price >= tp3) or (not is_long and price <= tp3)
    if tp3_hit and status in ("open", "tp1_hit", "tp2_hit"):
        closed = close_trade(trade_id, "TP3", price)
        pnl = closed.get("pnl_pct", 0) if closed else 0
        return (
            f"🎯🎯🎯 <b>TP3 достигнут — {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP3: {tp3:,.2f} ₽\n"
            f"📈 Результат: +{pnl:.2f}%\n"
            f"<b>Отличная сделка!</b>"
        )

    # ── Проверка TP2 ──────────────────────────────────────
    tp2_hit = (is_long and price >= tp2) or (not is_long and price <= tp2)
    if tp2_hit and status == "tp1_hit":
        trades[trade_id]["status"] = "tp2_hit"
        if not t.get("sl_moved_to_tp1"):
            trades[trade_id]["sl"] = tp1
            trades[trade_id]["sl_moved_to_tp1"] = True
            changed = True
        save_trades(trades)
        sl_note = f"\n📌 SL перенесён на TP1 ({tp1:,.2f} ₽)" if changed else ""
        return (
            f"🎯🎯 <b>TP2 достигнут — {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP2: {tp2:,.2f} ₽\n"
            f"Удерживаем позицию, цель TP3: {tp3:,.2f} ₽{sl_note}"
        )

    # ── Проверка TP1 ──────────────────────────────────────
    tp1_hit = (is_long and price >= tp1) or (not is_long and price <= tp1)
    if tp1_hit and status == "open":
        trades[trade_id]["status"] = "tp1_hit"
        if not t.get("sl_moved_to_be"):
            trades[trade_id]["sl"] = entry
            trades[trade_id]["sl_moved_to_be"] = True
            changed = True
        save_trades(trades)
        sl_note = f"\n📌 SL перенесён в безубыток ({entry:,.2f} ₽)" if changed else ""
        return (
            f"🎯 <b>TP1 достигнут — {t['ticker']}!</b>\n"
            f"Цена: {price:,.2f} ₽ | TP1: {tp1:,.2f} ₽\n"
            f"Ждём TP2: {tp2:,.2f} ₽{sl_note}"
        )

    if changed:
        save_trades(trades)
    return None


async def _force_close_all(app, fetch_price_fn, moex_stocks):
    """Принудительно закрывает все открытые сделки в конце сессии."""
    trades = load_trades()
    open_trades = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}
    if not open_trades:
        return
    for trade_id, t in open_trades.items():
        ticker = t["ticker"]
        figi = moex_stocks.get(ticker, ("",))[0]
        price = await fetch_price_fn(figi) if figi else t["entry"]
        if not price:
            price = t["entry"]
        closed = close_trade(trade_id, "EOD", price)
        pnl = closed.get("pnl_pct", 0) if closed else 0
        pnl_e = "📈" if pnl >= 0 else "📉"
        try:
            await app.bot.send_message(
                t["chat_id"],
                f"🕐 <b>Сессия закрывается — {ticker} закрыт по рынку</b>\n"
                f"Цена закрытия: {price:,.2f} ₽\n"
                f"{pnl_e} Результат: {pnl:+.2f}%",
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning(f"EOD close alert: {e}")


async def monitor_trades_loop(app, fetch_price_fn):
    """Фоновый цикл мониторинга открытых сделок."""
    logger.info("Trade monitor started")
    while True:
        try:
            trades = load_trades()
            open_trades = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}

            if open_trades:
                for trade_id, t in list(open_trades.items()):
                    ticker = t["ticker"]
                    if ticker not in MOEX_STOCKS:
                        continue
                    figi = MOEX_STOCKS[ticker][0]
                    price = await fetch_price_fn(figi)
                    if not price:
                        continue
                    alert_msg = await _check_trade(trade_id, t, price)
                    if alert_msg:
                        try:
                            await app.bot.send_message(t["chat_id"], alert_msg, parse_mode="HTML")
                        except Exception as e:
                            logger.warning(f"Alert send failed: {e}")

            # Проверяем закрытие сессии — 23:48 МСК
            now_msk_hour = (datetime.now(timezone.utc).hour + 3) % 24
            now_msk_min = datetime.now(timezone.utc).minute
            if now_msk_hour == 23 and now_msk_min >= 48:
                await _force_close_all(app, fetch_price_fn, MOEX_STOCKS)

        except Exception as e:
            logger.error(f"monitor_trades_loop: {e}")

        await asyncio.sleep(120)


# ══════════════════════════════════════════════
# WATCHLIST
# ══════════════════════════════════════════════
def load_watchlist() -> list[str]:
    try:
        if WATCHLIST_FILE.exists():
            data = json.loads(WATCHLIST_FILE.read_text())
            return [t.upper() for t in data if t.upper() in MOEX_STOCKS]
    except Exception:
        pass
    return ["SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "YNDX", "TATN", "CHMF", "NLMK", "MOEX", "VTBR", "MGNT", "FIVE", "AFLT"]


def save_watchlist(tickers: list[str]):
    WATCHLIST_FILE.write_text(json.dumps(tickers, ensure_ascii=False))


def add_to_watchlist(ticker: str) -> tuple[bool, str]:
    ticker = ticker.upper().strip()
    if ticker not in MOEX_STOCKS:
        similar = [t for t in MOEX_STOCKS if t.startswith(ticker[:3])]
        hint = f" Похожие: {', '.join(similar[:4])}" if similar else ""
        return False, f"❌ Тикер {ticker} не найден в базе.{hint}"
    wl = load_watchlist()
    if ticker in wl:
        return False, f"ℹ️ {ticker} уже в ватчлисте."
    if len(wl) >= 100:
        return False, "⚠️ Максимум 100 инструментов. Сначала удали что-нибудь: /remove TICKER"
    wl.append(ticker)
    save_watchlist(wl)
    _, name, sector = MOEX_STOCKS[ticker]
    return True, f"✅ <b>{ticker}</b> ({name}, {sector}) добавлен в ватчлист.\nВсего: {len(wl)}/100"


def remove_from_watchlist(ticker: str) -> tuple[bool, str]:
    ticker = ticker.upper().strip()
    wl = load_watchlist()
    if ticker not in wl:
        return False, f"ℹ️ {ticker} не найден в твоём ватчлисте."
    wl.remove(ticker)
    save_watchlist(wl)
    return True, f"🗑 <b>{ticker}</b> удалён из ватчлиста. Осталось: {len(wl)}"


# ══════════════════════════════════════════════
# TIME FILTERS
# ══════════════════════════════════════════════
def get_msk_time() -> tuple[int, int, int]:
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    return now_msk.weekday(), now_msk.hour, now_msk.minute


def is_acceptable_entry_time(tf: str) -> tuple[bool, str]:
    if tf not in INTRADAY_TFS:
        return True, ""
    _, hour, minute = get_msk_time()
    current_minutes = hour * 60 + minute
    close_minutes = 23 * 60 + 50
    remaining_minutes = close_minutes - current_minutes

    if remaining_minutes <= 0:
        return False, "Сессия уже закрыта."
    if tf == "1h" and remaining_minutes < 180:
        return False, f"⚠️ До закрытия сессии менее 3 часов ({remaining_minutes} мин)."
    elif tf == "15m" and remaining_minutes < 90:
        return False, f"⚠️ До закрытия сессии менее 1.5 часов ({remaining_minutes} мин)."
    elif tf == "5m" and remaining_minutes < 45:
        return False, f"⚠️ До закрытия сессии менее 45 минут."
    return True, ""


# ══════════════════════════════════════════════
# TINKOFF API
# ══════════════════════════════════════════════
def _tinkoff_headers() -> dict:
    return {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }


async def fetch_candles_tinkoff(figi: str, interval: str, limit: int) -> pd.DataFrame | None:
    cache_key = f"candles_{figi}_{interval}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 120:
        return _cache[cache_key]["df"]

    interval_minutes = {
        "CANDLE_INTERVAL_1_MIN": 1, "CANDLE_INTERVAL_5_MIN": 5,
        "CANDLE_INTERVAL_15_MIN": 15, "CANDLE_INTERVAL_HOUR": 60,
        "CANDLE_INTERVAL_4_HOUR": 240, "CANDLE_INTERVAL_DAY": 1440,
        "CANDLE_INTERVAL_WEEK": 10080,
    }.get(interval, 1440)

    trading_minutes_per_day = 830
    trading_days_needed = max(2, (limit * interval_minutes) // trading_minutes_per_day + 2)
    delta_days = int(trading_days_needed * 1.5) + 3
    delta_days = max(delta_days, 5)

    _utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    dt_from = _utcnow - timedelta(days=delta_days)
    dt_to = _utcnow

    body = {
        "figi": figi,
        "from": dt_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to": dt_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": interval,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        data = None
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for attempt in range(3):
                async with session.post(
                    f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
                    headers=_tinkoff_headers(), json=body,
                ) as r:
                    if r.status == 429:
                        wait = 2 ** attempt
                        logger.warning(f"Tinkoff 429 {figi}, retry {attempt+1} in {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    if r.status != 200:
                        logger.warning(f"Tinkoff candles {figi}: HTTP {r.status}")
                        return None
                    data = await r.json()
                    break
        if data is None:
            return None
    except Exception as e:
        logger.error(f"Tinkoff candles {figi}: {e}")
        return None

    candles = data.get("candles", [])
    if not candles:
        return None

    rows = []
    for c in candles:
        try:
            def units_nano(q):
                return float(q.get("units", 0)) + float(q.get("nano", 0)) / 1e9
            rows.append({
                "timestamp": pd.Timestamp(c["time"]),
                "open": units_nano(c["open"]),
                "high": units_nano(c["high"]),
                "low": units_nano(c["low"]),
                "close": units_nano(c["close"]),
                "volume": float(c.get("volume", 0)),
            })
        except Exception:
            continue

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
    df = df.tail(limit)
    _cache[cache_key] = {"df": df, "ts": now}
    return df


async def fetch_last_price_tinkoff(figi: str) -> float | None:
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices",
                headers=_tinkoff_headers(), json={"figi": [figi]},
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                prices = data.get("lastPrices", [])
                if prices:
                    p = prices[0]["price"]
                    return float(p.get("units", 0)) + float(p.get("nano", 0)) / 1e9
    except Exception as e:
        logger.warning(f"Tinkoff last price {figi}: {e}")
    return None


async def fetch_stock_data(ticker: str, tf: str = DEFAULT_TF):
    info = MOEX_STOCKS.get(ticker.upper())
    if not info:
        return None, None
    figi, name, sector = info
    interval, _, limit = TF_MAP.get(tf, TF_MAP[DEFAULT_TF])
    df = await fetch_candles_tinkoff(figi, interval, limit)
    return df, {"figi": figi, "name": name, "sector": sector, "ticker": ticker}


# ══════════════════════════════════════════════
# IMOEX REGIME (PROXY SBER)
# ══════════════════════════════════════════════
async def fetch_imoex_regime() -> dict:
    cache_key = "imoex_regime"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 1200:
        return _cache[cache_key]["val"]
    try:
        figi = MOEX_STOCKS["SBER"][0]
        df = await fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_DAY", 120)
        if df is None or len(df) < 50:
            raise ValueError("Недостаточно данных для IMOEX-прокси (SBER)")

        close = df["close"].values
        price = close[-1]
        s = pd.Series(close)
        ema20 = float(s.ewm(span=20).mean().iloc[-1])
        ema50 = float(s.ewm(span=50).mean().iloc[-1])
        ema50_arr = s.ewm(span=50).mean().values
        slope_10d = (ema50_arr[-1] - ema50_arr[-10]) / ema50_arr[-10] * 100
        slope_20d = (ema50_arr[-1] - ema50_arr[-20]) / ema50_arr[-20] * 100

        is_bull = price > ema20 > ema50 and slope_10d > 0.05
        is_bear = price < ema20 < ema50 and slope_10d < -0.05

        if is_bull:
            strong = slope_20d > 1.5
            regime = "bull"
            label = ("🟢🟢 IMOEX: сильный бычий тренд" if strong else "🟢 IMOEX: умеренный бычий тренд")
        elif is_bear:
            strong = slope_20d < -1.5
            regime = "bear"
            label = ("🔴🔴 IMOEX: нисходящий тренд" if strong else "🔴 IMOEX: локальная слабость рынка")
        else:
            regime = "neutral"
            label = "⚪ IMOEX: консолидация / боковой рынок"

        result = {
            "regime": regime, "label": label,
            "price": round(price, 2), "ema20": round(ema20, 2), "ema50": round(ema50, 2),
            "slope_10d": round(slope_10d, 3), "slope_20d": round(slope_20d, 3),
        }
        _cache[cache_key] = {"val": result, "ts": now}
        return result
    except Exception as e:
        logger.warning(f"fetch_imoex_regime: {e}")
        fallback = {"regime": "neutral", "label": "⚪ IMOEX: тренд неопределен",
                     "price": 0, "ema20": 0, "ema50": 0, "slope_10d": 0, "slope_20d": 0}
        _cache[cache_key] = {"val": fallback, "ts": now - 900}
        return fallback


# ══════════════════════════════════════════════
# VOLUME PROFILE
# ══════════════════════════════════════════════
def calculate_volume_profile(df: pd.DataFrame, num_bins: int = 100):
    price_min = df["low"].min()
    price_max = df["high"].max()
    if price_min >= price_max:
        return np.array([price_min]), np.array([df["volume"].sum()])
    bins = np.linspace(price_min, price_max, num_bins + 1)
    centers = (bins[:-1] + bins[1:]) / 2
    vp = np.zeros(num_bins)
    lows = df["low"].values
    highs = df["high"].values
    volumes = df["volume"].values
    lo_idx = np.clip(np.searchsorted(bins, lows, side="left") - 1, 0, num_bins - 1)
    hi_idx = np.clip(np.searchsorted(bins, highs, side="right") - 1, 0, num_bins - 1)
    for i in range(len(volumes)):
        lo, hi = lo_idx[i], hi_idx[i]
        if lo == hi:
            vp[lo] += volumes[i]
        else:
            vp[lo:hi + 1] += volumes[i] / (hi - lo + 1)
    return centers, vp


def find_hvn_lvn(df: pd.DataFrame, price: float, dist_limit_pct: float = 20.0):
    if len(df) < 20:
        return {}
    centers, vp = calculate_volume_profile(df)
    vp_mean = float(vp.mean())
    threshold_h = np.percentile(vp, 70)
    threshold_l = np.percentile(vp, 30)
    poc_idx = int(np.argmax(vp))
    poc = round(float(centers[poc_idx]), 2)
    hvn_above, hvn_below = None, None
    lvn_above, lvn_below = None, None
    for i in range(1, len(vp) - 1):
        c = float(centers[i])
        dist = abs(c - price) / price * 100
        if dist > dist_limit_pct:
            continue
        is_hvn = vp[i] >= threshold_h and vp[i] > vp[i - 1] and vp[i] > vp[i + 1]
        is_lvn = vp[i] <= threshold_l and vp[i] < vp[i - 1] and vp[i] < vp[i + 1]
        if c > price:
            if is_hvn and hvn_above is None:
                hvn_above = {"price": round(c, 2), "strength": round(float(vp[i]) / max(vp_mean, 1), 1)}
            if is_lvn and lvn_above is None:
                lvn_above = {"price": round(c, 2)}
        else:
            if is_hvn and hvn_below is None:
                hvn_below = {"price": round(c, 2), "strength": round(float(vp[i]) / max(vp_mean, 1), 1)}
            if is_lvn and lvn_below is None:
                lvn_below = {"price": round(c, 2)}
    return {"poc": poc, "vp_mean": round(vp_mean, 2),
            "hvn_above": hvn_above, "hvn_below": hvn_below,
            "lvn_above": lvn_above, "lvn_below": lvn_below}


def vp_score_adjustment(vp_nodes: dict, price: float, signal: str) -> tuple[int, list[str]]:
    if not vp_nodes:
        return 0, []
    pts, reasons = 0, []
    hvn_a = vp_nodes.get("hvn_above")
    hvn_b = vp_nodes.get("hvn_below")
    lvn_a = vp_nodes.get("lvn_above")
    if "LONG" in signal:
        if hvn_b:
            pts += 8; reasons.append(f"Уровень HVN {hvn_b['price']:,.2f} снизу (поддержка)")
        if hvn_a:
            pts += 5; reasons.append(f"HVN магнит сверху {hvn_a['price']:,.2f}")
        if lvn_a:
            pts += 4; reasons.append(f"Зона пустоты LVN выше {lvn_a['price']:,.2f}")
    elif "SHORT" in signal or "ВЫХОД" in signal:
        if hvn_a:
            pts += 8; reasons.append(f"Уровень HVN {hvn_a['price']:,.2f} сверху (сопротивление)")
        if hvn_b:
            pts += 5; reasons.append(f"HVN магнит снизу {hvn_b['price']:,.2f}")
    return pts, reasons


# ══════════════════════════════════════════════
# NEWS & FACT CLASSIFICATION
# ══════════════════════════════════════════════
# ══════════════════════════════════════════════
# ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ
# ══════════════════════════════════════════════
CALENDAR_FILE = Path("calendar_events.json")

def load_calendar_events() -> list:
    try:
        if CALENDAR_FILE.exists():
            return json.loads(CALENDAR_FILE.read_text())
    except Exception:
        pass
    return []

def save_calendar_events(events: list):
    CALENDAR_FILE.write_text(json.dumps(events, ensure_ascii=False, indent=2))

def get_upcoming_events(hours_ahead: int = 4) -> list:
    events = load_calendar_events()
    now = datetime.now(timezone.utc)
    upcoming = []
    for ev in events:
        try:
            ev_time = datetime.fromisoformat(ev["datetime_utc"])
            if ev_time.tzinfo is None:
                ev_time = ev_time.replace(tzinfo=timezone.utc)
            diff_h = (ev_time - now).total_seconds() / 3600
            if -1 < diff_h <= hours_ahead:
                ev["hours_ahead"] = round(diff_h, 1)
                upcoming.append(ev)
        except Exception:
            continue
    return sorted(upcoming, key=lambda x: x["hours_ahead"])

def check_calendar_block(ticker: str = "") -> dict:
    upcoming = get_upcoming_events(hours_ahead=2)
    if not upcoming:
        return {"block": False, "warning": "", "events": []}
    relevant = []
    for ev in upcoming:
        tickers = ev.get("tickers", [])
        impact  = ev.get("impact", "low")
        if (not tickers or ticker.upper() in [t.upper() for t in tickers]
                or impact in ("high", "critical")):
            relevant.append(ev)
    if not relevant:
        return {"block": False, "warning": "", "events": []}
    block_events = [e for e in relevant
                    if e["impact"] in ("high", "critical") and e["hours_ahead"] <= 0.5]
    warnings = []
    for ev in relevant[:3]:
        h = ev["hours_ahead"]
        time_str = (f"{abs(h)*60:.0f}м назад" if h < 0 else
                    f"через {h*60:.0f} мин" if h < 1 else f"через {h:.1f} ч")
        imp_e = {"critical": "\U0001f534", "high": "\U0001f7e0", "medium": "\U0001f7e1"}.get(ev["impact"], "\u26aa")
        warnings.append(f"{imp_e} {ev['name']} — {time_str}")
    return {"block": len(block_events) > 0, "warning": "\n".join(warnings), "events": relevant}

async def fetch_moex_dividends() -> list:
    events = []
    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/shares/dividends.json"
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)
                columns = data.get("dividends", {}).get("columns", [])
                rows    = data.get("dividends", {}).get("data", [])
                if not columns or not rows:
                    return []
                col_idx = {c: i for i, c in enumerate(columns)}
                now = datetime.now(timezone.utc)
                for row in rows[:100]:
                    try:
                        ticker_val   = str(row[col_idx["secid"]])
                        cutoff_date  = str(row[col_idx["registryclosedate"]])
                        dividend_val = row[col_idx["value"]]
                        if not cutoff_date or cutoff_date == "None":
                            continue
                        ev_dt = datetime.strptime(cutoff_date, "%Y-%m-%d").replace(
                            hour=7, tzinfo=timezone.utc)
                        if ev_dt < now - timedelta(days=1):
                            continue
                        events.append({
                            "name":         f"Дивидендная отсечка {ticker_val} ({dividend_val} руб.)",
                            "datetime_utc": ev_dt.isoformat(),
                            "impact":       "high",
                            "tickers":      [ticker_val],
                            "type":         "dividend_cutoff",
                            "source":       "moex",
                        })
                    except Exception:
                        continue
    except Exception as e:
        logger.warning(f"MOEX dividends fetch: {e}")
    return events

def get_cbr_dates_2026() -> list:
    """Заседания ЦБ РФ 2026. Обновляй раз в год."""
    cbr_dates = [
        "2026-02-14", "2026-03-21", "2026-04-25",
        "2026-06-06", "2026-07-25", "2026-09-12",
        "2026-10-24", "2026-12-19",
    ]
    events = []
    now = datetime.now(timezone.utc)
    for d in cbr_dates:
        try:
            # Решение обычно в 13:30 МСК = 10:30 UTC
            ev_dt = datetime.strptime(d, "%Y-%m-%d").replace(hour=10, minute=30,
                                                              tzinfo=timezone.utc)
            if ev_dt < now - timedelta(days=1):
                continue
            events.append({
                "name":         "🏦 Заседание ЦБ РФ — ключевая ставка",
                "datetime_utc": ev_dt.isoformat(),
                "impact":       "critical",
                "tickers":      [],
                "type":         "cbr_rate",
                "source":       "cbr",
                "note":         "Не торгуй за 2ч до и 1ч после. Волатильность аномальная.",
            })
        except Exception:
            continue
    return events


async def fetch_moex_reports() -> list:
    """
    Получает расписание отчётности эмитентов через MOEX ISS API.
    Отчёт по МСФО = важное событие, может двинуть бумагу на 3-8%.
    """
    events = []
    url = "https://iss.moex.com/iss/statistics/engines/stock/markets/shares/securities/reports.json"
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params={"limit": 50}) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)
                cols = data.get("reports", {}).get("columns", [])
                rows = data.get("reports", {}).get("data", [])
                if not cols or not rows:
                    return []
                idx = {c: i for i, c in enumerate(cols)}
                now = datetime.now(timezone.utc)
                for row in rows:
                    try:
                        ticker   = str(row[idx.get("secid", 0)])
                        rep_date = str(row[idx.get("reportdate", 1)])
                        rep_type = str(row[idx.get("reporttype", 2)])
                        if not rep_date or rep_date == "None":
                            continue
                        ev_dt = datetime.strptime(rep_date[:10], "%Y-%m-%d").replace(
                            hour=6, tzinfo=timezone.utc)
                        if ev_dt < now - timedelta(days=1) or ev_dt > now + timedelta(days=14):
                            continue
                        impact = "high" if "МСФО" in rep_type or "IFRS" in rep_type.upper() else "medium"
                        events.append({
                            "name":         f"📋 Отчёт {ticker} — {rep_type}",
                            "datetime_utc": ev_dt.isoformat(),
                            "impact":       impact,
                            "tickers":      [ticker],
                            "type":         "earnings",
                            "source":       "moex",
                        })
                    except Exception:
                        continue
    except Exception as e:
        logger.debug(f"MOEX reports: {e}")
    return events


async def auto_update_calendar():
    """
    Автообновление календаря: дивиденды + отчёты + ЦБ.
    Запускается при старте бота и потом раз в 6 часов.
    """
    try:
        cbr_events = get_cbr_dates_2026()
        div_events = await fetch_moex_dividends()
        rep_events = await fetch_moex_reports()

        all_events = cbr_events + div_events + rep_events

        # Удаляем дубликаты по имени+дате
        seen = set()
        unique = []
        for ev in all_events:
            key = f"{ev['name']}_{ev['datetime_utc'][:10]}"
            if key not in seen:
                seen.add(key)
                unique.append(ev)

        save_calendar_events(unique)
        logger.info(f"Calendar updated: {len(cbr_events)} ЦБ + {len(div_events)} дивиденды + {len(rep_events)} отчёты = {len(unique)} событий")
        return len(unique)
    except Exception as e:
        logger.error(f"auto_update_calendar: {e}")
        return 0

RUSSIAN_NEWS_RSS = [
    "https://www.e-disclosure.ru/RSS/company.aspx",
    "https://www.interfax.ru/rss.asp",
    "https://tass.ru/rss/v2.xml",
    "https://www.kommersant.ru/RSS/news.xml",
    "https://smart-lab.ru/blog/feed/",
    "https://www.moex.com/export/news.aspx?mode=rss",
]

FACT_PATTERNS: list[tuple[str, str, int, bool]] = [
    ("рекомендовал дивиденд", "рекомендация дивидендов", 9, True),
    ("рекомендует дивиденд", "рекомендация дивидендов", 9, True),
    ("дивиденды за", "дивиденды объявлены", 8, True),
    ("дивиденды выше", "дивиденды выше ожиданий", 10, True),
    ("обратный выкуп", "байбек", 9, True),
    ("buyback", "байбек", 9, True),
    ("байбек", "байбек", 9, True),
    ("рекордная прибыль", "рекордная прибыль", 8, True),
    ("прибыль выросла", "рост прибыли", 7, True),
    ("выручка выросла", "рост выручки", 6, True),
    ("дополнительная эмиссия", "допэмиссия", -10, True),
    ("допэмиссия", "допэмиссия", -10, True),
    ("spo", "SPO", -8, True),
    ("отменил дивиденд", "отмена дивидендов", -9, True),
    ("не будет дивидендов", "отмена дивидендов", -9, True),
    ("чистый убыток", "убыток", -8, True),
    ("новые санкции", "санкции", -9, False),
    ("санкции против", "санкции", -9, False),
]

OPINION_PATTERNS = [
    "считает аналитик", "по мнению", "эксперт полагает", "аналитики ожидают",
    "прогноз аналитик", "целевая цена", "рекомендация покупать",
    "обзор рынка", "итоги торгов", "утренний обзор",
]


def classify_news_item(title: str) -> dict:
    tl = title.lower()
    is_opinion = any(op in tl for op in OPINION_PATTERNS)
    for pattern, event_label, weight, is_corp in FACT_PATTERNS:
        if pattern in tl:
            return {"event": event_label, "weight": weight, "is_corporate": is_corp,
                    "is_opinion": False, "is_fact": True}
    return {"event": "новость", "weight": 0, "is_corporate": False,
            "is_opinion": is_opinion, "is_fact": False}


async def _fetch_rss(session: aiohttp.ClientSession, url: str, headers: dict) -> list[dict]:
    try:
        async with session.get(url, headers=headers) as r:
            if r.status != 200:
                return []
            text = await r.text(errors="replace")
            root = ET.fromstring(text)
            items = []
            for item in root.findall(".//item")[:20]:
                items.append({
                    "title": item.findtext("title", ""),
                    "link": item.findtext("link", ""),
                    "pub": item.findtext("pubDate", "")[:16],
                    "desc": item.findtext("description", "")[:300],
                })
            return items
    except Exception as e:
        logger.warning(f"RSS {url}: {e}")
        return []


async def fetch_russian_news(ticker: str = "", sector: str = "") -> list[dict]:
    cache_key = f"news_{ticker}_{sector}"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < 600:
        return _news_cache[cache_key]["items"]

    company_name = ""
    if ticker and ticker.upper() in MOEX_STOCKS:
        _, company_name, _ = MOEX_STOCKS[ticker.upper()]

    search_words: set[str] = set()
    if ticker:
        search_words.update([ticker.lower(), ticker.upper()])
    if company_name:
        search_words.update(w for w in company_name.lower().split() if len(w) > 3)
    search_words.update(["дивиденд", "байбек", "buyback", "обратный выкуп",
                         "допэмиссия", "spo", "санкции", "прибыль", "выручка", "убыток"])
    if sector and sector in SECTOR_KEYWORDS:
        search_words.update(SECTOR_KEYWORDS[sector])

    raw_items: list[dict] = []
    timeout = aiohttp.ClientTimeout(total=10)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; MOEXBot/1.0)"}

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [_fetch_rss(session, url, headers) for url in RUSSIAN_NEWS_RSS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for url, result in zip(RUSSIAN_NEWS_RSS, results):
        if isinstance(result, Exception) or not result:
            continue
        for item in result:
            title = item.get("title", "").strip()
            if not title:
                continue
            full = (title + " " + item.get("desc", "")).lower()
            matched = [w for w in search_words if w in full]
            if not matched:
                continue
            is_specific = (
                ticker.lower() in full or
                bool(company_name and any(w in full for w in company_name.lower().split() if len(w) > 3))
            )
            cls = classify_news_item(title)
            if cls["is_corporate"] and cls["is_fact"] and not is_specific:
                continue
            raw_items.append({
                "title": title[:200], "link": item.get("link", ""),
                "pub": item.get("pub", ""), "source": url.split("/")[2],
                "is_specific": is_specific, "is_fact": cls["is_fact"],
                "is_opinion": cls["is_opinion"], "is_corporate": cls.get("is_corporate", False),
                "event": cls["event"], "weight": cls["weight"], "matched": matched[:3],
            })

    seen, unique = set(), []
    for it in raw_items:
        key = it["title"][:60]
        if key not in seen:
            seen.add(key)
            unique.append(it)

    def sort_key(x):
        if x["is_corporate"] and x["is_fact"]:
            return (0, -abs(x["weight"]))
        if x["is_fact"]:
            return (1, -abs(x["weight"]))
        if x["is_specific"] and not x["is_opinion"]:
            return (2, 0)
        return (3, 0)

    unique.sort(key=sort_key)
    unique = unique[:10]
    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique


async def fetch_market_news() -> list[dict]:
    return await fetch_russian_news()


# ══════════════════════════════════════════════
# AI NEWS EVALUATION (GROQ)
# ══════════════════════════════════════════════
def _groq_call(messages: list, model_idx: int = 0) -> str:
    if not groq_client:
        return ""
    for i in range(model_idx, len(GROQ_MODELS)):
        try:
            resp = groq_client.chat.completions.create(
                model=GROQ_MODELS[i], messages=messages,
                max_tokens=400, temperature=0.05,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"Groq {GROQ_MODELS[i]}: {e}")
    return ""


def _score_facts(news_items: list[dict]) -> tuple[int, list[str]]:
    total = 0
    events = []
    for it in news_items:
        if it.get("is_opinion"):
            continue
        w = it.get("weight", 0)
        if w != 0:
            total += w
            events.append(f"{it['event']} ({'+' if w > 0 else ''}{w})")
    return total, events


async def ai_evaluate_news(news_items: list[dict], ticker: str, sector: str,
                           tech_signal: str, tech_score: int) -> dict:
    is_long = "LONG" in tech_signal
    is_short = "SHORT" in tech_signal or "ВЫХОД" in tech_signal
    has_signal = is_long or is_short
    fact_weight, fact_events = _score_facts(news_items)

    blocking_found = [it["event"] for it in news_items
                      if it.get("is_fact") and it.get("weight", 0) <= -8]
    fact_items = [it for it in news_items if it.get("is_fact") and not it.get("is_opinion")]
    opinion_items = [it for it in news_items if it.get("is_opinion")]

    event_type = fact_events[0].split(" (")[0] if fact_events else "нет событий"
    event_weight = fact_weight
    llm_summary = ""

    if fact_items and groq_client:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        facts_text = "\n".join(
            f"- [{it['event']}, вес {it['weight']}] {it['title']}" for it in fact_items[:4]
        )
        prompt = f"""Ты — аналитик российского финансового рынка.
Компания: {company_name} ({ticker}), сектор: {sector}
Важные факты:
{facts_text}
Сделай краткое описание события в одно предложение без оценочных суждений о направлении торговли.
Ответь строго в формате JSON:
{{"event": "короткое название события", "weight": вес от -10 до 10, "summary": "краткое пояснение"}}"""
        raw = _groq_call([{"role": "user", "content": prompt}])
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            event_type = obj.get("event", event_type)
            llm_w = int(obj.get("weight", fact_weight))
            event_weight = max(-10, min(10, (fact_weight + llm_w) // 2))
            llm_summary = obj.get("summary", "")
        except Exception:
            pass

    final_weight = event_weight

    if blocking_found and is_long:
        filter_status = "BLOCKED"
    elif not has_signal and abs(final_weight) >= 8:
        filter_status = "NEWS_ONLY"
    elif not has_signal:
        filter_status = "NO_SIGNAL"
    elif final_weight >= 5:
        filter_status = "CONFIRMED"
    elif final_weight >= -2:
        filter_status = "CONFIRMED"
    elif final_weight >= -5:
        filter_status = "WEAK"
    else:
        filter_status = "BLOCKED"

    if tech_signal == "🟩 LONG":
        status_map = {"CONFIRMED": "🟩 LONG CONFIRMED", "WEAK": "🟡 LONG WEAK",
                      "WATCH": "👀 LONG WATCH", "BLOCKED": "🚫 LONG BLOCKED",
                      "NO_SIGNAL": "🟩 LONG", "NEWS_ONLY": "🟩 LONG (сильный фон)"}
    elif is_short:
        status_map = {"CONFIRMED": "🟥 ВЫХОД CONFIRMED", "WEAK": "🟡 ВЫХОД WEAK",
                      "WATCH": "👀 ВЫХОД WATCH", "BLOCKED": "🟥 ВЫХОД (сдерживающий позитив)",
                      "NO_SIGNAL": "🟥 ВЫХОД", "NEWS_ONLY": "🟥 ВЫХОД"}
    else:
        status_map = {k: "НЕТ СИГНАЛА" for k in ["CONFIRMED", "WEAK", "WATCH", "BLOCKED", "NO_SIGNAL", "NEWS_ONLY"]}

    confirmed = status_map.get(filter_status, tech_signal)
    underreaction = any(it.get("is_specific") and abs(it.get("weight", 0)) >= 8 for it in fact_items)

    return {
        "event_type": event_type, "event_weight": final_weight,
        "fact_events": fact_events[:3], "filter_status": filter_status,
        "summary": llm_summary, "blocking": blocking_found,
        "underreaction": underreaction, "confirmed": confirmed,
        "opinions_skipped": len(opinion_items),
        "sentiment": "позитив" if final_weight > 2 else ("негатив" if final_weight < -2 else "нейтрально"),
        "score": min(10, max(0, abs(final_weight))),
    }


async def ai_classify_news_impact(headline: str, ticker: str) -> dict:
    text = headline.lower()
    weight = 0
    for kw, w in FACT_PATTERNS:
        if kw in text:
            weight = w
            break
    if groq_client and weight == 0:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        prompt = f'Новость для {ticker} ({company_name}): "{headline}"\nОпредели влияние. Ответь строго JSON: {{"event": "описание", "weight": число от -10 до 10}}'
        raw = _groq_call([{"role": "user", "content": prompt}])
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            weight = int(obj.get("weight", 0))
        except Exception:
            pass
    sentiment = "позитив" if weight > 2 else ("негатив" if weight < -2 else "нейтрально")
    return {"sentiment": sentiment, "score": min(10, abs(weight)), "asset": ticker, "weight": weight}


# ══════════════════════════════════════════════
# TECHNICAL ANALYSIS
# ══════════════════════════════════════════════
def calculate_indicators(df: pd.DataFrame, tf: str = "15m") -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    is_intraday = tf in INTRADAY_TFS

    if is_intraday:
        df["ema9"] = ta.ema(close, length=9)
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200) if len(df) >= 200 else np.nan
    else:
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200)

    rsi_len = 7 if tf == "5m" else (9 if tf == "15m" else 14)
    df["rsi"] = ta.rsi(close, length=rsi_len)

    if is_intraday:
        macd = ta.macd(close, fast=8, slow=17, signal=9)
        macd_key, macd_s, macd_h = "MACD_8_17_9", "MACDs_8_17_9", "MACDh_8_17_9"
    else:
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        macd_key, macd_s, macd_h = "MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"
    if macd is not None:
        df["macd"] = macd.get(macd_key, np.nan)
        df["macd_signal"] = macd.get(macd_s, np.nan)
        df["macd_hist"] = macd.get(macd_h, np.nan)

    atr_len = 7 if is_intraday else 14
    df["atr"] = ta.atr(df["high"], df["low"], close, length=atr_len)

    bb = ta.bbands(close, length=20, std=2)
    if bb is not None:
        df["bb_upper"] = bb.get("BBU_20_2.0", np.nan)
        df["bb_lower"] = bb.get("BBL_20_2.0", np.nan)
        df["bb_mid"] = bb.get("BBM_20_2.0", np.nan)

    df["vol_ma20"] = ta.sma(df["volume"], length=20)
    df["vol_ratio"] = df["volume"] / df["vol_ma20"].replace(0, np.nan)

    if is_intraday and "volume" in df.columns:
        df["timestamp_msk"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("Europe/Moscow")
        df["date_msk"] = df["timestamp_msk"].dt.date
        df["day_open"] = df.groupby("date_msk")["open"].transform("first")
        tp = (df["high"] + df["low"] + df["close"]) / 3
        df["_tp_vol"] = tp * df["volume"]
        cum_vol = df.groupby("date_msk")["volume"].cumsum()
        cum_tpv = df.groupby("date_msk")["_tp_vol"].cumsum()
        df["vwap"] = cum_tpv / cum_vol.replace(0, np.nan)
        df["vwap_dev"] = (close - df["vwap"]) / df["vwap"] * 100
        df.drop(columns=["_tp_vol"], inplace=True)

    return df


# ══════════════════════════════════════════════
# УРОВНИ ПРЕДЫДУЩЕГО ДНЯ (PDH/PDL/PDC)
# ══════════════════════════════════════════════
def get_previous_day_levels(df: pd.DataFrame) -> dict:
    """
    Рассчитывает ключевые уровни предыдущего торгового дня.
    PDH = Previous Day High
    PDL = Previous Day Low
    PDC = Previous Day Close
    PDM = Previous Day Midpoint (High+Low)/2
    Эти уровни работают как магниты и зоны разворота на РФ рынке.
    """
    if "timestamp" not in df.columns or len(df) < 50:
        return {}
    try:
        df2 = df.copy()
        df2["ts"] = pd.to_datetime(df2["timestamp"])
        try:
            df2["date"] = df2["ts"].dt.tz_convert("Europe/Moscow").dt.date
        except Exception:
            df2["date"] = df2["ts"].dt.date

        daily = df2.groupby("date").agg(
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            open=("open", "first"),
        ).reset_index().sort_values("date")

        if len(daily) < 2:
            return {}

        prev = daily.iloc[-2]
        today_open = float(daily.iloc[-1]["open"]) if len(daily) >= 1 else None

        pdh = float(prev["high"])
        pdl = float(prev["low"])
        pdc = float(prev["close"])
        pdm = round((pdh + pdl) / 2, 4)
        # Гэп открытия — если открытие сегодня далеко от закрытия вчера
        gap_pct = 0.0
        if today_open:
            gap_pct = round((today_open - pdc) / pdc * 100, 2)

        return {
            "pdh": round(pdh, 4),
            "pdl": round(pdl, 4),
            "pdc": round(pdc, 4),
            "pdm": round(pdm, 4),
            "gap_pct": gap_pct,
            "today_open": round(today_open, 4) if today_open else None,
            "range": round(pdh - pdl, 4),
        }
    except Exception as e:
        logger.debug(f"PDL calc error: {e}")
        return {}


def get_pd_level_context(levels: dict, price: float) -> tuple[str, float]:
    """
    Определяет: находится ли цена у ключевого уровня предыдущего дня.
    Возвращает (название уровня, расстояние в %).
    Зона касания = 0.15% от уровня.
    """
    if not levels or not price:
        return "", 0.0
    touch_zone = 0.0015  # 0.15%
    for name, val in [("PDH", levels.get("pdh", 0)),
                       ("PDL", levels.get("pdl", 0)),
                       ("PDC", levels.get("pdc", 0)),
                       ("PDM", levels.get("pdm", 0))]:
        if not val:
            continue
        dist_pct = abs(price - val) / val
        if dist_pct <= touch_zone:
            return name, round((price - val) / val * 100, 3)
    return "", 0.0


# ══════════════════════════════════════════════
# МУЛЬТИТАЙМФРЕЙМНЫЙ ФИЛЬТР (MTF)
# ══════════════════════════════════════════════
async def get_htf_trend(ticker: str, figi: str, base_tf: str) -> dict:
    """
    Получает тренд старшего таймфрейма (HTF).
    15м → смотрим 1ч
    5м  → смотрим 15м
    1ч  → смотрим 4ч или 1д

    Возвращает:
      trend: "bull" | "bear" | "neutral"
      label: строка для показа
      htf:   название TF
    """
    htf_map = {"5m": "15m", "15m": "1h", "1h": "1d", "4h": "1d"}
    htf = htf_map.get(base_tf, "1h")
    interval, _, limit = TF_MAP.get(htf, TF_MAP["1h"])

    cache_key = f"htf_{ticker}_{htf}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 900:
        return _cache[cache_key]["val"]

    default = {"trend": "neutral", "label": "❓ HTF неизвестен", "htf": htf,
                "ema20": 0, "ema50": 0, "price": 0}
    try:
        df = await fetch_candles_tinkoff(figi, interval, limit)
        if df is None or len(df) < 30:
            return default

        close_s = df["close"]
        ema20 = float(ta.ema(close_s, length=20).iloc[-1] or 0)
        ema50 = float(ta.ema(close_s, length=50).iloc[-1] or 0)
        price_htf = float(close_s.iloc[-1])

        if price_htf > ema20 > ema50:
            slope = (ema50 - ta.ema(close_s, length=50).iloc[-10]) / ema50 * 100 if len(close_s) > 10 else 0
            trend = "bull" if slope > 0 else "neutral"
            label = f"📈 {htf.upper()}: бычий тренд"
        elif price_htf < ema20 < ema50:
            trend = "bear"
            label = f"📉 {htf.upper()}: медвежий тренд"
        else:
            trend = "neutral"
            label = f"↔️ {htf.upper()}: боковик"

        result = {"trend": trend, "label": label, "htf": htf,
                  "ema20": round(ema20, 4), "ema50": round(ema50, 4), "price": round(price_htf, 4)}
        _cache[cache_key] = {"val": result, "ts": now}
        return result
    except Exception as e:
        logger.debug(f"HTF {ticker} {htf}: {e}")
        return default


# ══════════════════════════════════════════════
# ДИВЕРГЕНЦИЯ MACD (лучше работает на РФ рынке)
# ══════════════════════════════════════════════
def detect_macd_divergence(df: pd.DataFrame) -> str:
    """
    Ищет дивергенцию между ценой и MACD-гистограммой.
    Бычья: цена делает новый минимум, MACD гист — нет → разворот вверх.
    Медвежья: цена делает новый максимум, MACD гист — нет → разворот вниз.
    Работает только если есть macd_hist в df.
    """
    if "macd_hist" not in df.columns or len(df) < 20:
        return ""
    recent = df.tail(30).dropna(subset=["macd_hist"])
    if len(recent) < 15:
        return ""

    prices     = recent["close"].values
    macd_vals  = recent["macd_hist"].values
    lows       = recent["low"].values
    highs      = recent["high"].values

    def local_extrema(arr, order=3):
        maxima, minima = [], []
        for i in range(order, len(arr) - order):
            if arr[i] == min(arr[i-order:i+order+1]): minima.append(i)
            if arr[i] == max(arr[i-order:i+order+1]): maxima.append(i)
        return maxima, minima

    _, minima = local_extrema(lows)
    maxima, _ = local_extrema(highs)

    # Бычья дивергенция: цена ниже, MACD выше
    if len(minima) >= 2:
        i1, i2 = minima[-2], minima[-1]
        price_lower  = lows[i2] < lows[i1] * 0.999
        macd_higher  = macd_vals[i2] > macd_vals[i1] + 0.0001
        if price_lower and macd_higher:
            return "🔄 Бычья дивергенция MACD — разворот вверх"

    # Медвежья дивергенция: цена выше, MACD ниже
    if len(maxima) >= 2:
        i1, i2 = maxima[-2], maxima[-1]
        price_higher = highs[i2] > highs[i1] * 1.001
        macd_lower   = macd_vals[i2] < macd_vals[i1] - 0.0001
        if price_higher and macd_lower:
            return "🔄 Медвежья дивергенция MACD — разворот вниз"

    return ""


# ══════════════════════════════════════════════
# ФИЛЬТР ВРЕМЕНИ ТОРГОВ (обновлённый)
# ══════════════════════════════════════════════
def get_session_phase() -> dict:
    """
    Определяет фазу торговой сессии MOEX.
    Фазы:
      pre_open  — до 10:00 МСК (не торгуем)
      opening   — 10:00-10:30 МСК (опасно, манипуляции)
      morning   — 10:30-12:30 МСК (лучшее время для входа)
      midday    — 12:30-14:30 МСК (затишье, меньше объёма)
      afternoon — 14:30-17:30 МСК (хорошее время, европа открылась)
      closing   — 17:30-18:45 МСК (закрываем позиции)
      evening   — 19:00-23:50 МСК (вечерняя сессия)
      closed    — нет торгов
    """
    now_utc  = datetime.now(timezone.utc)
    msk_hour = (now_utc.hour + 3) % 24
    msk_min  = now_utc.minute
    msk_time = msk_hour * 60 + msk_min  # минуты с начала дня
    weekday  = now_utc.weekday()

    if weekday >= 5:  # суббота/воскресенье
        return {"phase": "closed", "label": "🔴 Выходной", "trade": False,
                "enter": False, "warning": "Рынок закрыт"}

    # Точное расписание MOEX с учётом клиринговых перерывов
    # Клиринг 1: 14:00-14:05 МСК (промежуточный)
    # Клиринг 2: 18:45-19:00 МСК (основной)
    # Источник: moex.com/s218
    phases = [
        (0,          10*60,       "pre_open",  "🔴 До открытия",         False, False, "Рынок ещё не открылся"),
        (10*60,      10*60+30,    "opening",   "🟡 Открытие (опасно)",   True,  False, "Первые 30 мин — не входим"),
        (10*60+30,   13*60+45,    "morning",   "🟢 Утренняя сессия",     True,  True,  ""),
        (13*60+45,   14*60+5,     "clearing1", "🔴 Клиринг 14:00",       False, False, "Клиринговый перерыв — не входим"),
        (14*60+5,    17*60+45,    "afternoon", "🟢 Дневная сессия",      True,  True,  ""),
        (17*60+45,   18*60+45,    "closing",   "🟡 Закрытие основной",   True,  False, "Закрывай позиции"),
        (18*60+45,   19*60,       "clearing2", "🔴 Клиринг 18:45",       False, False, "Основной клиринг — не входим"),
        (19*60,      23*60+45,    "evening",   "🟢 Вечерняя сессия",     True,  True,  ""),
        (23*60+45,   24*60,       "eod",       "🔴 Закрытие сессии",     False, False, "Закрывай всё до 23:50"),
    ]

    for start, end, phase, label, trade, enter, warning in phases:
        if start <= msk_time < end:
            return {"phase": phase, "label": label, "trade": trade,
                    "enter": enter, "warning": warning,
                    "msk_time": f"{msk_hour:02d}:{msk_min:02d}"}

    return {"phase": "closed", "label": "🔴 Закрыто", "trade": False,
            "enter": False, "warning": ""}


def detect_candle_pattern(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "—"
    c, p = df.iloc[-1], df.iloc[-2]
    body = abs(c["close"] - c["open"])
    rng = c["high"] - c["low"]
    if rng == 0:
        return "Дожи"
    uw = c["high"] - max(c["close"], c["open"])
    lw = min(c["close"], c["open"]) - c["low"]
    if lw > body * 2 and uw < body * 0.5:
        return "📌 Пин-бар снизу"
    if uw > body * 2 and lw < body * 0.5:
        return "📌 Пин-бар сверху"
    if c["close"] > c["open"] and p["close"] < p["open"] and c["close"] > p["open"] and c["open"] < p["close"]:
        return "🟢 Бычье поглощение"
    if c["close"] < c["open"] and p["close"] > p["open"] and c["close"] < p["open"] and c["open"] > p["close"]:
        return "🔴 Медвежье поглощение"
    return "Обычная свеча"


def detect_rsi_divergence(df: pd.DataFrame) -> str:
    if len(df) < 40 or "rsi" not in df.columns:
        return ""
    recent = df.tail(40).copy().dropna(subset=["rsi"])
    if len(recent) < 20:
        return ""
    prices = recent["low"].values
    highs = recent["high"].values
    rsi_v = recent["rsi"].values

    def extrema(arr, order=3):
        maxima, minima = [], []
        for i in range(order, len(arr) - order):
            w = arr[i - order:i + order + 1]
            if arr[i] == w.max():
                maxima.append(i)
            if arr[i] == w.min():
                minima.append(i)
        return maxima, minima

    _, minima = extrema(prices)
    maxima, _ = extrema(highs)

    if len(minima) >= 2:
        i1, i2 = minima[-2], minima[-1]
        if prices[i2] < prices[i1] * 0.998 and rsi_v[i2] > rsi_v[i1] + 1.5:
            return f"🔄 Бычья дивергенция RSI (+{rsi_v[i2] - rsi_v[i1]:.1f})"
    if len(maxima) >= 2:
        i1, i2 = maxima[-2], maxima[-1]
        if highs[i2] > highs[i1] * 1.002 and rsi_v[i2] < rsi_v[i1] - 1.5:
            return f"🔄 Медвежья дивергенция RSI (-{rsi_v[i1] - rsi_v[i2]:.1f})"
    return ""


def detect_market_regime(df: pd.DataFrame) -> dict:
    if len(df) < 40:
        return {"regime": "ranging", "label": "↔️ Боковик", "trend_mult": 1.0, "slope": 0}
    close = df["close"].values
    price = close[-1]
    ema20 = ta.ema(pd.Series(close), length=20).values
    ema50 = ta.ema(pd.Series(close), length=50).values
    ema20_v = ema20[~np.isnan(ema20)]
    ema50_v = ema50[~np.isnan(ema50)]
    if len(ema50_v) < 10:
        return {"regime": "ranging", "label": "↔️ Боковик", "trend_mult": 1.0, "slope": 0}
    slope = (ema50_v[-1] - ema50_v[-10]) / ema50_v[-10] * 100
    if price > ema20_v[-1] > ema50_v[-1] and slope > 0.1:
        return {"regime": "trending_up", "label": "📈 Восходящий тренд", "trend_mult": 1.2, "slope": slope}
    if price < ema20_v[-1] < ema50_v[-1] and slope < -0.1:
        return {"regime": "trending_down", "label": "📉 Нисходящий тренд", "trend_mult": 1.2, "slope": slope}
    return {"regime": "ranging", "label": "↔️ Боковик / Флэт", "trend_mult": 0.8, "slope": slope}


def find_support_resistance(df: pd.DataFrame, price: float):
    r = df.tail(150)
    highs, lows = [], []
    for i in range(2, len(r) - 2):
        h = r.iloc[i]["high"]
        l = r.iloc[i]["low"]
        if h > r.iloc[i - 1]["high"] and h > r.iloc[i + 1]["high"]:
            highs.append(float(h))
        if l < r.iloc[i - 1]["low"] and l < r.iloc[i + 1]["low"]:
            lows.append(float(l))
    supports = sorted([l for l in lows if l < price], reverse=True)[:3]
    resistances = sorted([h for h in highs if h > price])[:3]
    return supports, resistances


def calculate_sl_tp_stocks(signal: str, price: float, atr: float,
                           supports: list, resistances: list,
                           pd_levels: dict = None, vp_nodes: dict = None) -> dict:
    """
    Расчёт SL/TP с привязкой к реальным уровням рынка.

    Приоритет для SL:
      1. Ближайший HVN (зона объёма) под ценой — за него ставим SL
      2. PDL/PDH (уровень предыдущего дня)
      3. Ближайший support/resistance из свечного анализа
      4. Если уровней нет — ATR×1.5 (запасной вариант)

    Приоритет для TP:
      1. Ближайший HVN выше цены (зона сопротивления)
      2. PDH/PDL предыдущего дня
      3. Следующий resistance/support
      4. ATR-кратные (запасной вариант)

    Ограничения:
      - Минимальный риск: 0.5% (чтобы SL не был слишком близко)
      - Максимальный риск: 3% (интрадей, не держим большие риски)
      - Минимальный R/R: 1.5 (если не получается — предупреждаем)
    """
    if signal not in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
        return {}

    is_long   = "LONG" in signal
    min_risk  = price * 0.005   # минимум 0.5%
    max_risk  = price * 0.030   # максимум 3%
    atr_risk  = atr * 1.5       # стандартный ATR-стоп

    # ── Собираем все уровни в один список ────────────────────────────────
    all_supports    = list(supports or [])
    all_resistances = list(resistances or [])

    # Добавляем HVN из Volume Profile
    if vp_nodes:
        hvn_below = vp_nodes.get("hvn_below", {})
        hvn_above = vp_nodes.get("hvn_above", {})
        lvn_below = vp_nodes.get("lvn_below", {})
        lvn_above = vp_nodes.get("lvn_above", {})
        if hvn_below and hvn_below.get("price"):
            all_supports.insert(0, float(hvn_below["price"]))
        if hvn_above and hvn_above.get("price"):
            all_resistances.insert(0, float(hvn_above["price"]))
        if lvn_below and lvn_below.get("price"):
            all_supports.append(float(lvn_below["price"]))
        if lvn_above and lvn_above.get("price"):
            all_resistances.append(float(lvn_above["price"]))

    # Добавляем уровни предыдущего дня
    if pd_levels:
        for key in ("pdl", "pdm", "pdc", "pdh"):
            val = pd_levels.get(key, 0)
            if not val:
                continue
            if val < price * 0.999:
                all_supports.append(float(val))
            elif val > price * 1.001:
                all_resistances.append(float(val))

    # Убираем дубликаты и сортируем
    all_supports    = sorted(set(round(s, 4) for s in all_supports if s > 0), reverse=True)
    all_resistances = sorted(set(round(r, 4) for r in all_resistances if r > 0))

    def _round_price(p):
        """Округляет до значимых цифр в зависимости от диапазона цены."""
        if price > 1000:  return round(p, 1)
        if price > 100:   return round(p, 2)
        if price > 10:    return round(p, 3)
        return round(p, 4)

    if is_long:
        # ── SL: ищем ближайший уровень поддержки ЗА которым ставим стоп ──
        # Логика: SL = уровень - небольшой буфер (0.1%)
        sl_level = None
        for sup in all_supports:
            dist = price - sup
            if min_risk <= dist <= max_risk:
                sl_level = sup
                break

        if sl_level:
            sl = _round_price(sl_level - sl_level * 0.001)  # буфер 0.1% за уровень
            sl_source = "за уровень поддержки"
        elif atr_risk <= max_risk:
            sl = _round_price(price - max(atr_risk, min_risk))
            sl_source = "ATR×1.5"
        else:
            sl = _round_price(price - min_risk)
            sl_source = "мин. риск"

        actual_risk = price - sl
        risk_pct    = actual_risk / price * 100

        # ── TP: ищем уровни сопротивления ────────────────────────────────
        tp_candidates = [r for r in all_resistances if r > price + actual_risk * 0.5]

        if len(tp_candidates) >= 3:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(tp_candidates[2] if len(tp_candidates) > 2
                               else price + actual_risk * 4)
        elif len(tp_candidates) == 2:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(price + actual_risk * 4)
        elif len(tp_candidates) == 1:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(price + actual_risk * 2.5)
            tp3 = _round_price(price + actual_risk * 4)
        else:
            # Нет уровней — ATR-кратные
            tp1 = _round_price(price + actual_risk * 1.5)
            tp2 = _round_price(price + actual_risk * 2.5)
            tp3 = _round_price(price + actual_risk * 4.0)

    else:  # SHORT
        # ── SL: ближайший уровень сопротивления ──────────────────────────
        sl_level = None
        for res in all_resistances:
            dist = res - price
            if min_risk <= dist <= max_risk:
                sl_level = res
                break

        if sl_level:
            sl = _round_price(sl_level + sl_level * 0.001)
            sl_source = "за уровень сопротивления"
        elif atr_risk <= max_risk:
            sl = _round_price(price + max(atr_risk, min_risk))
            sl_source = "ATR×1.5"
        else:
            sl = _round_price(price + min_risk)
            sl_source = "мин. риск"

        actual_risk = sl - price
        risk_pct    = actual_risk / price * 100

        # ── TP: уровни поддержки ──────────────────────────────────────────
        tp_candidates = [s for s in all_supports if s < price - actual_risk * 0.5]

        if len(tp_candidates) >= 3:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(tp_candidates[2] if len(tp_candidates) > 2
                               else price - actual_risk * 4)
        elif len(tp_candidates) == 2:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(tp_candidates[1])
            tp3 = _round_price(price - actual_risk * 4)
        elif len(tp_candidates) == 1:
            tp1 = _round_price(tp_candidates[0])
            tp2 = _round_price(price - actual_risk * 2.5)
            tp3 = _round_price(price - actual_risk * 4)
        else:
            tp1 = _round_price(price - actual_risk * 1.5)
            tp2 = _round_price(price - actual_risk * 2.5)
            tp3 = _round_price(price - actual_risk * 4.0)

    rr = round(abs(tp2 - price) / max(abs(price - sl), 0.001), 2)

    # Предупреждения
    warnings = []
    if rr < 1.5:
        warnings.append(f"⚠️ R/R низкий ({rr:.1f}) — рассмотри пропустить")
    if risk_pct > 2.5:
        warnings.append(f"⚠️ Большой риск {risk_pct:.1f}% — уменьши лот")

    return {
        "sl":        sl,
        "tp1":       tp1,
        "tp2":       tp2,
        "tp3":       tp3,
        "risk_pct":  round(risk_pct, 2),
        "rr_ratio":  rr,
        "sl_source": sl_source,
        "warn":      " | ".join(warnings),
    }


# ══════════════════════════════════════════════
# SCORING — СИСТЕМА ГЕЙТОВ
# ══════════════════════════════════════════════
# Логика: сигнал выдаётся только если прошли ВСЕ обязательные гейты.
# Это убирает ложные срабатывания при суммировании слабых сигналов.
#
# ГЕЙТЫ для интрадея (15м):
#   [1] ОБЯЗАТЕЛЬНО: VWAP-позиция (цена выше/ниже VWAP)
#   [2] ОБЯЗАТЕЛЬНО: Объём (минимум x1.3 от среднего)
#   [3] ОБЯЗАТЕЛЬНО: EMA-тренд (EMA9 > EMA20 или разворот)
#   [4] ДОПОЛНИТЕЛЬНО: RSI не в противоположной зоне (не шортить RSI<40)
#
# Скор считается только ПОСЛЕ прохождения гейтов.
# Бонусы за дополнительные совпадения.

def compute_tech_score(df: pd.DataFrame, mode_cfg: dict,
                       vp_nodes: dict = None, imoex_regime: dict = None,
                       htf_trend: dict = None, pd_levels: dict = None,
                       macd_div: str = "") -> tuple[str, int, list]:
    row = df.iloc[-1]
    rsi = float(row.get("rsi", 50) or 50)
    macd_h = float(row.get("macd_hist", 0) or 0)
    close = float(row["close"])
    vol_r = float(row.get("vol_ratio", 1) or 1)
    bb_low = float(row.get("bb_lower", 0) or 0)
    bb_up = float(row.get("bb_upper", 0) or 0)
    regime = detect_market_regime(df)
    candle = detect_candle_pattern(df)
    is_trend = regime["regime"] in ["trending_up", "trending_down"]

    long_score, short_score = 0.0, 0.0
    long_r, short_r = [], []
    vwap     = float(row.get("vwap",     0) or 0)
    vwap_dev = float(row.get("vwap_dev", 0) or 0)
    day_open = float(row.get("day_open", 0) or 0)
    ema9     = float(row.get("ema9",     0) or 0)
    ema20    = float(row.get("ema20",    0) or 0)
    ema50    = float(row.get("ema50",    0) or 0)
    has_vwap = vwap > 0  # True на интрадейных TF

    # ─────────────────────────────────────────────────────────────────
    # ГЕЙТ 0: HTF-фильтр — не торгуем против старшего тренда
    # На 15м смотрим 1ч: если 1ч медвежий — только SHORT или нет сигнала
    # ─────────────────────────────────────────────────────────────────
    htf_bias = "neutral"
    if htf_trend and htf_trend.get("trend") != "neutral":
        htf_bias = htf_trend["trend"]
        # При строгом HTF не блокируем полностью, но учтём в скоре ниже

    # ─────────────────────────────────────────────────────────────────
    # ГЕЙТ 1: VWAP или EMA-тренд (хотя бы одно должно быть чётким)
    # ─────────────────────────────────────────────────────────────────
    long_gates,  short_gates  = 0, 0
    long_r,      short_r      = [], []

    # VWAP-позиция — главный интрадей-фильтр
    if has_vwap:
        if close > vwap * 1.002:          # +0.2% выше VWAP
            long_gates += 1
            long_r.append(f"Выше VWAP (+{vwap_dev:.2f}%)")
        elif close < vwap * 0.998:
            short_gates += 1
            short_r.append(f"Ниже VWAP ({vwap_dev:.2f}%)")
        else:
            # В зоне VWAP ±0.2% — нет чёткой позиции
            return "НЕТ СИГНАЛА", 0, ["Цена в зоне VWAP — нет чёткой позиции"]

    # EMA-тренд
    if ema9 > 0 and ema20 > 0:
        if close > ema9 > ema20:
            long_gates += 1
            long_r.append("EMA9 > EMA20 (бычий импульс)")
        elif close < ema9 < ema20:
            short_gates += 1
            short_r.append("EMA9 < EMA20 (медвежий импульс)")
        # Ни туда ни сюда — нейтрально, не блокируем
    elif ema20 > 0 and ema50 > 0:
        if close > ema20 > ema50:
            long_gates += 1
            long_r.append("EMA20 > EMA50 (восходящий тренд)")
        elif close < ema20 < ema50:
            short_gates += 1
            short_r.append("EMA20 < EMA50 (нисходящий тренд)")

    # ─────────────────────────────────────────────────────────────────
    # ГЕЙТ 2: Объём — ОБЯЗАТЕЛЬНОЕ условие
    # Без подтверждения объёмом движение ненастоящее.
    # ─────────────────────────────────────────────────────────────────
    min_vol = mode_cfg.get("min_vol_ratio", 1.3)
    if vol_r < min_vol:
        return "НЕТ СИГНАЛА", 0, [f"Низкий объём (x{vol_r:.1f} < x{min_vol:.1f} — нет подтверждения)"]

    # ─────────────────────────────────────────────────────────────────
    # ГЕЙТ 3: RSI не против нас
    # Не лонгуем при RSI>70, не шортим при RSI<30
    # ─────────────────────────────────────────────────────────────────
    if long_gates > short_gates:
        direction = "long"
        if rsi > 72:
            return "НЕТ СИГНАЛА", 0, [f"RSI перекуплен ({rsi:.0f}) — лонг опасен"]
    elif short_gates > long_gates:
        direction = "short"
        if rsi < 28:
            return "НЕТ СИГНАЛА", 0, [f"RSI перепродан ({rsi:.0f}) — шорт опасен"]
    else:
        # Равное количество гейтов — нет чёткого направления
        return "НЕТ СИГНАЛА", 0, ["Противоречивые сигналы — нет чёткого направления"]

    # ─────────────────────────────────────────────────────────────────
    # Все гейты пройдены — считаем скор по дополнительным факторам
    # ─────────────────────────────────────────────────────────────────
    score = 50  # базовый скор за прохождение всех гейтов

    # Сила VWAP-отклонения
    if has_vwap:
        dev = abs(vwap_dev)
        if dev > 1.0:   score += 15
        elif dev > 0.5: score += 8
        elif dev > 0.2: score += 3

    # Сила объёма
    if vol_r > 3.0:   score += 20
    elif vol_r > 2.0: score += 12
    elif vol_r > 1.5: score += 6

    # HTF совпадение/противоречие — важный фактор
    if htf_bias == "bull":
        if direction == "long":
            score += 12
            long_r.append(f"✅ {htf_trend['htf'].upper()} тренд бычий — направление совпадает")
        else:
            score -= 20  # шортим против старшего тренда — штраф
            short_r.append(f"⚠️ {htf_trend['htf'].upper()} тренд бычий — SHORT против тренда")
    elif htf_bias == "bear":
        if direction == "short":
            score += 12
            short_r.append(f"✅ {htf_trend['htf'].upper()} тренд медвежий — направление совпадает")
        else:
            score -= 20
            long_r.append(f"⚠️ {htf_trend['htf'].upper()} тренд медвежий — LONG против тренда")

    # PDH/PDL уровни — отбой от уровня + подтверждение
    if pd_levels:
        pdh = pd_levels.get("pdh", 0)
        pdl = pd_levels.get("pdl", 0)
        pdc = pd_levels.get("pdc", 0)
        pdm = pd_levels.get("pdm", 0)
        touch = 0.002  # 0.2% зона касания
        if direction == "long":
            # Отбой от PDL или PDC снизу — хорошая точка входа
            if pdl and abs(close - pdl) / pdl < touch:
                score += 15
                long_r.append(f"📌 Отбой от PDL ({pdl:,.2f})")
            elif pdc and abs(close - pdc) / pdc < touch and close > pdc:
                score += 10
                long_r.append(f"📌 Выше PDC ({pdc:,.2f})")
            elif pdm and abs(close - pdm) / pdm < touch:
                score += 7
                long_r.append(f"📌 Зона PDM ({pdm:,.2f})")
            # Цена выше PDH — пробой с потенциалом
            if pdh and close > pdh * 1.001:
                score += 8
                long_r.append(f"🚀 Пробой PDH ({pdh:,.2f})")
        else:
            # Отбой от PDH сверху — хорошая точка шорта
            if pdh and abs(close - pdh) / pdh < touch:
                score += 15
                short_r.append(f"📌 Отбой от PDH ({pdh:,.2f})")
            elif pdc and abs(close - pdc) / pdc < touch and close < pdc:
                score += 10
                short_r.append(f"📌 Ниже PDC ({pdc:,.2f})")
            elif pdm and abs(close - pdm) / pdm < touch:
                score += 7
                short_r.append(f"📌 Зона PDM ({pdm:,.2f})")
            # Пробой PDL вниз
            if pdl and close < pdl * 0.999:
                score += 8
                short_r.append(f"🔻 Пробой PDL ({pdl:,.2f})")

    # MACD дивергенция — сильный разворотный сигнал
    if macd_div:
        if direction == "long" and "Бычья" in macd_div:
            score += 18
            long_r.append(macd_div)
        elif direction == "short" and "Медвежья" in macd_div:
            score += 18
            short_r.append(macd_div)
        elif direction == "long" and "Медвежья" in macd_div:
            score -= 10  # дивергенция против нас
        elif direction == "short" and "Бычья" in macd_div:
            score -= 10

    # Подтверждение открытием дня
    if day_open > 0:
        dev_open = (close / day_open - 1) * 100
        if direction == "long" and close > day_open * 1.003:
            score += 8
            long_r.append(f"Выше открытия дня ({dev_open:+.1f}%)")
        elif direction == "short" and close < day_open * 0.997:
            score += 8
            short_r.append(f"Ниже открытия дня ({dev_open:+.1f}%)")

    # MACD подтверждение
    if direction == "long" and macd_h > 0:
        score += 6; long_r.append("MACD > 0")
    elif direction == "short" and macd_h < 0:
        score += 6; short_r.append("MACD < 0")

    # RSI в нейтральной зоне — небольшой бонус
    if 40 < rsi < 60:
        score += 4  # нет перегрева
    elif direction == "long" and 60 < rsi < 70:
        score += 2  # RSI силён но не перекуплен
    elif direction == "short" and 30 < rsi < 40:
        score += 2

    # Свечной паттерн
    if direction == "long" and ("Бычье" in candle or "снизу" in candle):
        score += 8; long_r.append(candle)
    elif direction == "short" and ("Медвежье" in candle or "сверху" in candle):
        score += 8; short_r.append(candle)

    # Volume Profile — HVN/LVN
    if vp_nodes:
        vp_pts, vp_reasons = vp_score_adjustment(vp_nodes, close,
            "🟩 LONG" if direction == "long" else "🟥 SHORT/ВЫХОД")
        score += vp_pts
        if direction == "long":   long_r.extend(vp_reasons)
        else:                     short_r.extend(vp_reasons)

    # IMOEX режим — блокируем лонги при медвежьем рынке
    if imoex_regime:
        ir = imoex_regime.get("regime", "neutral")
        if ir == "bear" and direction == "long":
            score -= 15  # штраф за торговлю против рынка
            long_r.append("⚠️ IMOEX медвежий")
        elif ir == "bull" and direction == "long":
            score += 5
        elif ir == "bear" and direction == "short":
            score += 5  # бонус за совпадение с рынком

    score = min(100, max(0, score))
    min_score = mode_cfg.get("min_score", 70)

    if direction == "long":
        signal = "🟩 LONG" if score >= min_score else "НЕТ СИГНАЛА"
        reasons = long_r
    else:
        signal = "🟥 SHORT/ВЫХОД" if score >= min_score else "НЕТ СИГНАЛА"
        reasons = short_r

    return signal, score, reasons


# ══════════════════════════════════════════════
# MAIN ANALYSIS
# ══════════════════════════════════════════════
async def analyze_stock(ticker: str, tf: str = DEFAULT_TF, mode_cfg: dict = None) -> dict | None:
    if mode_cfg is None:
        mode_cfg = TRADE_MODES["mid"]
    ticker = ticker.upper()
    if ticker not in MOEX_STOCKS:
        return {"error": f"Тикер {ticker} не найден в списке MOEX_STOCKS."}

    _, name, sector = MOEX_STOCKS[ticker]

    try:
        df_task, news_task, imoex_task = await asyncio.gather(
            fetch_stock_data(ticker, tf),
            fetch_russian_news(ticker, sector),
            fetch_imoex_regime(),
            return_exceptions=True,
        )
        df_result, meta = df_task if not isinstance(df_task, Exception) else (None, None)
        news_items = news_task if not isinstance(news_task, Exception) else []
        imoex_regime = imoex_task if not isinstance(imoex_task, Exception) else None
    except Exception as e:
        logger.error(f"Error gathering data for {ticker}: {e}")
        return {"error": str(e)}

    if df_result is None or len(df_result) < 30:
        return {"error": f"Недостаточно данных ({ticker}, TF={tf})"}

    df = calculate_indicators(df_result, tf)
    df_closed = df.iloc[:-1].copy()
    price = float(df_closed["close"].iloc[-1])
    atr   = float(df_closed["atr"].dropna().iloc[-1]) if "atr" in df_closed.columns else price * 0.01
    regime  = detect_market_regime(df_closed)
    supports, resistances = find_support_resistance(df_closed, price)
    vp_nodes = find_hvn_lvn(df_closed, price)

    # ── Новые модули ──────────────────────────────────────────────────────
    # 1. Уровни предыдущего дня
    pd_levels  = get_previous_day_levels(df_closed)
    pd_level_name, pd_level_dist = get_pd_level_context(pd_levels, price)

    # 2. MACD дивергенция (лучше RSI на РФ рынке)
    macd_div   = detect_macd_divergence(df_closed)

    # 3. Фаза торговой сессии
    session    = get_session_phase()

    # 4. HTF тренд (параллельно с другими задачами)
    figi = MOEX_STOCKS[ticker][0]
    htf_trend  = await get_htf_trend(ticker, figi, tf)
    # ─────────────────────────────────────────────────────────────────────

    tech_signal, tech_score, tech_reasons = compute_tech_score(
        df_closed, mode_cfg, vp_nodes=vp_nodes, imoex_regime=imoex_regime,
        htf_trend=htf_trend, pd_levels=pd_levels, macd_div=macd_div)

    news_ai = await ai_evaluate_news(news_items, ticker, sector, tech_signal, tech_score)
    final_signal = news_ai.get("confirmed", tech_signal)

    # Блок итогового сигнала с учётом всех фильтров
    if imoex_regime and imoex_regime.get("regime") == "bear" and "LONG" in final_signal:
        final_signal = f"⚠️ {final_signal} (IMOEX медвежий)"

    if not session["enter"] and ("LONG" in final_signal or "SHORT" in final_signal):
        phase_warn = session.get("warning", "")
        final_signal = f"⏸ {final_signal} ({phase_warn or session['label']})"

    # Календарь — блокируем/предупреждаем перед важными событиями
    cal_check = check_calendar_block(ticker)
    if cal_check["block"] and ("LONG" in final_signal or "SHORT" in final_signal):
        final_signal = f"🚫 {final_signal} — СТОП (важное событие через <30 мин)"
    elif cal_check["score_penalty"] >= 15 and ("LONG" in final_signal or "SHORT" in final_signal):
        # Сильное событие близко — понижаем итоговый сигнал
        if "CONFIRMED" in final_signal:
            final_signal = final_signal.replace("CONFIRMED", "WEAK ⚠️ (событие)")
    # Снижаем tech_score на размер penalty (для фильтрации в сканере)
    if cal_check["score_penalty"] > 0:
        tech_score = max(0, tech_score - cal_check["score_penalty"])

    vp_supports    = ([vp_nodes["hvn_below"]["price"]] if vp_nodes.get("hvn_below") else []) + supports
    vp_resistances = ([vp_nodes["hvn_above"]["price"]] if vp_nodes.get("hvn_above") else []) + resistances
    sl_tp = calculate_sl_tp_stocks(tech_signal, price, atr, vp_supports, vp_resistances,
                                     pd_levels=pd_levels, vp_nodes=vp_nodes)

    return {
        "ticker": ticker, "name": name, "sector": sector, "tf": tf,
        "price": price, "atr": round(atr, 2), "atr_pct": round(atr / price * 100, 2),
        "tech_signal": tech_signal, "tech_score": tech_score, "tech_reasons": tech_reasons,
        "regime": regime, "imoex_regime": imoex_regime, "vp_nodes": vp_nodes,
        "htf_trend": htf_trend, "pd_levels": pd_levels,
        "pd_level_name": pd_level_name, "pd_level_dist": pd_level_dist,
        "macd_div": macd_div, "session": session,
        "news_items": news_items[:5], "news_ai": news_ai, "final_signal": final_signal,
        "sl_tp": sl_tp, "supports": supports, "resistances": resistances,
        "rsi_div": detect_rsi_divergence(df_closed), "candle": detect_candle_pattern(df_closed),
        "vol_ratio": round(float(df_closed["vol_ratio"].iloc[-1] or 1), 2),
        "time_warning": session.get("warning", ""),
        "calendar": cal_check,
    }


# ══════════════════════════════════════════════
# FORMAT ANALYSIS
# ══════════════════════════════════════════════
def format_analysis(result: dict) -> str:
    if "error" in result:
        return f"❌ {esc(result['error'])}"

    ticker = result["ticker"]; name = result["name"]; sector = result["sector"]
    tf = result["tf"]; price = result["price"]; atr_pct = result["atr_pct"]
    ts = result["tech_signal"]; tscore = result["tech_score"]; treasons = result["tech_reasons"]
    regime = result["regime"]; imoex = result.get("imoex_regime"); vp = result.get("vp_nodes", {})
    news_ai = result["news_ai"]; sl_tp = result["sl_tp"]; final = result["final_signal"]
    rsi_div = result["rsi_div"]; candle = result["candle"]; vol_r = result["vol_ratio"]
    news = result["news_items"]; time_warn = result.get("time_warning", "")

    bars = "█" * (tscore // 10) + "░" * (10 - tscore // 10)

    htf    = result.get("htf_trend", {})
    pd_lev = result.get("pd_levels", {})
    pd_nm  = result.get("pd_level_name", "")
    sess   = result.get("session", {})

    lines = [
        f"📊 <b>{esc(ticker)} — {esc(name)}</b> | {esc(sector.upper())}",
        f"⏱ <b>{tf}</b>  |  💰 <b>{price:,.2f} ₽</b>  |  ATR {atr_pct:.2f}%  |  Объём x{vol_r:.1f}",
    ]

    cal   = result.get("calendar", {})

    # Фаза сессии
    if sess:
        warn = f" — {esc(sess['warning'])}" if sess.get("warning") else ""
        lines.append(f"🕐 {esc(sess['label'])}{warn}")

    # Предупреждение календаря
    if cal and cal.get("warning"):
        cal_block = cal.get("block", False)
        lines.append(f"{'🚫' if cal_block else '⚠️'} <b>КАЛЕНДАРЬ:</b> {esc(cal['warning'])}")
    lines.append("")

    # HTF тренд
    if htf and htf.get("label"):
        lines.append(f"<b>{esc(htf['label'])}</b>")

    # IMOEX
    if imoex:
        slope_arrow = "↑" if imoex.get("slope_10d", 0) > 0 else "↓"
        lines.append(
            f"<b>🏛 {esc(imoex['label'])}</b>  "
            f"СБЕР: {imoex['price']:,.2f} ₽  {slope_arrow}{imoex.get('slope_10d',0):+.2f}%"
        )

    # Уровни предыдущего дня
    if pd_lev:
        pdh = pd_lev.get("pdh", 0); pdl = pd_lev.get("pdl", 0)
        pdc = pd_lev.get("pdc", 0); gap = pd_lev.get("gap_pct", 0)
        pd_line = f"📅 PDH: {pdh:,.2f}  PDL: {pdl:,.2f}  PDC: {pdc:,.2f}"
        if abs(gap) > 0.3:
            pd_line += f"  Гэп: {gap:+.2f}%"
        if pd_nm:
            pd_line += f"  ⚡ Цена у <b>{pd_nm}</b>"
        lines.append(pd_line)
    lines.append("")

    lines += [
        f"<b>🔧 ТЕХНИЧЕСКИЙ АНАЛИЗ</b>",
        f"{ts} (Скор: {tscore}/100)",
        f"<code>{bars}</code>",
        f"Факторы: {esc(', '.join(treasons[:3]))}",
        f"Состояние: {esc(regime['label'])}",
    ]
    if rsi_div:
        lines.append(esc(rsi_div))
    if candle and candle != "Обычная свеча":
        lines.append(f"Свеча: {esc(candle)}")

    if vp:
        vp_lines = []
        if vp.get("poc"):
            vp_lines.append(f"POC (макс. объем): <b>{vp['poc']:,.2f} ₽</b>")
        if vp.get("hvn_above"):
            vp_lines.append(f"HVN сверху: {vp['hvn_above']['price']:,.2f} ₽ — сопротивление")
        if vp.get("hvn_below"):
            vp_lines.append(f"HVN снизу: {vp['hvn_below']['price']:,.2f} ₽ — поддержка")
        if vp_lines:
            lines += ["", "<b>📊 VOLUME PROFILE</b>"] + vp_lines

    lines += ["", "<b>📰 НОВОСТНОЙ ФИЛЬТР</b>"]
    fs = news_ai.get("filter_status", "CONFIRMED")
    ew = news_ai.get("event_weight", 0)
    ev = news_ai.get("event_type", "нет событий")
    summ = news_ai.get("summary", "")
    fs_emoji = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀", "BLOCKED": "🚫", "NEWS_ONLY": "📢"}.get(fs, "⚪")
    ew_sign = f"+{ew}" if ew > 0 else str(ew)
    lines.append(f"{fs_emoji} <b>{fs}</b>  |  Вес события: {ew_sign}/10")
    if ev and ev != "нет событий":
        lines.append(f"Событие: {esc(ev)}")
    if summ:
        lines.append(f"<i>{esc(summ)}</i>")

    lines += ["", f"<b>🎯 СИГНАЛ: {esc(final)}</b>"]
    if time_warn:
        lines.append(f"<i>{esc(time_warn)}</i>")

    if sl_tp:
        lines += [
            "", "<b>📐 ЦЕЛИ (ИНТРАДЕЙ)</b>",
            f"STOP: {sl_tp['sl']:,.2f} ₽  ({sl_tp['risk_pct']:.2f}% риск)",
            f"TP1:  {sl_tp['tp1']:,.2f} ₽",
            f"TP2:  {sl_tp['tp2']:,.2f} ₽  (R/R {sl_tp['rr_ratio']:.1f})",
        ]
        if sl_tp.get("warn"):
            lines.append(sl_tp["warn"])

    fact_news = [it for it in news if it.get("is_fact")]
    neutral_news = [it for it in news if not it.get("is_fact") and not it.get("is_opinion")]

    if fact_news:
        lines += ["", "📋 <b>Корпоративные факты:</b>"]
        for it in fact_news[:3]:
            w = it.get("weight", 0)
            w_str = f"+{w}" if w > 0 else str(w)
            w_e = "🟢" if w > 2 else ("🔴" if w < -2 else "⚪")
            lines.append(f"{w_e} [{w_str}] {esc(it['title'][:90])}")
    elif neutral_news:
        lines += ["", "📌 <b>Лента новостей:</b>"]
        for it in neutral_news[:3]:
            lines.append(f"⚪ {esc(it['title'][:90])}")

    lines += [
        "",
        f"<i>⏰ {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК</i>",
        "<i>⚠️ Сделки закрываются внутри дня перед клирингом в 23:50. Без переноса на ночь.</i>",
    ]
    return "\n".join(lines)


# ══════════════════════════════════════════════
# USER STATE
# ══════════════════════════════════════════════
_user_state: dict = {}


def get_user_state(chat_id: int) -> dict:
    return _user_state.get(chat_id, {"mode": "mid", "tf": DEFAULT_TF})


def set_user_state(chat_id: int, **kwargs):
    s = get_user_state(chat_id)
    s.update(kwargs)
    _user_state[chat_id] = s


# ══════════════════════════════════════════════
# FIGI UPDATER (из get_figi.py)
# ══════════════════════════════════════════════
FIGI_FILE = Path("figi_result.json")


FUTURES_EXPIRY_FILE = Path("futures_expiry.json")


def _load_figi_from_file():
    """Загружает FIGI из файла и обновляет MOEX_STOCKS в памяти."""
    if not FIGI_FILE.exists():
        return 0
    try:
        data = json.loads(FIGI_FILE.read_text())
        updated = 0
        for ticker, info in data.items():
            if ticker in MOEX_STOCKS and info.get("figi"):
                old_entry = MOEX_STOCKS[ticker]
                MOEX_STOCKS[ticker] = (info["figi"], old_entry[1], old_entry[2])
                updated += 1
        if updated:
            logger.info(f"FIGI loaded from file: {updated} tickers updated")
        return updated
    except Exception as e:
        logger.warning(f"Failed to load FIGI from file: {e}")
        return 0


async def fetch_nearest_futures() -> dict:
    """
    Получает список активных фьючерсов через Tinkoff API.
    Автоматически выбирает ближний контракт по каждому базовому активу.
    Обновляет FUTURES в памяти.
    Возвращает dict: base_asset -> {ticker, figi, expiry}
    """
    headers = {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }
    result = {}
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures",
                headers=headers,
                json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
            ) as r:
                if r.status != 200:
                    logger.warning(f"GetFutures: HTTP {r.status}")
                    return {}
                data = await r.json()

        now = datetime.now(timezone.utc)
        # Группируем по базовому активу (первые 2 символа тикера)
        by_base: dict[str, list] = {}
        for inst in data.get("instruments", []):
            ticker   = inst.get("ticker", "")
            figi     = inst.get("figi", "")
            exp_str  = inst.get("expirationDate", "")
            exchange = inst.get("exchange", "")
            if not figi or not ticker or exchange not in ("FORTS", "MOEX", "FORTS_EVENING"):
                continue
            # Базовый актив = первые 2 буквы (Si, Ri, BR, GD и т.д.)
            base = ticker[:2].upper()
            try:
                exp_dt = datetime.fromisoformat(exp_str.replace("Z", "+00:00"))
            except Exception:
                continue
            if exp_dt < now:
                continue  # уже истёк
            by_base.setdefault(base, []).append({
                "ticker": ticker,
                "figi":   figi,
                "expiry": exp_dt,
            })

        # Выбираем ближний контракт по каждому базовому активу
        for base, contracts in by_base.items():
            nearest = min(contracts, key=lambda x: x["expiry"])
            result[base] = nearest

        # Обновляем FUTURES в памяти
        for code, info in list(FUTURES.items()):
            base = code[:2].upper()
            if base in result:
                near = result[base]
                old  = FUTURES[code]
                # Обновляем FIGI и тикер если изменился
                FUTURES[code] = (near["figi"],) + old[1:]
                logger.debug(f"Futures {code}: FIGI → {near['figi']} (exp {near['expiry'].date()})")

        # Сохраняем для информации
        save_data = {k: {"ticker": v["ticker"], "figi": v["figi"],
                         "expiry": v["expiry"].isoformat()}
                     for k, v in result.items()}
        FUTURES_EXPIRY_FILE.write_text(json.dumps(save_data, ensure_ascii=False, indent=2))
        logger.info(f"Futures FIGI updated: {len(result)} base assets")

    except Exception as e:
        logger.error(f"fetch_nearest_futures: {e}")

    return result


async def update_figi_data() -> tuple[int | None, str | None]:
    """
    Запрашивает актуальные FIGI у Tinkoff API,
    сохраняет в figi_result.json и обновляет MOEX_STOCKS в памяти.
    """
    headers = {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares",
                headers=headers, json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
            ) as r:
                if r.status != 200:
                    return None, f"API Error: {r.status}"
                data = await r.json()

        instruments = data.get("instruments", [])
        # Индексируем всё с MOEX
        api_index = {}
        for inst in instruments:
            ticker   = inst.get("ticker", "")
            exchange = inst.get("exchange", "")
            currency = inst.get("currency", "")
            if exchange in ("MOEX", "MOEX_PLUS") and currency == "rub" and inst.get("figi"):
                api_index[ticker] = {
                    "figi": inst["figi"],
                    "name": inst.get("name", ""),
                    "uid":  inst.get("uid", ""),
                }

        # Обновляем MOEX_STOCKS в памяти
        updated, not_found = [], []
        for ticker in list(MOEX_STOCKS.keys()):
            if ticker in api_index:
                old_entry = MOEX_STOCKS[ticker]
                MOEX_STOCKS[ticker] = (api_index[ticker]["figi"], old_entry[1], old_entry[2])
                updated.append(ticker)
            else:
                not_found.append(ticker)

        # Также обновляем FUTURES через отдельный запрос (GetFutures)
        fut_updated = 0
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/Futures",
                    headers=headers, json={"instrumentStatus": "INSTRUMENT_STATUS_BASE"},
                ) as r2:
                    if r2.status == 200:
                        fdata = await r2.json()
                        fut_index = {
                            i.get("ticker",""): i.get("figi","")
                            for i in fdata.get("instruments", [])
                            if i.get("exchange","") in ("MOEX","FORTS","FORTS_EVENING")
                            and i.get("figi")
                        }
                        for code in list(FUTURES.keys()):
                            if code in fut_index:
                                old = FUTURES[code]
                                FUTURES[code] = (fut_index[code],) + old[1:]
                                fut_updated += 1
                        # Дописываем в файл
                        api_index.update({k: {"figi": v} for k, v in fut_index.items()})
        except Exception as fe:
            logger.warning(f"Futures FIGI update: {fe}")

        # Сохраняем в файл (для следующего запуска)
        FIGI_FILE.write_text(json.dumps(api_index, indent=2, ensure_ascii=False))

        logger.info(f"FIGI updated: акции {len(updated)} OK, фьючерсы {fut_updated} OK")
        if not_found:
            logger.warning(f"Акции не найдены в API: {not_found}")

        return len(updated) + fut_updated, None

    except Exception as e:
        logger.error(f"FIGI update failed: {e}")
        return None, str(e)


# ══════════════════════════════════════════════
# TELEGRAM COMMANDS
# ══════════════════════════════════════════════
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🏛 <b>MOEX Intraday Bot</b>\n"
        "Интрадей-сигналы 1-2 эшелона МосБиржи.\n"
        "Работает во время основной и вечерней сессии (10:00 - 23:50 МСК).\n\n"
        "<b>📊 Функции:</b>\n"
        "/analyze SBER — технический + vwap анализ\n"
        "/watchlist — управление ватчлистом\n"
        "/scan — ручной запуск сканера\n"
        "/scan_start — автосигналы каждые 30 минут\n"
        "/scan_stop — выключить автосигналы\n"
        "/mode — настройки риска (LOW / MID / HARD)\n"
        "/tf — таймфрейм (15m по умолчанию)\n"
        "/trades — открытые позиции\n"
        "/open_trade — открыть сделку вручную\n"
        "/close_trade — закрыть сделку\n"
        "/update_figi — обновить базу FIGI\n\n"
        "<b>📈 Фьючерсы FORTS:</b>\n"
        "/futures CODE — анализ фьючерса (SBM6, BRM6, RIM6...)\n"
        "/scan_futures — сканер фьючерсов\n"
        "/futures_list — все доступные\n"
        "/fadd_all — добавить все сразу\n"
        "/fadd CODE — добавить один\n"
        "/fremove CODE — убрать\n\n"
        "<b>🚀 Памп/Дамп детектор:</b>\n"
        "/pd — найти памп/дамп прямо сейчас\n"
        "/pd futures — только фьючерсы\n"
        "/pd stocks — только акции\n"
        "Авто-алерты: включаются вместе с /scan_start\n\n"
        "<b>📅 Календарь событий:</b>\n"
        "/calendar — заседания ЦБ, отсечки, события\n"
        "/calendar update — загрузить с MOEX и ЦБ\n"
        "/calendar_add — добавить событие вручную\n\n"
        "<i>⚠️ Все позиции закрываются до 23:50 МСК. Без овернайтов.</i>"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wl = load_watchlist()
    lines = [f"🗂 <b>Мой ватчлист ({len(wl)}/100):</b>\n"]
    for t in wl:
        info = MOEX_STOCKS.get(t)
        if info:
            _, name, sector = info
            lines.append(f"<code>{t}</code> — {name} <i>({sector})</i>")
        else:
            lines.append(f"<code>{t}</code>")
    lines += ["", "/add TICKER — добавить инструмент",
              "/remove TICKER — удалить инструмент",
              "/clear_watchlist — очистить весь ватчлист"]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_all_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sector_map: dict[str, list] = {}
    for ticker, (_, name, sector) in MOEX_STOCKS.items():
        sector_map.setdefault(sector, []).append(f"<code>{ticker}</code> {name}")
    lines = ["📋 <b>Инструменты МосБиржи (1-2 эшелон)</b>\n"]
    for sector, items in sorted(sector_map.items()):
        lines.append(f"\n<b>{sector.upper()}</b>")
        lines.extend(items)
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /add TICKER")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = add_to_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Формат: /remove TICKER")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = remove_from_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_add_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    all_tickers = list(MOEX_STOCKS.keys())
    added, skipped = [], []
    for t in all_tickers:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)
    await update.message.reply_text(f"Добавлено: {len(added)}, уже в списке: {len(skipped)}")


async def cmd_clear_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or args[0].lower() != "confirm":
        await update.message.reply_text("Для очистки всего списка введите: /clear_watchlist confirm", parse_mode="HTML")
        return
    save_watchlist([])
    await update.message.reply_text("🗑 Ватчлист успешно очищен.")


async def cmd_add_sector(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        sectors = sorted(set(v[2] for v in MOEX_STOCKS.values()))
        lines = ["📂 <b>Доступные секторы рынка:</b>\n"]
        for s in sectors:
            lines.append(f"<code>/add_sector {s}</code>")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        return
    sector_query = " ".join(context.args).lower().strip()
    to_add = [t for t, v in MOEX_STOCKS.items() if v[2].lower() == sector_query]
    added, skipped = [], []
    for t in to_add:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)
    await update.message.reply_text(f"Добавлено из сектора {sector_query}: {len(added)}")


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Формат: /analyze TICKER [таймфрейм]")
        return
    ticker = args[0].upper()
    tf = args[1].lower() if len(args) > 1 else get_user_state(update.effective_chat.id)["tf"]
    if tf not in TF_MAP:
        tf = DEFAULT_TF

    msg = await update.message.reply_text(f"⏳ Рассчитываю индикаторы для <b>{ticker}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(update.effective_chat.id)["mode"]]
    result = await analyze_stock(ticker, tf, mode_cfg)
    text = format_analysis(result)

    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("📰 Новости", callback_data=f"news_{ticker}"),
            InlineKeyboardButton("5м", callback_data=f"analyze_{ticker}_5m"),
            InlineKeyboardButton("15м", callback_data=f"analyze_{ticker}_15m"),
            InlineKeyboardButton("1ч", callback_data=f"analyze_{ticker}_1h"),
        ])
        kb.append([
            InlineKeyboardButton("1д график", callback_data=f"analyze_{ticker}_1d"),
            InlineKeyboardButton("➕ В Ватчлист", callback_data=f"wl_add_{ticker}"),
        ])
        sl_tp = result.get("sl_tp", {})
        if sl_tp and result.get("tech_signal") in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
            direction = "LONG" if "LONG" in result["tech_signal"] else "SHORT"
            price = result["price"]
            sl = sl_tp.get("sl", 0)
            tp1 = sl_tp.get("tp1", 0)
            tp2 = sl_tp.get("tp2", 0)
            tp3 = sl_tp.get("tp3", 0)
            kb.append([
                InlineKeyboardButton(
                    f"✅ Войти в сделку ({direction})",
                    callback_data=f"enter_{ticker}_{direction}_{price:.2f}_{sl:.2f}_{tp1:.2f}_{tp2:.2f}_{tp3:.2f}"
                )
            ])

    markup = InlineKeyboardMarkup(kb) if kb else None
    await msg.edit_text(text, parse_mode="HTML", reply_markup=markup)


async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    ticker = args[0].upper() if args else ""
    sector = ""
    if ticker in MOEX_STOCKS:
        _, _, sector = MOEX_STOCKS[ticker]
    msg = await update.message.reply_text(f"⏳ Поиск событий по {ticker or 'рынку'}...", parse_mode="HTML")
    news = await fetch_russian_news(ticker, sector)
    if not news:
        await msg.edit_text("📭 Новостных фактов за последнее время не обнаружено.")
        return
    lines = [f"📰 <b>События рынка — {ticker or 'MOEX'}</b>\n"]
    for it in news[:6]:
        sp = "🔵" if it.get("is_specific") else "⚪"
        lines.append(f"{sp} <b>{it['title'][:120]}</b>")
        lines.append(f"  └ {it['source']} | {it['pub'][:16]}")
        ai = await ai_classify_news_impact(it["title"], ticker or "IMOEX")
        s_e = {"позитив": "🟢", "негатив": "🔴", "нейтрально": "⚪"}.get(ai["sentiment"], "⚪")
        lines.append(f"  └ Влияние: {s_e} {ai['sentiment']} ({ai['score']}/10)")
        lines.append("")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")


async def cmd_market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Загружаю текущую картину рынка...", parse_mode="HTML")
    tasks = [
        fetch_market_news(),
        analyze_stock("SBER", "1h", TRADE_MODES["mid"]),
        analyze_stock("GAZP", "1h", TRADE_MODES["mid"]),
        analyze_stock("LKOH", "1h", TRADE_MODES["mid"]),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    news = results[0] if not isinstance(results[0], Exception) else []
    stocks = [r for r in results[1:] if not isinstance(r, Exception) and r and "error" not in r]

    lines = ["🏛 <b>MOEX ТЕКУЩИЙ СТАТУС</b>\n"]
    if stocks:
        lines.append("<b>Ведущие инструменты (1h):</b>")
        for s in stocks:
            sig_e = {"🟩 LONG": "🟢", "🟥 SHORT/ВЫХОД": "🔴"}.get(s["tech_signal"], "⚪")
            lines.append(
                f"{sig_e} <b>{s['ticker']}</b> — {s['price']:,.2f} ₽  "
                f"Скор: {s['tech_score']} ({s['regime']['label']})"
            )
        lines.append("")
    if news:
        lines.append("<b>Лента новостей:</b>")
        for it in news[:5]:
            lines.append(f"⚪ {esc(it['title'][:100])}")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")



# ══════════════════════════════════════════════
# ФЬЮЧЕРСНЫЙ АНАЛИЗ
# ══════════════════════════════════════════════

async def _find_futures_figi(ticker: str) -> str | None:
    """Ищет FIGI фьючерса по тикеру через FindInstrument."""
    cache_key = f"fut_figi_{ticker}"
    if cache_key in _cache and time.time() - _cache[cache_key]["ts"] < 86400:
        return _cache[cache_key]["figi"]
    try:
        headers = {"Authorization": f"Bearer {TINKOFF_TOKEN}", "Content-Type": "application/json"}
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument",
                headers=headers,
                json={"query": ticker, "instrumentKind": "INSTRUMENT_TYPE_FUTURES",
                      "apiTradeAvailableFlag": False},
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                instruments = data.get("instruments", [])
                # Ищем точное совпадение тикера
                for inst in instruments:
                    if inst.get("ticker", "").upper() == ticker.upper():
                        figi = inst.get("figi", "")
                        if figi:
                            _cache[cache_key] = {"figi": figi, "ts": time.time()}
                            # Обновляем FUTURES в памяти
                            if ticker in FUTURES:
                                old = FUTURES[ticker]
                                FUTURES[ticker] = (figi,) + old[1:]
                            return figi
                # Если точного нет — берём первый
                if instruments:
                    figi = instruments[0].get("figi", "")
                    if figi:
                        _cache[cache_key] = {"figi": figi, "ts": time.time()}
                        if ticker in FUTURES:
                            old = FUTURES[ticker]
                            FUTURES[ticker] = (figi,) + old[1:]
                        return figi
    except Exception as e:
        logger.warning(f"FindInstrument {ticker}: {e}")
    return None


async def analyze_futures(code: str, tf: str = "15m", mode_cfg: dict = None) -> dict:
    """Анализ фьючерса — те же индикаторы что и для акций."""
    if mode_cfg is None:
        mode_cfg = TRADE_MODES["mid"]
    code = code.upper()
    info = FUTURES.get(code)
    if not info:
        return {"error": f"Фьючерс {code} не найден. /futures_list — список доступных"}

    figi, name, category, tick, lot = info

    # Если FIGI заглушка — ищем через FindInstrument
    if not figi or figi.startswith("FUT") and len(figi) < 15:
        found_figi = await _find_futures_figi(code)
        if found_figi:
            figi = found_figi
        else:
            return {
                "error": f"FIGI для {code} не найден.\nЗапусти /update_figi для обновления базы."
            }

    interval, _, limit = TF_MAP.get(tf, TF_MAP["15m"])
    df = await fetch_candles_tinkoff(figi, interval, limit)

    if df is None or len(df) < 20:
        return {"error": f"Недостаточно данных для {code} [{tf}].\nВозможно неверный FIGI — запусти /update_figi"}

    df = calculate_indicators(df)
    df_c = df.iloc[:-1].copy()
    price = float(df_c["close"].iloc[-1])
    atr   = float(df_c["atr"].dropna().iloc[-1]) if "atr" in df_c.columns else price * 0.01
    regime = detect_market_regime(df_c)

    tech_signal, tech_score, tech_reasons = compute_tech_score(df_c, mode_cfg)
    supports, resistances = find_support_resistance(df_c, price)
    sl_tp = calculate_sl_tp_stocks(tech_signal, price, atr, supports, resistances)

    return {
        "ticker":       code,
        "name":         name,
        "sector":       category,
        "tf":           tf,
        "price":        price,
        "atr":          round(atr, 4),
        "atr_pct":      round(atr / price * 100, 2) if price else 0,
        "tech_signal":  tech_signal,
        "tech_score":   tech_score,
        "tech_reasons": tech_reasons,
        "regime":       regime,
        "sl_tp":        sl_tp,
        "supports":     supports,
        "resistances":  resistances,
        "rsi_div":      detect_rsi_divergence(df_c),
        "candle":       detect_candle_pattern(df_c),
        "vol_ratio":    round(float(df_c["vol_ratio"].iloc[-1] or 1), 2),
        "news_ai":      {"filter_status": "NO_SIGNAL", "event_weight": 0,
                         "confirmed": tech_signal, "summary": "",
                         "blocking": [], "underreaction": False,
                         "sentiment": "нейтрально", "score": 0,
                         "fact_events": [], "opinions_skipped": 0},
        "final_signal": tech_signal,
        "is_futures":   True,
        "tick_size":    tick,
        "lot_value":    lot,
    }


async def cmd_futures(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /futures CODE [tf] — анализ фьючерса
    /futures — список доступных
    """
    args = context.args
    if not args:
        await cmd_futures_list(update, context)
        return

    chat_id = update.effective_chat.id
    code = args[0].upper()
    tf   = args[1].lower() if len(args) > 1 else get_user_state(chat_id)["tf"]
    if tf not in TF_MAP:
        tf = "15m"

    msg = await update.message.reply_text(
        f"⏳ Анализирую фьючерс <b>{esc(code)}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    result = await analyze_futures(code, tf, mode_cfg)
    text   = format_analysis(result)

    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("5м",  callback_data=f"fut_{code}_5m"),
            InlineKeyboardButton("15м", callback_data=f"fut_{code}_15m"),
            InlineKeyboardButton("1ч",  callback_data=f"fut_{code}_1h"),
        ])
        sl_tp = result.get("sl_tp", {})
        if sl_tp and result.get("tech_signal") in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
            direction = "LONG" if "LONG" in result["tech_signal"] else "SHORT"
            p  = result["price"]
            sl = sl_tp.get("sl", 0)
            t1 = sl_tp.get("tp1", 0)
            t2 = sl_tp.get("tp2", 0)
            t3 = sl_tp.get("tp3", 0)
            kb.append([InlineKeyboardButton(
                f"✅ Войти ({direction})",
                callback_data=f"enter_{code}_{direction}_{p:.4f}_{sl:.4f}_{t1:.4f}_{t2:.4f}_{t3:.4f}"
            )])

    await msg.edit_text(text, parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)


async def cmd_futures_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список всех доступных фьючерсов."""
    cats: dict[str, list] = {}
    for code, (_, name, cat, tick, _) in FUTURES.items():
        cats.setdefault(cat, []).append(f"<code>{code}</code> — {name}")
    lines = ["📋 <b>Доступные фьючерсы FORTS</b>\n"]
    cat_labels = {"валюта": "💵 Валюта", "индекс": "📈 Индексы",
                  "товар": "🛢 Товары", "акция": "📊 Акционные"}
    for cat, items in cats.items():
        lines.append(f"\n<b>{cat_labels.get(cat, cat)}</b>")
        lines.extend(items)
    lines += [
        "",
        "Анализ: /futures CODE [tf]",
        "Пример: /futures SiZ5 15m",
        "Сканер: /scan_futures",
        "Добавить в сканер: /fadd CODE",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_scan_futures(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сканирует фьючерсный ватчлист."""
    chat_id  = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf       = get_user_state(chat_id)["tf"]
    wl       = load_futures_watchlist()

    if not wl:
        await update.message.reply_text(
            "📭 Фьючерсный ватчлист пуст.\n/fadd CODE — добавить\n/futures_list — список")
        return

    msg = await update.message.reply_text(
        f"🔍 Сканирую {len(wl)} фьючерсов [{tf}]...", parse_mode="HTML")

    long_sigs, short_sigs, watch_sigs = [], [], []
    for i in range(0, len(wl), 4):
        batch = wl[i:i+4]
        tasks = [analyze_futures(c, tf, mode_cfg) for c in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) or not r or "error" in r:
                continue
            sig = r["tech_signal"]
            if sig == "🟩 LONG":      long_sigs.append(r)
            elif sig == "🟥 SHORT/ВЫХОД": short_sigs.append(r)
            elif r["tech_score"] >= 52:   watch_sigs.append(r)
        await asyncio.sleep(0.3)

    long_sigs.sort(key=lambda x: -x["tech_score"])
    short_sigs.sort(key=lambda x: -x["tech_score"])
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")

    if not long_sigs and not short_sigs:
        await msg.edit_text(
            f"😶 Сигналов по фьючерсам нет [{tf}]\n<i>{ts}</i>", parse_mode="HTML")
        return

    lines = [f"🔍 <b>СКАНЕР ФЬЮЧЕРСОВ FORTS</b> | {tf}", f"<i>{ts}</i>", ""]
    for sig_list, label in [(long_sigs, "🟩 ПОКУПКА"), (short_sigs, "🟥 ПРОДАЖА")]:
        if sig_list:
            lines.append(f"<b>{label}:</b>")
            for s in sig_list[:5]:
                lines.append(
                    f"<b>{esc(s['ticker'])}</b> — {esc(s['name'])}\n"
                    f" 💰 {s['price']:,.2f} | Скор: {s['tech_score']}/100\n"
                    f" ⚙️ {esc(s['tech_reasons'][0] if s['tech_reasons'] else '—')}"
                )
                lines.append("")
    if watch_sigs:
        lines.append("👀 <b>На радаре:</b>")
        for s in watch_sigs[:3]:
            lines.append(f" <b>{esc(s['ticker'])}</b> {s['price']:,.2f} Скор:{s['tech_score']}")

    # Кнопки: анализ + вход для топ-3
    top = (long_sigs + short_sigs)[:3]
    kb  = []
    for s in top:
        sl_tp     = s.get("sl_tp", {})
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        row = [InlineKeyboardButton(f"📊 {s['ticker']}", callback_data=f"fut_{s['ticker']}_{tf}")]
        if sl_tp and sl_tp.get("sl"):
            p  = s["price"]; sl = sl_tp.get("sl",0)
            t1 = sl_tp.get("tp1",0); t2 = sl_tp.get("tp2",0); t3 = sl_tp.get("tp3",0)
            row.append(InlineKeyboardButton(
                f"✅ Войти {direction}",
                callback_data=f"enter_{s['ticker']}_{direction}_{p:.4f}_{sl:.4f}_{t1:.4f}_{t2:.4f}_{t3:.4f}"
            ))
        kb.append(row)
    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)


async def cmd_fadd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавить фьючерс в ватчлист сканера."""
    if not context.args:
        await update.message.reply_text("Использование: /fadd CODE\nПример: /fadd SiZ5")
        return
    code = context.args[0].upper()
    if code not in FUTURES:
        similar = [c for c in FUTURES if c[:2] == code[:2]]
        hint = f" Похожие: {', '.join(similar[:4])}" if similar else ""
        await update.message.reply_text(f"❌ {esc(code)} не найден.{hint}\n/futures_list")
        return
    wl = load_futures_watchlist()
    if code in wl:
        await update.message.reply_text(f"ℹ️ {code} уже в ватчлисте.")
        return
    wl.append(code)
    save_futures_watchlist(wl)
    _, name, cat, _, _ = FUTURES[code]
    await update.message.reply_text(
        f"✅ <b>{code}</b> ({name}) добавлен в фьючерсный ватчлист.\nВсего: {len(wl)}",
        parse_mode="HTML")


async def cmd_fremove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Убрать фьючерс из ватчлиста."""
    if not context.args:
        await update.message.reply_text("Использование: /fremove CODE")
        return
    code = context.args[0].upper()
    wl = load_futures_watchlist()
    if code not in wl:
        await update.message.reply_text(f"ℹ️ {code} не найден в ватчлисте.")
        return
    wl.remove(code)
    save_futures_watchlist(wl)
    await update.message.reply_text(f"🗑 {code} удалён. Осталось: {len(wl)}")


# ══════════════════════════════════════════════
# ПАМП/ДАМП ДЕТЕКТОР
# ══════════════════════════════════════════════
# Детектирует резкие движения на акциях И фьючерсах.
# Памп: рост >N% за последние M свечей + объём >2x
# Дамп: падение >N% за последние M свечей + объём >2x
# Сигнал: контртрендовый вход (SHORT на памп, LONG на дамп)

# Пороги памп/дамп — только настоящие резкие движения
PUMP_DUMP_CONFIG = {
    "15m": {"threshold_pct": 5.0,  "lookback": 3, "vol_mult": 2.5,
            "sl_pct": 1.2, "tp1_pct": 2.5, "tp2_pct": 4.0, "tp3_pct": 6.0},
    "5m":  {"threshold_pct": 3.5,  "lookback": 4, "vol_mult": 3.0,
            "sl_pct": 0.8, "tp1_pct": 1.8, "tp2_pct": 3.0, "tp3_pct": 4.5},
    "1h":  {"threshold_pct": 7.0,  "lookback": 2, "vol_mult": 2.0,
            "sl_pct": 2.0, "tp1_pct": 3.5, "tp2_pct": 6.0, "tp3_pct": 9.0},
}

def detect_pump_dump(df: pd.DataFrame, tf: str = "15m") -> dict | None:
    """
    Анализирует последние свечи на наличие памп/дамп паттерна.
    Возвращает dict с сигналом или None если паттерна нет.
    """
    cfg = PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG["15m"])
    threshold = cfg["threshold_pct"]
    lookback  = cfg["lookback"]
    vol_mult  = cfg["vol_mult"]

    if len(df) < lookback + 10:
        return None

    recent   = df.tail(lookback + 1)
    baseline = df.tail(30).head(30 - lookback)  # базовый объём

    price_now   = float(recent["close"].iloc[-1])
    price_start = float(recent["close"].iloc[0])
    pct_change  = (price_now - price_start) / price_start * 100

    avg_vol = float(baseline["volume"].mean()) if len(baseline) > 0 else 0
    cur_vol = float(recent["volume"].sum())
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0

    # Фильтр: нужно движение И объём
    if abs(pct_change) < threshold or vol_ratio < vol_mult:
        return None

    is_pump = pct_change > 0
    signal_type = "PUMP" if is_pump else "DUMP"

    # Контртрендовый сигнал
    trade_direction = "SHORT" if is_pump else "LONG"

    # Уровни входа
    entry = price_now
    if is_pump:
        sl  = round(entry * (1 + cfg["sl_pct"] / 100), 4)
        tp1 = round(entry * (1 - cfg["tp1_pct"] / 100), 4)
        tp2 = round(entry * (1 - cfg["tp2_pct"] / 100), 4)
        tp3 = round(entry * (1 - cfg["tp3_pct"] / 100), 4)
    else:
        sl  = round(entry * (1 - cfg["sl_pct"] / 100), 4)
        tp1 = round(entry * (1 + cfg["tp1_pct"] / 100), 4)
        tp2 = round(entry * (1 + cfg["tp2_pct"] / 100), 4)
        tp3 = round(entry * (1 + cfg["tp3_pct"] / 100), 4)

    # Сила сигнала
    strength = min(100, int(
        (abs(pct_change) / threshold * 40) +
        (min(vol_ratio, 5) / 5 * 40) +
        (20 if abs(pct_change) > threshold * 1.5 else 0)
    ))

    return {
        "type":       signal_type,
        "direction":  trade_direction,
        "pct_change": round(pct_change, 2),
        "vol_ratio":  round(vol_ratio, 1),
        "strength":   strength,
        "entry":      entry,
        "sl":         sl,
        "tp1":        tp1,
        "tp2":        tp2,
        "tp3":        tp3,
        "candles":    lookback,
    }


# Хранит уже отправленные памп/дамп алерты (дедупликация)
_pd_sent: dict[str, float] = {}  # ticker -> timestamp последнего алерта

async def _scan_pump_dump(tickers: list[str], tf: str,
                          is_futures: bool = False) -> list[dict]:
    """Сканирует список тикеров на памп/дамп. Возвращает найденные события."""
    results = []
    now = time.time()

    for i in range(0, len(tickers), 6):
        batch = tickers[i:i+6]
        tasks = []
        for code in batch:
            if is_futures:
                info = FUTURES.get(code)
                if info:
                    figi = info[0]
                    tasks.append((code, info[1], fetch_candles_tinkoff(
                        figi, TF_MAP[tf][0], 50)))
            else:
                info = MOEX_STOCKS.get(code)
                if info:
                    figi = info[0]
                    tasks.append((code, info[1], fetch_candles_tinkoff(
                        figi, TF_MAP[tf][0], 50)))

        fetched = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

        for (code, name, _), df in zip(tasks, fetched):
            if isinstance(df, Exception) or df is None or len(df) < 20:
                continue

            pd_signal = detect_pump_dump(df, tf)
            if not pd_signal:
                continue

            # Дедупликация: не слать один тикер чаще раз в 30 минут
            last_sent = _pd_sent.get(f"pd_{code}", 0)
            if now - last_sent < 1800:
                continue

            _pd_sent[f"pd_{code}"] = now
            results.append({
                **pd_signal,
                "ticker":      code,
                "name":        name,
                "tf":          tf,
                "is_futures":  is_futures,
            })

        await asyncio.sleep(0.3)

    return results


def format_pump_dump_alert(s: dict) -> str:
    """Форматирует алерт памп/дамп для Telegram."""
    t         = s["ticker"]
    name      = s["name"]
    sig_type  = s["type"]
    direction = s["direction"]
    pct       = s["pct_change"]
    vol_r     = s["vol_ratio"]
    strength  = s["strength"]
    entry     = s["entry"]
    sl        = s["sl"]
    tp1, tp2, tp3 = s["tp1"], s["tp2"], s["tp3"]
    tf        = s["tf"]

    type_e  = "🚀" if sig_type == "PUMP" else "💥"
    dir_e   = "🟥 SHORT" if direction == "SHORT" else "🟩 LONG"
    fut_tag = " [фьюч]" if s.get("is_futures") else ""

    sl_pct  = abs(entry - sl) / entry * 100
    rr      = abs(entry - tp2) / abs(entry - sl) if abs(entry - sl) > 0 else 0

    return (
        f"{type_e} <b>{sig_type} ДЕТЕКТОР — {esc(t)}{fut_tag}</b>\n"
        f"{esc(name)} | {tf}\n\n"
        f"📊 Движение: <b>{pct:+.1f}%</b> за {s['candles']} свечи | Объём: x{vol_r:.1f}\n"
        f"💪 Сила сигнала: {strength}/100\n\n"
        f"<b>Сигнал: {dir_e}</b> (контртренд)\n"
        f"Вход: {entry:,.2f}\n"
        f"SL: {sl:,.2f}  ({sl_pct:.1f}%)\n"
        f"TP1: {tp1:,.2f}  ({abs(entry-tp1)/entry*100:.1f}%)\n"
        f"TP2: {tp2:,.2f}  ({abs(entry-tp2)/entry*100:.1f}%)  R/R {rr:.1f}\n"
        f"TP3: {tp3:,.2f}  ({abs(entry-tp3)/entry*100:.1f}%)\n\n"
        f"<i>⚠️ Контртренд — высокий риск. Используй строгий SL.</i>"
    )


async def cmd_fadd_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет все доступные фьючерсы в ватчлист."""
    wl = load_futures_watchlist()
    added, skipped = [], []
    for code in FUTURES:
        if code not in wl:
            wl.append(code)
            added.append(code)
        else:
            skipped.append(code)
    save_futures_watchlist(wl)
    await update.message.reply_text(
        f"✅ Добавлено: {len(added)}\n"
        f"ℹ️ Уже были: {len(skipped)}\n"
        f"📋 Итого в ватчлисте: {len(wl)} фьючерсов\n\n"
        f"Запустить сканер: /scan_futures",
        parse_mode="HTML")


async def cmd_pump_dump(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /pd — ручной поиск памп/дамп на всём ватчлисте
    /pd futures — только фьючерсы
    /pd stocks  — только акции
    """
    chat_id = update.effective_chat.id
    args = context.args
    mode = args[0].lower() if args else "all"
    tf   = get_user_state(chat_id)["tf"]

    msg = await update.message.reply_text(
        f"🔍 Ищу памп/дамп [{tf}]...", parse_mode="HTML")

    results = []
    if mode in ("all", "stocks"):
        wl = load_watchlist()
        results += await _scan_pump_dump(wl, tf, is_futures=False)
    if mode in ("all", "futures"):
        fwl = load_futures_watchlist()
        results += await _scan_pump_dump(fwl, tf, is_futures=True)

    if not results:
        await msg.edit_text(
            f"😶 Памп/дамп сигналов не найдено [{tf}]\n"
            f"Порог: {PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG['15m'])['threshold_pct']}% "
            f"за {PUMP_DUMP_CONFIG.get(tf, PUMP_DUMP_CONFIG['15m'])['lookback']} свечи")
        return

    # Сортируем по силе
    results.sort(key=lambda x: -x["strength"])
    lines = [f"{'🚀' if r['type']=='PUMP' else '💥'} <b>ПАМП/ДАМП [{tf}]</b>  "
             f"{datetime.now().strftime('%H:%M')}", ""]
    for r in results[:5]:
        lines.append(format_pump_dump_alert(r))
        lines.append("")

        # Кнопка быстрого входа
    kb = []
    for r in results[:3]:
        kb.append([InlineKeyboardButton(
            f"✅ Войти {r['ticker']} {r['direction']}",
            callback_data=(
                f"enter_{r['ticker']}_{r['direction']}_"
                f"{r['entry']:.4f}_{r['sl']:.4f}_"
                f"{r['tp1']:.4f}_{r['tp2']:.4f}_{r['tp3']:.4f}"
            )
        )])

    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)


# Фоновый мониторинг памп/дамп (встраивается в scanner_loop)
async def run_pump_dump_broadcast(app, tf: str = "15m"):
    """Рассылает памп/дамп алерты подписчикам."""
    wl  = load_watchlist()
    fwl = load_futures_watchlist()
    all_results = (
        await _scan_pump_dump(wl,  tf, is_futures=False) +
        await _scan_pump_dump(fwl, tf, is_futures=True)
    )
    if not all_results:
        return
    all_results.sort(key=lambda x: -x["strength"])
    for r in all_results[:3]:
        text = format_pump_dump_alert(r)
        for chat_id in SCANNER_CHAT_IDS:
            try:
                await app.bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"PD broadcast {chat_id}: {e}")

async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /calendar — ближайшие события
    /calendar update — обновить с MOEX + ЦБ
    """
    args = context.args
    if args and args[0].lower() == "update":
        msg = await update.message.reply_text("⏳ Обновляю календарь...", parse_mode="HTML")
        try:
            moex_ev = await fetch_moex_dividends()
            cbr_ev  = get_cbr_dates_2026()
            existing = [e for e in load_calendar_events() if e.get("source") == "manual"]
            save_calendar_events(existing + moex_ev + cbr_ev)
            await msg.edit_text(
                f"✅ Календарь обновлён\n"
                f"📅 Отсечки MOEX: {len(moex_ev)}\n"
                f"🏦 Заседания ЦБ: {len(cbr_ev)}\n\n"
                f"Смотреть: /calendar",
                parse_mode="HTML")
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка: {esc(str(e))}")
        return

    upcoming = get_upcoming_events(hours_ahead=72)
    all_ev   = load_calendar_events()

    if not all_ev:
        await update.message.reply_text(
            "📭 Календарь пуст.\n\n"
            "/calendar update — загрузить события с MOEX и ЦБ\n"
            "/calendar_add — добавить вручную",
            parse_mode="HTML")
        return

    lines = ["📅 <b>ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ</b>\n"]

    if upcoming:
        lines.append("<b>🔔 Ближайшие (до 72ч):</b>")
        for ev in upcoming[:8]:
            h      = ev["hours_ahead"]
            imp_e  = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(ev.get("impact", "low"), "⚪")
            t_str  = (f"{abs(h)*60:.0f}м назад" if h < 0 else
                      f"через {h*60:.0f} мин ⚡" if h < 1 else
                      f"через {h:.1f} ч" if h < 24 else f"через {h/24:.1f} дн")
            tk_str = f" [{', '.join(ev['tickers'][:3])}]" if ev.get("tickers") else ""
            lines.append(f"{imp_e} {esc(ev['name'])}{tk_str} — {t_str}")

    future = [e for e in all_ev
              if e not in upcoming and e.get("datetime_utc", "") > datetime.now(timezone.utc).isoformat()]
    if future:
        lines.append("\n<b>📋 Далее:</b>")
        for ev in future[:5]:
            try:
                dt = datetime.fromisoformat(ev["datetime_utc"])
                dt_msk = dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m %H:%M МСК")
            except Exception:
                dt_msk = ""
            imp_e = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(ev.get("impact","low"), "⚪")
            lines.append(f"{imp_e} {esc(ev['name'])} — {dt_msk}")

    lines.append("\n<i>Обновить: /calendar update | Добавить: /calendar_add</i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_calendar_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /calendar_add ГГГГ-ММ-ДД ЧЧ:ММ Название [ТИКЕР] [high/medium/low]
    Пример: /calendar_add 2026-06-10 11:00 Отчёт_SBER_МСФО SBER high
    """
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Формат:\n<code>/calendar_add ГГГГ-ММ-ДД ЧЧ:ММ Название [ТИКЕР] [high/medium/low]</code>\n\n"
            "Пример:\n<code>/calendar_add 2026-06-10 11:00 Отчёт_SBER_МСФО SBER high</code>",
            parse_mode="HTML")
        return
    try:
        name   = args[2].replace("_", " ")
        ticker = args[3].upper() if len(args) > 3 else ""
        impact = args[4].lower() if len(args) > 4 and args[4] in ("critical","high","medium","low") else "medium"
        dt_msk = datetime.strptime(f"{args[0]} {args[1]}", "%Y-%m-%d %H:%M")
        dt_utc = dt_msk.replace(tzinfo=timezone(timedelta(hours=3)))
        event  = {
            "name": name, "datetime_utc": dt_utc.isoformat(),
            "impact": impact, "tickers": [ticker] if ticker else [],
            "type": "manual", "source": "manual",
        }
        evs = load_calendar_events()
        evs.append(event)
        save_calendar_events(evs)
        imp_e = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "⚪"}.get(impact, "⚪")
        await update.message.reply_text(
            f"✅ Добавлено:\n{imp_e} <b>{esc(name)}</b>\n"
            f"📅 {args[0]} {args[1]} МСК\n"
            f"Тикер: {esc(ticker) if ticker else 'весь рынок'}  Импакт: {impact}",
            parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ {esc(str(e))}")


async def cmd_calendar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if args and args[0].lower() == "update":
        msg = await update.message.reply_text("⏳ Обновляю календарь...", parse_mode="HTML")
        try:
            moex_ev = await fetch_moex_dividends()
            cbr_ev  = get_cbr_dates_2026()
            existing = [e for e in load_calendar_events() if e.get("source") == "manual"]
            save_calendar_events(existing + moex_ev + cbr_ev)
            await msg.edit_text(
                f"✅ Календарь обновлён\n"
                f"📅 Отсечки MOEX: {len(moex_ev)}\n"
                f"🏦 Заседания ЦБ: {len(cbr_ev)}",
                parse_mode="HTML")
        except Exception as e:
            await msg.edit_text(f"❌ {esc(str(e))}")
        return
    upcoming = get_upcoming_events(hours_ahead=72)
    all_ev   = load_calendar_events()
    if not all_ev:
        await update.message.reply_text(
            "📭 Календарь пуст.\n\n"
            "/calendar update — загрузить\n"
            "/calendar_add — добавить вручную",
            parse_mode="HTML")
        return
    lines_out = ["📅 <b>ЭКОНОМИЧЕСКИЙ КАЛЕНДАРЬ</b>\n"]
    for ev in (upcoming or all_ev[:8]):
        h     = ev.get("hours_ahead", 999)
        imp_e = {"critical": "🔴", "high": "🟠", "medium": "🟡"}.get(ev.get("impact","low"), "⚪")
        t_str = (f"{abs(h)*60:.0f}м назад" if h < 0 else
                 f"через {h*60:.0f} мин" if h < 1 else
                 f"через {h:.1f} ч" if h < 24 else f"через {h/24:.1f} дн") if h != 999 else ""
        tk    = f" [{', '.join(ev['tickers'][:3])}]" if ev.get("tickers") else ""
        lines_out.append(f"{imp_e} {esc(ev['name'])}{tk} {t_str}")
    lines_out.append("\n<i>/calendar update | /calendar_add</i>")
    await update.message.reply_text("\n".join(lines_out), parse_mode="HTML")


async def cmd_calendar_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Формат:\n<code>/calendar_add ГГГГ-ММ-ДД ЧЧ:ММ Название [ТИКЕР] [high/medium/low]</code>",
            parse_mode="HTML")
        return
    try:
        name   = args[2].replace("_", " ")
        ticker = args[3].upper() if len(args) > 3 else ""
        impact = args[4].lower() if len(args) > 4 and args[4] in ("critical","high","medium","low") else "medium"
        dt_msk = datetime.strptime(f"{args[0]} {args[1]}", "%Y-%m-%d %H:%M")
        dt_utc = dt_msk.replace(tzinfo=timezone(timedelta(hours=3)))
        ev = {"name": name, "datetime_utc": dt_utc.isoformat(),
              "impact": impact, "tickers": [ticker] if ticker else [],
              "type": "manual", "source": "manual"}
        evs = load_calendar_events()
        evs.append(ev)
        save_calendar_events(evs)
        await update.message.reply_text(
            f"✅ Добавлено: <b>{esc(name)}</b>\n"
            f"{args[0]} {args[1]} МСК | {esc(ticker) or 'весь рынок'} | {impact}",
            parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ {esc(str(e))}")


async def cmd_update_figi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Обновление базы FIGI...")
    count, err = await update_figi_data()
    if err:
        await msg.edit_text(f"❌ Ошибка обновления: {err}")
    else:
        await msg.edit_text(f"✅ База FIGI обновлена: {count} инструментов.")


# ══════════════════════════════════════════════
# TRADE COMMANDS
# ══════════════════════════════════════════════
async def cmd_open_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if len(args) < 7:
        await update.message.reply_text(
            "📝 <b>Открыть сделку:</b>\n"
            "<code>/open_trade TICKER LONG/SHORT ENTRY SL TP1 TP2 TP3</code>\n\n"
            "Пример:\n<code>/open_trade SBER LONG 320.5 316.0 325.0 330.0 337.0</code>\n\n"
            "Или нажми кнопку <b>✅ Войти в сделку</b> в анализе.",
            parse_mode="HTML")
        return
    try:
        ticker = args[0].upper()
        direction = args[1].upper()
        entry = float(args[2])
        sl = float(args[3])
        tp1 = float(args[4])
        tp2 = float(args[5])
        tp3 = float(args[6])
    except (ValueError, IndexError):
        await update.message.reply_text("❌ Неверный формат. Пример: /open_trade SBER LONG 320.5 316.0 325.0 330.0 337.0")
        return
    if ticker not in MOEX_STOCKS:
        await update.message.reply_text(f"❌ Тикер {esc(ticker)} не найден.")
        return
    if direction not in ("LONG", "SHORT"):
        await update.message.reply_text("❌ Направление должно быть LONG или SHORT.")
        return

    tf = get_user_state(chat_id)["tf"]
    trade_id = open_trade(ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf)
    direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"
    risk_pct = abs(entry - sl) / entry * 100

    await update.message.reply_text(
        f"✅ <b>Сделка открыта!</b>\n\n"
        f"<b>{esc(ticker)}</b> {direction_e}\n"
        f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
        f"SL: {sl:,.2f} ₽\n"
        f"TP1: {tp1:,.2f} ₽\n"
        f"TP2: {tp2:,.2f} ₽\n"
        f"TP3: {tp3:,.2f} ₽\n\n"
        f"Мониторинг активен. Алерты при достижении уровней.\n/trades — все позиции",
        parse_mode="HTML"
    )


async def cmd_close_trade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    args = context.args
    if not args:
        await update.message.reply_text(
            "Использование:\n/close_trade SBER — закрыть по тикеру\n/close_trade ALL — закрыть все")
        return

    trades = load_trades()
    target = args[0].upper()

    if target == "ALL":
        open_ids = [k for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")]
        if not open_ids:
            await update.message.reply_text("📭 Нет открытых сделок.")
            return
        for tid in open_ids:
            figi = MOEX_STOCKS.get(trades[tid]["ticker"], ("",))[0]
            price = await fetch_last_price_tinkoff(figi) if figi else trades[tid]["entry"]
            price = price or trades[tid]["entry"]
            close_trade(tid, "manual", price)
        await update.message.reply_text(f"✅ Закрыто сделок: {len(open_ids)}")
        return

    found = [(k, v) for k, v in trades.items()
             if v["ticker"] == target and v["status"] in ("open", "tp1_hit", "tp2_hit")]
    if not found:
        await update.message.reply_text(f"ℹ️ Нет открытых сделок по {esc(target)}.")
        return

    trade_id, t = found[0]
    figi = MOEX_STOCKS.get(target, ("",))[0]
    price = await fetch_last_price_tinkoff(figi) if figi else t["entry"]
    price = price or t["entry"]
    closed = close_trade(trade_id, "manual", price)
    pnl = closed.get("pnl_pct", 0) if closed else 0
    pnl_e = "📈" if pnl >= 0 else "📉"

    await update.message.reply_text(
        f"🔒 <b>Сделка {esc(target)} закрыта вручную</b>\n"
        f"Цена: {price:,.2f} ₽\n{pnl_e} Результат: {pnl:+.2f}%",
        parse_mode="HTML")


async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает открытые позиции с реальным нереализованным PnL."""
    trades = load_trades()
    open_t   = {k: v for k, v in trades.items() if v["status"] in ("open", "tp1_hit", "tp2_hit")}
    closed_t = sorted(
        [v for v in trades.values() if v["status"] == "closed"],
        key=lambda x: x.get("closed_at", ""), reverse=True
    )[:5]

    if not open_t and not closed_t:
        await update.message.reply_text(
            "📭 Нет сделок.\n\nОткрыть: /open_trade TICKER LONG/SHORT ENTRY SL TP1 TP2 TP3")
        return

    msg = await update.message.reply_text("⏳ Загружаю текущие цены...", parse_mode="HTML")

    lines = []
    total_pnl = 0.0

    if open_t:
        lines.append(f"📂 <b>Открытые позиции ({len(open_t)}):</b>")
        for k, t in open_t.items():
            ticker = t["ticker"]
            entry  = t["entry"]
            direction = t["direction"]
            sl = t.get("sl", 0); tp1 = t.get("tp1", 0)
            tp2 = t.get("tp2", 0); tp3 = t.get("tp3", 0)

            # Получаем текущую цену
            cur_price = None
            src = MOEX_STOCKS.get(ticker) or FUTURES.get(ticker)
            if src:
                figi = src[0]
                cur_price = await fetch_last_price_tinkoff(figi)

            if cur_price:
                if direction == "LONG":
                    upnl_pct = (cur_price - entry) / entry * 100
                else:
                    upnl_pct = (entry - cur_price) / entry * 100
                total_pnl += upnl_pct
                pnl_e = "📈" if upnl_pct >= 0 else "📉"

                # Прогресс до TP
                dist_to_tp1 = abs(cur_price - tp1) / abs(entry - tp1) * 100 if tp1 and entry != tp1 else 0
                progress = f" → TP1 {100-dist_to_tp1:.0f}%" if dist_to_tp1 < 100 else " ✅TP1"

                status_e = {"open": "🔵", "tp1_hit": "🟡", "tp2_hit": "🟠"}.get(t["status"], "⚪")
                sl_note = " (б/у)" if t.get("sl_moved_to_be") else (" (на TP1)" if t.get("sl_moved_to_tp1") else "")

                lines += [
                    f"{status_e} <b>{esc(ticker)}</b> {'🟩 LONG' if direction=='LONG' else '🟥 SHORT'}",
                    f"  Вход: {entry:,.2f} → Сейчас: <b>{cur_price:,.2f}</b>  {pnl_e} <b>{upnl_pct:+.2f}%</b>{progress}",
                    f"  SL: {sl:,.2f}{sl_note}  TP1: {tp1:,.2f}  TP2: {tp2:,.2f}  TP3: {tp3:,.2f}",
                    f"  <code>/close_trade {ticker}</code>",
                    "",
                ]
            else:
                lines += [
                    format_trade_status(t),
                    f"  <code>/close_trade {ticker}</code>",
                    "",
                ]

        if len(open_t) > 1:
            total_e = "📈" if total_pnl >= 0 else "📉"
            lines.append(f"<b>Итого нереализованный PnL: {total_e} {total_pnl:+.2f}%</b>")
            lines.append("")

    if closed_t:
        lines.append("📋 <b>Последние закрытые:</b>")
        for t in closed_t:
            pnl = t.get("pnl_pct", 0)
            pnl_e = "📈" if pnl >= 0 else "📉"
            reason_map = {"SL": "🛑 SL", "TP3": "🎯 TP3", "TP2": "🎯 TP2",
                          "TP1": "🎯 TP1", "manual": "🔒 ручное", "EOD": "🕐 конец сессии"}
            reason = reason_map.get(t.get("close_reason", ""), t.get("close_reason", ""))
            lines.append(
                f"{pnl_e} <b>{esc(t['ticker'])}</b> {t['direction']}  "
                f"{reason}  {pnl:+.2f}%  "
                f"<i>{t.get('closed_at', '')[:16].replace('T', ' ')}</i>"
            )

    await msg.edit_text("\n".join(lines), parse_mode="HTML")


# ══════════════════════════════════════════════
# SCANNER
# ══════════════════════════════════════════════
def _format_scan_row(s: dict) -> str:
    """Форматирует строку сканера с SL/TP и кнопкой входа."""
    ticker = s["ticker"]
    name   = s["name"]
    price  = s["price"]
    score  = s["tech_score"]
    sector = s["sector"]
    regime = s["regime"]["label"]
    reason = s["tech_reasons"][0] if s["tech_reasons"] else "—"
    sig    = s["tech_signal"]
    na     = s["news_ai"]
    fs     = na.get("filter_status", "NO_SIGNAL")
    ew     = na.get("event_weight", 0)
    fs_e   = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀",
              "BLOCKED": "🚫", "NEWS_ONLY": "📢"}.get(fs, "⚪")
    ew_s   = f"+{ew}" if ew > 0 else str(ew)

    # SL/TP из результата анализа
    sl_tp  = s.get("sl_tp", {})
    sl     = sl_tp.get("sl", 0)
    tp1    = sl_tp.get("tp1", 0)
    tp2    = sl_tp.get("tp2", 0)
    tp3    = sl_tp.get("tp3", 0)
    rr     = sl_tp.get("rr_ratio", 0)
    risk   = sl_tp.get("risk_pct", 0)

    lines = [
        f"<b>{esc(ticker)}</b> — {esc(name)} <i>({esc(sector)})</i>",
        f" 💰 {price:,.2f} ₽ | Скор: {score}/100 | {esc(regime)}",
        f" ⚙️ {esc(reason)}",
        f" {fs_e} Новость: {fs} [{ew_s}]",
    ]

    if sl_tp and sl and tp1:
        dir_e = "🟩" if "LONG" in sig else "🟥"
        lines.append(
            f" {dir_e} SL: {sl:,.2f} ({risk:.1f}%) | "
            f"TP1: {tp1:,.2f} | TP2: {tp2:,.2f} | TP3: {tp3:,.2f} | R/R {rr:.1f}"
        )
    else:
        lines.append(" ⚠️ Уровни недоступны")

    return "\n".join(lines)


# Семафор — не более 5 параллельных запросов к Tinkoff API
_API_SEMAPHORE = asyncio.Semaphore(5)


async def _analyze_with_semaphore(ticker: str, tf: str, mode_cfg: dict) -> dict | None:
    """Анализ тикера с защитой от rate limit (HTTP 429)."""
    async with _API_SEMAPHORE:
        for attempt in range(3):
            try:
                result = await analyze_stock(ticker, tf, mode_cfg)
                return result
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "Too Many Requests" in err_str:
                    wait = 2 ** attempt  # 1s, 2s, 4s
                    logger.warning(f"Rate limit {ticker}, retry in {wait}s")
                    await asyncio.sleep(wait)
                else:
                    logger.debug(f"analyze {ticker}: {e}")
                    return None
        return None


async def _run_scan(tickers: list[str], tf: str, mode_cfg: dict,
                    progress_cb=None) -> tuple[list, list, list]:
    long_sigs, short_sigs, watch_sigs = [], [], []
    total = len(tickers)

    # Запускаем всё параллельно — семафор сам ограничит до 5 одновременных
    tasks = [_analyze_with_semaphore(t, tf, mode_cfg) for t in tickers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, r in enumerate(results):
        if isinstance(r, Exception) or not r or "error" in r:
            continue
        sig = r["tech_signal"]
        if "Время входа вышло" in r.get("final_signal", ""):
            continue
        if sig == "🟩 LONG":
            long_sigs.append(r)
        elif sig == "🟥 SHORT/ВЫХОД":
            short_sigs.append(r)
        elif r["tech_score"] >= 52:
            watch_sigs.append(r)
        # Обновляем прогресс каждые 10 тикеров
        if progress_cb and i > 0 and i % 10 == 0:
            await progress_cb(i, total)

    long_sigs.sort(key=lambda x: -x["tech_score"])
    short_sigs.sort(key=lambda x: -x["tech_score"])
    return long_sigs, short_sigs, watch_sigs


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf = get_user_state(chat_id)["tf"]
    wl = load_watchlist()
    if not wl:
        await update.message.reply_text("Ватчлист пуст. Добавьте тикеры через /add или /add_all")
        return

    msg = await update.message.reply_text(f"🔍 Сканирую рынок [{tf}]... Найдено: {len(wl)} активов.", parse_mode="HTML")

    async def progress(done, total):
        try:
            await msg.edit_text(f"🔍 Сканирование рынка... {done}/{total} инструментов [{tf}]", parse_mode="HTML")
        except Exception:
            pass

    long_sigs, short_sigs, watch_sigs = await _run_scan(wl, tf, mode_cfg, progress)
    ts = datetime.now().strftime("%d.%m.%Y %H:%M")

    if not long_sigs and not short_sigs:
        await msg.edit_text(f"😶 <b>Интрадей-сигналов на таймфрейме {tf} не обнаружено.</b>\n<i>{ts}</i>", parse_mode="HTML")
        return

    lines = [
        f"🔍 <b>СКАНЕР РЫНКА MOEX</b> | {esc(mode_cfg['label'])} | {tf}",
        f"<i>Интрадей-сессия | {ts}</i>",
        "",
    ]
    if long_sigs:
        lines.append("🟩 <b>СИГНАЛЫ НА ПОКУПКУ:</b>")
        for s in long_sigs[:5]:
            lines.append(_format_scan_row(s))
            lines.append("")
    if short_sigs:
        lines.append("🟥 <b>СИГНАЛЫ НА ВЫХОД:</b>")
        for s in short_sigs[:5]:
            lines.append(_format_scan_row(s))
            lines.append("")

    # Кнопки: вход прямо из сканера
    top = (long_sigs + short_sigs)[:3]
    kb  = []
    for s in top:
        sl_tp     = s.get("sl_tp", {})
        direction = "LONG" if "LONG" in s["tech_signal"] else "SHORT"
        row = [InlineKeyboardButton(
            f"📊 {s['ticker']}", callback_data=f"analyze_{s['ticker']}_{tf}")]
        if sl_tp and sl_tp.get("sl"):
            p  = s["price"]; sl = sl_tp.get("sl",0)
            t1 = sl_tp.get("tp1",0); t2 = sl_tp.get("tp2",0); t3 = sl_tp.get("tp3",0)
            row.append(InlineKeyboardButton(
                f"✅ {direction}",
                callback_data=f"enter_{s['ticker']}_{direction}_{p:.2f}_{sl:.2f}_{t1:.2f}_{t2:.2f}_{t3:.2f}"
            ))
        kb.append(row)

    await msg.edit_text("\n".join(lines), parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(kb) if kb else None)


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_mode = get_user_state(chat_id)["mode"]
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if m == cur_mode else ''}{TRADE_MODES[m]['label']}",
            callback_data=f"mode_{m}"
        ) for m in TRADE_MODES
    ]]
    await update.message.reply_text(
        f"⚙️ Режим чувствительности: <b>{TRADE_MODES[cur_mode]['label']}</b>\n\n"
        "LOW — жесткие условия фильтрации\n"
        "MID — сбалансированные параметры (рекомендуется)\n"
        "HARD — импульсный вход для интрадея",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb),
    )


async def cmd_tf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_tf = get_user_state(chat_id)["tf"]
    tfs = ["5m", "15m", "1h", "4h", "1d"]
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if t == cur_tf else ''}{t}",
            callback_data=f"tf_{t}"
        ) for t in tfs
    ]]
    await update.message.reply_text(
        f"⏱ Рабочий таймфрейм: <b>{cur_tf}</b>\n"
        "Для работы без овернайтов используйте 15m или 1h.",
        parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb),
    )


# ══════════════════════════════════════════════
# CALLBACK HANDLER
# ══════════════════════════════════════════════
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("mode_"):
        mode = data.split("_")[1]
        set_user_state(chat_id, mode=mode)
        await query.edit_message_text(f"✅ Режим изменен на: <b>{TRADE_MODES[mode]['label']}</b>", parse_mode="HTML")

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        set_user_state(chat_id, tf=tf)
        await query.edit_message_text(f"✅ Таймфрейм: <b>{tf}</b>", parse_mode="HTML")

    elif data.startswith("news_"):
        ticker = data.split("_")[1]
        await query.edit_message_text(f"⏳ Загружаю новости для {ticker}...", parse_mode="HTML")
        sector = MOEX_STOCKS.get(ticker, ("", "", ""))[2]
        news = await fetch_russian_news(ticker, sector)
        lines = [f"📰 <b>События — {ticker}</b>\n"]
        for it in news[:5]:
            lines.append(f"⚪ {it['title'][:120]}\n")
        await query.edit_message_text("\n".join(lines), parse_mode="HTML")

    elif data.startswith("analyze_"):
        parts = data.split("_")
        ticker = parts[1]
        tf = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(f"⏳ Перерасчет {ticker} [{tf}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_stock(ticker, tf, mode_cfg)
        await query.edit_message_text(format_analysis(result), parse_mode="HTML")

    elif data.startswith("wl_add_"):
        ticker = data.split("_", 2)[2]
        ok, msg = add_to_watchlist(ticker)
        await query.answer(msg.replace("<b>", "").replace("</b>", "")[:200], show_alert=True)

    elif data.startswith("fut_"):
        parts = data.split("_")
        code = parts[1]
        tf_  = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(
            f"⏳ Анализирую фьючерс {esc(code)} [{tf_}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_futures(code, tf_, mode_cfg)
        await query.edit_message_text(format_analysis(result), parse_mode="HTML")

    elif data.startswith("enter_"):
        parts = data.split("_")
        try:
            ticker = parts[1]
            direction = parts[2]
            entry = float(parts[3])
            sl = float(parts[4])
            tp1 = float(parts[5])
            tp2 = float(parts[6])
            tp3 = float(parts[7])
        except (IndexError, ValueError):
            await query.answer("Ошибка данных", show_alert=True)
            return
        tf = get_user_state(chat_id)["tf"]
        trade_id = open_trade(ticker, direction, entry, sl, tp1, tp2, tp3, chat_id, tf)
        risk_pct = abs(entry - sl) / entry * 100 if entry else 0
        direction_e = "🟩 LONG" if direction == "LONG" else "🟥 SHORT"
        await query.answer("✅ Сделка открыта!", show_alert=True)
        await query.message.reply_text(
            f"✅ <b>Сделка открыта!</b>\n\n"
            f"<b>{esc(ticker)}</b> {direction_e}\n"
            f"Вход: {entry:,.2f} ₽  |  Риск: {risk_pct:.1f}%\n"
            f"SL: {sl:,.2f} ₽\n"
            f"TP1: {tp1:,.2f} ₽ → SL в безубыток\n"
            f"TP2: {tp2:,.2f} ₽ → SL на TP1\n"
            f"TP3: {tp3:,.2f} ₽ → Закрытие\n\n"
            f"Алерты придут автоматически.\n/trades — все позиции",
            parse_mode="HTML"
        )


# ══════════════════════════════════════════════
# SCANNER LOOP
# ══════════════════════════════════════════════
SCANNER_CHAT_IDS: list[int] = []


async def cmd_scan_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.append(chat_id)
        await update.message.reply_text(
            "✅ <b>Интрадей авто-сканер активирован.</b>\n\n"
            "Бот будет сканировать рынок каждые 30 минут во время торговых сессий:\n"
            "• Дневная сессия: 10:00 - 18:50 МСК\n"
            "• Вечерняя сессия: 19:00 - 23:50 МСК\n\n"
            "<i>🔔 Перед закрытием сессии в 23:35 придет уведомление о закрытии сделок.</i>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("ℹ️ Авто-сканер уже запущен.")


async def cmd_scan_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.remove(chat_id)
        await update.message.reply_text("🔕 Авто-сканер отключен.")
    else:
        await update.message.reply_text("ℹ️ Авто-сканер не был запущен.")


_last_broadcast_signals: set = set()


async def run_scanner_broadcast(app):
    global _last_broadcast_signals
    tf = "15m"
    mode_cfg = TRADE_MODES["mid"]
    wl = load_watchlist()
    if not wl:
        return
    long_sigs, short_sigs, _ = await _run_scan(wl, tf, mode_cfg)
    all_sigs = long_sigs + short_sigs
    if not all_sigs:
        return
    new_sigs = [s for s in all_sigs if f"{s['ticker']}_{s['tech_signal']}" not in _last_broadcast_signals]
    if not new_sigs:
        return
    for s in new_sigs:
        _last_broadcast_signals.add(f"{s['ticker']}_{s['tech_signal']}")
    ts = datetime.now().strftime("%H:%M")
    lines = [
        f"🔔 <b>НОВЫЕ ИНТРАДЕЙ СИГНАЛЫ [{tf}] | {ts} МСК</b>",
        "<i>Сделки закройте до 23:50 МСК.</i>",
        "",
    ]
    for s in new_sigs[:5]:
        lines.append(_format_scan_row(s))
        lines.append("")
    text = "\n".join(lines)
    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Broadcast failed for {chat_id}: {e}")


async def scanner_loop(app):
    global _last_broadcast_signals
    while True:
        try:
            day, hour, minute = get_msk_time()
            is_trading_day = day < 5
            is_main_session = (10 <= hour < 18) or (hour == 18 and minute < 50)
            is_evening_session = (19 <= hour < 23) or (hour == 23 and minute < 50)
            is_trading_time = is_trading_day and (is_main_session or is_evening_session)

            if is_trading_day and hour == 23 and 34 <= minute <= 36:
                for chat_id in SCANNER_CHAT_IDS:
                    try:
                        await app.bot.send_message(
                            chat_id,
                            "🚨 <b>ВНИМАНИЕ! До закрытия вечерней сессии осталось 15 минут!</b>\n"
                            "Убедитесь, что все открытые интрадей-сделки закрыты. Не оставляйте позиции на овернайт.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Failed warning to {chat_id}: {e}")
                await asyncio.sleep(120)

            if is_trading_time and SCANNER_CHAT_IDS:
                await run_scanner_broadcast(app)
                # Памп/дамп проверяем при каждом цикле
                await run_pump_dump_broadcast(app, tf="15m")
            else:
                if not is_trading_time:
                    _last_broadcast_signals = set()
                    _pd_sent.clear()

        except Exception as e:
            logger.error(f"Error in scanner loop: {e}")

        await asyncio.sleep(1800)


# ══════════════════════════════════════════════
# TEXT HANDLER
# ══════════════════════════════════════════════
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip().upper()
    chat_id = update.effective_chat.id
    if text in MOEX_STOCKS:
        msg = await update.message.reply_text(f"⏳ Анализирую {text}...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        tf = get_user_state(chat_id)["tf"]
        result = await analyze_stock(text, tf, mode_cfg)
        await msg.edit_text(format_analysis(result), parse_mode="HTML")
    else:
        matches = [t for t in MOEX_STOCKS if t.startswith(text[:3])]
        if matches:
            await update.message.reply_text(
                f"Тикер <code>{text}</code> не найден.\n"
                f"Возможно, вы имели в виду: {', '.join(f'<code>{m}</code>' for m in matches[:5])}",
                parse_mode="HTML")


# ══════════════════════════════════════════════
# RAILWAY HEALTHCHECK
# ══════════════════════════════════════════════
async def health_check(request):
    return web.Response(text="OK", status=200)


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Healthcheck server started on port {PORT}")


# ══════════════════════════════════════════════
# INIT & MAIN
# ══════════════════════════════════════════════
async def _calendar_loop():
    """Обновляет экономический календарь каждые 6 часов."""
    while True:
        count = await auto_update_calendar()
        logger.info(f"Calendar refresh: {count} events")
        await asyncio.sleep(21600)  # 6 часов


async def post_init(app):
    # Подгружаем FIGI из файла при старте
    loaded = _load_figi_from_file()
    if loaded:
        logger.info(f"Startup: loaded {loaded} FIGIs from file")
    else:
        logger.warning("Startup: figi_result.json not found. Run /update_figi")

    # Сразу обновляем календарь при старте
    asyncio.create_task(auto_update_calendar())
    # Получаем актуальные FIGI фьючерсов
    asyncio.create_task(fetch_nearest_futures())

    asyncio.create_task(scanner_loop(app))
    asyncio.create_task(monitor_trades_loop(app, fetch_last_price_tinkoff))
    asyncio.create_task(_calendar_loop())
    asyncio.create_task(start_web_server())
    logger.info("🚀 MOEX Railway Bot fully initialized")


def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN отсутствует в настройках среды.")

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("analyze", cmd_analyze))
    application.add_handler(CommandHandler("scan", cmd_scan))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("market", cmd_market))
    application.add_handler(CommandHandler("mode", cmd_mode))
    application.add_handler(CommandHandler("tf", cmd_tf))
    application.add_handler(CommandHandler("trades", cmd_trades))
    application.add_handler(CommandHandler("open_trade", cmd_open_trade))
    application.add_handler(CommandHandler("close_trade", cmd_close_trade))
    application.add_handler(CommandHandler("watchlist", cmd_watchlist))
    application.add_handler(CommandHandler("all_tickers", cmd_all_tickers))
    application.add_handler(CommandHandler("add", cmd_add))
    application.add_handler(CommandHandler("remove", cmd_remove))
    application.add_handler(CommandHandler("clear_watchlist", cmd_clear_watchlist))
    application.add_handler(CommandHandler("add_sector", cmd_add_sector))
    application.add_handler(CommandHandler("add_all", cmd_add_all))
    application.add_handler(CommandHandler("scan_start", cmd_scan_start))
    application.add_handler(CommandHandler("scan_stop", cmd_scan_stop))
    application.add_handler(CommandHandler("update_figi",    cmd_update_figi))
    application.add_handler(CommandHandler("calendar",        cmd_calendar))
    application.add_handler(CommandHandler("calendar_add",    cmd_calendar_add))
    application.add_handler(CommandHandler("futures",        cmd_futures))
    application.add_handler(CommandHandler("futures_list",   cmd_futures_list))
    application.add_handler(CommandHandler("scan_futures",   cmd_scan_futures))
    application.add_handler(CommandHandler("fadd",           cmd_fadd))
    application.add_handler(CommandHandler("fadd_all",       cmd_fadd_all))
    application.add_handler(CommandHandler("fremove",        cmd_fremove))
    application.add_handler(CommandHandler("pd",             cmd_pump_dump))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot starting in polling mode...")
    application.run_polling(
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()