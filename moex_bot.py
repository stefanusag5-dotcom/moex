"""
MOEX Signal Bot — Telegram-бот для российского фондового рынка (1-2 эшелон)
Источники данных: Tinkoff Invest API (только чтение), RSS-новости
ИИ-анализ: Groq (llama) — оценка влияния новостей + классификация сигнала
"""

import logging
import asyncio
import aiohttp
import json
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import numpy as np
import pandas_ta as ta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, filters, ContextTypes)
from dotenv import load_dotenv
from groq import Groq

# На Railway переменные приходят из Environment Variables (dashboard).
load_dotenv()

# ══════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY     = os.getenv("GROQ_API_KEY")
TINKOFF_TOKEN    = os.getenv("TINKOFF_TOKEN")   # read-only sandbox/prod токен

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

GROQ_MODELS = [
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
]

def esc(text: str) -> str:
    """Экранирует спецсимволы HTML для Telegram parse_mode=HTML."""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════
# ИНСТРУМЕНТЫ — 1 и 2 эшелон MOEX (акции)
# ══════════════════════════════════════════════
MOEX_STOCKS = {
    # Нефтегаз
    "GAZP":  ("BBG004730RP0", "Газпром",              "нефтегаз"),
    "LKOH":  ("BBG004731032", "Лукойл",               "нефтегаз"),
    "ROSN":  ("BBG004731354", "Роснефть",             "нефтегаз"),
    "NVTK":  ("BBG00475KKY8", "Новатэк",              "нефтегаз"),
    "TATN":  ("BBG004731642", "Татнефть",             "нефтегаз"),
    "TATNP": ("BBG004731706", "Татнефть п.",           "нефтегаз"),
    "SNGS":  ("BBG004730JJ5", "Сургутнефтегаз",       "нефтегаз"),
    "SNGSP": ("BBG0047315Y7", "Сургутнефтегаз п.",    "нефтегаз"),
    "TRNFP": ("BBG00475KHX6", "Транснефть п.",         "нефтегаз"),
    "BANE":  ("BBG004S68758", "Башнефть",             "нефтегаз"),
    "BANEP": ("BBG004S687B4", "Башнефть п.",           "нефтегаз"),
    # Банки и финансы
    "SBER":  ("BBG004730N88", "Сбербанк",             "банки"),
    "SBERP": ("BBG004730N96", "Сбербанк п.",           "банки"),
    "VTBR":  ("BBG004730ZJ9", "ВТБ",                  "банки"),
    "BSPB":  ("BBG0029SNB14", "БСП",                  "банки"),
    "CBOM":  ("BBG009GSYN76", "МКБ",                  "банки"),
    "MOEX":  ("BBG004730JJ5", "МосБиржа",             "финансы"),
    "TCSG":  ("BBG00QPYJ5H0", "Т-Банк (TCS)",         "банки"),
    "SVCB":  ("BBG00F9XX7H4", "Совкомбанк",           "банки"),
    # Металлы и горнодобыча
    "GMKN":  ("BBG004731489", "Норникель",            "металлы"),
    "CHMF":  ("BBG00475JZZ6", "Северсталь",           "металлы"),
    "NLMK":  ("BBG004S68BH6", "НЛМК",                "металлы"),
    "MAGN":  ("BBG004S68507", "ММК",                  "металлы"),
    "RUAL":  ("BBG008F2T3T2", "РусАл",                "металлы"),
    "ENPG":  ("BBG00F6NK0J8", "Эн+ Груп",             "металлы"),
    "ALRS":  ("BBG004S68B31", "Алроса",               "горнодобыча"),
    "POLY":  ("BBG004PYF2N3", "Полюс",                "золото"),
    "PLZL":  ("BBG000R607Y3", "Полюс Золото",         "золото"),
    "RASP":  ("BBG00475M5R0", "Распадская",           "уголь"),
    "MTLR":  ("BBG004S68598", "Мечел",                "уголь"),
    "MTLRP": ("BBG004S68716", "Мечел п.",              "уголь"),
    # IT и телеком
    "YNDX":  ("BBG006L8G4H1", "Яндекс",              "IT"),
    "VKCO":  ("BBG00178PGX3", "ВКонтакте",            "IT"),
    "POSI":  ("BBG01FD18M82", "Позитив",              "кибербезопасность"),
    "ASTR":  ("BBG016S3QJ60", "Астра",                "IT"),
    "HEAD":  ("BBG00DHTYPH4", "HeadHunter",           "IT"),
    "CIAN":  ("BBG009S39JX6", "ЦИАН",                 "IT"),
    "MTSS":  ("BBG004S68473", "МТС",                  "телеком"),
    "RTKM":  ("BBG004S681B4", "Ростелеком",           "телеком"),
    "RTKMP": ("BBG004S68696", "Ростелеком п.",         "телеком"),
    # Ритейл и потребительский
    "MGNT":  ("BBG004RVFCY3", "Магнит",               "ритейл"),
    "FIVE":  ("BBG00JXPFBN0", "X5 Group",             "ритейл"),
    "OZON":  ("BBG00Y91R9T3", "Ozon",                 "e-commerce"),
    "LENT":  ("BBG00264RNXT", "Лента",                "ритейл"),
    "MDMG":  ("BBG001M2SC01", "MD Medical (Мать и дитя)", "медицина"),
    "FIXP":  ("BBG00ZHCX1X2", "Fix Price",            "ритейл"),
    # Энергетика
    "FEES":  ("BBG00475K6C3", "ФСК ЕЭС",             "энергетика"),
    "HYDR":  ("BBG00475K2X9", "РусГидро",             "энергетика"),
    "IRAO":  ("BBG004S68829", "Интер РАО",            "энергетика"),
    "OGKB":  ("BBG004S686G4", "ОГК-2",               "энергетика"),
    "MSNG":  ("BBG004S686W0", "Мосэнерго",            "энергетика"),
    "TGKA":  ("BBG004S68C23", "ТГК-1",               "энергетика"),
    # Транспорт и инфраструктура
    "AFLT":  ("BBG004S683W7", "Аэрофлот",             "транспорт"),
    "FLOT":  ("BBG000R04X57", "Совкомфлот",           "транспорт"),
    "GLTR":  ("BBG000VFX6Y4", "Globaltrans",          "транспорт"),
    # Девелопмент
    "PIKK":  ("BBG004S68BF0", "ПИК",                  "девелопмент"),
    "SMLT":  ("BBG005D1WCQ1", "Самолёт",              "девелопмент"),
    "LSRG":  ("BBG0040F7B78", "ЛСР",                  "девелопмент"),
    "ETLN":  ("BBG00475JZY3", "Эталон",               "девелопмент"),
    # Удобрения и химия
    "PHOR":  ("BBG004S689R0", "ФосАгро",              "химия"),
    "KAZT":  ("BBG004S68614", "КуйбышевАзот",         "химия"),
    "NKNC":  ("BBG004S681N6", "Нижнекамскнефтехим",   "химия"),
    # Прочее — 2 эшелон
    "SGZH":  ("BBG0100R9963", "Сегежа",               "лесопромышленность"),
    "UWGN":  ("BBG008HD3V85", "ОВК",                  "машиностроение"),
    "GCHE":  ("BBG000RP9B63", "Черкизово",            "АПК"),
    "AGRO":  ("BBG005Y6BNR6", "РусАгро",              "АПК"),
    "SFIN":  ("BBG00HGDC7N7", "ЭсЭфАй",              "финансы"),
    "MFGS":  ("BBG004S68BR5", "Мегафон",              "телеком"),
}

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
        "label": "🟢 LOW",
        "rsi_oversold": 30, "rsi_overbought": 72,
        "min_score": 70, "min_news_score": 7,
        "personality": "Ты консервативный инвестор. Торгуй только очень чёткие сигналы на российском рынке акций.",
    },
    "mid": {
        "label": "🟡 MID",
        "rsi_oversold": 38, "rsi_overbought": 67,
        "min_score": 58, "min_news_score": 6,  # Оптимизировали под новую скоринг-систему
        "personality": "Ты сбалансированный трейдер на MOEX. Ищи сигналы в 1-2 эшелоне акций.",
    },
    "hard": {
        "label": "🔴 HARD",
        "rsi_oversold": 45, "rsi_overbought": 58,
        "min_score": 48, "min_news_score": 5,
        "personality": "Ты агрессивный трейдер на MOEX. Давай конкретный вход без лишних оговорок.",
    },
}

_cache: dict = {}
_news_cache: dict = {}

TRADES_FILE    = Path("open_trades.json")
SCANNER_FILE   = Path("scanner_state.json")
WATCHLIST_FILE = Path("watchlist.json")

def load_trades() -> dict:
    try:
        return json.loads(TRADES_FILE.read_text()) if TRADES_FILE.exists() else {}
    except:
        return {}

def save_trades(d: dict):
    TRADES_FILE.write_text(json.dumps(d, indent=2, ensure_ascii=False))

def load_scanner_state() -> dict:
    try:
        return json.loads(SCANNER_FILE.read_text()) if SCANNER_FILE.exists() else {}
    except:
        return {}

def save_scanner_state(s: dict):
    SCANNER_FILE.write_text(json.dumps(s, ensure_ascii=False))

# ══════════════════════════════════════════════
# ЛИЧНЫЙ ВАТЧЛИСТ
# ══════════════════════════════════════════════
def load_watchlist() -> list[str]:
    try:
        if WATCHLIST_FILE.exists():
            data = json.loads(WATCHLIST_FILE.read_text())
            return [t.upper() for t in data if t.upper() in MOEX_STOCKS]
    except:
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
# ВРЕМЕННЫЕ ФИЛЬТРЫ ДЛЯ ИНТРАДЕЯ MOEX
# ══════════════════════════════════════════════
def get_msk_time() -> tuple[int, int, int]:
    """Возвращает день недели (0-6), час и минуты по Московскому времени."""
    now_utc = datetime.now(timezone.utc)
    now_msk = now_utc.astimezone(timezone(timedelta(hours=3)))
    return now_msk.weekday(), now_msk.hour, now_msk.minute

def is_acceptable_entry_time(tf: str) -> tuple[bool, str]:
    """Проверяет, достаточно ли времени до закрытия сессии (23:50 МСК) для открытия сделки."""
    if tf not in INTRADAY_TFS:
        return True, ""
        
    _, hour, minute = get_msk_time()
    current_minutes = hour * 60 + minute
    close_minutes = 23 * 60 + 50
    remaining_minutes = close_minutes - current_minutes

    if remaining_minutes <= 0:
        return False, "Сессия уже закрыта."

    if tf == "1h" and remaining_minutes < 180:
        return False, f"⚠️ До закрытия сессии менее 3 часов ({remaining_minutes} мин). Сделка на 1h не успеет отыграть."
    elif tf == "15m" and remaining_minutes < 90:
        return False, f"⚠️ До закрытия сессии менее 1.5 часов ({remaining_minutes} мин). Сделка на 15m не успеет отыграть."
    elif tf == "5m" and remaining_minutes < 45:
        return False, f"⚠️ До закрытия сессии менее 45 минут. Риск не успеть закрыть позицию."

    return True, ""

# ══════════════════════════════════════════════
# TINKOFF INVEST API
# ══════════════════════════════════════════════
TINKOFF_API = "https://invest-public-api.tinkoff.ru/rest"

def _tinkoff_headers() -> dict:
    return {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type":  "application/json",
    }

async def fetch_candles_tinkoff(figi: str, interval: str, limit: int) -> pd.DataFrame | None:
    cache_key = f"candles_{figi}_{interval}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 120:  # Сократили кэш до 2 минут для интрадея
        return _cache[cache_key]["df"]

    interval_minutes = {
        "CANDLE_INTERVAL_1_MIN":   1,
        "CANDLE_INTERVAL_5_MIN":   5,
        "CANDLE_INTERVAL_15_MIN":  15,
        "CANDLE_INTERVAL_HOUR":    60,
        "CANDLE_INTERVAL_4_HOUR":  240,
        "CANDLE_INTERVAL_DAY":     1440,
        "CANDLE_INTERVAL_WEEK":    10080,
    }.get(interval, 1440)

    trading_minutes_per_day = 830  # С учетом вечерней сессии (10:00 - 23:50 МСК)
    trading_days_needed = max(2, (limit * interval_minutes) // trading_minutes_per_day + 2)
    delta_days = int(trading_days_needed * 1.5) + 3
    delta_days = max(delta_days, 5)

    _utcnow = datetime.now(timezone.utc).replace(tzinfo=None)
    dt_from = _utcnow - timedelta(days=delta_days)
    dt_to   = _utcnow

    body = {
        "figi":     figi,
        "from":     dt_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "to":       dt_to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": interval,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles",
                headers=_tinkoff_headers(),
                json=body,
            ) as r:
                if r.status != 200:
                    logger.warning(f"Tinkoff candles {figi}: HTTP {r.status}")
                    return None
                data = await r.json()
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
                "open":   units_nano(c["open"]),
                "high":   units_nano(c["high"]),
                "low":    units_nano(c["low"]),
                "close":  units_nano(c["close"]),
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
                headers=_tinkoff_headers(),
                json={"figi": [figi]},
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
# РЕЖИМ РЫНКА IMOEX (ПРОКСИ СБЕРБАНК)
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
        s     = pd.Series(close)

        ema20 = float(s.ewm(span=20).mean().iloc[-1])
        ema50 = float(s.ewm(span=50).mean().iloc[-1])

        ema50_arr = s.ewm(span=50).mean().values
        slope_10d = (ema50_arr[-1] - ema50_arr[-10]) / ema50_arr[-10] * 100
        slope_20d = (ema50_arr[-1] - ema50_arr[-20]) / ema50_arr[-20] * 100

        is_bull = price > ema20 > ema50 and slope_10d > 0.05
        is_bear = price < ema20 < ema50 and slope_10d < -0.05

        if is_bull:
            strong  = slope_20d > 1.5
            regime  = "bull"
            allowed = ["LONG"]
            label   = ("🟢🟢 IMOEX: сильный бычий тренд — лонг в приоритете"
                       if strong else "🟢 IMOEX: умеренный бычий тренд")
        elif is_bear:
            strong  = slope_20d < -1.5
            regime  = "bear"
            allowed = ["WATCH"]
            label   = ("🔴🔴 IMOEX: нисходящий тренд — повышенный риск покупок"
                       if strong else "🔴 IMOEX: локальная слабость рынка")
        else:
            regime  = "neutral"
            allowed = ["LONG", "WATCH"]
            label   = "⚪ IMOEX: консолидация / боковой рынок"

        result = {
            "regime":    regime,
            "allowed":   allowed,
            "label":     label,
            "price":     round(price, 2),
            "ema20":     round(ema20, 2),
            "ema50":     round(ema50, 2),
            "slope_10d": round(slope_10d, 3),
            "slope_20d": round(slope_20d, 3),
        }
        _cache[cache_key] = {"val": result, "ts": now}
        return result

    except Exception as e:
        logger.warning(f"fetch_imoex_regime: {e}")
        fallback = {
            "regime": "neutral", "allowed": ["LONG", "WATCH"],
            "label": "⚪ IMOEX: тренд неопределен",
            "price": 0, "ema20": 0, "ema50": 0, "slope_10d": 0, "slope_20d": 0,
        }
        _cache[cache_key] = {"val": fallback, "ts": now - 900}
        return fallback

# ══════════════════════════════════════════════
# VOLUME PROFILE — узлы объёма (HVN/LVN)
# ══════════════════════════════════════════════
def calculate_volume_profile(df: pd.DataFrame, num_bins: int = 100):
    price_min = df["low"].min()
    price_max = df["high"].max()
    if price_min >= price_max:
        return np.array([price_min]), np.array([df["volume"].sum()])

    bins    = np.linspace(price_min, price_max, num_bins + 1)
    centers = (bins[:-1] + bins[1:]) / 2
    vp      = np.zeros(num_bins)

    lows    = df["low"].values
    highs   = df["high"].values
    volumes = df["volume"].values

    lo_idx = np.clip(np.searchsorted(bins, lows,  side="left")  - 1, 0, num_bins - 1)
    hi_idx = np.clip(np.searchsorted(bins, highs, side="right") - 1, 0, num_bins - 1)

    for i in range(len(volumes)):
        lo, hi = lo_idx[i], hi_idx[i]
        if lo == hi:
            vp[lo] += volumes[i]
        else:
            vp[lo:hi+1] += volumes[i] / (hi - lo + 1)

    return centers, vp

def find_hvn_lvn(df: pd.DataFrame, price: float, dist_limit_pct: float = 20.0):
    if len(df) < 20:
        return {}

    centers, vp = calculate_volume_profile(df)
    vp_mean     = float(vp.mean())
    threshold_h = np.percentile(vp, 70)
    threshold_l = np.percentile(vp, 30)

    poc_idx = int(np.argmax(vp))
    poc     = round(float(centers[poc_idx]), 2)

    hvn_above, hvn_below = None, None
    lvn_above, lvn_below = None, None

    for i in range(1, len(vp) - 1):
        c   = float(centers[i])
        dist = abs(c - price) / price * 100
        if dist > dist_limit_pct:
            continue
        is_hvn = vp[i] >= threshold_h and vp[i] > vp[i-1] and vp[i] > vp[i+1]
        is_lvn = vp[i] <= threshold_l and vp[i] < vp[i-1] and vp[i] < vp[i+1]

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

    return {
        "poc":       poc,
        "vp_mean":   round(vp_mean, 2),
        "hvn_above": hvn_above,
        "hvn_below": hvn_below,
        "lvn_above": lvn_above,
        "lvn_below": lvn_below,
    }

def vp_score_adjustment(vp_nodes: dict, price: float, signal: str) -> tuple[int, list[str]]:
    if not vp_nodes:
        return 0, []

    pts, reasons = 0, []
    hvn_a = vp_nodes.get("hvn_above")
    hvn_b = vp_nodes.get("hvn_below")
    lvn_a = vp_nodes.get("lvn_above")
    lvn_b = vp_nodes.get("lvn_below")
    poc   = vp_nodes.get("poc", 0)

    if "LONG" in signal:
        if hvn_b:
            pts += 8
            reasons.append(f"Уровень HVN {hvn_b['price']:,.2f} снизу (поддержка)")
        if hvn_a:
            pts += 5
            reasons.append(f"HVN магнит сверху {hvn_a['price']:,.2f}")
        if lvn_a:
            pts += 4
            reasons.append(f"Зона пустоты LVN выше {lvn_a['price']:,.2f}")
    elif "SHORT" in signal or "ВЫХОД" in signal:
        if hvn_a:
            pts += 8
            reasons.append(f"Уровень HVN {hvn_a['price']:,.2f} сверху (сопротивление)")
        if hvn_b:
            pts += 5
            reasons.append(f"HVN магнит снизу {hvn_b['price']:,.2f}")

    return pts, reasons

# ══════════════════════════════════════════════
# НОВОСТИ И ПЕРВИЧНЫЕ ИСТОЧНИКИ ФАКТОВ
# ══════════════════════════════════════════════
RUSSIAN_NEWS_RSS = [
    "https://www.e-disclosure.ru/RSS/company.aspx",
    "https://www.interfax.ru/rss.asp",
    "https://tass.ru/rss/v2.xml",
    "https://www.kommersant.ru/RSS/news.xml",
    "https://smart-lab.ru/blog/feed/",
    "https://www.moex.com/export/news.aspx?mode=rss",
]

FACT_PATTERNS: list[tuple[str, str, int, bool]] = [
    ("рекомендовал дивиденд",      "рекомендация дивидендов",      9,  True),
    ("рекомендует дивиденд",       "рекомендация дивидендов",      9,  True),
    ("дивиденды за",               "дивиденды объявлены",          8,  True),
    ("дивиденды выше",             "дивиденды выше ожиданий",      10, True),
    ("обратный выкуп",             "байбек",                       9,  True),
    ("buyback",                    "байбек",                       9,  True),
    ("байбек",                     "байбек",                       9,  True),
    ("рекордная прибыль",          "рекордная прибыль",            8,  True),
    ("прибыль выросла",            "рост прибыли",                 7,  True),
    ("выручка выросла",            "рост выручки",                 6,  True),
    ("дополнительная эмиссия",     "допэмиссия",                   -10, True),
    ("допэмиссия",                 "допэмиссия",                   -10, True),
    ("spo",                        "SPO",                          -8,  True),
    ("отменил дивиденд",           "отмена дивидендов",            -9,  True),
    ("не будет дивидендов",        "отмена дивидендов",            -9,  True),
    ("чистый убыток",              "убыток",                       -8,  True),
    ("новые санкции",              "санкции",                      -9,  False),
    ("санкции против",             "санкции",                      -9,  False),
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
            return {
                "event":        event_label,
                "weight":       weight,
                "is_corporate": is_corp,
                "is_opinion":   False,
                "is_fact":      True,
            }

    return {
        "event":        "новость",
        "weight":       0,
        "is_corporate": False,
        "is_opinion":   is_opinion,
        "is_fact":      False,
    }

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

    search_words.update([
        "дивиденд", "байбек", "buyback", "обратный выкуп",
        "допэмиссия", "spo", "санкции", "прибыль", "выручка", "убыток",
    ])
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
                bool(company_name and any(
                    w in full for w in company_name.lower().split() if len(w) > 3
                ))
            )

            cls = classify_news_item(title)
            if cls["is_corporate"] and cls["is_fact"] and not is_specific:
                continue

            raw_items.append({
                "title":        title[:200],
                "link":         item.get("link", ""),
                "pub":          item.get("pub", ""),
                "source":       url.split("/")[2],
                "is_specific":  is_specific,
                "is_fact":      cls["is_fact"],
                "is_opinion":   cls["is_opinion"],
                "is_corporate": cls.get("is_corporate", False),
                "event":        cls["event"],
                "weight":       cls["weight"],
                "matched":      matched[:3],
            })

    seen, unique = set(), []
    for it in raw_items:
        key = it["title"][:60]
        if key not in seen:
            seen.add(key)
            unique.append(it)

    def sort_key(x):
        if x["is_corporate"] and x["is_fact"]:   return (0, -abs(x["weight"]))
        if x["is_fact"]:                          return (1, -abs(x["weight"]))
        if x["is_specific"] and not x["is_opinion"]: return (2, 0)
        return (3, 0)

    unique.sort(key=sort_key)
    unique = unique[:10]

    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique

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
                    "link":  item.findtext("link", ""),
                    "pub":   item.findtext("pubDate", "")[:16],
                    "desc":  item.findtext("description", "")[:300],
                })
            return items
    except Exception as e:
        logger.warning(f"RSS {url}: {e}")
        return []

async def fetch_market_news() -> list[dict]:
    return await fetch_russian_news()

# ══════════════════════════════════════════════
# AI ОЦЕНКА НОВОСТЕЙ С ИСПОЛЬЗОВАНИЕМ GROQ
# ══════════════════════════════════════════════
def _groq_call(messages: list, model_idx: int = 0) -> str:
    if not groq_client:
        return ""
    for i in range(model_idx, len(GROQ_MODELS)):
        try:
            resp = groq_client.chat.completions.create(
                model=GROQ_MODELS[i],
                messages=messages,
                max_tokens=400,
                temperature=0.05,
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
            events.append(f"{it['event']} ({'+' if w>0 else ''}{w})")
    return total, events

async def ai_evaluate_news(news_items: list[dict], ticker: str, sector: str,
                            tech_signal: str, tech_score: int) -> dict:
    is_long  = "LONG" in tech_signal
    is_short = "SHORT" in tech_signal or "ВЫХОД" in tech_signal
    has_signal = is_long or is_short

    fact_weight, fact_events = _score_facts(news_items)

    blocking_found = [
        it["event"] for it in news_items
        if it.get("is_fact") and it.get("weight", 0) <= -8
    ]

    fact_items    = [it for it in news_items if it.get("is_fact") and not it.get("is_opinion")]
    opinion_items = [it for it in news_items if it.get("is_opinion")]

    event_type   = fact_events[0].split(" (")[0] if fact_events else "нет событий"
    event_weight = fact_weight
    llm_summary  = ""

    if fact_items and groq_client:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        facts_text = "\n".join(
            f"- [{it['event']}, вес {it['weight']}] {it['title']}"
            for it in fact_items[:4]
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
            event_type   = obj.get("event", event_type)
            llm_w = int(obj.get("weight", fact_weight))
            event_weight = max(-10, min(10, (fact_weight + llm_w) // 2))
            llm_summary  = obj.get("summary", "")
        except:
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
        status_map = {
            "CONFIRMED": "🟩 LONG CONFIRMED",
            "WEAK":      "🟡 LONG WEAK",
            "WATCH":     "👀 LONG WATCH",
            "BLOCKED":   "🚫 LONG BLOCKED",
            "NO_SIGNAL": "🟩 LONG",
            "NEWS_ONLY": "🟩 LONG (сильный фон)",
        }
    elif is_short:
        status_map = {
            "CONFIRMED": "🟥 ВЫХОД CONFIRMED",
            "WEAK":      "🟡 ВЫХОД WEAK",
            "WATCH":     "👀 ВЫХОД WATCH",
            "BLOCKED":   "🟥 ВЫХОД (сдерживающий позитив)",
            "NO_SIGNAL": "🟥 ВЫХОД",
            "NEWS_ONLY": "🟥 ВЫХОД",
        }
    else:
        status_map = {k: "НЕТ СИГНАЛА" for k in ["CONFIRMED", "WEAK", "WATCH", "BLOCKED", "NO_SIGNAL", "NEWS_ONLY"]}

    confirmed = status_map.get(filter_status, tech_signal)
    underreaction = any(it.get("is_specific") and abs(it.get("weight", 0)) >= 8 for it in fact_items)

    return {
        "event_type":    event_type,
        "event_weight":  final_weight,
        "fact_events":   fact_events[:3],
        "filter_status": filter_status,
        "summary":       llm_summary,
        "blocking":      blocking_found,
        "underreaction": underreaction,
        "confirmed":     confirmed,
        "opinions_skipped": len(opinion_items),
        "sentiment":  "позитив" if final_weight > 2 else ("негатив" if final_weight < -2 else "нейтрально"),
        "score":      min(10, max(0, abs(final_weight))),
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
        prompt = (
            f'Новость для {ticker} ({company_name}): "{headline}"\n'
            f'Определи влияние. Ответь строго JSON: {{"event": "описание", "weight": число от -10 до 10}}'
        )
        raw = _groq_call([{"role": "user", "content": prompt}])
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            weight = int(obj.get("weight", 0))
        except:
            pass

    sentiment = "позитив" if weight > 2 else ("негатив" if weight < -2 else "нейтрально")
    return {"sentiment": sentiment, "score": min(10, abs(weight)), "asset": ticker, "weight": weight}

# ══════════════════════════════════════════════
# ТЕХНИЧЕСКИЙ АНАЛИЗ (ИНТРАДЕЙ И ДНЕВНОЙ)
# ══════════════════════════════════════════════
def calculate_indicators(df: pd.DataFrame, tf: str = "15m") -> pd.DataFrame:
    df = df.copy()
    close = df["close"]
    is_intraday = tf in INTRADAY_TFS

    # 1. Трендовые средние (EMA)
    if is_intraday:
        df["ema9"]  = ta.ema(close, length=9)
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200) if len(df) >= 200 else np.nan
    else:
        df["ema20"]  = ta.ema(close, length=20)
        df["ema50"]  = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200)

    # 2. RSI
    rsi_len = 7 if tf == "5m" else (9 if tf == "15m" else 14)
    df["rsi"] = ta.rsi(close, length=rsi_len)

    # 3. Быстрый MACD для интрадея, стандартный для дней
    if is_intraday:
        macd = ta.macd(close, fast=8, slow=17, signal=9)
        macd_key, macd_s, macd_h = "MACD_8_17_9", "MACDs_8_17_9", "MACDh_8_17_9"
    else:
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        macd_key, macd_s, macd_h = "MACD_12_26_9", "MACDs_12_26_9", "MACDh_12_26_9"
        
    if macd is not None:
        df["macd"]        = macd.get(macd_key, np.nan)
        df["macd_signal"] = macd.get(macd_s,   np.nan)
        df["macd_hist"]   = macd.get(macd_h,   np.nan)

    # 4. Волатильность и объемы
    atr_len = 7 if is_intraday else 14
    df["atr"] = ta.atr(df["high"], df["low"], close, length=atr_len)

    bb = ta.bbands(close, length=20, std=2)
    if bb is not None:
        df["bb_upper"] = bb.get("BBU_20_2.0", np.nan)
        df["bb_lower"] = bb.get("BBL_20_2.0", np.nan)
        df["bb_mid"]   = bb.get("BBM_20_2.0", np.nan)

    df["vol_ma20"]  = ta.sma(df["volume"], length=20)
    df["vol_ratio"] = df["volume"] / df["vol_ma20"].replace(0, np.nan)

    # 5. ИНТРАДЕЙ VWAP И DAY OPEN (ПРАВИЛЬНЫЙ СБРОС)
    if is_intraday and "volume" in df.columns:
        # Локализуем метку времени и переводим в Московское время
        df["timestamp_msk"] = pd.to_datetime(df["timestamp"]).dt.tz_convert("Europe/Moscow")
        df["date_msk"] = df["timestamp_msk"].dt.date
        
        # Первая свеча календарного дня
        df["day_open"] = df.groupby("date_msk")["open"].transform("first")
        
        tp = (df["high"] + df["low"] + df["close"]) / 3
        df["_tp_vol"] = tp * df["volume"]
        
        # Накопительная сумма с ежедневным сбросом
        cum_vol = df.groupby("date_msk")["volume"].cumsum()
        cum_tpv = df.groupby("date_msk")["_tp_vol"].cumsum()
        
        df["vwap"] = cum_tpv / cum_vol.replace(0, np.nan)
        df["vwap_dev"] = (close - df["vwap"]) / df["vwap"] * 100
        
        df.drop(columns=["_tp_vol"], inplace=True)

    return df

def detect_candle_pattern(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "—"
    c, p = df.iloc[-1], df.iloc[-2]
    body = abs(c["close"] - c["open"])
    rng  = c["high"] - c["low"]
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
    highs  = recent["high"].values
    rsi_v  = recent["rsi"].values

    def extrema(arr, order=3):
        maxima, minima = [], []
        for i in range(order, len(arr) - order):
            w = arr[i-order:i+order+1]
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
            return f"🔄 Бычья дивергенция RSI (+{rsi_v[i2]-rsi_v[i1]:.1f})"
    if len(maxima) >= 2:
        i1, i2 = maxima[-2], maxima[-1]
        if highs[i2] > highs[i1] * 1.002 and rsi_v[i2] < rsi_v[i1] - 1.5:
            return f"🔄 Медвежья дивергенция RSI (-{rsi_v[i1]-rsi_v[i2]:.1f})"
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
        return {"regime": "trending_up",   "label": "📈 Восходящий тренд", "trend_mult": 1.2, "slope": slope}
    if price < ema20_v[-1] < ema50_v[-1] and slope < -0.1:
        return {"regime": "trending_down", "label": "📉 Нисходящий тренд",  "trend_mult": 1.2, "slope": slope}
    return {"regime": "ranging", "label": "↔️ Боковик / Флэт", "trend_mult": 0.8, "slope": slope}

def find_support_resistance(df: pd.DataFrame, price: float):
    r = df.tail(150)
    highs, lows = [], []
    for i in range(2, len(r) - 2):
        h = r.iloc[i]["high"]
        l = r.iloc[i]["low"]
        if h > r.iloc[i-1]["high"] and h > r.iloc[i+1]["high"]:
            highs.append(float(h))
        if l < r.iloc[i-1]["low"] and l < r.iloc[i+1]["low"]:
            lows.append(float(l))
    supports    = sorted([l for l in lows  if l < price], reverse=True)[:3]
    resistances = sorted([h for h in highs if h > price])[:3]
    return supports, resistances

def calculate_sl_tp_stocks(signal: str, price: float, atr: float,
                            supports: list, resistances: list) -> dict:
    if signal not in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
        return {}
    is_long = "LONG" in signal
    raw_risk = max(atr * 1.5, price * 0.008)  # Минимальный риск снижен для интрадея (0.8%)

    if is_long:
        sl  = round(price - raw_risk, 2)
        tp1 = round(price + raw_risk * 1.2, 2)
        tp2 = round(price + raw_risk * 2.2, 2)
        tp3 = round(price + raw_risk * 3.5, 2)
        if resistances:
            close_r = [r for r in resistances if r > price]
            if close_r:
                tp1 = round(min(close_r[0], tp1), 2)
        if supports:
            sl = round(max(supports[0], sl), 2)
    else:
        sl  = round(price + raw_risk, 2)
        tp1 = round(price - raw_risk * 1.2, 2)
        tp2 = round(price - raw_risk * 2.2, 2)
        tp3 = round(price - raw_risk * 3.5, 2)
        if supports:
            close_s = [s for s in supports if s < price]
            if close_s:
                tp1 = round(max(close_s[0], tp1), 2)

    rr = round(abs(tp2 - price) / max(abs(price - sl), 0.001), 2)
    risk_pct = round(abs(price - sl) / price * 100, 2)
    return {
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "risk_pct": risk_pct, "rr_ratio": rr,
        "warn": "⚠️ Низкое соотношение Risk/Reward" if rr < 1.3 else "",
    }

# ══════════════════════════════════════════════
# СБАЛАНСИРОВАННЫЙ СКОРИНГ (ТРЕНД И ФЛЭТ)
# ══════════════════════════════════════════════
def compute_tech_score(df: pd.DataFrame, mode_cfg: dict,
                       vp_nodes: dict = None,
                       imoex_regime: dict = None) -> tuple[str, int, list]:
    row = df.iloc[-1]
    rsi    = float(row.get("rsi", 50) or 50)
    macd_h = float(row.get("macd_hist", 0) or 0)
    close  = float(row["close"])
    vol_r  = float(row.get("vol_ratio", 1) or 1)
    bb_low = float(row.get("bb_lower", 0) or 0)
    bb_up  = float(row.get("bb_upper", 0) or 0)

    regime = detect_market_regime(df)
    candle = detect_candle_pattern(df)
    is_trend = regime["regime"] in ["trending_up", "trending_down"]

    long_score, short_score = 0.0, 0.0
    long_r, short_r = [], []
    
    # Сбалансированный предел баллов
    max_pts = 85.0

    # 1. Свойства Интрадея (VWAP и Day Open)
    vwap     = float(row.get("vwap", 0) or 0)
    vwap_dev = float(row.get("vwap_dev", 0) or 0)
    day_open = float(row.get("day_open", 0) or 0)
    ema9     = float(row.get("ema9", 0) or 0)
    ema20    = float(row.get("ema20", 0) or 0)

    if vwap > 0:
        if close > vwap * 1.001:
            long_score += 20
            long_r.append(f"Выше VWAP (+{vwap_dev:.2f}%)")
        elif close < vwap * 0.999:
            short_score += 20
            short_r.append(f"Ниже VWAP ({vwap_dev:.2f}%)")

    if day_open > 0:
        dev_open = (close / day_open - 1) * 100
        if close > day_open * 1.001:
            long_score += 10
            long_r.append(f"Выше открытия дня ({dev_open:+.2f}%)")
        elif close < day_open * 0.999:
            short_score += 10
            short_r.append(f"Ниже открытия дня ({dev_open:+.2f}%)")

    # 2. Логическое переключение: Трендовые или Контртрендовые баллы
    if is_trend:
        if ema9 > 0 and close > ema9 > ema20:
            long_score += 20
            long_r.append("Импульс EMA9 > EMA20")
        elif ema9 > 0 and close < ema9 < ema20:
            short_score += 20
            short_r.append("Слабость EMA9 < EMA20")

        if macd_h > 0:
            long_score += 10
            long_r.append("MACD > 0")
        elif macd_h < 0:
            short_score += 10
            short_r.append("MACD < 0")
    else:
        # В боковике оцениваем осцилляторы
        if rsi < mode_cfg["rsi_oversold"]:
            long_score += 20
            long_r.append(f"RSI в зоне перепроданности ({rsi:.0f})")
        elif rsi > mode_cfg["rsi_overbought"]:
            short_score += 20
            short_r.append(f"RSI в зоне перекупки ({rsi:.0f})")

        if bb_low > 0 and close < bb_low * 1.002:
            long_score += 10
            long_r.append("Нижняя граница Bollinger")
        elif bb_up > 0 and close > bb_up * 0.998:
            short_score += 10
            short_r.append("Верхняя граница Bollinger")

    # 3. Объемы
    if vol_r > 1.5:
        if close > vwap:
            long_score += 15
            long_r.append(f"Всплеск объема x{vol_r:.1f} вверх")
        else:
            short_score += 15
            short_r.append(f"Всплеск объема x{vol_r:.1f} вниз")

    # 4. Паттерны
    if "Бычье" in candle or "снизу" in candle:
        long_score += 10
        long_r.append(candle)
    elif "Медвежье" in candle or "сверху" in candle:
        short_score += 10
        short_r.append(candle)

    # 5. Оценка Volume Profile (HVN / LVN)
    _cand = "🟩 LONG" if long_score >= short_score else "🟥 SHORT/ВЫХОД"
    if vp_nodes:
        vp_pts, vp_reasons = vp_score_adjustment(vp_nodes, close, _cand)
        if "LONG" in _cand:
            long_score  += vp_pts
            long_r.extend(vp_reasons)
        else:
            short_score += vp_pts
            short_r.extend(vp_reasons)

    # 6. Фильтр общего рынка IMOEX
    if imoex_regime:
        ir = imoex_regime.get("regime", "neutral")
        if ir == "bull":
            long_score += 5
        elif ir == "bear":
            short_score += 5

    # Вычисление финальных результатов
    if long_score > short_score:
        score = min(100, int(long_score / max_pts * 100))
        signal = "🟩 LONG" if score >= mode_cfg["min_score"] and len(long_r) >= 2 else "НЕТ СИГНАЛА"
        reasons = long_r
    else:
        score = min(100, int(short_score / max_pts * 100))
        signal = "🟥 SHORT/ВЫХОД" if score >= mode_cfg["min_score"] and len(short_r) >= 2 else "НЕТ СИГНАЛА"
        reasons = short_r

    return signal, score, reasons

# ══════════════════════════════════════════════
# ОСНОВНОЙ АНАЛИЗ ИНСТРУМЕНТА
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
        news_items       = news_task if not isinstance(news_task, Exception) else []
        imoex_regime     = imoex_task if not isinstance(imoex_task, Exception) else None
    except Exception as e:
        logger.error(f"Error gathering data for {ticker}: {e}")
        return {"error": str(e)}

    if df_result is None or len(df_result) < 30:
        return {"error": f"Недостаточно данных ({ticker}, TF={tf})"}

    df = calculate_indicators(df_result, tf)
    df_closed = df.iloc[:-1].copy()
    price   = float(df_closed["close"].iloc[-1])
    atr     = float(df_closed["atr"].dropna().iloc[-1]) if "atr" in df_closed.columns else price * 0.01
    regime  = detect_market_regime(df_closed)
    supports, resistances = find_support_resistance(df_closed, price)

    vp_nodes = find_hvn_lvn(df_closed, price)

    tech_signal, tech_score, tech_reasons = compute_tech_score(
        df_closed, mode_cfg, vp_nodes=vp_nodes, imoex_regime=imoex_regime)

    news_ai = await ai_evaluate_news(news_items, ticker, sector, tech_signal, tech_score)

    final_signal = news_ai.get("confirmed", tech_signal)
    if imoex_regime and imoex_regime.get("regime") == "bear" and "LONG" in final_signal:
        final_signal = f"⚠️ {final_signal} (Рынок в коррекции)"

    # Проверка лимитов времени сессии для входа
    time_ok, time_warning = is_acceptable_entry_time(tf)
    if not time_ok and ("LONG" in final_signal or "SHORT" in final_signal):
        final_signal = f"⏸ {final_signal} (Время входа вышло)"

    vp_supports    = ([vp_nodes["hvn_below"]["price"]] if vp_nodes.get("hvn_below") else []) + supports
    vp_resistances = ([vp_nodes["hvn_above"]["price"]] if vp_nodes.get("hvn_above") else []) + resistances
    sl_tp = calculate_sl_tp_stocks(tech_signal, price, atr, vp_supports, vp_resistances)

    return {
        "ticker":        ticker,
        "name":          name,
        "sector":        sector,
        "tf":            tf,
        "price":         price,
        "atr":           round(atr, 2),
        "atr_pct":       round(atr / price * 100, 2),
        "tech_signal":   tech_signal,
        "tech_score":    tech_score,
        "tech_reasons":  tech_reasons,
        "regime":        regime,
        "imoex_regime":  imoex_regime,
        "vp_nodes":      vp_nodes,
        "news_items":    news_items[:5],
        "news_ai":       news_ai,
        "final_signal":  final_signal,
        "sl_tp":         sl_tp,
        "supports":      supports,
        "resistances":   resistances,
        "rsi_div":       detect_rsi_divergence(df_closed),
        "candle":        detect_candle_pattern(df_closed),
        "vol_ratio":     round(float(df_closed["vol_ratio"].iloc[-1] or 1), 2),
        "time_warning":  time_warning
    }

# ══════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ ОТВЕТА
# ══════════════════════════════════════════════
def format_analysis(result: dict) -> str:
    if "error" in result:
        return f"❌ {esc(result['error'])}"

    ticker   = result["ticker"]
    name     = result["name"]
    sector   = result["sector"]
    tf       = result["tf"]
    price    = result["price"]
    atr_pct  = result["atr_pct"]
    ts       = result["tech_signal"]
    tscore   = result["tech_score"]
    treasons = result["tech_reasons"]
    regime   = result["regime"]
    imoex    = result.get("imoex_regime")
    vp       = result.get("vp_nodes", {})
    news_ai  = result["news_ai"]
    sl_tp    = result["sl_tp"]
    final    = result["final_signal"]
    rsi_div  = result["rsi_div"]
    candle   = result["candle"]
    vol_r    = result["vol_ratio"]
    news     = result["news_items"]
    time_warn = result.get("time_warning", "")

    bars = "█" * (tscore // 10) + "░" * (10 - tscore // 10)

    lines = [
        f"📊 <b>{esc(ticker)} — {esc(name)}</b> | {esc(sector.upper())}",
        f"⏱ <b>{tf}</b>  |  💰 <b>{price:,.2f} ₽</b>  |  ATR {atr_pct:.2f}%  |  Объем x{vol_r:.1f}",
        "",
    ]

    if imoex:
        slope_arrow = "↑" if imoex["slope_10d"] > 0 else "↓"
        lines += [
            f"<b>🏛 {esc(imoex['label'])}</b>",
            f"   СБЕР: {imoex['price']:,.2f} ₽  Наклон: {slope_arrow}{imoex['slope_10d']:+.2f}%",
            "",
        ]

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
    fs      = news_ai.get("filter_status", "CONFIRMED")
    ew      = news_ai.get("event_weight", 0)
    ev      = news_ai.get("event_type", "нет событий")
    summ    = news_ai.get("summary", "")
    
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
            "",
            "<b>📐 ЦЕЛИ (ИНТРАДЕЙ)</b>",
            f"STOP: {sl_tp['sl']:,.2f} ₽  ({sl_tp['risk_pct']:.2f}% риск)",
            f"TP1:  {sl_tp['tp1']:,.2f} ₽",
            f"TP2:  {sl_tp['tp2']:,.2f} ₽  (R/R {sl_tp['rr_ratio']:.1f})",
        ]
        if sl_tp.get("warn"):
            lines.append(sl_tp["warn"])

    fact_news    = [it for it in news if it.get("is_fact")]
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
# СОСТОЯНИЕ ПОЛЬЗОВАТЕЛЕЙ
# ══════════════════════════════════════════════
_user_state: dict = {}

def get_user_state(chat_id: int) -> dict:
    return _user_state.get(chat_id, {"mode": "mid", "tf": DEFAULT_TF})

def set_user_state(chat_id: int, **kwargs):
    s = get_user_state(chat_id)
    s.update(kwargs)
    _user_state[chat_id] = s

# ══════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
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
        "/tf — таймфрейм (15m по умолчанию)\n\n"
        "<i>⚠️ Жесткое ограничение: все позиции закрываются до 23:50 МСК. Без овернайтов.</i>"
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
    lines += [
        "",
        "/add TICKER — добавить инструмент",
        "/remove TICKER — удалить инструмент",
        "/clear_watchlist — очистить весь ватчлист"
    ]
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
    tf     = args[1].lower() if len(args) > 1 else get_user_state(update.effective_chat.id)["tf"]
    if tf not in TF_MAP:
        tf = DEFAULT_TF

    msg = await update.message.reply_text(f"⏳ Рассчитываю индикаторы для <b>{ticker}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(update.effective_chat.id)["mode"]]

    result = await analyze_stock(ticker, tf, mode_cfg)
    text   = format_analysis(result)

    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("📰 Новости",  callback_data=f"news_{ticker}"),
            InlineKeyboardButton("5м",          callback_data=f"analyze_{ticker}_5m"),
            InlineKeyboardButton("15м",       callback_data=f"analyze_{ticker}_15m"),
            InlineKeyboardButton("1ч",          callback_data=f"analyze_{ticker}_1h"),
        ])
        kb.append([
            InlineKeyboardButton("1д график", callback_data=f"analyze_{ticker}_1d"),
            InlineKeyboardButton("➕ В Ватчлист",  callback_data=f"wl_add_{ticker}"),
        ])

    markup = InlineKeyboardMarkup(kb) if kb else None
    await msg.edit_text(text, parse_mode="HTML", reply_markup=markup)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args   = context.args
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
        lines.append(f"   └ {it['source']} | {it['pub'][:16]}")
        ai = await ai_classify_news_impact(it["title"], ticker or "IMOEX")
        s_e = {"позитив": "🟢", "негатив": "🔴", "нейтрально": "⚪"}.get(ai["sentiment"], "⚪")
        lines.append(f"   └ Влияние: {s_e} {ai['sentiment']} ({ai['score']}/10)")
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
    news    = results[0] if not isinstance(results[0], Exception) else []
    stocks  = [r for r in results[1:] if not isinstance(r, Exception) and r and "error" not in r]

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

def _format_scan_row(s: dict) -> str:
    ticker = s["ticker"]
    name   = s["name"]
    price  = s["price"]
    score  = s["tech_score"]
    sector = s["sector"]
    regime = s["regime"]["label"]
    reason = s["tech_reasons"][0] if s["tech_reasons"] else "—"
    na     = s["news_ai"]
    fs     = na.get("filter_status", "NO_SIGNAL")
    ew     = na.get("event_weight", 0)
    fs_e   = {"CONFIRMED":"✅","WEAK":"🟡","WATCH":"👀","BLOCKED":"🚫","NEWS_ONLY":"📢"}.get(fs,"⚪")
    ew_s   = f"+{ew}" if ew > 0 else str(ew)
    event  = na.get("event_type","")
    event_str = f" 📎 {esc(event)}" if event and event != "нет событий" else ""
    
    return (
        f"<b>{esc(ticker)}</b> — {esc(name)} <i>({esc(sector)})</i>\n"
        f"  💰 {price:,.2f} ₽ | Скор: {score}/100 | {esc(regime)}\n"
        f"  ⚙️ {esc(reason)}\n"
        f"  {fs_e} ИИ-Новость: {fs} [{ew_s}]{event_str}"
    )

async def _run_scan(tickers: list[str], tf: str, mode_cfg: dict,
                    progress_cb=None) -> tuple[list, list, list]:
    long_sigs, short_sigs, watch_sigs = [], [], []
    total = len(tickers)

    for i in range(0, total, 5):
        batch = tickers[i:i+5]
        tasks = [analyze_stock(t, tf, mode_cfg) for t in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception) or not r or "error" in r:
                continue
            sig = r["tech_signal"]
            # Фильтруем заблокированные по времени сессии сигналы
            if "Время входа вышло" in r.get("final_signal", ""):
                continue
                
            if sig == "🟩 LONG":
                long_sigs.append(r)
            elif sig == "🟥 SHORT/ВЫХОД":
                short_sigs.append(r)
            elif r["tech_score"] >= 52:
                watch_sigs.append(r)
        await asyncio.sleep(0.5)
        if progress_cb and i > 0:
            await progress_cb(i, total)

    long_sigs.sort(key=lambda x: -x["tech_score"])
    short_sigs.sort(key=lambda x: -x["tech_score"])
    return long_sigs, short_sigs, watch_sigs

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf       = get_user_state(chat_id)["tf"]
    wl       = load_watchlist()

    if not wl:
        await update.message.reply_text("Ватчлист пуст. Добавьте тикеры через /add или /add_all")
        return

    msg = await update.message.reply_text(f"🔍 Сканирую рынок [{tf}]... Найдено: {len(wl)} активов.", parse_mode="HTML")

    async def progress(done, total):
        try:
            await msg.edit_text(f"🔍 Сканирование рынка... {done}/{total} инструментов [{tf}]", parse_mode="HTML")
        except:
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
        for s in long_sigs[:8]:
            lines.append(_format_scan_row(s))
            lines.append("")

    if short_sigs:
        lines.append("🟥 <b>СИГНАЛЫ НА ВЫХОД:</b>")
        for s in short_sigs[:5]:
            lines.append(_format_scan_row(s))
            lines.append("")

    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
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
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )

async def cmd_tf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_tf  = get_user_state(chat_id)["tf"]
    tfs     = ["5m", "15m", "1h", "4h", "1d"]
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if t == cur_tf else ''}{t}",
            callback_data=f"tf_{t}"
        ) for t in tfs
    ]]
    await update.message.reply_text(
        f"⏱ Рабочий таймфрейм: <b>{cur_tf}</b>\n"
        "Для работы без овернайтов используйте 15m или 1h.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )

async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = load_trades()
    if not trades:
        await update.message.reply_text("📭 Активные позиции отсутствуют.")
        return
    lines = ["📂 <b>Открытые внутридневные сделки:</b>\n"]
    for key, t in trades.items():
        lines.append(
            f"<b>{t['ticker']}</b> {t['signal']} | Вход: {t['entry']:,.2f} ₽\n"
            f"SL: {t['sl']:,.2f} | TP1: {t['tp1']:,.2f} | TP2: {t['tp2']:,.2f}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════
# ОБРАБОТЧИКИ КНОПОК
# ══════════════════════════════════════════════
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    if data.startswith("mode_"):
        mode = data.split("_")[1]
        set_user_state(chat_id, mode=mode)
        await query.edit_message_text(f"✅ Режим чувствительности изменен на: <b>{TRADE_MODES[mode]['label']}</b>", parse_mode="HTML")

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        set_user_state(chat_id, tf=tf)
        await query.edit_message_text(f"✅ Установлен рабочий таймфрейм: <b>{tf}</b>", parse_mode="HTML")

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
        parts  = data.split("_")
        ticker = parts[1]
        tf     = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(f"⏳ Перерасчет {ticker} [{tf}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_stock(ticker, tf, mode_cfg)
        await query.edit_message_text(format_analysis(result), parse_mode="HTML")

    elif data.startswith("wl_add_"):
        ticker = data.split("_", 2)[2]
        ok, msg = add_to_watchlist(ticker)
        await query.answer(msg.replace("<b>","").replace("</b>","")[:200], show_alert=True)

# ══════════════════════════════════════════════
# ФОНОВЫЙ ЦИКЛ СКАНЕРА (ИНТРАДЕЙ И ВЕЧЕРНЯЯ СЕССИЯ)
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

async def scanner_loop(app):
    """
    Фоновый цикл сканера.
    Работает каждые 30 минут. Проверяет торговое время (включая вечернюю сессию).
    Рассылает предупреждение о принудительном закрытии в 23:35 МСК.
    """
    global _last_broadcast_signals
    while True:
        try:
            day, hour, minute = get_msk_time()
            is_trading_day = day < 5  # Пн-Пт

            # Торговые интервалы МосБиржи
            is_main_session = (10 <= hour < 18) or (hour == 18 and minute < 50)
            is_evening_session = (19 <= hour < 23) or (hour == 23 and minute < 50)
            is_trading_time = is_trading_day and (is_main_session or is_evening_session)

            # 1. Рассылка уведомления о принудительном закрытии сделок в 23:35 МСК
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
                # Спим чуть дольше, чтобы не отправить повторно на этой же минуте
                await asyncio.sleep(120)

            # 2. Обычное сканирование рынка каждые 30 минут
            if is_trading_time and SCANNER_CHAT_IDS:
                await run_scanner_broadcast(app)
            else:
                if not is_trading_time:
                    _last_broadcast_signals = set()  # Сброс сигналов вне торговых сессий

        except Exception as e:
            logger.error(f"Error in scanner loop: {e}")

        await asyncio.sleep(1800)  # Интервал сканирования — 30 минут

async def run_scanner_broadcast(app):
    global _last_broadcast_signals
    tf       = "15m"
    mode_cfg = TRADE_MODES["mid"]
    wl       = load_watchlist()

    if not wl:
        return

    long_sigs, short_sigs, _ = await _run_scan(wl, tf, mode_cfg)
    all_sigs = long_sigs + short_sigs

    if not all_sigs:
        return

    new_sigs = [
        s for s in all_sigs
        if f"{s['ticker']}_{s['tech_signal']}" not in _last_broadcast_signals
    ]

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

# ══════════════════════════════════════════════
# ТЕКСТОВЫЙ ОБРАБОТЧИК (БЫСТРЫЙ ВВОД ТИКЕРА)
# ══════════════════════════════════════════════
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text    = update.message.text.strip().upper()
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
# ИНИЦИАЛИЗАЦИЯ И СТАРТ
# ══════════════════════════════════════════════
async def post_init(app):
    asyncio.create_task(scanner_loop(app))
    logger.info("MOEX Intraday Bot готов к работе.")

def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN отсутствует в настройках среды.")

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("analyze",   cmd_analyze))
    app.add_handler(CommandHandler("scan",      cmd_scan))
    app.add_handler(CommandHandler("news",      cmd_news))
    app.add_handler(CommandHandler("market",    cmd_market))
    app.add_handler(CommandHandler("mode",      cmd_mode))
    app.add_handler(CommandHandler("tf",        cmd_tf))
    app.add_handler(CommandHandler("trades",    cmd_trades))
    app.add_handler(CommandHandler("watchlist",   cmd_watchlist))
    app.add_handler(CommandHandler("all_tickers",    cmd_all_tickers))
    app.add_handler(CommandHandler("add",            cmd_add))
    app.add_handler(CommandHandler("remove",         cmd_remove))
    app.add_handler(CommandHandler("clear_watchlist",cmd_clear_watchlist))
    app.add_handler(CommandHandler("add_sector",     cmd_add_sector))
    app.add_handler(CommandHandler("add_all",         cmd_add_all))
    app.add_handler(CommandHandler("scan_start", cmd_scan_start))
    app.add_handler(CommandHandler("scan_stop",  cmd_scan_stop))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Бот запущен в режиме Polling.")
    app.run_polling(
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )

if __name__ == "__main__":
    main()