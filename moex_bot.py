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
# load_dotenv() безвредно — если .env нет, просто ничего не делает.
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
# Формат: ticker -> (figi, name, sector)
# FIGI: актуальные значения из Tinkoff Invest API
MOEX_STOCKS = {
    # ══ 1 ЭШЕЛОН — индекс IMOEX ══════════════════════════════════════════
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

# Ключевые слова для мониторинга новостей по секторам
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
    "телеком":         ["связь", "телеком", "5g", "тариф связь"],
    "транспорт":       ["транспорт", "авиация", "санкции авиа", "фрахт"],
    "горнодобыча":     ["алмазы", "добыча", "санкции добыча"],
    "уголь":           ["уголь", "coal", "экспорт уголь"],
    "лесопромышленность": ["лес", "целлюлоза", "бумага", "лесозаготовка"],
    "e-commerce":      ["электронная торговля", "маркетплейс", "онлайн торговля"],
    "финансы":         ["биржа", "торги", "ликвидность рынок"],
    "горнодобыча":     ["алмазы", "добыча"],
}

# Общерыночные триггеры
MARKET_KEYWORDS = [
    "цб рф", "ключевая ставка", "минфин", "минэкономразвития",
    "санкции", "нефть", "газ", "курс рубля", "рубль",
    "дивиденды", "байбек", "buyback", "допэмиссия", "сделка слияние",
    "ввп россия", "инфляция россия",
]

# Таймфреймы: label -> (tinkoff_interval, минут, лимит свечей)
TF_MAP = {
    "5m":  ("CANDLE_INTERVAL_5_MIN",      5,  300),   # интрадей скальпинг
    "15m": ("CANDLE_INTERVAL_15_MIN",    15,  300),   # интрадей основной
    "1h":  ("CANDLE_INTERVAL_HOUR",      60,  200),   # свинг внутри дня
    "4h":  ("CANDLE_INTERVAL_4_HOUR",   240,  200),
    "1d":  ("CANDLE_INTERVAL_DAY",     1440,  300),
    "1w":  ("CANDLE_INTERVAL_WEEK",   10080,  100),
}
DEFAULT_TF = "15m"   # ← интрадей по умолчанию

# Таймфреймы которые считаются интрадейными (для адаптации индикаторов)
INTRADAY_TFS = {"5m", "15m", "1h"}

# Режимы торговли
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
        "min_score": 60, "min_news_score": 6,
        "personality": "Ты сбалансированный трейдер на MOEX. Ищи сигналы в 1-2 эшелоне акций.",
    },
    "hard": {
        "label": "🔴 HARD",
        "rsi_oversold": 45, "rsi_overbought": 58,
        "min_score": 50, "min_news_score": 5,
        "personality": "Ты агрессивный трейдер на MOEX. Давай конкретный вход без лишних оговорок.",
    },
}

# ══════════════════════════════════════════════
# КЕШИ
# ══════════════════════════════════════════════
_cache: dict = {}
_news_cache: dict = {}

# ══════════════════════════════════════════════
# ФАЙЛЫ СОСТОЯНИЯ
# ══════════════════════════════════════════════
TRADES_FILE    = Path("open_trades.json")
SCANNER_FILE   = Path("scanner_state.json")
WATCHLIST_FILE = Path("watchlist.json")   # личный ватчлист пользователя

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
    """
    Загружает личный ватчлист из файла.
    Если файла нет — возвращает дефолтный список (топ-10 ликвидных).
    """
    try:
        if WATCHLIST_FILE.exists():
            data = json.loads(WATCHLIST_FILE.read_text())
            return [t.upper() for t in data if t.upper() in MOEX_STOCKS]
    except:
        pass
    # Дефолт — самые ликвидные бумаги 1 эшелона
    return ["SBER", "GAZP", "LKOH", "GMKN", "ROSN", "NVTK", "YNDX", "TATN", "CHMF", "NLMK", "MOEX", "VTBR", "MGNT", "FIVE", "AFLT"]

def save_watchlist(tickers: list[str]):
    WATCHLIST_FILE.write_text(json.dumps(tickers, ensure_ascii=False))

def add_to_watchlist(ticker: str) -> tuple[bool, str]:
    """Добавляет тикер в ватчлист. Возвращает (успех, сообщение)."""
    ticker = ticker.upper().strip()
    if ticker not in MOEX_STOCKS:
        # Ищем похожие
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
    """Удаляет тикер из ватчлиста."""
    ticker = ticker.upper().strip()
    wl = load_watchlist()
    if ticker not in wl:
        return False, f"ℹ️ {ticker} не найден в твоём ватчлисте."
    wl.remove(ticker)
    save_watchlist(wl)
    return True, f"🗑 <b>{ticker}</b> удалён из ватчлиста. Осталось: {len(wl)}"

# ══════════════════════════════════════════════
# TINKOFF INVEST API — получение свечей
# ══════════════════════════════════════════════
TINKOFF_API = "https://invest-public-api.tinkoff.ru/rest"

def _tinkoff_headers() -> dict:
    return {
        "Authorization": f"Bearer {TINKOFF_TOKEN}",
        "Content-Type":  "application/json",
    }

async def fetch_candles_tinkoff(figi: str, interval: str, limit: int) -> pd.DataFrame | None:
    """
    Загружает свечи через Tinkoff Invest API v1.
    interval — строка CANDLE_INTERVAL_HOUR / CANDLE_INTERVAL_4_HOUR / CANDLE_INTERVAL_DAY / CANDLE_INTERVAL_WEEK
    """
    cache_key = f"candles_{figi}_{interval}"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 300:
        return _cache[cache_key]["df"]

    # Вычисляем период запроса
    # MOEX торгует ~7ч/день (10:00-18:40 МСК).
    # Для интрадейных TF нужно запрашивать с запасом — учитываем выходные.
    interval_minutes = {
        "CANDLE_INTERVAL_1_MIN":   1,
        "CANDLE_INTERVAL_2_MIN":   2,
        "CANDLE_INTERVAL_3_MIN":   3,
        "CANDLE_INTERVAL_5_MIN":   5,
        "CANDLE_INTERVAL_10_MIN":  10,
        "CANDLE_INTERVAL_15_MIN":  15,
        "CANDLE_INTERVAL_30_MIN":  30,
        "CANDLE_INTERVAL_HOUR":    60,
        "CANDLE_INTERVAL_2_HOUR":  120,
        "CANDLE_INTERVAL_4_HOUR":  240,
        "CANDLE_INTERVAL_DAY":     1440,
        "CANDLE_INTERVAL_WEEK":    10080,
    }.get(interval, 1440)

    # Торговых минут в день на MOEX = ~510 (08:50-15:50 UTC)
    trading_minutes_per_day = 510
    # Сколько торговых дней нужно чтобы набрать limit свечей
    trading_days_needed = max(2, (limit * interval_minutes) // trading_minutes_per_day + 2)
    # Переводим в календарные дни с поправкой на выходные (×1.5) + запас 3 дня
    delta_days = int(trading_days_needed * 1.5) + 3
    delta_days = max(delta_days, 5)   # минимум 5 дней

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
    """Последняя цена через GetLastPrices."""
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
    """Получает свечи и последнюю цену для тикера."""
    info = MOEX_STOCKS.get(ticker.upper())
    if not info:
        return None, None
    figi, name, sector = info
    interval, _, limit = TF_MAP.get(tf, TF_MAP[DEFAULT_TF])
    df = await fetch_candles_tinkoff(figi, interval, limit)
    return df, {"figi": figi, "name": name, "sector": sector, "ticker": ticker}

# ══════════════════════════════════════════════
# IMOEX — МАКРО-РЕЖИМ РЫНКА
# Аналог BTC macro regime, но для MOEX.
# Если IMOEX в аптренде → разрешаем только LONG.
# Если в даунтренде → LONG под вопросом (предупреждение).
# ══════════════════════════════════════════════
# IMOEX через Tinkoff API — индексы не имеют FIGI свечей.
# Используем прокси: берём SBER (самая ликвидная бумага, ~15% индекса)
# + дополнительно пытаемся получить IMOEX через GetIndicatives.
# Если ничего не работает — нейтральный режим (не блокируем торговлю).

IMOEX_TICKER_PROXY = "SBER"   # прокси если IMOEX недоступен

async def _fetch_imoex_via_indicatives() -> float | None:
    """Пытается получить текущее значение IMOEX через GetIndicatives."""
    try:
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # IMOEX uid в Tinkoff
            async with session.post(
                f"{TINKOFF_API}/tinkoff.public.invest.api.contract.v1.MarketDataService/GetIndicatives",
                headers=_tinkoff_headers(),
                json={},
            ) as r:
                if r.status != 200:
                    return None
                data = await r.json()
                for item in data.get("indicatives", []):
                    name = item.get("ticker", "") + item.get("name", "")
                    if "IMOEX" in name or "МосБирж" in name:
                        p = item.get("lastPrice", {})
                        val = float(p.get("units", 0)) + float(p.get("nano", 0)) / 1e9
                        return val if val > 0 else None
    except Exception as e:
        logger.debug(f"GetIndicatives: {e}")
    return None

async def fetch_imoex_regime() -> dict:
    """
    Определяет режим индекса МосБиржи.
    Стратегия:
      1. Пробуем загрузить свечи SBER (прокси IMOEX) — надёжно
      2. EMA20/EMA50 по SBER как индикатор рыночного тренда
    Кеш 30 минут.
    """
    cache_key = "imoex_regime"
    now = time.time()
    if cache_key in _cache and now - _cache[cache_key]["ts"] < 1800:
        return _cache[cache_key]["val"]

    try:
        # Используем SBER как прокси — он тянет за собой весь рынок
        figi = MOEX_STOCKS["SBER"][0]
        df = await fetch_candles_tinkoff(figi, "CANDLE_INTERVAL_DAY", 120)
        if df is None or len(df) < 50:
            raise ValueError("Мало данных для IMOEX-прокси (SBER)")

        close = df["close"].values
        price = close[-1]
        s     = pd.Series(close)

        ema20 = float(s.ewm(span=20).mean().iloc[-1])
        ema50 = float(s.ewm(span=50).mean().iloc[-1])

        ema50_arr = s.ewm(span=50).mean().values
        slope_10d = (ema50_arr[-1] - ema50_arr[-10]) / ema50_arr[-10] * 100
        slope_20d = (ema50_arr[-1] - ema50_arr[-20]) / ema50_arr[-20] * 100

        is_bull = price > ema20 > ema50 and slope_10d > 0.1
        is_bear = price < ema20 < ema50 and slope_10d < -0.1

        if is_bull:
            strong  = slope_20d > 2.0
            regime  = "bull"
            allowed = ["LONG"]
            label   = ("🟢🟢 IMOEX: сильный бычий рынок — только LONG"
                       if strong else "🟢 IMOEX: бычий рынок — приоритет LONG")
        elif is_bear:
            strong  = slope_20d < -2.0
            regime  = "bear"
            allowed = ["WATCH"]   # на акциях нет шорта, только наблюдение
            label   = ("🔴🔴 IMOEX: сильный медвежий рынок — воздержаться от покупок"
                       if strong else "🔴 IMOEX: медвежий рынок — осторожность с покупками")
        else:
            regime  = "neutral"
            allowed = ["LONG", "WATCH"]
            label   = "⚪ IMOEX: нейтральный рынок — оба направления"

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
            "label": "⚪ IMOEX: режим неизвестен",
            "price": 0, "ema20": 0, "ema50": 0, "slope_10d": 0, "slope_20d": 0,
        }
        _cache[cache_key] = {"val": fallback, "ts": now - 1500}
        return fallback

# ══════════════════════════════════════════════
# VOLUME PROFILE — узлы объёма (HVN/LVN)
# Адаптирован из старого бота под акции MOEX.
# ══════════════════════════════════════════════
def calculate_volume_profile(df: pd.DataFrame, num_bins: int = 100):
    """
    Векторизованный Volume Profile.
    Возвращает (centers, vp) — центры бинов и объёмы.
    """
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
    """
    Находит HVN (High Volume Node) и LVN (Low Volume Node) вблизи текущей цены.
    HVN = зоны накопления объёма → поддержка/сопротивление
    LVN = зоны пустоты → быстрое прохождение цены

    Возвращает dict с ключами:
      hvn_above: ближайший HVN выше цены
      hvn_below: ближайший HVN ниже цены
      lvn_above: ближайший LVN выше (цена легко пройдёт)
      lvn_below: ближайший LVN ниже
      poc:       Price of Control (бин с максимальным объёмом)
      vp_mean:   средний объём по бинам
    """
    if len(df) < 20:
        return {}

    centers, vp = calculate_volume_profile(df)
    vp_mean     = float(vp.mean())
    threshold_h = np.percentile(vp, 70)   # HVN — топ 30%
    threshold_l = np.percentile(vp, 30)   # LVN — нижние 30%

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
    """
    Корректирует скор на основе Volume Profile.
    Логика (контекстно-зависимая, как в старом боте):
      LONG:
        HVN снизу = поддержка → +10
        HVN сверху в аптренде = магнит к цели → +8
        LVN сверху = путь чист → +6
        Цена у POC = зона принятия → +5
      SHORT/ВЫХОД:
        HVN сверху = сопротивление → +10
        HVN снизу в даунтренде = цель → +8
    """
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
            pts += 10
            reasons.append(f"HVN {hvn_b['price']:,.2f} — поддержка ({hvn_b['strength']}x)")
        if hvn_a:
            pts += 8
            reasons.append(f"HVN {hvn_a['price']:,.2f} — магнит лонга ({hvn_a['strength']}x)")
        if lvn_a:
            pts += 6
            reasons.append(f"LVN {lvn_a['price']:,.2f} — путь наверх чист")
        if poc and abs(poc - price) / price * 100 < 1.5:
            pts += 5
            reasons.append(f"Цена у POC {poc:,.2f} (зона накопления)")
    elif "SHORT" in signal or "ВЫХОД" in signal:
        if hvn_a:
            pts += 10
            reasons.append(f"HVN {hvn_a['price']:,.2f} — сопротивление ({hvn_a['strength']}x)")
        if hvn_b:
            pts += 8
            reasons.append(f"HVN {hvn_b['price']:,.2f} — цель снижения ({hvn_b['strength']}x)")
        if lvn_b:
            pts += 6
            reasons.append(f"LVN {lvn_b['price']:,.2f} — путь вниз чист")

    return pts, reasons

# ══════════════════════════════════════════════
# ══════════════════════════════════════════════
# НОВОСТИ — источники и классификация событий
# ══════════════════════════════════════════════
# Главный принцип: нам нужны ФАКТЫ, а не мнения.
# Факты: дивиденды, отчёт, байбек, допэмиссия, санкции, ставка ЦБ, SPO.
# Мнения аналитиков и обзоры рынка — отфильтровываются ДО отправки в LLM.

# RSS-ленты: только первичные источники
RUSSIAN_NEWS_RSS = [
    # Корпоративные факты
    "https://www.e-disclosure.ru/RSS/company.aspx",       # раскрытие эмитентов (дивиденды, допэмиссии)
    "https://www.interfax.ru/rss.asp",                    # Интерфакс — первичные новости
    "https://tass.ru/rss/v2.xml",                         # ТАСС
    "https://www.kommersant.ru/RSS/news.xml",             # Коммерсантъ
    "https://smart-lab.ru/blog/feed/",                    # Smart-lab — корпоративные события
    "https://www.moex.com/export/news.aspx?mode=rss",     # МосБиржа официальные сообщения
]

# Паттерны ФАКТИЧЕСКИХ событий (pre-filter до LLM)
# Ключ — паттерн в заголовке, значение — (event_label, weight, is_corporate)
FACT_PATTERNS: list[tuple[str, str, int, bool]] = [
    # Позитивные корпоративные факты
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
    ("повысил прогноз",            "повышение прогноза",           7,  True),
    ("повышает прогноз",           "повышение прогноза",           7,  True),
    ("новый контракт",             "новый контракт",               6,  True),
    ("сделка на",                  "крупная сделка",               5,  True),
    # Негативные корпоративные факты
    ("дополнительная эмиссия",     "допэмиссия",                   -10, True),
    ("допэмиссия",                 "допэмиссия",                   -10, True),
    ("дополнительный выпуск акций","допэмиссия",                   -10, True),
    ("spo",                        "SPO (доп. выпуск)",            -8,  True),
    ("отменил дивиденд",           "отмена дивидендов",            -9,  True),
    ("не будет дивидендов",        "отмена дивидендов",            -9,  True),
    ("снизил дивиденд",            "снижение дивидендов",          -7,  True),
    ("чистый убыток",              "убыток",                       -8,  True),
    ("зафиксировал убыток",        "убыток",                       -8,  True),
    ("снизил прогноз",             "снижение прогноза",            -7,  True),
    ("снижает прогноз",            "снижение прогноза",            -7,  True),
    ("штраф",                      "штраф",                        -6,  True),
    ("иск",                        "судебный иск",                 -5,  True),
    # Макро/регуляторные факты (некорпоративные но важные)
    ("ключевую ставку повысил",    "повышение ставки ЦБ",          -7,  False),
    ("ключевую ставку снизил",     "снижение ставки ЦБ",           7,   False),
    ("новые санкции",              "санкции",                      -9,  False),
    ("санкции против",             "санкции",                      -9,  False),
    ("санкции сша",                "санкции США",                  -9,  False),
    ("санкции ес",                 "санкции ЕС",                   -8,  False),
    ("экспортные ограничения",     "экспортные ограничения",       -6,  False),
    ("пошлина на",                 "пошлина",                      -5,  False),
    ("решение цб",                 "решение ЦБ",                   -4,  False),
]

# Паттерны МНЕНИЙ — отфильтровываем (не отправляем в LLM как значимые)
OPINION_PATTERNS = [
    "считает аналитик", "по мнению", "эксперт полагает", "аналитики ожидают",
    "прогноз аналитик", "целевая цена", "рекомендация покупать",
    "рекомендация продавать", "обзор рынка", "итоги торгов",
    "рынок акций", "индекс мосбиржи вырос", "индекс мосбиржи снизился",
    "утренний обзор", "вечерний обзор", "дайджест",
]

def classify_news_item(title: str) -> dict:
    """
    Pre-filter: классифицируем новость до LLM.
    Возвращает dict с полями: event, weight, is_corporate, is_opinion, is_fact.
    """
    tl = title.lower()

    # Сначала проверяем — это мнение/обзор?
    is_opinion = any(op in tl for op in OPINION_PATTERNS)

    # Ищем факт по паттернам
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
        "event":        "общая новость",
        "weight":       0,
        "is_corporate": False,
        "is_opinion":   is_opinion,
        "is_fact":      False,
    }


async def fetch_russian_news(ticker: str = "", sector: str = "") -> list[dict]:
    """
    Загружает новости, применяет pre-filter событий.
    Возвращает список с полями: title, source, pub, is_specific,
    is_fact, is_opinion, event, weight.
    Кеш 15 минут.
    """
    cache_key = f"news_{ticker}_{sector}"
    now = time.time()
    if cache_key in _news_cache and now - _news_cache[cache_key]["ts"] < 900:
        return _news_cache[cache_key]["items"]

    company_name = ""
    if ticker and ticker.upper() in MOEX_STOCKS:
        _, company_name, _ = MOEX_STOCKS[ticker.upper()]

    # Слова для поиска релевантности
    search_words: set[str] = set()
    if ticker:
        search_words.update([ticker.lower(), ticker.upper()])
    if company_name:
        search_words.update(w for w in company_name.lower().split() if len(w) > 3)
    # Всегда ищем макро-события
    search_words.update([
        "дивиденд", "байбек", "buyback", "обратный выкуп",
        "допэмиссия", "spo", "санкции", "ключевая ставка",
        "прибыль", "выручка", "убыток", "отчёт", "мсфо",
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

            # Проверяем релевантность
            matched = [w for w in search_words if w in full]
            if not matched:
                continue

            is_specific = (
                ticker.lower() in full or
                bool(company_name and any(
                    w in full for w in company_name.lower().split() if len(w) > 3
                ))
            )

            # Pre-classify
            cls = classify_news_item(title)

            # КЛЮЧЕВОЕ ПРАВИЛО: корпоративный факт (дивиденды, байбек и т.д.)
            # принимаем ТОЛЬКО если новость явно про нашу компанию (is_specific).
            # Иначе дивиденды МГКЛ попадут в анализ ВТБ просто по слову "дивиденды".
            if cls["is_corporate"] and cls["is_fact"] and not is_specific:
                continue  # чужая корпоративная новость — пропускаем

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

    # Дедупликация
    seen, unique = set(), []
    for it in raw_items:
        key = it["title"][:60]
        if key not in seen:
            seen.add(key)
            unique.append(it)

    # Сортировка: корпоративные факты → остальные факты → нейтральные → мнения
    def sort_key(x):
        if x["is_corporate"] and x["is_fact"]:   return (0, -abs(x["weight"]))
        if x["is_fact"]:                          return (1, -abs(x["weight"]))
        if x["is_specific"] and not x["is_opinion"]: return (2, 0)
        if not x["is_opinion"]:                   return (3, 0)
        return (4, 0)  # мнения — в конец

    unique.sort(key=sort_key)
    unique = unique[:12]

    _news_cache[cache_key] = {"items": unique, "ts": now}
    return unique


async def _fetch_rss(session: aiohttp.ClientSession, url: str, headers: dict) -> list[dict]:
    """Загружает и парсит один RSS-фид."""
    try:
        async with session.get(url, headers=headers) as r:
            if r.status != 200:
                return []
            text = await r.text(errors="replace")
            root = ET.fromstring(text)
            items = []
            for item in root.findall(".//item")[:25]:
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
    """Общерыночные новости — только факты, без привязки к тикеру."""
    return await fetch_russian_news()


# ══════════════════════════════════════════════
# ══════════════════════════════════════════════
# AI — ФИЛЬТР КАЧЕСТВА НОВОСТЕЙ
# ══════════════════════════════════════════════
# Принцип: ИИ НЕ меняет направление сигнала (не LONG→SHORT).
# ИИ только оценивает КАЧЕСТВО новостного фона для уже принятого
# технического решения. Итог: CONFIRMED / WEAK / WATCH / BLOCKED.
#
# Весовая таблица событий (по важности для РФ рынка):
EVENT_WEIGHTS = {
    # Позитивные события
    "дивиденды выше ожиданий":  10,
    "рекомендация дивидендов":   9,
    "байбек":                    9,
    "обратный выкуп":            9,
    "рекордная прибыль":         8,
    "повышение прогноза":        8,
    "повышение рейтинга":        7,
    "сильные результаты":        7,
    "рост выручки":              6,
    "новый контракт":            6,
    # Негативные события
    "допэмиссия":               -10,
    "санкции":                   -9,
    "отмена дивидендов":         -9,
    "снижение дивидендов":       -8,
    "убыток":                    -8,
    "снижение прогноза":         -7,
    "снижение рейтинга":         -7,
    "штраф":                     -6,
    "иск":                       -5,
    # Малозначимые (LLM не должна на них опираться)
    "интервью менеджера":         2,
    "мнение аналитика":           1,
    "комментарий":                1,
}

# ── Groq helper ───────────────────────────────
def _groq_call(messages: list, model_idx: int = 0) -> str:
    if not groq_client:
        return ""
    for i in range(model_idx, len(GROQ_MODELS)):
        try:
            resp = groq_client.chat.completions.create(
                model=GROQ_MODELS[i],
                messages=messages,
                max_tokens=500,
                temperature=0.05,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"Groq {GROQ_MODELS[i]}: {e}")
    return ""

# ── Весовая оценка без LLM ────────────────────
def _score_facts(news_items: list[dict]) -> tuple[int, list[str]]:
    """
    Суммирует вес УЖЕ классифицированных фактических новостей.
    Мнения (is_opinion=True) полностью игнорируются.
    Возвращает (total_weight, список найденных событий).
    """
    total = 0
    events = []
    for it in news_items:
        if it.get("is_opinion"):
            continue  # мнения не считаем
        w = it.get("weight", 0)
        if w != 0:
            total += w
            events.append(f"{it['event']} ({'+' if w>0 else ''}{w})")
    return total, events


# ── Основная функция фильтра ──────────────────
async def ai_evaluate_news(news_items: list[dict], ticker: str, sector: str,
                            tech_signal: str, tech_score: int) -> dict:
    """
    Новостной фильтр качества сигнала.

    Правила:
    - ИИ НЕ меняет направление (LONG остаётся LONG).
    - Pre-filter уже отсеял мнения — LLM получает только факты.
    - LLM подтверждает самое важное событие и пишет одно предложение.
    - Итог: CONFIRMED / WEAK / WATCH / BLOCKED
    """
    is_long  = "LONG" in tech_signal
    is_short = "SHORT" in tech_signal or "ВЫХОД" in tech_signal
    has_signal = is_long or is_short

    # ── Шаг 1: Весовой скор по pre-classified фактам (без LLM) ────────────
    fact_weight, fact_events = _score_facts(news_items)

    # Блокирующие события — определяем из уже классифицированных
    blocking_found = [
        it["event"] for it in news_items
        if it.get("is_fact") and it.get("weight", 0) <= -8
    ]

    # Отделяем факты от мнений для LLM
    fact_items    = [it for it in news_items if it.get("is_fact") and not it.get("is_opinion")]
    opinion_items = [it for it in news_items if it.get("is_opinion")]

    # ── Шаг 2: LLM — подтверждает событие и даёт краткий контекст ─────────
    # LLM видит ТОЛЬКО факты, не мнения. Не знает про техсигнал.
    event_type   = fact_events[0].split(" (")[0] if fact_events else "нет значимых событий"
    event_weight = fact_weight
    llm_summary  = ""

    if fact_items and groq_client:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        facts_text = "\n".join(
            f"- [{it['event']}, вес {'+' if it['weight']>0 else ''}{it['weight']}] {it['title']}"
            for it in fact_items[:5]
        )

        prompt = f"""Ты — аналитик российского рынка акций.
Компания: {company_name} ({ticker}), сектор: {sector}

Фактические корпоративные события (уже классифицированы):
{facts_text}

Задача: написать ОДНО предложение — что именно произошло и почему это важно для акции.
Не оценивай направление сделки. Только факт.

Ответь СТРОГО JSON без markdown:
{{"event": "самое важное событие", "weight": итоговый вес от -10 до 10, "summary": "одно предложение"}}"""

        raw = _groq_call([{"role": "user", "content": prompt}])
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            event_type   = obj.get("event", event_type)
            # LLM может уточнить вес, но не выйти за рамки pre-filter значительно
            llm_w = int(obj.get("weight", fact_weight))
            event_weight = max(-10, min(10, (fact_weight + llm_w) // 2))
            llm_summary  = obj.get("summary", "")
        except Exception:
            pass

    final_weight = event_weight

    # ── Шаг 3: Статус фильтра ─────────────────────────────────────────────
    if blocking_found and is_long:
        filter_status = "BLOCKED"
    elif not has_signal and abs(final_weight) >= 8:
        # Нет техсигнала, но есть сильное корпоративное событие —
        # сообщаем о нём отдельно (не блокируем, просто информируем)
        filter_status = "NEWS_ONLY"
    elif not has_signal:
        filter_status = "NO_SIGNAL"
    elif final_weight >= 6:
        filter_status = "CONFIRMED"
    elif final_weight >= -2:
        filter_status = "CONFIRMED"   # нейтрально — техника главная
    elif final_weight >= -5:
        filter_status = "WEAK"
    elif final_weight >= -7:
        filter_status = "WATCH"
    else:
        filter_status = "BLOCKED"

    # ── Шаг 4: Итоговая метка (НЕ меняем направление) ─────────────────────
    if tech_signal == "🟩 LONG":
        status_map = {
            "CONFIRMED": "🟩 LONG CONFIRMED",
            "WEAK":      "🟡 LONG WEAK",
            "WATCH":     "👀 LONG WATCH",
            "BLOCKED":   "🚫 LONG BLOCKED",
            "NO_SIGNAL": "🟩 LONG",
            "NEWS_ONLY": "🟩 LONG (сильная новость)",
        }
    elif is_short:
        status_map = {
            "CONFIRMED": "🟥 ВЫХОД CONFIRMED",
            "WEAK":      "🟡 ВЫХОД WEAK",
            "WATCH":     "👀 ВЫХОД WATCH",
            "BLOCKED":   "🟥 ВЫХОД (позитивный фон — проверь)",
            "NO_SIGNAL": "🟥 ВЫХОД",
            "NEWS_ONLY": "🟥 ВЫХОД (сильная новость)",
        }
    else:
        status_map = {
            "CONFIRMED": "НЕТ СИГНАЛА",
            "WEAK":      "НЕТ СИГНАЛА",
            "WATCH":     "НЕТ СИГНАЛА",
            "BLOCKED":   "НЕТ СИГНАЛА",
            "NO_SIGNAL": "НЕТ СИГНАЛА",
            "NEWS_ONLY": "НЕТ СИГНАЛА",
        }

    confirmed = status_map.get(filter_status, tech_signal)

    # underreaction: сильное корпоративное событие, is_specific=True
    specific_facts = [it for it in fact_items if it.get("is_specific") and abs(it.get("weight",0)) >= 8]
    underreaction  = len(specific_facts) > 0

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
        # совместимость
        "sentiment":  "позитив" if final_weight > 2 else ("негатив" if final_weight < -2 else "нейтрально"),
        "score":      min(10, max(0, abs(final_weight))),
        "horizon":    "краткосрочный",
        "reaction":   "рост" if final_weight > 2 else ("падение" if final_weight < -2 else "нейтрально"),
    }


async def ai_classify_news_impact(headline: str, ticker: str) -> dict:
    """Быстрая весовая классификация одного заголовка (для /news команды)."""
    text = headline.lower()
    weight = 0
    for kw, w in EVENT_WEIGHTS.items():
        if kw in text:
            weight = w
            break

    # LLM только если есть groq и заголовок не классифицирован
    if groq_client and weight == 0:
        company_name = MOEX_STOCKS.get(ticker.upper(), ("", ticker, ""))[1]
        prompt = (
            f'Новость для {ticker} ({company_name}): "{headline}"\n'
            f'Ответь JSON: {{"event": "краткое название события", "weight": число от -10 до 10}}\n'
            f'weight>0 позитив, <0 негатив, 0 нейтрально. Только JSON без markdown.'
        )
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
# ТЕХНИЧЕСКИЙ АНАЛИЗ (адаптирован под акции)
# ══════════════════════════════════════════════
def calculate_indicators(df: pd.DataFrame, tf: str = "1d") -> pd.DataFrame:
    """
    Рассчитывает индикаторы.
    Для интрадейных TF (5m/15m/1h) добавляет VWAP и EMA9.
    Для дневных — оставляет EMA200 и классические периоды.
    """
    df = df.copy()
    close = df["close"]
    is_intraday = tf in INTRADAY_TFS

    # ── EMA ──────────────────────────────────────────────────────────────
    if is_intraday:
        df["ema9"]  = ta.ema(close, length=9)    # быстрая для интрадея
        df["ema20"] = ta.ema(close, length=20)
        df["ema50"] = ta.ema(close, length=50)
        # EMA200 на 15м = ~50 часов торгов, считаем только если данных достаточно
        df["ema200"] = ta.ema(close, length=200) if len(df) >= 200 else np.nan
    else:
        df["ema20"]  = ta.ema(close, length=20)
        df["ema50"]  = ta.ema(close, length=50)
        df["ema200"] = ta.ema(close, length=200)

    # ── RSI ───────────────────────────────────────────────────────────────
    rsi_len = 7 if tf == "5m" else (9 if tf == "15m" else 14)
    df["rsi"] = ta.rsi(close, length=rsi_len)

    # ── MACD ──────────────────────────────────────────────────────────────
    # Для интрадея — быстрый MACD
    if is_intraday:
        macd = ta.macd(close, fast=8, slow=17, signal=9)
        macd_key = "MACD_8_17_9"
        macd_s   = "MACDs_8_17_9"
        macd_h   = "MACDh_8_17_9"
    else:
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        macd_key = "MACD_12_26_9"
        macd_s   = "MACDs_12_26_9"
        macd_h   = "MACDh_12_26_9"
    if macd is not None:
        df["macd"]        = macd.get(macd_key, np.nan)
        df["macd_signal"] = macd.get(macd_s,   np.nan)
        df["macd_hist"]   = macd.get(macd_h,   np.nan)

    # ── ATR ───────────────────────────────────────────────────────────────
    atr_len = 7 if is_intraday else 14
    df["atr"] = ta.atr(df["high"], df["low"], close, length=atr_len)

    # ── Боллинджер ────────────────────────────────────────────────────────
    bb = ta.bbands(close, length=20, std=2)
    if bb is not None:
        df["bb_upper"] = bb.get("BBU_20_2.0", np.nan)
        df["bb_lower"] = bb.get("BBL_20_2.0", np.nan)
        df["bb_mid"]   = bb.get("BBM_20_2.0", np.nan)

    # ── Объём ─────────────────────────────────────────────────────────────
    df["vol_ma20"]  = ta.sma(df["volume"], length=20)
    df["vol_ratio"] = df["volume"] / df["vol_ma20"].replace(0, np.nan)

    # ── VWAP (только для интрадея) ────────────────────────────────────────
    # Простой VWAP по всем доступным свечам текущей сессии
    # Настоящий дневной VWAP требует знания даты — делаем накопительный
    if is_intraday and "volume" in df.columns:
        tp = (df["high"] + df["low"] + df["close"]) / 3   # typical price
        cum_vol = df["volume"].cumsum()
        cum_tpv = (tp * df["volume"]).cumsum()
        df["vwap"] = cum_tpv / cum_vol.replace(0, np.nan)
        # Отклонение от VWAP в %
        df["vwap_dev"] = (close - df["vwap"]) / df["vwap"] * 100

    # ── Уровень открытия дня (для интрадея) ──────────────────────────────
    if is_intraday and len(df) > 0:
        df["day_open"] = float(df["open"].iloc[0])   # первая свеча в загруженном окне

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
        return "📌 Бычий пин-бар"
    if uw > body * 2 and lw < body * 0.5:
        return "📌 Медвежий пин-бар"
    if (c["close"] > c["open"] and p["close"] < p["open"] and
            c["close"] > p["open"] and c["open"] < p["close"]):
        return "🟢 Бычье поглощение"
    if (c["close"] < c["open"] and p["close"] > p["open"] and
            c["close"] < p["open"] and c["open"] > p["close"]):
        return "🔴 Медвежье поглощение"
    if body < rng * 0.1:
        return "〰️ Дожи"
    return "Обычная свеча"

def detect_rsi_divergence(df: pd.DataFrame) -> str:
    if len(df) < 40 or "rsi" not in df.columns:
        return ""
    recent   = df.tail(50).copy().dropna(subset=["rsi"])
    if len(recent) < 20:
        return ""
    prices   = recent["low"].values
    highs    = recent["high"].values
    rsi_vals = recent["rsi"].values

    def extrema(arr, order=3):
        maxima, minima = [], []
        for i in range(order, len(arr) - order):
            w = arr[i-order:i+order+1]
            if arr[i] == w.max():
                maxima.append(i)
            if arr[i] == w.min():
                minima.append(i)
        return np.array(maxima), np.array(minima)

    _, minima = extrema(prices)
    maxima, _ = extrema(highs)

    if len(minima) >= 2:
        i1, i2 = minima[-2], minima[-1]
        if prices[i2] < prices[i1] * 0.998 and rsi_vals[i2] > rsi_vals[i1] + 2:
            return f"🔄 Бычья дивергенция RSI (+{rsi_vals[i2]-rsi_vals[i1]:.1f})"
    if len(maxima) >= 2:
        i1, i2 = maxima[-2], maxima[-1]
        if highs[i2] > highs[i1] * 1.002 and rsi_vals[i2] < rsi_vals[i1] - 2:
            return f"🔄 Медвежья дивергенция RSI (-{rsi_vals[i1]-rsi_vals[i2]:.1f})"
    return ""

def detect_market_regime(df: pd.DataFrame) -> dict:
    if len(df) < 50:
        return {"regime": "unknown", "label": "❓ Неизвестно", "trend_mult": 1.0}
    close = df["close"].values
    price = close[-1]
    ema20 = ta.ema(pd.Series(close), length=20).values
    ema50 = ta.ema(pd.Series(close), length=50).values
    ema20_v = ema20[~np.isnan(ema20)]
    ema50_v = ema50[~np.isnan(ema50)]
    if len(ema50_v) < 10:
        return {"regime": "unknown", "label": "❓ Неизвестно", "trend_mult": 1.0}

    slope = (ema50_v[-1] - ema50_v[-10]) / ema50_v[-10] * 100

    if price > ema20_v[-1] > ema50_v[-1] and slope > 0.2:
        return {"regime": "trending_up",   "label": "📈 Тренд вверх",  "trend_mult": 1.3, "slope": slope}
    if price < ema20_v[-1] < ema50_v[-1] and slope < -0.2:
        return {"regime": "trending_down", "label": "📉 Тренд вниз",   "trend_mult": 1.3, "slope": slope}
    if abs(slope) < 0.1:
        return {"regime": "ranging",       "label": "↔️ Боковик",       "trend_mult": 0.8, "slope": slope}
    return {"regime": "mixed",             "label": "🔀 Смешанный",    "trend_mult": 1.0, "slope": slope}

def find_support_resistance(df: pd.DataFrame, price: float):
    r = df.tail(200)
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
    """SL/TP для акций. SL = 1.5×ATR, TP1=1:1.5, TP2=1:2.5"""
    if signal not in ("🟩 LONG", "🟥 SHORT/ВЫХОД"):
        return {}
    is_long = "LONG" in signal
    raw_risk = max(atr * 1.5, price * 0.015)  # минимум 1.5%

    if is_long:
        sl  = round(price - raw_risk, 2)
        tp1 = round(price + raw_risk * 1.5, 2)
        tp2 = round(price + raw_risk * 2.5, 2)
        tp3 = round(price + raw_risk * 4.0, 2)
        # Привязываем к уровням
        if resistances:
            close_r = [r for r in resistances if r > price and r < price + raw_risk * 5]
            if close_r:
                tp1 = round(min(close_r[0], tp1 + price * 0.005), 2)
        if supports:
            sl = round(max(supports[0] - price * 0.005, sl), 2)
    else:
        sl  = round(price + raw_risk, 2)
        tp1 = round(price - raw_risk * 1.5, 2)
        tp2 = round(price - raw_risk * 2.5, 2)
        tp3 = round(price - raw_risk * 4.0, 2)
        if supports:
            close_s = [s for s in supports if s < price and s > price - raw_risk * 5]
            if close_s:
                tp1 = round(max(close_s[0], tp1 - price * 0.005), 2)

    rr = round(abs(tp2 - price) / max(abs(price - sl), 0.001), 2)
    risk_pct = round(abs(price - sl) / price * 100, 2)
    return {
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "risk_pct": risk_pct, "rr_ratio": rr,
        "warn": "⚠️ R/R ниже 1.5" if rr < 1.5 else "",
    }

def compute_tech_score(df: pd.DataFrame, mode_cfg: dict,
                       vp_nodes: dict = None,
                       imoex_regime: dict = None) -> tuple[str, int, list]:
    """
    Скоринг технического сигнала для акций.
    Возвращает (signal, score 0-100, reasons)

    Включает:
    - EMA20/50/200 тренд
    - RSI + дивергенция
    - MACD
    - Боллинджер
    - Объём
    - Volume Profile (HVN/LVN/POC)
    - Фильтр IMOEX: медвежий рынок → штраф к LONG

    SHORT/ВЫХОД = сигнал к продаже/фиксации (не классический шорт).
    """
    row = df.iloc[-1]
    rsi    = float(row.get("rsi", 50) or 50)
    ema20  = float(row.get("ema20", 0) or 0)
    ema50  = float(row.get("ema50", 0) or 0)
    ema200 = float(row.get("ema200", 0) or 0)
    macd_h = float(row.get("macd_hist", 0) or 0)
    close  = float(row["close"])
    vol_r  = float(row.get("vol_ratio", 1) or 1)
    bb_low = float(row.get("bb_lower", 0) or 0)
    bb_up  = float(row.get("bb_upper", 0) or 0)

    regime    = detect_market_regime(df)
    rsi_div   = detect_rsi_divergence(df)
    candle    = detect_candle_pattern(df)

    long_score, short_score = 0.0, 0.0
    long_r, short_r = [], []

    # ── 1. IMOEX макро-фильтр ─────────────────────────────────────────────
    # Медвежий IMOEX → штраф к LONG (аналог BTC bear в старом боте)
    imoex_penalty_long  = 0.0
    imoex_penalty_short = 0.0
    if imoex_regime:
        ir = imoex_regime.get("regime", "neutral")
        if ir == "bull":
            long_score += 12
            long_r.append(f"IMOEX бычий (наклон {imoex_regime.get('slope_10d', 0):+.2f}%)")
        elif ir == "bear":
            imoex_penalty_long = -15.0   # штраф к лонгу в медвежьем рынке
            short_score += 10
            short_r.append(f"IMOEX медвежий — осторожность с LONG")

    # Интрадейный режим (5м/15м/1ч) vs позиционный (4ч/1д/1w)
    is_intraday = df.shape[0] > 0  # определяем по наличию VWAP
    vwap     = float(row.get("vwap", 0) or 0)
    vwap_dev = float(row.get("vwap_dev", 0) or 0)
    day_open = float(row.get("day_open", 0) or 0)
    ema9     = float(row.get("ema9", 0) or 0)
    has_vwap = vwap > 0

    # ── 2. VWAP (главный интрадей-индикатор) ──────────────────────────────
    if has_vwap:
        if close > vwap * 1.001:
            long_score += 22
            long_r.append(f"Цена выше VWAP (+{vwap_dev:.1f}%)")
        elif close < vwap * 0.999:
            short_score += 22
            short_r.append(f"Цена ниже VWAP ({vwap_dev:.1f}%)")
        # Пересечение VWAP снизу вверх — сильный сигнал
        prev_close = float(df.iloc[-2]["close"]) if len(df) > 1 else close
        if prev_close < vwap and close > vwap:
            long_score += 15
            long_r.append("Пробой VWAP снизу вверх ↑")
        elif prev_close > vwap and close < vwap:
            short_score += 15
            short_r.append("Пробой VWAP сверху вниз ↓")

    # ── 3. Уровень открытия дня ───────────────────────────────────────────
    if day_open > 0:
        if close > day_open * 1.002:
            long_score += 10
            long_r.append(f"Выше открытия дня ({(close/day_open-1)*100:+.1f}%)")
        elif close < day_open * 0.998:
            short_score += 10
            short_r.append(f"Ниже открытия дня ({(close/day_open-1)*100:+.1f}%)")

    # ── 4. Тренд EMA ──────────────────────────────────────────────────────
    if ema9 > 0 and close > ema9 > ema20:
        long_score += 18
        long_r.append("Цена > EMA9 > EMA20 (интрадей тренд)")
    elif ema9 > 0 and close < ema9 < ema20:
        short_score += 18
        short_r.append("Цена < EMA9 < EMA20")
    elif close > ema20 > ema50:
        long_score += 14
        long_r.append("Цена > EMA20 > EMA50")
    elif close < ema20 < ema50:
        short_score += 14
        short_r.append("Цена < EMA20 < EMA50")

    # EMA200 только на дневных+ (на 15м это ~200 часов, нет смысла)
    ema200 = float(row.get("ema200", 0) or 0)
    if ema200 > 0 and not has_vwap:
        if close > ema200:
            long_score += 8
            long_r.append("Выше EMA200")
        else:
            short_score += 8
            short_r.append("Ниже EMA200")

    # ── 5. RSI ────────────────────────────────────────────────────────────
    if rsi < mode_cfg["rsi_oversold"]:
        long_score += 13
        long_r.append(f"RSI {rsi:.0f} — перепроданность")
    elif rsi > mode_cfg["rsi_overbought"]:
        short_score += 13
        short_r.append(f"RSI {rsi:.0f} — перекупленность")
    elif 45 < rsi < 55:
        pass  # нейтральная зона — не считаем
    elif rsi > 50:
        long_score += 4
    else:
        short_score += 4

    # ── 6. MACD ───────────────────────────────────────────────────────────
    if macd_h > 0:
        long_score += 10
        long_r.append("MACD гист. > 0")
    elif macd_h < 0:
        short_score += 10
        short_r.append("MACD гист. < 0")

    # ── 7. Свечной паттерн ────────────────────────────────────────────────
    if "Бычье поглощение" in candle or ("пин-бар" in candle and "Бычий" in candle):
        long_score += 12
        long_r.append(candle)
    elif "Медвежье поглощение" in candle or ("пин-бар" in candle and "Медвежий" in candle):
        short_score += 12
        short_r.append(candle)

    # ── 8. Боллинджер ─────────────────────────────────────────────────────
    if bb_low > 0 and close < bb_low * 1.005:
        long_score += 7
        long_r.append("У нижней полосы BB")
    if bb_up > 0 and close > bb_up * 0.995:
        short_score += 7
        short_r.append("У верхней полосы BB")

    # ── 9. Объём ──────────────────────────────────────────────────────────
    if vol_r > 1.8:
        if close > (vwap if has_vwap else ema20):
            long_score += 10
            long_r.append(f"Объём x{vol_r:.1f} — бычий всплеск")
        else:
            short_score += 10
            short_r.append(f"Объём x{vol_r:.1f} — медвежий всплеск")
    elif vol_r > 1.3:
        if close > (vwap if has_vwap else ema20):
            long_score += 5
        else:
            short_score += 5

    # ── 10. RSI дивергенция (только на дневных TF) ────────────────────────
    if not has_vwap:  # нет VWAP = дневной+ TF
        rsi_div = detect_rsi_divergence(df)
        if rsi_div:
            if "Бычья" in rsi_div:
                long_score += 20
                long_r.append(rsi_div)
            elif "Медвежья" in rsi_div:
                short_score += 20
                short_r.append(rsi_div)

    # ── 9. Volume Profile (HVN/LVN/POC) ──────────────────────────────────
    # Предварительно определяем кандидата на сигнал для контекстной интерпретации
    _candidate = "🟩 LONG" if long_score >= short_score else "🟥 SHORT/ВЫХОД"
    if vp_nodes:
        vp_pts, vp_reasons = vp_score_adjustment(vp_nodes, close, _candidate)
        if "LONG" in _candidate:
            long_score  += vp_pts
            long_r.extend(vp_reasons)
        else:
            short_score += vp_pts
            short_r.extend(vp_reasons)

    # ── 10. Применяем штраф IMOEX ────────────────────────────────────────
    long_score  += imoex_penalty_long
    short_score += imoex_penalty_short

    # ── Финальный расчёт ──────────────────────────────────────────────────
    max_pts = 130.0   # увеличили из-за VP (+30) и IMOEX (+12)
    if long_score > short_score:
        score   = min(100, int(long_score / max_pts * 100))
        diff    = long_score - short_score
        signal  = "🟩 LONG" if diff >= 15 and score >= mode_cfg["min_score"] and len(long_r) >= 2 else "НЕТ СИГНАЛА"
        reasons = long_r
    else:
        score   = min(100, int(short_score / max_pts * 100))
        diff    = short_score - long_score
        signal  = "🟥 SHORT/ВЫХОД" if diff >= 15 and score >= mode_cfg["min_score"] and len(short_r) >= 2 else "НЕТ СИГНАЛА"
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
        return {"error": f"Тикер {ticker} не найден в списке. Доступны: {', '.join(MOEX_STOCKS.keys())}"}

    _, name, sector = MOEX_STOCKS[ticker]

    # Загружаем данные параллельно: свечи + новости + IMOEX
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
        logger.error(f"analyze_stock {ticker}: {e}")
        return {"error": str(e)}

    if df_result is None or len(df_result) < 30:
        return {"error": f"Недостаточно данных для {ticker} (нужно 30+ свечей на TF={tf})"}

    # Индикаторы
    df = calculate_indicators(df_result)
    df_closed = df.iloc[:-1].copy()  # только закрытые свечи
    price   = float(df_closed["close"].iloc[-1])
    atr     = float(df_closed["atr"].dropna().iloc[-1]) if "atr" in df_closed.columns else price * 0.02
    regime  = detect_market_regime(df_closed)
    supports, resistances = find_support_resistance(df_closed, price)

    # Volume Profile
    vp_nodes = find_hvn_lvn(df_closed, price)

    # Технический сигнал (с VP и IMOEX)
    tech_signal, tech_score, tech_reasons = compute_tech_score(
        df_closed, mode_cfg, vp_nodes=vp_nodes, imoex_regime=imoex_regime)

    # AI оценка новостей
    news_ai = await ai_evaluate_news(news_items, ticker, sector, tech_signal, tech_score)

    # IMOEX фильтр итогового сигнала
    # Если рынок медвежий — понижаем финальный сигнал LONG до предупреждения
    final_signal = news_ai.get("confirmed", tech_signal)
    if imoex_regime and imoex_regime.get("regime") == "bear" and "LONG" in final_signal:
        final_signal = f"⚠️ {final_signal} (IMOEX медвежий — повышен риск)"

    # SL/TP (с учётом VP-уровней как TP-якорей)
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

    # Скор-бар
    bars = "█" * (tscore // 10) + "░" * (10 - tscore // 10)

    lines = [
        f"📊 <b>{esc(ticker)} — {esc(name)}</b> | {esc(sector.upper())}",
        f"⏱ <b>{tf}</b>  |  💰 <b>{price:,.2f} ₽</b>  |  ATR {atr_pct:.2f}%  |  Объём x{vol_r:.1f}",
        "",
    ]

    # ── IMOEX макро ──────────────────────────────────────────────────────
    if imoex:
        ir_emoji = {"bull": "🟢", "bear": "🔴", "neutral": "⚪"}.get(imoex["regime"], "⚪")
        slope_arrow = "↑" if imoex["slope_10d"] > 0 else ("↓" if imoex["slope_10d"] < 0 else "→")
        lines += [
            f"<b>🏛 {esc(imoex['label'])}</b>",
            f"   {imoex['price']:,.0f} п.  EMA20: {imoex['ema20']:,.0f}  EMA50: {imoex['ema50']:,.0f}  Наклон: {slope_arrow}{imoex['slope_10d']:+.2f}%",
            "",
        ]

    # ── Техника ───────────────────────────────────────────────────────────
    lines += [
        f"<b>🔧 ТЕХНИКА</b>",
        f"{ts}  Скор: {tscore}/100",
        f"<code>{bars}</code>",
        f"Причины: {esc(' + '.join(treasons[:3]))}",
        f"{esc(regime['label'])}",
    ]
    if rsi_div:
        lines.append(esc(rsi_div))
    if candle and candle != "Обычная свеча":
        lines.append(f"Свеча: {esc(candle)}")

    # ── Volume Profile ─────────────────────────────────────────────────────
    if vp:
        vp_lines = []
        if vp.get("poc"):
            vp_lines.append(f"POC: <b>{vp['poc']:,.2f} ₽</b> (макс. объём)")
        if vp.get("hvn_above"):
            vp_lines.append(f"HVN↑: {vp['hvn_above']['price']:,.2f} ₽ ({vp['hvn_above']['strength']}x) — сопротивление/цель")
        if vp.get("hvn_below"):
            vp_lines.append(f"HVN↓: {vp['hvn_below']['price']:,.2f} ₽ ({vp['hvn_below']['strength']}x) — поддержка")
        if vp.get("lvn_above"):
            vp_lines.append(f"LVN↑: {vp['lvn_above']['price']:,.2f} ₽ — путь наверх чист")
        if vp.get("lvn_below"):
            vp_lines.append(f"LVN↓: {vp['lvn_below']['price']:,.2f} ₽ — путь вниз чист")
        if vp_lines:
            lines += ["", "<b>📊 VOLUME PROFILE</b>"] + vp_lines

    # ── Новостной фильтр (только качество, не меняет направление) ───────────
    lines += ["", "<b>📰 НОВОСТНОЙ ФИЛЬТР</b>"]
    fs      = news_ai.get("filter_status", "CONFIRMED")
    ew      = news_ai.get("event_weight", 0)
    ev      = news_ai.get("event_type", "нет значимых событий")
    summ    = news_ai.get("summary", "")
    block   = news_ai.get("blocking", [])
    fs_emoji = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀",
                "BLOCKED": "🚫", "NO_SIGNAL": "⚪", "NEWS_ONLY": "📢"}.get(fs, "⚪")
    ew_sign = f"+{ew}" if ew > 0 else str(ew)
    lines.append(f"{fs_emoji} <b>{fs}</b>  |  Вес события: {ew_sign}/10")
    if ev and ev != "нет значимых событий":
        lines.append(f"📎 Событие: {esc(ev)}")
    if summ:
        lines.append(f"💬 {esc(summ)}")
    if block:
        lines.append(f"🚫 Блокирующий фактор: {esc(', '.join(block))}")
    if news_ai.get("underreaction"):
        lines.append("⚠️ <b>Сильное событие — рынок мог не отреагировать полностью</b>")

    # ── Итог ─────────────────────────────────────────────────────────────
    lines += ["", f"<b>🎯 ИТОГ: {esc(final)}</b>"]

    # ── Уровни ───────────────────────────────────────────────────────────
    if sl_tp:
        lines += [
            "",
            "<b>📐 УРОВНИ</b>",
            f"SL:  {sl_tp['sl']:,.2f} ₽  ({sl_tp['risk_pct']:.1f}% риск)",
            f"TP1: {sl_tp['tp1']:,.2f} ₽",
            f"TP2: {sl_tp['tp2']:,.2f} ₽  (R/R {sl_tp['rr_ratio']:.1f})",
            f"TP3: {sl_tp['tp3']:,.2f} ₽",
        ]
        if sl_tp.get("warn"):
            lines.append(sl_tp["warn"])

    # ── Новости: факты отдельно от мнений ────────────────────────────────
    fact_news    = [it for it in news if it.get("is_fact")]
    neutral_news = [it for it in news if not it.get("is_fact") and not it.get("is_opinion")]
    skip_count   = news_ai.get("opinions_skipped", 0)

    if fact_news:
        lines += ["", "📋 <b>Корпоративные события:</b>"]
        for it in fact_news[:4]:
            w = it.get("weight", 0)
            w_str = f"+{w}" if w > 0 else str(w)
            w_e = "🟢" if w > 2 else ("🔴" if w < -2 else "⚪")
            lines.append(f"{w_e} [{w_str}] {esc(it['title'][:100])}")
    elif neutral_news:
        lines += ["", "📌 <b>Релевантные новости:</b>"]
        for it in neutral_news[:3]:
            sp = "🔵" if it.get("is_specific") else "⚪"
            lines.append(f"{sp} {esc(it['title'][:100])}")
    else:
        lines += ["", "📭 <i>Фактических событий не найдено</i>"]
    if skip_count > 0:
        lines.append(f"<i>↳ отфильтровано мнений/обзоров: {skip_count}</i>")

    lines += [
        "",
        f"<i>⏰ {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК</i>",
        "<i>⚠️ Только чтение данных. Не является инвестиционной рекомендацией.</i>",
    ]

    return "\n".join(lines)

# ══════════════════════════════════════════════
# СОСТОЯНИЕ ПОЛЬЗОВАТЕЛЕЙ (режим, TF)
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
        "🏛 <b>MOEX Signal Bot — интрадей 15м</b>\n"
        "Акции 1-2 эшелона МосБиржи с плечом\n\n"
        "<b>📊 Анализ:</b>\n"
        "/analyze SBER — полный анализ (техника + VWAP + новости)\n"
        "/analyze SBER 15m — указать таймфрейм явно\n"
        "Просто напиши тикер: <code>SBER</code> — быстрый анализ\n\n"
        "<b>🗂 Ватчлист (до 100 инструментов):</b>\n"
        "/watchlist — мой список\n"
        "/add SBER — добавить тикер\n"
        "/remove SBER — убрать тикер\n"
        "/add_sector нефтегаз — добавить весь сектор\n"
        "/all_tickers — все доступные (68 шт.)\n"
        "/clear_watchlist confirm — очистить всё\n\n"
        "<b>🔍 Сканер:</b>\n"
        "/scan — сканировать мой ватчлист прямо сейчас\n"
        "/scan_start — авторассылка сигналов каждые 2ч\n"
        "/scan_stop — выключить авторассылку\n\n"
        "<b>📰 Новости:</b>\n"
        "/news SBER — корпоративные события\n"
        "/market — обзор рынка\n\n"
        "<b>⚙️ Настройки:</b>\n"
        "/mode — режим LOW / MID / HARD\n"
        "/tf — таймфрейм (15m по умолчанию)\n"
        "/trades — мои позиции\n\n"
        "<i>Данные: Tinkoff Invest API (read-only)</i>"
    )
    await update.message.reply_text(text, parse_mode="HTML")

async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    wl = load_watchlist()
    lines = [f"🗂 <b>Мой ватчлист</b>  ({len(wl)}/100)\n"]
    for t in wl:
        info = MOEX_STOCKS.get(t)
        if info:
            _, name, sector = info
            lines.append(f"<code>{t}</code> — {name} <i>({sector})</i>")
        else:
            lines.append(f"<code>{t}</code>")
    lines += [
        "",
        "➕ /add TICKER — добавить",
        "➖ /remove TICKER — убрать",
        "/clear_watchlist — очистить весь список",
        "",
        f"<i>Всего доступно {len(MOEX_STOCKS)} инструментов. /all_tickers — полный список</i>",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_all_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает все доступные тикеры по секторам."""
    sector_map: dict[str, list] = {}
    for ticker, (_, name, sector) in MOEX_STOCKS.items():
        sector_map.setdefault(sector, []).append(f"<code>{ticker}</code> {name}")
    lines = ["📋 <b>Все доступные инструменты</b>\n"]
    for sector, items in sorted(sector_map.items()):
        lines.append(f"\n<b>{sector.capitalize()}</b>")
        lines.extend(items)
    lines.append("\n<i>Добавить в ватчлист: /add TICKER</i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет тикер в личный ватчлист."""
    if not context.args:
        await update.message.reply_text(
            "Использование: /add TICKER\nПример: /add SBER\n\n"
            "Все доступные тикеры: /all_tickers",
            parse_mode="HTML")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = add_to_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Удаляет тикер из личного ватчлиста."""
    if not context.args:
        await update.message.reply_text(
            "Использование: /remove TICKER\nПример: /remove SBER\n\n"
            "Мой ватчлист: /watchlist",
            parse_mode="HTML")
        return
    ticker = context.args[0].upper().strip()
    ok, msg = remove_from_watchlist(ticker)
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_add_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет все доступные инструменты в ватчлист."""
    all_tickers = list(MOEX_STOCKS.keys())
    added, skipped = [], []
    for t in all_tickers:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)
    wl = load_watchlist()
    await update.message.reply_text(
        f"✅ Добавлено: {len(added)}\n"
        f"ℹ️ Уже были: {len(skipped)}\n"
        f"📋 Итого в ватчлисте: {len(wl)} инструментов\n\n"
        f"Запустить сканер: /scan",
        parse_mode="HTML")


async def cmd_clear_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Полностью очищает ватчлист."""
    args = context.args
    if not args or args[0].lower() != "confirm":
        await update.message.reply_text(
            "⚠️ Это удалит <b>весь</b> ватчлист.\n"
            "Для подтверждения: /clear_watchlist confirm",
            parse_mode="HTML")
        return
    save_watchlist([])
    await update.message.reply_text("🗑 Ватчлист очищен. Добавить инструменты: /add TICKER")


async def cmd_add_sector(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Добавляет все инструменты определённого сектора в ватчлист.
    Использование: /add_sector нефтегаз
    Без аргумента — показывает список секторов.
    """
    if not context.args:
        sectors = sorted(set(v[2] for v in MOEX_STOCKS.values()))
        lines = ["📂 <b>Доступные секторы:</b>\n"]
        for s in sectors:
            tickers = [t for t, v in MOEX_STOCKS.items() if v[2] == s]
            lines.append(f"<code>/add_sector {s}</code> — {', '.join(tickers)}")
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")
        return

    sector_query = " ".join(context.args).lower().strip()
    to_add = [t for t, v in MOEX_STOCKS.items() if v[2].lower() == sector_query]
    if not to_add:
        # Нечёткий поиск
        to_add = [t for t, v in MOEX_STOCKS.items() if sector_query in v[2].lower()]

    if not to_add:
        await update.message.reply_text(
            f"❌ Сектор '{sector_query}' не найден.\nСписок секторов: /add_sector",
            parse_mode="HTML")
        return

    added, skipped = [], []
    for t in to_add:
        ok, _ = add_to_watchlist(t)
        (added if ok else skipped).append(t)

    lines = [f"✅ Добавлены ({len(added)}): {', '.join(added)}"] if added else []
    if skipped:
        lines.append(f"ℹ️ Уже в списке или лимит: {', '.join(skipped)}")
    wl = load_watchlist()
    lines.append(f"\nВатчлист: {len(wl)}/100 инструментов")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text(
            "Использование: /analyze TICKER [tf]\nПример: /analyze SBER 1d",
            parse_mode="HTML")
        return

    ticker = args[0].upper()
    tf     = args[1].lower() if len(args) > 1 else get_user_state(update.effective_chat.id)["tf"]
    if tf not in TF_MAP:
        tf = DEFAULT_TF

    msg = await update.message.reply_text(f"⏳ Анализирую <b>{ticker}</b> [{tf}]...", parse_mode="HTML")
    mode_cfg = TRADE_MODES[get_user_state(update.effective_chat.id)["mode"]]

    result = await analyze_stock(ticker, tf, mode_cfg)
    text   = format_analysis(result)

    # Кнопки действий
    kb = []
    if "error" not in result:
        kb.append([
            InlineKeyboardButton("📰 Новости",  callback_data=f"news_{ticker}"),
            InlineKeyboardButton("5м",          callback_data=f"analyze_{ticker}_5m"),
            InlineKeyboardButton("15м ✓",       callback_data=f"analyze_{ticker}_15m"),
            InlineKeyboardButton("1ч",          callback_data=f"analyze_{ticker}_1h"),
        ])
        kb.append([
            InlineKeyboardButton("1д (контекст)", callback_data=f"analyze_{ticker}_1d"),
            InlineKeyboardButton("➕ В ватчлист",  callback_data=f"wl_add_{ticker}"),
        ])
        final = result.get("final_signal", "")
        if "LONG" in final or "SHORT" in final:
            kb.append([
                InlineKeyboardButton("✅ Записать сделку", callback_data=f"trade_{ticker}")
            ])

    markup = InlineKeyboardMarkup(kb) if kb else None
    await msg.edit_text(text, parse_mode="HTML", reply_markup=markup)

async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args   = context.args
    ticker = args[0].upper() if args else ""
    sector = ""
    if ticker in MOEX_STOCKS:
        _, _, sector = MOEX_STOCKS[ticker]

    msg = await update.message.reply_text(
        f"⏳ Загружаю новости{'для ' + ticker if ticker else ''}...", parse_mode="HTML")

    news = await fetch_russian_news(ticker, sector)

    if not news:
        await msg.edit_text("📭 Свежих релевантных новостей не найдено.")
        return

    # AI оценка каждой новости
    lines = [f"📰 <b>Новости{' — ' + ticker if ticker else ' — Рынок'}</b>\n"]
    for it in news[:8]:
        sp = "🔵" if it.get("is_specific") else "⚪"
        lines.append(f"{sp} <b>{it['title'][:120]}</b>")
        lines.append(f"   └ {it['source']} {it['pub'][:16]}")
        if len(it.get("matched", [])) <= 3:
            ai = await ai_classify_news_impact(it["title"], ticker or "IMOEX")
            s_e = {"позитив": "🟢", "негатив": "🔴", "нейтрально": "⚪"}.get(ai["sentiment"], "⚪")
            lines.append(f"   └ {s_e} {ai['sentiment']} {ai['score']}/10")
        lines.append("")

    lines.append(f"<i>⏰ {datetime.now().strftime('%d.%m.%Y %H:%M')}</i>")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def cmd_market(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = await update.message.reply_text("⏳ Загружаю обзор рынка...", parse_mode="HTML")

    # Параллельно: новости + несколько ключевых индексных бумаг
    tasks = [
        fetch_market_news(),
        analyze_stock("SBER", "1d", TRADE_MODES["mid"]),
        analyze_stock("GAZP", "1d", TRADE_MODES["mid"]),
        analyze_stock("LKOH", "1d", TRADE_MODES["mid"]),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    news    = results[0] if not isinstance(results[0], Exception) else []
    stocks  = [r for r in results[1:] if not isinstance(r, Exception) and r and "error" not in r]

    lines = ["🏛 <b>ОБЗОР РЫНКА MOEX</b>", f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')}</i>\n"]

    if stocks:
        lines.append("<b>📊 Ключевые бумаги (1d):</b>")
        for s in stocks:
            sig_e = {"🟩 LONG": "🟢", "🟥 SHORT/ВЫХОД": "🔴"}.get(s["tech_signal"], "⚪")
            lines.append(
                f"{sig_e} <b>{s['ticker']}</b> {s['price']:,.2f}₽  "
                f"ATR{s['atr_pct']:.1f}%  {s['regime']['label']}  "
                f"Скор:{s['tech_score']}"
            )
        lines.append("")

    if news:
        lines.append("<b>📰 Рыночные новости:</b>")
        for it in news[:6]:
            sp = "🔵" if it.get("is_specific") else "⚪"
            lines.append(f"{sp} {esc(it['title'][:100])}")
        lines.append("")

    lines.append("<i>Используй /analyze TICKER для детального анализа</i>")
    await msg.edit_text("\n".join(lines), parse_mode="HTML")

async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id  = update.effective_chat.id
    mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
    tf       = get_user_state(chat_id)["tf"]

    msg = await update.message.reply_text(
        f"🔍 Сканирую {len(MOEX_STOCKS)} инструментов [{tf}]...\n"
        "Это займёт ~1-2 минуты.", parse_mode="HTML")

    signals = []
    # Сканируем батчами по 5 чтобы не перегружать API
    tickers = list(MOEX_STOCKS.keys())
    for i in range(0, len(tickers), 5):
        batch = tickers[i:i+5]
        tasks = [analyze_stock(t, tf, mode_cfg) for t in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in batch_results:
            if isinstance(r, Exception) or not r or "error" in r:
                continue
            if r["tech_signal"] != "НЕТ СИГНАЛА" or r["tech_score"] >= 55:
                signals.append(r)
        await asyncio.sleep(0.5)  # пауза между батчами

    signals.sort(key=lambda x: -x["tech_score"])

    if not signals:
        await msg.edit_text("😶 Нет значимых сигналов. Рынок в ожидании.")
        return

    lines = [
        f"🔍 <b>СКАНЕР MOEX</b> | {mode_cfg['label']} | {tf}",
        f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')} | Сигналов: {len(signals)}</i>\n",
    ]

    long_sigs  = [s for s in signals if s["tech_signal"] == "🟩 LONG"]
    short_sigs = [s for s in signals if s["tech_signal"] == "🟥 SHORT/ВЫХОД"]
    watch_sigs = [s for s in signals if s["tech_signal"] == "НЕТ СИГНАЛА"]

    if long_sigs:
        lines.append("🟩 <b>ПОКУПКА:</b>")
        for s in long_sigs[:5]:
            news_score = s["news_ai"]["score"]
            news_e = "🟢" if s["news_ai"]["sentiment"] == "позитив" else (
                     "🔴" if s["news_ai"]["sentiment"] == "негатив" else "⚪")
            lines.append(
                f"  <b>{s['ticker']}</b> {s['price']:,.1f}₽  "
                f"Скор:{s['tech_score']}  Новости:{news_e}{news_score}/10\n"
                f"  {s['regime']['label']}  {' | '.join(s['tech_reasons'][:2])}"
            )
        lines.append("")

    if short_sigs:
        lines.append("🟥 <b>ПРОДАЖА/ВЫХОД:</b>")
        for s in short_sigs[:5]:
            lines.append(
                f"  <b>{s['ticker']}</b> {s['price']:,.1f}₽  "
                f"Скор:{s['tech_score']}"
            )
        lines.append("")

    if watch_sigs:
        lines.append("👀 <b>На радаре (накопление):</b>")
        for s in watch_sigs[:5]:
            lines.append(f"  <b>{s['ticker']}</b> {s['price']:,.1f}₽  Скор:{s['tech_score']}")

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
        f"⚙️ Текущий режим: <b>{TRADE_MODES[cur_mode]['label']}</b>\n\n"
        "LOW — только очень чёткие сигналы\n"
        "MID — сбалансированный режим\n"
        "HARD — агрессивный, ловит больше входов",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )

async def cmd_tf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    cur_tf  = get_user_state(chat_id)["tf"]
    tfs     = list(TF_MAP.keys())
    kb = [[
        InlineKeyboardButton(
            f"{'✅ ' if t == cur_tf else ''}{t}",
            callback_data=f"tf_{t}"
        ) for t in tfs
    ]]
    await update.message.reply_text(
        f"⏱ Текущий таймфрейм: <b>{cur_tf}</b>\n"
        "1h — внутридневной | 4h — среднесрочный | 1d — дневной | 1w — недельный",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(kb),
    )

async def cmd_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = load_trades()
    if not trades:
        await update.message.reply_text("📭 Нет открытых позиций.")
        return
    lines = ["📂 <b>Открытые позиции</b>\n"]
    for key, t in trades.items():
        lines.append(
            f"<b>{t['ticker']}</b> {t['signal']}  Вход: {t['entry']:,.2f}₽\n"
            f"SL: {t['sl']:,.2f}  TP1: {t['tp1']:,.2f}  TP2: {t['tp2']:,.2f}\n"
            f"TF: {t['tf']}  Открыто: {t['opened_at'][:10]}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# ══════════════════════════════════════════════
# CALLBACK HANDLERS
# ══════════════════════════════════════════════
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    if data.startswith("mode_"):
        mode = data.split("_")[1]
        set_user_state(chat_id, mode=mode)
        await query.edit_message_text(
            f"✅ Режим установлен: <b>{TRADE_MODES[mode]['label']}</b>",
            parse_mode="HTML")

    elif data.startswith("tf_"):
        tf = data.split("_")[1]
        set_user_state(chat_id, tf=tf)
        await query.edit_message_text(
            f"✅ Таймфрейм установлен: <b>{tf}</b>", parse_mode="HTML")

    elif data.startswith("news_"):
        ticker = data.split("_")[1]
        await query.edit_message_text(f"⏳ Загружаю новости для {ticker}...", parse_mode="HTML")
        sector = MOEX_STOCKS.get(ticker, ("", "", ""))[2]
        news = await fetch_russian_news(ticker, sector)
        lines = [f"📰 <b>Новости — {ticker}</b>\n"]
        for it in news[:6]:
            sp = "🔵" if it.get("is_specific") else "⚪"
            lines.append(f"{sp} {it['title'][:120]}")
            ai = await ai_classify_news_impact(it["title"], ticker)
            s_e = {"позитив": "🟢", "негатив": "🔴", "нейтрально": "⚪"}.get(ai["sentiment"], "⚪")
            lines.append(f"   └ {s_e} {ai['sentiment']} {ai['score']}/10\n")
        await query.edit_message_text("\n".join(lines), parse_mode="HTML")

    elif data.startswith("analyze_"):
        parts  = data.split("_")
        ticker = parts[1]
        tf     = parts[2] if len(parts) > 2 else DEFAULT_TF
        await query.edit_message_text(f"⏳ Анализирую {ticker} [{tf}]...", parse_mode="HTML")
        mode_cfg = TRADE_MODES[get_user_state(chat_id)["mode"]]
        result = await analyze_stock(ticker, tf, mode_cfg)
        text   = format_analysis(result)
        await query.edit_message_text(text, parse_mode="HTML")

    elif data.startswith("wl_add_"):
        ticker = data.split("_", 2)[2]
        ok, msg = add_to_watchlist(ticker)
        await query.answer(msg.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","")[:200], show_alert=True)

    elif data.startswith("trade_"):
        ticker = data.split("_")[1]
        await query.edit_message_text(
            f"✅ Позиция <b>{ticker}</b> записана в /trades\n"
            "(Добавь вручную через /trade после подтверждения входа)",
            parse_mode="HTML")

# ══════════════════════════════════════════════
# ФОНОВЫЙ СКАНЕР (авторассылка сигналов)
# ══════════════════════════════════════════════
SCANNER_CHAT_IDS: list[int] = []  # чаты с активной авторассылкой

async def cmd_scan_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подписывает чат на авторассылку сигналов каждые 2 часа в торговое время."""
    chat_id = update.effective_chat.id
    if chat_id not in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.append(chat_id)
        await update.message.reply_text(
            "✅ <b>Авто-сканер включён</b>\n\n"
            "Буду присылать сигналы каждые 2 часа в торговое время МосБиржи "
            "(пн–пт, 10:00–18:40 МСК).\n\n"
            "Выключить: /scan_stop",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("ℹ️ Авто-сканер уже включён. Выключить: /scan_stop")

async def cmd_scan_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отписывает чат от авторассылки."""
    chat_id = update.effective_chat.id
    if chat_id in SCANNER_CHAT_IDS:
        SCANNER_CHAT_IDS.remove(chat_id)
        await update.message.reply_text("🔕 Авто-сканер отключён. Включить снова: /scan_start")
    else:
        await update.message.reply_text("ℹ️ Авто-сканер и так выключен.")

async def scanner_loop(app):
    """Фоновый цикл сканера. Запускается каждые 2 часа в торговое время."""
    while True:
        now = datetime.now()
        # MOEX торгует пн-пт 10:00-18:40 МСК (UTC+3)
        is_trading_day  = now.weekday() < 5
        is_trading_time = 7 <= now.hour < 16  # UTC ≈ 10-19 МСК

        if is_trading_day and is_trading_time and SCANNER_CHAT_IDS:
            try:
                await run_scanner_broadcast(app)
            except Exception as e:
                logger.error(f"scanner_loop: {e}")

        # Следующая итерация через 2 часа
        await asyncio.sleep(7200)

async def run_scanner_broadcast(app):
    """Рассылает сигналы подписанным чатам."""
    mode_cfg = TRADE_MODES["mid"]
    tf = "1d"
    signals = []

    for ticker in list(MOEX_STOCKS.keys()):
        try:
            r = await analyze_stock(ticker, tf, mode_cfg)
            if r and "error" not in r and r["tech_signal"] != "НЕТ СИГНАЛА":
                signals.append(r)
        except Exception as e:
            logger.warning(f"scanner {ticker}: {e}")
        await asyncio.sleep(0.3)

    if not signals:
        return

    signals.sort(key=lambda x: -x["tech_score"])
    lines = [
        "🔔 <b>АВТО-СКАНЕР MOEX</b>",
        f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')}</i>\n",
    ]
    for s in signals[:6]:
        na  = s["news_ai"]
        fs  = na.get("filter_status", "CONFIRMED")
        ew  = na.get("event_weight", 0)
        fs_e = {"CONFIRMED": "✅", "WEAK": "🟡", "WATCH": "👀", "BLOCKED": "🚫", "NEWS_ONLY": "📢"}.get(fs, "⚪")
        ew_s = f"+{ew}" if ew > 0 else str(ew)
        lines.append(
            f"{s['tech_signal']} <b>{s['ticker']}</b>  {s['price']:,.1f}₽\n"
            f"Скор: {s['tech_score']}/100  {fs_e} {fs}  Вес: {ew_s}\n"
            f"Итог: {na['confirmed']}\n"
        )

    text = "\n".join(lines)
    for chat_id in SCANNER_CHAT_IDS:
        try:
            await app.bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"broadcast {chat_id}: {e}")

# ══════════════════════════════════════════════
# ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ (тикер напрямую)
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
        # Поиск по частичному совпадению
        matches = [t for t in MOEX_STOCKS if t.startswith(text[:3])]
        if matches:
            await update.message.reply_text(
                f"❓ Тикер <code>{text}</code> не найден.\n"
                f"Похожие: {', '.join(f'<code>{m}</code>' for m in matches[:5])}",
                parse_mode="HTML")

# ══════════════════════════════════════════════
# ЗАПУСК
# ══════════════════════════════════════════════
async def post_init(app):
    asyncio.create_task(scanner_loop(app))
    logger.info("🚀 MOEX Signal Bot запущен")

def main():
    # On Railway env vars come from Variables dashboard, not .env
    if not TELEGRAM_TOKEN:
        raise RuntimeError(
            "TELEGRAM_TOKEN не задан. "
            "На Railway: Settings → Variables → Add Variable."
        )
    if not TINKOFF_TOKEN:
        logger.warning(
            "TINKOFF_TOKEN не задан — данные свечей недоступны. "
            "Добавь в Railway Variables."
        )
    if not GROQ_API_KEY:
        logger.warning(
            "GROQ_API_KEY не задан — AI-анализ отключён. "
            "Добавь в Railway Variables."
        )

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

    # Polling — правильный режим для Railway (не webhook).
    # Railway не гарантирует постоянный URL, webhook не нужен.
    logger.info("MOEX Signal Bot запущен (polling, Railway)")
    app.run_polling(
        allowed_updates=["message", "callback_query"],
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
