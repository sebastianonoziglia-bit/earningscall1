from __future__ import annotations

import logging
import re
import time
from typing import Dict

import pandas as pd


logger = logging.getLogger(__name__)

_ALIAS_TO_COMPANY = {
    "alphabet": "Alphabet",
    "google": "Alphabet",
    "googl": "Alphabet",
    "goog": "Alphabet",
    "apple": "Apple",
    "aapl": "Apple",
    "meta platforms": "Meta Platforms",
    "meta": "Meta Platforms",
    "fb": "Meta Platforms",
    "microsoft": "Microsoft",
    "msft": "Microsoft",
    "amazon": "Amazon",
    "amzn": "Amazon",
    "netflix": "Netflix",
    "nflx": "Netflix",
    "disney": "Disney",
    "dis": "Disney",
    "comcast": "Comcast",
    "cmcsa": "Comcast",
    "warner bros": "Warner Bros. Discovery",
    "warner bros. discovery": "Warner Bros. Discovery",
    "wbd": "Warner Bros. Discovery",
    "paramount global": "Paramount Global",
    "paramount": "Paramount Global",
    "para": "Paramount Global",
    "spotify": "Spotify",
    "spot": "Spotify",
    "roku": "Roku",
    "roku inc": "Roku",
    "s&p 500": "S&P 500",
    "sp500": "S&P 500",
    "nasdaq": "Nasdaq",
    "gold": "Gold",
    "bitcoin": "Bitcoin",
}


def live_feed_cache_bucket(seconds: int = 300) -> int:
    seconds = max(int(seconds or 300), 60)
    return int(time.time() // seconds)


def _empty_feed_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "price", "volume", "asset", "tag"])


def _clean_ticker(text: str) -> str:
    value = re.sub(r"[^A-Z0-9.-]+", "", str(text or "").strip().upper())
    if value in {"", "NONE", "NAN", "NULL"}:
        return ""
    if len(value) > 10:
        return ""
    if not any(ch.isalpha() for ch in value):
        return ""
    return value


def load_live_stock_feed(cache_bucket: int = 0, url: str = "") -> pd.DataFrame:
    del cache_bucket, url
    logger.warning(
        "load_live_stock_feed() is deprecated and disabled. "
        "Use workbook tabs Daily/Minute via utils.workbook_market_data instead."
    )
    return _empty_feed_frame()


def merge_with_live_stock_feed(base_df: pd.DataFrame, cache_bucket: int = 0) -> pd.DataFrame:
    del cache_bucket
    logger.warning(
        "merge_with_live_stock_feed() is deprecated and disabled. "
        "Use utils.workbook_market_data.load_combined_stock_market_data() instead."
    )
    if base_df is None:
        return _empty_feed_frame()
    if base_df.empty:
        return _empty_feed_frame()
    out = base_df.copy()
    for col in ["date", "price", "volume", "asset", "tag"]:
        if col not in out.columns:
            out[col] = None if col == "volume" else ""
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["asset"] = out["asset"].fillna("").astype(str).str.strip()
    out["tag"] = out["tag"].fillna("").astype(str).str.strip().str.upper().apply(_clean_ticker)
    out = out.dropna(subset=["date", "price"])
    out = out[out["asset"] != ""].copy()
    return out[["date", "price", "volume", "asset", "tag"]].sort_values("date").reset_index(drop=True)


def infer_company_label(asset: str, tag: str) -> str:
    asset_text = str(asset or "").strip()
    tag_text = str(tag or "").strip()
    combined = f"{asset_text} {tag_text}".strip().lower()
    for alias, company in _ALIAS_TO_COMPANY.items():
        if alias in combined:
            return company
    if asset_text and asset_text.upper() not in {"NAN", "NONE", "NULL"}:
        return asset_text
    ticker = _clean_ticker(tag_text)
    if ticker:
        return ticker
    return ""


def build_live_company_ticker_map(cache_bucket: int = 0) -> Dict[str, str]:
    del cache_bucket
    logger.warning(
        "build_live_company_ticker_map() is deprecated and disabled. "
        "Use utils.workbook_market_data.build_company_ticker_map_from_market_data() instead."
    )
    return {}
