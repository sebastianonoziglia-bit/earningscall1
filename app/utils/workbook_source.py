from __future__ import annotations

import logging
import os
import re
import tempfile
import time
import io
import zipfile
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests


DEFAULT_GOOGLE_SHEET_ID = "10pOfzRRd0Mhbb_jq_fQRCqNr7N_R2KUpBBNsUuo5sxs"
DEFAULT_GOOGLE_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{DEFAULT_GOOGLE_SHEET_ID}/edit"
DEFAULT_FINANCIAL_DATA_SOURCE = "google"
EXPECTED_WORKBOOK_MIN_SHEET_COUNT = 30  # kept for reference, no longer used in primary resolution
REQUIRED_WORKBOOK_SHEETS = {
    "Company_metrics_earnings_values",
}
logger = logging.getLogger(__name__)

# ── Single shared cache path ────────────────────────────────────────────
_CACHE_DIR = Path(tempfile.gettempdir()) / "replit_revival_data"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_SHARED_CACHE = _CACHE_DIR / "workbook.xlsx"
_DEFAULT_CACHE_TTL = 14400  # 4 hours


def extract_google_sheet_id(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None

    # Handles full URLs like:
    # https://docs.google.com/spreadsheets/d/<ID>/edit?gid=...
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", text)
    if match:
        return match.group(1)

    # Accept raw ID as input.
    if re.fullmatch(r"[a-zA-Z0-9_-]{20,}", text):
        return text

    return None


def _is_valid_xlsx_payload(content_type: str, body: bytes) -> bool:
    ct = str(content_type or "").lower()
    if "spreadsheetml" in ct or "application/vnd.ms-excel" in ct:
        return True
    # XLSX is a zip container.
    return body.startswith(b"PK")


def _is_valid_xlsx_bytes(body: bytes) -> bool:
    if not body or len(body) < 512 or not body.startswith(b"PK"):
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            names = set(zf.namelist())
        return "[Content_Types].xml" in names and "xl/workbook.xml" in names
    except Exception:
        return False


def _is_valid_xlsx_file(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        if path.stat().st_size < 512:
            return False
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
        return "[Content_Types].xml" in names and "xl/workbook.xml" in names
    except Exception:
        return False


def _cache_age(path: Path) -> float:
    """Return age of file in seconds, or infinity if missing."""
    if not path.exists():
        return float("inf")
    return time.time() - path.stat().st_mtime


def _download_once(ttl: int = _DEFAULT_CACHE_TTL) -> Optional[str]:
    """Download Google Sheet ONCE and cache it. All callers share this cache.

    Returns path to valid xlsx, or None.
    """
    # Return cache if still fresh
    if _cache_age(_SHARED_CACHE) <= ttl and _is_valid_xlsx_file(_SHARED_CACHE):
        logger.info("Using cached workbook (age %.0fs, ttl %ds)", _cache_age(_SHARED_CACHE), ttl)
        return str(_SHARED_CACHE)

    # Resolve which sheet to download
    gsheet_url = os.getenv("GOOGLE_SHEET_URL", "").strip()
    if not gsheet_url:
        gsheet_url = f"https://docs.google.com/spreadsheets/d/{DEFAULT_GOOGLE_SHEET_ID}/export?format=xlsx"

    # If URL doesn't end with export format, convert it
    if "/export?" not in gsheet_url:
        sheet_id = extract_google_sheet_id(gsheet_url)
        if sheet_id:
            gsheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

    # Download with retries
    for attempt in range(3):
        try:
            logger.info("Downloading workbook (attempt %d/3, timeout=120s)...", attempt + 1)
            resp = requests.get(gsheet_url, timeout=120)
            if resp.status_code == 200 and _is_valid_xlsx_bytes(resp.content):
                tmp = _CACHE_DIR / f".download.{int(time.time() * 1000)}.tmp"
                tmp.write_bytes(resp.content)
                os.replace(tmp, _SHARED_CACHE)
                logger.info("Workbook downloaded and cached (%d bytes)", len(resp.content))
                return str(_SHARED_CACHE)
            else:
                logger.warning("Download returned status=%s size=%d", resp.status_code, len(resp.content))
        except Exception as exc:
            logger.warning("Download attempt %d/3 failed: %s", attempt + 1, exc)
        if attempt < 2:
            time.sleep(10)

    # All retries failed — return stale cache if valid
    if _is_valid_xlsx_file(_SHARED_CACHE):
        logger.warning("All download attempts failed — using stale cache (age %.0fs)", _cache_age(_SHARED_CACHE))
        return str(_SHARED_CACHE)

    logger.error("No valid workbook available (download failed, no cache)")
    return None


def _has_expected_workbook_tabs(path: str | Path | None) -> bool:
    if not path:
        return False
    p = Path(path)
    if not p.exists() or not _is_valid_xlsx_file(p):
        return False
    try:
        xls = pd.ExcelFile(p)
        names = {str(name).strip() for name in xls.sheet_names}
    except Exception:
        return False

    if len(names) < int(EXPECTED_WORKBOOK_MIN_SHEET_COUNT):
        return False
    return REQUIRED_WORKBOOK_SHEETS.issubset(names)


def _has_core_financial_coverage(path: str | Path | None) -> bool:
    """Validate that a workbook contains a usable yearly metrics backbone."""
    if not path:
        return False
    p = Path(path)
    if not p.exists() or not _is_valid_xlsx_file(p):
        return False
    try:
        df = pd.read_excel(
            p,
            sheet_name="Company_metrics_earnings_values",
            usecols=["Company", "Year"],
        )
    except Exception:
        return False
    if df is None or df.empty:
        return False
    cols = {str(c).strip().lower() for c in df.columns}
    if "company" not in cols or "year" not in cols:
        return False
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    company_col = next((c for c in df.columns if str(c).strip().lower() == "company"), None)
    year_col = next((c for c in df.columns if str(c).strip().lower() == "year"), None)
    if not company_col or not year_col:
        return False
    df[company_col] = df[company_col].astype(str).str.strip()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
    df = df.dropna(subset=[company_col, year_col])
    if df.empty:
        return False
    unique_companies = df[company_col].nunique()
    unique_years = df[year_col].astype(int).nunique()
    if unique_companies < 5 or unique_years < 8:
        return False
    year_min = int(df[year_col].min())
    return year_min <= 2015


def _should_refresh(dest: Path, max_age_seconds: int = 900) -> bool:
    if not dest.exists():
        return True
    return (time.time() - dest.stat().st_mtime) > max_age_seconds


def resolve_financial_data_xlsx(local_candidates: Iterable[str] | None = None) -> Optional[str]:
    """Return path to a valid financial data XLSX.

    Uses single shared download cache — no duplicate downloads.
    """
    # Try shared cache / download
    result = _download_once(ttl=_DEFAULT_CACHE_TTL)
    if result:
        return result

    # Fallback: any existing xlsx in attached_assets
    attached_assets = Path(__file__).resolve().parent.parent / "attached_assets"
    attached_assets.mkdir(parents=True, exist_ok=True)
    candidates = list(attached_assets.glob("*.xlsx"))
    if local_candidates:
        candidates += [Path(p) for p in local_candidates]
    for p in candidates:
        if p.name == "live_data.xlsx":
            continue  # skip old download artifacts
        if _is_valid_xlsx_file(p):
            logger.info("WORKBOOK_RESOLVE fallback local file: %s", p)
            return str(p)

    logger.error("WORKBOOK_RESOLVE no valid XLSX found")
    return None


def get_workbook_source_stamp(path: str | None) -> int:
    if not path:
        return 0
    try:
        return int(os.stat(path).st_mtime_ns)
    except OSError:
        return 0


def get_live_data_xlsx(refresh_seconds: int = 3600) -> Optional[str]:
    """Return a path to a freshly-downloaded Google Sheets XLSX for live sheets
    (Minute, Daily, Holders). Uses same shared cache as resolve_financial_data_xlsx.
    """
    # Use the shared cache with the caller's TTL
    ttl = max(int(refresh_seconds), 600)  # minimum 10 min
    return _download_once(ttl=ttl)
