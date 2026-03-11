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


def _download_google_sheet_xlsx(sheet_id: str, refresh_seconds: int = 60) -> Optional[str]:
    cache_dir = Path(tempfile.gettempdir()) / "replit_revival_data"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{sheet_id}.xlsx"

    if cache_file.exists():
        age_seconds = time.time() - cache_file.stat().st_mtime
        if age_seconds <= max(int(refresh_seconds), 30) and _is_valid_xlsx_file(cache_file):
            return str(cache_file)
        if not _is_valid_xlsx_file(cache_file):
            try:
                cache_file.unlink(missing_ok=True)
            except Exception:
                pass

    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    try:
        response = requests.get(export_url, timeout=20)
        response.raise_for_status()
        if not _is_valid_xlsx_payload(response.headers.get("content-type", ""), response.content):
            # Keep older cache, if present, when response is not a workbook (private/no-access pages often return HTML).
            return str(cache_file) if _is_valid_xlsx_file(cache_file) else None
        if not _is_valid_xlsx_bytes(response.content):
            return str(cache_file) if _is_valid_xlsx_file(cache_file) else None

        temp_file = cache_dir / f".{sheet_id}.{int(time.time() * 1000)}.tmp"
        temp_file.write_bytes(response.content)
        if not _is_valid_xlsx_file(temp_file):
            try:
                temp_file.unlink(missing_ok=True)
            except Exception:
                pass
            return str(cache_file) if _is_valid_xlsx_file(cache_file) else None

        os.replace(temp_file, cache_file)
        return str(cache_file)
    except Exception:
        return str(cache_file) if _is_valid_xlsx_file(cache_file) else None


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
    """Validate that a workbook contains a usable yearly metrics backbone.

    This protects the app from selecting a downloadable-but-incomplete Google export
    that would otherwise collapse selectors to a single fallback year.
    """
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


def _should_refresh(dest: Path, max_age_seconds: int = 3600) -> bool:
    if not dest.exists():
        return True
    return (time.time() - dest.stat().st_mtime) > max_age_seconds


def _download_google_sheet(url: str, dest: Path, timeout: int = 30) -> bool:
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200 and len(resp.content) > 10_000:
            dest.write_bytes(resp.content)
            return True
    except Exception:
        pass
    return False


def resolve_financial_data_xlsx(local_candidates: Iterable[str] | None = None) -> Optional[str]:
    """Return path to a valid financial data XLSX.

    Resolution order:
    1. GOOGLE_SHEET_URL env var → download to app/attached_assets/live_data.xlsx
       (only re-downloads if file is older than 1 hour)
    2. Existing *.xlsx files in app/attached_assets/ as fallback
    """
    google_sheet_url = os.getenv("GOOGLE_SHEET_URL", "").strip()
    attached_assets = Path(__file__).resolve().parent.parent / "attached_assets"
    attached_assets.mkdir(parents=True, exist_ok=True)
    dest = attached_assets / "live_data.xlsx"

    if google_sheet_url:
        if _should_refresh(dest):
            downloaded = _download_google_sheet(google_sheet_url, dest)
            if downloaded:
                logger.info("WORKBOOK_RESOLVE downloaded from GOOGLE_SHEET_URL → %s", dest)
            else:
                logger.warning("WORKBOOK_RESOLVE GOOGLE_SHEET_URL download failed, trying cache/fallback")
        else:
            logger.info("WORKBOOK_RESOLVE using cached live_data.xlsx (age < 1h)")

        if dest.exists() and _is_valid_xlsx_file(dest):
            return str(dest)

    # Fallback: any existing xlsx in attached_assets
    candidates = list(attached_assets.glob("*.xlsx"))
    if local_candidates:
        candidates += [Path(p) for p in local_candidates]
    for p in candidates:
        if _is_valid_xlsx_file(p):
            logger.info("WORKBOOK_RESOLVE fallback local file: %s", p)
            return str(p)

    logger.error("WORKBOOK_RESOLVE no valid XLSX found (set GOOGLE_SHEET_URL env var)")
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
    (Minute, Daily, Holders). Cached locally for *refresh_seconds* (default 1 h).
    Returns None if the download fails and no stale cache is available.
    """
    sheet_ref = (
        os.getenv("FINANCIAL_DATA_GSHEET_URL")
        or os.getenv("FINANCIAL_DATA_GSHEET_ID")
        or DEFAULT_GOOGLE_SHEET_URL
    )
    sheet_id = extract_google_sheet_id(sheet_ref)
    if not sheet_id:
        return None
    try:
        return _download_google_sheet_xlsx(sheet_id, refresh_seconds=refresh_seconds)
    except Exception:
        return None
