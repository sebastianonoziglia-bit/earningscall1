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
EXPECTED_WORKBOOK_MIN_SHEET_COUNT = 43
REQUIRED_WORKBOOK_SHEETS = {
    "Company_metrics_earnings_values",
    "Daily",
    "Minute",
    "Holders",
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


def resolve_financial_data_xlsx(local_candidates: Iterable[str] | None = None) -> Optional[str]:
    """Resolve the primary workbook path.

    Resolution order:
    1. Explicit local_candidates (caller-supplied paths).
    2. Auto-detected bundled XLSX files in app/attached_assets/ relative to this file.
    3. Google Sheets export (network download, used as fallback).

    Using a valid local file avoids the 20-45 s cold-start network round-trip.
    """
    # 1 + 2: try local files first (fast path — no network)
    _auto_candidates: list[str] = []
    _here = Path(__file__).resolve()
    for _rel in (
        _here.parents[1] / "attached_assets",
        _here.parents[0] / "attached_assets",
    ):
        if _rel.is_dir():
            _auto_candidates.extend(str(p) for p in sorted(_rel.glob("*.xlsx")))

    _all_local = list(local_candidates or []) + _auto_candidates
    for _candidate in _all_local:
        _p = Path(_candidate)
        if not _p.exists() or not _is_valid_xlsx_file(_p):
            continue
        if _has_expected_workbook_tabs(_p) and _has_core_financial_coverage(_p):
            logger.info("WORKBOOK_RESOLVE using local file: %s", _p)
            return str(_p.resolve())
        logger.info("WORKBOOK_RESOLVE local candidate failed validation: %s", _p)

    # 3: fall back to Google Sheets download
    sheet_ref = (
        os.getenv("FINANCIAL_DATA_GSHEET_URL")
        or os.getenv("FINANCIAL_DATA_GSHEET_ID")
        or DEFAULT_GOOGLE_SHEET_URL
    )
    sheet_id = extract_google_sheet_id(sheet_ref)
    if not sheet_id:
        logger.warning("WORKBOOK_RESOLVE could not parse Google Sheet ID from %s", sheet_ref)
        return None

    try:
        refresh_seconds = int(os.getenv("FINANCIAL_DATA_GSHEET_REFRESH_SECONDS", "14400"))
    except Exception:
        refresh_seconds = 14400
        logger.warning(
            "WORKBOOK_RESOLVE invalid FINANCIAL_DATA_GSHEET_REFRESH_SECONDS; using default=%s",
            refresh_seconds,
        )

    try:
        downloaded = _download_google_sheet_xlsx(sheet_id, refresh_seconds=refresh_seconds)
    except Exception as exc:
        logger.warning(
            "WORKBOOK_RESOLVE Google export download failed for sheet_id=%s: %s",
            sheet_id,
            exc,
        )
        downloaded = None

    if not downloaded or not os.path.exists(downloaded):
        logger.warning("WORKBOOK_RESOLVE Google export unavailable for sheet_id=%s", sheet_id)
        return None
    if not _has_expected_workbook_tabs(downloaded):
        logger.warning(
            "WORKBOOK_RESOLVE Google export missing required workbook topology (>= %s tabs + required sheets).",
            EXPECTED_WORKBOOK_MIN_SHEET_COUNT,
        )
        return None
    if not _has_core_financial_coverage(downloaded):
        logger.warning("WORKBOOK_RESOLVE Google export failed core financial coverage validation.")
        return None
    return os.path.abspath(downloaded)


def get_workbook_source_stamp(path: str | None) -> int:
    if not path:
        return 0
    try:
        return int(os.stat(path).st_mtime_ns)
    except OSError:
        return 0
