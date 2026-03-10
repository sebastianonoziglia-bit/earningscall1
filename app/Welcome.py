import json
import os
import base64
import logging
import re
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from utils.workbook_source import get_live_data_xlsx, get_workbook_source_stamp, resolve_financial_data_xlsx


logger = logging.getLogger(__name__)


@st.cache_data(ttl=3600, show_spinner=False)
def ensure_intelligence_pipeline_is_fresh() -> dict:
    app_dir = Path(__file__).resolve().parent
    root_dir = app_dir.parent
    transcripts_dir = root_dir / "earningscall_transcripts"
    db_path = root_dir / "earningscall_intelligence.db"

    ONE_HOUR = 3600
    if db_path.exists() and (time.time() - db_path.stat().st_mtime) < ONE_HOUR:
        return {"ran": False, "reason": "DB built recently, skipping rebuild"}

    newest_txt_mtime = 0.0
    if transcripts_dir.exists():
        txt_files = [p for p in transcripts_dir.rglob("*.txt") if p.is_file()]
        xlsx_files = [p for p in transcripts_dir.rglob("*.xlsx") if p.is_file()]
        # Explicitly exclude Excel files from the mtime check
        txt_files = [p for p in txt_files if p.suffix.lower() != ".xlsx"]
        if txt_files:
            newest_txt_mtime = max(p.stat().st_mtime for p in txt_files)

    db_mtime = db_path.stat().st_mtime if db_path.exists() else 0.0
    should_run = (not db_path.exists()) or (newest_txt_mtime > db_mtime)

    if not should_run:
        return {"ran": False}

    transcript_index_path = transcripts_dir / "transcript_index.csv"
    if not transcript_index_path.exists():
        return {"ran": False, "reason": "No transcripts found — skipping pipeline"}
    try:
        transcript_index_df = pd.read_csv(transcript_index_path)
    except Exception:
        transcript_index_df = pd.DataFrame()
    if transcript_index_df.empty:
        return {"ran": False, "reason": "No transcripts found — skipping pipeline"}

    def _run_script(script: str, timeout_seconds: int = 60) -> None:
        try:
            result = subprocess.run(
                [sys.executable, script],
                cwd=str(root_dir),
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
            if result.returncode != 0:
                pass  # silently skip failed scripts, never block startup
        except Exception as exc:
            st.warning(f"Pipeline step failed ({script}): {exc}")

    _run_script("scripts/rebuild_transcript_index.py")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_run_script, "scripts/build_intelligence_db.py"),
            pool.submit(_run_script, "scripts/extract_transcript_topics.py"),
        ]
        for future in futures:
            future.result()

    insights_csv = root_dir / "earningscall_transcripts" / "generated_insights_latest.csv"
    if not (insights_csv.exists() and (time.time() - insights_csv.stat().st_mtime) < 86400):
        _run_script("scripts/generate_insights.py", timeout_seconds=180)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(_run_script, "scripts/generate_financial_narratives.py"),
            pool.submit(_run_script, "scripts/generate_diagnostic_report.py"),
        ]
        for future in futures:
            future.result()

    return {"ran": True}


# Must stay first Streamlit command
st.set_page_config(
    page_title="Global Media Intelligence",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Force dark root canvas early so no white bleed before sections render.
st.markdown(
    """
<style>
.stApp, .stApp > div, .main, .main > div,
section[data-testid="stMain"],
section[data-testid="stMain"] > div,
div[data-testid="stAppViewContainer"],
div[data-testid="block-container"] {
    background-color: #0d1117 !important;
    background: #0d1117 !important;
}
.element-container, .stMarkdown, .stPlotlyChart,
.stCaption, div[data-testid="stVerticalBlock"] {
    background: transparent !important;
}
</style>
""",
    unsafe_allow_html=True,
)
st.markdown('<style>iframe{border:none!important;}</style>', unsafe_allow_html=True)
_resolved = resolve_financial_data_xlsx([])
logger.info(f"STARTUP: Excel resolved to → {_resolved}")
if "pipeline_refreshed" not in st.session_state:
    st.session_state["pipeline_refreshed"] = False
AUTO_PIPELINE_REFRESH_ENV = "AUTO_REFRESH_INTELLIGENCE_PIPELINE_ON_STARTUP"
if not st.session_state.get("pipeline_refreshed", False):
    auto_refresh_enabled = str(os.getenv(AUTO_PIPELINE_REFRESH_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}
    if auto_refresh_enabled:
        st.session_state["pipeline_refresh_result"] = ensure_intelligence_pipeline_is_fresh()
    else:
        st.session_state["pipeline_refresh_result"] = {
            "ran": False,
            "reason": f"startup intelligence refresh disabled (set {AUTO_PIPELINE_REFRESH_ENV}=1 to enable)",
        }
    st.session_state["pipeline_refreshed"] = True

from utils.global_fonts import apply_global_fonts
from utils.header import display_header
from utils.logos import load_company_logos
from utils.state_management import get_data_processor
from utils.theme import get_theme_mode
from utils.transcript_startup_sync import sync_local_transcripts_to_workbook

# One-time sync per container startup (not per session)
SYNC_FLAG_FILE = "/tmp/transcript_sync_done"
AUTO_SYNC_ENV = "AUTO_SYNC_TRANSCRIPTS_ON_STARTUP"


def _run_startup_transcript_sync() -> None:
    if os.path.exists(SYNC_FLAG_FILE):
        return
    try:
        sync_local_transcripts_to_workbook()
        with open(SYNC_FLAG_FILE, "w", encoding="utf-8") as handle:
            handle.write(str(datetime.now()))
    except Exception as exc:
        st.warning(f"Transcript sync failed: {exc}")


# Keep startup fast/stable on hosted runtimes (HF).
# Enable only when explicitly requested via env var.
if str(os.getenv(AUTO_SYNC_ENV, "")).strip().lower() in {"1", "true", "yes", "on"}:
    _run_startup_transcript_sync()


st.session_state["active_nav_page"] = "home"
st.session_state["_active_nav_page"] = "home"
display_header(enable_dom_patch=False)
apply_global_fonts()


APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent

hero_home_path = APP_DIR / "attached_assets" / "hero_home.jpg"
hero_home_b64 = ""
hero_home_mime = "image/jpeg"
if hero_home_path.exists():
    hero_home_b64 = base64.b64encode(hero_home_path.read_bytes()).decode()
elif (APP_DIR / "attached_assets" / "FAQ MFE.png").exists():
    fallback_path = APP_DIR / "attached_assets" / "FAQ MFE.png"
    hero_home_b64 = base64.b64encode(fallback_path.read_bytes()).decode()
    hero_home_mime = "image/png"


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _format_money_musd(value, precision: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"

    sign = "-" if number < 0 else ""
    amount = abs(number)
    if amount >= 1_000_000:
        return f"{sign}${amount / 1_000_000:.{precision}f}T"
    if amount >= 1_000:
        return f"{sign}${amount / 1_000:.{precision}f}B"
    return f"{sign}${amount:.{precision}f}M"


def _safe_pct(numerator, denominator):
    if denominator in (None, 0) or pd.isna(denominator):
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return float(numerator) / float(denominator) * 100


def _build_hero_narrative(year: int, revenue_fmt: str, growth_pct: Optional[float], company_count: int) -> str:
    n = company_count or "N/A"
    if growth_pct is None:
        return f"In {year}, the tracked universe reported {revenue_fmt} in combined revenue across {n} major media and technology players."
    if growth_pct >= 8:
        return (
            f"In {year}, the tracked universe generated {revenue_fmt} in combined revenue, accelerating "
            f"{growth_pct:.1f}% YoY on the back of sustained monetization strength across {n} major players."
        )
    if growth_pct >= 2:
        return (
            f"In {year}, combined revenue reached {revenue_fmt} — a {growth_pct:.1f}% YoY expansion across {n} "
            "tracked companies, reflecting steady but moderating growth momentum."
        )
    if growth_pct >= 0:
        return (
            f"In {year}, revenue grew modestly to {revenue_fmt} (+{growth_pct:.1f}% YoY), signaling a "
            f"normalization phase across the {n}-company universe."
        )
    return (
        f"In {year}, combined revenue contracted to {revenue_fmt} ({growth_pct:.1f}% YoY), with margin pressure "
        f"visible across the {n}-company universe."
    )


def _company_color(company: str) -> str:
    palette = {
        "Alphabet": "#4285F4",
        "Apple": "#A3A3A3",
        "Meta": "#0668E1",
        "Meta Platforms": "#0668E1",
        "Microsoft": "#00A4EF",
        "Amazon": "#FF9900",
        "Netflix": "#E50914",
        "Disney": "#113CCF",
        "Comcast": "#0088D2",
        "Warner Bros. Discovery": "#1E40AF",
        "Paramount Global": "#0060FF",
        "Spotify": "#1ED760",
        "Roku": "#6F1AB1",
    }
    return palette.get(company, "#0073FF")


def _resolve_logo(company: str, logos: Dict[str, str]) -> str:
    if company in logos:
        return logos[company]
    aliases = {
        "Meta": "Meta Platforms",
        "Warner Bros Discovery": "Warner Bros. Discovery",
        "Paramount": "Paramount Global",
    }
    alt = aliases.get(company)
    if alt and alt in logos:
        return logos[alt]
    for key in logos:
        if key.lower() == company.lower():
            return logos[key]
    return ""


def _render_company_logos(companies_raw: str, logos: Dict[str, str]) -> str:
    if not companies_raw:
        return ""
    chips = []
    for token in str(companies_raw).replace(",", "|").split("|"):
        name = token.strip()
        if not name:
            continue
        img_b64 = _resolve_logo(name, logos)
        if not img_b64:
            continue
        chips.append(
            "<span class='wm-mini-logo-wrap'>"
            f"<img class='wm-mini-logo' src='data:image/png;base64,{img_b64}' alt='{escape(name)} logo' />"
            "</span>"
        )
    if not chips:
        return ""
    return f"<div class='wm-insight-logos'>{''.join(chips)}</div>"


def _clean_signal_title(raw_title: str) -> str:
    title = str(raw_title or "").strip()
    title = re.sub(r"\s*\(auto\)\s*$", "", title, flags=re.IGNORECASE)
    return title


def _build_hero_company_logo_bar(logos: Dict[str, str]) -> str:
    logo_order = [
        "Alphabet",
        "Meta Platforms",
        "Amazon",
        "Apple",
        "Microsoft",
        "Netflix",
        "Disney",
        "Comcast",
        "Paramount Global",
        "Warner Bros. Discovery",
        "Spotify",
        "Roku",
    ]
    blocks: List[str] = []
    for company in logo_order:
        img_b64 = _resolve_logo(company, logos)
        if not img_b64:
            continue
        company_q = quote_plus(company)
        blocks.append(
            f"<a class='wm-hero-logo-link' href='?nav=earnings&company={company_q}' "
            f"target='_self' rel='noopener' onclick=\"window.location.assign('?nav=earnings&company={company_q}'); return false;\" "
            f"aria-label='Open earnings for {escape(company)}'>"
            "<span class='wm-hero-logo-wrap'>"
            f"<img class='wm-hero-logo' src='data:image/png;base64,{img_b64}' alt='{escape(company)} logo' />"
            "</span>"
            "</a>"
        )
    if not blocks:
        return ""
    return f"<div class='wm-hero-logo-bar'>{''.join(blocks)}</div>"


def _render_leaderboard_strip(title: str, subtitle: str, cards: List[dict]) -> None:
    if not cards:
        return
    visible_cards = cards

    header_html = (
        "<div class='wm-strip-header'>"
        f"<div class='wm-strip-title'>{escape(str(title))}</div>"
        f"<div class='wm-strip-subtitle'>{escape(str(subtitle))}</div>"
        "</div>"
    )

    card_blocks = []
    for card in visible_cards:
        company_name = str(card.get("company", "Unknown")).strip()
        company_color = str(card.get("color") or _company_color(company_name))
        rank = card.get("rank")
        rank_badge = f"<span class='wm-rank'>{int(rank)}</span>" if isinstance(rank, (int, float)) else ""
        logo_b64 = str(card.get("logo", "") or "").strip()
        logo_html = (
            f"<img class='wm-company-logo' src='data:image/png;base64,{logo_b64}' alt='{escape(company_name)} logo'/>"
            if logo_b64
            else ""
        )
        value_text = escape(str(card.get("value", "N/A")))
        yoy_pct = card.get("yoy_pct")
        if isinstance(yoy_pct, (int, float)) and not pd.isna(yoy_pct):
            yoy_arrow = "↑" if float(yoy_pct) >= 0 else "↓"
            yoy_color = "#22c55e" if float(yoy_pct) >= 0 else "#ef4444"
            yoy_html = f"<div class='wm-company-yoy' style='color:{yoy_color};'>{yoy_arrow} {abs(float(yoy_pct)):.1f}% YoY</div>"
        else:
            yoy_html = ""
        card_blocks.append(
            f"<div class='wm-company-card wm-strip-card' style='--wm-color:{company_color};'>"
            f"{rank_badge}"
            "<div class='wm-company-head'>"
            f"{logo_html}<span class='wm-company-name'>{escape(company_name)}</span>"
            "</div>"
            f"<div class='wm-company-value'>{value_text}</div>"
            f"{yoy_html}"
            f"<div class='wm-company-caption'>{escape(title)}</div>"
            "</div>"
        )

    # Show the "scroll for more" indicator only when there are hidden cards
    # beyond what we actually render.
    if len(cards) > len(visible_cards):
        card_blocks.append(
            "<div class='wm-strip-card wm-more-indicator'>"
            "<div class='wm-more-arrow'>→</div>"
            f"<div class='wm-more-label'>Scroll for<br/>{len(cards)} companies</div>"
            "</div>"
        )

    st.markdown(
        f"{header_html}<div class='wm-hscroll'>{''.join(card_blocks)}</div>",
        unsafe_allow_html=True,
    )


def _resolve_workbook_path(data_path: Optional[str] = None) -> Optional[Path]:
    if data_path:
        candidate = Path(data_path)
        if candidate.exists():
            return candidate

    resolved = resolve_financial_data_xlsx([])
    if not resolved:
        return None

    path = Path(resolved)
    return path if path.exists() else None


DEFAULT_HOME_NARRATIVE_FALLBACK = (
    "In 2024, the tracked universe generated $2.1T in combined revenue, accelerating 9.6% YoY "
    "on the back of sustained monetization strength across 12 major players."
)

_QUARTER_SORT = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}

_COMPANY_ALIASES = {
    "ALPHABET": "Alphabet",
    "GOOGLE": "Alphabet",
    "META": "Meta Platforms",
    "META PLATFORMS": "Meta Platforms",
    "AMAZON": "Amazon",
    "APPLE": "Apple",
    "MICROSOFT": "Microsoft",
    "NETFLIX": "Netflix",
    "DISNEY": "Disney",
    "COMCAST": "Comcast",
    "PARAMOUNT": "Paramount Global",
    "PARAMOUNT GLOBAL": "Paramount Global",
    "WARNER BROS DISCOVERY": "Warner Bros. Discovery",
    "WARNER BROS. DISCOVERY": "Warner Bros. Discovery",
    "WBD": "Warner Bros. Discovery",
    "SPOTIFY": "Spotify",
    "ROKU": "Roku",
}

_AD_COLUMN_TO_COMPANY = {
    "GOOGLE ADS": "Alphabet",
    "META ADS": "Meta Platforms",
    "AMAZON ADS": "Amazon",
    "SPOTIFY ADS": "Spotify",
    "WBD ADS": "Warner Bros. Discovery",
    "MICROSOFT ADS": "Microsoft",
    "PARAMOUNT": "Paramount Global",
    "APPLE": "Apple",
    "DISNEY": "Disney",
    "COMCAST": "Comcast",
    "NETFLIX": "Netflix",
}


def _normalize_quarter_label(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text in _QUARTER_SORT:
        return text
    number_match = re.search(r"([1-4])", text)
    if number_match:
        return f"Q{number_match.group(1)}"
    return text


def _quarter_sort_value(value: Any) -> int:
    return _QUARTER_SORT.get(_normalize_quarter_label(value), 0)


def _normalize_company_name(value: Any) -> str:
    text = str(value or "").replace("_", " ").strip()
    text = re.sub(r"\s+", " ", text)
    canonical = _COMPANY_ALIASES.get(text.upper())
    return canonical if canonical else text


def _company_variants(company: str) -> list[str]:
    canonical = _normalize_company_name(company)
    variants = {canonical}
    reverse_aliases = {
        "Meta Platforms": {"Meta"},
        "Warner Bros. Discovery": {"Warner Bros Discovery", "WBD"},
        "Paramount Global": {"Paramount"},
        "Alphabet": {"Google"},
    }
    variants.update(reverse_aliases.get(canonical, set()))
    return sorted({v.strip() for v in variants if v and str(v).strip()})


_UNKNOWN_SPEAKER_VALUES = {"", "unknown", "n/a", "none", "nan"}
_ROLE_SPEAKER_LABELS = {
    "CEO": "Chief Executive Officer",
    "CFO": "Chief Financial Officer",
    "COO": "Chief Operating Officer",
}


def _resolve_speaker_label(
    *,
    speaker: Any = "",
    name: Any = "",
    executive: Any = "",
    who: Any = "",
    rolebucket: Any = "",
    role: Any = "",
) -> str:
    for raw in (speaker, name, executive, who):
        value = str(raw or "").strip()
        if value and value.lower() not in _UNKNOWN_SPEAKER_VALUES:
            return value

    role_value = str(rolebucket or "").strip() or str(role or "").strip()
    if role_value:
        return _ROLE_SPEAKER_LABELS.get(role_value.upper(), role_value)
    return "Executive"


def _split_company_tokens(raw: Any) -> list[str]:
    tokens = re.split(r"[|,;/]", str(raw or ""))
    return [token.strip() for token in tokens if token and token.strip()]


def _normalize_text_for_compare(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


@st.cache_data(ttl=3600, show_spinner=False)
def _read_excel_sheet_cached(excel_path: str, sheet_name: str, source_stamp: int) -> pd.DataFrame:
    _ = source_stamp
    if not excel_path:
        return pd.DataFrame()
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    return df.copy()


def _pick_col(df: pd.DataFrame, aliases: list[str]) -> str:
    normalized = {
        re.sub(r"[^a-z0-9]+", "", str(col).strip().lower()): str(col).strip() for col in df.columns
    }
    for alias in aliases:
        if alias in normalized:
            return normalized[alias]
    return ""


@st.cache_data(ttl=3600, show_spinner=False)
def _load_company_metrics_sheet(excel_path: str, source_stamp: int) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "Company_metrics_earnings_values", source_stamp)
    if raw.empty:
        return pd.DataFrame()

    company_col = _pick_col(raw, ["company", "ticker", "name"])
    year_col = _pick_col(raw, ["year", "fiscalyear"])
    revenue_col = _pick_col(raw, ["revenue", "totalrevenue"])
    net_income_col = _pick_col(raw, ["netincome"])
    operating_income_col = _pick_col(raw, ["operatingincome", "operatingprofit"])
    market_cap_col = _pick_col(raw, ["marketcap", "marketcapitalization"])
    rd_col = _pick_col(raw, ["rd", "researchanddevelopment", "researchdevelopment"])

    if not company_col or not year_col:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "company": raw[company_col],
            "year": raw[year_col],
            "revenue": raw[revenue_col] if revenue_col else np.nan,
            "net_income": raw[net_income_col] if net_income_col else np.nan,
            "operating_income": raw[operating_income_col] if operating_income_col else np.nan,
            "market_cap": raw[market_cap_col] if market_cap_col else np.nan,
            "rd": raw[rd_col] if rd_col else np.nan,
        }
    )
    df["company"] = df["company"].apply(_normalize_company_name)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    for col in ["revenue", "net_income", "operating_income", "market_cap", "rd"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["company", "year"]).copy()
    if df.empty:
        return pd.DataFrame()
    df["year"] = df["year"].astype(int)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def _load_overview_macro_sheet(excel_path: str, source_stamp: int) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "Overview_Macro", source_stamp)
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    if "year" not in df.columns:
        return pd.DataFrame()
    if "quarter" not in df.columns:
        df["quarter"] = ""
    if "macro_comment" not in df.columns:
        df["macro_comment"] = ""
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = df["quarter"].apply(_normalize_quarter_label)
    df["macro_comment"] = df["macro_comment"].astype(str).str.strip()
    df = df.dropna(subset=["year"]).copy()
    if df.empty:
        return pd.DataFrame()
    df["year"] = df["year"].astype(int)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def _load_auto_insights(excel_path: str, source_stamp: int, selected_year: int, selected_quarter: str) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "Overview_Auto_Insights", source_stamp)
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]

    for col in ["title", "text", "comment", "priority", "companies", "sort_order", "year", "quarter", "is_active"]:
        if col not in df.columns:
            df[col] = np.nan if col in {"sort_order", "year"} else ""

    df["is_active"] = pd.to_numeric(df["is_active"], errors="coerce").fillna(0).astype(int)
    df = df[df["is_active"] == 1].copy()
    if df.empty:
        return pd.DataFrame()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["quarter"] = df["quarter"].apply(_normalize_quarter_label)
    df["sort_order"] = pd.to_numeric(df["sort_order"], errors="coerce")
    df["priority"] = df["priority"].astype(str).str.strip().str.lower()

    year_scope = df[df["year"] == int(selected_year)].copy()
    if not year_scope.empty:
        df = year_scope

    rank_map = {"high": 0, "medium": 1, "low": 2}
    df["priority_rank"] = df["priority"].map(rank_map).fillna(3).astype(int)
    df = df.sort_values(["priority_rank", "sort_order", "title"], na_position="last")
    return df.head(6).copy()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_company_ad_revenue_sheet(excel_path: str, source_stamp: int) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "Company_advertising_revenue", source_stamp)
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(col).strip() for col in df.columns]
    year_col = ""
    for candidate in df.columns:
        if str(candidate).strip().lower() == "year":
            year_col = candidate
            break
    if not year_col:
        return pd.DataFrame()
    df["Year"] = pd.to_numeric(df[year_col], errors="coerce")
    return df.dropna(subset=["Year"]).copy()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_company_employees_sheet(excel_path: str, source_stamp: int) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "Company_Employees", source_stamp)
    if raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    company_col = "company" if "company" in df.columns else ""
    year_col = "year" if "year" in df.columns else ""
    employee_col = ""
    for candidate in ["employee count", "employee_count", "employees", "employee"]:
        if candidate in df.columns:
            employee_col = candidate
            break
    if not company_col or not year_col or not employee_col:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "company": df[company_col].apply(_normalize_company_name),
            "year": pd.to_numeric(df[year_col], errors="coerce"),
            "employees": pd.to_numeric(df[employee_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["company", "year"]).copy()
    if out.empty:
        return pd.DataFrame()
    out["year"] = out["year"].astype(int)
    return out


def _select_latest_quarter_for_year(macro_df: pd.DataFrame, year: int) -> str:
    if macro_df.empty:
        return "Q4"
    scoped = macro_df[macro_df["year"] == int(year)].copy()
    if scoped.empty:
        return "Q4"
    scoped["q_sort"] = scoped["quarter"].apply(_quarter_sort_value)
    scoped = scoped.sort_values(["year", "q_sort"], ascending=[False, False])
    quarter = str(scoped.iloc[0].get("quarter", "") or "").strip()
    return quarter if quarter else "Q4"


@st.cache_data(ttl=3600, show_spinner=False)
def _load_m2_yearly_series(excel_path: str, source_stamp: int) -> pd.DataFrame:
    raw = _read_excel_sheet_cached(excel_path, "M2", source_stamp)
    if raw.empty:
        raw = _read_excel_sheet_cached(excel_path, "M2_values", source_stamp)
    if raw.empty:
        return pd.DataFrame(columns=["year", "m2_value"])

    df = raw.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    date_col = ""
    for candidate in ["observation_date", "observation date", "date", "period"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if not date_col:
        for col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().mean() >= 0.7:
                date_col = col
                df["_parsed_date"] = parsed
                break
    if "_parsed_date" not in df.columns:
        if not date_col:
            return pd.DataFrame(columns=["year", "m2_value"])
        df["_parsed_date"] = pd.to_datetime(df[date_col], errors="coerce")

    value_col = ""
    preferred = {"wm2ns", "wm2", "m2", "m2sl", "m2value", "value"}
    for col in df.columns:
        normalized = re.sub(r"[^a-z0-9]+", "", col.lower())
        if normalized in preferred:
            value_col = col
            break
    if not value_col:
        excluded = {date_col, "_parsed_date", "year"}
        numeric_candidates = []
        for col in df.columns:
            if col in excluded:
                continue
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().sum() > 0:
                numeric_candidates.append(col)
        if numeric_candidates:
            value_col = numeric_candidates[0]
    if not value_col:
        return pd.DataFrame(columns=["year", "m2_value"])

    out = pd.DataFrame(
        {
            "date": df["_parsed_date"],
            "m2_value": pd.to_numeric(df[value_col], errors="coerce"),
        }
    )
    out = out.dropna(subset=["date", "m2_value"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["year", "m2_value"])
    out["year"] = out["date"].dt.year.astype(int)
    out = out.groupby("year", as_index=False)["m2_value"].mean()
    return out


def _build_home_narrative(
    year: int,
    metrics_df: pd.DataFrame,
    ad_df: pd.DataFrame,
    macro_df: pd.DataFrame,
    m2_df: pd.DataFrame,
) -> str:
    selected_year = int(year)

    total_revenue = None
    revenue_yoy_pct = None
    if isinstance(metrics_df, pd.DataFrame) and not metrics_df.empty and {"year", "revenue"}.issubset(metrics_df.columns):
        current = metrics_df[metrics_df["year"] == selected_year]["revenue"]
        if not current.empty:
            total_revenue = float(pd.to_numeric(current, errors="coerce").sum())
        previous = metrics_df[metrics_df["year"] == (selected_year - 1)]["revenue"]
        if total_revenue is not None and not previous.empty:
            prev_total = float(pd.to_numeric(previous, errors="coerce").sum())
            if prev_total not in (0, None) and not pd.isna(prev_total):
                revenue_yoy_pct = ((total_revenue - prev_total) / abs(prev_total)) * 100.0

    duopoly_share = None
    if isinstance(ad_df, pd.DataFrame) and not ad_df.empty and "Year" in ad_df.columns:
        scoped = ad_df[pd.to_numeric(ad_df["Year"], errors="coerce") == selected_year]
        if not scoped.empty:
            row = scoped.iloc[-1]
            total_ad = 0.0
            google_ad = 0.0
            meta_ad = 0.0
            for col in ad_df.columns:
                if str(col).strip().lower() == "year":
                    continue
                value = pd.to_numeric(row.get(col), errors="coerce")
                if pd.isna(value):
                    continue
                value_musd = float(value) * 1000.0
                total_ad += value_musd
                clean = str(col).strip().lstrip("*")
                norm = re.sub(r"[^a-z0-9]+", " ", clean.lower()).strip()
                if norm == "google ads":
                    google_ad += value_musd
                if norm == "meta ads":
                    meta_ad += value_musd
            if total_ad > 0:
                duopoly_share = ((google_ad + meta_ad) / total_ad) * 100.0

    digital_share = None
    if isinstance(macro_df, pd.DataFrame) and not macro_df.empty and "year" in macro_df.columns:
        scoped = macro_df[macro_df["year"] == selected_year].copy()
        if not scoped.empty:
            row = scoped.sort_values("quarter", key=lambda s: s.map(_quarter_sort_value)).iloc[-1]
            internet = pd.to_numeric(pd.Series([row.get("internet_ad_spend", np.nan)]), errors="coerce").iloc[0]
            total_market = pd.to_numeric(pd.Series([row.get("global_ad_market", np.nan)]), errors="coerce").iloc[0]
            if pd.notna(internet) and pd.notna(total_market) and float(total_market) != 0.0:
                digital_share = (float(internet) / float(total_market)) * 100.0

    m2_growth = None
    if isinstance(m2_df, pd.DataFrame) and not m2_df.empty and {"year", "m2_value"}.issubset(m2_df.columns):
        curr = m2_df[m2_df["year"] == selected_year]["m2_value"]
        prev = m2_df[m2_df["year"] == (selected_year - 1)]["m2_value"]
        if not curr.empty and not prev.empty:
            curr_val = float(pd.to_numeric(curr, errors="coerce").iloc[-1])
            prev_val = float(pd.to_numeric(prev, errors="coerce").iloc[-1])
            if prev_val not in (0, None) and not pd.isna(prev_val):
                m2_growth = ((curr_val - prev_val) / abs(prev_val)) * 100.0

    if total_revenue is None:
        return DEFAULT_HOME_NARRATIVE_FALLBACK.replace("2024", str(selected_year))

    sentence_one = f"In {selected_year}, the tracked universe generated {_format_money_musd(total_revenue, 1)} in combined revenue"
    if revenue_yoy_pct is not None:
        sentence_one += f" ({revenue_yoy_pct:+.1f}% YoY)"
    if digital_share is not None:
        sentence_one += f", with digital advertising at {digital_share:.1f}% of total spend"
    sentence_one += "."

    sentence_two = ""
    if duopoly_share is not None:
        sentence_two = f"The Alphabet/Meta duopoly controlled {duopoly_share:.1f}% of tracked ad revenue"
        if m2_growth is not None:
            sentence_two += f", against a backdrop of {m2_growth:.1f}% M2 growth"
        sentence_two += "."
    elif m2_growth is not None:
        sentence_two = f"M2 grew {m2_growth:.1f}% year over year."

    return f"{sentence_one} {sentence_two}".strip()


def _macro_comment_is_placeholder(comment: str) -> bool:
    text = str(comment or "").strip()
    if not text:
        return False
    return bool(re.search(r"(baseline\s*row|update\s*this)", text, flags=re.IGNORECASE))


def _pick_macro_comment_for_period(macro_df: pd.DataFrame, year: int, quarter: str) -> str:
    if macro_df is None or macro_df.empty:
        return ""
    if "year" not in macro_df.columns or "macro_comment" not in macro_df.columns:
        return ""

    scoped = macro_df[pd.to_numeric(macro_df["year"], errors="coerce") == int(year)].copy()
    if scoped.empty:
        return ""

    scoped["quarter"] = scoped["quarter"].apply(_normalize_quarter_label)
    quarter_norm = _normalize_quarter_label(quarter)
    exact = scoped[scoped["quarter"] == quarter_norm].copy() if quarter_norm else pd.DataFrame()
    if not exact.empty:
        candidate = exact.iloc[-1]
    else:
        scoped["q_sort"] = scoped["quarter"].apply(_quarter_sort_value)
        candidate = scoped.sort_values("q_sort").iloc[-1]

    return str(candidate.get("macro_comment", "") or "").strip()


def _map_ad_column_to_company(raw_col_name: str) -> tuple[str, bool]:
    raw = str(raw_col_name or "").strip()
    is_estimated = raw.startswith("*")
    clean = raw.lstrip("*").strip()
    normalized = re.sub(r"[^a-z0-9]+", " ", clean.lower()).strip().upper()
    company = _AD_COLUMN_TO_COMPANY.get(normalized, "")
    return (company, is_estimated)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_ad_revenue_by_company(excel_path: str, source_stamp: int, selected_year: int) -> dict[str, dict[str, Any]]:
    ad_df = _load_company_ad_revenue_sheet(excel_path, source_stamp)
    if ad_df.empty:
        return {}
    scoped = ad_df[ad_df["Year"] == int(selected_year)].copy()
    if scoped.empty:
        return {}
    row = scoped.iloc[-1]
    output: dict[str, dict[str, Any]] = {}
    for col in ad_df.columns:
        if str(col).strip().lower() in {"year"}:
            continue
        company, is_estimated = _map_ad_column_to_company(col)
        if not company:
            continue
        value = pd.to_numeric(row.get(col), errors="coerce")
        if pd.isna(value):
            continue
        ad_musd = float(value) * 1000.0
        bucket = output.setdefault(company, {"ad_revenue_musd": 0.0, "ad_estimated": False})
        bucket["ad_revenue_musd"] = float(bucket.get("ad_revenue_musd", 0.0)) + ad_musd
        bucket["ad_estimated"] = bool(bucket.get("ad_estimated", False) or is_estimated)
    return output


def _format_people(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    number = float(value)
    if number >= 1_000_000:
        return f"{number / 1_000_000:.2f}M"
    if number >= 1_000:
        return f"{number / 1_000:.1f}K"
    return f"{int(number):,}"


@st.cache_data(ttl=900, show_spinner=False)
def _build_bubble_dataset(excel_path: str, source_stamp: int, selected_year: int) -> pd.DataFrame:
    metrics = _load_company_metrics_sheet(excel_path, source_stamp)
    if metrics.empty:
        return pd.DataFrame()
    metrics = metrics[metrics["year"] == int(selected_year)].copy()
    if metrics.empty:
        return pd.DataFrame()

    ad_lookup = _load_ad_revenue_by_company(excel_path, source_stamp, selected_year)
    employees_df = _load_company_employees_sheet(excel_path, source_stamp)

    employee_lookup: dict[str, float] = {}
    if not employees_df.empty:
        for company, group in employees_df.groupby("company"):
            exact = group[group["year"] == int(selected_year)]
            if not exact.empty:
                employee_lookup[company] = float(exact.iloc[-1]["employees"]) if pd.notna(exact.iloc[-1]["employees"]) else np.nan
                continue
            prior = group[group["year"] <= int(selected_year)].sort_values("year")
            if not prior.empty:
                employee_lookup[company] = float(prior.iloc[-1]["employees"]) if pd.notna(prior.iloc[-1]["employees"]) else np.nan
                continue
            latest = group.sort_values("year")
            employee_lookup[company] = float(latest.iloc[-1]["employees"]) if pd.notna(latest.iloc[-1]["employees"]) else np.nan

    rows: list[dict[str, Any]] = []
    for row in metrics.itertuples(index=False):
        company = _normalize_company_name(getattr(row, "company", ""))
        revenue = pd.to_numeric(pd.Series([getattr(row, "revenue", np.nan)]), errors="coerce").iloc[0]
        op_income = pd.to_numeric(pd.Series([getattr(row, "operating_income", np.nan)]), errors="coerce").iloc[0]
        market_cap = pd.to_numeric(pd.Series([getattr(row, "market_cap", np.nan)]), errors="coerce").iloc[0]
        rd_value = pd.to_numeric(pd.Series([getattr(row, "rd", np.nan)]), errors="coerce").iloc[0]
        if pd.isna(revenue) or pd.isna(market_cap):
            continue

        ad_info = ad_lookup.get(company, {})
        ad_revenue = pd.to_numeric(pd.Series([ad_info.get("ad_revenue_musd", np.nan)]), errors="coerce").iloc[0]
        ad_estimated = bool(ad_info.get("ad_estimated", False))

        op_margin = float(op_income) / float(revenue) * 100.0 if pd.notna(op_income) and float(revenue) != 0.0 else np.nan
        ad_dependency = float(ad_revenue) / float(revenue) * 100.0 if pd.notna(ad_revenue) and float(revenue) != 0.0 else np.nan
        employees = employee_lookup.get(company, np.nan)

        rows.append(
            {
                "company": company,
                "revenue": float(revenue),
                "revenue_b": float(revenue) / 1000.0,
                "op_margin": float(op_margin) if pd.notna(op_margin) else np.nan,
                "market_cap": float(market_cap),
                "market_cap_b": float(market_cap) / 1000.0,
                "ad_revenue": float(ad_revenue) if pd.notna(ad_revenue) else np.nan,
                "ad_revenue_b": (float(ad_revenue) / 1000.0) if pd.notna(ad_revenue) else np.nan,
                "ad_dependency": float(ad_dependency) if pd.notna(ad_dependency) else np.nan,
                "rd": float(rd_value) if pd.notna(rd_value) else np.nan,
                "employees": float(employees) if pd.notna(employees) else np.nan,
                "ad_estimated": ad_estimated,
            }
        )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["hover_revenue"] = out["revenue"].apply(lambda v: f"Revenue: {_format_money_musd(v, 1)}")
    out["hover_op_margin"] = out["op_margin"].apply(lambda v: f"Operating Margin: {v:.1f}%" if pd.notna(v) else "Operating Margin: N/A")
    out["hover_market_cap"] = out["market_cap"].apply(lambda v: f"Market Cap: {_format_money_musd(v, 1)}")
    out["hover_ad_revenue"] = out.apply(
        lambda r: (
            f"Ad Revenue: {_format_money_musd(r['ad_revenue'], 1)}{'*' if bool(r.get('ad_estimated', False)) else ''}"
            if pd.notna(r.get("ad_revenue"))
            else "Ad Revenue: N/A"
        ),
        axis=1,
    )
    out["hover_rd"] = out["rd"].apply(lambda v: f"R&D: {_format_money_musd(v, 1)}" if pd.notna(v) else "R&D: N/A")
    out["hover_employees"] = out["employees"].apply(lambda v: f"Employees: {_format_people(v)}")
    return out


def _pick_primary_company_for_insight(row: pd.Series, available_companies: list[str]) -> str:
    known = {c.lower(): c for c in available_companies}

    for token in _split_company_tokens(row.get("companies", "")):
        canonical = _normalize_company_name(token)
        if canonical.lower() in known:
            return known[canonical.lower()]

    title = str(row.get("title", "") or "").strip()
    if ":" in title:
        prefix = title.split(":", 1)[0].strip()
        canonical = _normalize_company_name(prefix)
        if canonical.lower() in known:
            return known[canonical.lower()]

    haystack = f"{row.get('title', '')} {row.get('text', '')} {row.get('comment', '')}".lower()
    for company in available_companies:
        if company.lower() in haystack:
            return company
    return ""


def _sqlite_has_column(conn: sqlite3.Connection, table_name: str, col_name: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return False
    return any(str(row[1]).strip().lower() == str(col_name).strip().lower() for row in rows)


@st.cache_data(ttl=600, show_spinner=False)
def _get_best_quote_for_insight(company: str, year: int, quarter: str, db_path: str) -> Optional[dict]:
    if not company or not db_path:
        return None
    db_file = Path(db_path)
    if not db_file.exists():
        return None

    try:
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
    except Exception:
        return None

    try:
        role_bucket_available = _sqlite_has_column(conn, "transcript_highlights", "role_bucket")
        company_values = [v.lower() for v in _company_variants(company)]
        placeholders = ",".join(["?"] * len(company_values))
        q_norm = _normalize_quarter_label(quarter)
        base_select = """
            SELECT
                h.text AS text,
                h.speaker AS speaker,
                {role_expr} AS role_bucket,
                h.highlight_type AS highlight_type,
                h.relevance_score AS relevance_score,
                t.year AS year,
                t.quarter AS quarter
            FROM transcript_highlights h
            JOIN transcripts t ON t.id = h.transcript_id
            WHERE lower(trim(t.company)) IN ({placeholders})
              AND {role_filter}
        """
        role_expr = "h.role_bucket" if role_bucket_available else "''"
        role_filter = (
            "upper(trim(ifnull(h.role_bucket, ''))) IN ('CEO', 'CFO')"
            if role_bucket_available
            else "(lower(ifnull(h.highlight_type, '')) LIKE '%ceo%' OR lower(ifnull(h.highlight_type, '')) LIKE '%cfo%')"
        )
        base_sql = base_select.format(role_expr=role_expr, placeholders=placeholders, role_filter=role_filter)

        exact_sql = (
            base_sql
            + """
              AND t.year = ?
              AND upper(trim(t.quarter)) = ?
            ORDER BY h.relevance_score DESC
            LIMIT 1
            """
        )
        row = conn.execute(exact_sql, (*company_values, int(year), q_norm)).fetchone()

        if row is None:
            fallback_sql = (
                base_sql
                + """
                ORDER BY h.relevance_score DESC, t.year DESC,
                    CASE upper(trim(t.quarter))
                        WHEN 'Q4' THEN 4
                        WHEN 'Q3' THEN 3
                        WHEN 'Q2' THEN 2
                        WHEN 'Q1' THEN 1
                        ELSE 0
                    END DESC
                LIMIT 1
                """
            )
            row = conn.execute(fallback_sql, tuple(company_values)).fetchone()

        if row is None:
            return None

        role_bucket = str(row["role_bucket"] or "").strip().upper()
        if role_bucket not in {"CEO", "CFO"}:
            highlight_type = str(row["highlight_type"] or "").strip().lower()
            if "ceo" in highlight_type:
                role_bucket = "CEO"
            elif "cfo" in highlight_type:
                role_bucket = "CFO"
            else:
                return None

        text = str(row["text"] or "").strip()
        speaker = str(row["speaker"] or "").strip()
        if not text:
            return None
        return {
            "text": text,
            "speaker": speaker if speaker else "Unknown",
            "role": role_bucket,
            "score": row["relevance_score"],
        }
    except Exception:
        return None
    finally:
        conn.close()


def _normalize_quotes_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    aliases = {
        "company": ["company", "ticker", "service"],
        "year": ["year"],
        "quarter": ["quarter", "qtr"],
        "speaker": ["speaker", "speaker_name"],
        "name": ["name"],
        "executive": ["executive"],
        "who": ["who"],
        "role_bucket": ["role_bucket", "rolebucket", "speaker_role", "role"],
        "rolebucket": ["rolebucket", "role_bucket"],
        "role": ["role", "title", "speaker_role", "position"],
        "highlight_type": ["highlight_type"],
        "quote": ["quote", "text", "highlight", "comment", "insight"],
        "score": ["score", "relevance_score", "importance", "rank_score"],
    }
    out = pd.DataFrame()
    for target, names in aliases.items():
        out[target] = ""
        for name in names:
            if name in df.columns:
                out[target] = df[name]
                break

    out["company"] = out["company"].apply(_normalize_company_name)
    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out["quarter"] = out["quarter"].apply(_normalize_quarter_label)
    out["speaker"] = out["speaker"].astype(str).str.strip()
    out["name"] = out["name"].astype(str).str.strip()
    out["executive"] = out["executive"].astype(str).str.strip()
    out["who"] = out["who"].astype(str).str.strip()
    out["quote"] = out["quote"].astype(str).str.strip()
    out["score"] = pd.to_numeric(out["score"], errors="coerce")

    def _role_from_row(r: pd.Series) -> str:
        role_text = str(r.get("role_bucket", "") or "").strip().upper()
        if role_text not in {"CEO", "CFO"}:
            role_text = str(r.get("rolebucket", "") or "").strip().upper()
        if role_text in {"CEO", "CFO"}:
            return role_text
        hl = str(r.get("highlight_type", "") or "").strip().lower()
        if "ceo" in hl:
            return "CEO"
        if "cfo" in hl:
            return "CFO"
        return ""

    out["role_bucket"] = out.apply(_role_from_row, axis=1)
    out["speaker"] = out.apply(
        lambda r: _resolve_speaker_label(
            speaker=r.get("speaker", ""),
            name=r.get("name", ""),
            executive=r.get("executive", ""),
            who=r.get("who", ""),
            rolebucket=r.get("rolebucket", "") or r.get("role_bucket", ""),
            role=r.get("role", ""),
        ),
        axis=1,
    )
    out = out.dropna(subset=["year"]).copy()
    out = out[(out["company"] != "") & (out["quote"] != "")].copy()
    if out.empty:
        return pd.DataFrame()
    out["year"] = out["year"].astype(int)
    return out


@st.cache_data(ttl=600, show_spinner=False)
def _load_pulse_quotes_csv(repo_root_path: str) -> pd.DataFrame:
    repo_root = Path(repo_root_path)
    candidates = [
        repo_root / "earningscall_transcripts" / "overview_iconic_quotes.csv",
        repo_root / "earningscall_transcripts" / "transcript_highlights.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            raw = pd.read_csv(path)
        except Exception:
            continue
        normalized = _normalize_quotes_frame(raw)
        if not normalized.empty:
            return normalized
    return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def _load_pulse_quotes_sqlite(db_path: str) -> pd.DataFrame:
    db_file = Path(db_path)
    if not db_file.exists():
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
    except Exception:
        return pd.DataFrame()
    try:
        role_bucket_available = _sqlite_has_column(conn, "transcript_highlights", "role_bucket")
        role_expr = "h.role_bucket" if role_bucket_available else "''"
        role_filter = (
            "upper(trim(ifnull(h.role_bucket, ''))) = 'CEO'"
            if role_bucket_available
            else "lower(ifnull(h.highlight_type, '')) LIKE '%ceo%'"
        )
        sql = f"""
            SELECT
                t.company AS company,
                t.year AS year,
                t.quarter AS quarter,
                h.speaker AS speaker,
                {role_expr} AS role_bucket,
                h.highlight_type AS highlight_type,
                h.text AS quote,
                h.relevance_score AS score
            FROM transcript_highlights h
            JOIN transcripts t ON t.id = h.transcript_id
            WHERE {role_filter}
            ORDER BY t.year DESC,
                CASE upper(trim(t.quarter))
                    WHEN 'Q4' THEN 4
                    WHEN 'Q3' THEN 3
                    WHEN 'Q2' THEN 2
                    WHEN 'Q1' THEN 1
                    ELSE 0
                END DESC,
                h.relevance_score DESC
            LIMIT 150
        """
        rows = conn.execute(sql).fetchall()
        if not rows:
            return pd.DataFrame()
        raw = pd.DataFrame([dict(row) for row in rows])
        return _normalize_quotes_frame(raw)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


@st.cache_data(ttl=600, show_spinner=False)
def _load_transcript_pulse_quotes(repo_root_path: str, db_path: str, selected_year: int, selected_quarter: str, limit: int = 5) -> tuple[pd.DataFrame, str]:
    csv_df = _load_pulse_quotes_csv(repo_root_path)
    source_label = ""
    working = csv_df.copy()
    if not working.empty:
        source_label = "earningscall_transcripts/overview_iconic_quotes.csv"
    else:
        working = _load_pulse_quotes_sqlite(db_path)
        if not working.empty:
            source_label = "earningscall_intelligence.db (transcript_highlights)"

    if working.empty:
        return (pd.DataFrame(), "")

    working = working[working["role_bucket"] == "CEO"].copy()
    if working.empty:
        return (pd.DataFrame(), source_label)

    q_norm = _normalize_quarter_label(selected_quarter)
    period = working[(working["year"] == int(selected_year)) & (working["quarter"] == q_norm)].copy()
    if not period.empty:
        working = period

    working["q_sort"] = working["quarter"].apply(_quarter_sort_value)
    working = working.sort_values(["year", "q_sort", "score"], ascending=[False, False, False])
    return (working.head(int(limit)).copy(), source_label)


@st.cache_data(ttl=3600, show_spinner=False)
def _load_page_data():
    try:
        dp = get_data_processor()
        raw_metrics = getattr(dp, "df_metrics", pd.DataFrame())
        metrics_df_cached = raw_metrics.copy() if raw_metrics is not None else pd.DataFrame()
        companies_cached = dp.get_companies() if hasattr(dp, "get_companies") else []
        data_path = str(getattr(dp, "data_path", "") or "")
        source_stamp = int(getattr(dp, "source_stamp", 0) or 0)
        return {
            "metrics_df": metrics_df_cached,
            "companies": companies_cached,
            "data_path": data_path,
            "source_stamp": source_stamp,
            "error": "",
        }
    except Exception as exc:
        return {
            "metrics_df": pd.DataFrame(),
            "companies": [],
            "data_path": "",
            "source_stamp": 0,
            "error": str(exc),
        }


# Load data
logos = load_company_logos()
mode = get_theme_mode()
is_dark = mode == "dark"

page_data = _load_page_data()
metrics_df_fallback = page_data.get("metrics_df", pd.DataFrame())
source_stamp = int(page_data.get("source_stamp", 0) or 0)
data_path = str(page_data.get("data_path", "") or "")

if page_data.get("error"):
    st.warning(f"Data initialization warning: {page_data['error']}")

workbook_path = _resolve_workbook_path(data_path)
excel_path = str(workbook_path) if workbook_path else ""

# Live path: always reads Minute / Daily / Holders from Google Sheets (1-hour refresh).
# Falls back to the local file if the download is unavailable.
_live_xlsx = get_live_data_xlsx(refresh_seconds=3600)
live_excel_path: str = _live_xlsx if _live_xlsx else excel_path
live_source_stamp: int = get_workbook_source_stamp(_live_xlsx) if _live_xlsx else source_stamp

metrics_df = _load_company_metrics_sheet(excel_path, source_stamp) if excel_path else pd.DataFrame()
if metrics_df.empty and not metrics_df_fallback.empty:
    fallback = metrics_df_fallback.copy()
    fallback.columns = [str(col).strip().lower() for col in fallback.columns]
    for col in ["company", "year", "revenue", "net_income", "operating_income", "market_cap", "rd"]:
        if col not in fallback.columns:
            fallback[col] = np.nan
    fallback["company"] = fallback["company"].apply(_normalize_company_name)
    fallback["year"] = pd.to_numeric(fallback["year"], errors="coerce")
    for col in ["revenue", "net_income", "operating_income", "market_cap", "rd"]:
        fallback[col] = pd.to_numeric(fallback[col], errors="coerce")
    fallback = fallback.dropna(subset=["company", "year"]).copy()
    if not fallback.empty:
        fallback["year"] = fallback["year"].astype(int)
        metrics_df = fallback[["company", "year", "revenue", "net_income", "operating_income", "market_cap", "rd"]]

available_years = sorted(metrics_df["year"].dropna().unique().tolist()) if not metrics_df.empty else []
latest_year = int(max(available_years)) if available_years else 2024
home_year_options = [int(y) for y in available_years if 2015 <= int(y) <= 2024]
if not home_year_options:
    home_year_options = [int(y) for y in available_years] if available_years else [latest_year]
home_year_default = int(home_year_options[-1]) if home_year_options else latest_year

macro_df = _load_overview_macro_sheet(excel_path, source_stamp) if excel_path else pd.DataFrame()
ad_sheet_df = _load_company_ad_revenue_sheet(excel_path, source_stamp) if excel_path else pd.DataFrame()
m2_yearly_df = _load_m2_yearly_series(excel_path, source_stamp) if excel_path else pd.DataFrame()

# Load Global_Adv_Aggregates (country-level aggregate, values in $M) for total market denominator
global_adv_df = _read_excel_sheet_cached(excel_path, "Global_Adv_Aggregates", source_stamp) if excel_path else pd.DataFrame()
if not global_adv_df.empty:
    global_adv_df.columns = [str(c).strip() for c in global_adv_df.columns]
    global_adv_df["year"] = pd.to_numeric(global_adv_df["year"], errors="coerce")
    global_adv_df["value"] = pd.to_numeric(global_adv_df["value"], errors="coerce")
    global_adv_df = global_adv_df.dropna(subset=["year", "value"])
    global_adv_df["year"] = global_adv_df["year"].astype(int)
# Per-year totals in $B (values in sheet are $M → ÷1000)
_global_adv_totals: "pd.Series" = (
    global_adv_df.groupby("year")["value"].sum() / 1_000.0
).round(1) if not global_adv_df.empty else pd.Series(dtype=float)

# Build structural-shift donut data from real channel-level data ($M values)
_ss_mt_to_ch = {
    "Free TV": "Free TV", "Pay TV": "Free TV",
    "Magazine": "Print", "Newspaper": "Print",
    "Search Desktop": "Digital Search", "Search Mobile": "Digital Search",
    "Social Desktop": "Digital Social", "Social Mobile": "Digital Social",
    "Video Desktop": "Digital Video", "Video Mobile": "Digital Video",
}
_ss_data: dict = {}
if not global_adv_df.empty:
    _ss_keys = ["Free TV", "Print", "Digital Search", "Digital Social", "Digital Video", "Everything Else"]
    for _ss_yr, _ss_grp in global_adv_df.groupby("year"):
        _yr_dict: dict = {k: 0.0 for k in _ss_keys}
        for _, _ss_row in _ss_grp.iterrows():
            _mt = str(_ss_row.get("metric_type", "")).strip()
            # Strip trailing " Worldwide" suffix if present
            _mt = _mt.replace(" Worldwide", "").strip()
            _ch = _ss_mt_to_ch.get(_mt, "Everything Else")
            _yr_dict[_ch] += float(_ss_row["value"])
        if sum(_yr_dict.values()) > 0:
            _ss_data[int(_ss_yr)] = {k: round(v) for k, v in _yr_dict.items()}
_ss_data_json = json.dumps(_ss_data)

# Serialize ad revenue data for animated bubble chart JS
_ad_col_map = {
    "Google_Ads": "Alphabet", "Meta_Ads": "Meta", "Amazon_Ads": "Amazon",
    "Spotify_Ads": "Spotify", "*WBD_Ads": "WBD", "*Microsoft_Ads": "Microsoft",
    "Paramount": "Paramount", "*Apple": "Apple", "*Disney": "Disney",
    "*Comcast": "Comcast", "Netflix*": "Netflix", "Twitter/X": "Twitter/X",
    "TikTok": "TikTok", "Snapchat": "Snapchat",
}
_ad_by_year: dict = {}
if not ad_sheet_df.empty:
    for _, _arow in ad_sheet_df.iterrows():
        try:
            _yr = int(_arow["Year"])
        except (TypeError, ValueError):
            continue
        _ad_by_year[_yr] = {}
        for _col, _name in _ad_col_map.items():
            _v = _arow.get(_col, None)
            if _v is not None and pd.notna(_v):
                try:
                    _vf = float(_v)
                    if _vf > 0:
                        _ad_by_year[_yr][_name] = round(_vf, 2)
                except (TypeError, ValueError):
                    pass
_ad_json_str = json.dumps(_ad_by_year)

# Build per-year global ad total for JS denominator (from Global_Adv_Aggregates)
_global_adv_by_year: dict = {}
for _gyr, _gval in _global_adv_totals.items():
    if _gval and _gval > 0:
        _global_adv_by_year[int(_gyr)] = round(float(_gval), 1)
_global_adv_json_str = json.dumps(_global_adv_by_year)

db_path = ROOT_DIR / "earningscall_intelligence.db"


def _build_rank_cards(block: pd.DataFrame, value_col: str, formatter) -> list[dict]:
    if block.empty or value_col not in block.columns:
        return []
    scoped = block[["company", value_col]].dropna(subset=[value_col]).copy()
    if scoped.empty:
        return []
    scoped = scoped.sort_values(value_col, ascending=False).head(6)
    cards: list[dict] = []
    for rank, (_, row) in enumerate(scoped.iterrows(), start=1):
        company_name = str(row["company"])
        cards.append(
            {
                "rank": rank,
                "company": company_name,
                "value": formatter(float(row[value_col])),
                "color": _company_color(company_name),
                "logo": _resolve_logo(company_name, logos),
                "raw": float(row[value_col]),
                "yoy_pct": None,
            }
        )
    return cards

# Styling
st.markdown(
    """
<style>
.stApp, .stApp > div, .main, .main > div,
section[data-testid="stMain"],
section[data-testid="stMain"] > div,
div[data-testid="stAppViewContainer"],
div[data-testid="block-container"] {
    background-color: #0d1117 !important;
    background: #0d1117 !important;
}
.element-container, .stMarkdown, .stPlotlyChart,
.stCaption, div[data-testid="stVerticalBlock"] {
    background: transparent !important;
}
.stApp p, .stApp span, .stApp div,
.stApp label, .stApp h1, .stApp h2, .stApp h3 {
    color: rgba(255,255,255,0.8);
}
.stCaption > p {
    color: rgba(255,255,255,0.38) !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.02em;
}
.stButton > button {
    background: rgba(255,255,255,0.06) !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    color: white !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: background 0.2s ease !important;
}
.stButton > button,
.stButton > button *,
.stButton > button p,
.stButton > button span,
.stButton > button div {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    opacity: 1 !important;
}
.stButton > button:hover {
    background: rgba(255,255,255,0.14) !important;
}
.stPlotlyChart,
.stPlotlyChart > div,
.stPlotlyChart iframe,
div[data-testid="stPlotlyChart"],
div[data-testid="stPlotlyChart"] > div {
    background: #0d1117 !important;
    border: none !important;
    box-shadow: none !important;
}
.wm-wrap {
    max-width: 1500px;
    margin: 0 auto;
    padding: 0 14px 36px;
}
.wm-pulse-strip,
.wm-stock-strip {
    width: 100%;
    overflow: hidden;
    border-radius: 12px;
    border: 1px solid rgba(148,163,184,0.24);
    background: rgba(6,11,20,0.92);
    padding: 10px 0;
}
.wm-stock-strip {
    padding: 7px 0;
}
.wm-pulse-track,
.wm-stock-track {
    display: flex;
    align-items: stretch;
    gap: 12px;
    width: max-content;
    animation: wmPulseScroll 42s linear infinite;
}
.wm-stock-track {
    animation-duration: 36s;
}
.wm-pulse-item {
    width: min(420px, 82vw);
    flex: 0 0 auto;
    border-radius: 10px;
    border: 1px solid rgba(148,163,184,0.22);
    background: rgba(15,23,42,0.72);
    padding: 10px 12px;
    color: #e2e8f0 !important;
    -webkit-text-fill-color: #e2e8f0 !important;
}
.wm-pulse-item * {
    color: inherit !important;
    -webkit-text-fill-color: inherit !important;
}
.wm-stock-item {
    width: min(300px, 78vw);
    flex: 0 0 auto;
    border-radius: 10px;
    border: 1px solid rgba(148,163,184,0.22);
    background: rgba(15,23,42,0.72);
    padding: 7px 10px;
    color: #e2e8f0 !important;
    -webkit-text-fill-color: #e2e8f0 !important;
}
.wm-stock-item * {
    color: inherit !important;
    -webkit-text-fill-color: inherit !important;
}
.wm-mini-logo {
    width: 42px;
    height: 42px;
    object-fit: contain;
    border-radius: 50%;
    background: rgba(148, 163, 184, 0.12);
    border: 1px solid rgba(148, 163, 184, 0.26);
    padding: 4px;
}
@keyframes wmPulseScroll {
    from { transform: translateX(0); }
    to { transform: translateX(-50%); }
}
@media (max-width: 768px) {
    .wm-wrap { padding: 0 6px 24px; }
}
/* KPI % change arrows — force color against any theme */
.kpi-yoy-pos, .kpi-yoy-pos * {
    color: #22c55e !important;
    -webkit-text-fill-color: #22c55e !important;
    font-size: 0.88rem !important;
    font-weight: 700 !important;
    opacity: 1 !important;
}
.kpi-yoy-neg, .kpi-yoy-neg * {
    color: #ef4444 !important;
    -webkit-text-fill-color: #ef4444 !important;
    font-size: 0.88rem !important;
    font-weight: 700 !important;
    opacity: 1 !important;
}
/* Hero narrative text */
.wm-hero-narrative, .wm-hero-narrative * {
    color: rgba(255,255,255,0.88) !important;
    -webkit-text-fill-color: rgba(255,255,255,0.88) !important;
    opacity: 1 !important;
}
.wm-hero-narrative strong {
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    font-weight: 700 !important;
}
/* Human Voice and stock ticker text */
.wm-pulse-item, .wm-pulse-item * {
    color: #e2e8f0 !important;
    -webkit-text-fill-color: #e2e8f0 !important;
    opacity: 1 !important;
}
.wm-stock-item, .wm-stock-item * {
    color: #e2e8f0 !important;
    -webkit-text-fill-color: #e2e8f0 !important;
    opacity: 1 !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# Render
st.markdown("<div class='wm-wrap'>", unsafe_allow_html=True)

selected_year = st.session_state.get("selected_year", None)
if selected_year is None:
    try:
        selected_year = int(metrics_df["year"].max())
    except Exception:
        selected_year = 2024

valid_years = sorted([int(y) for y in available_years]) if available_years else [2024]
if int(selected_year) not in valid_years:
    selected_year = int(valid_years[-1])
year_index = valid_years.index(int(selected_year))
selected_year = int(
    st.selectbox(
        "Story Year",
        options=valid_years,
        index=year_index,
        key="selected_year",
    )
)
effective_year = int(selected_year)
selected_quarter = _select_latest_quarter_for_year(macro_df, effective_year)

st.components.v1.html(
    """
<script>
const _revealObs = new IntersectionObserver((entries) => {
  entries.forEach(e => {
    if (e.isIntersecting) e.target.classList.add('sv-visible');
  });
}, { threshold: 0.08 });
document.querySelectorAll('.sv').forEach(el => _revealObs.observe(el));

function _startTicker() {
  const els = document.querySelectorAll('[data-rps]');
  if (!els.length) { setTimeout(_startTicker, 300); return; }
  const t0 = Date.now();
  setInterval(() => {
    const elapsed = (Date.now() - t0) / 1000;
    document.querySelectorAll('[data-rps]').forEach(el => {
      const rps = parseFloat(el.getAttribute('data-rps'));
      el.textContent = '$' + (rps * elapsed).toLocaleString('en-US',
        {minimumFractionDigits:0, maximumFractionDigits:0});
    });
  }, 120);
}
_startTicker();
</script>
<style>
html,body{margin:0;padding:0;background:#0d1117;border:none;outline:none;}
.sv {
  opacity: 0;
  transform: translateY(18px);
  transition: opacity .6s ease, transform .6s ease;
}
.sv.sv-visible {
  opacity: 1;
  transform: translateY(0);
}
.human-voice-fix {
  margin-top: -1rem;
}
</style>
""",
    height=0,
)


def _section(label: str, headline: str, body: str, section_class: str = ""):
    class_attr = "sv"
    if section_class:
        class_attr = f"{class_attr} {section_class}"
    st.markdown(
        f"""
        <div class="{class_attr}" style="padding:56px 0 20px;background:transparent;">
          <div style="color:#ff5b1f;font-size:0.7rem;letter-spacing:0.28em;
                      text-transform:uppercase;margin-bottom:10px;">{escape(str(label))}</div>
          <div style="color:white;font-size:1.45rem;font-weight:700;
                      line-height:1.25;margin-bottom:16px;">{escape(str(headline))}</div>
          <div style="color:rgba(255,255,255,0.55);font-size:0.97rem;
                      line-height:1.8;max-width:760px;">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _separator():
    st.markdown(
        """
        <div style="border-top:1px solid rgba(255,255,255,0.06);
                    margin:8px 0 0 0;"></div>
        """,
        unsafe_allow_html=True,
    )


def _find_col(df: pd.DataFrame, includes: list[str], excludes: Optional[list[str]] = None) -> str:
    if df is None or df.empty:
        return ""
    excludes = excludes or []
    for col in df.columns:
        norm = re.sub(r"[^a-z0-9]+", "", str(col).lower())
        if all(re.sub(r"[^a-z0-9]+", "", x.lower()) in norm for x in includes):
            if any(re.sub(r"[^a-z0-9]+", "", x.lower()) in norm for x in excludes):
                continue
            return str(col)
    return ""


def _yr(df, year, year_col="year"):
    if df is None or df.empty or year_col not in df.columns:
        return pd.DataFrame()
    series = pd.to_numeric(df[year_col], errors="coerce")
    row = df[series == int(year)]
    if row.empty:
        valid = series.dropna()
        if valid.empty:
            return pd.DataFrame()
        row = df[series == int(valid.max())]
    return row


def _yoy(current, previous):
    if previous and previous > 0:
        return (current - previous) / previous * 100
    return None


def _yoy_vec(current_series, prev_series):
    return ((pd.to_numeric(current_series, errors="coerce") - pd.to_numeric(prev_series, errors="coerce")) / pd.to_numeric(prev_series, errors="coerce") * 100).where(pd.to_numeric(prev_series, errors="coerce") > 0, np.nan)


def _apply_dark_chart_layout(fig, *, height=360, margin=None, extra_layout=None):
    base_layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font=dict(color="rgba(255,255,255,0.7)"),
        xaxis=dict(
            color="rgba(255,255,255,0.35)",
            gridcolor="rgba(255,255,255,0.05)",
            linecolor="rgba(255,255,255,0.08)",
        ),
        yaxis=dict(
            color="rgba(255,255,255,0.35)",
            gridcolor="rgba(255,255,255,0.05)",
            linecolor="rgba(255,255,255,0.08)",
        ),
        legend=dict(font=dict(color="rgba(255,255,255,0.6)")),
        margin=margin or dict(l=0, r=0, t=32, b=40),
        height=height,
    )
    if extra_layout:
        base_layout.update(extra_layout)
    fig.update_layout(**base_layout)


def _normalize_market_feed(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame()
    df = raw.copy()
    df.columns = [str(c).strip() for c in df.columns]
    date_col = _find_col(df, ["date"]) or _find_col(df, ["time"])
    price_col = _find_col(df, ["price"]) or _find_col(df, ["close"])
    tag_col = _find_col(df, ["tag"]) or _find_col(df, ["ticker"]) or _find_col(df, ["symbol"])
    asset_col = _find_col(df, ["asset"]) or _find_col(df, ["company"]) or _find_col(df, ["name"])
    change_col = _find_col(df, ["change"]) or _find_col(df, ["chg"])
    if not date_col or not price_col:
        return pd.DataFrame()
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], errors="coerce"),
            "price": pd.to_numeric(df[price_col], errors="coerce"),
            "tag": df[tag_col].astype(str).str.strip().str.upper() if tag_col else "",
            "asset": df[asset_col].astype(str).str.strip() if asset_col else "",
            "change": pd.to_numeric(df[change_col].astype(str).str.replace("%", "", regex=False), errors="coerce") if change_col else np.nan,
        }
    )
    out = out.dropna(subset=["date", "price"]).copy()
    return out.sort_values("date")


def _load_market_feed() -> pd.DataFrame:
    if not excel_path and not live_excel_path:
        return pd.DataFrame()
    # Minute and Daily always come from the live (Google Sheets) path so prices stay current.
    _src = live_excel_path or excel_path
    _stamp = live_source_stamp if live_excel_path else source_stamp
    minute_raw = _read_excel_sheet_cached(_src, "Minute", _stamp)
    daily_raw = _read_excel_sheet_cached(_src, "Daily", _stamp)
    minute_df = _normalize_market_feed(minute_raw)
    daily_df = _normalize_market_feed(daily_raw)
    combined = pd.concat([minute_df, daily_df], ignore_index=True)
    if combined.empty:
        # Legacy workbook fallback: local copies may only include "Stocks & Crypto".
        stocks_raw = _read_excel_sheet_cached(excel_path or _src, "Stocks & Crypto", source_stamp)
        combined = _normalize_market_feed(stocks_raw)
    if combined.empty:
        return pd.DataFrame()
    combined = combined.sort_values("date")
    dedup_keys = ["date"]
    if "tag" in combined.columns:
        dedup_keys.append("tag")
    if "asset" in combined.columns:
        dedup_keys.append("asset")
    combined = combined.drop_duplicates(subset=dedup_keys, keep="last")
    return combined


def _render_transcript_pulse_strip(current_year: int, current_quarter: str) -> None:
    pulse_df, pulse_source = _load_transcript_pulse_quotes(
        repo_root_path=str(ROOT_DIR),
        db_path=str(db_path),
        selected_year=int(current_year),
        selected_quarter=current_quarter,
        limit=5,
    )
    if pulse_df.empty:
        st.info("No transcript data available yet — run the intelligence pipeline first.")
        return
    pulse_items: list[str] = []
    for row in pulse_df.itertuples(index=False):
        company = _normalize_company_name(getattr(row, "company", ""))
        speaker = _resolve_speaker_label(
            speaker=getattr(row, "speaker", ""),
            name=getattr(row, "name", ""),
            executive=getattr(row, "executive", ""),
            who=getattr(row, "who", ""),
            rolebucket=getattr(row, "rolebucket", "") or getattr(row, "role_bucket", ""),
            role=getattr(row, "role", ""),
        )
        quote = str(getattr(row, "quote", "") or "").strip()
        if not quote:
            continue
        logo_b64 = _resolve_logo(company, logos)
        logo_html = (
            f"<img class='logo' src='data:image/png;base64,{logo_b64}' alt='{escape(company)} logo' />"
            if logo_b64
            else "<span class='logo' style='display:inline-flex;align-items:center;justify-content:center;'>&#8226;</span>"
        )
        pulse_items.append(
            "<div class='item'>"
            f"<div class='item-quote'>&ldquo;{escape(quote)}&rdquo;</div>"
            f"<div class='item-meta'>"
            f"{logo_html}<span style='font-weight:700;'>{escape(company)}</span>"
            f"<span style='opacity:0.72;'>&#8212; {escape(speaker)}</span></div>"
            "</div>"
        )
    if not pulse_items:
        st.info("No transcript data available yet — run the intelligence pipeline first.")
        return
    _pulse_track = "".join(pulse_items + pulse_items)
    st.components.v1.html(
        "<style>@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');"
        "html,body{margin:0;padding:0;background:#0d1117;}*{box-sizing:border-box;}"
        ".strip{width:100%;overflow:hidden;border-radius:12px;border:1px solid rgba(148,163,184,0.24);background:rgba(6,11,20,0.92);padding:12px 0;}"
        ".track{display:flex;align-items:flex-start;gap:12px;width:max-content;animation:scroll 42s linear infinite;}"
        ".item{width:380px;height:170px;flex:0 0 auto;border-radius:10px;border:1px solid rgba(148,163,184,0.22);background:rgba(15,23,42,0.72);padding:12px 14px;display:flex;flex-direction:column;justify-content:space-between;overflow:hidden;}"
        ".item-quote{font-style:italic;font-size:0.83rem;line-height:1.5;overflow:hidden;display:-webkit-box;-webkit-line-clamp:4;-webkit-box-orient:vertical;}"
        ".item-meta{margin-top:8px;display:flex;align-items:center;gap:8px;font-size:0.74rem;flex-shrink:0;}"
        ".logo{width:32px;height:32px;object-fit:contain;border-radius:50%;background:rgba(148,163,184,0.12);border:1px solid rgba(148,163,184,0.26);padding:3px;flex-shrink:0;}"
        "@keyframes scroll{from{transform:translateX(0);}to{transform:translateX(-50%);}}"
        "</style>"
        f"<div class='strip' style='color:#e2e8f0;font-family:DM Sans,sans-serif;'><div class='track'>{_pulse_track}</div></div>",
        height=210,
    )
    if pulse_source:
        st.caption(f"Source: {pulse_source}")


def _render_stock_price_strip(feed_df: pd.DataFrame) -> None:
    company_ticker_fallback = {
        "Alphabet": ["GOOGL", "GOOG"],
        "Meta Platforms": ["META"],
        "Amazon": ["AMZN"],
        "Apple": ["AAPL"],
        "Microsoft": ["MSFT"],
        "Netflix": ["NFLX"],
        "Disney": ["DIS"],
        "Comcast": ["CMCSA"],
        "Spotify": ["SPOT"],
        "Roku": ["ROKU"],
        "Warner Bros. Discovery": ["WBD"],
        "Paramount Global": ["PARA"],
    }
    if feed_df is None or feed_df.empty:
        st.info("Market ticker unavailable.")
        return

    items = []
    feed = feed_df.copy()
    feed["asset_norm"] = feed["asset"].astype(str).str.lower()
    feed["tag_norm"] = feed["tag"].astype(str).str.upper()
    for company, ticker_aliases in company_ticker_fallback.items():
        company_variants = _company_variants(company)
        pattern = "|".join(re.escape(v.lower()) for v in company_variants if v)
        subset = pd.DataFrame()
        if pattern:
            subset = feed[feed["asset_norm"].str.contains(pattern, na=False, regex=True)]
        if subset.empty:
            subset = feed[feed["tag_norm"].isin(ticker_aliases)]
        if subset.empty:
            continue

        last = subset.sort_values("date").iloc[-1]
        price = float(last.get("price", np.nan))
        if pd.isna(price):
            continue
        change = pd.to_numeric(pd.Series([last.get("change", np.nan)]), errors="coerce").iloc[0]
        if pd.notna(change):
            arrow = "&#9650;" if change >= 0 else "&#9660;"
            chg_color = "#22c55e" if change >= 0 else "#ef4444"
            change_html = f"<span style='color:{chg_color};font-weight:700;'>{arrow} {abs(change):.2f}%</span>"
        else:
            change_html = "<span style='opacity:0.4;'>&#8212;</span>"
        ticker_display = ticker_aliases[0]
        logo_b64 = _resolve_logo(company, logos)
        logo_html = (
            f"<img class='logo' src='data:image/png;base64,{logo_b64}' alt='{escape(company)} logo' />"
            if logo_b64
            else "<span class='logo' style='display:inline-flex;align-items:center;justify-content:center;'>&#8226;</span>"
        )
        items.append(
            "<div class='item'>"
            f"<div style='display:flex;align-items:center;gap:8px;width:100%;'>{logo_html}"
            f"<span style='font-weight:700;font-size:0.8rem;'>{escape(ticker_display)}</span>"
            f"<span style='margin-left:auto;font-family:monospace;font-size:0.92rem;font-weight:700;'>${price:,.2f}</span></div>"
            f"<div style='font-size:0.75rem;margin-top:2px;'>{change_html}</div>"
            "</div>"
        )
    if not items:
        st.info("Market ticker unavailable.")
        return
    _stock_track = "".join(items + items)
    st.components.v1.html(
        "<style>@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');"
        "html,body{margin:0;padding:0;background:#0d1117;}*{box-sizing:border-box;}"
        ".strip{width:100%;overflow:hidden;border-radius:12px;border:1px solid rgba(148,163,184,0.24);background:rgba(6,11,20,0.92);padding:7px 0;}"
        ".track{display:flex;align-items:stretch;gap:12px;width:max-content;animation:scroll 36s linear infinite;}"
        ".item{width:220px;flex:0 0 auto;border-radius:10px;border:1px solid rgba(148,163,184,0.22);background:rgba(15,23,42,0.72);padding:7px 10px;}"
        ".logo{width:32px;height:32px;object-fit:contain;border-radius:50%;background:rgba(148,163,184,0.12);border:1px solid rgba(148,163,184,0.26);padding:3px;flex-shrink:0;}"
        "@keyframes scroll{from{transform:translateX(0);}to{transform:translateX(-50%);}}"
        "</style>"
        f"<div class='strip' style='color:#e6edf3;font-family:DM Sans,sans-serif;'><div class='track'>{_stock_track}</div></div>",
        height=110,
    )


metrics = metrics_df.copy() if isinstance(metrics_df, pd.DataFrame) else pd.DataFrame()
if not metrics.empty:
    metrics.columns = [str(c).strip() for c in metrics.columns]
    if "year" in metrics.columns:
        metrics["year"] = pd.to_numeric(metrics["year"], errors="coerce")
    metrics = metrics.dropna(subset=["year", "company"]).copy()
    metrics["year"] = metrics["year"].astype(int)
for col in ["revenue", "operating_income", "market_cap", "rd", "net_income"]:
    if col in metrics.columns:
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce")

mcap_col = next((c for c in metrics.columns if "market" in c.lower() and "cap" in c.lower()), "market_cap")
if mcap_col not in metrics.columns and "market_cap" in metrics.columns:
    mcap_col = "market_cap"

# Derive global ad spend totals from Global_Adv_Aggregates (loaded earlier)
global_adv_b = None
global_adv_yoy = None
effective_year_global_adv = effective_year

if not _global_adv_totals.empty:
    _lookup_yr = effective_year if effective_year in _global_adv_totals.index else int(_global_adv_totals.index.max())
    effective_year_global_adv = _lookup_yr
    global_adv_b = float(_global_adv_totals[_lookup_yr])
    _prev_yr = _lookup_yr - 1
    if _prev_yr in _global_adv_totals.index:
        _prev_val = float(_global_adv_totals[_prev_yr])
        if _prev_val > 0:
            global_adv_yoy = _yoy(global_adv_b, _prev_val)

# Keep legacy aliases so remaining code below continues to work
groupm_b = global_adv_b
groupm_yoy = global_adv_yoy
effective_year_groupm = effective_year_global_adv

rev_b = None
rev_yoy = None
mcap_b = None
mcap_yoy = None
if not metrics.empty and "revenue" in metrics.columns and mcap_col in metrics.columns:
    yr = metrics[metrics["year"] == effective_year].copy()
    py = metrics[metrics["year"] == (effective_year - 1)].copy()
    if not yr.empty:
        rev_m = float(pd.to_numeric(yr["revenue"], errors="coerce").sum())
        mcap_m = float(pd.to_numeric(yr[mcap_col], errors="coerce").sum())
        rev_b = rev_m / 1e3 if rev_m else None
        mcap_b = mcap_m / 1e3 if mcap_m else None
        if not py.empty:
            prev_rev_m = float(pd.to_numeric(py["revenue"], errors="coerce").sum())
            prev_mcap_m = float(pd.to_numeric(py[mcap_col], errors="coerce").sum())
            rev_yoy = _yoy(rev_m, prev_rev_m)
            mcap_yoy = _yoy(mcap_m, prev_mcap_m)

ad_df = _load_company_ad_revenue_sheet(excel_path, source_stamp) if excel_path else pd.DataFrame()
if not ad_df.empty:
    ad_df = ad_df.copy()
    ad_df.columns = [str(c).strip() for c in ad_df.columns]
    if "Year" in ad_df.columns:
        ad_df["Year"] = pd.to_numeric(ad_df["Year"], errors="coerce").astype("Int64")
ad_lookup = _load_ad_revenue_by_company(excel_path, source_stamp, effective_year) if excel_path else {}
total_tracked_musd = float(sum(float(v.get("ad_revenue_musd", 0.0)) for v in ad_lookup.values()))
total_tracked_b = total_tracked_musd / 1e3 if total_tracked_musd else 0.0
big_tech_names = ["Alphabet", "Meta Platforms", "Amazon", "Apple", "Microsoft"]
big_tech_b = sum(float(ad_lookup.get(c, {}).get("ad_revenue_musd", 0.0)) for c in big_tech_names) / 1e3
other_b = max(total_tracked_b - big_tech_b, 0.0)
global_ad_denom_raw = groupm_b if groupm_b else total_tracked_b
global_ad_denom = global_ad_denom_raw
# Safety guard for mixed units ($M vs $B): keep denominator in billions.
if global_ad_denom and global_ad_denom > 5_000 and total_tracked_b < 5_000:
    global_ad_denom = global_ad_denom / 1_000.0
untracked_b = max((global_ad_denom or 0) - total_tracked_b, 0.0)
market_feed_df = _load_market_feed()

# Hero + KPIs + Narrative block
narrative_parts = []
if groupm_b:
    narrative_parts.append(
        f"In {effective_year_groupm}, the world spent <strong style='color:white;'>${groupm_b:.0f}B</strong> on advertising."
    )
if rev_yoy is not None:
    if rev_yoy >= 10:
        narrative_parts.append(
            f"The 14 companies we track grew revenues <strong style='color:#22c55e;'>+{rev_yoy:.1f}%</strong> — a year of strong expansion."
        )
    elif rev_yoy >= 0:
        narrative_parts.append(
            f"The 14 companies we track grew revenues modestly at <strong style='color:#22c55e;'>+{rev_yoy:.1f}%</strong>."
        )
    else:
        narrative_parts.append(
            f"The 14 companies we track saw revenues contract <strong style='color:#ef4444;'>{rev_yoy:.1f}%</strong> — a difficult macro year."
        )
if mcap_yoy is not None:
    if mcap_yoy >= 15:
        narrative_parts.append(
            f"Markets rewarded them: combined market cap surged <strong style='color:#22c55e;'>+{mcap_yoy:.1f}%</strong>."
        )
    elif mcap_yoy >= 0:
        narrative_parts.append(
            f"Markets moved cautiously: combined market cap rose <strong style='color:#22c55e;'>+{mcap_yoy:.1f}%</strong>."
        )
    else:
        narrative_parts.append(
            f"Markets were skeptical: combined market cap fell <strong style='color:#ef4444;'>{mcap_yoy:.1f}%</strong>."
        )

narrative_html = " ".join(narrative_parts) if narrative_parts else "Narrative unavailable for the selected year."
kpi1_val = f"${groupm_b:.0f}B" if groupm_b else "&#8212;"
kpi1_yoy = ""
if groupm_yoy is not None:
    _c = "#22c55e" if groupm_yoy >= 0 else "#ef4444"
    _a = "&#9650;" if groupm_yoy >= 0 else "&#9660;"
    kpi1_yoy = f"<span style='color:{_c};font-weight:700;font-size:0.88rem;'>{_a} {abs(groupm_yoy):.1f}%</span>"
kpi2_val = f"${rev_b/1e3:.1f}T" if rev_b and rev_b >= 1000 else (f"${rev_b:.0f}B" if rev_b else "&#8212;")
kpi2_yoy = ""
if rev_yoy is not None:
    _c = "#22c55e" if rev_yoy >= 0 else "#ef4444"
    _a = "&#9650;" if rev_yoy >= 0 else "&#9660;"
    kpi2_yoy = f"<span style='color:{_c};font-weight:700;font-size:0.88rem;'>{_a} {abs(rev_yoy):.1f}%</span>"
kpi3_val = f"${mcap_b/1e3:.1f}T" if mcap_b and mcap_b >= 1000 else (f"${mcap_b:.0f}B" if mcap_b else "&#8212;")
kpi3_yoy = ""
if mcap_yoy is not None:
    _c = "#22c55e" if mcap_yoy >= 0 else "#ef4444"
    _a = "&#9650;" if mcap_yoy >= 0 else "&#9660;"
    kpi3_yoy = f"<span style='color:{_c};font-weight:700;font-size:0.88rem;'>{_a} {abs(mcap_yoy):.1f}%</span>"

st.components.v1.html(
    "<style>@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800;900&family=DM+Sans:wght@400;500;700&display=swap');"
    "html,body{margin:0;padding:0;background:#0d1117;}*{box-sizing:border-box;}</style>"
    "<div style='background:linear-gradient(160deg,#0d1117 0%,#0f1f35 50%,#0d1117 100%);padding:72px 48px 64px;font-family:DM Sans,sans-serif;'>"
    "<div style='color:#ff5b1f;font-size:0.72rem;letter-spacing:0.3em;text-transform:uppercase;margin-bottom:20px;'>The Attention Economy</div>"
    "<div style='color:#ffffff;font-size:3.2rem;font-weight:900;line-height:1.05;margin-bottom:40px;font-family:Syne,sans-serif;'>14 companies.<br>One dashboard.</div>"
    "<div style='display:flex;gap:16px;margin-bottom:40px;flex-wrap:wrap;'>"
    f"<div style='flex:1;min-width:150px;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.12);border-radius:10px;padding:20px 16px;'>"
    f"<div style='color:#a8b3c0;font-size:0.7rem;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:8px;'>Global Ad Spend</div>"
    f"<div style='color:#ff5b1f;font-size:2rem;font-weight:900;font-family:monospace;line-height:1.1;'>{kpi1_val}</div>"
    f"<div style='margin-top:4px;'>{kpi1_yoy}</div>"
    f"<div style='color:#8b949e;font-size:0.68rem;margin-top:6px;'>{effective_year_groupm} &middot; Global Aggregates</div></div>"
    f"<div style='flex:1;min-width:150px;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.12);border-radius:10px;padding:20px 16px;'>"
    f"<div style='color:#a8b3c0;font-size:0.7rem;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:8px;'>Tracked Revenue</div>"
    f"<div style='color:#ff5b1f;font-size:2rem;font-weight:900;font-family:monospace;line-height:1.1;'>{kpi2_val}</div>"
    f"<div style='margin-top:4px;'>{kpi2_yoy}</div>"
    f"<div style='color:#8b949e;font-size:0.68rem;margin-top:6px;'>{effective_year} &middot; 14 companies</div></div>"
    f"<div style='flex:1;min-width:150px;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.12);border-radius:10px;padding:20px 16px;'>"
    f"<div style='color:#a8b3c0;font-size:0.7rem;letter-spacing:0.1em;text-transform:uppercase;margin-bottom:8px;'>Combined Market Cap</div>"
    f"<div style='color:#ff5b1f;font-size:2rem;font-weight:900;font-family:monospace;line-height:1.1;'>{kpi3_val}</div>"
    f"<div style='margin-top:4px;'>{kpi3_yoy}</div>"
    f"<div style='color:#8b949e;font-size:0.68rem;margin-top:6px;'>{effective_year} &middot; 14 companies</div></div>"
    "</div>"
    f"<div style='font-size:1.05rem;line-height:1.85;color:#c9d1d9;max-width:680px;'>{narrative_html}</div>"
    "<div style='color:#8b949e;font-size:0.85rem;margin-top:48px;letter-spacing:0.1em;'>&#8595; Scroll to explore</div>"
    "</div>",
    height=560,
)

# Beat 1 — Map
map_body = (
    f"In {effective_year_groupm}, global advertising reached <span style='color:#e6edf3;font-weight:700;'>${groupm_b:.0f}B</span>. "
    f"The map below shows how that spend is distributed — colored by advertising intensity as a share of each country's GDP."
    if groupm_b
    else "Global advertising data for this year is unavailable."
)
_section("The World", "Every dollar. Every country.", map_body)
try:
    country_df = _read_excel_sheet_cached(excel_path, "Country_Totals_vs_GDP", source_stamp) if excel_path else pd.DataFrame()
    if country_df.empty:
        st.info("Global map unavailable.")
    else:
        country_df.columns = [str(c).strip() for c in country_df.columns]
        country_col = _find_col(country_df, ["country"]) or _find_col(country_df, ["name"])
        year_col = _find_col(country_df, ["year"])
        value_col = _find_col(country_df, ["ad", "gdp"]) or _find_col(country_df, ["value"])
        scoped_map = _yr(country_df, effective_year, year_col) if year_col else country_df.copy()
        if scoped_map.empty or not country_col or not value_col:
            st.info("Global map unavailable.")
        else:
            scoped_map[value_col] = pd.to_numeric(scoped_map[value_col], errors="coerce")
            scoped_map = scoped_map.dropna(subset=[country_col, value_col]).copy()
            if scoped_map.empty:
                st.info("Global map unavailable.")
            else:
                map_fig = px.choropleth(
                    scoped_map,
                    locations=country_col,
                    locationmode="country names",
                    color=value_col,
                    color_continuous_scale="Blues",
                    hover_name=country_col,
                    hover_data={country_col: False, value_col: ":.2f"},
                    labels={value_col: "Ad Spend % GDP"},
                )
                map_fig.update_traces(
                    hovertemplate="<b>%{hovertext}</b><br>Ad spend % of GDP: %{z:.2f}%<extra></extra>",
                    marker_line_color="rgba(255,255,255,0.12)",
                    marker_line_width=0.5,
                )
                _apply_dark_chart_layout(
                    map_fig,
                    height=520,
                    margin=dict(l=0, r=0, t=0, b=0),
                    extra_layout=dict(
                        paper_bgcolor="#0d1117",
                        plot_bgcolor="#0d1117",
                        geo=dict(
                            bgcolor="#0d1117",
                            showland=True,
                            landcolor="#1a2332",
                            showframe=False,
                            showcoastlines=True,
                            coastlinecolor="rgba(255,255,255,0.12)",
                            showocean=True,
                            oceancolor="#060b14",
                            showlakes=False,
                            showcountries=False,
                            projection_type="orthographic",
                            lataxis_showgrid=False,
                            lonaxis_showgrid=False,
                        ),
                    ),
                )
                map_fig.update_geos(
                    showocean=True,
                    oceancolor="#060b14",
                    bgcolor="#0d1117",
                    lataxis_showgrid=False,
                    lonaxis_showgrid=False,
                )
                st.plotly_chart(map_fig, use_container_width=True)
except Exception:
    st.info("Global map unavailable.")
st.caption("Map shows advertising spend by country as a % of GDP. Darker = higher ad market intensity.")
_separator()

# Beat 1.5 — Structural Shift donut animation
@st.cache_data(show_spinner=False)
def _build_ss_html(ss_data_json: str) -> str:
    return f"""
<div id="wm-ss-root">
<style>
html,body{{margin:0;padding:0;background:#0d1117;border:none;outline:none;}}
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Syne:wght@700;800&display=swap');
#wm-ss-root{{background:#0d1117;color:#e6edf3;font-family:'DM Sans',sans-serif;width:100%;padding:32px 24px 24px;}}
#wm-ss-root *{{box-sizing:border-box;}}
.wm-ss-label{{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:11px;letter-spacing:.28em;text-transform:uppercase;margin-bottom:10px;font-weight:700;}}
.wm-ss-headline{{color:#e6edf3;font-family:'Syne',sans-serif;font-size:28px;line-height:1.14;margin:0 0 8px;font-weight:800;}}
.wm-ss-body{{color:#8b949e;font-size:14px;line-height:1.55;margin:0 0 20px;}}
.wm-ss-main{{display:flex;gap:24px;align-items:center;min-height:280px;}}
.wm-ss-left{{width:55%;display:flex;justify-content:center;align-items:center;}}
.wm-ss-right{{width:45%;padding:0 8px;}}
.wm-ss-year{{font-family:'Syne',sans-serif;font-size:72px;font-weight:800;color:#e6edf3;line-height:1;}}
.wm-ss-yearlabel{{color:#8b949e;font-size:13px;margin-top:8px;line-height:1.4;}}
.wm-ss-total{{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:18px;font-weight:700;margin-top:12px;}}
.wm-ss-legend{{display:flex;flex-wrap:wrap;gap:10px;margin-top:16px;}}
.wm-ss-leg-item{{display:flex;align-items:center;gap:6px;font-size:11px;color:#8b949e;}}
.wm-ss-leg-dot{{width:10px;height:10px;border-radius:3px;flex-shrink:0;}}
</style>
<div class="wm-ss-label">THE STRUCTURAL SHIFT</div>
<div class="wm-ss-headline">Television had the world's attention.<br>Then the internet took it.</div>
<div class="wm-ss-body">Global advertising by channel. Watch where the money moved.</div>
<div class="wm-ss-main">
  <div class="wm-ss-left"><canvas id="wm-ss-canvas" width="280" height="280"></canvas></div>
  <div class="wm-ss-right">
    <div class="wm-ss-year" id="wm-ss-yr">—</div>
    <div class="wm-ss-yearlabel" id="wm-ss-lbl"></div>
    <div class="wm-ss-total" id="wm-ss-tot"></div>
  </div>
</div>
<div class="wm-ss-legend">
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#3a5a8c"></div>Free TV</div>
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#6b7280"></div>Print</div>
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#ff5b1f"></div>Digital Search</div>
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#f59e0b"></div>Digital Social</div>
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#10b981"></div>Digital Video</div>
  <div class="wm-ss-leg-item"><div class="wm-ss-leg-dot" style="background:#374151"></div>Everything Else</div>
</div>
<script>
const KEYS=["Free TV","Print","Digital Search","Digital Social","Digital Video","Everything Else"];
const COLORS={{"Free TV":"#3a5a8c","Print":"#6b7280","Digital Search":"#ff5b1f","Digital Social":"#f59e0b","Digital Video":"#10b981","Everything Else":"#374151"}};
const DATA={ss_data_json};
const LABELS={{
  1999:"TV dominates. Internet is a rounding error.",
  2000:"The dot-com bubble. Digital spend is minimal.",
  2001:"Dot-com bust. Traditional media holds firm.",
  2002:"Recovery. Print still leads all digital combined.",
  2003:"Search advertising takes root.",
  2004:"Search starts to matter.",
  2005:"Google goes public. Search accelerates.",
  2006:"YouTube launches. Video is coming.",
  2007:"iPhone arrives. The mobile era begins.",
  2008:"Mobile arrives. Print begins its long decline.",
  2009:"Financial crisis. Ad budgets contract sharply.",
  2010:"Recovery. Digital now rivals radio spend.",
  2011:"Social explodes as Facebook hits 500M users.",
  2012:"Social explodes on mobile.",
  2013:"Mobile search surpasses desktop search.",
  2014:"Programmatic display transforms ad buying.",
  2015:"Digital overtakes TV in the largest markets.",
  2016:"Digital overtakes TV globally for the first time.",
  2017:"Duopoly: Google + Meta capture most digital spend.",
  2018:"GDPR reshapes data and targeting.",
  2019:"Mobile search surpasses all of TV.",
  2020:"Pandemic. Streaming surges. OOH collapses.",
  2021:"Pandemic accelerates everything digital.",
  2022:"Retail media rises. Privacy changes hit targeting.",
  2023:"AI reshapes search. Retail media surpasses $100B.",
  2024:"Search + Social = 60% of all ad spend."
}};
const YEARS=Object.keys(DATA).map(Number).sort((a,b)=>a-b);
const canvas=document.getElementById('wm-ss-canvas');
const ctx=canvas.getContext('2d');
let currentAngles=null,rafId=null,stepIdx=0,pauseTimer=null;
function getAngles(yr){{const d=DATA[yr];const total=KEYS.reduce((s,k)=>s+d[k],0);let a=-Math.PI/2;return KEYS.map(k=>{{const slice=total>0?(d[k]/total)*Math.PI*2:0;const start=a;a+=slice;return{{start,end:a,color:COLORS[k]}};}}); }}
function lerp(a,b,t){{return a+(b-a)*t;}}
function drawDonut(angles){{ctx.clearRect(0,0,280,280);const cx=140,cy=140,r=120,ir=72;angles.forEach(s=>{{ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,s.start,s.end);ctx.closePath();ctx.fillStyle=s.color;ctx.fill();}});ctx.beginPath();ctx.arc(cx,cy,ir,0,Math.PI*2);ctx.fillStyle='#0d1117';ctx.fill();}}
function animateTo(from,to,onDone){{let t=0;function step(){{t=Math.min(t+0.04,1);const interp=from.map((s,i)=>(({{start:lerp(s.start,to[i].start,t),end:lerp(s.end,to[i].end,t),color:to[i].color}})));drawDonut(interp);if(t<1){{rafId=requestAnimationFrame(step);}}else{{onDone();}}}}rafId=requestAnimationFrame(step);}}
function formatB(yr){{const total=KEYS.reduce((s,k)=>s+DATA[yr][k],0);return'$'+(total/1000).toFixed(0)+'B total';}}
function runStep(){{if(stepIdx>=YEARS.length){{stepIdx=0;}}const yr=YEARS[stepIdx];const to=getAngles(yr);document.getElementById('wm-ss-yr').textContent=yr;document.getElementById('wm-ss-lbl').textContent=LABELS[yr]||'';document.getElementById('wm-ss-tot').textContent=formatB(yr);const lastYr=YEARS[YEARS.length-1];const pause=yr===lastYr?2000:700;animateTo(currentAngles||to,to,()=>{{currentAngles=to;stepIdx++;pauseTimer=setTimeout(runStep,pause);}});}}
if(YEARS.length>0){{currentAngles=getAngles(YEARS[0]);drawDonut(currentAngles);document.getElementById('wm-ss-yr').textContent=YEARS[0];document.getElementById('wm-ss-lbl').textContent=LABELS[YEARS[0]]||'';document.getElementById('wm-ss-tot').textContent=formatB(YEARS[0]);stepIdx=1;pauseTimer=setTimeout(runStep,800);}}
</script>
</div>
"""

st.components.v1.html(_build_ss_html(_ss_data_json), height=540)
_separator()

@st.cache_data(show_spinner=False)
def _build_attn_html(ad_json_str: str, groupm_json_str: str) -> str:
    return (
    """
<div id="wm-attn-root">
<style>
html,body{margin:0;padding:0;background:#0d1117;border:none;outline:none;}
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Syne:wght@700;800&display=swap');
#wm-attn-root{background:#0d1117;color:#e6edf3;font-family:'DM Sans',sans-serif;width:100%;padding:0 24px;}
#wm-attn-root *{box-sizing:border-box;}
.wa-scene{padding:48px 0 32px;}
.wa-label{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:11px;letter-spacing:.28em;text-transform:uppercase;margin-bottom:10px;font-weight:700;}
.wa-headline{color:#e6edf3;font-family:'Syne',sans-serif;font-size:28px;font-weight:800;line-height:1.14;margin:0 0 8px;}
.wa-body{color:#8b949e;font-size:14px;line-height:1.55;margin:0 0 32px;}
.wa-col-hdr{font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:#8b949e;margin-bottom:16px;font-family:'Syne',sans-serif;font-weight:700;}
.wa-bars{display:flex;flex-direction:column;gap:12px;width:100%;}
.wb-row{display:flex;align-items:center;gap:10px;opacity:0;transform:translateY(10px);transition:opacity .4s ease,transform .4s ease;width:100%;}
.wb-row.wb-r{transform:translateY(10px);}
.wb-row.vis{opacity:1;transform:translateY(0);}
.wb-n{font-size:13px;font-weight:700;color:#c9d1d9;min-width:90px;white-space:nowrap;font-family:'Syne',sans-serif;}
.wb-track{flex:1;height:22px;background:rgba(255,255,255,0.06);border-radius:6px;overflow:hidden;position:relative;}
.wb-fill{height:100%;width:0;background:var(--c);border-radius:6px;transition:width 1.2s cubic-bezier(.22,1,.36,1);}
.wb-v{font-size:17px;font-weight:800;color:#ff5b1f;min-width:68px;text-align:right;font-family:'Syne',sans-serif;}
.wb-s{font-size:11px;color:#8b949e;min-width:92px;white-space:nowrap;}
.wa-sep{width:40px;height:2px;background:#ff5b1f;margin:8px 0 32px;}
.wa-hero-stats{display:flex;gap:40px;margin-bottom:28px;padding:18px 0;border-top:1px solid rgba(255,255,255,0.06);border-bottom:1px solid rgba(255,255,255,0.06);opacity:0;transition:opacity .9s ease .1s;flex-wrap:wrap;}
.wa-hs-label{font-size:10px;letter-spacing:.22em;text-transform:uppercase;font-family:'Syne',sans-serif;font-weight:700;margin-bottom:3px;}
.wa-hs-num{font-size:40px;font-weight:800;color:#e6edf3;font-family:'Syne',sans-serif;line-height:1;}
.wa-hs-unit{font-size:16px;color:#8b949e;font-weight:500;}
</style>
<div class="wa-scene" id="wa-s1">
  <div class="wa-label">THE SCALE OF ATTENTION</div>
  <div class="wa-headline">Humanity gives these companies its most precious resource.</div>
  <div class="wa-body">Every day. Every minute. Here is where billions of hours go — and who gets paid for them.</div>
  <div class="wa-hero-stats" id="wa-hero-stats">
    <div><div class="wa-hs-label" style="color:#ff5b1f;">Platforms</div><div class="wa-hs-num">11</div></div>
    <div><div class="wa-hs-label" style="color:#ff5b1f;">Top Engagement</div><div class="wa-hs-num">120<span class="wa-hs-unit"> min/day</span></div></div>
    <div><div class="wa-hs-label" style="color:#10b981;">Best Yield</div><div class="wa-hs-num">$0.0066<span class="wa-hs-unit">/min</span></div></div>
    <div><div class="wa-hs-label" style="color:#10b981;">Yield Spread</div><div class="wa-hs-num">4×<span class="wa-hs-unit"> lowest→highest</span></div></div>
  </div>
  <div style="display:flex;gap:32px;margin-top:8px;width:100%;">
    <div style="flex:1;min-width:0;">
      <div class="wa-col-hdr">Minutes per user · per day</div>
      <div class="wa-bars" id="wa-min-bars">
        <div class="wb-row" data-delay="350"  data-w="100"  style="--c:rgba(229,9,20,0.8)"><span class="wb-n">Netflix</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">120 min</span><span class="wb-s">301M subs</span></div>
        <div class="wb-row" data-delay="410"  data-w="42"   style="--c:rgba(255,91,31,0.8)"><span class="wb-n">Spotify</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">50 min</span><span class="wb-s">600M MAUs</span></div>
        <div class="wb-row" data-delay="470"  data-w="50"   style="--c:rgba(100,65,165,0.8)"><span class="wb-n">WBD / Max</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">60 min</span><span class="wb-s">97M subs</span></div>
        <div class="wb-row" data-delay="530"  data-w="50"   style="--c:rgba(0,100,180,0.8)"><span class="wb-n">Disney+</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">60 min</span><span class="wb-s">149M subs</span></div>
        <div class="wb-row" data-delay="590"  data-w="50"   style="--c:rgba(0,84,160,0.8)"><span class="wb-n">Paramount+</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">60 min</span><span class="wb-s">71M subs</span></div>
        <div class="wb-row" data-delay="650"  data-w="50"   style="--c:rgba(210,32,42,0.8)"><span class="wb-n">Peacock</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">60 min</span><span class="wb-s">35M subs</span></div>
        <div class="wb-row" data-delay="710"  data-w="32"   style="--c:rgba(255,153,0,0.85)"><span class="wb-n">Amazon PV</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">40 min</span><span class="wb-s">200M actives</span></div>
        <div class="wb-row" data-delay="770"  data-w="32"   style="--c:rgba(24,119,242,0.85)"><span class="wb-n">Meta Fb</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">39 min</span><span class="wb-s">2.1B DAUs</span></div>
        <div class="wb-row" data-delay="830"  data-w="26"   style="--c:rgba(225,48,108,0.85)"><span class="wb-n">Instagram</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">31 min</span><span class="wb-s">2.0B MAUs</span></div>
        <div class="wb-row" data-delay="890"  data-w="20"   style="--c:rgba(255,0,0,0.75)"><span class="wb-n">YouTube</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">24 min</span><span class="wb-s">2.5B MAUs</span></div>
        <div class="wb-row" data-delay="950"  data-w="12"   style="--c:rgba(145,70,255,0.8)"><span class="wb-n">Twitch</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-v">14 min</span><span class="wb-s">240M MAUs</span></div>
      </div>
    </div>
    <div style="flex:1;min-width:0;">
      <div class="wa-col-hdr" style="text-align:right;">$ earned per minute watched · yield rank</div>
      <div class="wa-bars" id="wa-rev-bars" style="align-items:flex-end;">
        <div class="wb-row wb-r" data-delay="400"  data-w="100" style="--c:rgba(0,100,180,0.9)"><span class="wb-v" style="text-align:right;">$0.0066</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Disney+</span><span class="wb-s" style="text-align:right;">$21.9B ÷ 3.3T min</span></div>
        <div class="wb-row wb-r" data-delay="460"  data-w="85"  style="--c:rgba(210,32,42,0.85)"><span class="wb-v" style="text-align:right;">$0.0056</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Peacock</span><span class="wb-s" style="text-align:right;">$5.0B ÷ 0.9T min</span></div>
        <div class="wb-row wb-r" data-delay="520"  data-w="73"  style="--c:rgba(0,84,160,0.8)"><span class="wb-v" style="text-align:right;">$0.0048</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Paramount+</span><span class="wb-s" style="text-align:right;">$7.6B ÷ 1.6T min</span></div>
        <div class="wb-row wb-r" data-delay="580"  data-w="70"  style="--c:rgba(100,65,165,0.8)"><span class="wb-v" style="text-align:right;">$0.0046</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">WBD / Max</span><span class="wb-s" style="text-align:right;">$10.2B ÷ 2.2T min</span></div>
        <div class="wb-row wb-r" data-delay="640"  data-w="67"  style="--c:rgba(255,153,0,0.85)"><span class="wb-v" style="text-align:right;">$0.0044</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Amazon PV</span><span class="wb-s" style="text-align:right;">$13.5B ÷ 3.1T min</span></div>
        <div class="wb-row wb-r" data-delay="700"  data-w="50"  style="--c:rgba(225,48,108,0.85)"><span class="wb-v" style="text-align:right;">$0.0033</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Instagram</span><span class="wb-s" style="text-align:right;">$73B ÷ 21.9T min</span></div>
        <div class="wb-row wb-r" data-delay="760"  data-w="44"  style="--c:rgba(229,9,20,0.8)"><span class="wb-v" style="text-align:right;">$0.0029</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Netflix</span><span class="wb-s" style="text-align:right;">$33.7B ÷ 11.8T min</span></div>
        <div class="wb-row wb-r" data-delay="820"  data-w="42"  style="--c:rgba(24,119,242,0.85)"><span class="wb-v" style="text-align:right;">$0.0028</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Meta Fb</span><span class="wb-s" style="text-align:right;">$90B ÷ 31.8T min</span></div>
        <div class="wb-row wb-r" data-delay="880"  data-w="36"  style="--c:rgba(30,215,96,0.85)"><span class="wb-v" style="text-align:right;">$0.0024</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Spotify</span><span class="wb-s" style="text-align:right;">$15.6B ÷ 6.5T min</span></div>
        <div class="wb-row wb-r" data-delay="940"  data-w="33"  style="--c:rgba(145,70,255,0.8)"><span class="wb-v" style="text-align:right;">$0.0022</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">Twitch</span><span class="wb-s" style="text-align:right;">$2.8B ÷ 1.25T min</span></div>
        <div class="wb-row wb-r" data-delay="1000" data-w="24"  style="--c:rgba(255,0,0,0.75)"><span class="wb-v" style="text-align:right;">$0.0016</span><div class="wb-track"><div class="wb-fill"></div></div><span class="wb-n" style="text-align:right;">YouTube</span><span class="wb-s" style="text-align:right;">$36.1B ÷ 21.9T min</span></div>
      </div>
    </div>
  </div>
</div>
<div class="wa-sep"></div>
<div class="wa-scene" id="wa-s2">
  <div class="wa-label">THE AD DUOPOLY</div>
  <div class="wa-headline">Two companies. Most of the money.</div>
  <div class="wa-body">Watch how Alphabet and Meta came to dominate digital advertising &#8212; from 2010 to today.</div>
  <div style="display:flex;width:100%;height:460px;align-items:stretch;margin-top:8px;">
    <div style="width:30%;display:flex;flex-direction:column;padding:16px 20px 16px 4px;border-right:1px solid rgba(255,255,255,0.07);flex-shrink:0;">
      <div style="color:#8b949e;font-family:'Syne',sans-serif;font-size:10px;letter-spacing:.2em;text-transform:uppercase;margin-bottom:2px;">Year</div>
      <div id="wa-dup-yr" style="font-family:'Syne',sans-serif;font-size:64px;font-weight:800;color:#e6edf3;line-height:1;transition:opacity .2s ease;">—</div>
      <div style="height:2px;width:100%;background:rgba(255,255,255,0.08);border-radius:2px;margin:10px 0 18px;"><div id="wa-dup-prog" style="height:100%;background:#ff5b1f;border-radius:2px;width:0%;transition:width 1s ease;"></div></div>
      <div style="flex:1;display:flex;flex-direction:column;justify-content:center;gap:14px;">
        <div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;">
            <div style="display:flex;align-items:center;gap:6px;"><div style="width:8px;height:8px;background:#4285f4;border-radius:2px;flex-shrink:0;"></div><span style="font-size:11px;color:#8b949e;font-family:'Syne',sans-serif;font-weight:700;letter-spacing:.06em;text-transform:uppercase;">Alphabet</span></div>
            <span id="wa-alp-val" style="font-size:13px;font-weight:800;color:#e6edf3;font-family:'Syne',sans-serif;"></span>
          </div>
          <div style="height:14px;background:rgba(255,255,255,0.06);border-radius:4px;overflow:hidden;"><div id="wa-alp-bar" style="height:100%;width:0%;background:#4285f4;border-radius:4px;transition:width 1.2s cubic-bezier(.22,1,.36,1);"></div></div>
        </div>
        <div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;">
            <div style="display:flex;align-items:center;gap:6px;"><div style="width:8px;height:8px;background:#0866ff;border-radius:2px;flex-shrink:0;"></div><span style="font-size:11px;color:#8b949e;font-family:'Syne',sans-serif;font-weight:700;letter-spacing:.06em;text-transform:uppercase;">Meta</span></div>
            <span id="wa-meta-val" style="font-size:13px;font-weight:800;color:#e6edf3;font-family:'Syne',sans-serif;"></span>
          </div>
          <div style="height:14px;background:rgba(255,255,255,0.06);border-radius:4px;overflow:hidden;"><div id="wa-meta-bar" style="height:100%;width:0%;background:#0866ff;border-radius:4px;transition:width 1.2s cubic-bezier(.22,1,.36,1);"></div></div>
        </div>
        <div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;">
            <div style="display:flex;align-items:center;gap:6px;"><div style="width:8px;height:8px;background:#4a4a4a;border-radius:2px;flex-shrink:0;"></div><span style="font-size:11px;color:#8b949e;font-family:'Syne',sans-serif;font-weight:700;letter-spacing:.06em;text-transform:uppercase;">Rest of Digital</span></div>
            <span id="wa-rest-val" style="font-size:13px;font-weight:800;color:#8b949e;font-family:'Syne',sans-serif;"></span>
          </div>
          <div style="height:14px;background:rgba(255,255,255,0.06);border-radius:4px;overflow:hidden;"><div id="wa-rest-bar" style="height:100%;width:0%;background:#4a4a4a;border-radius:4px;transition:width 1.2s cubic-bezier(.22,1,.36,1);"></div></div>
        </div>
      </div>
      <div style="margin-top:14px;padding-top:12px;border-top:1px solid rgba(255,255,255,0.06);display:flex;justify-content:space-between;align-items:flex-end;">
        <div><div style="color:#8b949e;font-size:10px;margin-bottom:2px;">Total market</div><div id="wa-dup-tot" style="font-family:'Syne',sans-serif;font-size:17px;font-weight:800;color:#ff5b1f;line-height:1.1;">&#8212;</div></div>
        <div style="text-align:right;"><div style="color:#8b949e;font-size:10px;margin-bottom:2px;">Duo share</div><div id="wa-dup-pct" style="font-family:'Syne',sans-serif;font-size:22px;font-weight:800;color:#4285f4;line-height:1.1;">&#8212;%</div></div>
      </div>
    </div>
    <div id="wa-dup-field" style="flex:1;position:relative;height:100%;overflow:hidden;min-width:0;"></div>
  </div>
</div>
<script>
// Fade in hero stats immediately, then trigger bar cascade after 280ms
var heroStats=document.getElementById('wa-hero-stats');
if(heroStats){heroStats.style.opacity='1';}
setTimeout(function(){
  document.querySelectorAll('.wb-row').forEach(function(el){
    var d=parseInt(el.dataset.delay||0);
    var w=el.dataset.w||'0';
    setTimeout(function(){
      el.classList.add('vis');
      var fill=el.querySelector('.wb-fill');
      if(fill)fill.style.width=w+'%';
    },d);
  });
},280);
</script>
<script>
(function(){
var AD_DATA="""
    + ad_json_str
    + """;
var GROUPM_DATA="""
    + groupm_json_str
    + """;
var COMPANIES=[
  {id:'Alphabet',  cx:24, cy:38, bg:'rgba(66,133,244,0.18)',  br:'rgba(66,133,244,0.6)'},
  {id:'Meta',      cx:52, cy:44, bg:'rgba(24,119,242,0.14)',  br:'rgba(24,119,242,0.55)'},
  {id:'Amazon',    cx:74, cy:20, bg:'rgba(255,153,0,0.14)',   br:'rgba(255,153,0,0.55)'},
  {id:'Microsoft', cx:80, cy:57, bg:'rgba(0,164,239,0.11)',   br:'rgba(0,164,239,0.42)'},
  {id:'TikTok',    cx:64, cy:68, bg:'rgba(238,29,82,0.11)',   br:'rgba(238,29,82,0.42)'},
  {id:'Netflix',   cx:87, cy:30, bg:'rgba(229,9,20,0.11)',    br:'rgba(229,9,20,0.42)'},
  {id:'Apple',     cx:38, cy:63, bg:'rgba(190,190,200,0.08)', br:'rgba(190,190,200,0.32)'},
  {id:'Spotify',   cx:91, cy:62, bg:'rgba(30,215,96,0.09)',   br:'rgba(30,215,96,0.34)'},
  {id:'Twitter/X', cx:70, cy:80, bg:'rgba(29,161,242,0.09)',  br:'rgba(29,161,242,0.32)'},
  {id:'Snapchat',  cx:86, cy:78, bg:'rgba(255,209,0,0.09)',   br:'rgba(255,209,0,0.34)'},
  {id:'Disney',    cx:78, cy:73, bg:'rgba(100,130,250,0.08)', br:'rgba(100,130,250,0.3)'},
  {id:'Comcast',   cx:57, cy:80, bg:'rgba(210,32,42,0.08)',   br:'rgba(210,32,42,0.3)'},
  {id:'WBD',       cx:46, cy:73, bg:'rgba(139,92,246,0.08)',  br:'rgba(139,92,246,0.3)'},
  {id:'Paramount', cx:36, cy:77, bg:'rgba(0,84,160,0.08)',    br:'rgba(0,84,160,0.3)'}
];
var MAX_R=90,MIN_R=10;
var ALL_VALS=[];
Object.keys(USE_DATA).forEach(function(y){var yr=USE_DATA[y];Object.keys(yr).forEach(function(k){ALL_VALS.push(yr[k]);});});
var MAX_VAL=ALL_VALS.length?Math.max.apply(null,ALL_VALS):1;
function logR(v){if(!v||v<=0)return 0;return MIN_R+(MAX_R-MIN_R)*Math.log(v+1)/Math.log(MAX_VAL+1);}
var field=document.getElementById('wa-dup-field');
var bubs={};
if(field){
  COMPANIES.forEach(function(c){
    var cid=c.id.replace(/[^a-zA-Z0-9]/g,'_');
    var el=document.createElement('div');
    el.style.cssText='position:absolute;border-radius:50%;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;padding:6px;opacity:0;width:20px;height:20px;transform:translate(-50%,-50%);transition:width 1.2s cubic-bezier(.34,1.1,.64,1),height 1.2s cubic-bezier(.34,1.1,.64,1),opacity .6s ease;box-sizing:border-box;overflow:hidden;pointer-events:none;';
    el.style.left=c.cx+'%';
    el.style.top=c.cy+'%';
    el.style.background='radial-gradient(circle at 35% 35%,'+c.bg+',transparent)';
    el.style.border='1.5px solid '+c.br;
    el.innerHTML='<div style="font-family:\'Syne\',sans-serif;font-size:9px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:rgba(255,255,255,.72);white-space:nowrap;overflow:hidden;max-width:95%;display:none;" id="lbl_'+cid+'">'+c.id+'</div><div style="font-family:\'Syne\',sans-serif;font-weight:800;color:#fff;line-height:1.1;white-space:nowrap;" id="val_'+cid+'"></div>';
    field.appendChild(el);
    bubs[c.id]={el:el,cid:cid};
  });
}
// Fallback dataset so bubbles always display even when live data is unavailable
var FALLBACK_DATA={
  2010:{"Alphabet":29,"Meta":2,"Amazon":0.8,"Microsoft":2.5},
  2012:{"Alphabet":43,"Meta":5,"Amazon":1.5,"Microsoft":3.5,"Twitter/X":0.5},
  2014:{"Alphabet":59,"Meta":12,"Amazon":2.5,"Microsoft":4,"Twitter/X":1.3,"Snapchat":0.1},
  2016:{"Alphabet":79,"Meta":27,"Amazon":4,"Microsoft":5,"Twitter/X":2.5,"Snapchat":0.8},
  2018:{"Alphabet":117,"Meta":55,"Amazon":10,"Microsoft":7,"Twitter/X":3,"Snapchat":1.2},
  2020:{"Alphabet":147,"Meta":86,"Amazon":21,"Microsoft":8,"Twitter/X":3.5,"Snapchat":2.5},
  2022:{"Alphabet":225,"Meta":116,"Amazon":38,"Microsoft":12,"TikTok":10,"Twitter/X":4.5,"Snapchat":4.6},
  2024:{"Alphabet":265,"Meta":160,"Amazon":57,"Microsoft":18,"TikTok":22,"Netflix":3.9,"Snapchat":5,"Twitter/X":3.4}
};
// Merge: live data takes priority, fallback fills gaps
var MERGED_DATA={};
Object.keys(FALLBACK_DATA).forEach(function(y){MERGED_DATA[y]=FALLBACK_DATA[y];});
Object.keys(AD_DATA).forEach(function(y){if(Object.keys(AD_DATA[y]).length>0)MERGED_DATA[y]=AD_DATA[y];});
var USE_DATA=MERGED_DATA;
var YEARS=Object.keys(USE_DATA).map(Number).sort(function(a,b){return a-b;});
var stepIdx=0,aTimer=null;
function updateYear(yr){
  var data=USE_DATA[yr]||{};
  var yrEl=document.getElementById('wa-dup-yr');
  if(yrEl){yrEl.style.opacity='0';setTimeout(function(){yrEl.textContent=yr;yrEl.style.opacity='1';},200);}
  var total=GROUPM_DATA[yr]||0;
  if(!total){Object.keys(data).forEach(function(k){if(data[k]>0)total+=data[k];});}
  var alpVal=data['Alphabet']||0;
  var metaVal=data['Meta']||0;
  var duo=alpVal+metaVal;
  var tEl=document.getElementById('wa-dup-tot');
  var pEl=document.getElementById('wa-dup-pct');
  var prEl=document.getElementById('wa-dup-prog');
  if(tEl)tEl.textContent=total>0?'$'+(total>=1000?(total/1000).toFixed(1)+'T':total.toFixed(0)+'B'):'—';
  if(pEl)pEl.textContent=total>0?(duo/total*100).toFixed(0)+'%':'—%';
  if(prEl){var idx=YEARS.indexOf(yr);var pct=YEARS.length>1?idx/(YEARS.length-1)*100:100;prEl.style.width=pct+'%';}
  // Update mini bar chart in left panel
  var alpPct=total>0?Math.min(alpVal/total*100,100):0;
  var metaPct=total>0?Math.min(metaVal/total*100,100):0;
  var restPct=Math.max(0,100-alpPct-metaPct);
  var alpBar=document.getElementById('wa-alp-bar'),metaBar=document.getElementById('wa-meta-bar'),restBar=document.getElementById('wa-rest-bar');
  var alpValEl=document.getElementById('wa-alp-val'),metaValEl=document.getElementById('wa-meta-val'),restValEl=document.getElementById('wa-rest-val');
  if(alpBar)alpBar.style.width=alpPct+'%';
  if(metaBar)metaBar.style.width=metaPct+'%';
  if(restBar)restBar.style.width=restPct+'%';
  if(alpValEl)alpValEl.textContent=alpVal>0?'$'+alpVal.toFixed(0)+'B':'—';
  if(metaValEl)metaValEl.textContent=metaVal>0?'$'+metaVal.toFixed(0)+'B':'—';
  if(restValEl){var restAbs=total-duo;restValEl.textContent=restAbs>0?'$'+restAbs.toFixed(0)+'B':'—';}
  COMPANIES.forEach(function(c){
    var val=data[c.id]||0;
    var b=bubs[c.id];
    if(!b)return;
    var r=logR(val),d=r*2;
    if(val<=0||r<MIN_R){b.el.style.opacity='0';b.el.style.width='20px';b.el.style.height='20px';}
    else{
      b.el.style.opacity='1';b.el.style.width=d+'px';b.el.style.height=d+'px';
      var vEl=document.getElementById('val_'+b.cid);
      if(vEl){var fs=r>55?'15px':r>35?'12px':r>22?'9px':'7px';vEl.style.fontSize=fs;vEl.textContent='$'+(val>=100?val.toFixed(0):val.toFixed(1))+'B';}
      var lEl=document.getElementById('lbl_'+b.cid);
      if(lEl)lEl.style.display=r>22?'block':'none';
    }
  });
}
function runStep(){
  if(stepIdx>=YEARS.length){stepIdx=0;aTimer=setTimeout(runStep,3500);return;}
  updateYear(YEARS[stepIdx]);
  var last=stepIdx===YEARS.length-1;
  stepIdx++;
  aTimer=setTimeout(runStep,last?3500:1350);
}
if(YEARS.length>0){
  updateYear(YEARS[0]);
  stepIdx=1;
  aTimer=setTimeout(runStep,1600);
} else {
  var field2=document.getElementById('wa-dup-field');
  if(field2){field2.innerHTML='<div style="padding:40px;color:#8b949e;font-size:13px;">Ad revenue data loading…</div>';}
}
})();
</script>
</div>
"""
    )

_attn_html = _build_attn_html(_ad_json_str, _global_adv_json_str)
st.components.v1.html(_attn_html, height=1500)

_separator()
# --- Concentration: animated stacked bar 2010→latest ---
_CONC_SEG_ORDER = ["Alphabet", "Meta", "Amazon", "Apple + MSFT", "Other Digital",
                   "TV (Free + Pay)", "Print", "Radio", "OOH", "Cinema"]
_CONC_SEG_COLORS = {
    "Alphabet": "#1a73e8", "Meta": "#0866ff", "Amazon": "#ff9900",
    "Apple + MSFT": "#ff4202", "Other Digital": "#4a4a4a",
    "TV (Free + Pay)": "#444444", "Print": "#333333",
    "Radio": "#282828", "OOH": "#1e1e1e", "Cinema": "#141414",
}
_CONC_DIG_PATTERNS = ["Search", "Social", "Display", "Video", "Digital OOH", "Other Desktop", "Other Mobile"]

def _safe_float(v) -> float:
    import math
    try:
        r = float(pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0])
        return 0.0 if math.isnan(r) else r
    except Exception:
        return 0.0

_conc_all_years: dict = {}
_conc_anim_start = 2010
for _ay in range(_conc_anim_start, (effective_year or 2024) + 1):
    if _ay not in _global_adv_totals.index:
        continue
    _ay_total = float(_global_adv_totals[_ay])
    if _ay_total <= 0:
        continue
    _ay_ar = _ad_by_year.get(_ay, {})
    _ay_alpha = _safe_float(_ay_ar.get("Alphabet", 0))
    _ay_meta  = _safe_float(_ay_ar.get("Meta", 0))
    _ay_amzn  = _safe_float(_ay_ar.get("Amazon", 0))
    _ay_apple = _safe_float(_ay_ar.get("Apple", 0))
    _ay_msft  = _safe_float(_ay_ar.get("Microsoft", 0))
    _ay_named = _ay_alpha + _ay_meta + _ay_amzn + _ay_apple + _ay_msft
    _ay_df = global_adv_df[global_adv_df["year"] == _ay] if not global_adv_df.empty else pd.DataFrame()
    _ay_by_type = (_ay_df.groupby("metric_type")["value"].sum() / 1_000.0).to_dict() if not _ay_df.empty else {}
    _ay_dig_total = sum(_ay_by_type.get(k, 0) for k in _ay_by_type if any(x in k for x in _CONC_DIG_PATTERNS))
    _ay_dig_other = max(0.0, _ay_dig_total - _ay_named)
    _ay_vals: dict = {
        "Alphabet": _ay_alpha, "Meta": _ay_meta, "Amazon": _ay_amzn,
        "Apple + MSFT": _ay_apple + _ay_msft,
        "Other Digital": _ay_dig_other if _ay_dig_other > 0.5 else 0.0,
        "TV (Free + Pay)": sum(_ay_by_type.get(k, 0) for k in ["Free TV Worldwide", "Pay TV Worldwide"]),
        "Print":  sum(_ay_by_type.get(k, 0) for k in ["Newspaper Worldwide", "Magazine Worldwide"]),
        "Radio":  _ay_by_type.get("Radio Worldwide", 0.0),
        "OOH":    _ay_by_type.get("Traditional OOH Worldwide", 0.0),
        "Cinema": _ay_by_type.get("Cinema Worldwide", 0.0),
    }
    _conc_all_years[_ay] = {
        "total": round(_ay_total, 1),
        "vals": {c: round(_ay_vals[c], 1) for c in _CONC_SEG_ORDER},
    }

_conc_yr = effective_year
_conc_ad = _ad_by_year.get(_conc_yr, {})
_conc_alpha    = _safe_float(_conc_ad.get("Alphabet", 0))
_conc_meta     = _safe_float(_conc_ad.get("Meta", 0))
_conc_amzn     = _safe_float(_conc_ad.get("Amazon", 0))
_conc_apple    = _safe_float(_conc_ad.get("Apple", 0))
_conc_msft     = _safe_float(_conc_ad.get("Microsoft", 0))
_conc_apple_msft = _conc_apple + _conc_msft
_conc_named    = _conc_alpha + _conc_meta + _conc_amzn + _conc_apple_msft
# Total from Global_Adv_Aggregates (best available whole-market figure)
_conc_total = groupm_b if groupm_b and groupm_b > 0 else (_conc_named * 2.0)

# Metric-type breakdown for _conc_yr from global_adv_df (already loaded)
_conc_by_type: dict = {}
if not global_adv_df.empty:
    _cyr_df = global_adv_df[global_adv_df["year"] == _conc_yr]
    _conc_by_type = (_cyr_df.groupby("metric_type")["value"].sum() / 1_000.0).to_dict()

# Digital residual = all digital metric-types minus named-company revenues
_digital_keys = [k for k in _conc_by_type if any(
    x in k for x in ["Search", "Social", "Display", "Video", "Digital OOH", "Other Desktop", "Other Mobile"]
)]
_digital_total = sum(_conc_by_type[k] for k in _digital_keys)
_digital_other = max(0.0, _digital_total - _conc_named)

# Non-digital categories
_trad_cats = [
    ("TV (Free + Pay)", ["Free TV Worldwide", "Pay TV Worldwide"],        "#444444"),
    ("Print",           ["Newspaper Worldwide", "Magazine Worldwide"],    "#333333"),
    ("Radio",           ["Radio Worldwide"],                              "#282828"),
    ("OOH",             ["Traditional OOH Worldwide"],                    "#1e1e1e"),
    ("Cinema",          ["Cinema Worldwide"],                             "#161616"),
]

_conc_top_share = (_conc_named / _conc_total * 100) if _conc_total > 0 else 0
_conc_all_years_json = json.dumps(_conc_all_years)
_conc_seg_colors_json = json.dumps(_CONC_SEG_COLORS)
_conc_seg_order_json = json.dumps(_CONC_SEG_ORDER)

st.components.v1.html(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;700&display=swap');
html,body{{margin:0;padding:0;background:#0d1117;}}
*{{box-sizing:border-box;}}
#conc-root{{background:#0d1117;padding:32px 24px 40px;font-family:'DM Sans',sans-serif;color:#e6edf3;}}
.wc-label{{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:11px;letter-spacing:.28em;text-transform:uppercase;font-weight:700;margin-bottom:10px;}}
.wc-headline{{font-family:'Syne',sans-serif;font-size:28px;font-weight:800;color:#e6edf3;margin:0 0 6px;}}
.wc-sub{{color:#8b949e;font-size:14px;margin:0 0 20px;}}
/* controls */
.conc-controls{{display:flex;align-items:center;gap:14px;margin-bottom:14px;}}
.conc-year{{font-family:'Syne',sans-serif;font-size:22px;font-weight:800;color:#e6edf3;min-width:52px;}}
.conc-total{{font-size:13px;color:#8b949e;}}
.conc-total span{{color:#c9d1d9;font-weight:600;}}
#conc-play{{background:rgba(255,255,255,0.08);border:1px solid rgba(255,255,255,0.18);color:#e6edf3;
  border-radius:6px;padding:5px 16px;font-size:12px;font-weight:600;cursor:pointer;letter-spacing:.05em;}}
#conc-play:hover{{background:rgba(255,255,255,0.14);}}
#conc-slider{{flex:1;accent-color:#ff5b1f;cursor:pointer;height:3px;}}
/* above-bar callout area */
#conc-above{{position:relative;width:100%;height:0;overflow:visible;}}
.cab{{position:absolute;border-radius:6px;padding:6px 10px;white-space:nowrap;pointer-events:none;
  box-shadow:0 2px 8px rgba(0,0,0,.5);transform:translateX(-50%);transition:left 0.5s ease,opacity 0.3s;}}
.cab-cat{{font-size:9px;font-weight:700;color:rgba(255,255,255,.82);text-transform:uppercase;letter-spacing:.04em;}}
.cab-amt{{font-size:13px;font-weight:800;color:#fff;margin-top:1px;}}
.cab-pct{{font-size:10px;color:rgba(255,255,255,.68);margin-top:1px;}}
.cab-line{{position:absolute;bottom:0;width:1px;opacity:0.5;transform:translateX(-50%);}}
/* bar */
.bar-container{{width:100%;height:130px;display:flex;position:relative;z-index:2;
  border-radius:8px;overflow:hidden;border:1px solid #2a2a2a;}}
.bar-segment{{height:100%;position:relative;border-right:2px solid rgba(255,255,255,0.18);
  flex-shrink:0;overflow:hidden;transition:width 0.7s cubic-bezier(.4,0,.2,1);}}
.bar-segment:first-child{{border-radius:8px 0 0 8px;}}
.bar-segment:last-child{{border-right:none;border-radius:0 8px 8px 0;}}
.bar-segment:hover{{filter:brightness(1.12);}}
.seg-label{{position:absolute;inset:0;display:flex;flex-direction:column;justify-content:center;
  padding:0 11px;pointer-events:none;overflow:hidden;}}
.seg-label-cat{{font-size:9px;font-weight:700;color:rgba(255,255,255,.82);text-transform:uppercase;
  letter-spacing:.05em;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}}
.seg-label-amt{{font-size:14px;font-weight:800;color:#fff;line-height:1.25;margin-top:2px;white-space:nowrap;}}
.seg-label-pct{{font-size:10px;color:rgba(255,255,255,.68);line-height:1.2;margin-top:1px;white-space:nowrap;}}
/* mini label for tight segments (2-5.5%) */
.seg-label-mini{{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  padding:0 4px;pointer-events:none;overflow:hidden;}}
.seg-label-mini-amt{{font-size:10px;font-weight:800;color:#fff;white-space:nowrap;
  writing-mode:vertical-rl;transform:rotate(180deg);}}
</style>
<div id="conc-root">
  <div class="wc-label">THE CONCENTRATION</div>
  <div class="wc-headline">Most of it went to very few hands.</div>
  <div class="wc-sub">Of <span id="conc-sub-total">${_conc_total:.0f}B</span> spent globally on advertising in <span id="conc-sub-yr">{_conc_yr}</span>, 4 companies captured <span id="conc-sub-pct">{_conc_top_share:.0f}%</span> of the market.</div>
  <div class="conc-controls">
    <button id="conc-play">&#9654; Play</button>
    <span class="conc-year" id="conc-yr-label">{_conc_yr}</span>
    <input type="range" id="conc-slider" min="0" value="0" />
    <span class="conc-total">Total: <span id="conc-total-label">${_conc_total:.0f}B</span></span>
  </div>
  <div id="conc-above"></div>
  <div class="bar-container" id="conc-bar"></div>
</div>
<script>
const ALL_YEARS = {_conc_all_years_json};
const SEG_COLORS = {_conc_seg_colors_json};
const SEG_ORDER = {_conc_seg_order_json};
const START_YR = {_conc_yr};
const years = Object.keys(ALL_YEARS).map(Number).sort((a,b)=>a-b);

const slider = document.getElementById('conc-slider');
const playBtn = document.getElementById('conc-play');
const yrLabel = document.getElementById('conc-yr-label');
const totalLabel = document.getElementById('conc-total-label');
const subTotal = document.getElementById('conc-sub-total');
const subYr = document.getElementById('conc-sub-yr');
const subPct = document.getElementById('conc-sub-pct');
const barEl = document.getElementById('conc-bar');
const aboveEl = document.getElementById('conc-above');

slider.max = years.length - 1;
let currentIdx = Math.max(0, years.indexOf(START_YR));
slider.value = currentIdx;

function fmtAmt(b) {{ return '$' + (b >= 1000 ? (b/1000).toFixed(1)+'T' : Math.round(b)+'B'); }}
function fmtPct(p) {{ return p.toFixed(1)+'%'; }}

/* Create one segment div per category (fixed order) */
const segEls = {{}};
SEG_ORDER.forEach(cat => {{
  const s = document.createElement('div');
  s.className = 'bar-segment';
  s.style.background = SEG_COLORS[cat] || '#333';
  s.style.width = '0%';
  s.dataset.cat = cat;
  barEl.appendChild(s);
  segEls[cat] = s;
}});

function updateYear(idx) {{
  currentIdx = Math.max(0, Math.min(years.length - 1, idx));
  slider.value = currentIdx;
  const yr = years[currentIdx];
  const data = ALL_YEARS[yr];
  if (!data) return;
  yrLabel.textContent = yr;
  totalLabel.textContent = fmtAmt(data.total);
  subTotal.textContent = fmtAmt(data.total);
  subYr.textContent = yr;

  const total = data.total;
  const vals = data.vals || {{}};
  const named4 = ['Alphabet','Meta','Amazon','Apple + MSFT'];
  const namedSum = named4.reduce((s,c) => s + (vals[c]||0), 0);
  subPct.textContent = total > 0 ? (namedSum/total*100).toFixed(0)+'%' : '—';

  /* Update bar widths */
  SEG_ORDER.forEach(cat => {{
    const v = vals[cat] || 0;
    const pct = total > 0 ? v / total * 100 : 0;
    const seg = segEls[cat];
    seg.style.width = pct + '%';
    /* Clear previous label */
    seg.innerHTML = '';
    if (pct <= 0) return;
    if (pct >= 5.5) {{
      const lbl = document.createElement('div'); lbl.className = 'seg-label';
      const c = document.createElement('div'); c.className = 'seg-label-cat'; c.textContent = cat;
      const a = document.createElement('div'); a.className = 'seg-label-amt'; a.textContent = fmtAmt(v);
      const p = document.createElement('div'); p.className = 'seg-label-pct'; p.textContent = fmtPct(pct);
      lbl.appendChild(c); lbl.appendChild(a); lbl.appendChild(p);
      seg.appendChild(lbl);
    }} else if (pct >= 1.8) {{
      const ml = document.createElement('div'); ml.className = 'seg-label-mini';
      const ma = document.createElement('div'); ma.className = 'seg-label-mini-amt'; ma.textContent = fmtAmt(v);
      ml.appendChild(ma); seg.appendChild(ml);
    }}
  }});

  /* Above-bar callouts for segments < 1.8% — only Cinema in late years */
  aboveEl.innerHTML = '';
  let aboveH = 0;
  const smallSegs = SEG_ORDER.map(cat => {{
    const v = vals[cat] || 0;
    const pct = total > 0 ? v / total * 100 : 0;
    return {{cat, v, pct}};
  }}).filter(x => x.pct > 0 && x.pct < 1.8);

  if (smallSegs.length > 0) {{
    aboveH = 90;
    aboveEl.style.height = aboveH + 'px';
    /* compute cumulative positions */
    let cum = 0;
    const positions = {{}};
    SEG_ORDER.forEach(cat => {{
      const v = vals[cat] || 0;
      const pct = total > 0 ? v / total * 100 : 0;
      positions[cat] = cum + pct / 2;
      cum += pct;
    }});
    smallSegs.forEach(item => {{
      const cx = positions[item.cat] / 100;
      const line = document.createElement('div'); line.className = 'cab-line';
      line.style.left = (cx*100)+'%'; line.style.height = aboveH+'px';
      line.style.background = SEG_COLORS[item.cat];
      aboveEl.appendChild(line);
      const card = document.createElement('div'); card.className = 'cab';
      card.style.background = SEG_COLORS[item.cat];
      card.style.bottom = '8px'; card.style.left = (cx*100)+'%';
      const cc = document.createElement('div'); cc.className = 'cab-cat'; cc.textContent = item.cat;
      const ca = document.createElement('div'); ca.className = 'cab-amt'; ca.textContent = fmtAmt(item.v);
      const cp = document.createElement('div'); cp.className = 'cab-pct'; cp.textContent = fmtPct(item.pct);
      card.appendChild(cc); card.appendChild(ca); card.appendChild(cp);
      aboveEl.appendChild(card);
    }});
  }} else {{
    aboveEl.style.height = '0px';
  }}
}}

/* Animation */
let timer = null;
let playing = false;
function tick() {{
  if (currentIdx >= years.length - 1) {{ stopPlay(); return; }}
  updateYear(currentIdx + 1);
}}
function startPlay() {{
  playing = true; playBtn.textContent = '⏸ Pause';
  if (currentIdx >= years.length - 1) updateYear(0);
  timer = setInterval(tick, 1100);
}}
function stopPlay() {{
  playing = false; playBtn.textContent = '▶ Play';
  clearInterval(timer); timer = null;
}}
playBtn.addEventListener('click', () => playing ? stopPlay() : startPlay());
slider.addEventListener('input', () => {{ stopPlay(); updateYear(Number(slider.value)); }});

updateYear(currentIdx);
</script>
""", height=500)
_separator()
st.components.v1.html(
    """
<div id="wm-rev-root">
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Syne:wght@700;800&display=swap');
html,body{margin:0;padding:0;background:#0d1117;border:none;outline:none;}
#wm-rev-root{background:#0d1117;color:#e6edf3;font-family:'DM Sans',sans-serif;width:100%;padding:32px 24px 24px;}
#wm-rev-root *{box-sizing:border-box;}
.wr-label{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:11px;letter-spacing:.28em;text-transform:uppercase;margin-bottom:10px;font-weight:700;}
.wr-headline{font-family:'Syne',sans-serif;font-size:28px;font-weight:800;margin:0 0 6px;color:#e6edf3;}
.wr-sub{color:#8b949e;font-size:14px;margin:0 0 36px;}
.wr-grid{display:flex;gap:0;align-items:flex-end;justify-content:space-between;width:100%;}
.wr-col{display:flex;flex-direction:column;align-items:center;flex:1;min-width:0;}
.wr-bars{display:flex;flex-direction:column;align-items:stretch;width:70%;margin:0 auto 6px;}
.wr-bar{width:100%;transition:height 1.2s cubic-bezier(.34,1.1,.64,1);position:relative;min-height:2px;}
.wr-bar-ad{background:#ff5b1f;}
.wr-bar-other{background:#1e3a5f;}
.wr-bar-other{border-radius:4px 4px 0 0;}
.wr-bar-ad{border-radius:0 0 4px 4px;}
.wr-name{font-size:9px;font-weight:700;color:#e6edf3;text-align:center;margin-bottom:4px;font-family:'Syne',sans-serif;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;width:100%;}
.wr-total{font-size:8px;color:#8b949e;text-align:center;}
.wr-legend{display:flex;gap:20px;margin-top:20px;}
.wr-leg{display:flex;align-items:center;gap:6px;font-size:11px;color:#8b949e;}
.wr-leg-dot{width:10px;height:10px;border-radius:2px;}
</style>
<div class="wr-label">REVENUE ANATOMY</div>
<div class="wr-headline">Not all revenue is advertising.</div>
<div class="wr-sub">Total 2024 revenue per company. Orange = ad revenue. Blue = everything else.</div>
<div class="wr-grid" id="wr-grid">
</div>
<div class="wr-legend">
  <div class="wr-leg"><div class="wr-leg-dot" style="background:#ff5b1f;"></div>Ad Revenue</div>
  <div class="wr-leg"><div class="wr-leg-dot" style="background:#1e3a5f;"></div>Other Revenue</div>
</div>
<script>
const companies=[
  {name:"Alphabet",total:350,ad:237,ticker:"GOOG"},
  {name:"Amazon",total:638,ad:56,ticker:"AMZN"},
  {name:"Apple",total:391,ad:18,ticker:"AAPL"},
  {name:"Microsoft",total:245,ad:18,ticker:"MSFT"},
  {name:"Meta",total:165,ad:164,ticker:"META"},
  {name:"Netflix",total:39,ad:2.4,ticker:"NFLX"},
  {name:"Disney",total:91,ad:3.4,ticker:"DIS"},
  {name:"Comcast",total:123,ad:6.8,ticker:"CMCSA"},
  {name:"Spotify",total:15.7,ad:2.1,ticker:"SPOT"},
  {name:"Roku",total:4.1,ad:3.8,ticker:"ROKU"},
].sort((a,b)=>b.total-a.total);
const maxTotal=Math.max(...companies.map(c=>c.total));
const maxH=240;
const grid=document.getElementById('wr-grid');
companies.forEach(c=>{
  const adH=Math.round((c.ad/maxTotal)*maxH);
  const otherH=Math.round(((c.total-c.ad)/maxTotal)*maxH);
  const adPct=Math.round((c.ad/c.total)*100);
  const col=document.createElement('div');
  col.className='wr-col';
  col.innerHTML=`
    <div class="wr-name">${c.name}</div>
    <div class="wr-bars">
      <div class="wr-bar wr-bar-other" style="height:0px" data-h="${otherH}"></div>
      <div class="wr-bar wr-bar-ad" style="height:0px" data-h="${adH}"></div>
    </div>
    <div class="wr-total">$${c.total>=10?Math.round(c.total)+'B':c.total+'B'} · ${adPct}% ad</div>
  `;
  grid.appendChild(col);
});
const io=new IntersectionObserver(entries=>{
  if(!entries[0].isIntersecting)return;
  document.querySelectorAll('.wr-bar').forEach(b=>{
    setTimeout(()=>{b.style.height=b.dataset.h+'px';},100);
  });
  io.unobserve(entries[0].target);
},{threshold:0.2});
io.observe(grid);
</script>
</div>
""",
    height=620,
)
_separator()

# Beat 3 — Treemap
_section(
    "The Revenue Map",
    "Not all revenue is advertising.",
    "Each rectangle represents one company, sized by total revenue. Color shows year-over-year growth — green is acceleration, red is contraction. Hover for details.",
)
try:
    if metrics.empty or "revenue" not in metrics.columns:
        st.info("Revenue treemap unavailable.")
    else:
        treemap_df = metrics[metrics["year"] == effective_year].copy()
        prev_df = metrics[metrics["year"] == (effective_year - 1)][["company", "revenue"]].rename(columns={"revenue": "prev_rev"})
        treemap_df = treemap_df.merge(prev_df, on="company", how="left")
        treemap_df["revenue_b"] = pd.to_numeric(treemap_df["revenue"], errors="coerce") / 1e3
        treemap_df["rev_yoy"] = _yoy_vec(treemap_df["revenue"], treemap_df["prev_rev"])
        treemap_df = treemap_df.dropna(subset=["company", "revenue_b"])
        if treemap_df.empty:
            st.info("Revenue treemap unavailable.")
        else:
            t_fig = px.treemap(
                treemap_df,
                path=["company"],
                values="revenue_b",
                color="rev_yoy",
                color_continuous_scale=["#ef4444", "#1f2937", "#22c55e"],
                color_continuous_midpoint=0,
                custom_data=["rev_yoy", "revenue_b"],
                title=f"Revenue by Company — {effective_year}",
            )
            t_fig.update_traces(
                hovertemplate="<b>%{label}</b><br>Revenue: $%{customdata[1]:.0f}B<br>YoY: %{customdata[0]:+.1f}%<extra></extra>",
                textfont=dict(color="white"),
            )
            _apply_dark_chart_layout(t_fig, height=420, margin=dict(l=0, r=0, t=46, b=0))
            st.plotly_chart(t_fig, use_container_width=True)
            st.caption("Rectangle size = total revenue. Color = YoY growth (green = growth, red = decline). Hover for details.")
except Exception:
    st.info("Revenue treemap unavailable.")
_separator()

# Beat 4 — Ad dependency bar
_section(
    "The Dependency",
    "Some live and die by advertising. Others barely care.",
    "This stack shows ad revenue as a share of total revenue for each company. High values indicate direct exposure to ad market volatility.",
)
try:
    import plotly.graph_objects as go
    if metrics.empty or "revenue" not in metrics.columns:
        st.info("Ad dependency chart unavailable.")
    else:
        dep_rows = []
        for _, row in metrics[metrics["year"] == effective_year].iterrows():
            company = str(row.get("company", ""))
            total_rev = float(pd.to_numeric(pd.Series([row.get("revenue", np.nan)]), errors="coerce").iloc[0] or 0.0)
            ad_rev = float(ad_lookup.get(company, {}).get("ad_revenue_musd", 0.0))
            ad_pct = min((ad_rev / total_rev * 100) if total_rev > 0 else 0.0, 100.0)
            dep_rows.append(
                {
                    "company": company,
                    "ad_pct": ad_pct,
                    "non_ad_pct": max(100 - ad_pct, 0),
                    "ad_rev_b": ad_rev / 1e3,
                    "total_rev_b": total_rev / 1e3,
                }
            )
        dep_rows = [r for r in dep_rows if r["company"]]
        if not dep_rows:
            st.info("Ad dependency chart unavailable.")
        else:
            dep_rows.sort(key=lambda x: x["ad_pct"], reverse=True)
            dep_rows = list(reversed(dep_rows))
            dep_fig = go.Figure()
            dep_fig.add_trace(
                go.Bar(
                    y=[r["company"] for r in dep_rows],
                    x=[r["ad_pct"] for r in dep_rows],
                    name="Ad Revenue %",
                    orientation="h",
                    marker=dict(color="#ff5b1f"),
                    customdata=[[r["ad_rev_b"], r["total_rev_b"]] for r in dep_rows],
                    hovertemplate="<b>%{y}</b><br>Ad Revenue: $%{customdata[0]:.1f}B (%{x:.1f}%)<br>Total Revenue: $%{customdata[1]:.1f}B<extra></extra>",
                )
            )
            dep_fig.add_trace(
                go.Bar(
                    y=[r["company"] for r in dep_rows],
                    x=[r["non_ad_pct"] for r in dep_rows],
                    name="Non-Ad Revenue %",
                    orientation="h",
                    marker=dict(color="rgba(255,255,255,0.08)"),
                    hoverinfo="skip",
                )
            )
            _apply_dark_chart_layout(
                dep_fig,
                height=430,
                extra_layout=dict(
                    barmode="stack",
                    xaxis=dict(range=[0, 100], ticksuffix="%", color="rgba(255,255,255,0.35)", gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.08)"),
                    yaxis=dict(color="rgba(255,255,255,0.35)", gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.08)"),
                    margin=dict(l=120, r=0, t=32, b=40),
                ),
            )
            st.plotly_chart(dep_fig, use_container_width=True)
            most_dep = dep_rows[0]
            least_dep = dep_rows[-1]
            st.caption(
                f"{most_dep['company']} derives {most_dep['ad_pct']:.0f}% of its revenue from advertising — it is functionally an ad company. "
                f"{least_dep['company']} sits at {least_dep['ad_pct']:.1f}% and is far less exposed."
            )
except Exception:
    st.info("Ad dependency chart unavailable.")
_separator()

# Beat 5 — Duopoly donut (animated timeline)
st.components.v1.html(
    """
<div style="margin:0;padding:56px 0 20px;background:transparent;font-family:'DM Sans',sans-serif;">
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Syne:wght@700;800&display=swap');
html,body{margin:0;padding:0;background:#0d1117;}
</style>
<div style="color:#ff5b1f;font-size:0.7rem;letter-spacing:0.28em;text-transform:uppercase;margin-bottom:10px;font-family:'DM Sans',sans-serif;">The Duopoly</div>
<div style="color:#ffffff;font-size:1.45rem;font-weight:700;line-height:1.25;margin-bottom:16px;font-family:'Syne',sans-serif;">Two companies. One grip.</div>
<div style="color:rgba(255,255,255,0.6);font-size:0.97rem;line-height:1.8;max-width:760px;">Alphabet and Meta dominate digital advertising. Watch their share of the global digital ad market grow from 2010 to today.</div>
</div>
""",
    height=160,
)
_duo_donut_html = (
    """
<div id="wm-duo-root">
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500&display=swap');
html,body{margin:0;padding:0;background:#0d1117;}
#wm-duo-root{background:#0d1117;padding:16px 24px 32px;font-family:'DM Sans',sans-serif;color:#e6edf3;}
#wm-duo-root *{box-sizing:border-box;}
.wmd-wrap{display:flex;gap:0;align-items:center;justify-content:center;min-height:300px;}
.wmd-canvas-col{display:flex;align-items:center;justify-content:center;flex-shrink:0;}
.wmd-info{padding:0 32px;min-width:200px;}
.wmd-year{font-family:'Syne',sans-serif;font-size:56px;font-weight:800;color:#e6edf3;line-height:1;transition:opacity .2s ease;}
.wmd-pct{font-family:'Syne',sans-serif;font-size:38px;font-weight:800;color:#ff5b1f;line-height:1;margin-top:10px;}
.wmd-pct-label{font-size:11px;color:#8b949e;margin-top:3px;letter-spacing:.05em;text-transform:uppercase;}
.wmd-total{font-size:14px;color:#8b949e;margin-top:14px;}
.wmd-total strong{color:#e6edf3;font-weight:700;}
.wmd-prog{height:3px;background:rgba(255,255,255,0.08);border-radius:2px;margin-top:20px;width:100%;}
.wmd-prog-bar{height:100%;background:#ff5b1f;border-radius:2px;width:0%;transition:width 1s ease;}
.wmd-legend{display:flex;gap:16px;margin-top:18px;flex-wrap:wrap;}
.wmd-leg{display:flex;align-items:center;gap:6px;font-size:11px;color:#8b949e;}
.wmd-leg-dot{width:10px;height:10px;border-radius:2px;flex-shrink:0;}
</style>
<div class="wmd-wrap">
  <div class="wmd-canvas-col">
    <canvas id="wmd-canvas" width="260" height="260"></canvas>
  </div>
  <div class="wmd-info">
    <div class="wmd-year" id="wmd-yr">2010</div>
    <div class="wmd-pct" id="wmd-pct">—%</div>
    <div class="wmd-pct-label">Duopoly share of digital ads</div>
    <div class="wmd-total" id="wmd-total">Total market: <strong>$—B</strong></div>
    <div class="wmd-prog"><div class="wmd-prog-bar" id="wmd-prog"></div></div>
    <div class="wmd-legend">
      <div class="wmd-leg"><div class="wmd-leg-dot" style="background:#ff5b1f;"></div>Alphabet + Meta</div>
      <div class="wmd-leg"><div class="wmd-leg-dot" style="background:rgba(255,255,255,0.12);border:1px solid rgba(255,255,255,0.2);"></div>Rest of market</div>
    </div>
  </div>
</div>
<script>
(function(){
var AD="""
    + _ad_json_str
    + """;
var GROUPM="""
    + _global_adv_json_str
    + """;
var canvas=document.getElementById('wmd-canvas');
var ctx=canvas.getContext('2d');
var cx=130,cy=130,r=110,ir=66;
function drawDonut(duoPct){
  var duo=Math.max(0,Math.min(1,duoPct));
  var gap=0.018;
  ctx.clearRect(0,0,260,260);
  var start=-Math.PI/2;
  var duoEnd=start+duo*(Math.PI*2)-gap;
  var restStart=start+duo*(Math.PI*2)+gap;
  var restEnd=start+Math.PI*2-gap;
  if(duo>0.01){ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,start,duoEnd);ctx.closePath();ctx.fillStyle='#ff5b1f';ctx.fill();}
  if(duo<0.99){ctx.beginPath();ctx.moveTo(cx,cy);ctx.arc(cx,cy,r,restStart,restEnd);ctx.closePath();ctx.fillStyle='rgba(255,255,255,0.1)';ctx.fill();}
  ctx.beginPath();ctx.arc(cx,cy,ir,0,Math.PI*2);ctx.fillStyle='#0d1117';ctx.fill();
}
var YEARS=Object.keys(AD).map(Number).sort(function(a,b){return a-b;});
function getYearStats(yr){
  var data=AD[yr]||{};
  var total=GROUPM[yr]||0;
  if(!total){Object.keys(data).forEach(function(k){if(data[k]>0)total+=data[k];});}
  var duo=0;
  if(data['Alphabet'])duo+=data['Alphabet'];
  if(data['Meta'])duo+=data['Meta'];
  return{total:total,duo:duo,pct:total>0?duo/total:0};
}
var current={pct:0};
var target={pct:0};
var rafId=null;
function lerp(a,b,t){return a+(b-a)*t;}
function animateTo(targetPct,onDone){
  var from=current.pct;
  var t=0;
  if(rafId)cancelAnimationFrame(rafId);
  function step(){
    t=Math.min(t+0.035,1);
    current.pct=lerp(from,targetPct,t);
    drawDonut(current.pct);
    if(t<1){rafId=requestAnimationFrame(step);}
    else{current.pct=targetPct;if(onDone)onDone();}
  }
  rafId=requestAnimationFrame(step);
}
var stepIdx=0,started=false,aTimer=null;
function updateLabels(yr,stats){
  var yrEl=document.getElementById('wmd-yr');
  if(yrEl){yrEl.style.opacity='0';setTimeout(function(){yrEl.textContent=yr;yrEl.style.opacity='1';},180);}
  var pEl=document.getElementById('wmd-pct');
  if(pEl)pEl.textContent=(stats.pct*100).toFixed(0)+'%';
  var tEl=document.getElementById('wmd-total');
  if(tEl)tEl.innerHTML='Total market: <strong>$'+stats.total.toFixed(0)+'B</strong>';
  var prEl=document.getElementById('wmd-prog');
  if(prEl){var idx=YEARS.indexOf(yr);var pct=YEARS.length>1?idx/(YEARS.length-1)*100:100;prEl.style.width=pct+'%';}
}
function runStep(){
  if(stepIdx>=YEARS.length){stepIdx=0;aTimer=setTimeout(runStep,3500);return;}
  var yr=YEARS[stepIdx];
  var stats=getYearStats(yr);
  var isLast=stepIdx===YEARS.length-1;
  stepIdx++;
  updateLabels(yr,stats);
  animateTo(stats.pct,function(){aTimer=setTimeout(runStep,isLast?3500:1300);});
}
var obs=new IntersectionObserver(function(entries){
  entries.forEach(function(e){
    if(e.isIntersecting&&!started){
      started=true;obs.disconnect();
      var s0=getYearStats(YEARS[0]);
      drawDonut(s0.pct);current.pct=s0.pct;
      updateLabels(YEARS[0],s0);
      stepIdx=1;aTimer=setTimeout(runStep,1500);
    }
  });
},{threshold:0.3});
var root=document.getElementById('wm-duo-root');
if(root)obs.observe(root);
})();
</script>
</div>
"""
)
st.components.v1.html(_duo_donut_html, height=400)
_separator()

# Beat 6 — M2 vs ad spend
_section(
    "The Money Printer",
    "When liquidity expands, ad markets follow.",
    "Both lines are indexed to 2010 = 100. The relationship between M2 and ad spend remains structurally tight through multiple cycles."
)
try:
    import plotly.graph_objects as go
    if m2_yearly_df.empty or _global_adv_totals.empty:
        st.info("M2 vs Ad Spend chart unavailable.")
    else:
        m2_scoped = m2_yearly_df.copy()
        m2_scoped["year"] = pd.to_numeric(m2_scoped["year"], errors="coerce")
        m2_scoped["m2_value"] = pd.to_numeric(m2_scoped["m2_value"], errors="coerce")
        m2_scoped = m2_scoped.dropna(subset=["year", "m2_value"])
        gm = _global_adv_totals.reset_index()
        gm.columns = ["year", "ad_total"]
        merged = m2_scoped.merge(gm, on="year", how="inner")
        merged = merged[merged["year"] >= 2010].sort_values("year")
        if merged.empty:
            st.info("M2 vs Ad Spend chart unavailable.")
        else:
            base_year = 2010 if (merged["year"] == 2010).any() else int(merged["year"].min())
            base_m2 = float(merged[merged["year"] == base_year]["m2_value"].iloc[0])
            base_ad = float(merged[merged["year"] == base_year]["ad_total"].iloc[0])
            if base_m2 <= 0 or base_ad <= 0:
                st.info("M2 vs Ad Spend chart unavailable.")
            else:
                merged["m2_idx"] = merged["m2_value"] / base_m2 * 100
                merged["ad_idx"] = merged["ad_total"] / base_ad * 100
                m2_fig = go.Figure()
                m2_fig.add_trace(go.Scatter(x=merged["year"], y=merged["m2_idx"], name="M2 Money Supply (indexed)", line=dict(color="#3b82f6", width=2.5)))
                m2_fig.add_trace(go.Scatter(x=merged["year"], y=merged["ad_idx"], name="Global Ad Spend (indexed)", line=dict(color="#ff9900", width=2.5), yaxis="y2"))
                m2_fig.add_vrect(x0=2020, x1=2021, fillcolor="rgba(255,255,255,0.05)", line_width=0, annotation_text="2020 stimulus", annotation_font_color="rgba(255,255,255,0.4)")
                _apply_dark_chart_layout(
                    m2_fig,
                    height=370,
                    margin=dict(l=0, r=60, t=32, b=40),
                    extra_layout=dict(
                        yaxis=dict(title="M2 (indexed, 2010=100)", color="rgba(255,255,255,0.35)", gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.08)"),
                        yaxis2=dict(title="Ad Spend (indexed)", overlaying="y", side="right", color="rgba(255,255,255,0.35)"),
                    ),
                )
                st.plotly_chart(m2_fig, use_container_width=True)
                st.caption("Both M2 money supply and global ad spend are indexed to 2010 = 100.")
except Exception:
    st.info("M2 vs Ad Spend chart unavailable.")
_separator()

# Beat 7 — Structural shift
_section(
    "The Structural Shift",
    "The ad market didn\'t just grow. It transformed.",
    "Traditional channels fade while search and retail media take share. Category mix, not just total spend, is now the core strategic signal."
)
try:
    import plotly.graph_objects as go
    if global_adv_df.empty:
        st.info("Structural shift chart unavailable.")
    else:
        # Group raw metric_types into display channels
        _channel_map = {
            "TV": ["Free TV", "Pay TV"],
            "Streaming / Video": ["Video Desktop", "Video Mobile"],
            "Search": ["Search Desktop", "Search Mobile"],
            "Social": ["Social Desktop", "Social Mobile"],
            "Display": ["Display Desktop", "Display Mobile"],
            "OOH": ["Traditional OOH", "Digital OOH"],
            "Print": ["Magazine", "Newspaper"],
            "Radio": ["Radio"],
            "Cinema / Other": ["Cinema", "Other Desktop", "Other Mobile"],
        }
        _channel_colors = {
            "TV": "#4472c4",
            "Streaming / Video": "#00bcd4",
            "Search": "#ff9900",
            "Social": "#ffd600",
            "Display": "#e57373",
            "OOH": "#888888",
            "Print": "#a5a5a5",
            "Radio": "#9c27b0",
            "Cinema / Other": "#607d8b",
        }
        gdf_agg = global_adv_df[global_adv_df["year"] >= 2010].copy()
        # Map metric_type → channel group, then sum per year
        _mt_to_ch = {mt: ch for ch, mts in _channel_map.items() for mt in mts}
        gdf_agg["channel"] = gdf_agg["metric_type"].map(_mt_to_ch)
        gdf_agg = gdf_agg.dropna(subset=["channel"])
        gdf_pivot = (
            gdf_agg.groupby(["year", "channel"])["value"].sum().unstack(fill_value=0) / 1_000.0
        )
        if gdf_pivot.empty:
            st.info("Structural shift chart unavailable.")
        else:
            s_fig = go.Figure()
            for ch in _channel_map:
                if ch not in gdf_pivot.columns:
                    continue
                s_fig.add_trace(go.Scatter(
                    x=gdf_pivot.index, y=gdf_pivot[ch],
                    name=ch, stackgroup="one",
                    line=dict(width=0), fillcolor=_channel_colors[ch],
                    hovertemplate=f"{ch}: $%{{y:.0f}}B<extra></extra>",
                ))
            _apply_dark_chart_layout(s_fig, height=390)
            st.plotly_chart(s_fig, use_container_width=True)
            st.caption("Global ad spend by channel category, sourced from country-level aggregates. Values in $B.")
except Exception:
    st.info("Structural shift chart unavailable.")
_separator()

# Beat 9 — Gapminder bubble
_section(
    "The Landscape",
    "Not all Big Tech is equal. Who won?",
    "Bubble size maps ad dependency, x-axis shows revenue growth, and y-axis captures market cap. This is the moving power map by year."
)
try:
    if metrics.empty or "revenue" not in metrics.columns or mcap_col not in metrics.columns:
        st.info("Bubble chart unavailable.")
    else:
        all_rows = []
        for yr_val in sorted(metrics["year"].dropna().astype(int).unique().tolist()):
            m = metrics[metrics["year"] == yr_val]
            p = metrics[metrics["year"] == (yr_val - 1)]
            ad_lookup_year = _load_ad_revenue_by_company(excel_path, source_stamp, yr_val) if excel_path else {}
            for _, row in m.iterrows():
                company = str(row.get("company", ""))
                rev = float(pd.to_numeric(pd.Series([row.get("revenue", np.nan)]), errors="coerce").iloc[0] or 0.0)
                prev_rows = p[p["company"] == company]
                prev_rev = None
                if not prev_rows.empty:
                    prev_rev = float(pd.to_numeric(pd.Series([prev_rows.iloc[0].get("revenue", np.nan)]), errors="coerce").iloc[0] or 0.0)
                rev_yoy = _yoy(rev, prev_rev) or 0.0
                if rev_yoy is not None and abs(rev_yoy) < 5:
                    rev_yoy = rev_yoy * 100
                mcap = float(pd.to_numeric(pd.Series([row.get(mcap_col, np.nan)]), errors="coerce").iloc[0] or 0.0)
                ad_rev = float(ad_lookup_year.get(company, {}).get("ad_revenue_musd", 0.0))
                ad_pct = min((ad_rev / rev * 100) if rev > 0 else 0.0, 100.0)
                all_rows.append(
                    {
                        "year": str(yr_val),
                        "company": company,
                        "rev_yoy": rev_yoy,
                        "market_cap_b": mcap / 1e3,
                        "ad_pct": max(ad_pct, 1.0),
                        "revenue_b": rev / 1e3,
                        "ad_rev_b": ad_rev / 1e3,
                    }
                )
        bubble_df = pd.DataFrame(all_rows).dropna(subset=["company", "year", "rev_yoy", "market_cap_b"])
        if bubble_df.empty:
            st.info("Bubble chart unavailable.")
        else:
            company_colors = {c: _company_color(c) for c in bubble_df["company"].unique()}
            b_fig = px.scatter(
                bubble_df,
                x="rev_yoy",
                y="market_cap_b",
                size="ad_pct",
                color="company",
                color_discrete_map=company_colors,
                animation_frame="year",
                animation_group="company",
                hover_name="company",
                custom_data=["revenue_b", "ad_pct", "ad_rev_b"],
                size_max=65,
                log_y=True,
                labels={"rev_yoy": "Revenue YoY Growth (%)", "market_cap_b": "Market Cap ($B)"},
            )
            b_fig.update_traces(
                hovertemplate="<b>%{hovertext}</b><br>Market Cap: $%{y:.0f}B<br>Rev YoY: %{x:+.1f}%<br>Ad Dependency: %{customdata[1]:.1f}%<br>Ad Revenue: $%{customdata[2]:.1f}B<extra></extra>"
            )
            _apply_dark_chart_layout(
                b_fig,
                height=590,
                extra_layout=dict(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(ticksuffix="%", color="rgba(255,255,255,0.35)", gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.08)", zeroline=True, zerolinecolor="rgba(255,255,255,0.15)"),
                    yaxis=dict(color="rgba(255,255,255,0.35)", gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.08)"),
                ),
            )
            st.plotly_chart(b_fig, use_container_width=True)
            latest = bubble_df[bubble_df["year"] == str(effective_year)]
            if not latest.empty:
                top_mcap = latest.nlargest(1, "market_cap_b").iloc[0]
                most_ad = latest.nlargest(1, "ad_pct").iloc[0]
                st.caption(
                    f"Bubble size = ad revenue dependency. {top_mcap['company']} leads on market cap at ${top_mcap['market_cap_b']:.0f}B. "
                    f"{most_ad['company']} is most ad-dependent at {most_ad['ad_pct']:.1f}% of total revenue."
                )
except Exception:
    st.info("Bubble chart unavailable.")
_separator()

# Beat 10 — Performance chart
_section(
    "The Market Bet",
    "Starting from the same base of 100, who compounded fastest?",
    "Top market-cap compounders are benchmarked against major indices on a normalized base."
)
try:
    import plotly.graph_objects as go
    company_ticker_fallback = {
        "Alphabet": "GOOGL",
        "Meta Platforms": "META",
        "Amazon": "AMZN",
        "Apple": "AAPL",
        "Microsoft": "MSFT",
        "Netflix": "NFLX",
        "Disney": "DIS",
        "Comcast": "CMCSA",
        "Spotify": "SPOT",
        "Roku": "ROKU",
        "Warner Bros. Discovery": "WBD",
        "Paramount Global": "PARA",
    }
    if metrics.empty or mcap_col not in metrics.columns:
        st.info("Performance chart unavailable.")
    else:
        min_year = int(metrics["year"].min())
        y_start = max(effective_year - 3, min_year)
        start_df = metrics[metrics["year"] == y_start][["company", mcap_col]].rename(columns={mcap_col: "mcap_start"})
        end_df = metrics[metrics["year"] == effective_year][["company", mcap_col]].rename(columns={mcap_col: "mcap_end"})
        perf = start_df.merge(end_df, on="company", how="inner")
        perf["tsr"] = _yoy_vec(perf["mcap_end"], perf["mcap_start"])
        perf = perf.dropna(subset=["tsr"])
        if perf.empty:
            st.info("Performance chart unavailable.")
        else:
            top3 = perf.nlargest(3, "tsr")["company"].tolist()
            if market_feed_df.empty:
                st.info("Performance chart unavailable.")
            else:
                p_fig = go.Figure()
                for idx_tag, idx_label, color, dash in [("^GSPC", "S&P 500", "white", "dash"), ("^IXIC", "Nasdaq", "#ff9900", "dot")]:
                    idx_feed = market_feed_df[market_feed_df["tag"].astype(str).str.upper() == idx_tag]
                    if idx_feed.empty:
                        idx_feed = market_feed_df[market_feed_df["asset"].astype(str).str.lower().str.contains(idx_label.lower(), na=False)]
                    if idx_feed.empty:
                        continue
                    idx_feed = idx_feed.sort_values("date")
                    base = float(idx_feed.iloc[0]["price"])
                    if base <= 0:
                        continue
                    norm = idx_feed["price"] / base * 100
                    p_fig.add_trace(go.Scatter(x=idx_feed["date"], y=norm, name=idx_label, line=dict(color=color, dash=dash, width=1.5), hovertemplate=f"{idx_label}: %{{y:.0f}}<extra></extra>"))
                for company in top3:
                    ticker = company_ticker_fallback.get(company)
                    if not ticker:
                        continue
                    co_feed = market_feed_df[market_feed_df["tag"].astype(str).str.upper() == ticker]
                    if co_feed.empty:
                        co_feed = market_feed_df[market_feed_df["asset"].astype(str).str.lower().str.contains(company.lower(), na=False)]
                    if co_feed.empty:
                        continue
                    co_feed = co_feed.sort_values("date")
                    base = float(co_feed.iloc[0]["price"])
                    if base <= 0:
                        continue
                    norm = co_feed["price"] / base * 100
                    p_fig.add_trace(go.Scatter(x=co_feed["date"], y=norm, name=company, line=dict(color=_company_color(company), width=2.5), hovertemplate=f"{company}: %{{y:.0f}}<extra></extra>"))
                if not p_fig.data:
                    st.info("Performance chart unavailable.")
                else:
                    _apply_dark_chart_layout(p_fig, height=370)
                    st.plotly_chart(p_fig, use_container_width=True)
                    best = perf.nlargest(1, "tsr").iloc[0]
                    st.caption(
                        f"All lines start at 100. A line at 200 means the asset doubled. {best['company']} was the top compounder at +{best['tsr']:.0f}% market cap growth {y_start}→{effective_year}. S&P 500 and Nasdaq shown as benchmarks."
                    )
except Exception:
    st.info("Performance chart unavailable.")
_separator()

# Beat 11 — Market cap then vs now
_section(
    "The Wealth Machine",
    "The market cap story, then vs now.",
    "A direct side-by-side view of scale migration across companies between the earliest available baseline and today."
)
try:
    import plotly.graph_objects as go
    if metrics.empty or mcap_col not in metrics.columns:
        st.info("Market cap history unavailable.")
    else:
        year_pool = sorted(metrics["year"].dropna().astype(int).unique().tolist())
        earlier = [y for y in year_pool if y < effective_year]
        y_then = year_pool[0] if year_pool and year_pool[0] < effective_year else (earlier[-1] if earlier else effective_year - 1)
        y_now = effective_year
        then_df = metrics[metrics["year"] == y_then][["company", mcap_col]].copy().rename(columns={mcap_col: "mcap_then"})
        now_df = metrics[metrics["year"] == y_now][["company", mcap_col]].copy().rename(columns={mcap_col: "mcap_now"})
        if then_df.empty or now_df.empty:
            st.info("Market cap history unavailable.")
        else:
            comp = then_df.merge(now_df, on="company", how="inner")
            if comp.empty:
                st.info("Market cap history unavailable.")
            else:
                comp = comp.sort_values("mcap_now", ascending=True)
                mc_fig = go.Figure()
                mc_fig.add_trace(go.Bar(y=comp["company"], x=comp["mcap_then"] / 1e3, name=str(y_then), orientation="h", marker=dict(color="rgba(255,255,255,0.24)"), hovertemplate=f"%{{y}} {y_then}: $%{{x:.0f}}B<extra></extra>"))
                mc_fig.add_trace(go.Bar(y=comp["company"], x=comp["mcap_now"] / 1e3, name=str(y_now), orientation="h", marker=dict(color="#ff5b1f"), hovertemplate=f"%{{y}} {y_now}: $%{{x:.0f}}B<extra></extra>"))
                _apply_dark_chart_layout(mc_fig, height=410, margin=dict(l=120, r=0, t=32, b=40), extra_layout=dict(barmode="group"))
                st.plotly_chart(mc_fig, use_container_width=True)
                total_then = comp["mcap_then"].sum() / 1e6
                total_now = comp["mcap_now"].sum() / 1e6
                growth = _yoy(total_now, total_then)
                cap = f"Combined market cap grew from ${total_then:.1f}T to ${total_now:.1f}T between {y_then} and {y_now}."
                if growth is not None:
                    cap += f" That\'s a +{growth:.0f}% increase over the period."
                st.caption(cap)
except Exception:
    st.info("Market cap history unavailable.")
_separator()

# Beat 12 — Human side bars
st.components.v1.html(
    """
<div id="wm-human-root">
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500&display=swap');
html,body{margin:0;padding:0;background:#0d1117;}
#wm-human-root{background:#0d1117;padding:32px 24px;font-family:'DM Sans',sans-serif;color:#e6edf3;}
.wh-label{color:#ff5b1f;font-family:'Syne',sans-serif;font-size:11px;letter-spacing:.28em;text-transform:uppercase;font-weight:700;margin-bottom:10px;}
.wh-headline{font-family:'Syne',sans-serif;font-size:28px;font-weight:800;margin:0 0 6px;color:#e6edf3;}
.wh-sub{color:#8b949e;font-size:14px;margin:0 0 32px;}
.wh-row{display:flex;align-items:center;gap:16px;margin-bottom:18px;}
.wh-name{width:100px;font-family:'Syne',sans-serif;font-size:13px;font-weight:700;color:#e6edf3;text-align:right;flex-shrink:0;}
.wh-track{flex:1;height:28px;background:rgba(255,255,255,0.04);border-radius:4px;overflow:hidden;position:relative;}
.wh-fill{height:100%;border-radius:4px;width:0%;transition:width 1.4s cubic-bezier(.34,1.1,.64,1);}
.wh-val{position:absolute;right:8px;top:50%;transform:translateY(-50%);font-family:'Syne',sans-serif;font-size:11px;font-weight:700;color:#e6edf3;}
.wh-caption{font-size:12px;color:#8b949e;margin-top:8px;}
</style>
<div class="wh-label">THE HUMAN SIDE</div>
<div class="wh-headline">Behind every dollar: a human being.</div>
<div class="wh-sub">Paid subscribers and active users across the tracked universe. 2024.</div>
<div id="wh-rows"></div>
<div class="wh-caption" id="wh-caption"></div>
<script>
const companies=[
  {name:"YouTube",val:2500,color:"#ff0000",label:"2.5B users"},
  {name:"Spotify",val:675,color:"#1db954",label:"675M users"},
  {name:"Netflix",val:301,color:"#e50914",label:"301M subs"},
  {name:"Disney+",val:174,color:"#113ccf",label:"174M subs"},
  {name:"Amazon",val:200,color:"#ff9900",label:"200M Prime"},
  {name:"Max (WBD)",val:116,color:"#0047ab",label:"116M subs"},
  {name:"Paramount+",val:77,color:"#0033a0",label:"77M subs"},
  {name:"Roku",val:89,color:"#6f1ab1",label:"89M accounts"},
];
const max=Math.max(...companies.map(c=>c.val));
const rows=document.getElementById('wh-rows');
const total=companies.reduce((s,c)=>s+c.val,0);
companies.forEach(c=>{
  const pct=Math.round((c.val/max)*100);
  rows.innerHTML+=`<div class="wh-row">
    <div class="wh-name">${c.name}</div>
    <div class="wh-track"><div class="wh-fill" style="background:${c.color}" data-w="${pct}"></div><span class="wh-val">${c.label}</span></div>
  </div>`;
});
document.getElementById('wh-caption').textContent=`Combined: ${(total/1000).toFixed(1)}B people — nearly half the world's population uses at least one of these platforms daily.`;
const io=new IntersectionObserver(entries=>{
  if(!entries[0].isIntersecting)return;
  document.querySelectorAll('.wh-fill').forEach(el=>el.style.width=el.dataset.w+'%');
  io.unobserve(entries[0].target);
},{threshold:0.2});
io.observe(rows);
</script>
</div>
""",
    height=420,
)
_separator()

# Beat 13 — Live ticker
_section(
    "The Clock",
    "Every second you stay on this page, revenue keeps running.",
    "This meter starts at zero when the component loads and accumulates in real-time from annualized run-rate assumptions."
)
try:
    company_ticker_map = {
        "Alphabet": "GOOGL",
        "Meta Platforms": "META",
        "Amazon": "AMZN",
        "Apple": "AAPL",
        "Microsoft": "MSFT",
        "Netflix": "NFLX",
        "Disney": "DIS",
        "Comcast": "CMCSA",
        "Spotify": "SPOT",
        "Roku": "ROKU",
        "Warner Bros. Discovery": "WBD",
        "Paramount Global": "PARA",
    }

    ticker_data: list[tuple[str, str, float]] = []
    total_rps = 0.0
    seconds_per_year = 365 * 24 * 3600

    metrics_scope = pd.DataFrame()
    if not metrics.empty and {"company", "year", "revenue"}.issubset(metrics.columns):
        metrics_scope = metrics[metrics["year"] == effective_year].copy()
        if metrics_scope.empty:
            latest_year = int(metrics["year"].max())
            metrics_scope = metrics[metrics["year"] == latest_year].copy()
        if not metrics_scope.empty:
            metrics_scope["company"] = metrics_scope["company"].apply(_normalize_company_name)
            metrics_scope["revenue"] = pd.to_numeric(metrics_scope["revenue"], errors="coerce")
            metrics_scope = metrics_scope.dropna(subset=["company", "revenue"])
            metrics_scope = metrics_scope[metrics_scope["revenue"] > 0].copy()
            revenue_scale = 1_000_000 if metrics_scope["revenue"].median() > 1000 else 1_000_000_000
            metrics_scope = metrics_scope.sort_values("revenue", ascending=False)

            seen_companies: set[str] = set()
            for row in metrics_scope.itertuples(index=False):
                company = _normalize_company_name(getattr(row, "company", ""))
                if not company or company in seen_companies:
                    continue
                seen_companies.add(company)
                annual_revenue_usd = float(getattr(row, "revenue", 0.0) or 0.0) * revenue_scale
                if annual_revenue_usd <= 0:
                    continue
                rps = annual_revenue_usd / seconds_per_year
                total_rps += rps
                ticker_data.append((company, company_ticker_map.get(company, ""), rps))

    # Fallback for legacy datasets where company revenue sheet is unavailable.
    if not ticker_data:
        minute_df = _read_excel_sheet_cached(excel_path, "Company_minute&dollar_earned", source_stamp) if excel_path else pd.DataFrame()
        if not minute_df.empty:
            minute_df.columns = [str(c).strip() for c in minute_df.columns]
            platform_col = _find_col(minute_df, ["platform"]) or _find_col(minute_df, ["company"])
            revenue_col = _find_col(minute_df, ["revenue"])
            if platform_col and revenue_col:
                minute_df[revenue_col] = pd.to_numeric(minute_df[revenue_col], errors="coerce")
                minute_df = minute_df.dropna(subset=[platform_col, revenue_col])
                for _, row in minute_df.iterrows():
                    platform = _normalize_company_name(row.get(platform_col, ""))
                    annual_revenue_usd = float(row.get(revenue_col, 0.0) or 0.0) * 1_000_000_000
                    if not platform or annual_revenue_usd <= 0:
                        continue
                    rps = annual_revenue_usd / seconds_per_year
                    total_rps += rps
                    ticker_data.append((platform, company_ticker_map.get(platform, ""), rps))

    if not ticker_data:
        st.info("Revenue ticker unavailable.")
    else:
        rows_html = ""
        for company, ticker, rps in ticker_data:
            logo_b64 = _resolve_logo(company, logos)
            logo_html = (
                f"<img src='data:image/png;base64,{logo_b64}' alt='{escape(company)} logo' "
                "style='width:30px;height:30px;object-fit:contain;border-radius:50%;"
                "background:rgba(148,163,184,0.12);border:1px solid rgba(148,163,184,0.26);padding:3px;' />"
                if logo_b64
                else "<span style='width:30px;height:30px;display:inline-flex;align-items:center;justify-content:center;"
                "border-radius:50%;background:rgba(148,163,184,0.16);color:#ffffff;'>•</span>"
            )
            ticker_label = f" · {ticker}" if ticker else ""
            rows_html += (
                "<div style='display:flex;justify-content:space-between;align-items:center;padding:12px 0;"
                "border-bottom:1px solid rgba(255,255,255,0.07);'>"
                "<div style='display:inline-flex;align-items:center;gap:8px;'>"
                f"{logo_html}<span style='color:#ffffff;font-weight:600;font-size:0.92rem;'>{escape(company)}</span>"
                f"<span style='color:rgba(255,255,255,0.55);font-size:0.75rem;'>{escape(ticker_label)}</span>"
                "</div>"
                f"<span data-rps='{rps:.6f}' style='color:#ff5b1f;font-family:monospace;font-size:1.2rem;font-weight:800;'>$0</span>"
                "</div>"
            )

        rows_html += (
            "<div style='display:flex;justify-content:space-between;align-items:center;padding:14px 0 0;'>"
            "<span style='color:rgba(255,255,255,0.45);font-size:0.88rem;'>Combined</span>"
            f"<span data-rps='{total_rps:.6f}' style='color:white;font-family:monospace;font-size:1.3rem;font-weight:800;'>$0</span>"
            "</div>"
        )

        component_height = int(min(860, max(240, 118 + len(ticker_data) * 50)))
        st.components.v1.html(
            f"""
            <style>
              html, body {{
                margin: 0;
                padding: 0;
                background: #0d1117;
                border: none;
                outline: none;
              }}
            </style>
            <div style="background:#0d1117;padding:20px 18px;border-radius:12px;font-family:sans-serif;">
              {rows_html}
              <div style="color:rgba(255,255,255,0.3);font-size:0.72rem;margin-top:14px;">
                Based on {effective_year} annual revenue ÷ seconds per year. Updates every 120ms since you opened this page.
              </div>
            </div>
            <script>
              (function() {{
                var t0 = Date.now();
                var els = document.querySelectorAll('[data-rps]');
                setInterval(function() {{
                  var elapsed = (Date.now() - t0) / 1000;
                  els.forEach(function(el) {{
                    var rps = parseFloat(el.getAttribute('data-rps'));
                    el.textContent = '$' + (rps * elapsed).toLocaleString('en-US', {{
                      minimumFractionDigits: 0,
                      maximumFractionDigits: 0
                    }});
                  }});
                }}, 120);
              }})();
            </script>
            """,
            height=component_height,
        )
except Exception as exc:
    st.info(f"Revenue ticker unavailable: {exc}")
_separator()

# Beat 14 — Transcript pulse
_section(
    "The Human Voice",
    "Here\'s what management teams are saying.",
    "Quotes are pulled from the transcript intelligence layer and filtered to the most relevant executive statements.",
    section_class="human-voice-fix",
)
_render_transcript_pulse_strip(effective_year, selected_quarter)
_separator()

# Beat 15 — Stock tape (no section header, just the tape)
_render_stock_price_strip(market_feed_df)
_separator()

# Gateway section
st.markdown(
    """
    <div style="padding:64px 0 32px;text-align:center;">
      <div style="color:#ff5b1f;font-size:0.7rem;letter-spacing:0.28em;
                  text-transform:uppercase;margin-bottom:12px;">Your Turn</div>
      <div style="color:white;font-size:2.2rem;font-weight:800;
                  margin-bottom:8px;">Go deeper.</div>
      <div style="color:rgba(255,255,255,0.35);font-size:1rem;
                  margin-bottom:36px;">Pick your path.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("Overview — Macro and Market", use_container_width=True, key="home_gateway_overview"):
        st.switch_page("pages/00_Overview.py")
with col2:
    if st.button("Earnings — Company Deep Dives", use_container_width=True, key="home_gateway_earnings"):
        st.switch_page("pages/01_Earnings.py")
with col3:
    if st.button("Genie — Ask the Data", use_container_width=True, key="home_gateway_genie"):
        st.switch_page("pages/04_Genie.py")

source_label = str(workbook_path) if workbook_path else "not found"
st.markdown(
    f"<div style='margin-top:32px;padding-top:14px;border-top:1px solid rgba(255,255,255,0.12);color:rgba(255,255,255,0.45);font-size:0.84rem;text-align:center;'>Source: {escape(source_label)} • Period baseline: {effective_year} {selected_quarter}</div>",
    unsafe_allow_html=True,
)

st.markdown("</div>", unsafe_allow_html=True)
