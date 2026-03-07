#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


STALE_MINUTES = 30


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    return out


def _pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = set(df.columns)
    for cand in candidates:
        c = str(cand).strip().lower()
        if c in cols:
            return c
    return None


def _split_keywords(value: object) -> List[str]:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return []
    parts = re.split(r"[,;|/]", text)
    cleaned = [p.strip() for p in parts if p and p.strip()]
    return cleaned


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        if pd.isna(num):
            return float(default)
        return float(num)
    except Exception:
        return float(default)


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(round(_to_float(value, float(default))))
    except Exception:
        return int(default)


def _resolve_workbook(repo_root: Path) -> Optional[Path]:
    candidates = [
        repo_root / "app" / "attached_assets" / "Earnings + stocks  copy.xlsx",
        repo_root / "Earnings + stocks  copy.xlsx",
        repo_root / "app" / "attached_assets" / "Earnings + stocks copy.xlsx",
        repo_root / "Earnings + stocks copy.xlsx",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return None


def _load_topic_colors(repo_root: Path) -> Dict[str, str]:
    workbook = _resolve_workbook(repo_root)
    if workbook is None:
        return {}
    try:
        df = pd.read_excel(workbook, sheet_name="Topics_Master")
    except Exception:
        return {}
    if df is None or df.empty:
        return {}
    df = _normalize_columns(df)
    if "is_active" in df.columns:
        active = pd.to_numeric(df["is_active"], errors="coerce").fillna(1).astype(int)
        df = df[active == 1]
    id_col = _pick_col(df, ["topic_id", "topic", "topic_key"])
    label_col = _pick_col(df, ["topic_label", "label", "topic_name"])
    color_col = _pick_col(df, ["color", "hex", "hex_color"])
    if color_col is None:
        return {}

    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        color = str(row.get(color_col, "") or "").strip()
        if not color:
            continue
        for col in [id_col, label_col]:
            if not col:
                continue
            key = str(row.get(col, "") or "").strip().lower()
            if key and key not in mapping:
                mapping[key] = color
    return mapping


def _default_topic_color(topic: str) -> str:
    palette = [
        "#2563EB", "#7C3AED", "#0891B2", "#EA580C", "#16A34A",
        "#DB2777", "#CA8A04", "#4F46E5", "#0F766E", "#B91C1C",
    ]
    idx = abs(hash(str(topic or ""))) % len(palette)
    return palette[idx]


def _topic_color(topic: str, topic_colors: Dict[str, str]) -> str:
    key = str(topic or "").strip().lower()
    if key in topic_colors:
        return topic_colors[key]
    return _default_topic_color(key)


def _is_recent(path: Path, now: datetime, max_age_minutes: int = STALE_MINUTES) -> bool:
    if not path.exists():
        return False
    age = now - datetime.fromtimestamp(path.stat().st_mtime)
    return age <= timedelta(minutes=max_age_minutes)


def _row_count(path: Path) -> int:
    if not path.exists():
        return 0
    if path.suffix.lower() == ".csv":
        try:
            return len(pd.read_csv(path))
        except Exception:
            return 0
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return len(payload)
            if isinstance(payload, dict):
                for key in ["transcripts", "items", "data", "index"]:
                    if key in payload and isinstance(payload[key], list):
                        return len(payload[key])
                return len(payload)
        except Exception:
            return 0
    return 0


def _db_transcript_count(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    try:
        with sqlite3.connect(str(db_path)) as conn:
            cur = conn.execute("SELECT COUNT(*) FROM transcripts")
            row = cur.fetchone()
            return int(row[0]) if row else 0
    except Exception:
        return 0


def _badge_class(priority: object) -> str:
    text = str(priority or "").strip().lower()
    if text == "high":
        return "badge-high"
    if text == "medium":
        return "badge-medium"
    if text == "low":
        return "badge-low"
    return "badge-neutral"


def _priority_text(priority: object) -> str:
    text = str(priority or "").strip().upper()
    return text if text else "N/A"


def _truncate_with_toggle(text: str, max_len: int = 200) -> str:
    raw = str(text or "")
    if len(raw) <= max_len:
        return f"<span>{escape(raw)}</span>"
    short = escape(raw[:max_len].rstrip()) + "..."
    full = escape(raw)
    return (
        "<div class='truncate-cell'>"
        f"<span class='short-text'>{short}</span>"
        f"<span class='full-text' style='display:none;'>{full}</span>"
        "<button type='button' class='link-btn' onclick='toggleFull(this)'>Show full</button>"
        "</div>"
    )


def _status_rows(
    repo_root: Path,
    topics_df: pd.DataFrame,
    topic_metrics_df: pd.DataFrame,
    kpis_df: pd.DataFrame,
    insights_df: pd.DataFrame,
    narratives_df: pd.DataFrame,
    now: datetime,
) -> List[Dict[str, str]]:
    outputs_dir = repo_root / "earningscall_transcripts"
    db_path = repo_root / "earningscall_intelligence.db"

    idx_candidates = [
        outputs_dir / "transcript_index.csv",
        outputs_dir / "transcript_index.json",
    ]
    idx_existing = [p for p in idx_candidates if p.exists()]
    if not idx_existing:
        idx_status = "❌"
        idx_records = "0"
    else:
        idx_status = "✅" if any(_is_recent(p, now) for p in idx_existing) else "⚠️"
        counts = []
        for p in idx_existing:
            recs = _row_count(p)
            counts.append(f"{p.name}: {recs}")
        idx_records = " | ".join(counts)

    db_status = "❌"
    db_records = "0"
    if db_path.exists():
        db_status = "✅" if _is_recent(db_path, now) else "⚠️"
        db_records = str(_db_transcript_count(db_path))

    topic_outputs = [
        outputs_dir / "transcript_topics.csv",
        outputs_dir / "topic_metrics.csv",
        outputs_dir / "transcript_kpis.csv",
    ]
    topic_exist = [p.exists() for p in topic_outputs]
    if not any(topic_exist):
        topic_status = "❌"
    elif all(topic_exist) and all(_is_recent(p, now) for p in topic_outputs):
        topic_status = "✅"
    else:
        topic_status = "⚠️"
    topic_records = (
        f"topics:{len(topics_df)} | metrics:{len(topic_metrics_df)} | kpis:{len(kpis_df)}"
    )

    insights_path = outputs_dir / "generated_insights_latest.csv"
    if not insights_path.exists():
        insights_status = "❌"
    elif _is_recent(insights_path, now):
        insights_status = "✅"
    else:
        insights_status = "⚠️"

    narratives_path = outputs_dir / "financial_narratives.csv"
    if not narratives_path.exists():
        narratives_status = "❌"
    elif _is_recent(narratives_path, now):
        narratives_status = "✅"
    else:
        narratives_status = "⚠️"

    return [
        {"script": "rebuild_transcript_index", "status": idx_status, "records": idx_records},
        {"script": "build_intelligence_db", "status": db_status, "records": db_records},
        {"script": "extract_transcript_topics", "status": topic_status, "records": topic_records},
        {"script": "generate_insights", "status": insights_status, "records": str(len(insights_df))},
        {"script": "generate_financial_narratives", "status": narratives_status, "records": str(len(narratives_df))},
    ]


def _render_status_table(rows: List[Dict[str, str]]) -> str:
    trs = []
    for row in rows:
        trs.append(
            "<tr>"
            f"<td>{escape(row['script'])}</td>"
            f"<td>{escape(row['status'])}</td>"
            f"<td>{escape(str(row['records']))}</td>"
            "</tr>"
        )
    return (
        "<table class='diag-table sortable' id='status-table'>"
        "<thead><tr><th>Script</th><th>Status</th><th>Records produced</th></tr></thead>"
        f"<tbody>{''.join(trs)}</tbody></table>"
    )


def _build_topic_debug_table(topics_df: pd.DataFrame, topic_colors: Dict[str, str]) -> Tuple[str, str]:
    if topics_df.empty:
        return "<p class='notice'>transcript_topics.csv not found or empty.</p>", ""

    company_col = _pick_col(topics_df, ["company"])
    year_col = _pick_col(topics_df, ["year"])
    quarter_col = _pick_col(topics_df, ["quarter"])
    topic_col = _pick_col(topics_df, ["topic"])
    keyword_col = _pick_col(topics_df, ["matched_keywords", "matched_keyword", "keywords", "keyword", "matched_terms", "terms"])
    raw_col = _pick_col(topics_df, ["raw_count", "mention_count", "matches", "count"])
    weighted_col = _pick_col(topics_df, ["weighted_score", "score", "weighted_count"])
    sentence_col = _pick_col(topics_df, ["sentence", "context", "context_sentence", "text", "paragraph_text"])

    rows_html = []
    detail_groups: Dict[str, List[Tuple[str, str, str, str]]] = defaultdict(list)

    for _, row in topics_df.iterrows():
        company = str(row.get(company_col, "") if company_col else "").strip()
        year = str(row.get(year_col, "") if year_col else "").strip()
        quarter = str(row.get(quarter_col, "") if quarter_col else "").strip()
        topic = str(row.get(topic_col, "") if topic_col else "").strip()
        keywords_val = row.get(keyword_col, "") if keyword_col else ""
        keywords = _split_keywords(keywords_val)
        if not keywords:
            keywords = [topic] if topic else []
        keywords_text = ", ".join(keywords)

        raw_count = _to_float(row.get(raw_col, 1) if raw_col else 1, 1.0)
        weighted_score = _to_float(row.get(weighted_col, raw_count) if weighted_col else raw_count, raw_count)
        sentence = str(row.get(sentence_col, "") if sentence_col else "").strip()

        topic_color = _topic_color(topic, topic_colors)
        rows_html.append(
            "<tr>"
            f"<td>{escape(company)}</td>"
            f"<td>{escape(year)}</td>"
            f"<td>{escape(quarter)}</td>"
            f"<td><span class='topic-pill' style='background:{escape(topic_color)}22; border-color:{escape(topic_color)}; color:{escape(topic_color)};'>{escape(topic)}</span></td>"
            f"<td>{escape(keywords_text)}</td>"
            f"<td>{raw_count:.2f}</td>"
            f"<td>{weighted_score:.2f}</td>"
            "</tr>"
        )

        if company and keywords:
            for kw in keywords:
                detail_groups[company].append((year, quarter, kw, sentence))

    table_html = (
        "<div class='table-toolbar'><label>Topic table search</label>"
        "<input type='text' class='table-search' data-target-table='topic-debug-table' placeholder='Filter topic rows...' /></div>"
        "<table class='diag-table sortable' id='topic-debug-table'>"
        "<thead><tr><th>Company</th><th>Year</th><th>Quarter</th><th>Topic</th><th>Matched Keywords</th><th>Raw Count</th><th>Weighted Score</th></tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody></table>"
    )

    details_html_parts = []
    for company in sorted(detail_groups):
        detail_rows = []
        for year, quarter, keyword, sentence in detail_groups[company]:
            detail_rows.append(
                "<tr>"
                f"<td>{escape(year)}</td>"
                f"<td>{escape(quarter)}</td>"
                f"<td>{escape(keyword)}</td>"
                f"<td>{escape(sentence)}</td>"
                "</tr>"
            )
        details_html_parts.append(
            "<details class='company-detail'>"
            f"<summary>{escape(company)} ({len(detail_rows)} keyword matches)</summary>"
            "<div class='detail-inner'>"
            "<table class='diag-table sortable'>"
            "<thead><tr><th>Year</th><th>Quarter</th><th>Matched Keyword</th><th>Sentence</th></tr></thead>"
            f"<tbody>{''.join(detail_rows)}</tbody></table>"
            "</div></details>"
        )
    details_html = "".join(details_html_parts)

    return table_html, details_html


def _split_signal_frames(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    suppressed_mask = pd.Series([False] * len(df), index=df.index)
    for col in ["is_suppressed", "suppressed"]:
        if col in df.columns:
            vals = df[col].astype(str).str.strip().str.lower()
            suppressed_mask = suppressed_mask | vals.isin(["1", "true", "yes", "y"])

    for col in ["status", "result", "outcome"]:
        if col in df.columns:
            vals = df[col].astype(str).str.strip().str.lower()
            suppressed_mask = suppressed_mask | vals.str.contains(r"suppressed|not\s*fired|blocked|negat", regex=True, na=False)

    suppressed = df[suppressed_mask].copy()
    fired = df[~suppressed_mask].copy()
    return fired, suppressed


def _render_signals_section(signals_df: pd.DataFrame) -> str:
    if signals_df.empty:
        return "<p class='notice'>transcript_signals.csv not found. No signal detection log available.</p>"

    signals_df = _normalize_columns(signals_df)
    fired_df, suppressed_df = _split_signal_frames(signals_df)

    company_col = _pick_col(fired_df, ["company"])
    year_col = _pick_col(fired_df, ["year"])
    quarter_col = _pick_col(fired_df, ["quarter"])
    label_col = _pick_col(fired_df, ["signal_label", "signal", "label", "signal_name"])
    priority_col = _pick_col(fired_df, ["priority", "severity", "level"])
    kw_col = _pick_col(fired_df, ["matched_keywords", "keywords", "matched_terms", "terms"])
    para_col = _pick_col(fired_df, ["paragraph_text", "text", "context", "sentence"])

    fired_rows = []
    for _, row in fired_df.iterrows():
        priority = row.get(priority_col, "") if priority_col else ""
        fired_rows.append(
            "<tr>"
            f"<td>{escape(str(row.get(company_col, '') if company_col else ''))}</td>"
            f"<td>{escape(str(row.get(year_col, '') if year_col else ''))}</td>"
            f"<td>{escape(str(row.get(quarter_col, '') if quarter_col else ''))}</td>"
            f"<td>{escape(str(row.get(label_col, '') if label_col else ''))}</td>"
            f"<td><span class='badge {_badge_class(priority)}'>{escape(_priority_text(priority))}</span></td>"
            f"<td>{escape(str(row.get(kw_col, '') if kw_col else ''))}</td>"
            f"<td>{_truncate_with_toggle(str(row.get(para_col, '') if para_col else ''))}</td>"
            "</tr>"
        )

    parts = [
        "<table class='diag-table sortable' id='signals-table'>"
        "<thead><tr><th>Company</th><th>Year</th><th>Quarter</th><th>Signal Label</th><th>Priority</th><th>Matched Keywords</th><th>Paragraph Text</th></tr></thead>"
        f"<tbody>{''.join(fired_rows)}</tbody></table>"
    ]

    if not suppressed_df.empty:
        s_company = _pick_col(suppressed_df, ["company"])
        s_year = _pick_col(suppressed_df, ["year"])
        s_quarter = _pick_col(suppressed_df, ["quarter"])
        s_label = _pick_col(suppressed_df, ["signal_label", "signal", "label", "signal_name"])
        s_kw = _pick_col(suppressed_df, ["matched_keywords", "keywords", "matched_terms", "terms"])
        s_reason = _pick_col(suppressed_df, ["suppression_reason", "reason", "status", "result", "outcome"])

        supp_rows = []
        for _, row in suppressed_df.iterrows():
            supp_rows.append(
                "<tr>"
                f"<td>{escape(str(row.get(s_company, '') if s_company else ''))}</td>"
                f"<td>{escape(str(row.get(s_year, '') if s_year else ''))}</td>"
                f"<td>{escape(str(row.get(s_quarter, '') if s_quarter else ''))}</td>"
                f"<td>{escape(str(row.get(s_label, '') if s_label else ''))}</td>"
                f"<td>{escape(str(row.get(s_kw, '') if s_kw else ''))}</td>"
                f"<td>{escape(str(row.get(s_reason, '') if s_reason else ''))}</td>"
                "</tr>"
            )
        parts.append("<h3 class='subhead'>Suppressed Signals</h3>")
        parts.append(
            "<table class='diag-table sortable' id='suppressed-signals-table'>"
            "<thead><tr><th>Company</th><th>Year</th><th>Quarter</th><th>Signal Label</th><th>Matched Keywords</th><th>Suppression Reason</th></tr></thead>"
            f"<tbody>{''.join(supp_rows)}</tbody></table>"
        )

    return "".join(parts)


def _split_narrative_frames(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()
    fired_mask = pd.Series([True] * len(df), index=df.index)

    if "fired" in df.columns:
        vals = df["fired"].astype(str).str.strip().str.lower()
        fired_mask = vals.isin(["1", "true", "yes", "y"])
    elif "result" in df.columns:
        vals = df["result"].astype(str).str.strip().str.lower()
        fired_mask = ~vals.str.contains(r"not\s*met|not\s*fired|false|0|fail", regex=True, na=False)
    elif "status" in df.columns:
        vals = df["status"].astype(str).str.strip().str.lower()
        fired_mask = vals.str.contains(r"fired|pass|met", regex=True, na=False)

    fired = df[fired_mask].copy()
    not_fired = df[~fired_mask].copy()
    return fired, not_fired


def _render_narratives_section(narratives_df: pd.DataFrame) -> str:
    if narratives_df.empty:
        return "<p class='notice'>financial_narratives.csv not found. No narrative generation log available.</p>"

    narratives_df = _normalize_columns(narratives_df)
    fired_df, not_fired_df = _split_narrative_frames(narratives_df)

    company_col = _pick_col(fired_df, ["company"])
    year_col = _pick_col(fired_df, ["year"])
    template_col = _pick_col(fired_df, ["template_id", "rule_id", "id", "insight_id", "narrative_id"])
    category_col = _pick_col(fired_df, ["category"])
    priority_col = _pick_col(fired_df, ["priority", "severity", "level"])
    text_col = _pick_col(fired_df, ["narrative_text", "text", "narrative", "comment"])

    fired_rows = []
    for _, row in fired_df.iterrows():
        priority = row.get(priority_col, "") if priority_col else ""
        fired_rows.append(
            "<tr>"
            f"<td>{escape(str(row.get(company_col, '') if company_col else ''))}</td>"
            f"<td>{escape(str(row.get(year_col, '') if year_col else ''))}</td>"
            f"<td>{escape(str(row.get(template_col, '') if template_col else ''))}</td>"
            f"<td>{escape(str(row.get(category_col, '') if category_col else ''))}</td>"
            f"<td><span class='badge {_badge_class(priority)}'>{escape(_priority_text(priority))}</span></td>"
            f"<td>{escape(str(row.get(text_col, '') if text_col else ''))}</td>"
            "</tr>"
        )

    parts = [
        "<table class='diag-table sortable' id='narratives-table'>"
        "<thead><tr><th>Company</th><th>Year</th><th>Template ID</th><th>Category</th><th>Priority</th><th>Narrative Text</th></tr></thead>"
        f"<tbody>{''.join(fired_rows)}</tbody></table>"
    ]

    if not not_fired_df.empty:
        nf_company_col = _pick_col(not_fired_df, ["company"])
        nf_year_col = _pick_col(not_fired_df, ["year"])
        nf_template_col = _pick_col(not_fired_df, ["template_id", "rule_id", "id", "insight_id", "narrative_id"])
        condition_col = _pick_col(not_fired_df, ["condition_checked", "condition", "rule_condition", "metric_condition"])
        actual_col = _pick_col(not_fired_df, ["actual_value", "actual", "value"])
        threshold_col = _pick_col(not_fired_df, ["threshold", "threshold_value", "target"])

        nf_rows = []
        for _, row in not_fired_df.iterrows():
            nf_rows.append(
                "<tr>"
                f"<td>{escape(str(row.get(nf_template_col, '') if nf_template_col else ''))}</td>"
                f"<td>{escape(str(row.get(nf_company_col, '') if nf_company_col else ''))}</td>"
                f"<td>{escape(str(row.get(nf_year_col, '') if nf_year_col else ''))}</td>"
                f"<td>{escape(str(row.get(condition_col, '') if condition_col else ''))}</td>"
                f"<td>{escape(str(row.get(actual_col, '') if actual_col else ''))}</td>"
                f"<td>{escape(str(row.get(threshold_col, '') if threshold_col else ''))}</td>"
                "<td>❌ NOT MET</td>"
                "</tr>"
            )

        parts.append("<h3 class='subhead'>Templates Evaluated But Not Fired</h3>")
        parts.append(
            "<table class='diag-table sortable' id='narratives-not-fired-table'>"
            "<thead><tr><th>Template ID</th><th>Company</th><th>Year</th><th>Condition checked</th><th>Actual value</th><th>Threshold</th><th>Result</th></tr></thead>"
            f"<tbody>{''.join(nf_rows)}</tbody></table>"
        )
    else:
        parts.append("<p class='notice'>No non-fired template logs were found in financial_narratives.csv.</p>")

    return "".join(parts)


def _render_raw_keyword_index(topics_df: pd.DataFrame) -> str:
    if topics_df.empty:
        return "<p class='notice'>No topic rows available to build the raw keyword index.</p>"

    company_col = _pick_col(topics_df, ["company"])
    topic_col = _pick_col(topics_df, ["topic"])
    keyword_col = _pick_col(topics_df, ["matched_keywords", "matched_keyword", "keywords", "keyword", "matched_terms", "terms"])
    raw_col = _pick_col(topics_df, ["raw_count", "mention_count", "matches", "count"])

    stats: Dict[str, Dict[str, object]] = {}
    for _, row in topics_df.iterrows():
        company = str(row.get(company_col, "") if company_col else "").strip()
        topic = str(row.get(topic_col, "") if topic_col else "").strip()
        raw_mentions = _to_int(row.get(raw_col, 1) if raw_col else 1, 1)
        if raw_mentions <= 0:
            raw_mentions = 1

        keywords = _split_keywords(row.get(keyword_col, "") if keyword_col else "")
        if not keywords:
            if topic:
                keywords = [topic]
            else:
                continue

        for kw in keywords:
            key = kw.lower()
            if key not in stats:
                stats[key] = {
                    "keyword": kw,
                    "total": 0,
                    "companies": set(),
                    "topic_counter": Counter(),
                    "company_counter": Counter(),
                }
            stats[key]["total"] = int(stats[key]["total"]) + raw_mentions
            if company:
                stats[key]["companies"].add(company)
                stats[key]["company_counter"][company] += raw_mentions
            if topic:
                stats[key]["topic_counter"][topic] += raw_mentions

    rows = []
    for _, payload in sorted(stats.items(), key=lambda item: int(item[1]["total"]), reverse=True):
        topic_counter: Counter = payload["topic_counter"]  # type: ignore[assignment]
        company_counter: Counter = payload["company_counter"]  # type: ignore[assignment]
        top_topic = topic_counter.most_common(1)[0][0] if topic_counter else ""
        top_company = company_counter.most_common(1)[0][0] if company_counter else ""
        rows.append(
            "<tr>"
            f"<td>{escape(str(payload['keyword']))}</td>"
            f"<td>{int(payload['total'])}</td>"
            f"<td>{len(payload['companies'])}</td>"
            f"<td>{escape(str(top_topic))}</td>"
            f"<td>{escape(str(top_company))}</td>"
            "</tr>"
        )

    return (
        "<table class='diag-table sortable' id='raw-keyword-table'>"
        "<thead><tr><th>Keyword</th><th>Total Mentions</th><th>Unique Companies Mentioning It</th><th>Topic Assigned</th><th>Top Company</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def _summary_counts(
    topics_df: pd.DataFrame,
    signals_fired_df: pd.DataFrame,
    narratives_fired_df: pd.DataFrame,
    insights_df: pd.DataFrame,
) -> Dict[str, int]:
    transcripts_processed = 0
    if not topics_df.empty:
        fp_col = _pick_col(topics_df, ["file_path"])
        if fp_col:
            transcripts_processed = int(topics_df[fp_col].dropna().astype(str).str.strip().nunique())
        else:
            key_cols = [c for c in ["company", "year", "quarter"] if c in topics_df.columns]
            if key_cols:
                transcripts_processed = int(topics_df[key_cols].drop_duplicates().shape[0])
            else:
                transcripts_processed = len(topics_df)

    return {
        "transcripts_processed": transcripts_processed,
        "total_topic_matches": int(len(topics_df)),
        "total_signals_fired": int(len(signals_fired_df)),
        "total_narratives_generated": int(len(narratives_fired_df)),
        "total_insights_generated": int(len(insights_df)),
    }


def _build_html(
    *,
    generated_at: datetime,
    summary: Dict[str, int],
    status_rows: List[Dict[str, str]],
    topic_table_html: str,
    topic_details_html: str,
    signals_html: str,
    narratives_html: str,
    keyword_index_html: str,
) -> str:
    generated_text = generated_at.strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Pipeline Diagnostic Report</title>
<style>
:root {{
  --bg:#f3f6fb;
  --panel:#ffffff;
  --border:#d9e0ea;
  --text:#0f172a;
  --muted:#475569;
  --sidebar:#0b1220;
  --sidebar-link:#cbd5e1;
  --accent:#2563eb;
  --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background:var(--bg); color:var(--text); }}
.layout {{ display:flex; min-height:100vh; }}
.sidebar {{ width:260px; background:var(--sidebar); color:#fff; padding:20px 16px; position:sticky; top:0; height:100vh; overflow:auto; }}
.sidebar h2 {{ margin:0 0 10px; font-size:1rem; letter-spacing:0.03em; text-transform:uppercase; color:#e2e8f0; }}
.sidebar a {{ display:block; color:var(--sidebar-link); text-decoration:none; padding:8px 10px; border-radius:8px; margin:4px 0; font-weight:600; font-size:0.92rem; }}
.sidebar a:hover {{ background:rgba(148,163,184,0.16); color:#fff; }}
.content {{ flex:1; padding:20px 24px 48px; }}
.topbar {{ display:flex; justify-content:space-between; align-items:center; gap:14px; margin-bottom:14px; }}
.topbar h1 {{ margin:0; font-size:1.35rem; }}
.search-wrap {{ width:min(520px,100%); }}
.search-wrap input {{ width:100%; border:1px solid var(--border); border-radius:10px; padding:10px 12px; font-size:0.92rem; }}
.section {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; margin:14px 0; overflow:hidden; }}
.section h2 {{ margin:0; padding:12px 14px; font-size:1rem; background:linear-gradient(90deg, #dbeafe, #eff6ff); border-bottom:1px solid var(--border); }}
.section-inner {{ padding:14px; }}
.summary-grid {{ display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:10px; margin-bottom:12px; }}
.summary-card {{ border:1px solid var(--border); border-radius:10px; padding:12px; background:#f8fafc; }}
.summary-label {{ font-size:0.78rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.05em; font-weight:700; }}
.summary-value {{ margin-top:6px; font-size:1.2rem; font-weight:800; }}
.diag-table {{ width:100%; border-collapse:collapse; table-layout:fixed; }}
.diag-table th, .diag-table td {{ border:1px solid var(--border); padding:8px 9px; vertical-align:top; font-size:0.82rem; }}
.diag-table th {{ background:#f8fafc; cursor:pointer; user-select:none; text-align:left; position:sticky; top:0; z-index:1; }}
.diag-table td {{ font-family:var(--mono); word-break:break-word; }}
.badge {{ display:inline-flex; align-items:center; justify-content:center; border-radius:999px; padding:2px 8px; font-weight:700; font-size:0.7rem; letter-spacing:0.04em; text-transform:uppercase; }}
.badge-high {{ background:rgba(239,68,68,.16); color:#b91c1c; border:1px solid rgba(239,68,68,.35); }}
.badge-medium {{ background:rgba(245,158,11,.16); color:#92400e; border:1px solid rgba(245,158,11,.35); }}
.badge-low {{ background:rgba(59,130,246,.14); color:#1d4ed8; border:1px solid rgba(59,130,246,.35); }}
.badge-neutral {{ background:rgba(148,163,184,.16); color:#334155; border:1px solid rgba(148,163,184,.35); }}
.notice {{ border:1px dashed var(--border); background:#f8fafc; padding:12px; border-radius:10px; color:var(--muted); font-size:0.9rem; }}
.topic-pill {{ display:inline-flex; align-items:center; border:1px solid; border-radius:999px; padding:2px 8px; font-weight:700; font-size:0.72rem; }}
.company-detail {{ margin:8px 0; border:1px solid var(--border); border-radius:10px; overflow:hidden; }}
.company-detail summary {{ padding:9px 10px; cursor:pointer; font-weight:700; background:#f8fafc; }}
.detail-inner {{ padding:10px; }}
.subhead {{ margin:14px 0 8px; font-size:0.95rem; }}
.link-btn {{ border:0; background:transparent; color:var(--accent); font-weight:700; cursor:pointer; padding:0; margin-left:8px; font-size:0.78rem; }}
.truncate-cell .full-text {{ white-space:pre-wrap; }}
.table-toolbar {{ display:flex; gap:10px; align-items:center; margin-bottom:8px; }}
.table-toolbar label {{ font-size:0.82rem; color:var(--muted); font-weight:700; }}
.table-toolbar input {{ border:1px solid var(--border); border-radius:8px; padding:7px 9px; min-width:280px; font-size:0.82rem; }}
@media (max-width:1200px) {{
  .summary-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
}}
@media (max-width:900px) {{
  .layout {{ flex-direction:column; }}
  .sidebar {{ width:100%; height:auto; position:relative; }}
  .summary-grid {{ grid-template-columns:1fr; }}
}}
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <h2>Diagnostics</h2>
    <a href="#pipeline-summary">1. Pipeline Summary</a>
    <a href="#topic-debugger">2. Topic Scoring Debugger</a>
    <a href="#signal-log">3. Signal Detection Log</a>
    <a href="#narrative-log">4. Narrative Generation Log</a>
    <a href="#keyword-index">5. Raw Keyword Index</a>
  </aside>

  <main class="content">
    <div class="topbar">
      <h1>Pipeline Diagnostic Report</h1>
      <div class="search-wrap">
        <input type="text" id="globalSearch" placeholder="Global search across all tables..." />
      </div>
    </div>

    <section class="section" id="pipeline-summary">
      <h2>Section 1 — Pipeline Summary</h2>
      <div class="section-inner">
        <div class="summary-grid">
          <div class="summary-card"><div class="summary-label">Report Generated</div><div class="summary-value">{escape(generated_text)}</div></div>
          <div class="summary-card"><div class="summary-label">Transcripts Processed</div><div class="summary-value">{summary['transcripts_processed']}</div></div>
          <div class="summary-card"><div class="summary-label">Total Topic Matches</div><div class="summary-value">{summary['total_topic_matches']}</div></div>
          <div class="summary-card"><div class="summary-label">Total Signals Fired</div><div class="summary-value">{summary['total_signals_fired']}</div></div>
          <div class="summary-card"><div class="summary-label">Total Narratives Generated</div><div class="summary-value">{summary['total_narratives_generated']}</div></div>
          <div class="summary-card"><div class="summary-label">Total Insights Generated</div><div class="summary-value">{summary['total_insights_generated']}</div></div>
        </div>
        {_render_status_table(status_rows)}
      </div>
    </section>

    <section class="section" id="topic-debugger">
      <h2>Section 2 — Topic Scoring Debugger</h2>
      <div class="section-inner">
        {topic_table_html}
        <h3 class="subhead">Keyword-to-Sentence Expansion by Company</h3>
        {topic_details_html if topic_details_html else "<p class='notice'>No keyword/sentence details available.</p>"}
      </div>
    </section>

    <section class="section" id="signal-log">
      <h2>Section 3 — Signal Detection Log</h2>
      <div class="section-inner">{signals_html}</div>
    </section>

    <section class="section" id="narrative-log">
      <h2>Section 4 — Narrative Generation Log</h2>
      <div class="section-inner">{narratives_html}</div>
    </section>

    <section class="section" id="keyword-index">
      <h2>Section 5 — Raw Keyword Index</h2>
      <div class="section-inner">{keyword_index_html}</div>
    </section>
  </main>
</div>

<script>
(function() {{
  const localQueries = {{}};
  let globalQuery = "";

  function normalize(text) {{
    return (text || "").toString().toLowerCase();
  }}

  function applyFilters() {{
    document.querySelectorAll('table.diag-table').forEach((table) => {{
      const tableId = table.id || "";
      const local = normalize(localQueries[tableId] || "");
      table.querySelectorAll('tbody tr').forEach((row) => {{
        const content = normalize(row.innerText);
        const passGlobal = !globalQuery || content.includes(globalQuery);
        const passLocal = !local || content.includes(local);
        row.style.display = (passGlobal && passLocal) ? "" : "none";
      }});
    }});
  }}

  const globalInput = document.getElementById('globalSearch');
  if (globalInput) {{
    globalInput.addEventListener('input', (event) => {{
      globalQuery = normalize(event.target.value);
      applyFilters();
    }});
  }}

  document.querySelectorAll('.table-search').forEach((input) => {{
    input.addEventListener('input', (event) => {{
      const target = event.target.getAttribute('data-target-table') || "";
      localQueries[target] = normalize(event.target.value);
      applyFilters();
    }});
  }});

  function parseSortableValue(raw) {{
    const text = (raw || "").trim();
    const numeric = text.replace(/[$,%xTByoY\s]/gi, '').replace(/,/g, '');
    if (numeric !== '' && !isNaN(Number(numeric))) {{
      return Number(numeric);
    }}
    return text.toLowerCase();
  }}

  function compareValues(a, b, asc) {{
    const va = parseSortableValue(a);
    const vb = parseSortableValue(b);
    if (typeof va === 'number' && typeof vb === 'number') {{
      return asc ? va - vb : vb - va;
    }}
    return asc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
  }}

  document.querySelectorAll('table.sortable th').forEach((th) => {{
    th.addEventListener('click', () => {{
      const table = th.closest('table');
      if (!table) return;
      const tbody = table.querySelector('tbody');
      if (!tbody) return;
      const headers = Array.from(th.parentElement.children);
      const colIdx = headers.indexOf(th);
      const asc = th.getAttribute('data-asc') !== 'true';
      headers.forEach((h) => h.removeAttribute('data-asc'));
      th.setAttribute('data-asc', asc ? 'true' : 'false');

      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort((r1, r2) => {{
        const c1 = r1.children[colIdx] ? r1.children[colIdx].innerText : '';
        const c2 = r2.children[colIdx] ? r2.children[colIdx].innerText : '';
        return compareValues(c1, c2, asc);
      }});
      rows.forEach((r) => tbody.appendChild(r));
      applyFilters();
    }});
  }});

  window.toggleFull = function(btn) {{
    const wrap = btn.closest('.truncate-cell');
    if (!wrap) return;
    const shortText = wrap.querySelector('.short-text');
    const fullText = wrap.querySelector('.full-text');
    const isExpanded = fullText && fullText.style.display !== 'none';
    if (shortText) shortText.style.display = isExpanded ? '' : 'none';
    if (fullText) fullText.style.display = isExpanded ? 'none' : '';
    btn.textContent = isExpanded ? 'Show full' : 'Show less';
  }};

  applyFilters();
}})();
</script>
</body>
</html>
"""


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = repo_root / "earningscall_transcripts"
    reports_dir = repo_root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now()

    topics_df = _normalize_columns(_read_csv(data_dir / "transcript_topics.csv"))
    topic_metrics_df = _normalize_columns(_read_csv(data_dir / "topic_metrics.csv"))
    kpis_df = _normalize_columns(_read_csv(data_dir / "transcript_kpis.csv"))
    signals_df = _normalize_columns(_read_csv(data_dir / "transcript_signals.csv"))
    narratives_df = _normalize_columns(_read_csv(data_dir / "financial_narratives.csv"))
    insights_df = _normalize_columns(_read_csv(data_dir / "generated_insights_latest.csv"))

    signals_fired_df, _ = _split_signal_frames(signals_df)
    narratives_fired_df, _ = _split_narrative_frames(narratives_df)

    summary = _summary_counts(topics_df, signals_fired_df, narratives_fired_df, insights_df)
    status_rows = _status_rows(
        repo_root=repo_root,
        topics_df=topics_df,
        topic_metrics_df=topic_metrics_df,
        kpis_df=kpis_df,
        insights_df=insights_df,
        narratives_df=narratives_df,
        now=now,
    )

    topic_colors = _load_topic_colors(repo_root)
    topic_table_html, topic_details_html = _build_topic_debug_table(topics_df, topic_colors)
    signals_html = _render_signals_section(signals_df)
    narratives_html = _render_narratives_section(narratives_df)
    keyword_index_html = _render_raw_keyword_index(topics_df)

    report_html = _build_html(
        generated_at=now,
        summary=summary,
        status_rows=status_rows,
        topic_table_html=topic_table_html,
        topic_details_html=topic_details_html,
        signals_html=signals_html,
        narratives_html=narratives_html,
        keyword_index_html=keyword_index_html,
    )

    stamp = now.strftime("%Y%m%d_%H%M")
    report_path = reports_dir / f"pipeline_diagnostic_{stamp}.html"
    latest_path = reports_dir / "latest_diagnostic.html"

    report_path.write_text(report_html, encoding="utf-8")
    latest_path.write_text(report_html, encoding="utf-8")

    print(f"Wrote diagnostic report: {report_path}")
    print(f"Updated latest report: {latest_path}")


if __name__ == "__main__":
    main()
