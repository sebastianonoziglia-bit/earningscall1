"""
Live transcript intelligence — reads directly from the Transcripts Excel sheet.
No pipeline scripts, no SQLite, no CSV files needed.
Works on HuggingFace with only the workbook available.
"""
from __future__ import annotations
import re
import logging
import pandas as pd
import streamlit as st

logger = logging.getLogger(__name__)

# ── TOPIC KEYWORDS ────────────────────────────────────────────────────────────
TOPIC_KEYWORDS = {
    "AI & Machine Learning": ["artificial intelligence", "machine learning", "ai model",
                               "large language model", "llm", "generative ai", "gemini",
                               "copilot", "gpt", "neural network", "deep learning"],
    "Advertising": ["advertising revenue", "ad revenue", "ad market", "programmatic",
                    "sponsored", "cpm", "arpu", "ad spend", "upfront", "scatter market"],
    "Streaming & Subscriptions": ["streaming", "subscribers", "paid members", "churn",
                                   "retention", "direct to consumer", "dtc", "password sharing"],
    "Cloud & Infrastructure": ["cloud revenue", "cloud growth", "aws", "azure", "gcp",
                                "google cloud", "infrastructure", "data center", "capex"],
    "Retail & E-Commerce": ["e-commerce", "online stores", "marketplace", "third party",
                             "prime", "fulfillment", "logistics", "retail media"],
    "Cost & Efficiency": ["headcount", "restructuring", "cost reduction", "efficiency",
                          "operating margin", "opex", "layoffs", "workforce"],
    "Macro & Economy": ["macroeconomic", "recession", "inflation", "interest rate",
                        "consumer spending", "currency", "foreign exchange", "fx"],
    "Content & IP": ["original content", "theatrical", "box office", "franchise",
                     "intellectual property", "licensing", "studio"],
    "Mobile & Devices": ["smartphone", "iphone", "pixel", "hardware", "wearables",
                         "connected devices", "mobile"],
    "Social & Engagement": ["daily active users", "dau", "monthly active users", "mau",
                            "engagement", "reels", "shorts", "tiktok", "feed"],
    "Payments & Fintech": ["payments", "fintech", "checkout", "buy now pay later", "bnpl"],
    "Privacy & Regulation": ["privacy", "regulation", "antitrust", "gdpr", "data protection",
                              "compliance", "consent"],
}

SIGNAL_CATEGORIES = ["Outlook", "Risks", "Opportunities"]

SIGNAL_ICONS = {
    "Outlook":       "🔭",
    "Risks":         "⚠️",
    "Opportunities": "🚀",
}

SIGNAL_COLORS = {
    "Outlook":       {"bg": "#eff6ff", "border": "#3b82f6", "tag": "#1d4ed8"},
    "Risks":         {"bg": "#fff7ed", "border": "#f97316", "tag": "#c2410c"},
    "Opportunities": {"bg": "#f0fdf4", "border": "#22c55e", "tag": "#15803d"},
}

OUTLOOK_KEYWORDS = [
    "we expect", "we anticipate", "going forward", "next quarter", "full year",
    "guidance", "we believe", "looking ahead", "heading into", "in the coming",
    "we remain confident", "we're well positioned", "positioned to", "on track",
    "our outlook", "we plan to", "we will continue", "for fiscal",
]

RISK_KEYWORDS = [
    "headwind", "challenge", "risk", "uncertainty", "slower", "softness",
    "pressure", "concern", "decline", "difficult", "competitive",
    "macro", "unfavorable", "weaker", "offset", "loss", "impairment",
    "caution", "volatility", "drag", "cost increase",
]

OPPORTUNITY_KEYWORDS = [
    "opportunity", "growth driver", "tailwind", "accelerate", "invest",
    "expand", "launch", "new market", "incremental", "upside",
    "untapped", "scale", "differentiate", "advantage", "momentum",
    "strong demand", "outperform", "gain share", "monetize",
]

CEO_TITLES = [
    "chief executive officer", "ceo", "president and chief executive",
    "co-founder and ceo", "co-founder and chief executive", "president & ceo",
]
CFO_TITLES = [
    "chief financial officer", "cfo", "senior vice president and chief financial",
    "executive vice president and chief financial", "evp and chief financial",
    "svp and chief financial",
]

FINANCIAL_SCORE_TERMS = [
    "revenue", "growth", "billion", "million", "margin", "profit",
    "operating income", "eps", "guidance", "quarter", "year",
    "advertising", "cloud", "subscribers", "users", "capex",
    "expect", "increase", "grew", "strong", "momentum", "opportunity",
    "record", "accelerat", "expand", "invest",
]


def score_quote_topics(quote: str) -> list[str]:
    """
    Return which TOPIC_KEYWORDS categories are mentioned in a quote.
    Used to tag signals, filter by topic, and enrich AI context.
    """
    q_lower = quote.lower()
    matched = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw in q_lower for kw in keywords):
            matched.append(topic)
    return matched


def enrich_signals_with_topics(signals: list[dict]) -> list[dict]:
    """
    Add a 'topics' list and use it as 'category' if category is missing.
    Call this after extracting any signals/quotes.
    """
    for sig in signals:
        quote = str(sig.get("quote", sig.get("text", ""))).lower()
        topics = score_quote_topics(quote)
        sig["topics"] = topics
        if not sig.get("category") and topics:
            sig["category"] = topics[0]
        elif not sig.get("category"):
            sig["category"] = "General"
    return signals


def _detect_role(title_lower: str) -> str:
    if any(t in title_lower for t in CEO_TITLES):
        return "CEO"
    if any(t in title_lower for t in CFO_TITLES):
        return "CFO"
    return ""


def _score_sentence(sentence: str) -> float:
    s = sentence.lower()
    return sum(1.0 for t in FINANCIAL_SCORE_TERMS if t in s)


def _parse_speaker_blocks(text: str) -> list[dict]:
    """
    Parse transcript text into speaker blocks.
    Handles formats:
      - "Sundar Pichai -- Chief Executive Officer"
      - "Ruth Porat - SVP and CFO"
      - "SUNDAR PICHAI: ..."
    Returns list of {speaker, role, sentences}
    """
    speaker_pattern = re.compile(
        r"^([A-Z][A-Za-z\s\.\'\-]{2,50})\s*(?:--|—|-|:)\s*(.{5,100})$"
    )
    lines = text.split("\n")
    blocks = []
    current_speaker = ""
    current_role = ""
    current_lines: list[str] = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        match = speaker_pattern.match(line)
        if match:
            # Save previous block
            if current_speaker and current_lines:
                full_text = " ".join(current_lines)
                sentences = [
                    s.strip() for s in re.split(r"(?<=[.!?])\s+", full_text)
                    if 40 < len(s.strip()) < 500
                ]
                blocks.append({
                    "speaker": current_speaker,
                    "role": current_role,
                    "sentences": sentences,
                })
            current_lines = []
            name = match.group(1).strip()
            title = match.group(2).strip().lower()
            role = _detect_role(title)
            if role:
                current_speaker = name
                current_role = role
            elif any(w in title for w in ["operator", "analyst", "moderator", "investor"]):
                current_speaker = ""
                current_role = ""
            else:
                current_speaker = name
                current_role = ""
        else:
            if len(line) > 20:
                current_lines.append(line)

    if current_speaker and current_lines:
        full_text = " ".join(current_lines)
        sentences = [
            s.strip() for s in re.split(r"(?<=[.!?])\s+", full_text)
            if 40 < len(s.strip()) < 500
        ]
        blocks.append({
            "speaker": current_speaker,
            "role": current_role,
            "sentences": sentences,
        })
    return blocks


@st.cache_data(ttl=3600, show_spinner=False)
def extract_ceo_cfo_quotes(
    excel_path: str,
    company: str,
    year: int,
    quarter: str = "",
    max_per_role: int = 3,
) -> dict:
    """
    Extract scored CEO/CFO quotes from Transcripts sheet.
    Returns {"CEO": [{"speaker","role","quote","score"}], "CFO": [...]}
    """
    if not excel_path:
        return {"CEO": [], "CFO": []}
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
        if not {"company", "year", "transcript_text"}.issubset(set(df.columns)):
            return {"CEO": [], "CFO": []}
        df["_c"] = df["company"].astype(str).str.strip().str.lower()
        df["_y"] = pd.to_numeric(df["year"], errors="coerce")
        comp = company.strip().lower()
        mask = (df["_c"] == comp) & (df["_y"] == int(year))
        rows = df[mask]
        if rows.empty:
            rows = df[(df["_c"] == comp) & (df["_y"].between(year - 1, year + 1))]
        if rows.empty:
            return {"CEO": [], "CFO": []}
        if quarter and "quarter" in df.columns:
            q_rows = rows[rows["quarter"].astype(str).str.upper().str.strip() == quarter.upper().strip()]
            if not q_rows.empty:
                rows = q_rows
        text = str(rows.iloc[0].get("transcript_text", "") or "")[:30000]
    except Exception:
        return {"CEO": [], "CFO": []}

    blocks = _parse_speaker_blocks(text)
    result: dict = {"CEO": [], "CFO": []}
    for block in blocks:
        role = block["role"]
        if role not in result:
            continue
        scored = sorted(
            [(round(_score_sentence(s), 2), s) for s in block["sentences"] if _score_sentence(s) > 0],
            key=lambda x: -x[0],
        )
        for score, sent in scored[:max_per_role]:
            if len(result[role]) < max_per_role:
                result[role].append({
                    "speaker": block["speaker"],
                    "role": role,
                    "quote": sent,
                    "score": score,
                })
    # Enrich with topic tags
    try:
        for role in result:
            result[role] = enrich_signals_with_topics(result[role])
    except Exception:
        pass
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def extract_pulse_quotes(
    excel_path: str,
    max_quotes: int = 30,
    min_score: float = 2.0,
) -> list[dict]:
    """
    Extract top-scored CEO/CFO quotes across ALL companies for the Human Voice strip.
    Returns list of {company, speaker, role, quote, score, year, quarter}
    """
    if not excel_path:
        return []
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
        if not {"company", "year", "quarter", "transcript_text"}.issubset(set(df.columns)):
            return []
    except Exception:
        return []

    # Get the most recent entry per company
    df["_y"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["_y"])
    df["_y"] = df["_y"].astype(int)
    latest_idx = df.groupby("company")["_y"].idxmax()
    df = df.loc[latest_idx].copy()

    results = []
    for _, row in df.iterrows():
        company = str(row.get("company", "")).strip()
        year = int(row.get("_y", 0))
        quarter = str(row.get("quarter", "")).strip()
        text = str(row.get("transcript_text", "") or "")[:30000]
        if not text:
            continue
        blocks = _parse_speaker_blocks(text)
        for block in blocks:
            if block["role"] not in ("CEO", "CFO"):
                continue
            scored = sorted(
                [(round(_score_sentence(s), 2), s) for s in block["sentences"] if _score_sentence(s) >= min_score],
                key=lambda x: -x[0],
            )
            if scored:
                score, sent = scored[0]  # best sentence per speaker block
                results.append({
                    "company": company,
                    "speaker": block["speaker"],
                    "role": block["role"],
                    "quote": sent,
                    "score": score,
                    "year": year,
                    "quarter": quarter,
                })

    results.sort(key=lambda x: -x["score"])
    return results[:max_quotes]


@st.cache_data(ttl=3600, show_spinner=False)
def extract_topic_metrics(
    excel_path: str,
    year: int,
    quarter: str = "",
) -> pd.DataFrame:
    """
    Build topic_metrics DataFrame directly from Transcripts sheet.
    Matches the schema expected by _render_transcript_topic_growth_chart:
    columns: year, quarter, topic, mention_count, companies_mentioned,
             total_companies, importance_pct, growth_pct
    """
    if not excel_path:
        return pd.DataFrame()
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
        if not {"company", "year", "quarter", "transcript_text"}.issubset(set(df.columns)):
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    df["_y"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["_y"])
    df["_y"] = df["_y"].astype(int)

    qnum = None
    if quarter:
        m = re.search(r"([1-4])", str(quarter))
        if m:
            qnum = int(m.group(1))

    curr = df[df["_y"] == int(year)]
    if qnum is not None and "quarter" in df.columns:
        q_curr = curr[curr["quarter"].astype(str).str.upper().str.strip() == f"Q{qnum}"]
        if not q_curr.empty:
            curr = q_curr

    if qnum is not None and qnum > 1:
        prior_qnum = qnum - 1
        prior = df[df["_y"] == int(year)]
        prior = prior[prior["quarter"].astype(str).str.upper().str.strip() == f"Q{prior_qnum}"]
    else:
        prior = df[df["_y"] == int(year) - 1]

    total_companies = curr["company"].nunique()
    if total_companies == 0:
        return pd.DataFrame()

    def count_topic(period_df: pd.DataFrame, keywords: list) -> tuple[int, int]:
        mention_count = 0
        companies: set = set()
        for _, row in period_df.iterrows():
            text = str(row.get("transcript_text", "") or "").lower()
            hits = sum(1 for kw in keywords if kw in text)
            if hits > 0:
                mention_count += hits
                companies.add(str(row.get("company", "")).strip())
        return mention_count, len(companies)

    rows = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        curr_count, curr_cos = count_topic(curr, keywords)
        prior_count, _ = count_topic(prior, keywords)
        importance_pct = (curr_cos / total_companies * 100) if total_companies > 0 else 0
        if prior_count > 0:
            growth_pct = (curr_count - prior_count) / prior_count * 100
        elif curr_count > 0:
            growth_pct = 100.0
        else:
            growth_pct = 0.0
        rows.append({
            "year": int(year),
            "quarter": f"Q{qnum}" if qnum else str(year),
            "topic": topic,
            "mention_count": curr_count,
            "companies_mentioned": curr_cos,
            "total_companies": total_companies,
            "importance_pct": round(importance_pct, 2),
            "growth_pct": round(growth_pct, 1),
        })

    result = pd.DataFrame(rows)
    result = result[result["mention_count"] > 0].copy()
    return result


@st.cache_data(ttl=3600, show_spinner=False)
def extract_outlook_risks_opportunities(
    excel_path: str,
    company: str,
    year: int,
    quarter: str = "",
    max_per_category: int = 3,
) -> dict:
    """
    Extract Outlook / Risks / Opportunities signals from CEO/CFO transcript blocks.
    Returns {"Outlook": [...], "Risks": [...], "Opportunities": [...]}
    each item: {speaker, role, quote, score, category}
    """
    if not excel_path:
        return {cat: [] for cat in SIGNAL_CATEGORIES}
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
        if not {"company", "year", "transcript_text"}.issubset(set(df.columns)):
            return {cat: [] for cat in SIGNAL_CATEGORIES}
        df["_c"] = df["company"].astype(str).str.strip().str.lower()
        df["_y"] = pd.to_numeric(df["year"], errors="coerce")
        comp = company.strip().lower()
        mask = (df["_c"] == comp) & (df["_y"] == int(year))
        rows = df[mask]
        if rows.empty:
            rows = df[(df["_c"] == comp) & (df["_y"].between(year - 1, year + 1))]
        if rows.empty:
            return {cat: [] for cat in SIGNAL_CATEGORIES}
        if quarter and "quarter" in df.columns:
            q_rows = rows[rows["quarter"].astype(str).str.upper().str.strip() == quarter.upper().strip()]
            if not q_rows.empty:
                rows = q_rows
        text = str(rows.iloc[0].get("transcript_text", "") or "")[:30000]
    except Exception:
        return {cat: [] for cat in SIGNAL_CATEGORIES}

    _cat_keywords = {
        "Outlook":       OUTLOOK_KEYWORDS,
        "Risks":         RISK_KEYWORDS,
        "Opportunities": OPPORTUNITY_KEYWORDS,
    }

    def _score_signal(sentence: str, keywords: list) -> float:
        s = sentence.lower()
        kw_hits = sum(1.5 for kw in keywords if kw in s)
        fin_hits = sum(1.0 for t in FINANCIAL_SCORE_TERMS if t in s)
        return round(kw_hits + fin_hits, 2)

    result: dict = {cat: [] for cat in SIGNAL_CATEGORIES}
    blocks = _parse_speaker_blocks(text)

    for block in blocks:
        role = block.get("role", "")
        role_bonus = 1.5 if role in ("CEO", "CFO") else 0.5
        for sentence in block.get("sentences", []):
            for cat, keywords in _cat_keywords.items():
                if len(result[cat]) >= max_per_category * 2:
                    continue
                if any(kw in sentence.lower() for kw in keywords):
                    score = _score_signal(sentence, keywords) + role_bonus
                    result[cat].append({
                        "speaker": block["speaker"],
                        "role": role,
                        "quote": sentence,
                        "score": score,
                        "category": cat,
                    })

    # Deduplicate and keep top N per category
    for cat in result:
        seen: set = set()
        deduped = []
        for sig in sorted(result[cat], key=lambda x: -x["score"]):
            key = sig["quote"][:60]
            if key not in seen:
                seen.add(key)
                deduped.append(sig)
        result[cat] = deduped[:max_per_category]

    return result


@st.cache_data(ttl=3600, show_spinner=False)
def extract_iconic_quotes(
    excel_path: str,
    year: int,
    quarter: str = "",
    max_quotes: int = 12,
) -> pd.DataFrame:
    """
    Extract best CEO/CFO quotes per company for Overview iconic quotes section.
    Matches schema: year, quarter, company, speaker, role_bucket, quote, score
    """
    pulse = extract_pulse_quotes(excel_path, max_quotes=max_quotes * 3)
    if not pulse:
        return pd.DataFrame()
    rows = []
    seen_companies: set = set()
    for q in pulse:
        co = q["company"]
        if co not in seen_companies:
            seen_companies.add(co)
            rows.append({
                "year": q["year"],
                "quarter": q["quarter"],
                "company": co,
                "speaker": q["speaker"],
                "role_bucket": q["role"],
                "quote": q["quote"],
                "score": q["score"],
            })
        if len(rows) >= max_quotes:
            break
    return pd.DataFrame(rows) if rows else pd.DataFrame()
