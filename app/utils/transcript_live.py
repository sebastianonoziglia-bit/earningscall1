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

SIGNAL_CATEGORIES = [
    "Outlook",
    "Risks",
    "Opportunities",
    "Investment",
    "Product Shifts",
    "User Behavior",
    "Monetization",
    "Strategic Direction",
    "Broadcaster Threats",
]

SIGNAL_ICONS = {
    "Outlook":             "🔭",
    "Risks":               "⚠️",
    "Opportunities":       "🚀",
    "Investment":          "💰",
    "Product Shifts":      "🔧",
    "User Behavior":       "👥",
    "Monetization":        "💵",
    "Strategic Direction": "♟️",
    "Broadcaster Threats": "📺",
}

SIGNAL_COLORS = {
    "Outlook":             {"bg": "#eff6ff", "border": "#3b82f6", "tag": "#1d4ed8"},
    "Risks":               {"bg": "#fff7ed", "border": "#f97316", "tag": "#c2410c"},
    "Opportunities":       {"bg": "#f0fdf4", "border": "#22c55e", "tag": "#15803d"},
    "Investment":          {"bg": "#faf5ff", "border": "#a855f7", "tag": "#7e22ce"},
    "Product Shifts":      {"bg": "#f0f9ff", "border": "#0ea5e9", "tag": "#0369a1"},
    "User Behavior":       {"bg": "#fdf4ff", "border": "#d946ef", "tag": "#a21caf"},
    "Monetization":        {"bg": "#fffbeb", "border": "#f59e0b", "tag": "#b45309"},
    "Strategic Direction": {"bg": "#f8fafc", "border": "#64748b", "tag": "#334155"},
    "Broadcaster Threats": {"bg": "#fff1f2", "border": "#f43f5e", "tag": "#be123c"},
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

INVESTMENT_KEYWORDS = [
    "capital expenditure", "capex", "we are investing",
    "infrastructure", "data center", "we hired", "headcount",
    "we acquired", "partnership", "we committed",
    "billion in", "we are spending", "investment in",
    "we are building out", "we are expanding capacity",
    "long term investment", "strategic investment",
]

PRODUCT_SHIFT_KEYWORDS = [
    "we launched", "we introduced", "we are building",
    "new capability", "we are developing", "we released",
    "artificial intelligence", "machine learning", "agent",
    "multimodal", "automation", "we integrated",
    "new feature", "new product", "new platform",
    "we are rolling out", "we shipped", "now available",
]

USER_BEHAVIOR_KEYWORDS = [
    "users are", "engagement", "time spent",
    "adoption", "daily active", "frequency",
    "behavior", "younger users", "mobile",
    "shift to", "increasing demand", "queries",
    "users come back", "retention", "habit",
    "watch time", "session", "more frequently",
]

MONETIZATION_KEYWORDS = [
    "new commercial", "monetize", "unlock",
    "new surface", "ad format", "performance",
    "retail media", "shoppable", "new pathway",
    "incremental revenue", "expand monetization",
    "new revenue stream", "commercial opportunity",
    "monetizable", "new inventory", "yield",
]

STRATEGIC_DIRECTION_KEYWORDS = [
    "we are the only", "full stack", "end to end",
    "vertical", "ecosystem", "we control",
    "expand into", "new market", "we are positioned",
    "competitive advantage", "moat", "differentiated",
    "we are uniquely", "only company", "at scale",
    "category leader", "market position",
]

BROADCASTER_THREAT_KEYWORDS = [
    "live", "sports", "broadcast", "linear tv",
    "connected tv", "ctv", "streaming rights",
    "upfront", "scatter", "brand advertising",
    "video advertising", "youtube tv", "live events",
    "nfl", "nba", "premier league", "content rights",
    "original content", "studio", "distribution",
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


def _score_sentence_advanced(
    sentence: str,
    keywords: list,
    role: str,
    sentence_idx: int,
    total_sentences: int,
) -> float:
    """Advanced scorer with negation, specificity, tense, position, and length factors."""
    s = sentence.strip()
    s_lower = s.lower()

    # 1. Negation check — skip negated sentences
    negation_patterns = [
        "we are not ", "we don't ", "we do not ",
        "we have not ", "we haven't ", "not investing",
        "no longer ", "we stopped ", "we ended ",
    ]
    if any(neg in s_lower for neg in negation_patterns):
        return 0.0

    # 2. Keyword hits — base score
    kw_hits = sum(1 for kw in keywords if kw.lower() in s_lower)
    if kw_hits == 0:
        return 0.0

    # 3. Specificity bonus — sentences with real numbers score higher
    has_number = bool(re.search(
        r'\$[\d,]+|\d+[\.,]\d+[BMK%]|\d{1,3}[BMK]\b|\d+\s*(?:billion|million|percent|%)',
        s, re.IGNORECASE
    ))
    specificity_bonus = 1.4 if has_number else 1.0

    # 4. Forward-looking tense bonus
    forward_phrases = [
        "we will", "we expect", "we are going to",
        "we plan to", "we intend to", "we are targeting",
        "going forward", "next quarter", "next year",
        "in 2025", "in 2026", "we anticipate",
        "we are positioned", "we believe",
    ]
    forward_bonus = 1.3 if any(p in s_lower for p in forward_phrases) else 1.0

    # 5. Role bonus
    role_bonus = 1.5 if role in ("CEO", "CFO") else 1.0

    # 6. Position bonus — early in transcript = strategic opening remarks
    position_ratio = 1 - (sentence_idx / max(total_sentences, 1))
    position_bonus = 1.0 + (position_ratio * 0.3)

    # 7. Length factor — prefer medium sentences
    length = len(s)
    if length < 50:
        len_factor = 0.6
    elif length < 80:
        len_factor = 0.85
    elif length <= 250:
        len_factor = 1.0
    else:
        len_factor = 0.8

    # 8. Financial term bonus
    fin_score = sum(0.4 for t in FINANCIAL_SCORE_TERMS if t in s_lower)

    return round(
        (kw_hits + fin_score)
        * specificity_bonus
        * forward_bonus
        * role_bonus
        * position_bonus
        * len_factor,
        3,
    )


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
    Extract signals across all 9 categories from CEO/CFO transcript blocks.
    Returns {category: [{speaker, role, quote, score, category}]}
    Uses advanced scoring: negation detection, specificity, forward tense, position.
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

    _kw_map = {
        "Outlook":             OUTLOOK_KEYWORDS,
        "Risks":               RISK_KEYWORDS,
        "Opportunities":       OPPORTUNITY_KEYWORDS,
        "Investment":          INVESTMENT_KEYWORDS,
        "Product Shifts":      PRODUCT_SHIFT_KEYWORDS,
        "User Behavior":       USER_BEHAVIOR_KEYWORDS,
        "Monetization":        MONETIZATION_KEYWORDS,
        "Strategic Direction": STRATEGIC_DIRECTION_KEYWORDS,
        "Broadcaster Threats": BROADCASTER_THREAT_KEYWORDS,
    }

    result: dict = {cat: [] for cat in _kw_map}
    seen: dict = {cat: set() for cat in _kw_map}
    blocks = _parse_speaker_blocks(text)

    # Pre-compute total sentence count for position bonus
    _all_sentences = [s for b in blocks for s in b.get("sentences", [])]
    _total_sents = len(_all_sentences)
    _sent_idx = 0

    for block in blocks:
        for sentence in block.get("sentences", []):
            s = sentence.strip()
            _sent_idx += 1
            if len(s) < 40 or len(s) > 400:
                continue
            for category, keywords in _kw_map.items():
                score = _score_sentence_advanced(
                    s, keywords, block.get("role", ""), _sent_idx, _total_sents
                )
                if score == 0.0:
                    continue
                key = s[:60].lower()
                if key in seen[category]:
                    continue
                seen[category].add(key)
                result[category].append({
                    "speaker": block["speaker"],
                    "role": block.get("role", ""),
                    "quote": s,
                    "score": score,
                    "category": category,
                })

    # Category exclusivity — keep sentence in top 2 categories only
    _all_quote_keys: dict = {}
    for cat in result:
        for sig in result[cat]:
            key = sig["quote"][:60].lower()
            if key not in _all_quote_keys:
                _all_quote_keys[key] = []
            _all_quote_keys[key].append((cat, sig["score"]))

    for key, cat_scores in _all_quote_keys.items():
        if len(cat_scores) <= 2:
            continue
        cat_scores.sort(key=lambda x: -x[1])
        keep_cats = {cs[0] for cs in cat_scores[:2]}
        for cat in result:
            if cat not in keep_cats:
                result[cat] = [
                    s for s in result[cat]
                    if s["quote"][:60].lower() != key
                ]

    # Sort and keep top N per category
    for cat in result:
        result[cat] = sorted(result[cat], key=lambda x: -x["score"])[:max_per_category]

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
