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

# ── SPEAKER EXTRACTION ───────────────────────────────────────────────────────
KNOWN_ROLE_TITLES = [
    "Chief Executive Officer", "CEO",
    "Chief Financial Officer", "CFO",
    "Chief Operating Officer", "COO",
    "Chief Technology Officer", "CTO",
    "Chief Revenue Officer", "CRO",
    "Chief Marketing Officer", "CMO",
    "Chief Product Officer", "CPO",
    "President", "Vice President",
    "Head of Investor Relations",
    "Investor Relations",
    "Senior Vice President",
    "Executive Vice President",
    "Managing Director",
    "General Counsel",
    "Analyst", "Operator",
]

ROLE_NORMALIZER = {
    "Chief Executive Officer": "CEO",
    "Chief Financial Officer": "CFO",
    "Chief Operating Officer": "COO",
    "Chief Technology Officer": "CTO",
    "Chief Revenue Officer": "CRO",
    "Chief Marketing Officer": "CMO",
    "Chief Product Officer": "CPO",
    "Senior Vice President": "SVP",
    "Executive Vice President": "EVP",
    "Vice President": "VP",
    "Head of Investor Relations": "IR",
    "Investor Relations": "IR",
    "President": "President",
    "Managing Director": "MD",
    "General Counsel": "GC",
    "Operator": "Operator",
    "Analyst": "Analyst",
    "CEO": "CEO", "CFO": "CFO", "COO": "COO", "CTO": "CTO",
}

FINANCIAL_SCORE_TERMS = [
    "revenue", "growth", "billion", "million", "margin", "profit",
    "operating income", "eps", "guidance", "quarter", "year",
    "advertising", "cloud", "subscribers", "users", "capex",
    "expect", "increase", "grew", "strong", "momentum", "opportunity",
    "record", "accelerat", "expand", "invest",
]

# ── FORWARD-LOOKING SIGNAL ENGINE ─────────────────────────────────────────────
# These keyword lists and scoring layers power the forward intelligence features
# across: Home CEO carousel, Earnings Forward Intelligence, Overview explorer,
# and Genie context building.

FUTURE_TENSE_MARKERS = [
    "we will", "we'll", "we plan to", "we expect", "we anticipate",
    "we intend", "we aim", "we are targeting", "we are working toward",
    "we are building", "we are investing", "going forward",
    "in the coming", "by end of", "over the next", "next quarter",
    "next year", "in 2025", "in 2026", "in 2027", "by 2025", "by 2026",
    "we are on track", "we expect to", "we plan on", "we will continue",
    "we will expand", "we will launch", "we will invest",
    "our goal is", "our target is", "our ambition is",
    "we are committed to", "we remain committed",
]

NEGATION_PREFIXES = [
    "we do not expect", "we cannot", "we don't expect",
    "we are not planning", "we have no plans", "we won't",
    "we will not", "we do not anticipate", "there is no guarantee",
    "we cannot guarantee", "we are unable to",
]

BOILERPLATE_PHRASES = [
    "as we have said before", "as previously mentioned",
    "as we noted last quarter", "as i mentioned earlier",
    "safe harbor", "forward-looking statements involve risk",
    "actual results may differ", "we cannot predict",
    "subject to change", "no obligation to update",
]

FORWARD_LOOKING_KEYWORDS = [
    # Guidance and targets
    "we expect", "our guidance", "looking ahead", "going forward",
    "we anticipate", "we project", "we forecast", "we target",
    "full year guidance", "next quarter guidance", "raised guidance",
    "updated our outlook", "we are raising", "we are lowering",
    # Investment and CapEx
    "capital expenditure", "capex", "we are investing", "we will invest",
    "infrastructure investment", "we are building", "we are deploying",
    "data center", "we are expanding capacity", "we are scaling",
    # Acquisitions and M&A
    "acquisition", "we acquired", "we are acquiring", "pending acquisition",
    "we closed the acquisition", "strategic acquisition", "we announced",
    "merger", "we intend to acquire", "we plan to acquire",
    # New products and launches
    "we will launch", "launching in", "coming soon", "planned release",
    "roadmap", "pipeline", "we are developing", "new product",
    "new feature", "new capability", "new service", "new market",
    # Geographic expansion
    "expanding into", "new markets", "international expansion",
    "we are entering", "new geographies", "we launched in",
    "we will expand to", "new region", "growing internationally",
    # Partnerships and deals
    "partnership with", "we partnered with", "strategic partnership",
    "we signed", "multi-year agreement", "we announced a deal",
    "collaboration with", "joint venture",
    # Hiring and headcount
    "we are hiring", "we plan to hire", "headcount growth",
    "we are growing our team", "new engineering talent",
    # Revenue and growth targets
    "revenue target", "we expect revenue", "growth target",
    "double digit growth", "we expect margins", "margin expansion",
    "operating leverage", "we expect to achieve",
    # AI and technology roadmap
    "ai roadmap", "we are training", "next generation model",
    "we are releasing", "new ai capability", "we are integrating ai",
    "autonomous", "agentic", "multimodal roadmap",
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

    base_score = round(
        (kw_hits + fin_score)
        * specificity_bonus
        * forward_bonus
        * role_bonus
        * position_bonus
        * len_factor,
        3,
    )

    # ── Additional verification layers (stacked on base score) ────────────

    # Layer 1 — Future tense detection (1.4x)
    if any(ft in s_lower for ft in FUTURE_TENSE_MARKERS):
        base_score *= 1.4

    # Layer 2 — Specificity bonus for forward-looking sentences (1.5x)
    # Fires when sentence has concrete numbers AND is forward-looking
    _has_concrete = bool(re.search(
        r'\$[\d,]+[BMbm]?|\d+\.?\d*\s*%|\b20(?:2[4-9]|3[0-9])\b|\bQ[1-4]\b',
        s, re.IGNORECASE,
    ))
    _is_forward = any(ft in s_lower for ft in FUTURE_TENSE_MARKERS)
    if _has_concrete and _is_forward:
        base_score *= 1.5

    # Layer 3 — Negation filter (hard penalty 0.1x)
    if any(neg in s_lower for neg in NEGATION_PREFIXES):
        base_score *= 0.1

    # Layer 4 — Boilerplate filter (0.05x)
    if any(bp in s_lower for bp in BOILERPLATE_PHRASES):
        base_score *= 0.05

    # Layer 5 — Speaker role bonus already applied above (role_bonus)

    return round(base_score, 3)


def _parse_speaker_blocks(text: str) -> list[dict]:
    """
    Parse transcript text into speaker blocks.
    Handles two formats:
    Format A (dash/colon): "Sundar Pichai -- Chief Executive Officer ..."
    Format B (inline):     "Sundar Pichai Chief Executive Officer Good afternoon..."
    Returns list of {speaker, role, sentences}
    """
    # ── Format A: explicit separator (-- / — / - / :) ──
    speaker_pattern = re.compile(
        r"^([A-Z][A-Za-z\s\.\'\-]{2,50})\s*(?:--|—|-|:)\s*(.{5,100})$"
    )
    # ── Format B: inline role titles (no separator) ──
    # Build regex: "FirstName LastName <known role> <speech...>"
    _role_alts = "|".join(re.escape(r) for r in sorted(KNOWN_ROLE_TITLES, key=len, reverse=True))
    inline_pattern = re.compile(
        r"^([A-Z][A-Za-z\.\'\-]+(?:\s+[A-Z][A-Za-z\.\'\-]+){0,3})\s+"
        r"(" + _role_alts + r")\s+(.{20,})",
        re.DOTALL,
    )

    def _save_block(speaker, role, lines_buf, out):
        if speaker and lines_buf:
            full_text = " ".join(lines_buf)
            sents = [
                s.strip() for s in re.split(r"(?<=[.!?])\s+", full_text)
                if 40 < len(s.strip()) < 500
            ]
            out.append({"speaker": speaker, "role": role, "sentences": sents})

    # First try splitting by double-newline (Format B — inline blocks)
    raw_blocks = re.split(r"\n\s*\n", text)
    blocks: list[dict] = []

    if len(raw_blocks) > 3:
        # Likely Format B (inline blocks separated by blank lines)
        for raw in raw_blocks:
            raw = raw.strip()
            if not raw or len(raw) < 30:
                continue
            if raw.startswith("Company:") or raw.startswith("---"):
                continue
            m = inline_pattern.match(raw)
            if m:
                name = m.group(1).strip()
                role_raw = m.group(2).strip()
                speech = m.group(3).strip()
                role = ROLE_NORMALIZER.get(role_raw, "")
                if not role:
                    role = _detect_role(role_raw.lower())
                if name.lower() in ("operator", "moderator"):
                    role = "Operator"
                sents = [
                    s.strip() for s in re.split(r"(?<=[.!?])\s+", speech)
                    if 40 < len(s.strip()) < 500
                ]
                blocks.append({"speaker": name, "role": role, "sentences": sents})

    if blocks:
        return blocks

    # Fallback: Format A (line-by-line with separators)
    lines = text.split("\n")
    current_speaker = ""
    current_role = ""
    current_lines: list[str] = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        match = speaker_pattern.match(line)
        if match:
            _save_block(current_speaker, current_role, current_lines, blocks)
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

    _save_block(current_speaker, current_role, current_lines, blocks)
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


@st.cache_data(ttl=3600, show_spinner=False)
def extract_forward_looking_signals(
    excel_path: str,
    company: str = "",
    year: int = 0,
    quarter: str = "",
    max_signals: int = 5,
) -> list[dict]:
    """
    Extract the highest-scoring forward-looking signals for a company.
    Uses multi-layer scoring: future tense + specificity + negation filter.
    Returns list of dicts with quote, speaker, role, score, year, quarter, company, category.
    Used by: Home page CEO carousel, Earnings Forward Intelligence panel,
    Overview cross-company outlook, Genie context building.
    """
    if not excel_path:
        return []
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
        if not {"company", "year", "transcript_text"}.issubset(set(df.columns)):
            return []
    except Exception:
        return []

    df["_c"] = df["company"].astype(str).str.strip().str.lower()
    df["_y"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["_y"])
    df["_y"] = df["_y"].astype(int)

    if company:
        comp = company.strip().lower()
        mask = df["_c"] == comp
        rows = df[mask]
        if year:
            yr_rows = rows[rows["_y"] == int(year)]
            if not yr_rows.empty:
                rows = yr_rows
            elif not rows.empty:
                rows = rows[rows["_y"] == rows["_y"].max()]
        if rows.empty:
            return []
        if quarter and "quarter" in df.columns:
            q_rows = rows[rows["quarter"].astype(str).str.upper().str.strip() == quarter.upper().strip()]
            if not q_rows.empty:
                rows = q_rows
    else:
        # Cross-company: use latest year per company
        if year:
            rows = df[df["_y"] == int(year)]
        else:
            latest_idx = df.groupby("_c")["_y"].idxmax()
            rows = df.loc[latest_idx]

    # Combined forward keywords for scoring
    _fwd_kw = FORWARD_LOOKING_KEYWORDS + OUTLOOK_KEYWORDS

    all_signals: list[dict] = []
    seen_keys: set = set()

    for _, row in rows.iterrows():
        _company = str(row.get("company", "")).strip()
        _year = int(row.get("_y", 0))
        _quarter = str(row.get("quarter", "")).strip()
        text = str(row.get("transcript_text", "") or "")[:30000]
        if not text:
            continue

        blocks = _parse_speaker_blocks(text)
        _all_sents = [s for b in blocks for s in b.get("sentences", [])]
        _total = len(_all_sents)
        _idx = 0

        for block in blocks:
            for sentence in block.get("sentences", []):
                s = sentence.strip()
                _idx += 1
                if len(s) < 40 or len(s) > 500:
                    continue
                # Must contain at least one forward-looking keyword
                s_lower = s.lower()
                if not any(kw in s_lower for kw in _fwd_kw):
                    continue

                score = _score_sentence_advanced(
                    s, _fwd_kw, block.get("role", ""), _idx, _total
                )
                if score < 0.5:
                    continue

                key = s[:60].lower()
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                # Determine category from signal keywords
                cat = "Outlook"
                for _cat, _kws in {
                    "Investment": INVESTMENT_KEYWORDS,
                    "Product Shifts": PRODUCT_SHIFT_KEYWORDS,
                    "Opportunities": OPPORTUNITY_KEYWORDS,
                    "Strategic Direction": STRATEGIC_DIRECTION_KEYWORDS,
                }.items():
                    if any(kw in s_lower for kw in _kws):
                        cat = _cat
                        break

                all_signals.append({
                    "quote": s,
                    "speaker": block["speaker"],
                    "role": block.get("role", ""),
                    "score": round(score, 3),
                    "year": _year,
                    "quarter": _quarter,
                    "company": _company,
                    "category": cat,
                    "has_number": bool(re.search(r'\$[\d,]+|\d+\.?\d*\s*%', s)),
                    "has_year_ref": bool(re.search(r'\b20(?:2[4-9]|3[0-9])\b', s)),
                })

    all_signals.sort(key=lambda x: -x["score"])
    result = all_signals[:max_signals]

    # Enrich with real speaker names from Company_Speakers sheet
    for sig in result:
        _sp = sig.get("speaker", "")
        _rl = sig.get("role", "")
        if _rl in ("CEO", "CFO", "COO", "CTO", "CRO", "CMO") and excel_path:
            _full = get_speaker_name(sig["company"], _rl, excel_path)
            if _full:
                sig["speaker"] = _full
                sig["speaker_display"] = f"{_full} \u00b7 {_rl}"
            else:
                sig["speaker_display"] = f"{_sp} \u00b7 {_rl}" if _rl else _sp
        else:
            sig["speaker_display"] = f"{_sp} \u00b7 {_rl}" if _rl else _sp

    return result


# TODO: extract_forward_looking_signals could also power:
# - Automated quarterly briefing generation (one-click "Q4 2024 briefing")
# - Editorial page forward-looking content (currently uses simpler keyword matching)
# - Overview cross-company forward intelligence cards
# - Automated Genie scenario context for "what if" questions


@st.cache_data(ttl=3600, show_spinner=False)
def extract_all_signals(
    excel_path: str,
    company: str,
    year: int,
    quarter: str = "",
    max_per_category: int = 3,
) -> list[dict]:
    """
    Extract signals across ALL categories for a company.
    Wrapper around extract_outlook_risks_opportunities that returns flat list.
    Used by Overview Transcript Signal Explorer.
    """
    result = extract_outlook_risks_opportunities(
        excel_path, company, year, quarter, max_per_category=max_per_category,
    )
    flat: list[dict] = []
    for cat, signals in result.items():
        for sig in signals:
            sig["company"] = company
            sig["year"] = year
            sig["quarter"] = quarter
            flat.append(sig)
    return flat


# ══════════════════════════════════════════════════════════════════════════════
# SPEAKER REGISTRY — extract, persist, and look up real speaker names
# ══════════════════════════════════════════════════════════════════════════════

def extract_speakers_from_transcript(
    transcript_text: str,
    company: str,
    year: int,
    quarter: str,
) -> list[dict]:
    """
    Parse transcript text to extract speaker names and roles.
    Format: "FirstName LastName RoleTitle Speech..."
    Returns list of dicts with company, year, quarter, full_name, role_raw,
    role_normalized, is_executive. Deduplicates per company.
    """
    speakers: list[dict] = []
    seen_names: set = set()

    raw_blocks = re.split(r"\n\s*\n", transcript_text)

    for block in raw_blocks:
        block = block.strip()
        if not block or len(block) < 20:
            continue
        if block.startswith("Company:") or block.startswith("---"):
            continue

        for role_title in sorted(KNOWN_ROLE_TITLES, key=len, reverse=True):
            if role_title in block:
                idx = block.index(role_title)
                name_part = block[:idx].strip()
                name_words = name_part.split()
                if 1 <= len(name_words) <= 4:
                    full_name = " ".join(name_words)
                    if full_name.lower() in ("operator", "moderator", ""):
                        break
                    key = f"{company}::{full_name.lower()}"
                    if key in seen_names:
                        break
                    seen_names.add(key)

                    role_normalized = ROLE_NORMALIZER.get(role_title, role_title)
                    is_executive = role_normalized in (
                        "CEO", "CFO", "COO", "CTO", "President",
                        "SVP", "EVP", "VP", "CRO", "CMO", "CPO",
                    )
                    speakers.append({
                        "company": company,
                        "year": int(year),
                        "quarter": quarter,
                        "full_name": full_name,
                        "role_raw": role_title,
                        "role_normalized": role_normalized,
                        "is_executive": is_executive,
                    })
                break  # only match first role per block

    return speakers


def build_speaker_registry(excel_path: str) -> list[dict]:
    """
    Scan ALL transcripts and build a deduplicated speaker registry.
    Each speaker appears once per company (most recent year wins).
    """
    try:
        df = pd.read_excel(excel_path, sheet_name="Transcripts")
        df.columns = [str(c).strip().lower() for c in df.columns]
    except Exception:
        return []

    all_speakers: dict = {}
    for _, row in df.iterrows():
        company = str(row.get("company", "")).strip()
        year = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
        quarter = str(row.get("quarter", "")).strip()
        txt = str(row.get("transcript_text", "") or "")
        if not txt or not company or pd.isna(year):
            continue
        for sp in extract_speakers_from_transcript(txt, company, int(year), quarter):
            key = f"{company}::{sp['full_name'].lower()}"
            if key not in all_speakers or sp["year"] > all_speakers[key]["year"]:
                all_speakers[key] = sp

    return list(all_speakers.values())


def write_speakers_to_excel(excel_path: str) -> int:
    """
    Build speaker registry and write Company_Speakers sheet to workbook.
    Returns number of speakers written.
    """
    from openpyxl import load_workbook

    speakers = build_speaker_registry(excel_path)
    if not speakers:
        return 0

    df = pd.DataFrame(speakers)
    df = df.sort_values(["company", "role_normalized", "full_name"])
    df = df[["company", "full_name", "role_normalized", "role_raw",
             "is_executive", "year", "quarter"]]
    df.columns = ["Company", "Full Name", "Role", "Role (raw)",
                  "Is Executive", "Last Seen Year", "Last Seen Quarter"]

    wb = load_workbook(excel_path)
    if "Company_Speakers" in wb.sheetnames:
        del wb["Company_Speakers"]
    ws = wb.create_sheet("Company_Speakers")
    ws.append(list(df.columns))
    for _, row in df.iterrows():
        ws.append(list(row))
    wb.save(excel_path)
    return len(df)


@st.cache_data(ttl=3600, show_spinner=False)
def get_speaker_name(company: str, role: str, excel_path: str) -> str | None:
    """Look up a speaker's full name by company and role from Company_Speakers sheet."""
    try:
        df = pd.read_excel(excel_path, sheet_name="Company_Speakers")
        df.columns = [str(c).strip() for c in df.columns]
        match = df[
            (df["Company"].str.lower() == company.lower())
            & (df["Role"].str.upper() == role.upper())
        ]
        if not match.empty:
            return str(match.iloc[0]["Full Name"])
    except Exception:
        pass
    return None
