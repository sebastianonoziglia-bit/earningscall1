"""
Unified Scoring Configuration — Single Source of Truth
=====================================================
All keyword lists, weights, thresholds, and layer multipliers live here.
Both the pipeline (extract_transcript_topics.py, build_intelligence_db.py)
and the app (transcript_live.py) import from this file.

When you update keywords or weights here, run:
  python3 scripts/sync_all_intelligence.py    # re-scores everything
  python3 scripts/generate_diagnostic_report.py  # see quality metrics

The diagnostic HTML report shows score distributions so you can tune
these values and see the impact immediately.
"""
from __future__ import annotations

# ═══════════════════════════════════════════════════════════════════════════════
# 1. TOPIC KEYWORDS — used for topic detection across all surfaces
# ═══════════════════════════════════════════════════════════════════════════════
TOPIC_KEYWORDS = {
    "AI & Machine Learning": [
        "artificial intelligence", "machine learning", "ai model",
        "large language model", "llm", "generative ai", "gemini",
        "copilot", "gpt", "neural network", "deep learning",
    ],
    "Advertising": [
        "advertising revenue", "ad revenue", "ad market", "programmatic",
        "sponsored", "cpm", "arpu", "ad spend", "upfront", "scatter market",
    ],
    "Streaming & Subscriptions": [
        "streaming", "subscribers", "paid members", "churn",
        "retention", "direct to consumer", "dtc", "password sharing",
    ],
    "Cloud & Infrastructure": [
        "cloud revenue", "cloud growth", "aws", "azure", "gcp",
        "google cloud", "infrastructure", "data center", "capex",
    ],
    "Retail & E-Commerce": [
        "e-commerce", "online stores", "marketplace", "third party",
        "prime", "fulfillment", "logistics", "retail media",
    ],
    "Cost & Efficiency": [
        "headcount", "restructuring", "cost reduction", "efficiency",
        "operating margin", "opex", "layoffs", "workforce",
    ],
    "Macro & Economy": [
        "macroeconomic", "recession", "inflation", "interest rate",
        "consumer spending", "currency", "foreign exchange", "fx",
    ],
    "Content & IP": [
        "original content", "theatrical", "box office", "franchise",
        "intellectual property", "licensing", "studio",
    ],
    "Mobile & Devices": [
        "smartphone", "iphone", "pixel", "hardware", "wearables",
        "connected devices", "mobile",
    ],
    "Social & Engagement": [
        "daily active users", "dau", "monthly active users", "mau",
        "engagement", "reels", "shorts", "tiktok", "feed",
    ],
    "Payments & Fintech": [
        "payments", "fintech", "checkout", "buy now pay later", "bnpl",
    ],
    "Privacy & Regulation": [
        "privacy", "regulation", "antitrust", "gdpr", "data protection",
        "compliance", "consent",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# 2. SIGNAL CATEGORIES — the 9 categories for signal classification
# ═══════════════════════════════════════════════════════════════════════════════
SIGNAL_CATEGORIES = [
    "Outlook", "Risks", "Opportunities", "Investment",
    "Product Shifts", "User Behavior", "Monetization",
    "Strategic Direction", "Broadcaster Threats",
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

# ═══════════════════════════════════════════════════════════════════════════════
# 3. CATEGORY KEYWORD LISTS — used for signal extraction
# ═══════════════════════════════════════════════════════════════════════════════
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

# Map category name → keyword list for programmatic access
CATEGORY_KEYWORDS = {
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

# ═══════════════════════════════════════════════════════════════════════════════
# 4. FINANCIAL SCORE TERMS — additive bonus for financial vocabulary
# ═══════════════════════════════════════════════════════════════════════════════
FINANCIAL_SCORE_TERMS = [
    "revenue", "growth", "billion", "million", "margin", "profit",
    "operating income", "eps", "guidance", "quarter", "year",
    "advertising", "cloud", "subscribers", "users", "capex",
    "expect", "increase", "grew", "strong", "momentum", "opportunity",
    "record", "accelerat", "expand", "invest",
]

# ═══════════════════════════════════════════════════════════════════════════════
# 5. FORWARD-LOOKING SIGNAL ENGINE — keywords + scoring layers
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
# 6. SPEAKER ROLE DETECTION
# ═══════════════════════════════════════════════════════════════════════════════
CEO_TITLES = [
    "chief executive officer", "ceo", "president and chief executive",
    "co-founder and ceo", "co-founder and chief executive", "president & ceo",
]
CFO_TITLES = [
    "chief financial officer", "cfo", "senior vice president and chief financial",
    "executive vice president and chief financial", "evp and chief financial",
    "svp and chief financial",
]

# ── Speaker role overrides ─────────────────────────────────────────────────────
# When a speaker's name is detected, force their role to the value here,
# overriding whatever the transcript parser inferred.  Use lowercase names.
# This corrects cases where a title is ambiguous or the transcript format
# causes the wrong role bucket to be assigned.
SPEAKER_ROLE_OVERRIDES: dict[str, str] = {
    # Alphabet / Google
    "philipp schindler": "CBO",   # SVP & Chief Business Officer — NOT CFO
    "philip schindler": "CBO",
    "phillip schindler": "CBO",
    "ruth porat": "President",    # Transitioned to President & CIO in 2023
    "anat ashkenazi": "CFO",      # CFO from July 2024
    # Add further corrections here as needed
}

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

# ═══════════════════════════════════════════════════════════════════════════════
# 7. SCORING LAYER WEIGHTS — tuneable multipliers
# ═══════════════════════════════════════════════════════════════════════════════
# These control the 5-layer scoring engine. Adjust and re-run pipeline to see
# the impact in the diagnostic report's Scoring Quality section.

LAYER_WEIGHTS = {
    # Base score multipliers (applied during initial scoring)
    "specificity_bonus":    1.4,   # sentences with real $ amounts or %
    "forward_tense_bonus":  1.3,   # sentences with forward-looking language
    "role_bonus_ceo_cfo":   1.5,   # CEO/CFO speaker premium
    "position_max_bonus":   0.3,   # max position bonus (early = +0.3x)
    "financial_term_bonus": 0.4,   # per-term additive for financial vocab

    # Length factors
    "len_very_short":       0.6,   # < 50 chars
    "len_short":            0.85,  # 50-80 chars
    "len_medium":           1.0,   # 80-250 chars (sweet spot)
    "len_long":             0.8,   # > 250 chars

    # Stacked verification layers (applied after base score)
    "future_tense_stack":   1.4,   # FUTURE_TENSE_MARKERS match
    "concrete_forward_stack": 1.5, # has numbers AND is forward-looking
    "negation_penalty":     0.1,   # NEGATION_PREFIXES match (near-kill)
    "boilerplate_penalty":  0.05,  # BOILERPLATE_PHRASES match (near-kill)
}

# Minimum score thresholds
THRESHOLDS = {
    "min_signal_score":      0.5,   # minimum to keep a signal
    "min_forward_score":     0.5,   # minimum for forward-looking signals
    "high_confidence_score": 3.0,   # signals above this are "high confidence"
    "min_sentence_length":   40,    # chars — skip shorter sentences
    "max_sentence_length":   500,   # chars — skip longer sentences
    "dedup_prefix_length":   60,    # first N chars for deduplication
}
