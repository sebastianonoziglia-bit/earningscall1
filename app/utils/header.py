import streamlit as st
from utils.language import init_language
from utils.theme import apply_theme


def _get_current_page_key() -> str:
    """Detect current page from the running script filename (reliable across navigations)."""
    try:
        import streamlit.runtime.scriptrunner as _sr
        ctx = _sr.get_script_run_ctx()
        if ctx:
            script = str(ctx.main_script_path)
            if "Welcome" in script or script.endswith("app.py"):
                return "home"
            if "00_Overview" in script:
                return "overview"
            if "01_Earnings" in script:
                return "earnings"
            if "02_Stocks" in script:
                return "stocks"
            if "03_Editorial" in script:
                return "editorial"
            if "04_Genie" in script:
                return "genie"
    except Exception:
        pass
    # Fallback: session state set by each page
    return str(st.session_state.get("active_nav_page", "home"))


_NAV_ITEMS = [
    {"key": "home", "target": "Welcome.py", "label": "Home", "icon": "🏠", "query": "home"},
    {"key": "overview", "target": "pages/00_Overview.py", "label": "Overview", "icon": "📊", "query": "overview"},
    {"key": "earnings", "target": "pages/01_Earnings.py", "label": "Earnings", "icon": "💰", "query": "earnings"},
    {"key": "stocks", "target": "pages/02_Stocks.py", "label": "Stocks", "icon": "📈", "query": "stocks"},
    {"key": "editorial", "target": "pages/03_Editorial.py", "label": "Editorial", "icon": "📝", "query": "editorial"},
    {"key": "genie", "target": "pages/04_Genie.py", "label": "Genie", "icon": "🧞", "query": "genie"},
]

_QUERY_PAGE_MAP = {
    "home": "Welcome.py",
    "welcome": "Welcome.py",
    "overview": "pages/00_Overview.py",
    "earnings": "pages/01_Earnings.py",
    "01_earnings": "pages/01_Earnings.py",
    "stocks": "pages/02_Stocks.py",
    "editorial": "pages/03_Editorial.py",
    "genie": "pages/04_Genie.py",
    "financial_genie": "pages/04_Genie.py",
    "financial-genie": "pages/04_Genie.py",
}

_SUPPORTED_LANGS = {"en", "it", "es"}


def _first_param(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _route_query_navigation():
    query_params = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
    if not query_params:
        return
    nav_param = _first_param(query_params.get("nav"))
    go_param = _first_param(query_params.get("go"))
    page_param = _first_param(query_params.get("page"))
    target_param = nav_param or go_param or page_param
    if not target_param:
        return
    target_key = str(target_param).strip().lower()
    target_page = _QUERY_PAGE_MAP.get(target_key)
    if not target_page:
        return
    current_key = str(
        st.session_state.get("active_nav_page")
        or st.session_state.get("_active_nav_page")
        or ""
    ).strip().lower()
    current_page = _QUERY_PAGE_MAP.get(current_key, "")
    if target_key == current_key or (current_page and target_page == current_page):
        # Already on target page: no switch and no query mutation to avoid rerun loops.
        return
    last_switch = st.session_state.get("_last_nav_switch", "")
    if last_switch == target_key:
        return
    st.session_state["_last_nav_switch"] = target_key
    st.switch_page(target_page)


def _apply_query_language():
    query_params = st.query_params if hasattr(st, "query_params") else st.experimental_get_query_params()
    if not query_params:
        return
    lang_param = _first_param(query_params.get("lang"))
    if not lang_param:
        return
    lang_code = str(lang_param).strip().lower()
    if lang_code in _SUPPORTED_LANGS:
        if str(st.session_state.get("language", "")).strip().lower() != lang_code:
            st.session_state.language = lang_code


def _render_bottom_nav(active_key: str):
    _nav_items = [
        ("home",      "🏠", "Home",      "/"),
        ("overview",  "📊", "Overview",  "/Overview"),
        ("earnings",  "💰", "Earnings",  "/Earnings"),
        ("stocks",    "📈", "Stocks",    "/Stocks"),
        ("editorial", "📝", "Editorial", "/Editorial"),
        ("genie",     "🧞", "Genie",     "/Genie"),
    ]
    nav_pills_html = ""
    for key, icon, label, url in _nav_items:
        is_active = (active_key == key)
        active_style = (
            "background:#2563eb;color:white;"
            if is_active
            else "background:rgba(255,255,255,0.08);color:#94a3b8;"
        )
        nav_pills_html += (
            f"<a href='{url}' style='"
            f"display:inline-flex;align-items:center;gap:6px;"
            f"padding:8px 16px;border-radius:20px;text-decoration:none;"
            f"font-size:0.82rem;font-weight:{'700' if is_active else '500'};"
            f"transition:all 0.2s;{active_style}'>"
            f"{icon} {label}</a>"
        )
    st.markdown(
        f"""
        <style>
        .app-bottom-nav-wrap {{
            position: fixed;
            left: 12px;
            right: 12px;
            bottom: 10px;
            z-index: 9999;
            display: flex;
            justify-content: center;
            pointer-events: none;
        }}
        .app-bottom-nav {{
            pointer-events: auto;
            display: flex;
            gap: 6px;
            overflow-x: auto;
            padding: 8px;
            border-radius: 28px;
            background: rgba(15,23,42,0.84);
            border: 1px solid rgba(148,163,184,0.34);
            backdrop-filter: blur(10px);
            box-shadow: 0 10px 28px rgba(2,6,23,0.26);
        }}
        .app-bottom-nav a:hover {{
            filter: brightness(1.15);
        }}
        </style>
        <div class="app-bottom-nav-wrap">
          <div class="app-bottom-nav">{nav_pills_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_sticky_top_bar(active_key: str):
    nav_links = []
    for item in _NAV_ITEMS:
        active_class = " active" if item["key"] == active_key else ""
        nav_links.append(
            f"<a href='?nav={item['query']}' target='_self' rel='noopener' class='{active_class}'>{item['icon']} {item['label']}</a>"
        )

    current_lang = str(st.session_state.get("language", "en")).strip().lower()
    lang_en_cls = "active" if current_lang == "en" else ""
    lang_it_cls = "active" if current_lang == "it" else ""
    lang_es_cls = "active" if current_lang == "es" else ""

    st.markdown(
        f"""
        <style>
          header[data-testid="stHeader"] {{
            display: none !important;
            height: 0 !important;
          }}
          [data-testid="stToolbar"] {{
            display: none !important;
            height: 0 !important;
          }}
          [data-testid="stAppViewContainer"] {{
            padding-top: 0 !important;
            margin-top: 0 !important;
          }}
          [data-testid="stAppViewContainer"] > .main,
          [data-testid="stAppViewContainer"] > section.main {{
            padding-top: 0 !important;
            margin-top: 0 !important;
          }}
          #root > div:first-child {{
            padding-top: 0 !important;
          }}
          .stApp > header {{
            display: none !important;
          }}
          .block-container,
          [data-testid="stAppViewContainer"] > .main .block-container,
          [data-testid="stAppViewContainer"] > section > div.block-container,
          section.main > div.block-container {{
            padding-top: 0 !important;
            margin-top: 0 !important;
          }}
          .main .block-container > div:first-child {{
            margin-top: 0 !important;
          }}
          .app-top-bar {{
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 9998;
            background: rgba(15,23,42,0.72);
            backdrop-filter: blur(10px);
            border-bottom: 1px solid rgba(148,163,184,0.24);
            padding: 7px 26px;
            display: flex; align-items: center; gap: 5px;
            margin: 0;
          }}
          .app-top-offset {{
            height: 0 !important;
            display: none !important;
          }}
          .app-top-bar a {{
            display: inline-flex; align-items: center; gap: 5px;
            padding: 6px 14px; border-radius: 8px;
            text-decoration: none !important;
            font-size: 0.88rem; font-weight: 700;
            color: #E2E8F0 !important;
            border: 1px solid transparent;
            transition: background 0.15s, color 0.15s;
            white-space: nowrap;
          }}
          .app-top-bar a:hover {{ background: rgba(30,64,175,0.35); color: #FFFFFF !important; }}
          .app-top-bar a.active {{
            background: #1d4ed8; color: #fff !important;
            border-color: #1e40af;
          }}
          .app-top-bar .app-top-bar-spacer {{
            flex: 1;
          }}
          .app-top-bar .lang-link {{
            padding: 6px 10px;
            min-width: 38px;
            justify-content: center;
          }}
          /* style-only markdown nodes still create flex gaps; collapse them globally */
          [data-testid="stMarkdownContainer"]:has(> style:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > style:only-child),
          [data-testid="stMarkdownContainer"]:has(> script:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > script:only-child) {{
            margin: 0 !important;
            padding: 0 !important;
            height: 0 !important;
            min-height: 0 !important;
            line-height: 0 !important;
            overflow: hidden !important;
          }}
          [data-testid="element-container"]:has(.app-top-bar),
          [data-testid="stMarkdownContainer"]:has(.app-top-bar) {{
            margin: 0 !important;
            padding: 0 !important;
            height: 0 !important;
            min-height: 0 !important;
            overflow: visible !important;
            line-height: 0 !important;
          }}
        </style>
        <div class="app-top-bar">
          {''.join(nav_links)}
          <span class="app-top-bar-spacer"></span>
          <a href="?lang=en" target="_self" rel="noopener" class="lang-link {lang_en_cls}">🇺🇸</a>
          <a href="?lang=it" target="_self" rel="noopener" class="lang-link {lang_it_cls}">🇮🇹</a>
          <a href="?lang=es" target="_self" rel="noopener" class="lang-link {lang_es_cls}">🇪🇸</a>
        </div>
        """,
        unsafe_allow_html=True,
    )


def display_header(enable_dom_patch: bool = True):
    """
    Display the common header across all app pages.
    This includes language selection buttons.
    """
    _route_query_navigation()
    _apply_query_language()

    # Initialize language from URL query params or session state
    init_language()

    # Replace sidebar app navigation with top navigation.
    st.session_state["hide_sidebar_nav"] = True
    active_key = _get_current_page_key()
    _render_sticky_top_bar(active_key)
    _render_bottom_nav(active_key)

    # Apply global styles/theme utilities (theme toggle removed).
    apply_theme(enable_dom_patch=enable_dom_patch)
    

def render_header(enable_dom_patch: bool = True):
    """
    Alias for display_header for backward compatibility
    """
    return display_header(enable_dom_patch=enable_dom_patch)
