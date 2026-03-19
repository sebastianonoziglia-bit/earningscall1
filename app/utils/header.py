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
        ("home",      "🏠", "Home",      "Welcome.py"),
        ("overview",  "📊", "Overview",  "pages/00_Overview.py"),
        ("earnings",  "💰", "Earnings",  "pages/01_Earnings.py"),
        ("stocks",    "📈", "Stocks",    "pages/02_Stocks.py"),
        ("editorial", "📝", "Editorial", "pages/03_Editorial.py"),
        ("genie",     "🧞", "Genie",     "pages/04_Genie.py"),
    ]
    st.markdown(
        """
        <style>
        div[data-testid="stPageLink"] a {
            background: rgba(255,255,255,0.08) !important;
            color: #94a3b8 !important;
            border-radius: 20px !important;
            padding: 8px 16px !important;
            text-decoration: none !important;
            font-size: 0.82rem !important;
            font-weight: 500 !important;
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
        }
        div[data-testid="stPageLink"] a[aria-current="page"] {
            background: #2563eb !important;
            color: white !important;
            font-weight: 700 !important;
        }
        div[data-testid="stPageLink"] a:hover {
            filter: brightness(1.15);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    nav_cols = st.columns(len(_nav_items))
    for col, (key, icon, label, page_file) in zip(nav_cols, _nav_items):
        with col:
            st.page_link(page_file, label=f"{icon} {label}", use_container_width=True)


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
