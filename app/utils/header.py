import streamlit as st
from utils.language import init_language
from utils.theme import apply_theme


_NAV_ITEMS = [
    {"key": "home",      "target": "Welcome.py",          "label": "Home",      "query": "home"},
    {"key": "overview",  "target": "pages/00_Overview.py", "label": "Overview",  "query": "overview"},
    {"key": "earnings",  "target": "pages/01_Earnings.py", "label": "Earnings",  "query": "earnings"},
    {"key": "stocks",    "target": "pages/02_Stocks.py",   "label": "Stocks",    "query": "stocks"},
    {"key": "editorial", "target": "pages/03_Editorial.py","label": "Editorial", "query": "editorial"},
    {"key": "genie",     "target": "pages/04_Genie.py",    "label": "Genie",     "query": "genie"},
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


def _render_nav(active_key: str):
    current_lang = str(st.session_state.get("language", "en")).strip().lower()

    nav_links_html = ""
    for item in _NAV_ITEMS:
        active_cls = " bnav-active" if item["key"] == active_key else ""
        nav_links_html += (
            f"<a href='?nav={item['query']}' target='_self' rel='noopener' "
            f"class='bnav-link{active_cls}'>{item['label']}</a>"
        )

    lang_en_cls = " bnav-lang-active" if current_lang == "en" else ""
    lang_it_cls = " bnav-lang-active" if current_lang == "it" else ""
    lang_es_cls = " bnav-lang-active" if current_lang == "es" else ""

    st.markdown(
        f"""
        <style>
          /* Hide Streamlit's default header and toolbar */
          header[data-testid="stHeader"] {{
            display: none !important; height: 0 !important;
          }}
          [data-testid="stToolbar"] {{
            display: none !important; height: 0 !important;
          }}
          .stApp > header {{ display: none !important; }}
          [data-testid="stAppViewContainer"] {{
            padding-top: 0 !important; margin-top: 0 !important;
          }}
          [data-testid="stAppViewContainer"] > .main,
          [data-testid="stAppViewContainer"] > section.main {{
            padding-top: 0 !important; margin-top: 0 !important;
          }}
          #root > div:first-child {{ padding-top: 0 !important; }}
          .block-container,
          [data-testid="stAppViewContainer"] > .main .block-container,
          [data-testid="stAppViewContainer"] > section > div.block-container,
          section.main > div.block-container {{
            padding-top: 0 !important; margin-top: 0 !important;
          }}
          .main .block-container > div:first-child {{ margin-top: 0 !important; }}

          /* Collapse style-only markdown nodes */
          [data-testid="stMarkdownContainer"]:has(> style:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > style:only-child),
          [data-testid="stMarkdownContainer"]:has(> script:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > script:only-child) {{
            margin: 0 !important; padding: 0 !important;
            height: 0 !important; min-height: 0 !important;
            line-height: 0 !important; overflow: hidden !important;
          }}
          [data-testid="element-container"]:has(.app-bottom-nav),
          [data-testid="stMarkdownContainer"]:has(.app-bottom-nav) {{
            margin: 0 !important; padding: 0 !important;
            height: 0 !important; min-height: 0 !important;
            overflow: visible !important; line-height: 0 !important;
          }}

          /* Bottom floating pill nav */
          .app-bottom-nav {{
            position: fixed; bottom: 16px; left: 50%;
            transform: translateX(-50%); z-index: 9999;
            background: rgba(15,23,42,0.95); backdrop-filter: blur(12px);
            border: 1px solid rgba(255,255,255,0.1); border-radius: 50px;
            padding: 6px 12px; display: flex; align-items: center; gap: 2px;
          }}
          .bnav-link {{
            display: inline-flex; align-items: center;
            padding: 5px 13px; border-radius: 20px;
            text-decoration: none !important;
            font-size: 0.82rem; font-weight: 500;
            color: #94a3b8 !important;
            border: 1px solid rgba(148,163,184,0.15);
            transition: border-color 0.15s, color 0.15s;
            white-space: nowrap;
          }}
          .bnav-link:hover {{
            border-color: rgba(148,163,184,0.55) !important;
            color: #e2e8f0 !important;
          }}
          .bnav-active {{
            color: #fff !important;
            border-color: rgba(99,130,255,0.75) !important;
            font-weight: 700 !important;
          }}
          .bnav-sep {{
            width: 1px; height: 20px;
            background: rgba(148,163,184,0.2);
            margin: 0 6px; flex-shrink: 0;
          }}
          .bnav-lang {{
            display: inline-flex; align-items: center;
            padding: 5px 8px; border-radius: 20px;
            text-decoration: none !important;
            font-size: 0.85rem;
            border: 1px solid transparent;
            transition: border-color 0.15s;
            opacity: 0.5;
          }}
          .bnav-lang:hover {{ opacity: 1; border-color: rgba(148,163,184,0.4) !important; }}
          .bnav-lang-active {{ opacity: 1 !important; border-color: rgba(148,163,184,0.3) !important; }}
          .app-bottom-nav-spacer {{ height: 80px; }}
        </style>
        <div class="app-bottom-nav">
          {nav_links_html}
          <span class="bnav-sep"></span>
          <a href="?lang=en" target="_self" rel="noopener" class="bnav-lang{lang_en_cls}">🇺🇸</a>
          <a href="?lang=it" target="_self" rel="noopener" class="bnav-lang{lang_it_cls}">🇮🇹</a>
          <a href="?lang=es" target="_self" rel="noopener" class="bnav-lang{lang_es_cls}">🇪🇸</a>
        </div>
        <div class="app-bottom-nav-spacer"></div>
        """,
        unsafe_allow_html=True,
    )


def display_header(enable_dom_patch: bool = True):
    """Display the common header across all app pages."""
    _route_query_navigation()
    _apply_query_language()
    init_language()
    st.session_state["hide_sidebar_nav"] = True
    active_key = str(
        st.session_state.get("active_nav_page")
        or st.session_state.get("_active_nav_page")
        or st.session_state.get("_last_nav_switch")
        or ""
    ).strip().lower()
    _render_nav(active_key)
    apply_theme(enable_dom_patch=enable_dom_patch)


def render_header(enable_dom_patch: bool = True):
    """Alias for display_header for backward compatibility."""
    return display_header(enable_dom_patch=enable_dom_patch)
