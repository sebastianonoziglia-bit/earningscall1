import base64
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as _components
from utils.language import init_language
from utils.theme import apply_theme


@st.cache_data(show_spinner=False)
def _load_hero_video_b64() -> str:
    """Read HeroVideo.mp4 and return base64-encoded string (cached in memory)."""
    video_path = Path(__file__).resolve().parent.parent / "attached_assets" / "HeroVideo.mp4"
    if not video_path.exists():
        return ""
    with open(video_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _inject_hero_loader():
    """Inject full-screen video overlay on first visit per browser session.

    Uses sessionStorage so the video only plays once per browser tab session.
    Also skips injection after the first Streamlit page to avoid re-sending 5 MB.
    """
    if st.session_state.get("_hero_shown"):
        return
    st.session_state["_hero_shown"] = True

    video_b64 = _load_hero_video_b64()
    if not video_b64:
        return

    _components.html(
        f"""<script>
(function(){{
  // sessionStorage guard — skip if video already played this browser session
  if(sessionStorage.getItem('_heroPlayed')){{return;}}
  sessionStorage.setItem('_heroPlayed','1');

  var pd=window.parent.document;
  if(!pd){{return;}}

  // Full-screen overlay
  var ov=pd.createElement('div');
  ov.id='hero-loader-ov';
  ov.style.cssText='position:fixed;top:0;left:0;width:100vw;height:100vh;'
    +'background:#000;z-index:2147483647;overflow:hidden;display:flex;'
    +'align-items:center;justify-content:center;transition:opacity 0.6s ease;';

  var vid=pd.createElement('video');
  vid.src='data:video/mp4;base64,{video_b64}';
  vid.autoplay=true;
  vid.muted=true;
  vid.playsInline=true;
  vid.style.cssText='width:100%;height:100%;object-fit:cover;';

  function dismiss(){{
    ov.style.opacity='0';
    pd.body.style.overflow='';
    setTimeout(function(){{if(ov.parentNode)ov.parentNode.removeChild(ov);}},650);
  }}

  vid.addEventListener('ended',dismiss);
  // Safety fallback — dismiss after 12s even if video stalls
  setTimeout(dismiss,12000);
  // Allow click-to-skip
  ov.addEventListener('click',dismiss);

  ov.appendChild(vid);
  pd.body.appendChild(ov);
  pd.body.style.overflow='hidden';

  vid.play().catch(function(){{
    // Autoplay blocked — dismiss immediately
    dismiss();
  }});
}})();
</script>""",
        height=0,
    )


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
        # Already on the right page — clear stale nav param to prevent redirect loops
        try:
            if hasattr(st, "query_params"):
                for _k in ("nav", "go", "page"):
                    if _k in st.query_params:
                        del st.query_params[_k]
        except Exception:
            pass
        return
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

    # Streamlit layout fixes (still via markdown — these work fine)
    st.markdown(
        """
        <style>
          header[data-testid="stHeader"] { display:none!important; height:0!important; }
          [data-testid="stToolbar"]       { display:none!important; height:0!important; }
          .stApp > header                 { display:none!important; }
          [data-testid="stAppViewContainer"]                          { padding-top:0!important; margin-top:0!important; }
          [data-testid="stAppViewContainer"] > .main,
          [data-testid="stAppViewContainer"] > section.main          { padding-top:0!important; margin-top:0!important; }
          #root > div:first-child                                     { padding-top:0!important; }
          .block-container,
          [data-testid="stAppViewContainer"] > .main .block-container,
          [data-testid="stAppViewContainer"] > section > div.block-container,
          section.main > div.block-container                          { padding-top:0!important; margin-top:0!important; }
          .main .block-container > div:first-child                    { margin-top:0!important; }
          [data-testid="stMarkdownContainer"]:has(> style:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > style:only-child),
          [data-testid="stMarkdownContainer"]:has(> script:only-child),
          [data-testid="element-container"]:has([data-testid="stMarkdownContainer"] > script:only-child) {
            margin:0!important; padding:0!important;
            height:0!important; min-height:0!important;
            line-height:0!important; overflow:hidden!important;
          }
        </style>
        <div style="height:80px;"></div>
        """,
        unsafe_allow_html=True,
    )

    # ── Nav pill injected into the parent DOM (same technique as hero loader).
    # Uses direct page-URL navigation — NO ?nav= params — so _route_query_navigation
    # never intercepts nav clicks and can never cause a redirect loop.
    slug_map = {
        "home": "",
        "overview": "Overview",
        "earnings": "Earnings",
        "stocks": "Stocks",
        "editorial": "Editorial",
        "genie": "Genie",
    }
    slug_map_js = "{" + ",".join(f"'{k}':'{v}'" for k, v in slug_map.items()) + "}"
    nav_items_js = "[" + ",".join(
        "{key:'" + item["key"] + "',label:'" + item["label"] + "'}"
        for item in _NAV_ITEMS
    ) + "]"

    _components.html(
        f"""<script>
(function(){{
  var pd = window.parent.document;
  if (!pd) return;

  // Remove stale nav from previous Streamlit rerun
  var old = pd.getElementById('_app_pill_nav');
  if (old) old.remove();

  var slugs = {slug_map_js};
  var items = {nav_items_js};
  var active = '{active_key}';
  var langActive = '{current_lang}';

  function getBase() {{
    var p = window.parent.location.pathname;
    var base = p.replace(/\\/(Welcome|Overview|Earnings|Stocks|Editorial|Genie)\\/?$/i,'');
    if (!base.endsWith('/')) base += '/';
    return window.parent.location.origin + base;
  }}

  function navTo(key) {{
    var slug = (slugs[key] !== undefined) ? slugs[key] : '';
    window.parent.location.href = getBase() + slug;
  }}

  function setLang(code) {{
    var url = window.parent.location.href.split('?')[0];
    window.parent.location.href = url + '?lang=' + code;
  }}

  // Build nav HTML
  var nav = pd.createElement('div');
  nav.id = '_app_pill_nav';
  nav.style.cssText =
    'position:fixed;bottom:16px;left:50%;transform:translateX(-50%);z-index:2147483646;'
    +'background:rgba(15,23,42,0.95);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);'
    +'border:1px solid rgba(255,255,255,0.1);border-radius:50px;'
    +'padding:6px 12px;display:flex;align-items:center;gap:2px;'
    +'white-space:nowrap;font-family:-apple-system,BlinkMacSystemFont,sans-serif;';

  var btnBase =
    'display:inline-flex;align-items:center;padding:5px 13px;border-radius:20px;'
    +'text-decoration:none;cursor:pointer;font-size:0.82rem;font-weight:500;color:#94a3b8;'
    +'border:1px solid rgba(148,163,184,0.15);background:transparent;'
    +'transition:border-color .15s,color .15s;white-space:nowrap;';
  var btnActive =
    'color:#fff!important;border-color:rgba(99,130,255,0.75)!important;font-weight:700!important;';

  items.forEach(function(item) {{
    var btn = pd.createElement('button');
    btn.textContent = item.label;
    btn.style.cssText = btnBase + (item.key === active ? btnActive : '');
    btn.onmouseover = function() {{
      if (item.key !== active) {{
        btn.style.borderColor='rgba(148,163,184,0.55)';
        btn.style.color='#e2e8f0';
      }}
    }};
    btn.onmouseout = function() {{
      if (item.key !== active) {{
        btn.style.borderColor='rgba(148,163,184,0.15)';
        btn.style.color='#94a3b8';
      }}
    }};
    btn.addEventListener('click', (function(k){{ return function(){{ navTo(k); }}; }})(item.key));
    nav.appendChild(btn);
  }});

  var sep = pd.createElement('span');
  sep.style.cssText = 'width:1px;height:20px;background:rgba(148,163,184,0.2);margin:0 6px;flex-shrink:0;';
  nav.appendChild(sep);

  [['en','🇺🇸'],['it','🇮🇹'],['es','🇪🇸']].forEach(function(pair) {{
    var btn = pd.createElement('button');
    btn.textContent = pair[1];
    var isAct = pair[0] === langActive;
    btn.style.cssText =
      'display:inline-flex;align-items:center;padding:5px 8px;border-radius:20px;cursor:pointer;'
      +'font-size:0.85rem;background:transparent;border:1px solid '
      +(isAct?'rgba(148,163,184,0.3)':'transparent')+';opacity:'+(isAct?'1':'0.5')+';';
    btn.addEventListener('click',(function(c){{ return function(){{ setLang(c); }}; }})(pair[0]));
    nav.appendChild(btn);
  }});

  pd.body.appendChild(nav);
}})();
</script>""",
        height=0,
    )


def display_header(enable_dom_patch: bool = True):
    """Display the common header across all app pages."""
    _inject_hero_loader()
    _route_query_navigation()
    _apply_query_language()
    init_language()
    st.session_state["hide_sidebar_nav"] = True
    active_key = str(
        st.session_state.get("active_nav_page")
        or st.session_state.get("_active_nav_page")
        or ""
    ).strip().lower()
    _render_nav(active_key)
    apply_theme(enable_dom_patch=enable_dom_patch)


def render_header(enable_dom_patch: bool = True):
    """Alias for display_header for backward compatibility."""
    return display_header(enable_dom_patch=enable_dom_patch)
