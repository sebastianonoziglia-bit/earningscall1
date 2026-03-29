import streamlit as st

# Page config must be the first Streamlit command
st.set_page_config(page_title="Editorial", page_icon="📝", layout="wide")

from utils.global_fonts import apply_global_fonts
apply_global_fonts()


from utils.auth import check_password
import plotly.graph_objects as go
from subscriber_data_processor import SubscriberDataProcessor
import pandas as pd
from datetime import datetime, timedelta
import re
from utils.styles import get_page_style, get_animation_style
import base64
from PIL import Image
import os
from io import BytesIO
from utils.page_transition import apply_page_transition_fix
from pathlib import Path

SERVICE_COLOR_MAP = {
    "Disney+": "#113CCF",
    "Netflix": "#E50914",
    "Spotify": "#1DB954",
    "Spotify Premium": "#1DB954",
    "Facebook": "#0866FF",
    "Instagram": "#C13584",
    "WhatsApp": "#25D366",
    "WBD": "#4a90d9",
    "Warner Bros. Discovery": "#4a90d9",
    "Amazon Prime": "#FF9900",
    "Amazon Prime Video": "#FF9900",
    "YouTube": "#FF0000",
    "Peacock": "#C01F33",
    "Paramount+": "#7B2FBE",
}

def _service_color(service: str) -> str:
    return SERVICE_COLOR_MAP.get(service, "#6366f1")

def _hex_to_rgb(h: str) -> tuple:
    h = h.lstrip("#")
    try:
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    except Exception:
        return (99, 102, 241)

# Apply fix for page transitions to prevent background bleed-through
apply_page_transition_fix()

# Apply shared styles
st.markdown(get_page_style(), unsafe_allow_html=True)
st.markdown(get_animation_style(), unsafe_allow_html=True)

# Add header with language selector
from utils.header import render_header
from utils.language import get_text
st.session_state["active_nav_page"] = "editorial"
st.session_state["_active_nav_page"] = "editorial"
render_header()

# Add SQL Assistant in the sidebar
from utils.sql_assistant_sidebar import render_sql_assistant_sidebar
if not st.session_state.get("hide_sidebar_nav", False):
    render_sql_assistant_sidebar()

# Check if user is logged in, redirect to Welcome page if not
# Always authenticated - no password check needed
from utils.time_utils import render_floating_clock
render_floating_clock()

st.title("Editorial Insights")
st.write("Quarterly subscriber metrics analysis.")

SERVICE_TO_COMPANY = {
    # Disney
    "Disney+": "Disney",
    "Hulu": "Disney",
    "ESPN+": "Disney",
    "ESPN": "Disney",
    # Amazon
    "Amazon Prime": "Amazon",
    "Amazon Prime Video": "Amazon",
    "Twitch": "Amazon",
    # Comcast / NBCUniversal
    "Peacock": "Comcast",
    "NBC": "Comcast",
    # Warner Bros. Discovery
    "WBD": "Warner Bros. Discovery",
    "Max": "Warner Bros. Discovery",
    "HBO Max": "Warner Bros. Discovery",
    "HBO": "Warner Bros. Discovery",
    "Discovery+": "Warner Bros. Discovery",
    # Paramount
    "Paramount+": "Paramount",
    "Pluto TV": "Paramount",
    "BET+": "Paramount",
    # Meta
    "Facebook": "Meta Platforms",
    "Instagram": "Meta Platforms",
    "WhatsApp": "Meta Platforms",
    "Threads": "Meta Platforms",
    # Alphabet
    "YouTube": "Alphabet",
    "YouTube Premium": "Alphabet",
    "YouTube TV": "Alphabet",
    "Google One": "Alphabet",
    # Apple
    "Apple TV+": "Apple",
    "Apple Music": "Apple",
    "iCloud": "Apple",
    # Microsoft
    "LinkedIn": "Microsoft",
    "Xbox Game Pass": "Microsoft",
    "Xbox": "Microsoft",
    # Spotify (standalone)
    "Spotify": "Spotify",
    "Spotify Premium": "Spotify",
    # Netflix (standalone)
    "Netflix": "Netflix",
    # Roku (standalone)
    "Roku": "Roku",
    "The Roku Channel": "Roku",
    # Snap
    "Snapchat": "Snap",
    # Pinterest
    "Pinterest": "Pinterest",
}


COMPANY_ASSET_MAP = {
    "Alphabet":{"color":"#4285F4","primary_business":"Search & Digital Advertising","key_assets":["Google Search","YouTube","Google Cloud (GCP)","Google Network","Waymo","DeepMind / Gemini AI"],"ad_products":["Search Ads","YouTube Ads","Google Display Network","DV360","Performance Max"],"competitive_note":"Dominant in search (90%+ share) and online video. YouTube is #1 video platform globally by watch time."},
    "Amazon":{"color":"#FF9900","primary_business":"E-Commerce, Cloud & Advertising","key_assets":["AWS","Amazon Ads","Amazon Prime Video","Prime membership","Twitch","Amazon Music","Alexa"],"ad_products":["Amazon Sponsored Products","Amazon DSP","Streaming TV Ads on Prime Video"],"competitive_note":"Fastest-growing major ad platform. AWS funds the consumer business. Prime Video now has ads."},
    "Apple":{"color":"#555555","primary_business":"Consumer Hardware & Services","key_assets":["iPhone","Mac","iPad","Apple Watch / AirPods","Apple TV+","App Store","iCloud","Apple Music","Apple Intelligence AI"],"ad_products":["App Store Search Ads","Apple TV+ brand partnerships"],"competitive_note":"Services (App Store, iCloud, Music, TV+) are the high-margin growth engine. iPhone = 50%+ of revenue."},
    "Meta Platforms":{"color":"#0866FF","primary_business":"Social Media & Digital Advertising","key_assets":["Facebook","Instagram","WhatsApp","Threads","Reels","Meta AI","Reality Labs / Quest VR"],"ad_products":["Facebook Ads","Instagram Ads","Reels Ads","WhatsApp Business API","Advantage+ AI ad buying"],"competitive_note":"Family of Apps generates ~99% of revenue. Reality Labs loses ~$5B/year."},
    "Microsoft":{"color":"#00A4EF","primary_business":"Enterprise Software & Cloud","key_assets":["Azure","Microsoft 365 / Office","LinkedIn","Bing / MSN","Xbox / Game Pass","GitHub","Copilot AI"],"ad_products":["LinkedIn Ads","Bing Ads / Microsoft Advertising","MSN display ads"],"competitive_note":"Azure is #2 cloud behind AWS. LinkedIn is the dominant B2B ad platform."},
    "Netflix":{"color":"#E50914","primary_business":"Subscription Video Streaming","key_assets":["Netflix Streaming","Ad-Supported Tier","Netflix Games","Live Events","Original Content"],"ad_products":["Netflix Ads (ad-supported tier)","Branded content partnerships"],"competitive_note":"Password sharing crackdown drove massive subscriber growth. Ad tier now ~70M MAU."},
    "Disney":{"color":"#113CCF","primary_business":"Entertainment, Streaming & Parks","key_assets":["Disney+","Hulu","ESPN / ESPN+","Linear TV (ABC/FX/NatGeo)","Disney Parks","Pixar/Marvel/Lucasfilm"],"ad_products":["Hulu Ads","ESPN Ads","ABC Network Ads","Disney+ Ad-Supported Tier"],"competitive_note":"Streaming profitability is the key challenge. Parks is the most profitable segment."},
    "Comcast":{"color":"#C01F33","primary_business":"Cable, Broadband & Entertainment","key_assets":["Xfinity broadband","NBCUniversal / NBC","Peacock","Universal Studios","Sky (Europe)","Telemundo"],"ad_products":["NBCUniversal Advertising","Peacock Ads","FreeWheel ad tech","Sky Ads (Europe)"],"competitive_note":"Broadband is the stable high-margin business. Peacock burning cash. Sky gives European footprint."},
    "Spotify":{"color":"#1DB954","primary_business":"Audio Streaming","key_assets":["Spotify Premium","Spotify Ad-Supported / Free Tier","Podcasts","Audiobooks","Spotify DJ AI"],"ad_products":["Spotify Audio Ads","Spotify Video Ads","Podcast Ads / SAI","Spotify Audience Network"],"competitive_note":"Reached profitability in 2024. Ad-supported tier is 60%+ of MAU."},
    "Roku":{"color":"#6C2DC7","primary_business":"Streaming Platform & CTV Advertising","key_assets":["Roku OS","The Roku Channel (FAST)","Roku Devices","Roku City screensaver ads"],"ad_products":["Roku Advertising (CTV)","Home Screen Ads","The Roku Channel programmatic"],"competitive_note":"~80M active accounts. Revenue 80%+ from platform/ads, not hardware."},
    "Warner Bros. Discovery":{"color":"#003087","primary_business":"Content, Streaming & Linear TV","key_assets":["Max (HBO Max)","HBO","CNN","Warner Bros. Pictures","Discovery+","TNT/TBS/Cartoon Network"],"ad_products":["Max Ads","CNN Ads","Discovery Networks Linear TV Ads","Turner Sports Ads"],"competitive_note":"Debt-heavy post-merger. Max is the premium streaming bet."},
    "Paramount":{"color":"#0064FF","primary_business":"Content, Streaming & Broadcast TV","key_assets":["Paramount+","CBS","CBS News / CBS Sports","MTV/Nickelodeon/Comedy Central","BET","Pluto TV","Paramount Pictures"],"ad_products":["CBS Broadcast Ads","Paramount+ Ads","Pluto TV (entirely ad-funded)","BET/MTV/Comedy Central linear ads"],"competitive_note":"Skydance merger completed 2024. Pluto TV is the largest free ad-supported streaming service."},
    "Snap":{"color":"#FFFC00","primary_business":"Social Camera / AR Platform","key_assets":["Snapchat","Snap Map","Spotlight (short video)","Spectacles (AR glasses)","Bitmoji / AR Lenses"],"ad_products":["Snap Ads (vertical video)","AR Lens Ads / Sponsored Filters","Dynamic Ads"],"competitive_note":"90%+ revenue from advertising. User base young (13-34). AR is the long-term bet."},
    "Pinterest":{"color":"#E60023","primary_business":"Visual Discovery & Shopping","key_assets":["Pinterest (image discovery)","Pinterest Shopping","Pinterest Predicts"],"ad_products":["Pinterest Ads (promoted pins)","Shopping Ads","Video Ads","Performance+"],"competitive_note":"480M+ MAU. Unique purchase-intent audience. Growing ARPU internationally."},
    "Nvidia":{"color":"#76B900","primary_business":"Semiconductors & AI Computing","key_assets":["Data Center GPUs (H100/B200)","Gaming GPUs (GeForce RTX)","NVIDIA DRIVE (automotive AI)","Omniverse","CUDA"],"ad_products":[],"competitive_note":"Data Center is now 85%+ of revenue. Every major AI model trains on Nvidia GPUs."},
}


def _metric_label_for_service(service: str) -> str:
    s = (service or "").lower()
    if any(k in s for k in ["whatsapp", "instagram", "facebook"]):
        return "Users"
    if "spotify" in s:
        if any(k in s for k in ["ad supported", "ad-supported", "adsupported", "free", "mau", "monthly active", "total", "totale"]):
            return "Ad Supported"
        if any(k in s for k in ["premium", "paid", "paying"]):
            return "Premium"
    return "Subscribers"

# Initialize subscriber data processor
if 'subscriber_processor' not in st.session_state:
    st.session_state['subscriber_processor'] = SubscriberDataProcessor()
else:
    existing_processor = st.session_state.get('subscriber_processor')
    if existing_processor is None or not hasattr(existing_processor, "is_source_updated"):
        st.session_state['subscriber_processor'] = SubscriberDataProcessor()
    else:
        try:
            if existing_processor.is_source_updated():
                st.session_state['subscriber_processor'] = SubscriberDataProcessor()
        except Exception:
            st.session_state['subscriber_processor'] = SubscriberDataProcessor()

# Get available streaming services and sort them alphabetically
services = sorted(st.session_state['subscriber_processor'].get_service_names())

# Load company logos function (same as in Welcome.py)
def load_company_logos():
    """Load and cache company logos with base64 encoding"""
    def _first_existing(*candidates: str):
        for p in candidates:
            if p and Path(p).exists():
                return p
        return None

    logo_paths = {
        'Disney+': 'attached_assets/icons8-logo-disney-240.png',
        'Netflix': 'attached_assets/Netflix_logo.png',
        'Paramount+': 'attached_assets/Paramount_logo.png',
        'Warner Bros Discovery': 'attached_assets/WarnerBrosDiscovery_log.png',
        'Warner Bros. Discovery': 'attached_assets/WarnerBrosDiscovery_log.png',
        'WBD': 'attached_assets/WarnerBrosDiscovery_log.png',
        'Spotify': 'attached_assets/Spotify_logo.png',
        'Alphabet': 'attached_assets/Google_logo.png',
        'Apple': 'attached_assets/apple_logo.png',
        'Microsoft': 'attached_assets/msft.png',
        'Meta Platforms': 'attached_assets/Meta_logo.png',
        'WhatsApp': _first_existing('attached_assets/Whatsapp_logo.png', 'attached_assets/Meta_logo.png'),
        'Instagram': _first_existing('attached_assets/Instagram_logo.png', 'attached_assets/Meta_logo.png'),
        'Facebook': _first_existing('attached_assets/Facebook.png', 'attached_assets/Meta_logo.png'),
        'Amazon': 'attached_assets/Amazon_icon.png',
        'Roku': 'attached_assets/roku_logo.png',
        'Comcast': 'attached_assets/Comcast_logo.png',
        'YouTube': _first_existing('attached_assets/Youtube_logo.png', 'attached_assets/Google_logo.png'),
        'Amazon Prime': _first_existing('attached_assets/Amazon_icon.png'),
        'Peacock': _first_existing('attached_assets/Comcast_logo.png'),
    }
    
    logos = {}
    
    for company, path in logo_paths.items():
        try:
            if os.path.exists(path):
                img = Image.open(path)
                img = img.convert('RGBA')
                buffered = BytesIO()
                img.save(buffered, format="PNG")
                img_str = base64.b64encode(buffered.getvalue()).decode()
                logos[company] = img_str
            else:
                print(f"Logo file not found: {path}")
        except Exception as e:
            print(f"Error loading logo for {company}: {e}")
    
    return logos

# Load logos with base64 encoding
service_logos = load_company_logos()

# Add hover effect CSS for company logos
st.markdown("""
<style>
    .company-logo {
        width: 50px;
        height: 50px;
        object-fit: contain;
        transition: all 0.3s ease;
    }
    .company-logo:hover {
        transform: scale(1.2);
        filter: drop-shadow(0 0 5px rgba(0,0,0,0.3));
        cursor: pointer;
    }
</style>
""", unsafe_allow_html=True)

def parse_quarter_to_date(quarter_str):
    """Convert quarter string to datetime object"""
    try:
        if quarter_str.startswith('Q'):
            # Format: "Q1 2020"
            q = int(quarter_str[1])
            year = int(quarter_str.split()[1])
        elif 'Q' in quarter_str:
            # Format: "20Q1"
            parts = quarter_str.split('Q')
            year = int(f"20{parts[0]}")
            q = int(parts[1])
        else:
            # Format: "1 2020"
            parts = quarter_str.split()
            q = int(parts[0])
            year = int(parts[1])

        month = (q - 1) * 3 + 1
        return pd.to_datetime(f"{year}-{month:02d}-01")
    except Exception as e:
        print(f"Failed to parse quarter '{quarter_str}': {str(e)}")
        return None

# Calculate current date and 5 years ago date for filtering
current_date = datetime.now()
five_years_ago = current_date - timedelta(days=5*365)

# Add tabs for single service view and comparison view
processor = st.session_state['subscriber_processor']
companies = processor.get_company_names() if hasattr(processor, "get_company_names") else []
selected_services = []

# Add chart zoom effect once
if not hasattr(st.session_state, 'editorial_chart_css_added'):
    st.markdown("""
    <style>
    .chart-container {
        transition: transform 0.3s ease;
        transform-origin: center center;
    }
    .chart-container:hover {
        transform: scale(1.02);
    }
    </style>
    """, unsafe_allow_html=True)
    st.session_state.editorial_chart_css_added = True

# Add tabs for single service view and comparison view
tab1, tab2 = st.tabs(["Individual Service Analysis", "Service Comparison"])

with tab1:
    raw_service_list = (
        processor.get_service_names()
        if hasattr(processor, "get_service_names")
        else services
    )
    service_list = sorted(raw_service_list, key=lambda s: str(s).lower())

    st.markdown(
        """
        <style>
        .editorial-mini-filter-label {
            font-size: 0.72rem;
            font-weight: 600;
            color: #6b7280;
            margin-bottom: 0.12rem;
            line-height: 1.1;
            letter-spacing: 0.02em;
        }
        [class*="st-key-editorial_"][class*="_series"] [data-baseweb="select"] > div,
        [class*="st-key-editorial_"][class*="_chart"] [data-baseweb="select"] > div,
        [class*="st-key-editorial_"][class*="_range"] [data-baseweb="select"] > div {
            min-height: 34px !important;
            padding-top: 0 !important;
            padding-bottom: 0 !important;
        }
        [class*="st-key-editorial_"][class*="_series"] [data-baseweb="select"] span,
        [class*="st-key-editorial_"][class*="_chart"] [data-baseweb="select"] span,
        [class*="st-key-editorial_"][class*="_range"] [data-baseweb="select"] span {
            font-size: 0.84rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    if not service_list:
        st.info("No services available.")
    else:
        for idx, service in enumerate(service_list):
            service_key_stub = re.sub(r"[^a-z0-9_]+", "_", str(service).lower()).strip("_") or f"service_{idx}"
            service_key_base = f"editorial_{service_key_stub}_{idx}"

            series_keys = (
                processor.get_series_columns([service])
                if hasattr(processor, "get_series_columns")
                else ["subscribers"]
            )
            default_series = (
                "subscribers"
                if "subscribers" in series_keys
                else (series_keys[0] if series_keys else None)
            )
            default_series_idx = (
                series_keys.index(default_series)
                if default_series in series_keys
                else 0
            )

            SERVICE_TO_LOGO = {
                "YouTube": "YouTube",
                "Amazon Prime": "Amazon Prime",
                "Amazon Prime Video": "Amazon",
                "Peacock": "Peacock",
                "Disney+": "Disney+",
                "Netflix": "Netflix",
                "Spotify": "Spotify",
                "Spotify Premium": "Spotify",
                "Facebook": "Facebook",
                "Instagram": "Instagram",
                "WhatsApp": "WhatsApp",
                "WBD": "Warner Bros. Discovery",
                "Paramount+": "Paramount+",
            }

            header_cols = st.columns([0.07, 0.24, 0.37, 0.14, 0.18])
            with header_cols[0]:
                logo_key_candidates = [
                    SERVICE_TO_LOGO.get(service, service),
                    service,
                    processor.df_subscribers[processor.df_subscribers["service"] == service]["company"].iloc[0]
                    if "company" in processor.df_subscribers.columns
                    and (processor.df_subscribers["service"] == service).any()
                    else "",
                ]
                logo_b64 = None
                for candidate in logo_key_candidates:
                    if candidate in service_logos:
                        logo_b64 = service_logos[candidate]
                        break
                if logo_b64:
                    st.markdown(
                        f"<img src='data:image/png;base64,{logo_b64}' class='company-logo'>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.write(str(service)[0] if str(service) else "?")

            with header_cols[1]:
                st.subheader(service)

            with header_cols[2]:
                st.markdown("<div class='editorial-mini-filter-label'>Split</div>", unsafe_allow_html=True)
                selected_series_key = st.selectbox(
                    "Series",
                    series_keys,
                    index=default_series_idx,
                    format_func=lambda key: processor.get_series_label(key)
                    if hasattr(processor, "get_series_label")
                    else str(key),
                    key=f"{service_key_base}_series",
                    label_visibility="collapsed",
                ) if series_keys else None

            with header_cols[3]:
                st.markdown("<div class='editorial-mini-filter-label'>Chart</div>", unsafe_allow_html=True)
                service_chart_type = st.selectbox(
                    "Chart",
                    options=["Line", "Bar"],
                    index=0,
                    key=f"{service_key_base}_chart",
                    label_visibility="collapsed",
                )

            with header_cols[4]:
                st.markdown("<div class='editorial-mini-filter-label'>Range</div>", unsafe_allow_html=True)
                service_range = st.selectbox(
                    "Range",
                    options=["5Y", "10Y", "All"],
                    index=0,
                    key=f"{service_key_base}_range",
                    label_visibility="collapsed",
                )

            service_data = processor.get_service_data(service, series_key=selected_series_key)
            df_service = service_data.get("data", pd.DataFrame()).copy()
            if df_service.empty:
                st.warning(f"No data available for {service}")
                st.markdown("---")
                continue

            column_name = service_data.get("column_name", "Subscribers")
            if column_name not in df_service.columns:
                st.warning(f"No {column_name} column found for {service}")
                st.markdown("---")
                continue

            df_service["date"] = df_service["Quarter"].apply(parse_quarter_to_date)
            df_service = df_service.dropna(subset=["date"])
            df_service = df_service.sort_values("date")

            if service_range != "All":
                years_back = int(service_range.replace("Y", ""))
                cutoff_date = current_date - timedelta(days=years_back * 365)
                df_service = df_service[df_service["date"] >= cutoff_date]

            if df_service.empty:
                st.info(f"No data for {service} in selected range.")
                st.markdown("---")
                continue

            latest_value = df_service.iloc[-1][column_name]
            latest_quarter = df_service.iloc[-1]["Quarter"]
            yoy_growth = None
            if len(df_service) >= 5:
                previous_value = df_service.iloc[-5][column_name]
                if previous_value not in (None, 0):
                    yoy_growth = ((latest_value - previous_value) / previous_value) * 100

            metric_col, chart_col = st.columns([0.22, 0.78])
            with metric_col:
                label_key = selected_series_key or "subscribers"
                if label_key == "subscribers":
                    metric_label = _metric_label_for_service(service)
                else:
                    metric_label = processor.get_series_label(label_key)
                st.metric(
                    label=f"{metric_label} ({service_data.get('unit', 'millions')})",
                    value=f"{latest_value:,.1f}",
                    delta=f"{yoy_growth:+.1f}%" if yoy_growth is not None else None,
                    delta_color="normal",
                )
                st.markdown(
                    f"<p style='color:#6b7280;font-size:0.78rem;margin:2px 0 0 0;'>Period: {latest_quarter}</p>",
                    unsafe_allow_html=True
                )

            with chart_col:
                _color = _service_color(service)
                _r, _g, _b = _hex_to_rgb(_color)
                _fill_rgba = f"rgba({_r},{_g},{_b},0.08)"

                _y_vals = pd.to_numeric(df_service[column_name], errors="coerce").tolist()
                _x_labels = df_service["Quarter"].tolist()
                _unit = service_data.get("unit", "millions")

                # Build animated Canvas chart — draws line on hover loop
                import json as _json
                _chart_id = f"sc_{service.replace(' ','_').replace('+','p').replace('/','s')}"
                _data_json = _json.dumps({"x": _x_labels, "y": _y_vals})
                _anim_html = (
                    "<div id='" + _chart_id + "_wrap' style='width:100%;height:220px;position:relative;cursor:crosshair;'>"
                    "<canvas id='" + _chart_id + "' style='width:100%;height:100%;display:block;'></canvas>"
                    "<div id='" + _chart_id + "_tip' style='position:absolute;display:none;pointer-events:none;"
                    "background:rgba(17,24,39,0.92);color:#f9fafb;font:11px DM Sans,Inter,sans-serif;"
                    "padding:5px 9px;border-radius:6px;white-space:nowrap;z-index:9;'></div>"
                    "</div>"
                    "<script>"
                    "(function(){"
                    "var D=" + _data_json + ";"
                    "var color='" + _color + "';"
                    "var fillRGBA='" + _fill_rgba + "';"
                    "var svc='" + service.replace("'", "\\'") + "';"
                    "var unit='" + _unit.replace("'", "\\'") + "';"
                    "var wrap=document.getElementById('" + _chart_id + "_wrap');"
                    "var cvs=document.getElementById('" + _chart_id + "');"
                    "var tip=document.getElementById('" + _chart_id + "_tip');"
                    "var ctx=cvs.getContext('2d');"
                    "var dpr=window.devicePixelRatio||1;"
                    "var W,H,padL=44,padR=12,padT=14,padB=36;"
                    "var xs=D.x,ys=D.y,N=xs.length;"
                    "var yMin=Math.min.apply(null,ys),yMax=Math.max.apply(null,ys);"
                    "if(yMin===yMax){yMin*=0.9;yMax*=1.1;}"
                    "var animProg=1,animId=null,speed=0.025;"

                    "function resize(){"
                    "  W=wrap.clientWidth;H=wrap.clientHeight;"
                    "  cvs.width=W*dpr;cvs.height=H*dpr;"
                    "  cvs.style.width=W+'px';cvs.style.height=H+'px';"
                    "  ctx.setTransform(dpr,0,0,dpr,0,0);"
                    "}"

                    "function px(i){return padL+(W-padL-padR)*i/(N-1);}"
                    "function py(v){return padT+(H-padT-padB)*(1-(v-yMin)/(yMax-yMin));}"

                    "function drawGrid(){"
                    "  ctx.clearRect(0,0,W,H);"
                    "  ctx.strokeStyle='rgba(0,0,0,0.05)';ctx.lineWidth=0.5;"
                    "  for(var g=0;g<5;g++){"
                    "    var gy=padT+(H-padT-padB)*g/4;"
                    "    ctx.beginPath();ctx.moveTo(padL,gy);ctx.lineTo(W-padR,gy);ctx.stroke();"
                    "    var val=yMax-(yMax-yMin)*g/4;"
                    "    ctx.fillStyle='#9ca3af';ctx.font='10px DM Sans,Inter,sans-serif';ctx.textAlign='right';"
                    "    ctx.fillText(val>=100?Math.round(val).toLocaleString():val.toFixed(1),padL-6,gy+3);"
                    "  }"
                    "  ctx.fillStyle='#9ca3af';ctx.font='10px DM Sans,Inter,sans-serif';ctx.textAlign='center';"
                    "  var step=Math.max(1,Math.floor(N/6));"
                    "  for(var i=0;i<N;i+=step){"
                    "    ctx.save();ctx.translate(px(i),H-padB+14);ctx.rotate(-0.65);"
                    "    ctx.fillText(xs[i],0,0);ctx.restore();"
                    "  }"
                    "}"

                    "function drawLine(prog){"
                    "  drawGrid();"
                    "  var pts=Math.min(N,Math.floor(prog*N)+1);"
                    "  if(pts<2)return;"
                    "  ctx.beginPath();ctx.moveTo(px(0),py(ys[0]));"
                    "  for(var i=1;i<pts;i++){ctx.lineTo(px(i),py(ys[i]));}"
                    "  if(prog<1){var frac=prog*N-Math.floor(prog*N);"
                    "    var li=Math.min(pts-1,N-2),ni=Math.min(li+1,N-1);"
                    "    ctx.lineTo(px(li)+frac*(px(ni)-px(li)),py(ys[li])+frac*(py(ys[ni])-py(ys[li])));"
                    "  }"
                    "  ctx.strokeStyle=color;ctx.lineWidth=2.5;ctx.lineJoin='round';ctx.lineCap='round';ctx.stroke();"
                    "  ctx.lineTo(px(pts-1),H-padB);ctx.lineTo(px(0),H-padB);ctx.closePath();"
                    "  ctx.fillStyle=fillRGBA;ctx.fill();"
                    "  if(prog>=1){"
                    "    ctx.beginPath();ctx.arc(px(N-1),py(ys[N-1]),4,0,Math.PI*2);"
                    "    ctx.fillStyle=color;ctx.fill();"
                    "  }"
                    "}"

                    "function animate(){"
                    "  animProg+=speed;"
                    "  if(animProg>=1){animProg=1;drawLine(1);animId=null;return;}"
                    "  drawLine(animProg);animId=requestAnimationFrame(animate);"
                    "}"

                    "function startAnim(){"
                    "  if(animId)cancelAnimationFrame(animId);"
                    "  animProg=0;animId=requestAnimationFrame(animate);"
                    "}"

                    "wrap.addEventListener('mouseenter',startAnim);"

                    "wrap.addEventListener('mousemove',function(e){"
                    "  var rect=cvs.getBoundingClientRect();"
                    "  var mx=e.clientX-rect.left,my=e.clientY-rect.top;"
                    "  var idx=Math.round((mx-padL)/(W-padL-padR)*(N-1));"
                    "  idx=Math.max(0,Math.min(N-1,idx));"
                    "  var v=ys[idx];"
                    "  tip.style.display='block';"
                    "  tip.innerHTML='<b>'+svc+'</b><br>'+xs[idx]+'<br>'+v.toFixed(1)+' '+unit;"
                    "  var tx=px(idx)+8,ty=py(v)-30;"
                    "  if(tx+130>W)tx=px(idx)-140;"
                    "  tip.style.left=tx+'px';tip.style.top=Math.max(0,ty)+'px';"
                    "});"

                    "wrap.addEventListener('mouseleave',function(){tip.style.display='none';});"

                    "resize();drawLine(1);"
                    "window.addEventListener('resize',function(){resize();drawLine(animProg);});"
                    "})();"
                    "</script>"
                )
                st.components.v1.html(_anim_html, height=220, scrolling=False)

            st.markdown("---")

with tab2:
    comparison_filter_cols = st.columns(3)

    with comparison_filter_cols[0]:
        comparison_company_filter = st.selectbox(
            "Company filter",
            ["All"] + companies,
            key="editorial_comparison_company_filter",
        )

    comparison_service_pool = (
        processor.get_service_names(None if comparison_company_filter == "All" else comparison_company_filter)
        if hasattr(processor, "get_service_names")
        else services
    )
    default_comparison_services = comparison_service_pool[:2] if len(comparison_service_pool) >= 2 else comparison_service_pool

    with comparison_filter_cols[1]:
        selected_services = st.multiselect(
            "Services",
            options=comparison_service_pool,
            default=default_comparison_services,
            key="editorial_comparison_services_filter",
            help="Select services to compare.",
        )

    comparison_series_options = (
        processor.get_series_columns(selected_services if selected_services else comparison_service_pool)
        if hasattr(processor, "get_series_columns")
        else ["subscribers"]
    )
    comparison_default_series = (
        "subscribers"
        if "subscribers" in comparison_series_options
        else (comparison_series_options[0] if comparison_series_options else None)
    )
    comparison_default_series_idx = (
        comparison_series_options.index(comparison_default_series)
        if comparison_default_series in comparison_series_options
        else 0
    )

    with comparison_filter_cols[2]:
        comparison_series_key = st.selectbox(
            "Series / Split",
            comparison_series_options,
            index=comparison_default_series_idx,
            format_func=lambda key: processor.get_series_label(key) if hasattr(processor, "get_series_label") else str(key),
            key="editorial_comparison_series_filter",
        ) if comparison_series_options else None

    comparison_control_cols = st.columns(2)
    with comparison_control_cols[0]:
        comparison_chart_type = st.radio(
            "Chart Type",
            options=["Line", "Bar"],
            horizontal=True,
            key="editorial_comparison_chart_type",
        )

    comparison_long = (
        processor.get_long_series_data(services=selected_services, series_keys=[comparison_series_key])
        if selected_services and comparison_series_key and hasattr(processor, "get_long_series_data")
        else pd.DataFrame()
    )
    if not comparison_long.empty:
        comparison_long["date"] = comparison_long["Quarter"].apply(parse_quarter_to_date)
        comparison_long = comparison_long.dropna(subset=["date"])
        comparison_long = comparison_long.sort_values(["service", "date"])

    with comparison_control_cols[1]:
        if not comparison_long.empty:
            years = sorted(comparison_long["date"].dt.year.unique().tolist())
            min_year = int(min(years))
            max_year = int(max(years))
            default_start = max(min_year, max_year - 4)
            comparison_year_range = st.slider(
                "Select Year Range",
                min_value=min_year,
                max_value=max_year,
                value=(default_start, max_year),
                step=1,
                key="editorial_comparison_year_range",
            )
            comparison_long = comparison_long[
                comparison_long["date"].dt.year.between(comparison_year_range[0], comparison_year_range[1])
            ]
        else:
            comparison_year_range = None

    if selected_services and not comparison_long.empty:
        available_service_set = set(comparison_long["service"].unique().tolist())
        missing_services = [s for s in selected_services if s not in available_service_set]
        if missing_services:
            st.warning(
                "No data for selected split in: " + ", ".join(missing_services)
            )

        quarter_order = (
            comparison_long[["Quarter", "date"]]
            .drop_duplicates()
            .sort_values("date")["Quarter"]
            .tolist()
        )

        fig = go.Figure()
        for service_name in selected_services:
            df_service = comparison_long[comparison_long["service"] == service_name]
            if df_service.empty:
                continue
            df_service = df_service.sort_values("date")
            hover_template = (
                f"<b>{service_name}</b><br>"
                "Quarter: %{x}<br>"
                "Value: %{y:.1f}<br>"
                "<extra></extra>"
            )
            if comparison_chart_type == "Line":
                fig.add_trace(
                    go.Scatter(
                        x=df_service["Quarter"],
                        y=df_service["value"],
                        mode="lines+markers",
                        connectgaps=False,
                        name=service_name,
                        hovertemplate=hover_template,
                        line=dict(width=2.5, shape='spline'),
                        marker=dict(size=5, opacity=0.8),
                    )
                )
            else:
                fig.add_trace(
                    go.Bar(
                        x=df_service["Quarter"],
                        y=df_service["value"],
                        name=service_name,
                        hovertemplate=hover_template,
                        marker_line_width=0,
                        opacity=0.85,
                    )
                )

        series_title = (
            processor.get_series_label(comparison_series_key)
            if hasattr(processor, "get_series_label") and comparison_series_key
            else "Subscribers"
        )
        units = sorted(
            comparison_long["unit"].dropna().astype(str).str.strip().replace("", "millions").unique().tolist()
        )
        y_title = series_title if len(units) != 1 else f"{series_title} ({units[0]})"

        fig.update_layout(
            title=f"Service Comparison — {series_title}",
            title_font=dict(color="#111827", size=14),
            xaxis_title="Quarter",
            yaxis_title=y_title,
            showlegend=True,
            height=500,
            template='plotly_white',
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(family="DM Sans, Inter, sans-serif", size=12, color="#374151"),
            xaxis=dict(
                type='category',
                categoryorder='array',
                categoryarray=quarter_order,
                tickangle=45,
                showgrid=False,
                showline=False,
                zeroline=False,
                tickfont=dict(size=11, color="#374151"),
                title_font=dict(color="#6b7280"),
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgba(0,0,0,0.05)',
                showline=False,
                zeroline=False,
                tickfont=dict(size=11, color="#374151"),
                title_font=dict(color="#6b7280"),
            ),
            legend=dict(
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="rgba(0,0,0,0.08)",
                borderwidth=1,
                font=dict(size=11, color="#374151"),
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            hoverlabel=dict(
                bgcolor="rgba(17,24,39,0.95)",
                bordercolor="rgba(99,179,237,0.4)",
                font=dict(size=12, color="#f9fafb", family="DM Sans, Inter, sans-serif"),
            ),
            barmode='group' if comparison_chart_type == "Bar" else None,
        )

        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        st.markdown('</div>', unsafe_allow_html=True)
    elif selected_services:
        st.info("No data available for the selected filters.")
    else:
        st.info("Select services to compare.")

# Update AI context
dashboard_state = {
    'page': 'Editorial',
    'comparison_services': selected_services if 'selected_services' in locals() else []
}
if 'ai_chat' in st.session_state:
    ai_chat = st.session_state.get("ai_chat")
    if ai_chat is not None and hasattr(ai_chat, "update_context"):
        ai_chat.update_context(dashboard_state)
