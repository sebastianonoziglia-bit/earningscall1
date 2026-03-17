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


def _normalize_service_to_company(service_name: str) -> str:
    """Map a service/platform name to its parent company."""
    return SERVICE_TO_COMPANY.get(service_name.strip(), service_name.strip())


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


@st.cache_data(ttl=3600)
def _load_transcript_editorial_insights(max_per_company: int = 5) -> list:
    try:
        from utils.workbook_source import resolve_financial_data_xlsx as _rfd
        excel_path = _rfd([])
    except Exception:
        excel_path = None
    if not excel_path:
        return []
    try:
        raw_df = pd.read_excel(excel_path, sheet_name="Transcripts")
        if raw_df is None or raw_df.empty:
            return []
        TRIGGERS = [
            "we expect","we anticipate","our outlook","looking ahead","heading into",
            "next quarter","we believe","going forward","guidance","we plan to",
            "opportunity","positioned to"
        ]
        rows = []
        for _, row in raw_df.iterrows():
            comp = str(row.get("company","")).strip()
            year = pd.to_numeric(row.get("year"), errors="coerce")
            quarter = str(row.get("quarter","")).strip()
            text = str(row.get("transcript_text","") or "")
            if not text or pd.isna(year):
                continue
            import re as _re2
            sentences = _re2.split(r'(?<=[.!?])\s+', text)
            count = 0
            for sentence in sentences:
                s = sentence.strip()
                if len(s) < 40 or len(s) > 350:
                    continue
                if any(t in s.lower() for t in TRIGGERS):
                    rows.append({
                        "company": comp, "year": int(year), "quarter": quarter,
                        "highlight": s, "speaker": "", "category": "Outlook"
                    })
                    count += 1
                    if count >= max_per_company:
                        break
        return rows
    except Exception:
        return []


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
        'Netflix': 'attached_assets/9.png',
        'Paramount+': 'attached_assets/Paramount.png',
        'Warner Bros Discovery': 'attached_assets/adadad.png',
        'Warner Bros. Discovery': 'attached_assets/adadad.png',
        'WBD': 'attached_assets/adadad.png',
        'Spotify': 'attached_assets/11.png',
        'Alphabet': 'attached_assets/8.png',  # Reverted to original working logo
        'Apple': 'attached_assets/10.png',
        'Microsoft': 'attached_assets/msft.png',
        'Meta Platforms': 'attached_assets/12.png',
        # Meta-owned apps: prefer dedicated logos if present, otherwise fall back to Meta.
        'WhatsApp': _first_existing('attached_assets/Whatsapp.png', 'attached_assets/WhatsApp.png', 'attached_assets/12.png'),
        'Instagram': _first_existing('attached_assets/Instagram.png', 'attached_assets/12.png'),
        'Facebook': _first_existing('attached_assets/Facebook.png', 'attached_assets/12.png'),
        'Amazon': 'attached_assets/Amazon_icon.png',
        'Roku': 'attached_assets/rokudef.png',
        'Comcast': 'attached_assets/6.png',
        'YouTube': _first_existing('attached_assets/youtube_logo.png', 'attached_assets/8.png'),
        'Amazon Prime': _first_existing('attached_assets/Amazon_icon.png', 'attached_assets/Amazon.png'),
        'Peacock': _first_existing('attached_assets/6.png'),
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
                _fill_color = f"rgba({_r},{_g},{_b},0.08)"

                _frames = []
                _y_vals = pd.to_numeric(df_service[column_name], errors="coerce")
                for _fi in range(2, len(df_service) + 1):
                    _frames.append(go.Frame(
                        data=[go.Scatter(
                            x=df_service["Quarter"].iloc[:_fi],
                            y=_y_vals.iloc[:_fi],
                            mode="lines",
                            line=dict(width=2.5, color=_color, shape="spline"),
                            fill="tozeroy",
                            fillcolor=_fill_color,
                        )],
                        name=str(_fi),
                    ))

                fig = go.Figure(
                    data=[go.Scatter(
                        x=df_service["Quarter"].iloc[:1],
                        y=_y_vals.iloc[:1],
                        mode="lines",
                        line=dict(width=2.5, color=_color, shape="spline"),
                        fill="tozeroy",
                        fillcolor=_fill_color,
                        hovertemplate=(
                            f"<b>{service}</b><br>Quarter: %{{x}}<br>"
                            f"Value: %{{y:.1f}} {service_data.get('unit','millions')}<extra></extra>"
                        ),
                    )],
                    frames=_frames,
                )
                fig.update_layout(
                    margin=dict(l=20, r=20, t=25, b=50),
                    height=220,
                    showlegend=False,
                    template="plotly_white",
                    xaxis_title=None,
                    yaxis_title=None,
                    plot_bgcolor="white",
                    paper_bgcolor="white",
                    font=dict(family="DM Sans, Inter, sans-serif", size=11, color="#374151"),
                    xaxis=dict(
                        type="category",
                        categoryorder="array",
                        categoryarray=df_service["Quarter"].tolist(),
                        tickangle=45,
                        showgrid=False,
                        tickfont=dict(size=10, color="#6b7280"),
                    ),
                    yaxis=dict(
                        showgrid=True,
                        gridcolor="rgba(0,0,0,0.05)",
                        tickfont=dict(size=10, color="#6b7280"),
                    ),
                    updatemenus=[dict(
                        type="buttons",
                        showactive=False,
                        visible=False,
                        buttons=[dict(
                            label="Play",
                            method="animate",
                            args=[None, dict(
                                frame=dict(duration=60, redraw=True),
                                fromcurrent=True,
                                mode="immediate",
                                transition=dict(duration=0),
                            )],
                        )],
                    )],
                )

                _chart_key = f"chart_{service.replace(' ','_').replace('+','plus')}_{abs(hash(service))}"
                st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                st.plotly_chart(fig, use_container_width=True, key=_chart_key)
                st.markdown('</div>', unsafe_allow_html=True)

                _js_key = f"anim_{abs(hash(service)) % 99999}"
                st.components.v1.html(f"""
<script>
(function() {{
    var attempts = 0;
    function tryPlay() {{
        attempts++;
        if (attempts > 20) return;
        var plots = window.parent.document.querySelectorAll('.js-plotly-plot');
        for (var i = 0; i < plots.length; i++) {{
            var el = plots[i];
            if (el._played_{_js_key}) continue;
            if (el._fullData && el._fullData.length > 0) {{
                el._played_{_js_key} = true;
                try {{
                    Plotly.animate(el, null, {{
                        frame: {{duration: 60, redraw: true}},
                        fromcurrent: true,
                        mode: 'immediate',
                        transition: {{duration: 0}},
                    }});
                }} catch(e) {{ el._played_{_js_key} = false; }}
                break;
            }}
        }}
        if (attempts < 20) setTimeout(tryPlay, 300);
    }}
    setTimeout(tryPlay, 400);
}})();
</script>
""", height=0)

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
        st.plotly_chart(fig, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    elif selected_services:
        st.info("No data available for the selected filters.")
    else:
        st.info("Select services to compare.")

# ── TRANSCRIPT INTELLIGENCE ─────────────────────────────────────────────────
st.markdown("<hr style='margin: 3rem 0 1.5rem 0;'>", unsafe_allow_html=True)
st.markdown("## Transcript Intelligence")
st.markdown(
    "<p style='color:#6b7280;margin-bottom:1.5rem;'>Management statements from earnings calls — "
    "organised by theme. Select a company to explore.</p>",
    unsafe_allow_html=True
)

_ti_insights = _load_transcript_editorial_insights(max_per_company=20)

_ti_by_company: dict = {}
if _ti_insights:
    for _item in _ti_insights:
        _parent_co = _normalize_service_to_company(_item["company"])
        if _parent_co not in _ti_by_company:
            _ti_by_company[_parent_co] = {"signals": []}
        _ti_by_company[_parent_co]["signals"].append(_item)

_ti_companies = sorted(_ti_by_company.keys()) if _ti_by_company else []
if not _ti_companies:
    st.info("No transcript insights loaded. Ensure transcripts are in the Excel Transcripts sheet.")
else:
    _selected_ti_co = st.session_state.get("ti_selected_company", _ti_companies[0])
    if _selected_ti_co not in _ti_companies:
        _selected_ti_co = _ti_companies[0]

    pills_html = "<div style='display:flex;flex-wrap:wrap;gap:8px;margin-bottom:1.5rem;'>"
    for _co in _ti_companies:
        _count = len(_ti_by_company.get(_co, {}).get("signals", []))
        _active = _co == _selected_ti_co
        _bg = "#111827" if _active else "#f3f4f6"
        _col = "white" if _active else "#374151"
        _border = "#111827" if _active else "#e5e7eb"
        pills_html += (
            f"<span style='background:{_bg};color:{_col};border:1px solid {_border};"
            f"border-radius:20px;padding:6px 14px;font-size:0.82rem;"
            f"font-family:DM Sans,sans-serif;font-weight:500;display:inline-block;'>"
            f"{_co} <span style='opacity:0.6;font-size:0.75rem;'>·&nbsp;{_count}</span>"
            f"</span>"
        )
    pills_html += "</div>"
    st.markdown(pills_html, unsafe_allow_html=True)

    _selected_ti_co = st.selectbox(
        "Company",
        _ti_companies,
        index=_ti_companies.index(_selected_ti_co) if _selected_ti_co in _ti_companies else 0,
        key="ti_company_selector",
        label_visibility="collapsed",
    )
    st.session_state["ti_selected_company"] = _selected_ti_co

    _co_data = _ti_by_company.get(_selected_ti_co, {})
    _signals = _co_data.get("signals", [])

    if not _signals:
        st.info(f"No transcript signals found for {_selected_ti_co}.")
    else:
        from collections import defaultdict as _dd
        _by_cat = _dd(list)
        for _sig in _signals:
            _by_cat[_sig.get("category", "General")].append(_sig)

        _cats = sorted(_by_cat.keys())
        if _cats:
            _cat_tabs = st.tabs(_cats)
            for _tab, _cat in zip(_cat_tabs, _cats):
                with _tab:
                    _cat_signals = _by_cat[_cat]
                    _periods = sorted(set(
                        f"{s.get('year','')} {s.get('quarter','')}".strip()
                        for s in _cat_signals
                        if s.get("year")
                    ), reverse=True)

                    if len(_periods) > 1:
                        _sel_period = st.select_slider(
                            "Period",
                            options=_periods,
                            value=_periods[0],
                            key=f"ti_period_{_selected_ti_co}_{_cat}",
                            label_visibility="collapsed",
                        )
                    else:
                        _sel_period = _periods[0] if _periods else None

                    if _sel_period:
                        _yr_str, *_q_parts = _sel_period.split()
                        _q_str = _q_parts[0] if _q_parts else ""
                        _filtered = [
                            s for s in _cat_signals
                            if str(s.get("year", "")) == _yr_str
                            and (not _q_str or str(s.get("quarter", "")).upper() == _q_str.upper())
                        ]
                    else:
                        _filtered = _cat_signals

                    _sig_cols = st.columns(2)
                    for _si, _sig in enumerate(_filtered[:20]):
                        with _sig_cols[_si % 2]:
                            _quote = str(_sig.get("quote", _sig.get("text", _sig.get("highlight", "")))).strip()
                            _speaker = str(_sig.get("speaker", "")).strip()
                            _role = str(_sig.get("role", _sig.get("role_bucket", ""))).strip()
                            _period = f"{_sig.get('year', '')} {_sig.get('quarter', '')}".strip()

                            if len(_quote) > 200:
                                _quote = _quote[:200].rsplit(" ", 1)[0] + "…"

                            if not _quote:
                                continue

                            _speaker_line = ""
                            if _speaker and _speaker.lower() not in ("", "unknown", "nan"):
                                _role_html = (
                                    f"&nbsp;·&nbsp;<span style=\"color:#9ca3af\">{_role}</span>"
                                    if _role else ""
                                )
                                _speaker_line = (
                                    f"<div style='font-size:0.75rem;color:#6b7280;margin-top:6px;'>"
                                    f"{_speaker}{_role_html}</div>"
                                )

                            st.markdown(
                                f"<div style='border:1px solid #e5e7eb;border-radius:8px;"
                                f"padding:12px 14px;margin-bottom:8px;background:#fafafa;"
                                f"border-left:3px solid #111827;'>"
                                f"<p style='margin:0;font-size:0.85rem;color:#374151;"
                                f"line-height:1.6;font-style:italic;'>\"{_quote}\"</p>"
                                f"{_speaker_line}"
                                f"<div style='font-size:0.7rem;color:#9ca3af;margin-top:4px;'>{_period}</div>"
                                f"</div>",
                                unsafe_allow_html=True
                            )

# Update AI context
dashboard_state = {
    'page': 'Editorial',
    'comparison_services': selected_services if 'selected_services' in locals() else []
}
if 'ai_chat' in st.session_state:
    ai_chat = st.session_state.get("ai_chat")
    if ai_chat is not None and hasattr(ai_chat, "update_context"):
        ai_chat.update_context(dashboard_state)
