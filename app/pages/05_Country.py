"""
Country Deep Dive — dark-theme page for individual country ad market analysis.
Navigated to from Overview's country buttons.
"""
from __future__ import annotations
import json
import logging
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

st.set_page_config(page_title="Country Deep Dive", layout="wide", initial_sidebar_state="collapsed")

# ── Dark theme CSS ────────────────────────────────────────────────────────────
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
[data-testid="stAppViewContainer"] { background: #0a1628 !important; }
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stSidebar"] { background: #0d1f35 !important; }
section[data-testid="stMain"] { background: #0a1628 !important; }
.stApp { background: #0a1628 !important; }
h1, h2, h3, h4, h5, h6, p, span, label, div { color: #e2e8f0; }
.stSelectbox label, .stRadio label, .stSlider label { color: #e2e8f0 !important; }
[data-testid="stMetricValue"] { color: #f1f5f9 !important; }
[data-testid="stMetricLabel"] { color: #94a3b8 !important; }
[data-testid="stMetricDelta"] { color: inherit !important; }
[data-testid="stButton"] button[kind="secondary"] {
    background: rgba(255,255,255,0.05) !important;
    color: #94a3b8 !important; border: 1px solid rgba(255,255,255,0.1) !important;
}
[data-testid="stButton"] button[kind="secondary"]:hover {
    background: rgba(74,174,255,0.1) !important; color: #e2e8f0 !important;
}
</style>""", unsafe_allow_html=True)

# ── Region mapping ────────────────────────────────────────────────────────────
REGION_MAP = {}
try:
    from utils.data_loader import CONTINENT_MAPPINGS
    for region, countries in CONTINENT_MAPPINGS.items():
        for c in countries:
            REGION_MAP[c] = region
except ImportError:
    pass


def _get_region(country: str) -> str:
    return REGION_MAP.get(country, "")


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def _load_country_totals(excel_path: str, mtime: float) -> pd.DataFrame:
    try:
        df = pd.read_excel(excel_path, sheet_name="Country_Totals_vs_GDP")
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_country_channels(excel_path: str, mtime: float) -> pd.DataFrame:
    try:
        df = pd.read_excel(excel_path, sheet_name="Country_Advertising_Data_FullVi")
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return pd.DataFrame()


def _find_col(df: pd.DataFrame, hints: list[str]) -> str | None:
    for h in hints:
        for c in df.columns:
            if h.lower() in c.lower():
                return c
    return None


def _dark_layout(fig, title=""):
    fig.update_layout(
        paper_bgcolor="#0a1628",
        plot_bgcolor="rgba(255,255,255,0.03)",
        font=dict(color="#e2e8f0", family="DM Sans"),
        title=dict(text=title, font=dict(color="#f1f5f9", size=16)),
        xaxis=dict(gridcolor="rgba(255,255,255,0.06)", tickfont=dict(color="#94a3b8")),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)", tickfont=dict(color="#94a3b8")),
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(font=dict(color="#94a3b8")),
    )
    return fig


# ── Globe renderer ────────────────────────────────────────────────────────────
def _render_country_globe(country: str, df_totals: pd.DataFrame, metric_col: str, year: int):
    """Render D3 choropleth globe highlighted on selected country."""
    _CHOROPLETH_ISO_TO_NAME = {
        "USA": "United States", "GBR": "United Kingdom", "DEU": "Germany", "FRA": "France",
        "JPN": "Japan", "CHN": "China", "IND": "India", "BRA": "Brazil", "CAN": "Canada",
        "AUS": "Australia", "ITA": "Italy", "ESP": "Spain", "KOR": "South Korea",
        "RUS": "Russia", "MEX": "Mexico", "IDN": "Indonesia", "TUR": "Turkey",
        "NLD": "Netherlands", "SAU": "Saudi Arabia", "CHE": "Switzerland", "SWE": "Sweden",
        "POL": "Poland", "BEL": "Belgium", "NOR": "Norway", "AUT": "Austria",
        "ARE": "United Arab Emirates", "THA": "Thailand", "SGP": "Singapore",
        "MYS": "Malaysia", "PHL": "Philippines", "VNM": "Vietnam", "ZAF": "South Africa",
        "EGY": "Egypt", "NGA": "Nigeria", "ARG": "Argentina", "COL": "Colombia",
        "CHL": "Chile", "PER": "Peru", "PRT": "Portugal", "DNK": "Denmark",
        "FIN": "Finland", "IRL": "Ireland", "NZL": "New Zealand", "ISR": "Israel",
        "CZE": "Czech Republic", "GRC": "Greece", "HUN": "Hungary", "ROU": "Romania",
        "HKG": "Hong Kong", "TWN": "Taiwan", "PAK": "Pakistan", "BGD": "Bangladesh",
    }
    _NAME_TO_ISO = {v: k for k, v in _CHOROPLETH_ISO_TO_NAME.items()}

    _NUM_ISO_MAP = {
        "356": "IND", "360": "IDN", "076": "BRA", "566": "NGA", "050": "BGD",
        "586": "PAK", "840": "USA", "826": "GBR", "276": "DEU", "250": "FRA",
        "380": "ITA", "724": "ESP", "792": "TUR", "364": "IRN", "682": "SAU",
        "784": "ARE", "818": "EGY", "504": "MAR", "710": "ZAF", "392": "JPN",
        "410": "KOR", "156": "CHN", "036": "AUS", "554": "NZL", "124": "CAN",
        "484": "MEX", "170": "COL", "032": "ARG", "604": "PER", "152": "CHL",
        "528": "NLD", "056": "BEL", "756": "CHE", "040": "AUT", "620": "PRT",
        "616": "POL", "203": "CZE", "348": "HUN", "300": "GRC", "752": "SWE",
        "578": "NOR", "208": "DNK", "246": "FIN", "372": "IRL", "702": "SGP",
        "458": "MYS", "344": "HKG", "158": "TWN", "764": "THA", "704": "VNM",
        "608": "PHL", "642": "ROU", "688": "SRB", "100": "BGR",
    }

    country_col = _find_col(df_totals, ["Country", "country"])
    value_col = _find_col(df_totals, [metric_col]) or _find_col(df_totals, ["Ad_vs_GDP", "ad", "gdp"])
    year_col = _find_col(df_totals, ["Year", "year"])
    if not country_col or not value_col or not year_col:
        st.info("Globe data unavailable.")
        return

    scoped = df_totals[df_totals[year_col] == year].copy()
    scoped[value_col] = pd.to_numeric(scoped[value_col], errors="coerce")
    scoped = scoped.dropna(subset=[country_col, value_col])

    ad_gdp_data = {}
    for _, row in scoped.iterrows():
        iso = _NAME_TO_ISO.get(str(row[country_col]), "")
        if iso:
            ad_gdp_data[iso] = round(float(row[value_col]), 3)

    highlight_iso = _NAME_TO_ISO.get(country, "")

    # Determine rotation to center on the highlighted country
    _COUNTRY_COORDS = {
        "USA": [-100, 40], "GBR": [0, 54], "DEU": [10, 51], "FRA": [2, 47],
        "JPN": [139, 36], "CHN": [105, 35], "IND": [78, 21], "BRA": [-48, -15],
        "CAN": [-95, 56], "AUS": [134, -25], "ITA": [12, 42], "ESP": [-4, 40],
        "KOR": [127, 37], "MEX": [-99, 19], "ZAF": [25, -29], "ARG": [-64, -34],
        "TUR": [32, 39], "SAU": [45, 24], "NLD": [5, 52], "SWE": [15, 62],
        "NOR": [8, 62], "POL": [20, 52], "BEL": [4, 51], "CHE": [8, 47],
        "SGP": [104, 1], "THA": [100, 14], "IDN": [118, -2], "MYS": [102, 4],
        "PHL": [122, 12], "VNM": [106, 16], "EGY": [30, 27], "NGA": [8, 10],
        "COL": [-74, 4], "PER": [-76, -10], "CHL": [-71, -33],
    }
    coords = _COUNTRY_COORDS.get(highlight_iso, [0, -20])
    init_lon = -coords[0]
    init_lat = -coords[1]

    st.components.v1.html(
        """<!DOCTYPE html><html><head>
<style>
html,body{margin:0;padding:0;background:#0a1628;overflow:hidden;font-family:'DM Sans',sans-serif;}
#cglobe-root{width:100%;height:480px;position:relative;background:#0a1628;}
#cglobe-tooltip{position:absolute;display:none;background:rgba(10,14,26,0.95);border:1px solid rgba(249,115,22,0.4);color:#e6edf3;padding:10px 14px;border-radius:8px;font-size:13px;pointer-events:none;z-index:100;max-width:220px;}
</style></head><body>
<div id="cglobe-root"><div id="cglobe-tooltip"></div></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"></script>
<script>
var adData=""" + json.dumps(ad_gdp_data) + """;
var num2a=""" + json.dumps(_NUM_ISO_MAP) + """;
var names=""" + json.dumps({v: k for k, v in _NAME_TO_ISO.items() if v in ad_gdp_data}) + """;
var hlISO='""" + highlight_iso + """';
var root=document.getElementById('cglobe-root');
var tooltip=document.getElementById('cglobe-tooltip');
var W=root.clientWidth||800,H=480;
var svg=d3.select('#cglobe-root').append('svg').attr('width',W).attr('height',H)
  .style('position','absolute').style('top','0').style('left','0');
var proj=d3.geoOrthographic().scale(Math.min(W,H)*0.42).translate([W/2,H/2])
  .clipAngle(90).rotate([""" + str(init_lon) + """,""" + str(init_lat) + """]);
var path=d3.geoPath().projection(proj);
svg.append('circle').attr('cx',W/2).attr('cy',H/2).attr('r',proj.scale())
  .attr('fill','#0d1f35').attr('stroke','rgba(249,115,22,0.2)').attr('stroke-width',1);
var g=svg.append('g');
function clr(v,iso){
  if(iso===hlISO)return '#f97316';
  if(!v&&v!==0)return '#1a2744';
  var t=Math.min(v/1.5,1);
  if(t<0.33)return d3.interpolateRgb('#0d2847','#1a5fb4')(t/0.33);
  if(t<0.66)return d3.interpolateRgb('#1a5fb4','#3b82f6')((t-0.33)/0.33);
  return d3.interpolateRgb('#3b82f6','#94a3b8')((t-0.66)/0.34);
}
fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r=>r.json()).then(function(world){
  var countries=topojson.feature(world,world.objects.countries).features;
  g.selectAll('path').data(countries).enter().append('path')
    .attr('d',path)
    .attr('fill',d=>{var a=num2a[String(d.id)]||'';return clr(adData[a],a);})
    .attr('stroke',d=>{var a=num2a[String(d.id)]||'';return a===hlISO?'#fff':'rgba(255,255,255,0.08)';})
    .attr('stroke-width',d=>{var a=num2a[String(d.id)]||'';return a===hlISO?2:0.3;})
    .style('cursor','pointer')
    .on('mousemove',function(ev,d){
      var a=num2a[String(d.id)]||'';var n=names[a]||'';
      if(!n){tooltip.style.display='none';return;}
      var v=adData[a];
      tooltip.style.display='block';
      tooltip.style.left=(ev.offsetX+12)+'px';tooltip.style.top=(ev.offsetY-10)+'px';
      var h='<strong>'+n+'</strong>';
      if(v!==undefined)h+='<br>Ad/GDP: <strong style="color:#f97316">'+v.toFixed(2)+'%</strong>';
      if(a===hlISO)h+='<br><span style="color:#4aaeff;font-size:11px;">Selected country</span>';
      tooltip.innerHTML=h;
    })
    .on('mouseleave',function(){tooltip.style.display='none';});
});
</script></body></html>""",
        height=500,
        scrolling=False,
    )


# ── Main page ─────────────────────────────────────────────────────────────────
country = st.session_state.get("deep_dive_country", "")

# Resolve workbook
try:
    from utils.workbook_source import resolve_financial_data_xlsx
    excel_path = resolve_financial_data_xlsx()
except Exception:
    excel_path = None

if not excel_path:
    st.error("Data workbook not available.")
    st.stop()

import os
_mtime = os.path.getmtime(str(excel_path)) if excel_path else 0
df_totals = _load_country_totals(str(excel_path), _mtime)
df_channels = _load_country_channels(str(excel_path), _mtime)

# Normalize column names
_tcol = _find_col(df_totals, ["Country"]) or "Country"
_ycol = _find_col(df_totals, ["Year"]) or "Year"

if df_totals.empty:
    st.warning("Country data unavailable.")
    st.stop()

# Country picker if none selected
all_countries = sorted(df_totals[_tcol].dropna().unique().tolist()) if not df_totals.empty else []
if not country or country not in all_countries:
    st.markdown("## Select a country")
    country = st.selectbox("Country", all_countries, key="country_dd_select")
    if not country:
        st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
col_back, col_title = st.columns([1, 6])
with col_back:
    if st.button("← Overview", key="back_to_overview"):
        st.switch_page("pages/00_Overview.py")
with col_title:
    st.markdown(f"<h1 style='color:#f1f5f9;margin:0;font-size:2.5rem;font-weight:800'>{country}</h1>", unsafe_allow_html=True)
    region_label = _get_region(country)
    if region_label:
        st.markdown(f"<div style='color:#94a3b8;font-size:0.9rem;margin-top:-8px'>{region_label}</div>", unsafe_allow_html=True)

# ── Filter data ───────────────────────────────────────────────────────────────
df_country = df_totals[df_totals[_tcol] == country].copy()
df_country[_ycol] = pd.to_numeric(df_country[_ycol], errors="coerce")
df_country = df_country.dropna(subset=[_ycol]).sort_values(_ycol)
df_country[_ycol] = df_country[_ycol].astype(int)

# Find value columns
_ad_gdp_col = _find_col(df_country, ["Ad_vs_GDP", "ad_gdp", "Ad_Spend_pct"])
_ad_usd_col = _find_col(df_country, ["AdSpending_USD", "Ad_Spend_USD", "AdSpending_Total"])
_gdp_col = _find_col(df_country, ["GDP_USD", "GDP"])

if df_country.empty:
    st.warning(f"No data available for {country}.")
    st.stop()

# ── KPI strip ─────────────────────────────────────────────────────────────────
latest_year = int(df_country[_ycol].max())
prior_year = latest_year - 1
latest = df_country[df_country[_ycol] == latest_year].iloc[0] if not df_country[df_country[_ycol] == latest_year].empty else None
prior = df_country[df_country[_ycol] == prior_year].iloc[0] if not df_country[df_country[_ycol] == prior_year].empty else None

def _safe_float(row, col):
    if row is None or col is None:
        return None
    v = row.get(col) if hasattr(row, "get") else getattr(row, col, None) if hasattr(row, col) else None
    try:
        return float(v) if v is not None and not pd.isna(v) else None
    except (ValueError, TypeError):
        return None

def _delta(curr, prev):
    if curr is not None and prev is not None and prev != 0:
        return f"{((curr - prev) / abs(prev)) * 100:+.1f}%"
    return None

kpi_cols = st.columns(4)
_lat_ad_gdp = _safe_float(latest, _ad_gdp_col)
_pri_ad_gdp = _safe_float(prior, _ad_gdp_col)
_lat_ad_usd = _safe_float(latest, _ad_usd_col)
_pri_ad_usd = _safe_float(prior, _ad_usd_col)
_lat_gdp = _safe_float(latest, _gdp_col)

with kpi_cols[0]:
    val = f"${_lat_ad_usd / 1e9:.1f}B" if _lat_ad_usd and _lat_ad_usd > 1e6 else (f"${_lat_ad_usd:.0f}M" if _lat_ad_usd else "—")
    st.metric("Ad Spend", val, delta=_delta(_lat_ad_usd, _pri_ad_usd))
with kpi_cols[1]:
    val = f"{_lat_ad_gdp:.2f}%" if _lat_ad_gdp else "—"
    delta = f"{_lat_ad_gdp - _pri_ad_gdp:+.2f}pp" if _lat_ad_gdp and _pri_ad_gdp else None
    st.metric("Ad/GDP %", val, delta=delta)
with kpi_cols[2]:
    val = f"${_lat_gdp / 1e9:.0f}B" if _lat_gdp and _lat_gdp > 1e6 else ("—")
    st.metric("GDP", val)
with kpi_cols[3]:
    yoy_val = _delta(_lat_ad_usd, _pri_ad_usd) if _lat_ad_usd and _pri_ad_usd else "—"
    st.metric("YoY Ad Growth", yoy_val)

st.markdown(f"<div style='color:#64748b;font-size:0.75rem;margin-bottom:16px;'>Latest data: {latest_year}</div>", unsafe_allow_html=True)

# ── Globe + Filters ───────────────────────────────────────────────────────────
col_globe, col_filters = st.columns([3, 1])

with col_filters:
    st.markdown("### Filters")
    metric_mode = st.radio(
        "Metric",
        ["Ad/GDP %", "Ad Spend ($B)"],
        key="country_metric_mode",
    )
    all_years = sorted(df_country[_ycol].unique().tolist())
    if len(all_years) >= 2:
        year_range = st.select_slider(
            "Year range",
            options=all_years,
            value=(min(all_years), max(all_years)),
            key="country_year_range",
        )
    else:
        year_range = (min(all_years), max(all_years))

    # Media channel filter
    if not df_channels.empty:
        _ch_country_col = _find_col(df_channels, ["Country"])
        _ch_metric_col = _find_col(df_channels, ["Metric_type"])
        if _ch_country_col and _ch_metric_col:
            _ch_df = df_channels[df_channels[_ch_country_col] == country]
            all_ch = sorted(_ch_df[_ch_metric_col].dropna().unique().tolist()) if not _ch_df.empty else []
            selected_channels = st.multiselect(
                "Media channels",
                all_ch,
                default=all_ch[:6] if len(all_ch) > 6 else all_ch,
                key="country_channels",
            )
        else:
            selected_channels = []
    else:
        selected_channels = []

with col_globe:
    _metric_for_globe = _ad_gdp_col if metric_mode == "Ad/GDP %" else (_ad_usd_col or _ad_gdp_col)
    _render_country_globe(country, df_totals, _metric_for_globe or "Ad_vs_GDP_%", latest_year)

# ── Media Distribution Bar ────────────────────────────────────────────────────
if not df_channels.empty and selected_channels:
    _ch_country_col = _find_col(df_channels, ["Country"])
    _ch_year_col = _find_col(df_channels, ["Year"])
    _ch_metric_col = _find_col(df_channels, ["Metric_type"])
    _ch_value_col = _find_col(df_channels, ["Value"])
    if _ch_country_col and _ch_year_col and _ch_metric_col and _ch_value_col:
        _ch_latest = df_channels[
            (df_channels[_ch_country_col] == country)
            & (df_channels[_ch_year_col] == latest_year)
            & (df_channels[_ch_metric_col].isin(selected_channels))
        ].copy()
        _ch_latest[_ch_value_col] = pd.to_numeric(_ch_latest[_ch_value_col], errors="coerce")
        if not _ch_latest.empty:
            st.markdown("### Media Channel Distribution")
            fig_bar = go.Figure()
            _ch_agg = _ch_latest.groupby(_ch_metric_col)[_ch_value_col].sum().sort_values(ascending=True)
            _colors = ["#3b82f6", "#f97316", "#22c55e", "#a855f7", "#ef4444", "#06b6d4", "#f59e0b", "#ec4899"]
            for i, (ch, val) in enumerate(_ch_agg.items()):
                fig_bar.add_trace(go.Bar(
                    y=["Distribution"], x=[val], name=str(ch), orientation="h",
                    marker_color=_colors[i % len(_colors)],
                    text=f"{ch}: ${val:.0f}M" if val > 0 else "",
                    textposition="inside", textfont=dict(color="white", size=11),
                ))
            fig_bar.update_layout(barmode="stack")
            _dark_layout(fig_bar, f"Ad Spend by Channel — {country} ({latest_year})")
            fig_bar.update_layout(height=120, showlegend=True, legend=dict(orientation="h", y=-0.3))
            st.plotly_chart(fig_bar, use_container_width=True, config={"displayModeBar": False})

# ── Trend Charts ──────────────────────────────────────────────────────────────
df_range = df_country[(df_country[_ycol] >= year_range[0]) & (df_country[_ycol] <= year_range[1])].copy()

if not df_range.empty:
    st.markdown("### Trends")
    col_trend1, col_trend2 = st.columns(2)

    with col_trend1:
        if _ad_gdp_col:
            df_range[_ad_gdp_col] = pd.to_numeric(df_range[_ad_gdp_col], errors="coerce")
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=df_range[_ycol], y=df_range[_ad_gdp_col],
                mode="lines+markers", name=country,
                line=dict(color="#f97316", width=3),
                marker=dict(size=7, color="#f97316"),
            ))
            # Global average reference
            if _ad_gdp_col in df_totals.columns:
                global_avg = df_totals.groupby(_ycol)[_ad_gdp_col].mean().reset_index()
                global_avg = global_avg[(global_avg[_ycol] >= year_range[0]) & (global_avg[_ycol] <= year_range[1])]
                if not global_avg.empty:
                    fig_line.add_trace(go.Scatter(
                        x=global_avg[_ycol], y=global_avg[_ad_gdp_col],
                        mode="lines", name="Global Avg",
                        line=dict(color="#64748b", width=1.5, dash="dot"),
                    ))
            _dark_layout(fig_line, "Ad/GDP % Trend")
            fig_line.update_layout(height=350)
            st.plotly_chart(fig_line, use_container_width=True, config={"displayModeBar": False})

    with col_trend2:
        if _ad_usd_col:
            df_range[_ad_usd_col] = pd.to_numeric(df_range[_ad_usd_col], errors="coerce")
            _ad_b = df_range[_ad_usd_col] / 1e9 if df_range[_ad_usd_col].max() > 1e6 else df_range[_ad_usd_col]
            fig_bars = go.Figure()
            fig_bars.add_trace(go.Bar(
                x=df_range[_ycol], y=_ad_b,
                marker_color="#3b82f6",
                text=[f"${v:.1f}B" for v in _ad_b], textposition="outside",
                textfont=dict(color="#94a3b8", size=10),
            ))
            _dark_layout(fig_bars, "Ad Spend ($B)")
            fig_bars.update_layout(height=350)
            st.plotly_chart(fig_bars, use_container_width=True, config={"displayModeBar": False})

# ── Data Table ────────────────────────────────────────────────────────────────
if not df_range.empty:
    st.markdown("### Historical Data")
    display_cols = [_ycol]
    rename_map = {_ycol: "Year"}
    if _ad_usd_col:
        display_cols.append(_ad_usd_col)
        rename_map[_ad_usd_col] = "Ad Spend ($)"
    if _ad_gdp_col:
        display_cols.append(_ad_gdp_col)
        rename_map[_ad_gdp_col] = "Ad/GDP %"
    if _gdp_col:
        display_cols.append(_gdp_col)
        rename_map[_gdp_col] = "GDP ($)"

    _display = df_range[display_cols].rename(columns=rename_map).sort_values("Year", ascending=False)
    st.dataframe(_display, use_container_width=True, hide_index=True)
