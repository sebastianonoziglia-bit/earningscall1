#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import numpy as np
import plotly.express as px
import plotly.graph_objects as go


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "reports" / "dev_manual_assets"


def _write_pipeline_dependency_graph() -> Path:
    nodes = [
        ("Google Sheet (43 tabs)", "source", 0.02, 0.80),
        ("Local transcripts folder", "source", 0.02, 0.25),
        ("app/utils/workbook_source.py", "connector", 0.20, 0.80),
        ("app/utils/workbook_market_data.py", "processor", 0.40, 0.88),
        ("app/data_processor.py", "processor", 0.40, 0.72),
        ("app/stock_processor_fix.py", "processor", 0.40, 0.58),
        ("app/pages/Welcome.py", "ui", 0.72, 0.93),
        ("app/pages/00_Overview.py", "ui", 0.72, 0.85),
        ("app/pages/01_Earnings.py", "ui", 0.72, 0.70),
        ("app/pages/04_Genie.py", "ui", 0.72, 0.20),
        ("scripts/rebuild_transcript_index.py", "script", 0.24, 0.33),
        ("scripts/extract_transcript_topics.py", "script", 0.40, 0.33),
        ("scripts/extract_transcript_highlights_from_sheet.py", "script", 0.40, 0.20),
        ("scripts/build_intelligence_db.py", "script", 0.56, 0.28),
        ("scripts/sync_gsheet_to_sql.py", "script", 0.56, 0.12),
        ("SQLite: earningscall_intelligence.db", "storage", 0.72, 0.10),
        ("scripts/generate_insights.py", "script", 0.56, 0.43),
        ("generated_insights_latest.csv", "storage", 0.72, 0.43),
    ]
    edges = [
        ("Google Sheet (43 tabs)", "app/utils/workbook_source.py"),
        ("app/utils/workbook_source.py", "app/utils/workbook_market_data.py"),
        ("app/utils/workbook_source.py", "app/data_processor.py"),
        ("app/utils/workbook_source.py", "app/stock_processor_fix.py"),
        ("app/utils/workbook_market_data.py", "app/stock_processor_fix.py"),
        ("app/data_processor.py", "app/pages/Welcome.py"),
        ("app/data_processor.py", "app/pages/00_Overview.py"),
        ("app/data_processor.py", "app/pages/01_Earnings.py"),
        ("app/data_processor.py", "app/pages/04_Genie.py"),
        ("app/stock_processor_fix.py", "app/pages/01_Earnings.py"),
        ("Local transcripts folder", "scripts/rebuild_transcript_index.py"),
        ("scripts/rebuild_transcript_index.py", "scripts/extract_transcript_topics.py"),
        ("scripts/rebuild_transcript_index.py", "scripts/extract_transcript_highlights_from_sheet.py"),
        ("scripts/extract_transcript_topics.py", "scripts/build_intelligence_db.py"),
        ("scripts/extract_transcript_highlights_from_sheet.py", "scripts/build_intelligence_db.py"),
        ("scripts/build_intelligence_db.py", "SQLite: earningscall_intelligence.db"),
        ("scripts/sync_gsheet_to_sql.py", "SQLite: earningscall_intelligence.db"),
        ("scripts/extract_transcript_topics.py", "scripts/generate_insights.py"),
        ("scripts/generate_insights.py", "generated_insights_latest.csv"),
        ("generated_insights_latest.csv", "app/pages/00_Overview.py"),
        ("generated_insights_latest.csv", "app/pages/Welcome.py"),
        ("SQLite: earningscall_intelligence.db", "app/pages/04_Genie.py"),
    ]
    coords = {name: (x, y) for name, _, x, y in nodes}
    colors = {
        "source": "#1f77b4",
        "connector": "#2ca02c",
        "processor": "#ff7f0e",
        "ui": "#9467bd",
        "script": "#17becf",
        "storage": "#d62728",
    }

    fig = go.Figure()
    for src, dst in edges:
        x0, y0 = coords[src]
        x1, y1 = coords[dst]
        fig.add_trace(
            go.Scatter(
                x=[x0, x1],
                y=[y0, y1],
                mode="lines",
                line=dict(color="rgba(120,120,120,0.5)", width=1.5),
                showlegend=False,
                hoverinfo="skip",
            )
        )
    for typ in ["source", "connector", "processor", "script", "storage", "ui"]:
        xs, ys, labels = [], [], []
        for name, node_type, x, y in nodes:
            if node_type == typ:
                xs.append(x)
                ys.append(y)
                labels.append(name)
        fig.add_trace(
            go.Scatter(
                x=xs,
                y=ys,
                mode="markers+text",
                text=labels,
                textposition="top center",
                marker=dict(size=18, color=colors[typ], line=dict(width=1, color="white")),
                name=typ.title(),
            )
        )
    fig.update_layout(
        title="Insights Pipeline Dependency Graph (Current-State)",
        width=1800,
        height=900,
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
    )
    out = OUT_DIR / "pipeline_dependency_graph.png"
    fig.write_image(str(out))
    return out


def _write_sheet_usage_heatmap() -> Path:
    sheets = [
        "Daily",
        "Minute",
        "Holders",
        "USD Inflation",
        "Nasdaq Composite Est. (FRED)",
        "Country_Totals_vs_GDP",
        "Country_Totals_vs_GDP_ RAW",
        "Country_Advertising_Data_FullVi",
        "Country_avg_timespent_intrnt24",
        "Global_Adv_Aggregates",
        "Global Advertising (GroupM)",
        " (GroupM) Granular ",
        "M2_values",
        "Company_metrics_earnings_values",
        "Company_Employees",
        "Company_Segments_insights_text",
        "Company_insights_text",
        "Company_advertising_revenue",
        "Company_subscribers_values",
        "Hardware_Smartphone_Shipments",
        "Macro_Wealth_by_Generation",
        "Company_revenue_by_region",
        "Company_minute&dollar_earned",
        "Company_yearly_segments_values",
        "Company_Quarterly_segments_valu",
        "Alphabet Quarterly Segments",
        "Apple Quarterly Segments",
        "Amazon Quarterly Segments ",
        "Meta Quarterly Segments",
        "Comcast Quarterly Segments Gran",
        "Disney Quarterly Segments",
        "Microsoft Quarterly Segments",
        "Netflix Quarterly Segments",
        "Paramount Quarterly Segments",
        "Roku Quarterly Segments",
        "Spotify Quarterly Segments",
        "Warner Bros Quarterly Segments",
        "Overview_Macro",
        "Overview_Insights",
        "Overview_Charts",
        "Transcripts",
        "Overview_Auto_Insights",
        "Macro_KPIs",
    ]
    pages = ["Welcome", "00_Overview", "01_Earnings", "04_Genie"]
    usage = {s: [0.0, 0.0, 0.0, 0.0] for s in sheets}

    for s in [
        "Overview_Macro",
        "Overview_Insights",
        "Overview_Charts",
        "Overview_Auto_Insights",
        "Macro_KPIs",
        "Global_Adv_Aggregates",
        "Global Advertising (GroupM)",
        " (GroupM) Granular ",
        "M2_values",
        "USD Inflation",
        "Nasdaq Composite Est. (FRED)",
        "Macro_Wealth_by_Generation",
        "Country_Totals_vs_GDP",
        "Country_Totals_vs_GDP_ RAW",
        "Country_Advertising_Data_FullVi",
        "Country_avg_timespent_intrnt24",
        "Company_advertising_revenue",
        "Company_revenue_by_region",
    ]:
        usage[s] = [1.0, 1.0, 0.0, 0.0]
    for s in [
        "Company_metrics_earnings_values",
        "Company_Employees",
        "Company_Segments_insights_text",
        "Company_insights_text",
        "Company_subscribers_values",
        "Company_yearly_segments_values",
        "Company_Quarterly_segments_valu",
        "Alphabet Quarterly Segments",
        "Apple Quarterly Segments",
        "Amazon Quarterly Segments ",
        "Meta Quarterly Segments",
        "Comcast Quarterly Segments Gran",
        "Disney Quarterly Segments",
        "Microsoft Quarterly Segments",
        "Netflix Quarterly Segments",
        "Paramount Quarterly Segments",
        "Roku Quarterly Segments",
        "Spotify Quarterly Segments",
        "Warner Bros Quarterly Segments",
        "Company_minute&dollar_earned",
        "Hardware_Smartphone_Shipments",
    ]:
        usage[s] = [0.3, 0.2, 1.0, 0.0]
    usage["Company_metrics_earnings_values"] = [1.0, 1.0, 1.0, 0.2]
    usage["Daily"] = [0.4, 0.0, 1.0, 0.0]
    usage["Minute"] = [0.2, 0.0, 1.0, 0.0]
    usage["Holders"] = [0.1, 0.0, 0.6, 0.0]
    usage["Transcripts"] = [0.7, 0.5, 0.3, 1.0]

    z = np.array([usage[s] for s in sheets])
    fig = go.Figure(data=go.Heatmap(z=z, x=pages, y=sheets, colorscale="YlGnBu", zmin=0, zmax=1))
    fig.update_layout(
        title="Sheet Usage Heatmap (43 sheets x active insight pages)",
        width=1500,
        height=1800,
        margin=dict(l=360, r=40, t=80, b=40),
    )
    out = OUT_DIR / "sheet_usage_heatmap.png"
    fig.write_image(str(out))
    return out


def _write_migration_impact() -> Path:
    files = [
        "app/utils/workbook_source.py",
        "app/utils/live_stock_feed.py",
        "app/utils/workbook_market_data.py",
        "app/stock_processor_fix.py",
        "app/data_processor.py",
        "app/pages/00_Overview.py",
        "app/pages/01_Earnings.py",
        "app/Welcome.py",
    ]
    impact = [5, 4, 4, 3, 3, 4, 4, 5]
    change_class = [
        "Source default + validation",
        "Connector deprecation",
        "Daily/Minute/Holders integration",
        "Live merge replacement",
        "Processor loading updates",
        "Overview UI architecture",
        "Earnings UI stock-source updates",
        "Home scrollytelling + ticker",
    ]
    fig = px.bar(
        x=impact,
        y=files,
        color=change_class,
        orientation="h",
        title="Google Sheet Migration Impact by File (Current-State)",
    )
    fig.update_layout(width=1600, height=820, yaxis={"categoryorder": "total ascending"})
    out = OUT_DIR / "migration_impact.png"
    fig.write_image(str(out))
    return out


def _write_risk_distribution() -> Path:
    categories = [
        "Stale cache/data",
        "Missing/renamed sheet columns",
        "Duplicate insight logic",
        "Legacy schema refs",
        "Silent pipeline failures",
    ]
    scores = [8, 7, 6, 6, 9]
    fig = go.Figure(data=[go.Bar(x=categories, y=scores, text=scores, textposition="outside")])
    fig.update_layout(
        title="Risk Distribution (Insights Pipeline)",
        width=1400,
        height=700,
        yaxis_title="Risk score (1-10)",
    )
    out = OUT_DIR / "risk_distribution.png"
    fig.write_image(str(out))
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = [
        _write_pipeline_dependency_graph(),
        _write_sheet_usage_heatmap(),
        _write_migration_impact(),
        _write_risk_distribution(),
    ]
    for path in generated:
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
