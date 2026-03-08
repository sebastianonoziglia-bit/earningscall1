#!/usr/bin/env python3
from __future__ import annotations

import ast
import datetime as dt
import json
import os
import re
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "reports" / "Developer_Insights_Bible_CURRENT.md"

KEY_FILES = [
    "app.py",
    "app/Welcome.py",
    "app/pages/00_Overview.py",
    "app/pages/01_Earnings.py",
    "app/pages/02_Stocks.py",
    "app/pages/03_Editorial.py",
    "app/pages/04_Genie.py",
    "app/data_processor.py",
    "app/stock_processor_fix.py",
    "app/utils/workbook_source.py",
    "app/utils/workbook_market_data.py",
    "app/utils/transcript_startup_sync.py",
    "scripts/rebuild_transcript_index.py",
    "scripts/extract_transcript_topics.py",
    "scripts/extract_transcript_highlights_from_sheet.py",
    "scripts/extract_kpi_values.py",
    "scripts/build_intelligence_db.py",
    "scripts/sync_gsheet_to_sql.py",
    "scripts/generate_insights.py",
    "scripts/sync_all_intelligence.py",
]


@dataclass
class FnInfo:
    name: str
    lineno: int
    end_lineno: int
    refs: int
    calls: int



def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")



def _git(cmd: list[str], default: str = "") -> str:
    try:
        out = subprocess.check_output(cmd, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
        return out or default
    except Exception:
        return default



def _find_function_inventory(path: Path) -> list[FnInfo]:
    src = _read(path)
    try:
        mod = ast.parse(src)
    except Exception:
        return []
    call_counts: dict[str, int] = defaultdict(int)
    for node in ast.walk(mod):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        if isinstance(fn, ast.Name):
            call_counts[fn.id] += 1
    out: list[FnInfo] = []
    for node in mod.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            refs = len(re.findall(r"\b" + re.escape(node.name) + r"\b", src))
            calls = call_counts.get(node.name, 0)
            out.append(FnInfo(node.name, node.lineno, getattr(node, "end_lineno", node.lineno), refs, calls))
    return out



def _count_pattern(path: Path, token: str) -> int:
    return _read(path).count(token)



def _extract_welcome_beats(path: Path) -> list[tuple[str, str, int]]:
    src = _read(path)
    try:
        mod = ast.parse(src)
    except Exception:
        return []
    out: list[tuple[str, str, int]] = []
    for node in ast.walk(mod):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "_section":
            continue
        if len(node.args) < 2:
            continue
        a0, a1 = node.args[0], node.args[1]
        if not isinstance(a0, ast.Constant) or not isinstance(a1, ast.Constant):
            continue
        if not isinstance(a0.value, str) or not isinstance(a1.value, str):
            continue
        out.append((a0.value, a1.value, getattr(node, "lineno", 0)))
    out.sort(key=lambda x: x[2])
    return out



def _extract_overview_areas(path: Path) -> list[dict]:
    src = _read(path)
    m = re.search(r"_OVERVIEW_AREA_CONFIG\s*=\s*\[(.*?)\]\n\n", src, flags=re.DOTALL)
    if not m:
        return []
    blob = m.group(1)
    areas: list[dict] = []
    for block in re.finditer(r"\{(.*?)\}", blob, flags=re.DOTALL):
        text = block.group(1)
        key = re.search(r'"key"\s*:\s*"([^"]+)"', text)
        title = re.search(r'"title"\s*:\s*"([^"]+)"', text)
        desc = re.search(r'"description"\s*:\s*"([^"]+)"', text)
        if key and title:
            areas.append({
                "key": key.group(1),
                "title": title.group(1),
                "description": desc.group(1) if desc else "",
            })
    return areas



def _extract_sync_steps(path: Path) -> list[str]:
    steps: list[str] = []
    for line in _read(path).splitlines():
        if "run_step(" in line and "scripts/" in line:
            steps.append(line.strip())
    return steps



def _section(title: str) -> list[str]:
    return [f"## {title}", ""]



def _md_table(headers: list[str], rows: Iterable[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    return lines



def build() -> str:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    sha = _git(["git", "rev-parse", "--short", "HEAD"], "unknown")
    branch = _git(["git", "branch", "--show-current"], "unknown")

    lines: list[str] = []
    lines.append("# Developer Insights Bible (Current-State Snapshot)")
    lines.append("")
    lines.append(f"Generated: {now}")
    lines.append(f"Git: `{branch}` @ `{sha}`")
    lines.append(f"Repository: `{ROOT}`")
    lines.append("")
    lines.append("This is a current-state technical map of what exists in code now.")
    lines.append("")

    lines += _section("1) Runtime Entry + Navigation")
    lines.append("- Entrypoint: `app.py` inserts `app` to `sys.path` then executes `app/Welcome.py`.")
    lines.append("- Streamlit pages currently discovered from `app/pages/` (no root `pages/` directory).")
    lines.append("- Theme config exists in both `.streamlit/config.toml` and `app/.streamlit/config.toml`.")
    lines.append("- Note: `ui.hideSidebarNav` key is present but invalid for Streamlit 1.31; client-side sidebar navigation setting should be used instead.")
    lines.append("")
    page_rows = []
    for p in sorted((ROOT / "app" / "pages").glob("*.py")):
        page_rows.append([f"`{p.relative_to(ROOT)}`"])
    lines += _md_table(["Discovered Page File"], page_rows)

    lines += _section("2) Home Page (Welcome.py) — Exact Render Order")
    beats = _extract_welcome_beats(ROOT / "app" / "Welcome.py")
    pre_items = [
        "Hero + KPI strip + dynamic narrative",
        "World choropleth map",
        "Structural shift donut (`wm-ss-root` HTML component)",
        "Attention + duopoly animated scene (`wm-attn-root` HTML component)",
        "Concentration HTML block (`wm-conc-root`)",
        "Revenue anatomy HTML block (`wm-rev-root`)",
    ]
    for i, item in enumerate(pre_items, start=1):
        lines.append(f"{i}. {item}")
    offset = len(pre_items)
    for i, (label, headline, ln) in enumerate(beats, start=1 + offset):
        lines.append(f"{i}. `{label}` — {headline} (`app/Welcome.py:{ln}`)")
    lines.append(f"{len(pre_items)+len(beats)+1}. Gateway section (Overview / Earnings / Genie)")
    lines.append("")

    lines += _section("3) Overview Page Architecture (00_Overview.py)")
    areas = _extract_overview_areas(ROOT / "app" / "pages" / "00_Overview.py")
    lines.append("Current navigator model is 8-section single-view mode with per-section `st.stop()` exits.")
    lines.append("")
    area_rows = []
    for a in areas:
        area_rows.append([f"`{a['key']}`", a["title"], a["description"]])
    lines += _md_table(["Key", "Title", "Description"], area_rows)

    lines += _section("4) Transcript Intelligence Pipeline (Current)")
    steps = _extract_sync_steps(ROOT / "scripts" / "sync_all_intelligence.py")
    lines.append("Pipeline orchestrator: `scripts/sync_all_intelligence.py`")
    lines.append("")
    for s in steps:
        lines.append(f"- `{s}`")
    lines.append("")
    lines.append("Core artifacts in `earningscall_transcripts/`:")
    for csv_name in [
        "transcript_index.csv",
        "transcript_topics.csv",
        "transcript_kpis.csv",
        "transcript_highlights.csv",
        "overview_iconic_quotes.csv",
        "topic_metrics.csv",
        "generated_insights_latest.csv",
    ]:
        exists = (ROOT / "earningscall_transcripts" / csv_name).exists()
        lines.append(f"- `{csv_name}`: {'present' if exists else 'missing'}")
    lines.append("")

    lines += _section("5) Function Inventory (Key Files)")
    inv_rows = []
    for rel in KEY_FILES:
        p = ROOT / rel
        if not p.exists():
            inv_rows.append([f"`{rel}`", "missing", "0", "0"])
            continue
        funcs = _find_function_inventory(p)
        src = _read(p)
        classes = len(re.findall(r"^class\\s+", src, flags=re.MULTILINE))
        inv_rows.append([f"`{rel}`", str(len(src.splitlines())), str(len(funcs)), str(classes)])
    lines += _md_table(["File", "Lines", "Functions", "Classes"], inv_rows)

    lines += _section("6) Welcome.py Active vs Legacy Helper Functions")
    welcome_funcs = _find_function_inventory(ROOT / "app" / "Welcome.py")
    active_rows = []
    legacy_rows = []
    for fn in welcome_funcs:
        row = [f"`{fn.name}`", f"`{fn.lineno}`", str(fn.calls), str(fn.refs)]
        if fn.calls == 0:
            legacy_rows.append(row)
        else:
            active_rows.append(row)
    lines.append("Active (called at least once):")
    lines += _md_table(["Function", "Line", "Call Count", "Name Reference Count"], active_rows)
    lines.append("Likely legacy/not currently wired (call count = 0):")
    lines += _md_table(["Function", "Line", "Call Count", "Name Reference Count"], legacy_rows)

    lines += _section("7) Chart + Component Footprint")
    chart_rows = []
    for rel in ["app/Welcome.py", "app/pages/00_Overview.py", "app/pages/01_Earnings.py", "app/pages/04_Genie.py"]:
        p = ROOT / rel
        if not p.exists():
            continue
        chart_rows.append([
            f"`{rel}`",
            str(_count_pattern(p, "st.plotly_chart(")),
            str(_count_pattern(p, "st.components.v1.html(")),
            str(_count_pattern(p, "@st.cache_data")),
            str(_count_pattern(p, "st.stop(")),
        ])
    lines += _md_table(["File", "Plotly Calls", "HTML Components", "@st.cache_data", "st.stop Calls"], chart_rows)

    lines += _section("8) Data Source Contracts")
    lines.append("Workbook resolver hard checks (current):")
    lines.append("- Minimum sheet count >= 43")
    lines.append("- Required tabs include `Company_metrics_earnings_values`, `Daily`, `Minute`, `Holders`")
    lines.append("- Core financial coverage gate: >=5 companies, >=8 years, earliest year <= 2015")
    lines.append("")

    lines += _section("9) Known Operational Risks (Current-State)")
    risks = [
        ("Sidebar ghost menu", "Config uses invalid key `ui.hideSidebarNav` for current Streamlit version."),
        ("Overview complexity", "`00_Overview.py` is >10k lines with many branch stops; hard to reason and easy to regress."),
        ("Home render weight", "Welcome stacks many Plotly + HTML components; can feel slow on cold starts."),
        ("Unused helper drift", "Welcome contains legacy helper loaders not currently rendered."),
        ("NLP enrichment gap", "No `scripts/enrich_transcript_intelligence.py` yet; enrichment layer still pending."),
    ]
    for r, d in risks:
        lines.append(f"- **{r}**: {d}")
    lines.append("")

    lines += _section("10) Regeneration Workflow")
    lines.append("To regenerate this snapshot after code changes:")
    lines.append("1. `python3 scripts/generate_current_bible.py`")
    lines.append("2. Review `reports/Developer_Insights_Bible_CURRENT.md`")
    lines.append("3. (Optional) export PDF from markdown if needed")
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(build(), encoding="utf-8")
    print(f"Wrote {OUT}")
