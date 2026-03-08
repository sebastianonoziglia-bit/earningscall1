#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Iterable

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MD = ROOT / "reports" / "Developer_Insights_Bible_CURRENT.md"
DEFAULT_PDF = ROOT / "reports" / "Developer_Insights_Bible_CURRENT_Full.pdf"
ASSET_DIR = ROOT / "reports" / "dev_manual_assets"
ASSET_ORDER = [
    "pipeline_dependency_graph.png",
    "sheet_usage_heatmap.png",
    "migration_impact.png",
    "risk_distribution.png",
]


def _styles():
    base = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle(
            "H1",
            parent=base["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=20,
            leading=24,
            textColor=colors.HexColor("#0f172a"),
            spaceBefore=8,
            spaceAfter=10,
        ),
        "h2": ParagraphStyle(
            "H2",
            parent=base["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            leading=18,
            textColor=colors.HexColor("#1e293b"),
            spaceBefore=10,
            spaceAfter=6,
        ),
        "h3": ParagraphStyle(
            "H3",
            parent=base["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=colors.HexColor("#334155"),
            spaceBefore=8,
            spaceAfter=5,
        ),
        "body": ParagraphStyle(
            "Body",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=13,
            textColor=colors.HexColor("#111827"),
            spaceAfter=4,
        ),
        "bullet": ParagraphStyle(
            "Bullet",
            parent=base["BodyText"],
            fontName="Helvetica",
            fontSize=9.3,
            leading=12.8,
            leftIndent=12,
            bulletIndent=0,
            textColor=colors.HexColor("#111827"),
            spaceAfter=2,
        ),
        "code": ParagraphStyle(
            "Code",
            parent=base["Code"],
            fontName="Courier",
            fontSize=8.2,
            leading=10.2,
            backColor=colors.HexColor("#f8fafc"),
            borderColor=colors.HexColor("#e2e8f0"),
            borderWidth=0.4,
            borderPadding=5,
            spaceAfter=6,
        ),
        "small": ParagraphStyle(
            "Small",
            parent=base["BodyText"],
            fontName="Helvetica-Oblique",
            fontSize=8.2,
            leading=11,
            textColor=colors.HexColor("#475569"),
            spaceAfter=6,
        ),
    }


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\t", "    ")
    )


def _flush_paragraph(buffer: list[str], story: list, style: ParagraphStyle) -> None:
    if not buffer:
        return
    text = " ".join(x.strip() for x in buffer if x.strip()).strip()
    if text:
        story.append(Paragraph(_escape(text), style))
    buffer.clear()


def _parse_table(lines: list[str]) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in lines:
        row = line.strip()
        if not row.startswith("|"):
            continue
        cells = [c.strip() for c in row.strip("|").split("|")]
        rows.append(cells)
    if len(rows) >= 2 and all(set(c) <= {"-", ":"} for c in rows[1]):
        rows.pop(1)
    return rows


def _append_table(story: list, table_rows: list[list[str]], styles: dict[str, ParagraphStyle]) -> None:
    if not table_rows:
        return
    width = len(table_rows[0])
    normalized = []
    for row in table_rows:
        r = row + [""] * (width - len(row))
        normalized.append([Paragraph(_escape(cell), styles["body"]) for cell in r[:width]])
    tbl = Table(normalized, hAlign="LEFT", repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(tbl)
    story.append(Spacer(1, 0.22 * cm))


def _build_story(md_path: Path) -> list:
    styles = _styles()
    story: list = []
    story.append(
        Paragraph(
            "Developer Insights Bible — Full Current-State Export",
            styles["h1"],
        )
    )
    story.append(
        Paragraph(
            _escape(f"Generated on {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"),
            styles["small"],
        )
    )
    story.append(Spacer(1, 0.18 * cm))

    lines = md_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    para_buf: list[str] = []
    in_code = False
    code_buf: list[str] = []
    table_buf: list[str] = []

    def flush_table():
        nonlocal table_buf
        if table_buf:
            _flush_paragraph(para_buf, story, styles["body"])
            _append_table(story, _parse_table(table_buf), styles)
            table_buf = []

    for raw in lines:
        line = raw.rstrip("\n")

        if line.strip().startswith("```"):
            flush_table()
            _flush_paragraph(para_buf, story, styles["body"])
            if in_code:
                story.append(Preformatted("\n".join(code_buf), styles["code"]))
                story.append(Spacer(1, 0.12 * cm))
                code_buf = []
                in_code = False
            else:
                in_code = True
            continue

        if in_code:
            code_buf.append(line)
            continue

        if line.strip().startswith("|"):
            table_buf.append(line)
            continue
        else:
            flush_table()

        if not line.strip():
            _flush_paragraph(para_buf, story, styles["body"])
            continue

        if line.startswith("### "):
            _flush_paragraph(para_buf, story, styles["body"])
            story.append(Paragraph(_escape(line[4:].strip()), styles["h3"]))
            continue
        if line.startswith("## "):
            _flush_paragraph(para_buf, story, styles["body"])
            story.append(Paragraph(_escape(line[3:].strip()), styles["h2"]))
            continue
        if line.startswith("# "):
            _flush_paragraph(para_buf, story, styles["body"])
            story.append(Paragraph(_escape(line[2:].strip()), styles["h1"]))
            continue

        stripped = line.lstrip()
        if stripped.startswith("- "):
            _flush_paragraph(para_buf, story, styles["body"])
            story.append(Paragraph(_escape(stripped[2:]), styles["bullet"], bulletText="•"))
            continue

        # Numbered list support: "1. text"
        if len(stripped) > 3 and stripped[0].isdigit() and stripped[1:3] == ". ":
            _flush_paragraph(para_buf, story, styles["body"])
            story.append(Paragraph(_escape(stripped[3:]), styles["bullet"], bulletText=stripped[:1] + "."))
            continue

        para_buf.append(line)

    flush_table()
    _flush_paragraph(para_buf, story, styles["body"])
    if in_code and code_buf:
        story.append(Preformatted("\n".join(code_buf), styles["code"]))

    return story


def _append_assets(story: list, asset_dir: Path, names: Iterable[str]) -> None:
    styles = _styles()
    first = True
    for name in names:
        path = asset_dir / name
        if not path.exists():
            continue
        if first:
            story.append(PageBreak())
            story.append(Paragraph("Appendix — Visual Assets", styles["h1"]))
            first = False
        story.append(Paragraph(_escape(name), styles["h3"]))
        img = Image(str(path))
        max_w = A4[0] - 2.1 * cm
        max_h = A4[1] - 5.2 * cm
        scale = min(max_w / img.imageWidth, max_h / img.imageHeight, 1.0)
        img.drawWidth = img.imageWidth * scale
        img.drawHeight = img.imageHeight * scale
        story.append(img)
        story.append(Spacer(1, 0.22 * cm))


def build_pdf(md_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    story = _build_story(md_path)
    _append_assets(story, ASSET_DIR, ASSET_ORDER)
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=A4,
        leftMargin=1.4 * cm,
        rightMargin=1.4 * cm,
        topMargin=1.4 * cm,
        bottomMargin=1.4 * cm,
        title="Developer Insights Bible — Full Current-State Export",
    )
    doc.build(story)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate full PDF from current Bible markdown.")
    parser.add_argument("--md", default=str(DEFAULT_MD), help="Input markdown path")
    parser.add_argument("--out", default=str(DEFAULT_PDF), help="Output PDF path")
    args = parser.parse_args()

    md_path = Path(args.md).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    if not md_path.exists():
        raise SystemExit(f"Input markdown not found: {md_path}")

    build_pdf(md_path, out_path)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
