#!/usr/bin/env python3
"""
Export all local earnings-call transcripts to a single CSV file
that can be imported into Google Sheets.

Usage:
    python scripts/export_transcripts_to_csv.py

Output:
    ~/Desktop/transcripts_for_gsheet.csv

Then import into Google Sheets:
    1. Open the Google Sheet
    2. File → Import → Upload → select transcripts_for_gsheet.csv
    3. Import location: "Insert new sheet(s)", Separator: Comma
    4. Rename the new tab to exactly: Transcripts
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

TRANSCRIPT_ROOT = Path(__file__).resolve().parents[1] / "earningscall_transcripts"
OUTPUT_FILE = Path.home() / "Desktop" / "transcripts_for_gsheet.csv"
MAX_CELL_CHARS = 50000  # Google Sheets cell limit

COMPANY_NAME_FIXES = {
    "Paramount_Global": "Paramount Global",
    "Warner_Bros_Discovery": "Warner Bros. Discovery",
}


def normalize_company(folder_name: str) -> str:
    return COMPANY_NAME_FIXES.get(folder_name, folder_name.replace("_", " ").strip())


def main() -> None:
    if not TRANSCRIPT_ROOT.exists():
        print(f"ERROR: Transcript folder not found: {TRANSCRIPT_ROOT}")
        return

    rows: list[list[str]] = []

    for company_dir in sorted(p for p in TRANSCRIPT_ROOT.iterdir() if p.is_dir()):
        company = normalize_company(company_dir.name)
        for year_dir in sorted(p for p in company_dir.iterdir() if p.is_dir() and p.name.isdigit()):
            year = year_dir.name
            for txt_file in sorted(year_dir.glob("Q[1-4].txt")):
                quarter = txt_file.stem.upper()
                text = txt_file.read_text(encoding="utf-8", errors="ignore").strip()
                if not text:
                    continue
                # File modification time as last_updated
                mtime = txt_file.stat().st_mtime
                last_updated = datetime.fromtimestamp(mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                # Truncate to Google Sheets cell limit
                if len(text) > MAX_CELL_CHARS:
                    text = text[:MAX_CELL_CHARS]
                    print(f"  TRUNCATED: {company} {year} {quarter} ({len(text)} chars)")
                rows.append([company, year, quarter, text, last_updated])

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["company", "year", "quarter", "transcript_text", "last_updated"])
        writer.writerows(rows)

    print(f"Exported {len(rows)} transcripts to {OUTPUT_FILE}")
    print(f"File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")
    print()
    print("Next steps:")
    print("  1. Open your Google Sheet")
    print("  2. File → Import → Upload → select transcripts_for_gsheet.csv")
    print('  3. Import location: "Insert new sheet(s)", Separator: Comma')
    print("  4. Rename the new tab to exactly: Transcripts")


if __name__ == "__main__":
    main()
