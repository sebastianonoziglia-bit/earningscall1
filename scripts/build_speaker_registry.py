#!/usr/bin/env python3
"""
Build Company_Speakers sheet in the Excel workbook by scanning
all transcripts and extracting speaker names and roles.

Run: python3 scripts/build_speaker_registry.py

Writes Company_Speakers sheet to the local Excel file.
Copy that sheet to your Google Sheet for use online.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

from utils.transcript_live import write_speakers_to_excel
from utils.workbook_source import resolve_financial_data_xlsx


def main():
    excel_path = resolve_financial_data_xlsx()
    if not excel_path:
        print("ERROR: Could not find Excel workbook")
        sys.exit(1)

    print(f"Scanning transcripts in: {excel_path}")
    n = write_speakers_to_excel(str(excel_path))
    print(f"Done. Wrote {n} speakers to Company_Speakers sheet.")
    print()
    print("Next steps:")
    print("1. Open the Excel file and review Company_Speakers sheet")
    print("2. Copy the sheet to your Google Sheet")
    print("3. The app will read it automatically on next load")


if __name__ == "__main__":
    main()
