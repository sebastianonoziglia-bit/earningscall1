#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EXCEL_PATH="$REPO_ROOT/app/attached_assets/Earnings + stocks  copy.xlsx"
GSHEET_URL="${GOOGLE_SHEET_URL}"
if [ -z "$GSHEET_URL" ]; then
    echo "ERROR: Set GOOGLE_SHEET_URL first"
    exit 1
fi
echo "Downloading from Google Sheets..."
curl -L "$GSHEET_URL" -o "$EXCEL_PATH" --progress-bar
echo "Committing and pushing..."
cd "$REPO_ROOT"
git add "$EXCEL_PATH"
git commit -m "data: sync Excel from Google Sheets $(date '+%Y-%m-%d %H:%M')"
git lfs push hf main --all 2>/dev/null
git push hf main
echo "Done. HuggingFace rebuilds in ~60s."
