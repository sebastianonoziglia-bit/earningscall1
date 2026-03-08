# Codex Standing Rules — The Attention Economy

## PUSH RULES
1. Always commit to `main` branch only. Never commit to `codex/welcome-push` or any feature branch.
2. After every push to `hf main`, immediately run py_compile on ALL 3 pages:
   - PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile app/Welcome.py
   - PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile app/pages/00_Overview.py
   - PYTHONPYCACHEPREFIX=/tmp/pycache python3 -m py_compile app/pages/01_Earnings.py
   If any fail, do not push. Fix first.

## MERGE CONFLICT RULES
3. When resolving rebase/merge conflicts, NEVER use `--theirs` on these files:
   - app/Welcome.py
   - app/pages/00_Overview.py
   - app/pages/01_Earnings.py
   - app/pages/04_Genie.py
   For these files always use `--ours` or manually merge.
4. For all other files (scripts/, utils/, config) `--theirs` is safe.

## IMPORT RULES
5. Never remove an import without checking if it is used elsewhere in the same file.
6. Every page file must have this sys.path guard at the very top before any `from utils.` import:
   import sys
   from pathlib import Path
   sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

## LARGE FILE RULES
7. Never commit these files — they are in .gitignore for a reason:
   - earningscall_intelligence.db
   - app/attached_assets/HeroVideo.mp4
   - Any file over 5MB
8. Before every push run: `git status --short` and check for large files.

## STOP CONDITIONS
9. If a push is rejected, STOP and report the exact error. Never force-push without explicit instruction.
10. If a rebase hits more than 3 conflict files, STOP and report. Do not continue resolving automatically.

## DEV MANUAL FRESHNESS RULES
11. After any edit in `app/`, `scripts/`, or `.streamlit/`, run:
    - `python3 scripts/refresh_dev_manual.py`
12. This command must refresh all of these artifacts before push:
    - `reports/Developer_Insights_Bible_CURRENT.md`
    - `reports/Developer_Insights_Bible_CURRENT_Full.pdf`
    - `reports/dev_manual_assets/pipeline_dependency_graph.png`
    - `reports/dev_manual_assets/sheet_usage_heatmap.png`
    - `reports/dev_manual_assets/migration_impact.png`
    - `reports/dev_manual_assets/risk_distribution.png`
    - `reports/DEV_MANUAL_STATUS.json`
13. If any artifact generation fails, STOP and report the error. Do not push partial manual updates.
