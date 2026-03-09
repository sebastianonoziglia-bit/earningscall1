#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
STATUS_PATH = REPORTS / "DEV_MANUAL_STATUS.json"
BIBLE_MD = REPORTS / "Developer_Insights_Bible_CURRENT.md"
BIBLE_PDF = REPORTS / "Developer_Insights_Bible_CURRENT_Full.pdf"
LEGACY_DUPLICATES = [
    REPORTS / "Developer_Insights_Bible.md",
    REPORTS / "Developer_Insights_Bible_Full.pdf",
]


def _run(cmd: list[str]) -> None:
    print(f"[run] {' '.join(cmd)}")
    subprocess.run(cmd, cwd=str(ROOT), check=True)


def _git(args: list[str], fallback: str = "unknown") -> str:
    try:
        return (
            subprocess.check_output(["git", *args], cwd=str(ROOT), text=True, stderr=subprocess.DEVNULL)
            .strip()
            or fallback
        )
    except Exception:
        return fallback


def _remove_legacy_duplicates() -> list[str]:
    removed: list[str] = []
    for path in LEGACY_DUPLICATES:
        if path.exists():
            path.unlink()
            removed.append(str(path.relative_to(ROOT)))
    return removed


def _build_status(removed_legacy: list[str] | None = None) -> dict:
    tracked = [
        BIBLE_MD,
        BIBLE_PDF,
        REPORTS / "dev_manual_assets" / "pipeline_dependency_graph.png",
        REPORTS / "dev_manual_assets" / "sheet_usage_heatmap.png",
        REPORTS / "dev_manual_assets" / "migration_impact.png",
        REPORTS / "dev_manual_assets" / "risk_distribution.png",
    ]
    file_meta = []
    for path in tracked:
        meta = {"path": str(path.relative_to(ROOT)), "exists": path.exists()}
        if path.exists():
            stat = path.stat()
            meta["size_bytes"] = stat.st_size
            meta["modified_utc"] = dt.datetime.utcfromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ")
        file_meta.append(meta)

    return {
        "generated_at_utc": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "git_branch": _git(["branch", "--show-current"]),
        "git_head": _git(["rev-parse", "HEAD"]),
        "git_head_short": _git(["rev-parse", "--short", "HEAD"]),
        "working_tree_dirty": bool(_git(["status", "--porcelain"], "").strip()),
        "generator_scripts": [
            "scripts/generate_dev_manual_assets.py",
            "scripts/generate_current_bible.py",
            "scripts/generate_current_bible_pdf.py",
        ],
        "canonical_bible_files": [
            str(BIBLE_MD.relative_to(ROOT)),
            str(BIBLE_PDF.relative_to(ROOT)),
        ],
        "removed_legacy_duplicates": removed_legacy or [],
        "artifacts": file_meta,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate current dev manual assets + bible + PDF + status manifest.")
    parser.add_argument(
        "--skip-pdf",
        action="store_true",
        help="Skip PDF export and only refresh markdown/assets/manifest.",
    )
    args = parser.parse_args()

    _run(["python3", "scripts/generate_dev_manual_assets.py"])
    _run(["python3", "scripts/generate_current_bible.py"])
    if not args.skip_pdf:
        _run(["python3", "scripts/generate_current_bible_pdf.py"])
    removed_legacy = _remove_legacy_duplicates()
    if removed_legacy:
        print("[cleanup] removed legacy duplicates:")
        for rel in removed_legacy:
            print(f"  - {rel}")

    REPORTS.mkdir(parents=True, exist_ok=True)
    status = _build_status(removed_legacy=removed_legacy)
    STATUS_PATH.write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {STATUS_PATH}")


if __name__ == "__main__":
    main()
