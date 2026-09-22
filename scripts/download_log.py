"""Per-type download_results.csv writer, mirroring Python-Actions.GoodInfo's log format."""
from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
FIELDNAMES = ["filename", "last_update_time", "success", "process_time", "retry_count", "status", "error_reason"]


def now_cst() -> str:
    return datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S CST")


def write_results(log_path: Path, results: list[dict]) -> None:
    """Overwrite the log with one row per filename for this run's outcomes.

    Each daily run covers the full watchlist, so replacing the file (rather
    than merging with prior rows) keeps it an accurate snapshot of the most
    recent attempt for every tracked stock.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in results:
            writer.writerow({name: row.get(name, "") for name in FIELDNAMES})


def result_row(filename: str, success: bool, process_time: str, status: str | None = None, error_reason: str = "") -> dict:
    return {
        "filename": filename,
        "last_update_time": process_time if success else "",
        "success": "true" if success else "false",
        "process_time": process_time,
        "retry_count": 0,
        "status": status or ("success" if success else "retryable_failed"),
        "error_reason": error_reason,
    }
