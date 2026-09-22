#!/usr/bin/env python3
"""Analyze per-type download_results.csv logs and render a GoodInfo-style status table.

Mirrors the column layout and badge scheme of Python-Actions.GoodInfo's
download_results_counts.py so the two repos' README "Status" sections read
the same way, adapted to FinMind's per-run (not per-retry) fetch model.
"""
from __future__ import annotations
import argparse
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import status_common

TAIPEI_TZ = ZoneInfo("Asia/Taipei")
UTC_TZ = ZoneInfo("UTC")

FOLDER_MAPPING = {int(type_id): folder for type_id, (folder, _, _) in status_common.TYPES.items()}
TYPE_PERIODS = {int(type_id): period for type_id, period in status_common.TYPE_PERIODS.items()}


def get_taipei_time() -> datetime:
    return datetime.now(TAIPEI_TZ)


def make_badge(text: str, color: str = "blue") -> str:
    if not text or text in ("N/A", "Never", "0"):
        return ""
    safe_text = text.replace(" ", "_").replace("/", "%2F")
    return f"![](https://img.shields.io/badge/{safe_text}-{color})"


def format_time_compact(time_diff: timedelta) -> str:
    if time_diff.total_seconds() < 0:
        return "future"
    days = time_diff.days
    hours = time_diff.seconds // 3600
    minutes = (time_diff.seconds % 3600) // 60
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 and len(parts) < 2:
        parts.append(f"{minutes}m")
    if not parts:
        return "now"
    return " ".join(parts[:2]) + " ago"


def get_time_badge_color(time_text: str) -> str:
    if not time_text or time_text in ("N/A", "Never", "Error"):
        return "lightgrey"
    if "now" in time_text:
        return "brightgreen"
    if "m ago" in time_text and "h" not in time_text and "d" not in time_text:
        return "brightgreen"
    if "h ago" in time_text and "d" not in time_text:
        return "blue"
    if "1d" in time_text:
        return "yellow"
    if "2d" in time_text or "3d" in time_text:
        return "orange"
    if "d" in time_text:
        return "red"
    return "blue"


def normalize_status(row: Dict) -> str:
    raw_status = (row.get("status") or "").strip().lower()
    success = (row.get("success") or "").strip().lower() == "true"
    if raw_status:
        return raw_status
    return "success" if success else "retryable_failed"


def safe_parse_date(date_string: str) -> Optional[datetime]:
    if not date_string or date_string.strip() in ("NOT_PROCESSED", "NEVER", ""):
        return None
    value = date_string.strip()
    explicit_cst = value.endswith(" CST")
    if explicit_cst:
        value = value[:-4].strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(value, fmt)
        except ValueError:
            continue
        if explicit_cst:
            return parsed.replace(tzinfo=TAIPEI_TZ)
        return parsed.replace(tzinfo=UTC_TZ).astimezone(TAIPEI_TZ)
    return None


def analyze_csv(csv_path: Path) -> Dict:
    current_time = get_taipei_time()
    stats = {
        "total": 0, "success": 0, "failed": 0,
        "retryable_failed": 0, "rate_limited": 0, "not_processed": 0,
        "updated_from_now": "N/A", "oldest": "N/A", "lag": "N/A", "latest": "N/A",
        "error": None,
    }
    if not csv_path.exists():
        stats["error"] = "File not found"
        return stats
    with csv_path.open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return stats

    status_counts = {"retryable_failed": 0, "rate_limited": 0, "not_processed": 0}
    for row in rows:
        status = normalize_status(row)
        if status in ("success", ""):
            continue
        if status in status_counts:
            status_counts[status] += 1
        else:
            status_counts["retryable_failed"] += 1

    success_count = sum(1 for row in rows if normalize_status(row) == "success")
    stats.update({
        "total": len(rows),
        "success": success_count,
        "failed": len(rows) - success_count,
        **status_counts,
    })

    process_times = [t for t in (safe_parse_date(r.get("process_time", "")) for r in rows) if t]
    update_times = [t for t in (safe_parse_date(r.get("last_update_time", "")) for r in rows) if t]

    if process_times:
        stats["updated_from_now"] = format_time_compact(current_time - max(process_times))
    else:
        stats["updated_from_now"] = "Never"

    if update_times:
        stats["latest"] = format_time_compact(current_time - max(update_times))
        stats["oldest"] = format_time_compact(current_time - min(update_times))
        stats["lag"] = f"{stats['latest']} / {stats['oldest']}"
    else:
        stats["latest"] = stats["oldest"] = "Never"
        stats["lag"] = "N/A"
    return stats


def scan_all_folders(data_root: Path) -> List[Dict]:
    results = []
    for type_id in sorted(FOLDER_MAPPING):
        folder = FOLDER_MAPPING[type_id]
        csv_path = data_root / f"type{type_id}" / "download_results.csv"
        csv_stats = analyze_csv(csv_path)
        results.append({
            "No": type_id,
            "Folder": folder,
            "Total": csv_stats["total"],
            "Success": csv_stats["success"],
            "Failed": csv_stats["failed"],
            "RetryableFailed": csv_stats.get("retryable_failed", 0),
            "RateLimited": csv_stats.get("rate_limited", 0),
            "NotProcessed": csv_stats.get("not_processed", 0),
            "Updated": csv_stats["updated_from_now"],
            "Lag": csv_stats["lag"],
            "error": csv_stats.get("error"),
        })
    return results


def compact_age_days(value: str) -> Optional[float]:
    if not value or value in ("N/A", "Never", "future"):
        return None
    if value == "now":
        return 0.0
    total = 0.0
    for part in value.replace("ago", "").strip().split():
        if part.endswith("d"):
            total += float(part[:-1])
        elif part.endswith("h"):
            total += float(part[:-1]) / 24
        elif part.endswith("m"):
            total += float(part[:-1]) / 1440
    return total


def get_stale_after_days(period: str) -> str:
    return {"Daily": "1", "Weekly": "7", "Monthly": "30", "Manual": ""}.get(period, "")


def get_status(result: Dict, expected_rows: int) -> str:
    total = result["Total"]
    success = result["Success"]
    period = TYPE_PERIODS.get(result["No"], "Manual")
    stale_after = get_stale_after_days(period)
    actionable_failures = result["RetryableFailed"] + result["RateLimited"] + result["NotProcessed"]
    age_days = compact_age_days(result["Updated"])
    stale_value = float(stale_after) if stale_after else None

    if total == 0:
        return "not run"
    if age_days is not None and stale_value is not None and age_days > stale_value:
        return "stale"
    if actionable_failures:
        return "warning"
    if success >= expected_rows:
        return "ready"
    return "partial"


def format_table(results: List[Dict], expected_rows: int) -> str:
    header = "| No | Folder | Period | Completion | Downloaded | Failures | Duration | Lag | Limit | Status |\n"
    header += "| -- | -- | -- | -- | -- | -- | -- | -- | -- | -- |\n"
    rows = []
    for r in results:
        period = TYPE_PERIODS.get(r["No"], "Manual")
        success_count = r["Success"]
        total_count = r["Total"]

        if total_count == 0:
            progress_color = "inactive-lightgrey"
        elif success_count >= expected_rows:
            progress_color = "success-brightgreen"
        elif total_count >= expected_rows:
            progress_color = "failed-orange"
        else:
            progress_color = "yellow"
        progress = make_badge(f"{success_count}/{expected_rows}", progress_color)
        downloaded = make_badge(str(success_count), "success-brightgreen") if success_count else ""

        failure_badges = [
            make_badge(f"retryable_{r['RetryableFailed']}", "failed-orange") if r["RetryableFailed"] else "",
            make_badge(f"rate_{r['RateLimited']}", "rate_limited-yellow") if r["RateLimited"] else "",
            make_badge(f"pending_{r['NotProcessed']}", "inactive-lightgrey") if r["NotProcessed"] else "",
        ]
        failures = " ".join(b for b in failure_badges if b)

        duration = make_badge(r["Updated"], get_time_badge_color(r["Updated"])) if r["Updated"] != "N/A" else "N/A"
        if r["Lag"] != "N/A":
            lag_display = " / ".join(make_badge(part, get_time_badge_color(part)) for part in r["Lag"].split(" / "))
        else:
            lag_display = "N/A"

        limit_value = get_stale_after_days(period)
        limit = f"{limit_value}d" if limit_value else "-"
        status_value = get_status(r, expected_rows)
        status_color = {
            "ready": "success-brightgreen", "warning": "yellow", "stale": "red", "not run": "lightgrey",
            "partial": "orange",
        }.get(status_value, "lightgrey")
        status = make_badge(status_value, status_color)

        rows.append(f"| {r['No']} | {r['Folder']} | {period} | {progress} | {downloaded} | {failures} | {duration} | {lag_display} | {limit} | {status} |")
    return header + "\n".join(rows)


def update_readme(readme_path: Path, table_text: str, expected_rows: int) -> None:
    update_time = get_taipei_time().strftime("%Y-%m-%d %H:%M:%S CST")
    block = f"""<!-- FINMIND_STATUS_START -->
## Status

Update time: {update_time}

Time units: `y/M/d/h/m`; `M` = month, `m` = minute. Completion is counted from FinMind fetch attempts logged per stock: `{expected_rows}` stocks total.

{table_text}
<!-- FINMIND_STATUS_END -->"""
    content = readme_path.read_text(encoding="utf-8")
    start, end = "<!-- FINMIND_STATUS_START -->", "<!-- FINMIND_STATUS_END -->"
    if start in content and end in content:
        begin = content.index(start)
        finish = content.index(end, begin) + len(end)
        content = content[:begin] + block + content[finish:]
    else:
        content = block + "\n\n" + content
    readme_path.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-list", default="data/StockID_TWSE_TPEX.csv")
    parser.add_argument("--data-root", default="financial")
    parser.add_argument("--readme", default="README.md")
    parser.add_argument("--update-readme", action="store_true")
    args = parser.parse_args()

    expected_rows = len(status_common.load_stocks(Path(args.stock_list)))
    results = scan_all_folders(Path(args.data_root))
    table_text = format_table(results, expected_rows)
    print(table_text)

    if args.update_readme:
        update_readme(Path(args.readme), table_text, expected_rows)
        print(f"Updated {args.readme}")


if __name__ == "__main__":
    main()
