#!/usr/bin/env python3
"""Fetch all currently implemented FinMind types for the observation list.

The workflow supplies the three token secrets as environment variables.  Each
stock subprocess is assigned one token so separate processes do not all restart
rotation at token zero.  Type 13 is fetched once as a combined CSV; Types 14
and 15 are derived locally from that file.
"""
from __future__ import annotations
import argparse
import csv
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills" / "skill-finmind-fetch" / "scripts"))
from token_env import get_finmind_tokens, order_tokens
sys.path.insert(0, str(ROOT / "scripts"))
import status_common
from download_log import now_cst, result_row, write_results

TOKEN_NAMES = ("FINDMIND_GMAIL_TOKEN1", "FINDMIND_GMAIL_TOKEN2", "FINDMIND_GMAIL_TOKEN3", "FINDMIND_GMAIL_TOKEN4", "FINDMIND_GMAIL_TOKEN5")
TOKEN_ORDER = []
PYTHON = sys.executable
SCRIPTS = ROOT / "skills" / "skill-finmind-fetch" / "scripts"


def stocks(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.reader(handle):
            if not row or row[0].strip() in {"代號", "stock_code"}:
                continue
            code = row[0].strip().zfill(4)
            if code and code != "0000":
                yield code, (row[1].strip() if len(row) > 1 else f"Stock_{code}")


def token_env(index: int):
    values = TOKEN_ORDER or [os.environ.get(name, "") for name in TOKEN_NAMES]
    selected = values[index % len(values)] if any(values) else ""
    env = os.environ.copy()
    # The child receives one token only; this makes round-robin assignment
    # effective while keeping token values out of command-line arguments.
    for name in TOKEN_NAMES:
        env[name] = selected
    return env


def run(args, token_index: int, label: str, preserve_pool: bool = False) -> bool:
    command = [PYTHON, *map(str, args)]
    print(f"[{label}] {Path(command[1]).name if len(command) > 1 else label}", flush=True)
    env = os.environ.copy() if preserve_pool else token_env(token_index)
    completed = subprocess.run(command, cwd=ROOT, env=env)
    if completed.returncode:
        print(f"[{label}] failed with exit code {completed.returncode}", flush=True)
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-list", required=True)
    parser.add_argument("--start-date", default="2021-01-01")
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()
    end = args.end_date or __import__("datetime").date.today().isoformat()
    target = list(stocks(Path(args.stock_list)))
    if not target:
        raise SystemExit("No stocks found")
    global TOKEN_ORDER
    configured = get_finmind_tokens()
    TOKEN_ORDER, details = order_tokens(configured)
    for slot, ((_, state), _) in enumerate(zip(details, TOKEN_ORDER), start=1):
        print(f"token-slot={slot} quota-check={state}", flush=True)
    print(f"Fetching {len(target)} stocks with quota-aware token rotation", flush=True)
    ok = 0

    # One combined Type 13 request loop; Type 14/15 reuse this file locally.
    type13 = ROOT / "financial" / "type13" / "raw_margin_daily.csv"
    if run([SCRIPTS / "fetch_to_csv.py", "--stock-list", args.stock_list,
            "--start-date", args.start_date, "--end-date", end,
            "--output-csv", type13], 0, "type13", preserve_pool=True):
        ok += 1

    jobs = [
        ("1", "fetch_type1.py", "raw_dividends", "2018-01-01"),
        ("5", "fetch_type5.py", "raw_revenue", "2021-01-01"),
        ("8", "fetch_k_chart_flow.py", "raw_weekly_flow", "2021-01-01"),
        ("12", "fetch_k_chart_flow.py", "raw_monthly_flow", "2021-01-01"),
        ("17", "fetch_k_chart_flow.py", "raw_weekly_k_chart_flow", "2021-01-01"),
        ("18", "fetch_k_chart_flow.py", "raw_daily_k_chart_flow", "2021-01-01"),
        ("11", "fetch_type11.py", "raw_weekly_trading_data", "2021-01-01"),
        ("16", "fetch_type16.py", "raw_fin_ratio_quarter", "2020-01-01"),
        ("19", "fetch_type19.py", "raw_dividend_schedule", "2018-01-01"),
    ]
    for type_id, script, stem, start in jobs:
        for index, (code, name) in enumerate(target):
            output = ROOT / "financial" / f"type{type_id}" / f"{stem}_{code}.csv"
            command = [SCRIPTS / script, "--stock-id", code, "--company-name", name,
                       "--start-date", start, "--end-date", end, "--output", output]
            if type_id in {"8", "12", "17", "18"}:
                command[1:1] = ["--type", type_id]
            if run(command, index, f"type{type_id}/{code}"):
                ok += 1

    for type_id, script, stem in (("14", "fetch_type14.py", "raw_margin_weekly"),
                                  ("15", "fetch_type15.py", "raw_margin_monthly")):
        for index, (code, name) in enumerate(target):
            output = ROOT / "financial" / f"type{type_id}" / f"{stem}_{code}.csv"
            command = [SCRIPTS / script, "--stock-id", code, "--company-name", name,
                       "--daily-csv", type13, "--start-date", args.start_date,
                       "--end-date", end, "--output", output]
            if run(command, index, f"type{type_id}/{code}"):
                ok += 1
    print(f"Completed fetch commands: {ok}", flush=True)

    process_time = now_cst()
    financial_root = ROOT / "financial"
    for type_id in status_common.ACTIVE_TYPES:
        present = status_common.output_stock_codes(financial_root, type_id)
        rows = [result_row(f"{status_common.ACTIVE_TYPES[type_id]}_{code}.csv", code in present, process_time)
                for code, _ in target]
        write_results(financial_root / f"type{type_id}" / "download_results.csv", rows)
    print("Wrote download_results.csv logs for active types", flush=True)


if __name__ == "__main__":
    main()
