#!/usr/bin/env python3
"""Backfill missing values in Python-Actions.GoodInfo.Analyzer's
data/stage1_raw/<stem>.csv using this repo's own already-fetched FinMind data
as a cross-check / fallback source (see skill-finmind-fetch's compare_stage1_raw.py
for the read-only comparison this reuses).

Only fills cells that are genuinely missing on the GoodInfo side (empty or "-"
placeholder); a value GoodInfo already has is never overwritten, even when it
disagrees with FinMind's number (that's a *difference* to review by hand via
compare_stage1_raw.py, not something this script resolves on its own).
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import status_common
from compare_stage1_raw import METADATA_COLUMNS, find_period_col, load, normalize_period

NON_DATA_COLUMNS = METADATA_COLUMNS | {"stock_code", "company_name"}


def infer_style(series: pd.Series) -> tuple[int, bool]:
    """回填數字要跟目標欄位既有的格式一致：抓現有值最常見的小數位數，
    以及該欄位是否習慣對正數加上顯式 "+" 號（例如 eps_年增 欄）。"""
    decimal_counts: dict[int, int] = {}
    explicit_plus = False
    for value in series.dropna():
        text = str(value)
        digits = len(text.split(".")[1]) if "." in text else 0
        decimal_counts[digits] = decimal_counts.get(digits, 0) + 1
        if text.startswith("+"):
            explicit_plus = True
    decimals = max(decimal_counts, key=decimal_counts.get) if decimal_counts else 2
    return decimals, explicit_plus


def format_value(value: str, decimals: int, explicit_plus: bool) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if number == 0:
        number = 0.0  # avoid "-0.00" from floating point negative zero
    formatted = f"{number:.{decimals}f}"
    if explicit_plus and number > 0:
        formatted = f"+{formatted}"
    return formatted


def backfill(source_path: Path, target_path: Path, dry_run: bool) -> bool:
    source = load(source_path)
    target = load(target_path)
    # 記錄哪些格子原本是 GoodInfo 的 "-" 缺值標記（相對於本來就是空白的欄位，
    # 例如 raw_performance1.csv 開頭那批成交價相關空欄），寫回檔案時只還原
    # 這些格子成 "-"，維持該欄位原有的缺值慣例，避免整份檔案跑出無意義的 diff。
    dash_mask = pd.read_csv(target_path, dtype=str, encoding="utf-8-sig") == "-"

    period_col = find_period_col(source)
    target_period_col = find_period_col(target)
    if target_period_col != period_col:
        target = target.rename(columns={target_period_col: period_col})
        dash_mask = dash_mask.rename(columns={target_period_col: period_col})

    source[period_col] = source[period_col].map(normalize_period)
    target[period_col] = target[period_col].map(normalize_period)

    source_keyed = source.set_index(["stock_code", period_col])
    source_keyed = source_keyed[~source_keyed.index.duplicated(keep="last")]

    data_columns = [c for c in target.columns if c not in NON_DATA_COLUMNS and c in source_keyed.columns]
    style_by_col = {col: infer_style(target[col]) for col in data_columns}

    fill_count_by_col: dict[str, int] = {}
    touched_rows = 0
    examples: list[str] = []

    for idx, row in target.iterrows():
        key = (row["stock_code"], row[period_col])
        if key not in source_keyed.index:
            continue
        source_row = source_keyed.loc[key]
        row_touched = False
        for col in data_columns:
            if pd.notna(row[col]):
                continue
            source_value = source_row[col]
            if pd.isna(source_value):
                continue
            decimals, explicit_plus = style_by_col[col]
            formatted = format_value(source_value, decimals, explicit_plus)
            target.at[idx, col] = formatted
            fill_count_by_col[col] = fill_count_by_col.get(col, 0) + 1
            row_touched = True
            if len(examples) < 20:
                examples.append(f"{key}: {col} -> {formatted} (FinMind={source_value})")
        if row_touched:
            touched_rows += 1

    print(f"Rows touched: {touched_rows}")
    for col, count in sorted(fill_count_by_col.items(), key=lambda kv: -kv[1]):
        print(f"  {col}: {count} cells filled")
    if examples:
        print("Examples:")
        for example in examples:
            print(f"  {example}")

    if touched_rows == 0:
        print("Nothing to fill.")
        return False
    if dry_run:
        print("(dry run, not writing)")
        return False

    # 還原「本來是 "-"、backfill 後仍然缺值」的格子，本來就是空白的格子維持空白，
    # 兩者對下游 to_float() 之類的解析都等同缺值，但保留原始慣例才不會產生
    # 整份檔案數萬列的無意義 diff。
    still_missing = target.isna()
    target = target.mask(still_missing & dash_mask, "-")
    # newline="" 避免 Windows 把 to_csv 的 "\n" 又翻譯成 "\r\n"，否則即使沒改到
    # 的列，換行符也會從 LF 變成 CRLF，造成整份檔案數萬列的無意義 diff。
    with open(target_path, "w", newline="", encoding="utf-8-sig") as handle:
        target.to_csv(handle, index=False, lineterminator="\n")
    print(f"Wrote {target_path}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True, help="Type id to backfill (e.g. 7)")
    parser.add_argument("--source-root", default=str(ROOT / "data" / "stage1_raw"),
                         help="This repo's stage1_raw dir (FinMind-fetched data, used as fill source)")
    parser.add_argument("--target-root",
                         default=str(ROOT.parent / "Python-Actions.GoodInfo.Analyzer" / "data" / "stage1_raw"),
                         help="Python-Actions.GoodInfo.Analyzer's stage1_raw dir (file patched in place)")
    parser.add_argument("--dry-run", action="store_true", help="Report what would change without writing")
    args = parser.parse_args()

    stem = status_common.ACTIVE_TYPES.get(args.type)
    if not stem:
        print(f"type{args.type}: unknown type id")
        raise SystemExit(1)

    source_path = Path(args.source_root) / f"{stem}.csv"
    target_path = Path(args.target_root) / f"{stem}.csv"
    if not source_path.exists():
        print(f"Missing source file: {source_path}")
        raise SystemExit(1)
    if not target_path.exists():
        print(f"Missing target file: {target_path}")
        raise SystemExit(1)

    backfill(source_path, target_path, args.dry_run)


if __name__ == "__main__":
    main()
