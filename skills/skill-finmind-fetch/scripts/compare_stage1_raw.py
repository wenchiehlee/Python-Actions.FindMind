#!/usr/bin/env python3
"""Compare this repo's data/stage1_raw/<stem>.csv against the reference
Python-Actions.GoodInfo.Analyzer data/stage1_raw/<stem>.csv for the same type.

Every stage1_raw CSV follows the same layout: stock_code, company_name, a
period column (3rd column -- 年度/季度/月別/交易_週別/期別/... depending on
type), then data columns, then 6 fixed metadata columns (file_type,
source_file, download_success, download_timestamp, process_timestamp,
stage1_process_timestamp) that always differ between sources and are
excluded from comparison.

Reports, per type:
- stocks present in one side but missing from the other
- (stock, period) rows present in one side but missing from the other
- per data column: how many matched rows differ beyond tolerance, with a
  few examples, and how many are missing on our side vs the reference
"""
from __future__ import annotations
import argparse
import re
import sys
from pathlib import Path

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import status_common

METADATA_COLUMNS = {"file_type", "source_file", "download_success",
                     "download_timestamp", "process_timestamp", "stage1_process_timestamp"}

# 期間欄位候選名稱：不同 type 的期間欄位名稱不同（季度/年度/月別/交易_週別/期別），
# 且部分 type（如 raw_performance1／raw_performance）在 company_name 之後還夾著
# 一批成交價相關的空白欄位，期間欄位不一定落在固定的第 3 欄，所以改用欄名查找。
PERIOD_COLUMN_CANDIDATES = ["季度", "年度", "月別", "交易_週別", "期別", "日期"]

# 同一個 quarter 在 GoodInfo（"2026Q2"）與 FinMind（"2026/2"）兩邊的表示法不同，
# 比對前先正規化成統一格式，否則就算欄位對上了也永遠 join 不到任何一列。
_QUARTER_SLASH_RE = re.compile(r"^(\d{4})/([1-4])$")


def find_period_col(df: pd.DataFrame) -> str:
    for name in PERIOD_COLUMN_CANDIDATES:
        if name in df.columns:
            return name
    return df.columns[2]


def normalize_period(value: str) -> str:
    if not isinstance(value, str):
        return value
    match = _QUARTER_SLASH_RE.match(value.strip())
    if match:
        return f"{match.group(1)}Q{match.group(2)}"
    return value.strip()


def load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df["stock_code"] = df["stock_code"].str.zfill(4)
    # GoodInfo 用單獨一個 "-" 字元代表「這欄沒有值」（例如財報還沒完全公布），
    # 視為缺值而非一個真正的字串值，否則後面數值比對會誤判成「差異」。
    non_dash_columns = {"stock_code", "company_name"}
    for col in df.columns:
        if col in non_dash_columns:
            continue
        df[col] = df[col].where(df[col] != "-", pd.NA)
    return df


def values_differ(a, b, tolerance: float) -> bool:
    if pd.isna(a) and pd.isna(b):
        return False
    if pd.isna(a) or pd.isna(b):
        return True
    try:
        fa, fb = float(a), float(b)
        if fa == fb:
            return False
        denom = max(abs(fa), abs(fb), 1e-9)
        return abs(fa - fb) / denom > tolerance
    except (TypeError, ValueError):
        return str(a).strip() != str(b).strip()


def compare(ours_path: Path, reference_path: Path, tolerance: float, examples: int) -> None:
    ours = load(ours_path)
    reference = load(reference_path)
    period_col = find_period_col(ours)
    reference_period_col = find_period_col(reference)
    if reference_period_col != period_col:
        print(f"  WARNING: period column name differs (ours={period_col!r}, reference={reference_period_col!r}); "
              f"using ours's column name for the join key")
        reference = reference.rename(columns={reference_period_col: period_col})

    ours[period_col] = ours[period_col].map(normalize_period)
    reference[period_col] = reference[period_col].map(normalize_period)

    ours = ours.set_index(["stock_code", period_col])
    reference = reference.set_index(["stock_code", period_col])
    ours = ours[~ours.index.duplicated(keep="last")]
    reference = reference[~reference.index.duplicated(keep="last")]

    our_stocks, ref_stocks = set(ours.index.get_level_values(0)), set(reference.index.get_level_values(0))
    missing_stocks = ref_stocks - our_stocks
    extra_stocks = our_stocks - ref_stocks
    if missing_stocks:
        print(f"  Stocks in reference but missing entirely from ours ({len(missing_stocks)}): "
              f"{', '.join(sorted(missing_stocks)[:20])}{' ...' if len(missing_stocks) > 20 else ''}")
    if extra_stocks:
        print(f"  Stocks in ours but not in reference ({len(extra_stocks)}): "
              f"{', '.join(sorted(extra_stocks)[:20])}{' ...' if len(extra_stocks) > 20 else ''}")

    common_keys = ours.index.intersection(reference.index)
    missing_rows = reference.index.difference(ours.index)
    extra_rows = ours.index.difference(reference.index)
    print(f"  Matched (stock, {period_col}) rows: {len(common_keys)}")
    if len(missing_rows):
        print(f"  Rows in reference but missing from ours: {len(missing_rows)} "
              f"(e.g. {list(missing_rows[:5])})")
    if len(extra_rows):
        print(f"  Rows in ours but not in reference: {len(extra_rows)} (e.g. {list(extra_rows[:5])})")

    if len(common_keys) == 0:
        return

    data_columns = [c for c in ours.columns if c not in METADATA_COLUMNS and c in reference.columns]
    ours_common = ours.loc[common_keys]
    ref_common = reference.loc[common_keys]

    print(f"  Column comparison over {len(common_keys)} matched rows (tolerance={tolerance:.0%}):")
    for col in data_columns:
        a, b = ours_common[col], ref_common[col]
        diff_mask = [values_differ(x, y, tolerance) for x, y in zip(a, b)]
        diff_count = sum(diff_mask)
        our_missing = sum(pd.isna(a) & ~pd.isna(b))
        ref_missing = sum(~pd.isna(a) & pd.isna(b))
        if diff_count == 0 and our_missing == 0 and ref_missing == 0:
            continue
        parts = []
        if diff_count:
            parts.append(f"{diff_count} differ")
        if our_missing:
            parts.append(f"{our_missing} missing on our side")
        if ref_missing:
            parts.append(f"{ref_missing} missing on reference side")
        print(f"    {col}: {', '.join(parts)}")
        if diff_count and examples:
            shown = 0
            for (key, x, y) in zip(common_keys, a, b):
                if shown >= examples:
                    break
                if values_differ(x, y, tolerance):
                    print(f"      e.g. {key}: ours={x!r} reference={y!r}")
                    shown += 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", help="Type id to compare (e.g. 1). Omit to compare every active type.")
    parser.add_argument("--ours-root", default=str(ROOT / "data" / "stage1_raw"))
    parser.add_argument("--reference-root", default=str(ROOT.parent / "Python-Actions.GoodInfo.Analyzer" / "data" / "stage1_raw"),
                         help="Path to Python-Actions.GoodInfo.Analyzer's data/stage1_raw directory")
    parser.add_argument("--tolerance", type=float, default=0.01, help="Relative tolerance for numeric comparison (default 1%%)")
    parser.add_argument("--examples", type=int, default=3, help="Example mismatches to print per column")
    args = parser.parse_args()

    ours_root, reference_root = Path(args.ours_root), Path(args.reference_root)
    type_ids = [args.type] if args.type else list(status_common.ACTIVE_TYPES)

    for type_id in type_ids:
        stem = status_common.ACTIVE_TYPES.get(type_id)
        if not stem:
            print(f"type{type_id}: unknown type id, skipping")
            continue
        ours_path = ours_root / f"{stem}.csv"
        reference_path = reference_root / f"{stem}.csv"
        print(f"=== type{type_id}: {stem}.csv ===")
        if not ours_path.exists():
            print(f"  Not generated yet: {ours_path}")
            continue
        if not reference_path.exists():
            print(f"  No reference file: {reference_path}")
            continue
        compare(ours_path, reference_path, args.tolerance, args.examples)


if __name__ == "__main__":
    main()
