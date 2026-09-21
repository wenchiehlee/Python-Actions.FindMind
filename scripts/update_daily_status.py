#!/usr/bin/env python3
"""Update README status from locally fetched CSV outputs."""
from __future__ import annotations
import argparse
import csv
import re
from datetime import datetime
from pathlib import Path

TYPES = {
    "1": ("DividendDetail", "TaiwanStockDividend", "direct"),
    "4": ("StockBzPerformance", "TaiwanStockFinancialStatements", "partial"),
    "5": ("ShowSaleMonChart", "TaiwanStockMonthRevenue", "direct"),
    "6": ("EquityDistribution", "TaiwanStockShareholding", "partial"),
    "7": ("StockBzPerformance1", "TaiwanStockFinancialStatements", "partial"),
    "8": ("ShowK_ChartFlow", "TaiwanStockPER", "direct"),
    "9": ("StockHisAnaQuar", "TaiwanStockFinancialStatements", "partial"),
    "10": ("EquityDistributionClassHis", "TaiwanStockHoldingSharesPer", "permission"),
    "11": ("WeeklyTradingData", "TaiwanStockInstitutionalInvestorsBuySellWide", "partial"),
    "12": ("ShowMonthlyK_ChartFlow", "TaiwanStockPER", "direct"),
    "13": ("ShowMarginChart", "TaiwanStockMarginPurchaseShortSale", "direct"),
    "14": ("ShowMarginChartWeek", "TaiwanStockMarginPurchaseShortSale", "derived"),
    "15": ("ShowMarginChartMonth", "TaiwanStockMarginPurchaseShortSale", "derived"),
    "16": ("StockFinDetail", "TaiwanStockFinancialStatements", "partial"),
    "17": ("ShowWeeklyK_ChartFlow", "TaiwanStockPER", "direct"),
    "18": ("ShowDailyK_ChartFlow", "TaiwanStockPER", "direct"),
    "19": ("Dividenschedule", "TaiwanStockDividend", "direct"),
}
STOCK_CODE = re.compile(r"(?:^|[_-])([0-9]{4})(?:\.[^.]+)?$")

def load_stocks(path: Path) -> list[str]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        result, seen = [], set()
        for row in csv.reader(handle):
            if not row or not row[0].strip() or row[0].strip() in {"代號", "stock_code"}:
                continue
            code = row[0].strip().zfill(4)
            if code not in seen:
                result.append(code)
                seen.add(code)
    return result

def output_stock_codes(data_root: Path, type_id: str) -> set[str]:
    directory = data_root / f"type{type_id}"
    codes = set()
    if not directory.is_dir():
        return codes
    for path in directory.glob("*.csv"):
        match = STOCK_CODE.search(path.stem)
        if match:
            codes.add(match.group(1))
            continue
        try:
            with path.open(encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    value = str(row.get("stock_id") or row.get("stock_code") or "").strip()
                    if value.isdigit():
                        codes.add(value.zfill(4))
        except (OSError, UnicodeError):
            continue
    return codes

def badge(text: str, color: str) -> str:
    return f'![](https://img.shields.io/badge/{text.replace(" ", "%20")}-{color})'

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-list", default="data/StockID_TWSE_TPEX.csv")
    parser.add_argument("--data-root", default="financial")
    parser.add_argument("--readme", default="README.md")
    args = parser.parse_args()
    stocks = set(load_stocks(Path(args.stock_list)))
    total = len(stocks)
    rows = []
    for type_id, (folder, dataset, mode) in TYPES.items():
        count = len(output_stock_codes(Path(args.data_root), type_id) & stocks)
        completion = f"{count}/{total}"
        if count == total and total:
            status, color, note = "ready", "brightgreen", "CSV 已涵蓋全部觀察名單"
        elif count:
            status, color, note = "partial", "orange", "CSV 尚未涵蓋全部觀察名單"
        elif mode == "permission":
            status, color, note = "permission", "red", "FinMind tier 不足或尚未取得資料"
        else:
            status, color, note = "not run", "lightgrey", "尚無成功產生的 CSV"
        rows.append(f"| {type_id} | {folder} | {dataset} | {completion} | local CSV | {mode} | {badge(status, color)} | {note} |")
    table = "\n".join(["| Type | GoodInfo type | FinMind dataset | Completion | API | Adapter | Status | Note |", "| -- | -- | -- | --: | -- | -- | -- | -- |", *rows])
    block = f"""<!-- FINMIND_STATUS_START -->
## Status

Update time: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}

Watchlist completion is counted from successfully generated local CSV files: `{total}` stocks total.

{table}
<!-- FINMIND_STATUS_END -->"""
    path = Path(args.readme)
    content = path.read_text(encoding="utf-8")
    start, end = "<!-- FINMIND_STATUS_START -->", "<!-- FINMIND_STATUS_END -->"
    if start in content and end in content:
        begin = content.index(start)
        finish = content.index(end, begin) + len(end)
        content = content[:begin] + block + content[finish:]
    else:
        content = block + "\n\n" + content
    path.write_text(content, encoding="utf-8")
    print(f"Updated {path} from {args.data_root}; watchlist={total}")

if __name__ == "__main__":
    main()
