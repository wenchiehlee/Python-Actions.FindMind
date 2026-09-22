"""Shared type/stock metadata used by the daily fetch and status scripts."""
from __future__ import annotations
import csv
import re
from pathlib import Path

# type_id -> (GoodInfo folder name, FinMind dataset, adapter mode)
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

# type_id -> refresh cadence, mirrors Python-Actions.GoodInfo's TYPE_PERIODS.
TYPE_PERIODS = {
    "1": "Daily",
    "4": "Weekly",
    "5": "Daily",
    "6": "Weekly",
    "7": "Weekly",
    "8": "Weekly",
    "9": "Weekly",
    "10": "Weekly",
    "11": "Weekly",
    "12": "Monthly",
    "13": "Daily",
    "14": "Weekly",
    "15": "Monthly",
    "16": "Monthly",
    "17": "Weekly",
    "18": "Daily",
    "19": "Weekly",
}

# type_id -> (output filename stem, fetch actually implemented)
ACTIVE_TYPES = {
    "1": "raw_dividends",
    "5": "raw_revenue",
    "8": "raw_weekly_flow",
    "11": "raw_weekly_trading_data",
    "12": "raw_monthly_flow",
    "13": "raw_margin_daily",
    "14": "raw_margin_weekly",
    "15": "raw_margin_monthly",
    "16": "raw_fin_ratio_quarter",
    "17": "raw_weekly_k_chart_flow",
    "18": "raw_daily_k_chart_flow",
    "19": "raw_dividend_schedule",
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
