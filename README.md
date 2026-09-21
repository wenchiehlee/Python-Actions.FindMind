<!-- FINMIND_STATUS_START -->
## Status

Update time: 2026-09-21 22:55:18 CST

Token rotation pool: `3` configured

| Type | GoodInfo type | FinMind dataset | Watchlist | API | Adapter | Status | Note |
| -- | -- | -- | --: | -- | -- | -- | -- |
| 1 | DividendDetail | TaiwanStockDividend | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 4 | StockBzPerformance | TaiwanStockFinancialStatements | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 5 | ShowSaleMonChart | TaiwanStockMonthRevenue | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 6 | EquityDistribution | TaiwanStockShareholding | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 7 | StockBzPerformance1 | TaiwanStockFinancialStatements | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 8 | ShowK_ChartFlow | TaiwanStockPER | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 9 | StockHisAnaQuar | TaiwanStockFinancialStatements | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 10 | EquityDistributionClassHis | TaiwanStockHoldingSharesPer | 142 | permission | permission | ![](https://img.shields.io/badge/permission-orange) | FinMind tier 不足 |
| 11 | WeeklyTradingData | TaiwanStockInstitutionalInvestorsBuySellWide | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 12 | ShowMonthlyK_ChartFlow | TaiwanStockPER | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 13 | ShowMarginChart | TaiwanStockMarginPurchaseShortSale | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 14 | ShowMarginChartWeek | TaiwanStockMarginPurchaseShortSale | 142 | 由 Type 13 daily 聚合 | derived | ![](https://img.shields.io/badge/derived-blue) | 不重複下載 |
| 15 | ShowMarginChartMonth | TaiwanStockMarginPurchaseShortSale | 142 | 由 Type 13 daily 聚合 | derived | ![](https://img.shields.io/badge/derived-blue) | 不重複下載 |
| 16 | StockFinDetail | TaiwanStockFinancialStatements | 142 | success | partial | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
| 17 | ShowWeeklyK_ChartFlow | TaiwanStockPER | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 18 | ShowDailyK_ChartFlow | TaiwanStockPER | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=11 |
| 19 | Dividenschedule | TaiwanStockDividend | 142 | success | direct | ![](https://img.shields.io/badge/ready-brightgreen) | sample rows=0 |
<!-- FINMIND_STATUS_END -->

# Python-Actions.FindMind

FinMind-based data fetchers for the GoodInfo Analyzer-compatible pipeline.

## Active implementations

- `skills/skill-finmind-fetch/`: FinMind Type 13 daily price and margin data. This is the single active copy of the shared `skill-finmind-fetch` skill.
- `skills/skill-finmind-fetch/scripts/fetch_type1.py`: FinMind Type 1 dividend-policy adapter.
- `skills/skill-finmind-fetch/scripts/fetch_type5.py`: FinMind Type 5 monthly-revenue adapter.
- `skills/skill-finmind-fetch/scripts/fetch_k_chart_flow.py`: shared FinMind adapter for Types 8, 12, 17, and 18.
- `skills/skill-finmind-fetch/scripts/fetch_type11.py`: FinMind Type 11 weekly trading/institutional-flow adapter.
- `skills/skill-finmind-fetch/scripts/fetch_type19.py`: FinMind Type 19 dividend-schedule adapter.
- `skills/skill-finmind-fetch/scripts/fetch_type14.py`: FinMind Type 14 weekly margin adapter. It reuses Type 13 daily CSV when available, otherwise aggregates FinMind daily price/margin data.
- `skills/skill-finmind-fetch/scripts/fetch_type15.py`: FinMind Type 15 monthly margin adapter. It reuses Type 13 daily CSV when available and converts lots to thousand-lots.
- `skills/skill-finmind-fetch/scripts/fetch_type16.py`: FinMind Type 16 quarterly financial-ratio adapter. It uses the Analyzer 164-column schema and is a separate script within the same skill because it consumes quarterly financial statements, balance sheets, and cash-flow statements.
- `skills/skill-finmind-fetch/scripts/compare_type16.py`: compares generated Type 16 CSV values with the Analyzer GoodInfo CSV.
- `archived/`: legacy FindMind scripts, data, and workflows; not active implementations.

## Credentials

Copy `.env.example` to `.env` and set one or more of `FINMIND_TOKEN`, `FINMIND_API_TOKEN`, `FINDMIND_GMAIL_TOKEN`, `FINDMIND_GMAIL_TOKEN1`, and `FINDMIND_GMAIL_TOKEN2`. Non-empty tokens rotate per request. The `.env` file is ignored and must not be committed.

## Type 16 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type16.py --stock-id 2330 --company-name 台積電 --start-date 2020-01-01 --end-date 2026-12-31 --output financial/type16/raw_fin_ratio_quarter_2330.csv

## Type 14 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type14.py --stock-id 2330 --company-name 台積電 --daily-csv /path/to/raw_margin_daily.csv --output financial/type14/raw_margin_weekly_2330.csv

## Type 15 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type15.py --stock-id 2330 --company-name 台積電 --daily-csv /path/to/raw_margin_daily.csv --output financial/type15/raw_margin_monthly_2330.csv

## Type 1 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type1.py --stock-id 2330 --company-name 台積電 --start-date 2018-01-01 --end-date 2026-12-31 --output financial/type1/raw_dividends_2330.csv

## Type 5 example

    python3 skills/skill-finmind-fetch/scripts/fetch_type5.py --stock-id 2330 --company-name 台積電 --start-date 2021-01-01 --end-date 2026-12-31 --output financial/type5/raw_revenue_2330.csv

## Type 17 example

    python3 skills/skill-finmind-fetch/scripts/fetch_k_chart_flow.py --type 17 --stock-id 2330 --company-name 台積電 --start-date 2021-01-01 --end-date 2026-12-31 --output financial/type17/raw_weekly_k_chart_flow_2330.csv
