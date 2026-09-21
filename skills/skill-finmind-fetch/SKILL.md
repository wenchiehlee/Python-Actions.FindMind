---
name: skill-finmind-fetch
description: 從 FinMind API 獲取台灣股市個股與大盤指數的融資融券以及收盤價資料，並格式化/合併寫入與 GoodInfo 結構相同的 stage1 raw CSV 中。
---

# FinMind Fetch Skill (FinMind 資料抓取與合併技能)

此技能利用 FinMind API，獲取台灣股市個股與大盤的每日收盤價與融資融券數據。它會將獲取的資料整理為與 `raw_margin_daily.csv` (Type 13: ShowMarginChart) 相同的 31 個欄位格式；另可由 Type 13 每日 CSV 聚合 `raw_margin_weekly.csv` (Type 14: ShowMarginChartWeek)，並自動與現有資料進行增量合併（Incremental Update）及去重。欄位排序與定義完全遵循 [raw_column_definition_Analyzer.md](file:///C:/Users/WJLEE/SynologyDrive/NAS/github.com/Python-Actions.GoodInfo.Analyzer/definitions/raw_column_definition_Analyzer.md) 規格。

## 適用場景

- 需要透過穩定的 API 管道（FinMind）獲取每日融資融券數據，以避開 GoodInfo 網頁強大的反爬蟲機制。
- 需要更新 `Python-Actions.GoodInfo.Analyzer` 專案中的 `data\stage1_raw\raw_margin_daily.csv`，使後續的籌碼分析管道（如 `margin_daily_report.py`）能使用最新資料。

## 依賴需求

- `pandas`
- `requests`
- `numpy`
- 可設定 `FINMIND_TOKEN`、`FINMIND_API_TOKEN`、`FINDMIND_GMAIL_TOKEN`、`FINDMIND_GMAIL_TOKEN1`、`FINDMIND_GMAIL_TOKEN2`；所有非空 token 會按 request round-robin rotation 使用。

## 核心腳本與指令

技能的執行腳本位於技能目錄下的 `scripts/fetch_to_csv.py`。

### 1. 增量更新（推薦）

讀取現有 CSV，分析每檔股票（與大盤）在 CSV 中的最新日期，並只向 API 請求最新日期之後的資料，追加並合併寫回：

```bash
python scripts/fetch_to_csv.py --input-csv "/path/to/raw_margin_daily.csv" --stock-list "/path/to/StockID_TWSE_TPEX.csv"
```

### 2. 獲取特定範圍與股票（全量或指定更新）

```bash
python scripts/fetch_to_csv.py --stocks "0000,2330,0050" --start-date "2026-07-01" --end-date "2026-07-10" --output-csv "/path/to/output.csv"
```

## 參數說明

- `--input-csv`：現有 `raw_margin_daily.csv` 檔案的路徑。若提供此參數，程式會自動以此進行增量更新。
- `--stock-list`：包含股票代號與名稱的 CSV 路徑（格式：`代號,名稱`）。若不指定 `--stocks`，則以此列表中的股票為更新目標。
- `--stocks`：以逗號分隔的股票代碼字串（例如 `0000,2330`）。會覆蓋 `--stock-list`。大盤代碼為 `0000`。
- `--start-date`：手動指定的起始日期 (YYYY-MM-DD)。
- `--end-date`：手動指定的結束日期 (YYYY-MM-DD)，預設為今天。
- `--output-csv`：輸出的 CSV 儲存路徑，若未指定則預設覆寫 `--input-csv` 的檔案。
- `--token`：手動指定的 FinMind API Token。
- `--debug-limit`：除大盤外，限制只下載前 N 檔個股，供測試使用。


## Type 1：股利政策 CSV

```bash
python skills/skill-finmind-fetch/scripts/fetch_type1.py \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2018-01-01 --end-date 2026-12-31 \
  --output financial/type1/raw_dividends_2330.csv
```

Type 1 使用 FinMind `TaiwanStockDividend`；GoodInfo 特有的填息天數與多種歷史殖利率欄位，若來源沒有提供則保留空值。

## Type 5：每月營收 CSV

```bash
python skills/skill-finmind-fetch/scripts/fetch_type5.py \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2021-01-01 --end-date 2026-12-31 \
  --output financial/type5/raw_revenue_2330.csv
```

Type 5 使用 FinMind `TaiwanStockMonthRevenue` 搭配 `TaiwanStockPrice`，輸出月營收、月增/年增、年度累計與月內價格欄位。

## Type 8、12、17、18：K 線 / PER 資金流向 CSV

四個類型共用 `fetch_k_chart_flow.py`，差異只在頻率與目標 PER 倍數：

```bash
python skills/skill-finmind-fetch/scripts/fetch_k_chart_flow.py --type 17 \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2021-01-01 --end-date 2026-12-31 \
  --output financial/type17/raw_weekly_k_chart_flow_2330.csv
```

`--type` 可使用 `8`（週）、`12`（月）、`17`（週）、`18`（日）。資料使用 FinMind `TaiwanStockPrice`、`TaiwanStockPER` 與可用的季度 EPS；來源缺少的欄位保留空值。

## Type 14：每週融資融券 CSV

Type 14 優先重用 Type 13 每日 CSV 聚合，不需重新下載同一批每日資料：

```bash
python skills/skill-finmind-fetch/scripts/fetch_type14.py \
  --stock-id 2330 --company-name 台積電 \
  --daily-csv /path/to/raw_margin_daily.csv \
  --output financial/type14/raw_margin_weekly_2330.csv
```

若沒有 Type 13 CSV，省略 `--daily-csv`，腳本會使用 FinMind 每日價格與融資融券 API 建立資料：

```bash
python skills/skill-finmind-fetch/scripts/fetch_type14.py \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2021-01-01 --end-date 2026-12-31 \
  --output financial/type14/raw_margin_weekly_2330.csv
```

用 `compare_type14.py` 可與 Analyzer 的 `raw_margin_weekly.csv` 做欄位及數值比對。

## Type 15：每月融資融券 CSV

Type 15 優先重用 Type 13 每日 CSV 聚合，並將張數轉為千張：

```bash
python skills/skill-finmind-fetch/scripts/fetch_type15.py \
  --stock-id 2330 --company-name 台積電 \
  --daily-csv /path/to/raw_margin_daily.csv \
  --output financial/type15/raw_margin_monthly_2330.csv
```

若沒有 Type 13 CSV，省略 `--daily-csv`，腳本會使用 FinMind 每日 API：

```bash
python skills/skill-finmind-fetch/scripts/fetch_type15.py \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2021-01-01 --end-date 2026-12-31 \
  --output financial/type15/raw_margin_monthly_2330.csv
```

用 `compare_type15.py` 可與 Analyzer 的 `raw_margin_monthly.csv` 做欄位及數值比對。

## Type 16：季度財務比率 CSV

同一個 skill 也提供季度財務比率 adapter，使用 FinMind 的綜合損益、資產負債表與現金流量表，輸出與 GoodInfo Analyzer 的 `raw_fin_ratio_quarter.csv` 相同的 164 欄 schema：

```bash
python skills/skill-finmind-fetch/scripts/fetch_type16.py \
  --stock-id 2330 --company-name 台積電 \
  --start-date 2020-01-01 --end-date 2026-12-31 \
  --output financial/type16/raw_fin_ratio_quarter_2330.csv
```

用 `compare_type16.py` 可對照 Analyzer 的 GoodInfo CSV。Type 13 與 Type 16 共用同一個 skill，但使用不同 script、dataset 與輸出 schema，不保留第二份實作。


## Parity 驗證

- Type 13：`python skills/skill-finmind-fetch/scripts/compare_type13.py <finmind.csv> <analyzer/raw_margin_daily.csv> --stock-id 2330`
- Type 14：`python skills/skill-finmind-fetch/scripts/compare_type14.py --candidate <finmind.csv> --reference <analyzer/raw_margin_weekly.csv> --stock-id 2330`
- Type 15：`python skills/skill-finmind-fetch/scripts/compare_type15.py --candidate <finmind.csv> --reference <analyzer/raw_margin_monthly.csv> --stock-id 2330`
- Type 16：`python skills/skill-finmind-fetch/scripts/compare_type16.py <finmind.csv> <analyzer/raw_fin_ratio_quarter.csv> --stock-id 2330`

驗證器以數值比較 CSV，會分開報告來源缺少的欄位；不會把缺少的 FinMind 欄位填成假資料。
