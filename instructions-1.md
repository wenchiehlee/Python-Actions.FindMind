# FindMind-fetch_and_save_stock_data.py 指南

## Version 1.0.2
{Guideline version}

## Way of working
0. `FindMind-fetch_and_save_stock_data.py` is genrated based on 指南 with {Guideline version}
0.1 its version is {Guideline version}.{increased sub version} which range from 0-500 and increase if any modification of code.
1. python FindMind-fetch_and_save_stock_data.py > output.log 2>&1


## 程式功能
開發一個 Python 腳本，用於從 FinMind API 獲取台灣股票數據，並將數據保存為 CSV 文件。

## 基本要求
1. 使用 Python 3.x 開發
2. 使用 UTF-8 編碼確保中文支持
3. 確保在 Windows/ Ubuntu 環境中正常運行

## 依賴庫
- pandas: 數據處理
- requests: API 請求
- csv: CSV 文件操作
- os: 文件系統操作
- datetime: 日期時間處理
- dotenv: 環境變量加載

## 主要功能
1. 從 Google Sheets 下載股票列表
2. 提取每隻股票的各類數據:
   - 股價數據 (TaiwanStockPrice)
   - 本益比/淨值比數據 (TaiwanStockPER)
   - 公司基本資料 (TaiwanStockInfo)
   - 股息數據 (TaiwanStockDividend)
   - 財務報表數據 (TaiwanStockFinancialStatements)
3. 檢查是否已有最新數據以避免重複請求
4. 將數據保存到對應的目錄和 CSV 文件中

## 核心函數實現

### 1. 數據檢查功能
實現 `is_file_complete_with_end_date` 函數，檢查文件是否已包含結束日期的數據:
- 檢查文件是否存在
- 使用 pandas 讀取 CSV 
- 檢查日期列中是否包含結束日期
- 適當處理異常

### 2. 股票數據獲取函數
為每種數據類型實現一個獲取函數:
- `fetch_and_save_stock_data`: 股價數據
- `fetch_and_save_stock_PER_PBR`: 本益比/淨值比數據
- `fetch_and_save_stock_company_profile`: 公司基本資料
- `fetch_and_save_stock_dividend`: 股息數據
- `fetch_and_save_stock_financialstatements`: 財務報表數據

每個函數需要:
- 先檢查數據是否已存在
- 構建 API 請求參數
- 發送 HTTP 請求
- 處理 API 響應
- 保存數據到 CSV
- 完善的錯誤處理

### 3. Google Sheets 下載功能
實現 `download_google_sheet` 和 `validate_saved_file` 函數，用於:
- 從 Google Sheets 公開連結下載 CSV
- 驗證下載文件的完整性
- 使用 UTF-8 編碼保存

### 4. CSV 處理功能
實現 `validate_and_process_csv` 函數，用於:
- 使用 pandas 讀取和處理 CSV
- 清理和驗證數據
- 保存處理後的數據

### 5. 主函數
實現 `main` 函數:
* 5.1 從環境變量加載 API 令牌
* 5.2 下載和處理 Google Sheets 數據
* 5.3 處理前 20 行數據 (if I modify to 0, then limit is removed..)
* 5.4 跟踪處理進度
* 5.5 完善的錯誤處理

## 文件保存規則
依照以下規則保存數據文件:
- 股價數據: `[股票代碼] 開始日期-結束日期.csv`
- 本益比/淨值比: `PER_PBR/[股票代碼] 開始日期-結束日期-PER_PBR.csv`
- 公司資料: `company-profile/[股票代碼] 開始日期-結束日期-company-profile.csv`
- 股息數據: `dividend/[股票代碼] 開始日期-結束日期-dividend.csv`
- 財務數據: `financial/[股票代碼] 開始日期-結束日期-financial.csv`

## 執行流程
1. 加載環境變量和 API 令牌
2. 下載並處理股票列表
3. 逐行處理股票數據（最多 20 行）
4. 對每個有效的股票:
   - 格式化開始和結束日期
   - 檢查並獲取五種數據類型
   - 跳過無效的數據行
5. 提供詳細的進度和錯誤日誌

## 安全考量
- 使用 .env 文件保存 API 令牌
- 使用 dotenv 安全加載環境變量
- 適當的錯誤處理以避免暴露敏感信�