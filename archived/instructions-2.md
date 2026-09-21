# FindMind-read_{xxxx}.py 指南

## Version 1.0.6
{Guideline version}

## Way of working
0. `FindMind-read_{xxxx}.py` is genrated based on guideline with {Guideline version}
0.1 its version is {Guideline version}.{increased sub version} which range from 0-500 and increase if any modification of code.
0.2 為每種數據類型實現一個獲取函數:
- `FindMind-read_PER_PBR`: 本益比/淨值比數據
      column defined as `日期`,`股票代號`,`股息殖利率`,`PER`,`PBR`
- `FindMind-read_company_profile`: 公司基本資料
      column defined as `行業類別`,`股票代號`,`股票名稱`,`類型`,`日期`
- `FindMind-read_dividend`: 股息數據
      column defined as `日期`, `股票代碼`, `年`, `股票收益分配`, `股票法定盈餘`, `股票除息交易日`, `員工股票股利額`, `員工股票股利總額`, `員工股票紅利佔總股本比例`, `員工股票股利比例`, `現金盈餘分配`, `現金法定盈餘`, `現金除息交易日`, `現金股利支付日`, `員工現金紅利總額`, `現金資本增加總數`, `現金增加認購利率`, `現金增加認購價`, `董事、監事報酬`, `參與分配股份總數`, `公告日期`, `公告時間`

<!-- - `FindMind-read_financial`: 財務報表數據 -->

command line usage
```
python FindMind-read_PER_PBR.py >output4.log 2>&1
python FindMind-read_company-profile.py >output5.log 2>&1
python FindMind-read_dividend.py >output6.log 2>&1
<!-- python FindMind-read_financial.py >output7.log 2>&1 -->

```

## 文件規則
<!-- - 股價數據: `[股票代號] 開始日期-結束日期.csv` -->
- 本益比/淨值比: `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv`
- 公司資料: `company-profile/[股票代號] YYYY-MM-DD-YYYY-MM-DD-company-profile.csv`
- 股息數據: `dividend/[股票代號] YYYY-MM-DD-YYYY-MM-DD-dividend.csv`
<!-- - 財務數據: `financial/[股票代號] YYYY-MM-DD-YYYY-MM-DD-financial.csv` -->

## 程式功能
開發一個 Python 腳本，read data from CSV files，並將數據保存為 CSV 文件。

## 基本要求
1. 使用 Python 3.x 開發
2. 使用 UTF-8 編碼確保中文支持
3. 確保在 Windows/ Ubuntu 環境中正常運行

## 主要功能 

### `FindMind-read_PER_PBR.py`

1.1. Read `https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv`. Its column is defined `股票代號` which is string type.

1.2. Write `auction_data_processed/Features-Company.csv` columns defined as `股票代號`,`股息殖利率`,`PER`,`PBR` as a new file.
   
   * 1.2.1 If `auction_data_processed/Features-Company.csv` exists, if the clumns are not exists, add new columns.
   * 1.2.2 if the clumns exists, refill the cells with updated values with the matched `股票代號` and fill cells of matched column and row
   where `股息殖利率` is 
      * 1.2.1 Empty - if file not avaiable or `股息殖利率` column of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` not valid
      * 1.2.2 Average value of `股息殖利率` column from all rows of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` (formatted to 1 decimal place)
   where `PER` is 
      * 1.2.3 Empty - if file not avaiable or `PER` column of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` not valid
      * 1.2.4 Average value of `PER` column from all rows of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` (formatted to 1 decimal place)
   where `PBR` is average value of `PBR` column from all rows of `PER_PBR/[股票代號] 開始日期-結束日期-PER_PBR.csv`
      * 1.2.5 Empty - if file not avaiable or `PBR` column of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` not valid
      * 1.2.6 Average value of `PBR` column from all rows of `PER_PBR/[股票代號] YYYY-MM-DD-YYYY-MM-DD-PER_PBR.csv` (formatted to 1 decimal place)
### `FindMind-read_company-profile.py`

2.1. Read `https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv`. Its column is defined `股票代號` which is string type.

2.2. Write `auction_data_processed/Features-Company.csv` columns defined as `股票代號`,`行業類別`,`類型`. 
   
   * 2.2.1 If `auction_data_processed/Features-Company.csv` exists, if the clumns are not exists, add new columns.
   * 2.2.2 if the clumns exists, refill the cells with updated values with the matched `股票代號` and fill cells of matched column and row
   where `行業類別` is 
      * 2.2.2.1 Empty - if file not avaiable or `行業類別` column of `company-profile/[股票代號] YYYY-MM-DD-YYYY-MM-DD-company-profile.csv` not valid
      * 2.2.2.2 newest date value of `行業類別` column from all rows of `company-profile/[股票代號] YYYY-MM-DD-YYYY-MM-DD-company-profile.csv`
   where `類型` is 
      * 2.2.2.3 Empty - if file not avaiable or `類型` column of `company-profile/[股票代號] YYYY-MM-DD-YYYY-MM-DD-company-profile.csv` not valid
      * 2.2.2.4 Average value of `類型` column from all rows of `company-profile/[股票代號] YYYY-MM-DD-YYYY-MM-DD-company-profile.csv`

### `FindMind-read_dividend.py`

3.1. Read `https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv`. Its column is defined `股票代號` which is string type.

3.2. Write `auction_data_processed/Features-Company.csv` columns defined as `股票代號`,`每股股利`. 
   
   * 3.2.1 If `auction_data_processed/Features-Company.csv` exists, if the clumns are not exists, add new columns.
   * 3.2.2 if the clumns exists, refill the cells with updated values with the matched `股票代號` and fill cells of matched column and row
   where `每股股利` is 
      * 3.2.2.1 Empty - if file not avaiable or `股票收益分配` and `現金盈餘分配` column of `dividend/[股票代號] YYYY-MM-DD-YYYY-MM-DD-dividend.csv` not valid
      * 3.2.2.2 `每股股利` = `股票收益分配` + `現金盈餘分配`. newest date value of `股票收益分配` and`現金盈餘分配` column from all rows of `dividend/[股票代號] YYYY-MM-DD-YYYY-MM-DD-dividend.csv`

## for all python code
1. 每個函數需要:
   - 先檢查數據是否已存在
   - 完善的錯誤處理
