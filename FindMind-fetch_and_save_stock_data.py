#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
FindMind-fetch_and_save_stock_data.py
Version 1.0.1.2
根據 指南 version 1.0.1 生成

從 FinMind API 獲取台灣股票數據並保存為 CSV 文件
"""
import pandas as pd
import requests
import csv
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Force UTF-8 encoding for Python in Windows
os.environ["PYTHONIOENCODING"] = "utf-8"

# Load secret .env file
load_dotenv()

def is_file_complete_with_end_date(output_file, end_date):
    """檢查文件是否已存在並包含結束日期的數據"""
    if not os.path.exists(output_file):
        return False
    
    try:
        # 讀取 CSV 文件
        df = pd.read_csv(output_file, encoding="utf-8")
        
        # 檢查是否為空文件或不包含日期列
        if df.empty or "日期" not in df.columns:
            return False
        
        # 檢查是否包含結束日期數據
        if end_date in df["日期"].values:
            print(f"文件 {output_file} 已包含結束日期 {end_date} 的數據，跳過 API 請求")
            return True
        
        return False
    except Exception as e:
        print(f"檢查文件時發生錯誤: {e}")
        return False

def fetch_and_save_TWSE_TPEX(api_token, start_date, end_date, output_file):
    """
    獲取並保存台灣證券交易所(TWSE)和證券櫃檯買賣中心(TPEX)的指數數據
    
    Parameters:
    - api_token: FinMind API 令牌
    - start_date: 開始日期 (格式: YYYY-MM-DD)
    - end_date: 結束日期 (格式: YYYY-MM-DD)
    - output_file: 輸出 CSV 文件路徑
    """
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已經包含結束日期
    if is_file_complete_with_end_date(output_file, end_date):
        return

    # 市場對應的數據集和索引識別符
    markets = {
        "TWSE": {"dataset": "TaiwanStockPrice", "data_id": "TAIEX"},
        "TPEX": {"dataset": "TaiwanStockPrice", "data_id": "TPEx"}
    }

    # 存儲每個市場的數據
    market_data = {}

    for market_name, market_info in markets.items():
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {
            "dataset": market_info["dataset"],
            "data_id": market_info["data_id"],
            "start_date": start_date,
            "end_date": end_date,
            "token": api_token
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("msg") != "success":
                print(f"Error for {market_name}: {data.get('msg', 'Unknown error')}")
                continue

            records = data.get("data", [])
            if not records:
                print(f"No data returned for {market_name}.")
                continue

            # 按日期建立字典，方便後續合併
            market_data[market_name] = {
                record['date']: {
                    '收盤指數': record.get('close'),
                    '開盤價': record.get('open'),
                    '最高價': record.get('max'),
                    '最低價': record.get('min'),
                    '漲跌點數': record.get('spread'),
                    '漲跌幅': record.get('spread_ratio')
                }
                for record in records
            }

        except requests.RequestException as e:
            print(f"HTTP Request error for {market_name}: {e}")
            continue
        except ValueError as e:
            print(f"Error processing response for {market_name}: {e}")
            continue

    # 如果沒有任何數據，則返回
    if not market_data:
        print("No data retrieved for either TWSE or TPEX.")
        return

    # 找出所有日期的聯合集
    all_dates = sorted(set.union(*[set(market.keys()) for market in market_data.values()]))

    # 寫入 CSV 文件
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        
        # 寫入標題行
        writer.writerow([
            "日期", 
            "TWSE收盤指數", "TWSE開盤價", "TWSE最高價", "TWSE最低價", "TWSE漲跌點數", "TWSE漲跌幅",
            "TPEX收盤指數", "TPEX開盤價", "TPEX最高價", "TPEX最低價", "TPEX漲跌點數", "TPEX漲跌幅"
        ])
        
        # 寫入數據
        for date in all_dates:
            row = [date]
            
            # 添加 TWSE 數據，如果該日期沒有數據則填充 None
            for key in ['收盤指數', '開盤價', '最高價', '最低價', '漲跌點數', '漲跌幅']:
                row.append(market_data.get('TWSE', {}).get(date, {}).get(key, None))
            
            # 添加 TPEX 數據，如果該日期沒有數據則填充 None
            for key in ['收盤指數', '開盤價', '最高價', '最低價', '漲跌點數', '漲跌幅']:
                row.append(market_data.get('TPEX', {}).get(date, {}).get(key, None))
            
            writer.writerow(row)

    print(f"Data successfully written to {output_file}")

def is_date_within_two_months(check_date, reference_date=None):
    """
    Check if the given date is within 2 months before or after the reference date
    
    Args:
        check_date (str): Date to check in YYYY-MM-DD format
        reference_date (str, optional): Reference date. Defaults to current date.
    
    Returns:
        bool: True if date is within 2 months, False otherwise
    """
    try:
        # Convert dates to datetime objects
        check = datetime.strptime(check_date, "%Y-%m-%d")
        
        # Use current date if no reference date provided
        if reference_date is None:
            reference = datetime.now()
        else:
            reference = datetime.strptime(reference_date, "%Y-%m-%d")
        
        # Calculate the date range (2 months before and after)
        two_months_before = reference - timedelta(days=60)
        two_months_after = reference + timedelta(days=60)
        
        # Check if the date is within the range
        return two_months_before <= check <= two_months_after
    
    except ValueError:
        print(f"Invalid date format: {check_date}")
        return False

def fetch_and_save_stock_financialstatements(api_token, stock_id, start_date, end_date, output_file):
    """獲取並保存財務報表數據"""
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已存在
    if os.path.exists(output_file):
        # 如果提供了結束日期，檢查是否在有效期內
        if end_date:
            # 讀取現有文件
            try:
                df = pd.read_csv(output_file, encoding="utf-8")
                
                # 檢查文件是否為空或不包含必要列
                if df.empty or "股票代碼" not in df.columns or "日期" not in df.columns:
                    print(f"文件 {output_file} 為空或格式不正確，將重新獲取數據")
                else:
                    # 檢查日期範圍
                    if not is_date_within_two_months(end_date):
                        print(f"文件 {output_file} 已包含股票代碼 {stock_id} 的數據，且開標日期非在今兩個月範圍內，跳過 API 請求")
                        return True
            
            except Exception as e:
                print(f"檢查financial文件時發生錯誤: {e}")

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockFinancialStatements",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": api_token
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("msg") != "success":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            return

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters on financialstatements.")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
       
            writer.writerow(["日期", "股票代碼", "類型", "值", "名稱"])
            for record in records:
                writer.writerow([
                    record.get("date"), record.get("stock_id"), record.get("type"),
                    record.get("value"), record.get("origin_name")
                ])
        print(f"Data successfully written to {output_file}")
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
    except ValueError as e:
        print(f"Error processing response: {e}")

def fetch_and_save_stock_company_profile(api_token, stock_id, output_file, end_date=None):
    """
    獲取並保存公司基本資料
    
    Parameters:
    - api_token: FinMind API 令牌
    - stock_id: 股票代碼
    - output_file: 輸出 CSV 文件路徑
    - end_date: 結束日期 (可選，用於日期範圍檢查)
    """
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return False
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已存在
    if os.path.exists(output_file):
        # 如果提供了結束日期，檢查是否在有效期內
        if end_date:
            # 讀取現有文件
            try:
                df = pd.read_csv(output_file, encoding="utf-8")
                
                # 檢查文件是否為空或不包含必要列
                if df.empty or "股票代碼" not in df.columns or "日期" not in df.columns:
                    print(f"文件 {output_file} 為空或格式不正確，將重新獲取數據")
                else:
                    # 檢查日期範圍
                    if not is_date_within_two_months(end_date):
                        print(f"文件 {output_file} 已包含股票代碼 {stock_id} 的數據，且開標日期非在今兩個月範圍內，跳過 API 請求")
                        return True
            
            except Exception as e:
                print(f"檢查公司概況文件時發生錯誤: {e}")
    
    # 發送 API 請求獲取公司基本資料
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockInfo",
        "data_id": stock_id,
        "token": api_token
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("msg") != "success":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            return False

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters on company_profile.")
            return False

        # 寫入 CSV 文件
        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
       
            writer.writerow(["行業類別", "股票代碼", "股票名稱", "類型", "日期"])
            for record in records:
                writer.writerow([
                    record.get("industry_category"), record.get("stock_id"), record.get("stock_name"),
                    record.get("type"), record.get("date")
                ])
        
        print(f"Data successfully written to {output_file}")
        return True
    
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
        return False
    except ValueError as e:
        print(f"Error processing response: {e}")
        return False

def fetch_and_save_stock_dividend(api_token, stock_id, start_date, end_date, output_file):
    """獲取並保存股息數據"""
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已經包含結束日期
    if is_file_complete_with_end_date(output_file, end_date):
        return

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockDividend",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": api_token
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("msg") != "success":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            return

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters on dividend.")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
       
            
            writer.writerow(["日期", "股票代碼", "年", "股票收益分配", "股票法定盈餘","股票除息交易日","員工股票股利額","員工股票股利總額","員工股票紅利佔總股本比例","員工股票股利比例","現金盈餘分配","現金法定盈餘","現金除息交易日","現金股利支付日","員工現金紅利總額","現金資本增加總數","現金增加認購利率","現金增加認購價","董事、監事報酬","參與分配股份總數","公告日期","公告時間"])
            for record in records:
                writer.writerow([
                    record.get("date"), record.get("stock_id"), record.get("year"),
                    record.get("StockEarningsDistribution"), record.get("StockStatutorySurplus"),record.get("StockExDividendTradingDate"),record.get("TotalEmployeeStockDividend"),
                    record.get("TotalEmployeeStockDividendAmount"),record.get("RatioOfEmployeeStockDividendOfTotal"),record.get("RatioOfEmployeeStockDividend"),record.get("CashEarningsDistribution"),
                    record.get("CashStatutorySurplus"),record.get("CashExDividendTradingDate"), record.get("CashDividendPaymentDate"),record.get("TotalEmployeeCashDividend"),
                    record.get("TotalNumberOfCashCapitalIncrease"),record.get("CashIncreaseSubscriptionRate"), record.get("CashIncreaseSubscriptionpRrice"),record.get("RemunerationOfDirectorsAndSupervisors"),
                    record.get("ParticipateDistributionOfTotalShares"),record.get("AnnouncementDate"), record.get("AnnouncementTime")
                ])
        print(f"Data successfully written to {output_file}")
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
    except ValueError as e:
        print(f"Error processing response: {e}")

def fetch_and_save_stock_PER_PBR(api_token, stock_id, start_date, end_date, output_file):
    """獲取並保存本益比/淨值比數據"""
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已經包含結束日期
    if is_file_complete_with_end_date(output_file, end_date):
        return

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPER",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": api_token
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("msg") != "success":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            return

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters on PER_PBR.")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
       
            writer.writerow(["日期", "股票代碼", "股息殖利率", "PER", "PBR"])
            for record in records:
                writer.writerow([
                    record.get("date"), record.get("stock_id"), record.get("dividend_yield"),
                    record.get("PER"), record.get("PBR")
                ])
        print(f"Data successfully written to {output_file}")
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
    except ValueError as e:
        print(f"Error processing response: {e}")

def fetch_and_save_stock_data(api_token, stock_id, start_date, end_date, output_file):
    """獲取並保存股價數據"""
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 檢查文件是否已經包含結束日期
    if is_file_complete_with_end_date(output_file, end_date):
        return

    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": api_token
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("msg") != "success":
            print(f"Error: {data.get('msg', 'Unknown error')}")
            return

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters.")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["日期", "股票代碼", "成交量", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "漲跌幅", "交易筆數"])
            for record in records:
                writer.writerow([
                    record.get("date"), record.get("stock_id"), record.get("Trading_Volume"),
                    record.get("Trading_money"), record.get("open"), record.get("max"),
                    record.get("min"), record.get("close"), record.get("spread"),
                    record.get("Trading_turnover")
                ])
        print(f"Data successfully written to {output_file}")
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
    except ValueError as e:
        print(f"Error processing response: {e}")

def download_google_sheet(url, output_file):
    """從 Google Sheets 下載數據"""
    response = requests.get(url)
    response.raise_for_status()

    # Decode raw content explicitly as UTF-8
    raw_text = response.content.decode('utf-8')
    #print("Raw Response Sample (First 500 Chars):")
    #print(raw_text[:500])

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(raw_text)
    print(f"Downloaded Google Sheet to {output_file}")

    validate_saved_file(output_file)

def validate_saved_file(file_path):
    """驗證保存的文件"""
    try:
        with open(file_path, 'rb') as f:
            raw_content = f.read(500)
        #print("Saved File Content Sample (Bytes):", raw_content[:100])
    except Exception as e:
        print(f"Error reading saved file: {e}")

def validate_and_process_csv(file_path):
    """驗證和處理 CSV 文件"""
    try:
        data = pd.read_csv(file_path, encoding="utf-8")
        #print("Columns:", data.columns.tolist())
        #print("First 5 rows:")
        #print(data.head())

        data.to_csv("cleaned_auction_data.csv", index=False, encoding="utf-8")
        print("Saved cleaned data to 'cleaned_auction_data.csv'.")
        return data
    except Exception as e:
        print(f"Error reading or processing CSV file: {e}")
        return None

def main():
    """主函數，程序入口點"""
    api_token = os.getenv("FINDMIND_GMAIL_TOKEN")
    if not api_token:
        print("Error: API token is not set in environment variables.")
        return

    print("API Token loaded successfully.")

    sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSINLlSv4NcCszvA5XOPsuYCxZEk9_tBnhgLvyDkcG73QgFObITFtaZRQ492wlS53NPBlQi0AfPHMVh/pub?gid=1407177187&single=true&output=csv"
    csv_file = "auction_data.csv"

    # 創建必要的目錄
    os.makedirs("stockdata", exist_ok=True)
    os.makedirs("PER_PBR", exist_ok=True)
    os.makedirs("company-profile", exist_ok=True)
    os.makedirs("dividend", exist_ok=True)
    os.makedirs("financial", exist_ok=True)
    os.makedirs("TWSE_TPEX", exist_ok=True)

    download_google_sheet(sheet_url, csv_file)
    data = validate_and_process_csv(csv_file)

    if data is None:
        return

    # 設定處理行數限制
    max_rows = 0  # 默認處理前20行
    
    # 如果 max_rows 設為 0，則處理所有行
    if max_rows == 0:
        print("No row limit set. Processing all rows from the CSV file")
        max_rows = len(data)
    else:
        print(f"Processing only the first {max_rows} rows from the CSV file")
    
    row_count = 0
    
    for index, row in data.iterrows():
        # 判斷是否已經處理了設定的行數
        if row_count >= max_rows:
            print(f"Reached the limit of {max_rows} rows to process. Stopping.")
            break
            
        stock_id = row.get("股票代號")
        date_start = row.get("DateStart")
        date_end = row.get("DateEnd")

        if pd.notna(stock_id) and pd.notna(date_start) and pd.notna(date_end):
            try:
                start_date = datetime.strptime(date_start, "%Y/%m/%d").strftime("%Y-%m-%d")
                end_date = datetime.strptime(date_end, "%Y/%m/%d").strftime("%Y-%m-%d")
                
                print(f"Processing stock {stock_id} for period {start_date} to {end_date}")
                
                # 股價數據 (直接保存在根目錄)
                file_name = f"stockdata/[{stock_id}] {start_date}-{end_date}.csv"
                fetch_and_save_stock_data(api_token, stock_id, start_date, end_date, file_name)

                # 股息數據
                file_name = f"dividend/[{stock_id}] {start_date}-{end_date}-dividend.csv"
                fetch_and_save_stock_dividend(api_token, stock_id, start_date, end_date, file_name)
                
                # 本益比/淨值比數據
                file_name = f"PER_PBR/[{stock_id}] {start_date}-{end_date}-PER_PBR.csv"
                fetch_and_save_stock_PER_PBR(api_token, stock_id, start_date, end_date, file_name)
                
                # 公司基本資料
                file_name = f"company-profile/[{stock_id}] {start_date}-{end_date}-company-profile.csv"
                fetch_and_save_stock_company_profile(api_token, stock_id, file_name, end_date)
                                
                # 財務報表數據
                file_name = f"financial/[{stock_id}] {start_date}-{end_date}-financial.csv"
                fetch_and_save_stock_financialstatements(api_token, stock_id, start_date, end_date, file_name)

                # TWSE 和 TPEX 數據
                file_name = f"TWSE_TPEX/[{stock_id}] {start_date}-{end_date}-TWSE_TPEX.csv"
                fetch_and_save_TWSE_TPEX(api_token, start_date, end_date, file_name)
                
                # 增加已處理行計數
                row_count += 1
                
                # 顯示進度
                if max_rows < len(data):
                    print(f"-----Processed row {row_count} of {max_rows} limit-----")
                else:
                    print(f"-----Processed row {row_count} of {len(data)}-----")
                
            except Exception as e:
                print(f"Error processing stock {stock_id}: {e}")
                # 儘管發生錯誤，仍計入處理的行數
                row_count += 1
        else:
            print(f"Skipping row {index} id={stock_id}: Invalid stock_id or dates. Row data: {row}")
            # 跳過的行也計入處理的行數
            row_count += 1

    print("Processing completed.")

if __name__ == "__main__":
    main()
