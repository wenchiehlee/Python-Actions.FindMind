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
from datetime import datetime
from dotenv import load_dotenv

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
    
    # 檢查文件是否已經包含結束日期
    if is_file_complete_with_end_date(output_file, end_date):
        return

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

def fetch_and_save_stock_company_profile(api_token, stock_id, output_file):
    """獲取並保存公司基本資料"""
    # 確保輸出文件路徑有效
    if not output_file:
        print("錯誤：輸出文件路徑無效")
        return
        
    # 確保目錄存在
    directory = os.path.dirname(output_file)
    if directory:  # 只有當目錄非空時才創建
        os.makedirs(directory, exist_ok=True)
    
    # 公司概況沒有指定日期，所以使用其他方法檢查
    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
        try:
            # 讀取 CSV 文件檢查是否已包含數據
            df = pd.read_csv(output_file, encoding="utf-8")
            if not df.empty and "股票代碼" in df.columns and stock_id in df["股票代碼"].values.astype(str):
                print(f"文件 {output_file} 已包含股票代碼 {stock_id} 的數據，跳過 API 請求")
                return
        except Exception as e:
            print(f"檢查公司概況文件時發生錯誤: {e}")

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
            return

        records = data.get("data", [])
        if not records:
            print("No data returned for the given parameters on company_profile.")
            return

        with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
       
            writer.writerow(["行業類別", "股票代碼", "股票名稱", "類型", "日期"])
            for record in records:
                writer.writerow([
                    record.get("industry_category"), record.get("stock_id"), record.get("stock_name"),
                    record.get("type"), record.get("date")
                ])
        print(f"Data successfully written to {output_file}")
    except requests.RequestException as e:
        print(f"HTTP Request error: {e}")
    except ValueError as e:
        print(f"Error processing response: {e}")

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
       
            writer.writerow(["日期", "股票代碼", "年", "股票收益分配", "股票法定盈餘","股票除息交易日"])
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
    os.makedirs("PER_PBR", exist_ok=True)
    os.makedirs("company-profile", exist_ok=True)
    os.makedirs("dividend", exist_ok=True)
    os.makedirs("financial", exist_ok=True)

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
                file_name = f"[{stock_id}] {start_date}-{end_date}.csv"
                fetch_and_save_stock_data(api_token, stock_id, start_date, end_date, file_name)

                # 股息數據
                file_name = f"dividend/[{stock_id}] {start_date}-{end_date}-dividend.csv"
                fetch_and_save_stock_dividend(api_token, stock_id, start_date, end_date, file_name)
                
                # 本益比/淨值比數據
                file_name = f"PER_PBR/[{stock_id}] {start_date}-{end_date}-PER_PBR.csv"
                fetch_and_save_stock_PER_PBR(api_token, stock_id, start_date, end_date, file_name)
                
                # 公司基本資料
                file_name = f"company-profile/[{stock_id}] {start_date}-{end_date}-company-profile.csv"
                fetch_and_save_stock_company_profile(api_token, stock_id, file_name)
                                
                # 財務報表數據
                file_name = f"financial/[{stock_id}] {start_date}-{end_date}-financial.csv"
                fetch_and_save_stock_financialstatements(api_token, stock_id, start_date, end_date, file_name)
                
                # 增加已處理行計數
                row_count += 1
                
                # 顯示進度
                if max_rows < len(data):
                    print(f"-----Processed row {row_count} of {max_rows} limit-----")
                else:
                    print(f"Processed row {row_count} of {len(data)}")
                
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