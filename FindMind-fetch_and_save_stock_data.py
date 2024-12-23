#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pandas as pd
import requests
import csv
import os
from datetime import datetime
from dotenv import load_dotenv
import chardet  # For dynamic encoding detection

# Force UTF-8 encoding for Python in Windows
os.environ["PYTHONIOENCODING"] = "utf-8"

# Load secret .env file
load_dotenv()

def fetch_and_save_stock_data(api_token, dataset, stock_id, start_date, end_date, output_file):
    """
    Fetch stock data from FinMind API and save it to a CSV file.

    Parameters:
    - api_token (str): API token for authentication.
    - dataset (str): Dataset to query (e.g., "TaiwanStockPrice").
    - stock_id (str): Stock ID to query.
    - start_date (str): Start date for the data (YYYY-MM-DD).
    - end_date (str): End date for the data (YYYY-MM-DD).
    - output_file (str): Path to save the output CSV file.
    """
    # API URL
    url = "https://api.finmindtrade.com/api/v4/data"

    # Parameters for the API call
    params = {
        "dataset": dataset,
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": api_token
    }

    try:
        # Fetch data from FinMind API
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
            writer.writerow([
                "日期", "股票代碼", "成交量", "成交金額", 
                "開盤價", "最高價", "最低價", "收盤價", 
                "漲跌幅", "交易筆數"
            ])
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

# Detect encoding of a file
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

# Download Google Sheet and save as CSV file
def download_google_sheet(url, output_file):
    response = requests.get(url)
    response.raise_for_status()

    # Debugging content
    print("Response sample (first 500 chars):")
    print(response.text[:500])

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(response.text)
    print(f"Downloaded Google Sheet to {output_file}")

def main():
    api_token = os.getenv("FINDMIND_WENCHIEHLEE1020_GMAIL_TOKEN")
    if not api_token:
        print("Error: API token is not set in environment variables.")
        return

    print("API Token loaded successfully.")

    sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSINLlSv4NcCszvA5XOPsuYCxZEk9_tBnhgLvyDkcG73QgFObITFtaZRQ492wlS53NPBlQi0AfPHMVh/pub?output=csv"
    csv_file = "auction_data.csv"

    download_google_sheet(sheet_url, csv_file)

    try:
        data = pd.read_csv(csv_file, encoding="utf-8")

        # Debug raw content
        with open(csv_file, 'rb') as file:
            raw_content = file.read(500)
            print("Raw Content Sample (Bytes):", raw_content[:100])

        # Map garbled columns
        column_mapping = {
            "è­å¸ä»£è": "證券代號",
            "è­å¸åç¨±": "證券名稱",
            "ç³è«æ¥æ": "申請日期",
            "ä¸æ«å¯©è­°å§å¡æå¯©è­°æ¥æ": "上櫃審議委員會審議日期",
            "æ«è²·è£äºæééä¸æ«æ¥æ": "櫃買董事會通過上櫃日期",
            "æ«è²·åæä¸æ«å¥ç´æ¥ææè­æå±æ ¸åä¸æ«å¥ç´æ¥æ": "櫃買同意上櫃契約日期或證期局核准上櫃契約日期",
            "ææ¨éå§æ¥(T-4)": "投標開始日(T-4)",
            "ææ¨çµææ¥(T-2)": "投標結束日(T-2)",
            "éæ¨æ¥æ(T)": "開標日期(T)",
            "æ¥å¸æ¥(ä¸å¸ä¸æ«æ¥) T+7": "撥券日(上市上櫃日) T+7",
            "DateStart": "DateStart",
            "DateEnd+14": "DateEnd+14"
        }
        data.rename(columns=column_mapping, inplace=True)

        # Save cleaned data for verification
        data.to_csv("cleaned_auction_data.csv", index=False, encoding="utf-8")
        print("Saved cleaned data to 'cleaned_auction_data.csv'.")
    except Exception as e:
        print(f"Error reading or processing CSV file: {e}")
        return

    dataset = "TaiwanStockPrice"
    for index, row in data.iterrows():
        stock_id = row.get("證券代號")
        date_start = row.get("DateStart")
        date_end = row.get("DateEnd+14")

        if pd.notna(stock_id) and pd.notna(date_start) and pd.notna(date_end):
            try:
                start_date = datetime.strptime(date_start, "%Y/%m/%d").strftime("%Y-%m-%d")
                end_date = datetime.strptime(date_end, "%Y/%m/%d").strftime("%Y-%m-%d")
                file_name = f"[{stock_id}] {start_date}-{end_date}.csv"
                fetch_and_save_stock_data(api_token, dataset, stock_id, start_date, end_date, file_name)
                print(f"Saved {stock_id} data to {file_name}")
            except Exception as e:
                print(f"Error processing stock {stock_id}: {e}")
        else:
            print(f"Skipping row {index} id={stock_id}: Invalid stock_id or dates. Row data: {row}")

if __name__ == "__main__":
    main()
