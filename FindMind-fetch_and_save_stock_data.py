#!/usr/bin/python
# -*- coding: UTF-8 -*-
import pandas as pd
import requests
import csv  # Ensure the CSV module is imported
import os
from datetime import datetime
from dotenv import load_dotenv

# Force UTF-8 encoding for Python in Windows
os.environ["PYTHONIOENCODING"] = "utf-8"

# Load secret .env file
load_dotenv()

def fetch_and_save_stock_data(api_token, dataset, stock_id, start_date, end_date, output_file):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": dataset,
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
    try:
        with open(file_path, 'rb') as f:
            raw_content = f.read(500)
        #print("Saved File Content Sample (Bytes):", raw_content[:100])
    except Exception as e:
        print(f"Error reading saved file: {e}")

def validate_and_process_csv(file_path):
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
    api_token = os.getenv("FINDMIND_GMAIL_TOKEN")
    if not api_token:
        print("Error: API token is not set in environment variables.")
        return

    print("API Token loaded successfully.")

    sheet_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSINLlSv4NcCszvA5XOPsuYCxZEk9_tBnhgLvyDkcG73QgFObITFtaZRQ492wlS53NPBlQi0AfPHMVh/pub?gid=1407177187&single=true&output=csv"
    csv_file = "auction_data.csv"

    download_google_sheet(sheet_url, csv_file)
    data = validate_and_process_csv(csv_file)

    if data is None:
        return

    dataset = "TaiwanStockPrice"
    for index, row in data.iterrows():
        stock_id = row.get("股票代號")
        date_start = row.get("DateStart")
        date_end = row.get("DateEnd")

        if pd.notna(stock_id) and pd.notna(date_start) and pd.notna(date_end):
            try:
                start_date = datetime.strptime(date_start, "%Y/%m/%d").strftime("%Y-%m-%d")
                end_date = datetime.strptime(date_end, "%Y/%m/%d").strftime("%Y-%m-%d")
                file_name = f"[{stock_id}] {start_date}-{end_date}.csv"
                fetch_and_save_stock_data(api_token, dataset, stock_id, start_date, end_date, file_name)
                #print(f"Saved {stock_id} data to {file_name}")
            except Exception as e:
                print(f"Error processing stock {stock_id}: {e}")
        else:
            print(f"Skipping row {index} id={stock_id}: Invalid stock_id or dates. Row data: {row}")

if __name__ == "__main__":
    main()
