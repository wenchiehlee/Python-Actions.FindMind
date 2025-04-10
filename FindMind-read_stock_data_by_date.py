import os
import pandas as pd
import re
from workalendar.asia import Taiwan  # 使用 workalendar 計算台灣的工作日
import csv
from datetime import timedelta

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

# 1. 動態擷取 DateStart 和 DateEnd 之間的欄位，並標記偏移量
columns = auction_data.columns.tolist()
start_index = columns.index("DateStart")
end_index = columns.index("DateEnd")


date_columns_raw = columns[start_index:end_index + 1]

# 修正：正確辨識偏移格式，並檢查基礎欄位是否存在
date_columns = {}
for col in date_columns_raw:
    match = re.match(r"(.+?)([+-]\d+)$", col.strip())
    
    if match:
        base_name = match.group(1).strip()
        offset = int(match.group(2))
        
        # 檢查基礎欄位是否存在於資料中
        if base_name in auction_data.columns:
            date_columns[col] = {'base': base_name, 'offset': offset}
        else:
            # 若基礎欄位不存在，視為完整欄位名稱，無偏移
            date_columns[col] = {'base': col, 'offset': 0}
    else:
        date_columns[col] = {'base': col, 'offset': 0}
print(date_columns,"<<AAAAAAAAAAAAAAA")


# 獲取所有文件列表
all_files = [f for f in os.listdir('stockdata') if f.endswith('.csv')]

# 初始化台灣工作日計算
cal = Taiwan()

# 讀取 holidays.csv，並處理格式
holidays_path = "holidays.csv"
if os.path.exists(holidays_path):
    try:
        holidays_list = []
        with open(holidays_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                date_part = row[0].strip() if len(row) > 0 else None
                if date_part:
                    holidays_list.append(date_part)

        holidays = pd.DataFrame(holidays_list, columns=["日期"])
        holidays["日期"] = holidays["日期"].str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
        holidays["日期"] = pd.to_datetime(holidays["日期"], errors="coerce").dt.date
        holidays_set = set(holidays["日期"].dropna())

        print(f"成功讀取 holidays.csv，共 {len(holidays_set)} 個假日")
    except Exception as e:
        print(f"讀取 holidays.csv 時發生錯誤: {e}")
        holidays_set = set()
else:
    print("找不到 holidays.csv，將不考慮假日")
    holidays_set = set()

# 2. 定義函數以獲取收盤價，並根據偏移量調整
def get_closing_price(security_id, base_date, offset=0):
    """
    根據證券代號和日期獲取收盤價，並根據偏移量調整資料位置。
    """
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # Update file path to use stockdata directory
                full_path = os.path.join('stockdata', file_name)
                price_data = pd.read_csv(full_path, encoding='utf-8')
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                price_data = price_data.sort_values(by='日期').reset_index(drop=True)

                # Convert base_date to datetime.date object
                base_date_dt = pd.to_datetime(base_date, errors='coerce')
                if pd.isna(base_date_dt):
                    print(f"無效日期格式: base_date={base_date}, offset={offset}")
                    return ""
                
                base_date = base_date_dt.date()
                
                # Try to find the exact base date in the data
                base_idx = price_data[price_data['日期'] == base_date].index
                
                if not base_idx.empty:
                    # Base date found, apply offset
                    target_idx = base_idx[0] + offset
                    if 0 <= target_idx < len(price_data):
                        return price_data.iloc[target_idx]['收盤價']
                    else:
                        print(f"超出範圍: base_date={base_date}, offset={offset}, 目標索引={target_idx}, 資料長度={len(price_data)}")
                        return ""
                else:
                    # Base date not found in data
                    print(f"日期不存在: base_date={base_date}, offset={offset}, 檔案={file_name}")
                    # Check if this date should exist (is it within the file's date range?)
                    min_date = price_data['日期'].min()
                    max_date = price_data['日期'].max()
                    if min_date <= base_date <= max_date:
                        print(f"  注意: 此日期在檔案日期範圍內 ({min_date} 至 {max_date})，但沒有數據 (可能是非交易日)")
                    return ""
                
            except (KeyError, FileNotFoundError, pd.errors.EmptyDataError) as e:
                print(f"處理檔案時出錯: {file_name}, 錯誤: {e}")
                continue
    
    print(f"無資料: security_id={security_id}, base_date={base_date}, offset={offset}")
    return ""

# 3. 計算資料總數與總工作天數
def get_security_stats(security_id):
    """
    計算資料總數與總工作天數
    """
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # Update file path to use stockdata directory
                full_path = os.path.join('stockdata', file_name)
                price_data = pd.read_csv(full_path, encoding='utf-8')

                total_rows = price_data.shape