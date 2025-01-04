import os
import pandas as pd

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

# 定義需要處理的日期欄位，包含新增的 DateStart 和 DateEnd+14
date_columns = [
    "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
    "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
    "開標日期(T)", "撥券日(上市上櫃日) T+7", "DateStart", "DateEnd+14"
]

# 獲取所有文件列表
all_files = os.listdir()

# 定義一個函數以匹配代號相關的檔案並讀取收盤價
def get_closing_price(security_id, date):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            file_path = file_name
            try:
                price_data = pd.read_csv(file_path, encoding='utf-8')
                # 確保日期格式一致 (YYYY-MM-DD)
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                date_obj = pd.to_datetime(date, errors='coerce').date()
                
                # 搜尋當天資料
                for offset in range(0, 3):  # 試圖搜尋當天及往後1~2天
                    search_date = date_obj + pd.Timedelta(days=offset)
                    closing_price_row = price_data.loc[price_data['日期'] == search_date]
                    if not closing_price_row.empty:
                        return closing_price_row['收盤價'].values[0]
            except (KeyError, FileNotFoundError, pd.errors.EmptyDataError):
                continue
    return None

# 更新資料中的日期欄位
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    for column in date_columns:
        if pd.notna(row[column]):  # 確保日期欄位不為空
            closing_price = get_closing_price(security_id, row[column])
            if closing_price is not None:
                auction_data.at[index, column] = closing_price
            else:
                auction_data.at[index, column] = "無資料"  # 未找到任何可用收盤價

# 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"已完成資料處理並儲存至 {output_path}")













      














"""import os
import pandas as pd

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

# 定義需要處理的日期欄位
date_columns = [
    "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
    "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
    "開標日期(T)", "撥券日(上市上櫃日) T+7"
]

# 獲取所有文件列表
all_files = os.listdir()

# 定義一個函數以匹配代號相關的檔案並讀取收盤價
def get_closing_price(security_id, date):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            file_path = file_name
            try:
                price_data = pd.read_csv(file_path, encoding='utf-8')
                # 確保日期格式一致 (YYYY-MM-DD)
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                date_obj = pd.to_datetime(date, errors='coerce').date()
                closing_price_row = price_data.loc[price_data['日期'] == date_obj]
                if not closing_price_row.empty:
                    return closing_price_row['收盤價'].values[0]
            except (KeyError, FileNotFoundError, pd.errors.EmptyDataError):
                continue
    return None

# 更新資料中的日期欄位
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    for column in date_columns:
        if pd.notna(row[column]):  # 確保日期欄位不為空
            closing_price = get_closing_price(security_id, row[column])
            if closing_price is not None:
                auction_data.at[index, column] = closing_price
            else:
                auction_data.at[index, column] = None  # 未找到收盤價

# 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"已完成資料處理並儲存至 {output_path}")"""


