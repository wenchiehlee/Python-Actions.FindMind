import os
import pandas as pd
import re
from workalendar.asia import Taiwan  # 使用 workalendar 計算台灣的工作日

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

# 初始化台灣工作日計算
cal = Taiwan()

# 讀取 holidays.csv 檔案
holidays_path = "holidays.csv"
if os.path.exists(holidays_path):
    try:
        # 嘗試讀取 holidays.csv
        custom_holidays = pd.read_csv(holidays_path, header=None, names=["日期"])
        # 使用 pd.to_datetime 處理各種可能的日期格式，並自動轉換為標準格式
        custom_holidays["日期"] = pd.to_datetime(custom_holidays["日期"], errors="coerce").dt.date
        
        # 去除無法解析的行（例如 "invalid_date" 或空值）
        custom_holidays = custom_holidays.dropna()
        
        # 構建假日集合
        custom_holidays_set = set(custom_holidays["日期"])
        print(f"成功讀取 holidays.csv，共 {len(custom_holidays_set)} 個假日")
    except Exception as e:
        print(f"讀取 holidays.csv 時出錯: {e}")
        custom_holidays_set = set()
else:
    print("找不到 holidays.csv，將不考慮額外假日")
    custom_holidays_set = set()

# 定義函數以獲取收盤價
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

# 定義函數以計算資料總數與總工作天數
def get_security_stats(security_id):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # 1. 計算資料總數
                price_data = pd.read_csv(file_name, encoding='utf-8')
                total_rows = price_data.shape[0]  # 資料總數（行數）

                # 2. 提取日期跨度，計算總工作天數
                match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
                if match:
                    start_date = pd.to_datetime(match.group(2), errors='coerce')
                    end_date = pd.to_datetime(match.group(3), errors='coerce')
                    if start_date and end_date:
                        # 使用 workalendar 計算台灣的工作天數
                        working_days = cal.get_working_days_delta(start_date, end_date)
                    else:
                        working_days = "無資料"
                else:
                    working_days = "無資料"

                return total_rows, working_days
            except (FileNotFoundError, pd.errors.EmptyDataError):
                return "無資料", "無資料"
    return "無資料", "無資料"

# 更新資料中的日期欄位並添加新列
auction_data.insert(auction_data.columns.get_loc("DateEnd+14") + 1, "資料總數", "無資料")  # 資料總數插入到 DateEnd+14 後面
auction_data.insert(auction_data.columns.get_loc("資料總數") + 1, "總工作天數", "無資料")  # 總工作天數插入到 資料總數 後面

for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    
    # 更新日期欄位
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    
    # 更新日期欄位
    for column in date_columns:
        if pd.notna(row[column]):  # 確保日期欄位不為空
            date_value = row[column]
            print(f"處理列: {column}, 日期: {date_value}")
            
            closing_price = get_closing_price(security_id, date_value)
            if closing_price is not None:
                auction_data.at[index, column] = closing_price
                print(f"更新 {column} 為收盤價: {closing_price}")
            else:
                # 判斷是否為假日
                date_obj = pd.to_datetime(date_value, errors='coerce').date()
                if date_obj in custom_holidays_set:
                    auction_data.at[index, column] = "holiday"
                    print(f"更新 {column} 為 holiday")
                else:
                    auction_data.at[index, column] = "無資料"
                    print(f"更新 {column} 為 無資料")

# 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f"已完成資料處理並儲存至 {output_path}")
