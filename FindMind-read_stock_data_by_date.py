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
all_files = os.listdir()

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
                price_data = pd.read_csv(file_name, encoding='utf-8')
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                price_data = price_data.sort_values(by='日期').reset_index(drop=True)

                base_date = pd.to_datetime(base_date, errors='coerce').date()
                if pd.isna(base_date):
                    return "無效日期"

                base_idx = price_data[price_data['日期'] == base_date].index
                if not base_idx.empty:
                    target_idx = base_idx[0] + offset
                    if 0 <= target_idx < len(price_data):
                        return price_data.iloc[target_idx]['收盤價']
                    else:
                        return "超出範圍"
                else:
                    return "日期不存在"
            except (KeyError, FileNotFoundError, pd.errors.EmptyDataError):
                continue
    return "無資料"


# 3. 計算資料總數與總工作天數
def get_security_stats(security_id):
    """
    計算資料總數與總工作天數
    """
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                price_data = pd.read_csv(file_name, encoding='utf-8')
                total_rows = price_data.shape[0]
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date

                match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
                if match:
                    start_date = pd.to_datetime(match.group(2), errors='coerce').date()
                    end_date = pd.to_datetime(match.group(3), errors='coerce').date()

                    if start_date and end_date:
                        working_days = 0
                        current_date = start_date
                        while current_date <= end_date:
                            if not cal.is_holiday(current_date) and current_date not in holidays_set and cal.is_working_day(current_date):
                                working_days += 1
                            current_date += timedelta(days=1)

                        print(f"股票代號: {security_id}, 總工作天數: {working_days}")
                        return total_rows, working_days
                    else:
                        return total_rows, "無資料"
                else:
                    return total_rows, "無資料"

            except (FileNotFoundError, pd.errors.EmptyDataError) as e:
                print(f"讀取證券檔案錯誤: {e}")
                return "無資料", "無資料"
    return "無資料", "無資料"


# 4. 更新資料中的日期欄位並添加新列
auction_data.insert(auction_data.columns.get_loc("DateEnd") + 1, "資料總數", "無資料")
auction_data.insert(auction_data.columns.get_loc("資料總數") + 1, "總工作天數", "無資料")

for index, row in auction_data.iterrows():
    security_id = row["股票代號"]

    # 更新日期欄位，根據偏移量調整收盤價查詢
    for col, info in date_columns.items():
        base_col = info['base']
        offset = info['offset']

        if base_col in auction_data.columns and pd.notna(row[base_col]):
            closing_price = get_closing_price(security_id, row[base_col], offset)
            auction_data.at[index, col] = closing_price
        else:
            auction_data.at[index, col] = "無資料"

    # 獲取資料總數和總工作天數
    total_rows, working_days = get_security_stats(security_id)
    auction_data.at[index, "資料總數"] = total_rows
    auction_data.at[index, "總工作天數"] = working_days

# 5. 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"已完成資料處理並儲存至 {output_path}")

