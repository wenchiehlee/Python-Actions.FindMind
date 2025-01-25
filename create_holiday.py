"""import os
import pandas as pd
import re
from workalendar.asia import Taiwan
from datetime import datetime, timedelta

# 初始化台灣工作日計算
cal = Taiwan()

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 定義要輸出的缺失日期 CSV
missing_dates_output_path = os.path.join(output_dir, "missing_dates.csv")

# 獲取所有文件列表
all_files = os.listdir()

# 定義函數以找出缺失日期
def find_missing_dates(security_id, start_date, end_date):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # 讀取證券資料
                price_data = pd.read_csv(file_name, encoding='utf-8')
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                
                # 獲取日期範圍內的所有工作日
                date_range = pd.date_range(start=start_date, end=end_date, freq='B')
                all_working_days = [d.date() for d in date_range if cal.is_working_day(d)]

                # 已存在的日期
                existing_dates = set(price_data['日期'].dropna())

                # 找出缺失的日期
                missing_dates = [d for d in all_working_days if d not in existing_dates]
                return missing_dates
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
                return []
    return []

# 構建缺失日期 CSV 的初始結構
missing_dates_data = []

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    
    # 提取證券檔案中的日期跨度
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
            if match:
                start_date = pd.to_datetime(match.group(2), errors='coerce').date()
                end_date = pd.to_datetime(match.group(3), errors='coerce').date()
                
                # 找出缺失日期
                missing_dates = find_missing_dates(security_id, start_date, end_date)
                if missing_dates:
                    # 加入缺失日期到輸出結構
                    missing_dates_data.append([security_id] + missing_dates)

# 生成缺失日期的 CSV 檔案
missing_dates_df = pd.DataFrame(missing_dates_data)
missing_dates_df.to_csv(missing_dates_output_path, index=False, header=False, encoding='utf-8-sig')

print(f"缺失日期已儲存至 {missing_dates_output_path}")"""















import os
import pandas as pd
import re
from workalendar.asia import Taiwan
from datetime import datetime, timedelta

# 初始化台灣工作日計算
cal = Taiwan()

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 定義要輸出的缺失日期 CSV
missing_dates_output_path = os.path.join(output_dir, "missing_dates.csv")

# 獲取所有文件列表
all_files = os.listdir()

# 讀取 holidays.csv 內的假日
holidays_path = "holidays.csv"
if os.path.exists(holidays_path):
    custom_holidays = pd.read_csv(holidays_path, header=None, names=["日期"])
    custom_holidays["日期"] = pd.to_datetime(custom_holidays["日期"], errors="coerce").dt.date
    custom_holidays_set = set(custom_holidays["日期"].dropna())
else:
    print("找不到 holidays.csv，將不考慮額外假日")
    custom_holidays_set = set()

# 定義函數以找出缺失日期
def find_missing_dates(security_id, start_date, end_date):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # 讀取證券資料
                price_data = pd.read_csv(file_name, encoding='utf-8')
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                
                # 獲取日期範圍內的所有工作日（考慮國定假日與額外假日）
                date_range = pd.date_range(start=start_date, end=end_date, freq='B')
                all_working_days = [d.date() for d in date_range if cal.is_working_day(d) and d.date() not in custom_holidays_set]

                # 已存在的日期
                existing_dates = set(price_data['日期'].dropna())

                # 找出缺失的日期
                missing_dates = [d for d in all_working_days if d not in existing_dates]
                return missing_dates
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")
                return []
    return []

# 構建缺失日期 CSV 的初始結構
missing_dates_data = []

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    
    # 提取證券檔案中的日期跨度
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
            if match:
                start_date = pd.to_datetime(match.group(2), errors='coerce').date()
                end_date = pd.to_datetime(match.group(3), errors='coerce').date()
                
                # 找出缺失日期
                missing_dates = find_missing_dates(security_id, start_date, end_date)
                if missing_dates:
                    # 加入缺失日期到輸出結構
                    missing_dates_data.append([security_id] + missing_dates)

# 生成缺失日期的 CSV 檔案
missing_dates_df = pd.DataFrame(missing_dates_data)
missing_dates_df.to_csv(missing_dates_output_path, index=False, header=False, encoding='utf-8-sig')

print(f"缺失日期已儲存至 {missing_dates_output_path}")

