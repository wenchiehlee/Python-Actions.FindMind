import os
import pandas as pd
from workalendar.asia import Taiwan
from datetime import datetime
import re

# 初始化台灣工作日
cal = Taiwan()

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

# 獲取所有文件列表
all_files = os.listdir()

# 定義函數以獲取證券代號的國定假日
def get_public_holidays(security_id):
    holidays = []
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
            if match:
                start_date = datetime.strptime(match.group(2), "%Y-%m-%d").date()
                end_date = datetime.strptime(match.group(3), "%Y-%m-%d").date()
                
                # 獲取指定日期範圍內的國定假日
                holidays = cal.holidays(start_date, end_date)
                return [holiday[0].strftime("%Y-%m-%d") for holiday in holidays]  # 只提取日期部分
    return []

# 準備生成新 CSV 資料
holiday_data = []

for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    holidays = get_public_holidays(security_id)
    
    # 第一列為證券代號，後續列為該證券影響期間的國定假日
    holiday_data.append([security_id] + holidays)

# 將資料寫入新的 CSV 檔案
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

output_file_path = os.path.join(output_dir, "security_holidays.csv")
with open(output_file_path, mode='w', encoding='utf-8-sig') as f:
    for record in holiday_data:
        f.write(",".join(record) + "\n")

print(f"已生成包含證券代號和國定假日的檔案，存儲於：{output_file_path}")
