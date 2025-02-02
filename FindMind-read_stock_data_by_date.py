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

# 讀取 holidays.csv，並處理格式
holidays_path = "holidays.csv"
if os.path.exists(holidays_path):
    try:
        holidays_list = []
        with open(holidays_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row in reader:
                # 提取每行的第一部分（逗號前的日期）
                date_part = row[0].strip() if len(row) > 0 else None
                if date_part:
                    holidays_list.append(date_part)

        # 建立 DataFrame
        holidays = pd.DataFrame(holidays_list, columns=["日期"])

        # 使用正則表達式提取日期部分（格式為 YYYY-MM-DD）
        holidays["日期"] = holidays["日期"].str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
        holidays["日期"] = holidays["日期"].str.strip()  # 去除空格

        # 將日期轉換為標準格式
        holidays["日期"] = pd.to_datetime(holidays["日期"], errors="coerce").dt.date

        # 移除無效日期
        holidays = holidays.dropna()  # 移除轉換失敗的行
        holidays_set = set(holidays["日期"])  # 建立假日集合

        print(f"成功讀取 holidays.csv，共 {len(holidays_set)} 個假日")
    except Exception as e:
        print(f"讀取 holidays.csv 時發生錯誤: {e}")
        holidays_set = set()
else:
    print("找不到 holidays.csv，將不考慮假日")
    holidays_set = set()

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



def get_security_stats(security_id):
    """
    計算資料總數與總工作天數
    :param security_id: 股票代號
    :return: (total_rows, working_days)
    """
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            try:
                # 1. 讀取證券檔案
                price_data = pd.read_csv(file_name, encoding='utf-8')
                total_rows = price_data.shape[0]  # 資料總數（行數）

                # 確保日期欄位格式一致
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date

                # 打印原始檔案日期範圍和行數
                print(f"股票代號: {security_id}, 原始檔案日期範圍: {price_data['日期'].min()} ~ {price_data['日期'].max()}")
                print(f"股票代號: {security_id}, 檔案行數 (資料總數): {total_rows}")

                # 2. 提取日期跨度
                match = re.search(r"\[(\d+)\] (\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})", file_name)
                if match:
                    start_date = pd.to_datetime(match.group(2), errors='coerce').date()
                    end_date = pd.to_datetime(match.group(3), errors='coerce').date()

                    if start_date and end_date:
                        # 3. 計算總工作天數
                        working_days = 0
                        current_date = start_date
                        while current_date <= end_date:
                            # 如果不是國定假日且不在自訂假日中，則計算為工作天
                            if not cal.is_holiday(current_date) and current_date not in holidays_set and cal.is_working_day(current_date):
                                working_days += 1
                            current_date += timedelta(days=1)

                        # 打印總工作天數
                        print(f"股票代號: {security_id}, 總工作天數: {working_days}")

                        return total_rows, working_days
                    else:
                        print(f"股票代號: {security_id}, 無效的日期範圍")
                        return total_rows, "無資料"
                else:
                    print(f"股票代號: {security_id}, 無法提取日期範圍")
                    return total_rows, "無資料"

            except (FileNotFoundError, pd.errors.EmptyDataError) as e:
                print(f"讀取證券檔案錯誤: {e}")
                return "無資料", "無資料"
    return "無資料", "無資料"











# 更新資料中的日期欄位並添加新列
auction_data.insert(auction_data.columns.get_loc("DateEnd+14") + 1, "資料總數", "無資料")  # 資料總數插入到 DateEnd+14 後面
auction_data.insert(auction_data.columns.get_loc("資料總數") + 1, "總工作天數", "無資料")  # 總工作天數插入到 資料總數 後面

for index, row in auction_data.iterrows():
    security_id = row["股票代號"]
    
    # 更新日期欄位
    for column in date_columns:
        if pd.notna(row[column]):  # 確保日期欄位不為空
            closing_price = get_closing_price(security_id, row[column])
            if closing_price is not None:
                auction_data.at[index, column] = closing_price
            else:
                auction_data.at[index, column] = "無資料"  # 未找到任何可用收盤價
    
    # 獲取資料總數和總工作天數
    total_rows, working_days = get_security_stats(security_id)
    auction_data.at[index, "資料總數"] = total_rows
    auction_data.at[index, "總工作天數"] = working_days

# 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"已完成資料處理並儲存至 {output_path}")
