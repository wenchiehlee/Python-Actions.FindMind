import os
import pandas as pd
from glob import glob

# 創建資料夾來存儲結果
output_folder = "updated_auction_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path)

# 確保第一列標題正確
columns = [
    "證券代號", "證券名稱", "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
    "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)", "開標日期(T)", 
    "撥券日(上市上櫃日) T+7", "DateStart", "DateEnd+14"
]
auction_data.columns = columns

# 遍歷證券代號，匹配收盤價資料
for security_id in auction_data["證券代號"].unique():
    # 查找符合該證券代號的CSV檔案
    price_file_pattern = f"/mnt/data/{security_id}*.csv"
    price_files = glob(price_file_pattern)
    
    if not price_files:
        print(f"無法找到證券代號 {security_id} 的收盤價資料檔案。")
        continue
    
    # 假設每個證券代號只有一個相關檔案
    price_file = price_files[0]
    price_data = pd.read_csv(price_file)

    # 確認有 "收盤價" 欄位
    if "收盤價" not in price_data.columns:
        print(f"{price_file} 缺少 '收盤價' 欄位，跳過。")
        continue

    # 替換 auction_data 中的日期資料為該證券代號的收盤價
    relevant_rows = auction_data["證券代號"] == security_id
    for date_column in columns[2:-2]:  # 忽略 DateStart 和 DateEnd+14
        if date_column in auction_data.columns and date_column in price_data.columns:
            auction_data.loc[relevant_rows, date_column] = price_data.set_index("日期").loc[
                auction_data.loc[relevant_rows, date_column], "收盤價"
            ].values

# 儲存結果至新的CSV檔案
output_path = os.path.join(output_folder, "cleaned_auction_data_updated.csv")
auction_data.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"已更新的資料存儲於 {output_path}")

















"""import os
import pandas as pd

# 設定資料夾名稱
output_folder = "updated_auction_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path)

# 更新收盤價的邏輯
for index, row in auction_data.iterrows():
   # if index < 2 or index > 9:  # 只處理第3到10行（從0開始計算）
        #continue
    
    # 取得證券代號，假設第一列名為 "證券代號"
    security_id = row["證券代號"]
    csv_file_pattern = f"[{security_id}]*.csv"
    
    # 在當前目錄中尋找符合模式的檔案
    matching_files = [f for f in os.listdir('.') if f.startswith(f"[{security_id}]") and f.endswith(".csv")]
    
    if not matching_files:
        print(f"未找到符合 {security_id} 的檔案")
        continue
    
    # 假設只有一個對應檔案
    closing_price_file = matching_files[0]
    closing_data = pd.read_csv(closing_price_file)
    
    # 確保含有 "收盤價" 列
    if "收盤價" not in closing_data.columns:
        print(f"檔案 {closing_price_file} 中未找到 '收盤價'")
        continue
    
    # 使用收盤價更新第3到10行
    auction_data.loc[index, auction_data.columns[2:10]] = closing_data["收盤價"].iloc[:8].values"""

# 將更新後的資料存入新檔案
updated_file_path = os.path.join(output_folder, "updated_cleaned_auction_data.csv")
auction_data.to_csv(updated_file_path, index=False, encoding="utf-8")
print(f"已將更新後的資料儲存至 {updated_file_path}")
