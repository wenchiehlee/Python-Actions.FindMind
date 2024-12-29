import os
import pandas as pd

# 定義目錄名稱
output_dir = "updated_auction_data"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path)

# 指定需要更新的欄位
date_columns = [
    "申請日期", 
    "上櫃審議委員會審議日期", 
    "櫃買董事會通過上櫃日期", 
    "櫃買同意上櫃契約日期", 
    "投標開始日(T-4)", 
    "投標結束日(T-2)", 
    "開標日期(T)", 
    "撥券日(上市上櫃日) T+7"
]

# 確保所有必要欄位存在
for column in date_columns:
    if column not in auction_data.columns:
        raise KeyError(f"Missing column '{column}' in auction data!")

# 更新每一列
for idx, row in auction_data.iterrows():
    security_id = row["證券代號"]
    matching_csv = None

    # 搜尋對應的收盤價檔案
    for file in os.listdir("."):
        if file.startswith(f"[{security_id}]") and file.endswith(".csv"):
            matching_csv = file
            break
    
    if not matching_csv:
        print(f"Warning: No closing price file found for security ID {security_id}. Skipping row {idx}.")
        continue

    # 讀取收盤價檔案
    closing_price_data = pd.read_csv(matching_csv)
    if "收盤價" not in closing_price_data.columns or "日期" not in closing_price_data.columns:
        print(f"Error: File '{matching_csv}' does not contain '收盤價' or '日期'. Skipping row {idx}.")
        continue

    # 將日期欄位對應至收盤價
    for column in date_columns:
        try:
            target_date = row[column]
            closing_price = closing_price_data.loc[closing_price_data["日期"] == target_date, "收盤價"]
            if not closing_price.empty:
                auction_data.at[idx, column] = closing_price.values[0]
            else:
                print(f"Warning: No closing price found for date '{target_date}' in file '{matching_csv}'.")
        except KeyError as e:
            print(f"KeyError: {e}. Skipping column '{column}' for row {idx}.")

# 儲存更新後的資料
output_path = os.path.join(output_dir, "cleaned_auction_data_updated.csv")
auction_data.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Updated data saved to '{output_path}'.")










      


















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
    auction_data.loc[index, auction_data.columns[2:10]] = closing_data["收盤價"].iloc[:8].values

# 將更新後的資料存入新檔案
updated_file_path = os.path.join(output_folder, "updated_cleaned_auction_data.csv")
auction_data.to_csv(updated_file_path, index=False, encoding="utf-8")
print(f"已將更新後的資料儲存至 {updated_file_path}")"""
