import os
import pandas as pd

# 建立資料夾存放結果
output_folder = "processed_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path, encoding="utf-8")

# 定義需要修改的欄位
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

# 確保所有檔案名稱前綴
data_files = [f for f in os.listdir(".") if f.startswith("[") and f.endswith(".csv")]

# 處理每個證券代號
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    relevant_file = next((f for f in data_files if f.startswith(f"[{security_id}]")), None)

    if not relevant_file:
        print(f"找不到 {security_id} 對應的收盤價檔案，跳過")
        continue

    # 讀取收盤價檔案
    closing_price_data = pd.read_csv(relevant_file, encoding="utf-8")
    
    if "收盤價" not in closing_price_data.columns:
        print(f"檔案 {relevant_file} 中缺少 '收盤價' 欄位，跳過")
        continue

    # 將日期欄位替換為收盤價
    for column in date_columns:
        date = row[column]
        try:
            price = closing_price_data.loc[closing_price_data["日期"] == date, "收盤價"].values[0]
            auction_data.at[index, column] = price
        except IndexError:
            print(f"日期 {date} 在檔案 {relevant_file} 中無對應收盤價，跳過該日期")

# 儲存處理後的檔案
output_file_path = os.path.join(output_folder, "cleaned_auction_data_processed.csv")
auction_data.to_csv(output_file_path, index=False, encoding="utf-8")
print(f"處理完成，檔案已儲存至 {output_file_path}")











      


















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
