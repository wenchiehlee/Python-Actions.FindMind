import os
import pandas as pd

# 創建資料夾以儲存輸出資料
output_folder = "processed_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path)

# 將證券代號、名稱與日期相關欄位讀取
required_columns = ["證券代號", "證券名稱", "投標開始日(T-4)", "投標結束日(T-2)", "開標日期(T)", "撥券日(上市上櫃日) T+7"]
auction_data = auction_data[required_columns]

# 遍歷證券代號處理對應的收盤價資料
for _, row in auction_data.iterrows():
    security_id = row["證券代號"]
    csv_file_name = f"[{security_id}]*.csv"
    
    # 搜尋對應收盤價檔案
    matching_files = [f for f in os.listdir() if f.startswith(f"[{security_id}]") and f.endswith(".csv")]
    
    if not matching_files:
        print(f"未找到與證券代號 {security_id} 對應的收盤價檔案，跳過。")
        continue
    
    # 使用第一個匹配的檔案
    price_data_path = matching_files[0]
    price_data = pd.read_csv(price_data_path)
    
    # 確保收盤價欄位存在，避免 KeyError
    if "收盤價" not in price_data.columns:
        print(f"檔案 {price_data_path} 缺少收盤價資料，跳過。")
        continue
    
    # 提取所需收盤價並更新
    auction_data.loc[auction_data["證券代號"] == security_id, required_columns[2:]] = price_data["收盤價"].values[:len(required_columns[2:])]

# 輸出處理後的資料
output_path = os.path.join(output_folder, "updated_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"已成功處理並儲存至 {output_path}")






      


















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
