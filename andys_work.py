import os
import pandas as pd

# 設定資料夾名稱
output_folder = "processed_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取主要的 auction data
auction_file = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_file, sep="\t", encoding="utf-8")

# 去除第一列的 DateStart 和 DateEnd+14
columns_to_update = auction_data.columns[7:-2]  # 忽略起始和結尾的非必要欄位

# 處理每個證券代號
for index, row in auction_data.iterrows():
    security_id = str(row["證券代號"])
    
    # 搜尋對應的收盤價 CSV
    for file in os.listdir("."):
        if file.startswith(f"[{security_id}]") and file.endswith(".csv"):
            price_file = file
            break
    else:
        print(f"未找到證券代號 {security_id} 的收盤價檔案，跳過...")
        continue

    # 讀取收盤價資料
    price_data = pd.read_csv(price_file, encoding="utf-8")
    if "收盤價" not in price_data.columns:
        print(f"檔案 {price_file} 中不包含 '收盤價' 欄位，跳過...")
        continue

    # 使用收盤價更新 auction data
    for col in columns_to_update:
        auction_date = row[col]
        closing_price = price_data.loc[price_data["日期"] == auction_date, "收盤價"]
        if not closing_price.empty:
            auction_data.at[index, col] = closing_price.values[0]
        else:
            print(f"無法找到 {auction_date} 對應的收盤價，留空...")

# 儲存結果
output_file = os.path.join(output_folder, "updated_auction_data.csv")
auction_data.to_csv(output_file, index=False, encoding="utf-8")
print(f"已儲存至 {output_file}")


















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
