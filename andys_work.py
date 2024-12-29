import os
import pandas as pd

# 創建資料夾來保存結果
output_folder = "processed_auction_data"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 讀取 cleaned_auction_data.csv
auction_data_file = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_file, encoding='utf-8')

# 確認日期相關欄位
columns_to_update = auction_data.columns.difference(["DateStart", "DateEnd+14"])

# 處理每個證券代號的數據
for security_id in auction_data["證券代號"].unique():
    # 找到對應的收盤價檔案
    price_file = None
    for file in os.listdir("."):
        if file.startswith(str(security_id)) and file.endswith(".csv"):
            price_file = file
            break
    
    if price_file:
        # 讀取對應的收盤價數據
        price_data = pd.read_csv(price_file, encoding='utf-8')
        
        # 確保包含收盤價的欄位，並匹配日期
        if "收盤價" in price_data.columns:
            for col in columns_to_update:
                if col in auction_data.columns and col in price_data.columns:
                    auction_data.loc[auction_data["證券代號"] == security_id, col] = \
                        auction_data.loc[auction_data["證券代號"] == security_id, col].map(
                            lambda x: price_data.loc[price_data["日期"] == x, "收盤價"].values[0]
                            if x in price_data["日期"].values else x
                        )
        else:
            print(f"收盤價欄位在檔案 {price_file} 中不存在，跳過該檔案")
    else:
        print(f"未找到對應證券代號 {security_id} 的收盤價檔案，跳過")

# 儲存結果到新的 CSV 文件中
output_file = os.path.join(output_folder, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_file, index=False, encoding='utf-8')

print(f"處理完成，結果儲存在 {output_file}")
















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
