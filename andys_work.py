import os
import pandas as pd

# 建立資料夾來儲存結果
output_folder = "auction_adjusted_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv 並檢查欄位名稱
cleaned_auction_data = pd.read_csv("cleaned_auction_data.csv", sep="\t")
print("讀取資料成功，欄位名稱為：", cleaned_auction_data.columns.tolist())

# 確保 '證券代號' 欄位存在
if '證券代號' not in cleaned_auction_data.columns:
    raise KeyError("無法找到欄位 '證券代號'，請檢查 CSV 檔案中的列標題")

# 替換收盤價
for index, row in cleaned_auction_data.iterrows():
    security_id = str(row['證券代號']).strip()
    price_csv_pattern = f"{security_id}"  # 模糊匹配 csv 檔案名
    price_csv = None
    
    # 搜尋該證券的收盤價 CSV
    for file in os.listdir("."):
        if file.startswith(price_csv_pattern) and file.endswith(".csv"):
            price_csv = file
            break

    if not price_csv:
        print(f"未找到符合的收盤價檔案: {security_id}")
        continue
    
    # 讀取收盤價資料
    price_data = pd.read_csv(price_csv)
    if '收盤價' not in price_data.columns:
        print(f"檔案 {price_csv} 中未包含 '收盤價' 欄位")
        continue
    
    # 更新收盤價到主要資料
    date_columns = cleaned_auction_data.columns[7:]  # 僅更新日期相關欄位
    if len(price_data['收盤價']) < len(date_columns):
        print(f"警告: 收盤價資料不足，無法完全覆蓋日期相關欄位（證券代號: {security_id}）")
        continue
    
    for i, col in enumerate(date_columns):
        cleaned_auction_data.at[index, col] = price_data['收盤價'].iloc[i]

# 儲存結果
output_path = os.path.join(output_folder, "cleaned_auction_data_updated.csv")
cleaned_auction_data.to_csv(output_path, index=False)
print(f"更新後的檔案已儲存至: {output_path}")

      


















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
