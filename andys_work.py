import os
import pandas as pd
import glob

# 建立一個儲存處理後資料的資料夾
output_folder = "processed_data"
os.makedirs(output_folder, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data_path = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_path)

# 定義處理函數
def process_auction_data(auction_data, output_folder):
    # 將證券代號從第一列提取並去除空格
    auction_data.columns = auction_data.columns.str.strip()
    for index, row in auction_data.iterrows():
        security_id = str(row["證券代號"]).strip()
        # 搜尋對應的收盤價 csv 檔案
        matching_files = glob.glob(f"*[{security_id}]*.csv")
        
        if not matching_files:
            print(f"未找到對應的收盤價檔案: {security_id}")
            continue
        
        closing_price_file = matching_files[0]
        closing_price_data = pd.read_csv(closing_price_file)
        
        # 確保有收盤價資料
        if "收盤價" not in closing_price_data.columns:
            print(f"收盤價欄位不存在於檔案: {closing_price_file}")
            continue

        # 選取需要的日期範圍
        closing_prices = closing_price_data["收盤價"]
        
        if len(closing_prices) < len(auction_data):
            print(f"收盤價資料不足: {closing_price_file}")
            continue

        # 替換 auction_data 中的行資料
        auction_data.iloc[index, 1:-2] = closing_prices.values[:len(row) - 2]

    # 儲存處理後的資料
    output_file_path = os.path.join(output_folder, "cleaned_auction_data_processed.csv")
    auction_data.to_csv(output_file_path, index=False)
    print(f"處理完成的檔案已儲存在 {output_file_path}")

# 執行處理函數
process_auction_data(auction_data, output_folder)




      


















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
