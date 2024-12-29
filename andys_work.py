import os
import pandas as pd
from glob import glob

# 創建儲存資料的資料夾
output_dir = "processed_data"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv
auction_data = pd.read_csv("cleaned_auction_data.csv")

# 獲取所有證券代號
security_ids = auction_data["證券代號"].unique()

# 處理每個證券代號
for security_id in security_ids:
    # 搜尋符合的收盤價檔案
    matching_files = glob(f"[{security_id}]*.csv")
    if not matching_files:
        print(f"警告: 找不到代號 {security_id} 的檔案，跳過...")
        continue

    # 假設只有一個符合的檔案
    closing_price_file = matching_files[0]
    try:
        # 讀取收盤價資料
        closing_prices = pd.read_csv(closing_price_file)

        # 確保存在收盤價欄位
        if "收盤價" not in closing_prices.columns:
            print(f"錯誤: 檔案 {closing_price_file} 缺少 '收盤價' 欄位，跳過...")
            continue

        # 以日期為索引對應收盤價
        closing_prices.set_index("日期", inplace=True)
        auction_data[f"{security_id}_收盤價"] = auction_data["開標日期(T)"].map(
            lambda date: closing_prices.loc[date, "收盤價"] if date in closing_prices.index else None
        )

    except Exception as e:
        print(f"處理檔案 {closing_price_file} 時發生錯誤: {e}")

# 將處理後的資料儲存回新的 CSV
output_file = os.path.join(output_dir, "cleaned_auction_data_with_prices.csv")
auction_data.to_csv(output_file, index=False, encoding="utf-8")
print(f"完成: 資料已儲存於 {output_file}")



      


















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
