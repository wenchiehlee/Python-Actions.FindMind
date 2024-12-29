import os
import pandas as pd
import glob

# 建立輸出資料夾
output_folder = "output_data"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 讀取 cleaned_auction_data.csv
auction_data_file = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_file, encoding="utf-8")

# 定義需要替換的列
columns_to_replace = [
    "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
    "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
    "開標日期(T)", "撥券日(上市上櫃日) T+7"
]

# 處理每個證券代號
for security_id in auction_data["證券代號"].unique():
    # 搜索以證券代號開頭的收盤價檔案
    price_files = glob.glob(f"{security_id}*.csv")
    if not price_files:
        print(f"未找到 {security_id} 的收盤價檔案，跳過處理。")
        continue

    # 加載收盤價檔案（假設每個證券代號只有一個檔案）
    price_file = price_files[0]
    price_data = pd.read_csv(price_file, encoding="utf-8")

    if "收盤價" not in price_data.columns:
        print(f"{price_file} 缺少 '收盤價' 欄位，跳過處理。")
        continue

    # 確保日期格式一致
    price_data["日期"] = pd.to_datetime(price_data["日期"], errors="coerce")
    price_data.set_index("日期", inplace=True)

    # 替換日期列的值為對應日期的收盤價
    for col in columns_to_replace:
        auction_data[col] = pd.to_datetime(auction_data[col], errors="coerce")
        auction_data[col] = auction_data[col].apply(
            lambda x: price_data.loc[x, "收盤價"] if x in price_data.index else None
        )

# 將處理後的資料儲存到輸出資料夾
output_file = os.path.join(output_folder, "processed_auction_data.csv")
auction_data.to_csv(output_file, index=False, encoding="utf-8")
print(f"處理完成，結果已儲存到 {output_file}")







      


















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
