import os
import pandas as pd
import glob

def update_auction_data(auction_file, output_folder):
    # 確保輸出資料夾存在
    os.makedirs(output_folder, exist_ok=True)

    # 讀取 cleaned_auction_data.csv
    auction_data = pd.read_csv(auction_file, encoding='utf-8')
    columns_to_update = [
        "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期", 
        "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)", 
        "開標日期(T)", "撥券日(上市上櫃日) T+7"
    ]
    
    # 確保所需的列存在
    if not all(col in auction_data.columns for col in columns_to_update):
        raise KeyError("The required columns are missing in the auction data file.")

    for index, row in auction_data.iterrows():
        security_id = row["證券代號"]
        if pd.isna(security_id):
            continue  # 跳過缺少證券代號的行

        # 搜尋對應的收盤價檔案
        price_files = glob.glob(f"[{security_id}]*.csv")
        if not price_files:
            print(f"收盤價檔案未找到: {security_id}")
            continue

        # 讀取收盤價檔案
        price_data = pd.read_csv(price_files[0], encoding='utf-8')
        if "收盤價" not in price_data.columns or "日期" not in price_data.columns:
            print(f"收盤價檔案格式錯誤: {price_files[0]}")
            continue

        # 建立日期到收盤價的對應
        price_mapping = price_data.set_index("日期")["收盤價"].to_dict()

        # 更新日期列為收盤價
        for col in columns_to_update:
            original_date = row[col]
            if pd.isna(original_date) or original_date not in price_mapping:
                auction_data.at[index, col] = None
            else:
                auction_data.at[index, col] = price_mapping[original_date]

    # 儲存更新後的資料
    output_path = os.path.join(output_folder, "updated_auction_data.csv")
    auction_data.to_csv(output_path, index=False, encoding='utf-8')
    print(f"更新後的資料已儲存至: {output_path}")

if __name__ == "__main__":
    auction_file = "cleaned_auction_data.csv"
    output_folder = "updated_data"
    update_auction_data(auction_file, output_folder)







      


















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
