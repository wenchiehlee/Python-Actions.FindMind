import os
import pandas as pd

def update_cleaned_auction_data():
    # 確認工作目錄中存在的 CSV 檔案
    file_list = [f for f in os.listdir('.') if f.endswith('.csv')]
    auction_data_file = "cleaned_auction_data.csv"

    # 確保輸出資料夾存在
    output_folder = "processed_data"
    os.makedirs(output_folder, exist_ok=True)
    
    if auction_data_file not in file_list:
        print(f"Error: {auction_data_file} not found.")
        return

    # 讀取 cleaned_auction_data.csv
    auction_data = pd.read_csv(auction_data_file, sep="\t")
    auction_data_columns = ["申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
                            "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
                            "開標日期(T)", "撥券日(上市上櫃日) T+7"]
    
    # 檢查是否有必要的列
    for col in auction_data_columns:
        if col not in auction_data.columns:
            print(f"Error: Missing column '{col}' in auction data.")
            return

    updated_data = auction_data.copy()

    for index, row in auction_data.iterrows():
        security_id = str(row["證券代號"]).strip()
        related_file = next((f for f in file_list if f.startswith(f"[{security_id}]")), None)

        if related_file:
            try:
                # 讀取對應的收盤價檔案
                closing_data = pd.read_csv(related_file)
                if "收盤價" not in closing_data.columns:
                    print(f"Error: Column '收盤價' not found in {related_file}.")
                    continue
                
                # 更新相關日期列為對應收盤價
                for col in auction_data_columns:
                    date_value = row[col]
                    closing_price_row = closing_data.loc[closing_data["日期"] == date_value]
                    
                    if not closing_price_row.empty:
                        updated_data.at[index, col] = closing_price_row.iloc[0]["收盤價"]
                    else:
                        print(f"Warning: Date {date_value} not found in {related_file}.")

            except Exception as e:
                print(f"Error processing {related_file}: {e}")
        else:
            print(f"Warning: No related file found for security ID {security_id}.")

    # 將更新後的資料存檔
    output_file = os.path.join(output_folder, "cleaned_auction_data_updated.csv")
    updated_data.to_csv(output_file, index=False, sep="\t")
    print(f"Updated auction data saved to {output_file}.")

if __name__ == "__main__":
    update_cleaned_auction_data()










      


















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
