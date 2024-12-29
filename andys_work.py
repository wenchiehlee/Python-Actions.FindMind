import os
import pandas as pd

def update_auction_data():
    # 設定檔案名稱與資料夾名稱
    input_csv = 'cleaned_auction_data.csv'
    output_folder = 'updated_auction_data'
    os.makedirs(output_folder, exist_ok=True)
    
    # 讀取 cleaned_auction_data.csv
    try:
        auction_data = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"Error: File {input_csv} not found.")
        return
    
    # 定義要更新的欄位
    date_columns = [
        "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
        "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
        "開標日期(T)", "撥券日(上市上櫃日) T+7"
    ]
    
    # 搜尋證券代號對應的檔案
    for index, row in auction_data.iterrows():
        security_id = row['證券代號']
        security_files = [
            f for f in os.listdir('.') if f.startswith(f"[{security_id}]") and f.endswith('.csv')
        ]
        if not security_files:
            print(f"Warning: No files found for security ID {security_id}.")
            continue

        # 使用第一個找到的檔案
        security_file = security_files[0]
        try:
            security_data = pd.read_csv(security_file)
        except Exception as e:
            print(f"Error reading file {security_file}: {e}")
            continue
        
        # 確保有收盤價欄位
        if '收盤價' not in security_data.columns:
            print(f"Warning: '收盤價' column not found in {security_file}.")
            continue
        
        # 更新 auction_data 的相關欄位
        for column in date_columns:
            try:
                auction_data.at[index, column] = security_data['收盤價'].iloc[0]  # 假設使用第一個收盤價
            except KeyError:
                print(f"KeyError when accessing column {column} or '收盤價' in {security_file}.")
            except IndexError:
                print(f"IndexError: No data in '收盤價' column for {security_file}.")
    
    # 輸出結果
    output_path = os.path.join(output_folder, 'cleaned_auction_data_updated.csv')
    auction_data.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Updated data saved to {output_path}.")

if __name__ == "__main__":
    update_auction_data()










      


















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
