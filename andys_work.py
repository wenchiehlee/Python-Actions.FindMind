import os
import pandas as pd
import glob

def process_auction_data(auction_data_path, price_data_folder, output_folder):
    # 確保輸出資料夾存在
    os.makedirs(output_folder, exist_ok=True)
    
    # 讀取 auction_data
    auction_data = pd.read_csv(auction_data_path, sep="\t", dtype=str)
    auction_data_columns = auction_data.columns.tolist()
    
    # 去除無用的 DateStart 和 DateEnd+14 欄位
    if "DateStart" in auction_data_columns and "DateEnd+14" in auction_data_columns:
        auction_data = auction_data.drop(columns=["DateStart", "DateEnd+14"])
    
    # 遍歷每個證券代號並查找相應的收盤價 CSV
    for _, row in auction_data.iterrows():
        security_id = row["證券代號"]
        price_csv_pattern = os.path.join(price_data_folder, f"[{security_id}]*.csv")
        price_csv_files = glob.glob(price_csv_pattern)
        
        if not price_csv_files:
            print(f"警告: 未找到證券代號 {security_id} 的收盤價資料，略過。")
            continue
        
        # 假設每個證券代號只有一個收盤價文件
        price_data_path = price_csv_files[0]
        price_data = pd.read_csv(price_data_path, dtype=str)
        
        if "收盤價" not in price_data.columns:
            print(f"錯誤: {price_data_path} 不包含 '收盤價' 欄位，略過。")
            continue
        
        # 替換資料中的日期為收盤價
        for column in auction_data_columns[6:]:  # 從投標開始日(T-4)到撥券日
            if column in row and row[column] in price_data["日期"].values:
                date_value = row[column]
                closing_price = price_data.loc[price_data["日期"] == date_value, "收盤價"].values[0]
                auction_data.at[_, column] = closing_price
            else:
                print(f"警告: 日期 {row[column]} 不存在於 {price_data_path}，略過。")
    
    # 儲存結果
    output_path = os.path.join(output_folder, "processed_auction_data.csv")
    auction_data.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"處理完成，結果儲存於: {output_path}")

# 使用範例
process_auction_data(
    auction_data_path="cleaned_auction_data.csv",
    price_data_folder="./price_data",
    output_folder="./processed_data"
)


      


















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
