import os
import pandas as pd
from glob import glob

def update_closing_prices(auction_file, data_folder, output_folder):
    # 建立儲存結果的資料夾
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # 讀取 cleaned_auction_data.csv
    auction_data = pd.read_csv(auction_file, sep='\t')
    auction_data['證券代號'] = auction_data['證券代號'].astype(str)
    
    # 遍歷每個證券代號
    for index, row in auction_data.iterrows():
        security_id = row['證券代號']
        
        # 搜尋對應的收盤價 CSV 檔案
        matching_files = glob(os.path.join(data_folder, f"[{security_id}]*.csv"))
        if not matching_files:
            print(f"找不到證券代號 {security_id} 的收盤價檔案，跳過...")
            continue
        
        # 使用找到的第一個檔案
        closing_price_file = matching_files[0]
        closing_data = pd.read_csv(closing_price_file)

        if '收盤價' not in closing_data.columns:
            print(f"檔案 {closing_price_file} 中未找到 '收盤價' 欄位，跳過...")
            continue
        
        # 確保 '日期' 與 '收盤價' 欄位存在
        if '日期' not in closing_data.columns:
            print(f"檔案 {closing_price_file} 中未找到 '日期' 欄位，跳過...")
            continue

        # 將 auction_data 的日期替換成對應的收盤價
        try:
            closing_prices = closing_data.set_index('日期')['收盤價']
            auction_data.loc[index, '收盤價'] = closing_prices.loc[row['撥券日(上市上櫃日) T+7']]
        except KeyError as e:
            print(f"日期 {row['撥券日(上市上櫃日) T+7']} 不存在於 {closing_price_file}，跳過...")

    # 將結果儲存到輸出檔案中
    output_file = os.path.join(output_folder, 'updated_auction_data.csv')
    auction_data.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"更新後的檔案已儲存至 {output_file}")

# 主程式
if __name__ == "__main__":
    auction_file = "cleaned_auction_data.csv"
    data_folder = "closing_prices_data"  # 收盤價資料夾
    output_folder = "processed_auction_data"  # 輸出資料夾
    update_closing_prices(auction_file, data_folder, output_folder)





      


















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
