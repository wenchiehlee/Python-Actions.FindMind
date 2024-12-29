import pandas as pd
import os

def update_auction_data(auction_file, data_folder):
    # 讀取 cleaned_auction_data.csv
    auction_data = pd.read_csv(auction_file, encoding='utf-8')
    
    # 定義需要修改的行數範圍
    rows_to_update = range(2, 10)  # 注意：0-indexed，3~10行為索引2~9
    
    # 讀取證券代號
    for index, row in auction_data.iloc[rows_to_update].iterrows():
        security_code = row['證券代號']
        
        # 搜索對應的收盤價檔案
        matching_files = [f for f in os.listdir(data_folder) if f.startswith(f"[{security_code}]") and f.endswith(".csv")]
        
        if matching_files:
            closing_price_file = matching_files[0]
            # 讀取收盤價資料
            price_data = pd.read_csv(os.path.join(data_folder, closing_price_file), encoding='utf-8')
            
            if '收盤價' in price_data.columns:
                # 假設以日期作為索引匹配
                for col in ['申請日期', '上櫃審議委員會審議日期', '櫃買董事會通過上櫃日期', 
                            '櫃買同意上櫃契約日期', '投標開始日(T-4)', '投標結束日(T-2)', '開標日期(T)', '撥券日(上市上櫃日) T+7']:
                    date = row[col]
                    closing_price = price_data.loc[price_data['日期'] == date, '收盤價']
                    if not closing_price.empty:
                        auction_data.at[index, col] = closing_price.values[0]
                    else:
                        print(f"警告: 找不到 {security_code} 在日期 {date} 的收盤價")
            else:
                print(f"錯誤: {closing_price_file} 缺少 '收盤價' 欄位")
        else:
            print(f"警告: 找不到與證券代號 {security_code} 匹配的收盤價檔案")
    
    # 儲存更新後的檔案
    auction_data.to_csv(auction_file, index=False, encoding='utf-8-sig')
    print(f"已更新 {auction_file} 的資料")

# 使用範例
update_auction_data("cleaned_auction_data.csv", "data_folder")
