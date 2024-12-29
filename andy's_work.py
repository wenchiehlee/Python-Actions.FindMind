import os
import pandas as pd

# 讀取 cleaned_auction_data.csv
auction_data_file = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_file)

# 提取第一列內容，獲取證券代號清單
security_codes = auction_data['證券代號'].unique()

# 遍歷證券代號並處理對應的收盤價
def update_closing_prices(auction_data, security_code):
    # 找到匹配的檔案
    matching_file = None
    for file in os.listdir():
        if file.startswith(f"[{security_code}]") and file.endswith(".csv"):
            matching_file = file
            break

    if not matching_file:
        print(f"未找到 {security_code} 的檔案，跳過處理。")
        return

    # 讀取對應證券代號的收盤價檔案
    stock_data = pd.read_csv(matching_file)

    if '收盤價' not in stock_data.columns:
        print(f"{matching_file} 缺少 '收盤價' 欄位，無法處理。")
        return

    # 獲取收盤價資料並更新到 auction_data
    for index, row in auction_data.iterrows():
        if row['證券代號'] == security_code:
            for col in ['申請日期', '上櫃審議委員會審議日期', '櫃買董事會通過上櫃日期',
                        '櫃買同意上櫃契約日期', '投標開始日(T-4)', '投標結束日(T-2)', '開標日期(T)', '撥券日(上市上櫃日) T+7']:
                try:
                    matching_date = pd.to_datetime(row[col])
                    closing_price_row = stock_data[stock_data['日期'] == matching_date.strftime("%Y-%m-%d")]

                    if not closing_price_row.empty:
                        auction_data.at[index, col] = closing_price_row['收盤價'].values[0]
                except Exception as e:
                    print(f"更新 {security_code} 時發生錯誤: {e}")

# 更新所有證券代號的資料
for code in security_codes:
    update_closing_prices(auction_data, code)

# 保存更新後的檔案
updated_file = "updated_cleaned_auction_data.csv"
auction_data.to_csv(updated_file, index=False, encoding='utf-8-sig')
print(f"處理完成，結果已儲存到 {updated_file}")
