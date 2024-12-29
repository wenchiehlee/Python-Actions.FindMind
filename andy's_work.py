import pandas as pd
import os

def update_auction_data(auction_file, stock_data_folder):
    # 讀取 "cleaned_auction_data.csv"
    auction_data = pd.read_csv(auction_file, encoding='utf-8', sep='\t')
    
    # 確認欄位名稱正確
    if '證券代號' not in auction_data.columns:
        raise ValueError("CSV 欄位名稱與預期不符，請確認文件格式。")
    
    # 逐一處理證券代號
    for index, row in auction_data.iterrows():
        stock_code = row['證券代號']
        start_date = row['DateStart']
        end_date = row['DateEnd+14']

        # 找到對應的股票數據文件
        stock_file_pattern = f"[{stock_code}]"
        stock_files = [f for f in os.listdir(stock_data_folder) if f.startswith(stock_file_pattern) and f.endswith('.csv')]
        
        if not stock_files:
            print(f"未找到證券代號 {stock_code} 的對應數據文件，跳過...")
            continue

        stock_file_path = os.path.join(stock_data_folder, stock_files[0])
        stock_data = pd.read_csv(stock_file_path, encoding='utf-8')
        
        # 確保存在 "收盤價" 欄位
        if '收盤價' not in stock_data.columns:
            raise ValueError(f"文件 {stock_file_path} 中缺少 '收盤價' 欄位。")
        
        # 選取日期範圍內的收盤價
        stock_data['日期'] = pd.to_datetime(stock_data['日期'], format='%Y-%m-%d')
        mask = (stock_data['日期'] >= pd.to_datetime(start_date)) & (stock_data['日期'] <= pd.to_datetime(end_date))
        filtered_prices = stock_data.loc[mask, '收盤價'].reset_index(drop=True)
        
        # 確保有足夠的數據替換
        if len(filtered_prices) < 8:
            print(f"證券代號 {stock_code} 的收盤價數據不足，跳過...")
            continue
        
        # 替換第 3 到第 10 行的數據
        auction_data.loc[index, auction_data.columns[3:11]] = filtered_prices[:8].values

    # 保存更新後的文件
    updated_file_path = auction_file.replace('.csv', '_updated.csv')
    auction_data.to_csv(updated_file_path, index=False, encoding='utf-8')
    print(f"已更新文件並保存為 {updated_file_path}")

# 主程式
if __name__ == "__main__":
    auction_csv_path = "cleaned_auction_data.csv"
    stock_data_directory = "./stock_data"
    update_auction_data(auction_csv_path, stock_data_directory)
