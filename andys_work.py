"""import os
import pandas as pd

def update_auction_data():
    input_file = "cleaned_auction_data.csv"
    output_dir = "updated_data"
    os.makedirs(output_dir, exist_ok=True)

    # 讀取主檔案
    auction_data = pd.read_csv(input_file)
    date_columns = [
        "申請日期",
        "上櫃審議委員會審議日期",
        "櫃買董事會通過上櫃日期",
        "櫃買同意上櫃契約日期",
        "投標開始日(T-4)",
        "投標結束日(T-2)",
        "開標日期(T)",
        "撥券日(上市上櫃日) T+7"
    ]

    # 處理每一個證券代號
    for index, row in auction_data.iterrows():
        security_id = row['證券代號']
        relevant_file = None

        # 搜索對應的收盤價檔案
        for file_name in os.listdir():
            if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
                relevant_file = file_name
                break

        if not relevant_file:
            print(f"未找到對應的收盤價檔案：{security_id}")
            continue

        # 讀取收盤價資料
        closing_data = pd.read_csv(relevant_file)
        if '收盤價' not in closing_data.columns or '日期' not in closing_data.columns:
            print(f"收盤價檔案缺少必要欄位：{relevant_file}")
            continue

        closing_data['日期'] = pd.to_datetime(closing_data['日期'])
        closing_price_map = dict(zip(closing_data['日期'].dt.date, closing_data['收盤價']))

        # 替換日期為收盤價
        for col in date_columns:
            if col in row and pd.notnull(row[col]):
                try:
                    date = pd.to_datetime(row[col]).date()
                    auction_data.at[index, col] = closing_price_map.get(date, "無收盤價")
                except Exception as e:
                    print(f"日期轉換錯誤於 {security_id}, 欄位 {col}: {e}")

    # 儲存處理後的檔案
    output_file = os.path.join(output_dir, "updated_auction_data.csv")
    auction_data.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"更新後的檔案已儲存至 {output_file}")

if __name__ == "__main__":
    update_auction_data()"""











      














import os
import pandas as pd

# 創建輸出資料夾名稱
output_dir = "auction_data_processed"
os.makedirs(output_dir, exist_ok=True)

# 讀取 cleaned_auction_data.csv 檔案
cleaned_auction_data_path = "cleaned_auction_data (1).csv"
auction_data = pd.read_csv(cleaned_auction_data_path, encoding='utf-8')

# 定義需要處理的日期欄位
date_columns = [
    "申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
    "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
    "開標日期(T)", "撥券日(上市上櫃日) T+7"
]

# 獲取所有文件列表
all_files = os.listdir()

# 定義一個函數以匹配代號相關的檔案並讀取收盤價
def get_closing_price(security_id, date):
    for file_name in all_files:
        if file_name.startswith(f"[{security_id}]") and file_name.endswith(".csv"):
            file_path = file_name
            try:
                price_data = pd.read_csv(file_path, encoding='utf-8')
                # 確保日期格式一致 (YYYY-MM-DD)
                price_data['日期'] = pd.to_datetime(price_data['日期'], errors='coerce').dt.date
                date_obj = pd.to_datetime(date, errors='coerce').date()
                closing_price_row = price_data.loc[price_data['日期'] == date_obj]
                if not closing_price_row.empty:
                    return closing_price_row['收盤價'].values[0]
            except (KeyError, FileNotFoundError, pd.errors.EmptyDataError):
                continue
    return None

# 更新資料中的日期欄位
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    for column in date_columns:
        if pd.notna(row[column]):  # 確保日期欄位不為空
            closing_price = get_closing_price(security_id, row[column])
            if closing_price is not None:
                auction_data.at[index, column] = closing_price
            else:
                auction_data.at[index, column] = None  # 未找到收盤價

# 儲存更新的資料至新的檔案中
output_path = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f"已完成資料處理並儲存至 {output_path}")


# 將更新後的資料存入新檔案
updated_file_path = os.path.join(output_folder, "updated_cleaned_auction_data.csv")
auction_data.to_csv(updated_file_path, index=False, encoding="utf-8")
print(f"已將更新後的資料儲存至 {updated_file_path}")
