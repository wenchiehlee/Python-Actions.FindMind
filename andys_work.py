import os
import pandas as pd

# Create output directory
output_dir = "processed_data"
os.makedirs(output_dir, exist_ok=True)

# Load cleaned auction data
auction_data_file = "cleaned_auction_data.csv"
auction_data = pd.read_csv(auction_data_file)

# Iterate over each security in the auction data
for index, row in auction_data.iterrows():
    security_id = row["證券代號"]
    start_date = row["投標開始日(T-4)"]
    end_date = row["撥券日(上市上櫃日) T+7"]

    # Look for the corresponding closing price file
    closing_price_file = f"/mnt/data/{security_id}.csv"
    if not os.path.exists(closing_price_file):
        print(f"Missing file for security: {security_id}")
        continue

    # Load closing price data
    closing_data = pd.read_csv(closing_price_file)

    # Ensure '收盤價' column exists
    if "收盤價" not in closing_data.columns:
        print(f"'收盤價' column missing in file: {closing_price_file}")
        continue

    # Update auction data rows with closing prices
    for date_col in ["申請日期", "上櫃審議委員會審議日期", "櫃買董事會通過上櫃日期",
                     "櫃買同意上櫃契約日期", "投標開始日(T-4)", "投標結束日(T-2)",
                     "開標日期(T)", "撥券日(上市上櫃日) T+7"]:
        date_value = row[date_col]
        matching_row = closing_data[closing_data["日期"] == date_value]
        if not matching_row.empty:
            auction_data.at[index, date_col] = matching_row.iloc[0]["收盤價"]

# Save the updated auction data
output_file = os.path.join(output_dir, "updated_cleaned_auction_data.csv")
auction_data.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Updated data saved to {output_file}")
