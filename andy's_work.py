import os
import pandas as pd

# Function to read the cleaned auction data
def read_cleaned_auction_data(file_path):
    return pd.read_csv(file_path, encoding='utf-8')

# Function to find the corresponding price file based on stock code
def find_price_file(stock_code, directory):
    for file_name in os.listdir(directory):
        if file_name.startswith(f"[{stock_code}]") and file_name.endswith(".csv"):
            return os.path.join(directory, file_name)
    return None

# Function to update auction data with closing prices
def update_auction_data(cleaned_file, price_data_directory, output_file):
    auction_data = read_cleaned_auction_data(cleaned_file)

    # Columns to update (3rd to 10th)
    cols_to_update = ["投標開始日(T-4)", "投標結束日(T-2)", "開標日期(T)", "撥券日(上市上櫃日) T+7", "DateStart", "DateEnd+14"]

    for index, row in auction_data.iterrows():
        stock_code = row["證券代號"]
        price_file = find_price_file(stock_code, price_data_directory)

        if price_file:
            price_data = pd.read_csv(price_file, encoding='utf-8')
            if "收盤價" not in price_data.columns:
                print(f"Error: '收盤價' column not found in {price_file}")
                continue

            # Replace specified rows (3rd to 10th) with closing prices
            closing_prices = price_data["收盤價"][:len(cols_to_update)].values
            auction_data.loc[index, cols_to_update] = closing_prices
        else:
            print(f"Price file not found for stock code: {stock_code}")

    # Save the updated data to a new file
    auction_data.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Updated data saved to {output_file}")

if __name__ == "__main__":
    cleaned_auction_data_file = "cleaned_auction_data.csv"
    price_data_directory = "price_data"  # Directory containing price files
    output_file = "updated_auction_data.csv"

    update_auction_data(cleaned_auction_data_file, price_data_directory, output_file)
