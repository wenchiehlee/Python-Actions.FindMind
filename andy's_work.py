import os
import pandas as pd

def find_price_file(security_code):
    """Locate the CSV file containing the closing price for the given security code."""
    for file_name in os.listdir('.'):  # Assumes all relevant files are in the current directory
        if file_name.startswith(f"[{security_code}]") and file_name.endswith(".csv"):
            return file_name
    return None

def update_cleaned_data():
    # Load cleaned auction data
    auction_file = 'cleaned_auction_data.csv'
    auction_data = pd.read_csv(auction_file, delimiter='\t')

    # Iterate through the securities in the file
    for _, row in auction_data.iterrows():
        security_code = row['證券代號']
        price_file = find_price_file(security_code)

        if not price_file:
            print(f"Price file for {security_code} not found.")
            continue

        # Load the price file and ensure it contains '收盤價'
        price_data = pd.read_csv(price_file)
        if '收盤價' not in price_data.columns:
            print(f"Closing price ('收盤價') column missing in file {price_file}.")
            continue

        # Replace 3rd to 10th rows with closing prices
        date_columns = ['投標開始日(T-4)', '投標結束日(T-2)', '開標日期(T)', '撥券日(上市上櫃日)', 'T+7', 'DateStart', 'DateEnd+14']
        for col in date_columns:
            if col in auction_data.columns:
                try:
                    auction_data.at[_, col] = price_data.loc[price_data['日期'] == row[col], '收盤價'].values[0]
                except (KeyError, IndexError):
                    print(f"Closing price not found for {security_code} on {row[col]}.")

    # Save the updated file
    auction_data.to_csv('cleaned_auction_data_updated.csv', index=False)
    print("Updated data saved to cleaned_auction_data_updated.csv")

if __name__ == "__main__":
    update_cleaned_data()
