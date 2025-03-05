#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FindMind-read_PER_PBR.py
Version 1.0.6.0

This script reads PER_PBR CSV files for companies listed in a source CSV,
calculates average values for key metrics, and outputs the results to a CSV file.
The script ensures that all companies are included in the output, with empty cells
for companies without valid data.
"""

import sys
import io
import os
import csv
import urllib.request
import pandas as pd
import glob
from pathlib import Path
import numpy as np
import stat
import tempfile
import shutil

# Set stdout encoding to UTF-8 to handle Chinese characters
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Constants
VERSION = "1.0.6.0"
COMPANY_LIST_URL = "https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv"
OUTPUT_DIR = "auction_data_processed"  # Output directory as specified in requirements
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Features-Company.csv")  # Output file as specified in requirements
OUTPUT_COLUMNS = ["股票代號", "股息殖利率", "PER", "PBR"]  # Required columns for output
PER_PBR_DIR = "PER_PBR"

def setup_directories():
    """Create necessary directories if they don't exist."""
    try:
        if not os.path.exists(PER_PBR_DIR):
            os.makedirs(PER_PBR_DIR)
            print(f"Created directory: {PER_PBR_DIR}")
        else:
            print(f"Directory already exists: {PER_PBR_DIR}")
            
        # Create output directory if it doesn't exist
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            print(f"Created output directory: {OUTPUT_DIR}")
        else:
            print(f"Output directory already exists: {OUTPUT_DIR}")
            
        return True
    except Exception as e:
        print(f"Error creating directories: {e}")
        return False

def download_company_list():
    """Download the company list CSV file."""
    try:
        # Create a temporary file to store the downloaded content
        temp_file = "temp_company_list.csv"
        urllib.request.urlretrieve(COMPANY_LIST_URL, temp_file)
        
        # Read the CSV file
        companies = []
        with open(temp_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Skip header if it exists
            try:
                header = next(reader)
                # Check if the first row is a header by looking for expected column name
                if "股票代號" not in header:
                    companies.append(str(header[0]).strip())  # If not a header, add it as a company
            except StopIteration:
                pass  # File is empty
            
            # Read the rest of the rows
            for row in reader:
                if row and row[0].strip():  # Skip empty rows or rows with empty stock codes
                    companies.append(str(row[0]).strip())  # Explicitly convert to string
        
        # Clean up the temporary file
        os.remove(temp_file)
        
        return companies
    except Exception as e:
        print(f"Error downloading or parsing company list: {e}")
        return []

def process_per_pbr_files(company_code):
    """
    Process PER_PBR files for a specific company code.
    
    Args:
        company_code (str): The company stock code.
        
    Returns:
        tuple: (dividend_yield_avg, per_avg, pbr_avg) or (None, None, None) if error.
    """
    try:
        # Look for files with exact format including brackets
        all_files = os.listdir(PER_PBR_DIR) if os.path.exists(PER_PBR_DIR) else []
        files = []
        
        # Find files that match the pattern [company_code]* in the directory listing
        pattern = f"[{company_code}]"
        for file in all_files:
            if pattern in file and file.endswith("-PER_PBR.csv"):
                files.append(os.path.join(PER_PBR_DIR, file))
        
        if not files:
            print(f"No PER_PBR files found for company {company_code}")
            return None, None, None
        
        print(f"Found {len(files)} PER_PBR files for company {company_code}")
        
        # Initialize lists to hold all values
        all_dividend_yields = []
        all_pers = []
        all_pbrs = []
        
        # Process each file
        for file_path in files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # Ensure the required columns exist
                required_columns = ["股息殖利率", "PER", "PBR"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    print(f"Warning: File {file_path} is missing columns: {missing_columns}")
                    continue
                
                # Extract values and filter out non-numeric, zero, or missing values
                # For dividend yield, accept zero as valid (some companies don't pay dividends)
                dividend_yields = pd.to_numeric(df["股息殖利率"], errors='coerce').dropna().tolist()
                
                # For PER and PBR, exclude zeros as they're likely invalid
                per_series = pd.to_numeric(df["PER"], errors='coerce')
                per_series = per_series[(~per_series.isna()) & (per_series > 0)]
                pers = per_series.tolist()
                
                pbr_series = pd.to_numeric(df["PBR"], errors='coerce')
                pbr_series = pbr_series[(~pbr_series.isna()) & (pbr_series > 0)]
                pbrs = pbr_series.tolist()
                
                all_dividend_yields.extend(dividend_yields)
                all_pers.extend(pers)
                all_pbrs.extend(pbrs)
                
                print(f"  - Processed {os.path.basename(file_path)}: Found {len(dividend_yields)} dividend yields, {len(pers)} PERs, {len(pbrs)} PBRs")
            
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue
        
        # Calculate averages if data exists, otherwise return None
        dividend_yield_avg = np.mean(all_dividend_yields) if all_dividend_yields else None
        per_avg = np.mean(all_pers) if all_pers else None
        pbr_avg = np.mean(all_pbrs) if all_pbrs else None
        
        # Round to 1 decimal place if value exists
        if dividend_yield_avg is not None:
            dividend_yield_avg = round(dividend_yield_avg, 1)
        if per_avg is not None:
            per_avg = round(per_avg, 1)
        if pbr_avg is not None:
            pbr_avg = round(pbr_avg, 1)
        
        if dividend_yield_avg is not None or per_avg is not None or pbr_avg is not None:
            print(f"  - Calculated averages: Dividend yield={dividend_yield_avg}, PER={per_avg}, PBR={pbr_avg}")
        
        return dividend_yield_avg, per_avg, pbr_avg
    
    except Exception as e:
        print(f"Error processing PER_PBR files for company {company_code}: {e}")
        return None, None, None

def write_output_file(company_data):
    """
    Write the output CSV file with company metrics to Features-Company.csv.
    
    This function implements requirement #2:
    "Write `Features-Company.csv` columns defined as `股票代號`,`股息殖利率`,`PER`,`PBR`
    where `股息殖利率` is average value of `股息殖利率` column from all rows of PER_PBR files
    where `PER` is average value of `PER` column from all rows of PER_PBR files
    where `PBR` is average value of `PBR` column from all rows of PER_PBR files"
    
    Args:
        company_data (list): List of dictionaries containing company metrics.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Check if we have any data to write
        if not company_data:
            print("No company data to write to output file.")
            return False
        
        # If the output file already exists, read it first to preserve existing data
        final_data = {}
        if os.path.exists(OUTPUT_FILE):
            try:
                existing_df = pd.read_csv(OUTPUT_FILE, encoding='utf-8', dtype={'股票代號': str})
                
                # Convert existing data to dictionary keyed by stock code (as string)
                for _, row in existing_df.iterrows():
                    stock_code = str(row['股票代號']) if not pd.isna(row['股票代號']) else None
                    if stock_code:
                        final_data[stock_code] = row.to_dict()
                        
                print(f"Read existing data for {len(final_data)} companies from {OUTPUT_FILE}")
            except Exception as e:
                print(f"Warning: Error reading existing file: {e}")
        
        # Update or add new company data
        for company in company_data:
            stock_code = str(company['股票代號'])
            if stock_code in final_data:
                # Update PER/PBR metrics while preserving other columns
                if '股息殖利率' in company and company['股息殖利率'] != "":
                    final_data[stock_code]['股息殖利率'] = company['股息殖利率']
                if 'PER' in company and company['PER'] != "":
                    final_data[stock_code]['PER'] = company['PER']
                if 'PBR' in company and company['PBR'] != "":
                    final_data[stock_code]['PBR'] = company['PBR']
            else:
                # Add new company
                final_data[stock_code] = company
        
        # Convert back to list for writing
        company_list = list(final_data.values())
        
        # Create DataFrame for CSV writing
        df = pd.DataFrame(company_list)
        
        # Ensure required columns exist
        required_columns = ['股票代號', '股息殖利率', 'PER', 'PBR']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
        # Reorder columns to ensure '股票代號' is first
        column_order = ['股票代號']
        for col in required_columns[1:]:
            if col in df.columns:
                column_order.append(col)
        # Add any other columns that might be in the data
        for col in df.columns:
            if col not in column_order:
                column_order.append(col)
        
        df = df[column_order]
        
        # First, write to a temporary file to avoid permission issues
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', newline='', delete=False) as temp_file:
            temp_filename = temp_file.name
            df.to_csv(temp_filename, index=False)
        
        # Try to handle permissions and replace the original file
        try:
            # If the output file exists, make sure it's writable
            if os.path.exists(OUTPUT_FILE):
                # Try to make the file writable if it's not
                try:
                    os.chmod(OUTPUT_FILE, stat.S_IWRITE | stat.S_IREAD)
                except Exception as e:
                    print(f"Warning: Could not change file permissions: {e}")
                    
                # Try to remove the existing file
                try:
                    os.remove(OUTPUT_FILE)
                except Exception as e:
                    print(f"Warning: Could not remove existing file: {e}")
            
            # Copy the temporary file to the output file
            shutil.copy2(temp_filename, OUTPUT_FILE)
            
            # Clean up the temporary file
            os.remove(temp_filename)
            
            print(f"Successfully wrote data for {len(df)} companies to {OUTPUT_FILE}")
            return True
        
        except Exception as e:
            # If we encountered an error, try a backup approach with a different filename
            backup_file = OUTPUT_FILE.replace('.csv', '_new.csv')
            print(f"Error writing to {OUTPUT_FILE}: {e}")
            print(f"Writing to backup file {backup_file} instead")
            
            # Copy the temp file to the backup filename
            shutil.copy2(temp_filename, backup_file)
            os.remove(temp_filename)
            
            print(f"Successfully wrote data for {len(df)} companies to {backup_file}")
            print(f"Please manually rename {backup_file} to {OUTPUT_FILE} if needed")
            return True
    
    except Exception as e:
        print(f"Error writing output file {OUTPUT_FILE}: {e}")
        return False

def check_output_exists():
    """
    Check if the output file already exists.
    Returns True if the file exists, False otherwise.
    """
    exists = os.path.exists(OUTPUT_FILE)
    if exists:
        try:
            print(f"Output file {OUTPUT_FILE} already exists.")
        except UnicodeEncodeError:
            print("Output file already exists.")
    return exists

def main():
    """Main function to orchestrate the script execution."""
    print(f"Starting FindMind-read_PER_PBR.py (Version {VERSION})")
    
    # Check if output already exists
    file_exists = check_output_exists()
    
    # Setup directories
    if not setup_directories():
        print("Failed to setup directories. Exiting.")
        return
    
    # Download company list
    print("Downloading company list...")
    companies = download_company_list()
    if not companies:
        print("Failed to get company list. Exiting.")
        return
    
    print(f"Processing data for {len(companies)} companies...")
    
    # Process each company
    company_data = []
    for i, company_code in enumerate(companies):
        print(f"Processing company {i+1}/{len(companies)}: {company_code}")
        
        # Always add the company to company_data, regardless of whether we have metrics
        company_entry = {
            "股票代號": company_code,
            "股息殖利率": "",
            "PER": "",
            "PBR": ""
        }
        
        # Try to get metrics for this company
        dividend_yield_avg, per_avg, pbr_avg = process_per_pbr_files(company_code)
        
        # Update the metrics if available
        if dividend_yield_avg is not None:
            company_entry["股息殖利率"] = dividend_yield_avg
        if per_avg is not None:
            company_entry["PER"] = per_avg
        if pbr_avg is not None:
            company_entry["PBR"] = pbr_avg
        
        # Add the company entry to our data
        company_data.append(company_entry)
    
    # Write output to the CSV file, even if it already exists
    try:
        print("Writing data to output file...")
        if write_output_file(company_data):
            print(f"Successfully created/updated output file with data for {len(company_data)} companies")
        else:
            print("Failed to write data to output file")
    except UnicodeEncodeError:
        # Fallback for console output if encoding issues occur
        print("Writing data to output CSV file...")
        if write_output_file(company_data):
            print("Successfully created/updated output CSV file")
        else:
            print("Failed to write data to output CSV file")
    
    print("Script execution completed.")

if __name__ == "__main__":
    main()