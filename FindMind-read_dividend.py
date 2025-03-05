#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FindMind-read_dividend.py
Version 1.0.6.0

This script reads dividend CSV files for companies listed in a source CSV,
extracts the most recent dividend information, calculates the per-share dividend amount,
and outputs the results to a CSV file. The script ensures that all companies are included
in the output, with empty cells for companies without valid data.
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
from datetime import datetime
import re

# Set stdout encoding to UTF-8 to handle Chinese characters
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Constants
VERSION = "1.0.6.0"
COMPANY_LIST_URL = "https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv"
OUTPUT_DIR = "auction_data_processed"  # Output directory as specified in requirements
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Features-Company.csv")  # Output file as specified in requirements
DIVIDEND_DIR = "dividend"

def setup_directories():
    """Create necessary directories if they don't exist."""
    try:
        if not os.path.exists(DIVIDEND_DIR):
            os.makedirs(DIVIDEND_DIR)
            print(f"Created directory: {DIVIDEND_DIR}")
        else:
            print(f"Directory already exists: {DIVIDEND_DIR}")
            
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

def find_dividend_files(company_code):
    """
    Find all dividend files for a specific company code.
    
    Args:
        company_code (str): The company stock code.
        
    Returns:
        list: List of file paths for the company's dividend files.
    """
    try:
        # Look for files with the pattern in the directory listing
        all_files = os.listdir(DIVIDEND_DIR) if os.path.exists(DIVIDEND_DIR) else []
        files = []
        
        # Find files that match the pattern [company_code]* in the directory listing
        pattern = f"[{company_code}]"
        for file in all_files:
            if pattern in file and file.endswith("-dividend.csv"):
                files.append(os.path.join(DIVIDEND_DIR, file))
        
        if not files:
            print(f"No dividend files found for company {company_code}")
            return []
        
        print(f"Found {len(files)} dividend files for company {company_code}")
        return files
    
    except Exception as e:
        print(f"Error finding dividend files for company {company_code}: {e}")
        return []

def extract_date_from_filename(filename):
    """Extract end date from filename for sorting."""
    match = re.search(r'(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})', filename)
    if match:
        return match.group(2)  # Return the end date for sorting
    return "1900-01-01"  # Default if no date found

def process_dividend_files(company_code):
    """
    Process dividend files for a specific company code.
    
    Args:
        company_code (str): The company stock code.
        
    Returns:
        float or None: Per share dividend amount or None if error.
    """
    try:
        # Find all dividend files for this company
        files = find_dividend_files(company_code)
        
        if not files:
            return None
        
        # Sort files by date in filename (newest first)
        files.sort(key=extract_date_from_filename, reverse=True)
        
        # Track the newest data
        latest_date = None
        per_share_dividend = None
        
        # Process each file, starting with the newest
        for file_path in files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # Ensure the required columns exist
                required_columns = ["日期", "股票收益分配", "現金盈餘分配"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    print(f"Warning: File {file_path} is missing columns: {missing_columns}")
                    continue
                
                # Convert dates to datetime for comparison
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                
                # Sort by date, most recent first
                df = df.sort_values('日期', ascending=False).reset_index(drop=True)
                
                # If we have valid date entries
                if not df['日期'].isna().all() and len(df) > 0:
                    # Get the most recent row with valid data
                    for _, row in df.iterrows():
                        # Skip rows with invalid dates
                        if pd.isna(row['日期']):
                            continue
                            
                        # Convert dividend values to numeric, handling errors
                        stock_dividend = pd.to_numeric(row['股票收益分配'], errors='coerce')
                        cash_dividend = pd.to_numeric(row['現金盈餘分配'], errors='coerce')
                        
                        # Skip rows with invalid dividend data
                        if pd.isna(stock_dividend) and pd.isna(cash_dividend):
                            continue
                        
                        # Use 0 for NaN values
                        stock_dividend = 0 if pd.isna(stock_dividend) else stock_dividend
                        cash_dividend = 0 if pd.isna(cash_dividend) else cash_dividend
                        
                        # Calculate total dividend
                        total_dividend = stock_dividend + cash_dividend
                        
                        # Round to 2 decimal places
                        total_dividend = round(total_dividend, 2)
                        
                        # Update latest data if this is the first valid row
                        if per_share_dividend is None:
                            latest_date = row['日期']
                            per_share_dividend = total_dividend
                            print(f"  - Found newest dividend data at {latest_date.strftime('%Y-%m-%d')}: {total_dividend}")
                            break
                
                # If we've found valid data, no need to process more files
                if per_share_dividend is not None:
                    break
                    
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue
        
        if per_share_dividend is not None:
            print(f"  - Extracted data: Per Share Dividend={per_share_dividend} (as of {latest_date.strftime('%Y-%m-%d')})")
        else:
            print(f"  - No valid dividend data found for company {company_code}")
            
        return per_share_dividend
    
    except Exception as e:
        print(f"Error processing dividend files for company {company_code}: {e}")
        return None

def write_output_file(company_data):
    """
    Write the output CSV file with company metrics to Features-Company.csv.
    
    Args:
        company_data (list): List of dictionaries containing company data.
        
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
                # Update dividend metric while preserving other columns
                if '每股股利' in company and company['每股股利'] != "":
                    final_data[stock_code]['每股股利'] = company['每股股利']
            else:
                # Add new company
                final_data[stock_code] = company
        
        # Convert back to list for writing
        company_list = list(final_data.values())
        
        # Create DataFrame for CSV writing
        df = pd.DataFrame(company_list)
        
        # Ensure required columns exist
        required_columns = ['股票代號', '每股股利']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
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
    print(f"Starting FindMind-read_dividend.py (Version {VERSION})")
    
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
            "每股股利": ""
        }
        
        # Try to get metrics for this company
        per_share_dividend = process_dividend_files(company_code)
        
        # Update the metrics if available
        if per_share_dividend is not None:
            company_entry["每股股利"] = per_share_dividend
        
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