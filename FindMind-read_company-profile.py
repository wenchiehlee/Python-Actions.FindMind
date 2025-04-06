#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FindMind-read_company-profile.py
Version 1.0.6.0

This script reads company-profile CSV files for companies listed in a source CSV,
extracts the latest industry category and type information, and outputs the results 
to a CSV file. The script ensures that all companies are included in the output, 
with empty cells for companies without valid data.
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

# Set stdout encoding to UTF-8 to handle Chinese characters
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Constants
VERSION = "1.0.6.0"
COMPANY_LIST_URL = "https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/refs/heads/main/%E7%AB%B6%E6%A8%99%E5%85%AC%E5%8F%B8(%E5%88%9D%E4%B8%8A%E5%B8%82%E6%AB%83)%E5%90%8D%E5%96%AE.csv"
OUTPUT_DIR = "auction_data_processed"  # Output directory as specified in requirements
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Features-Company.csv")  # Output file as specified in requirements
COMPANY_PROFILE_DIR = "company-profile"

def setup_directories():
    """Create necessary directories if they don't exist."""
    try:
        if not os.path.exists(COMPANY_PROFILE_DIR):
            os.makedirs(COMPANY_PROFILE_DIR)
            print(f"Created directory: {COMPANY_PROFILE_DIR}")
        else:
            print(f"Directory already exists: {COMPANY_PROFILE_DIR}")
            
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
                    # Always treat stock code as string
                    companies.append(str(row[0]).strip())
        
        # Clean up the temporary file
        os.remove(temp_file)
        
        return companies
    except Exception as e:
        print(f"Error downloading or parsing company list: {e}")
        return []

def process_company_profile_files(company_code):
    """
    Process company-profile files for a specific company code.
    
    Args:
        company_code (str): The company stock code.
        
    Returns:
        tuple: (industry_category, company_type) or (None, None) if error.
    """
    try:
        # Look for files with exact format including brackets
        all_files = os.listdir(COMPANY_PROFILE_DIR) if os.path.exists(COMPANY_PROFILE_DIR) else []
        files = []
        
        # Find files that match the original expected pattern
        pattern = f"[{company_code}]"
        for file in all_files:
            if pattern in file and file.endswith("-company-profile.csv"):
                files.append(os.path.join(COMPANY_PROFILE_DIR, file))
        
        if not files:
            print(f"No company-profile files found for company {company_code}")
            return None, None
        
        print(f"Found {len(files)} company-profile files for company {company_code}")
        
        # Track the newest data for industry category
        latest_date = None
        latest_industry_category = None
        
        # Collection of all non-null type values
        all_types = []
        
        # Process each file
        for file_path in files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # Ensure the required columns exist
                required_columns = ["日期", "行業類別", "類型"]
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    print(f"Warning: File {file_path} is missing columns: {missing_columns}")
                    continue
                
                # Convert dates to datetime for comparison
                df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                
                # Get the newest row for industry category
                if not df['日期'].isna().all() and len(df) > 0:
                    newest_row = df.loc[df['日期'].idxmax()]
                    
                    # If this is the newest data overall, update industry category
                    if latest_date is None or newest_row['日期'] > latest_date:
                        latest_date = newest_row['日期']
                        latest_industry_category = newest_row['行業類別'] if pd.notna(newest_row['行業類別']) else None
                
                # Collect all non-null type values
                types = df['類型'].dropna().unique().tolist()
                all_types.extend(types)
                
                print(f"  - Processed {os.path.basename(file_path)}: Latest date {latest_date.strftime('%Y-%m-%d') if latest_date else 'N/A'}")
            
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
                continue
        
        # Determine the most common type value if any exist
        company_type = None
        if all_types:
            # Use the most common value
            company_type = max(set(all_types), key=all_types.count)
        
        print(f"  - Extracted data: Industry Category={latest_industry_category} (newest), Type={company_type} (most common)")
        return latest_industry_category, company_type
    
    except Exception as e:
        print(f"Error processing company-profile files for company {company_code}: {e}")
        return None, None

def write_output_file(company_data):
    """
    Write the output CSV file with company metrics to Features-Company.csv.
    
    Args:
        company_data (list): List of dictionaries containing company data.
        
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Create a dictionary of new company data keyed by stock code for easier lookup
        new_data_dict = {str(comp['股票代號']): comp for comp in company_data}
        
        # Prepare final data structure
        final_data = {}
        
        # If output file exists, read it first to preserve existing data
        if os.path.exists(OUTPUT_FILE):
            try:
                # Read the existing CSV file, forcing stock code to be string
                existing_df = pd.read_csv(OUTPUT_FILE, encoding='utf-8', dtype={'股票代號': str})
                print(f"Reading existing file with {len(existing_df)} companies")
                
                # Process each row in the existing file
                for _, row in existing_df.iterrows():
                    # Always treat stock code as string to match input data
                    stock_code = str(row['股票代號']) if not pd.isna(row['股票代號']) else None
                    
                    if stock_code and stock_code not in final_data:
                        # Convert row to dictionary and store
                        row_dict = row.to_dict()
                        final_data[stock_code] = row_dict
            except Exception as e:
                print(f"Warning: Error reading existing file: {e}")
        
        # Update or add company profile data
        for stock_code, company in new_data_dict.items():
            if stock_code in final_data:
                # Update existing company data
                if '行業類別' in company and company['行業類別']:
                    final_data[stock_code]['行業類別'] = company['行業類別']
                if '類型' in company and company['類型']:
                    final_data[stock_code]['類型'] = company['類型']
            else:
                # Add new company
                final_data[stock_code] = company
        
        # Convert dictionary back to list for DataFrame
        final_list = list(final_data.values())
        print(f"Preparing to write {len(final_list)} companies to output file")
        
        # Create DataFrame with all data
        df = pd.DataFrame(final_list)
        
        # Ensure all required columns exist
        required_columns = ['股票代號', '行業類別', '類型']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
        # First, write to a temporary file to avoid permission issues
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', newline='', delete=False) as temp_file:
            temp_filename = temp_file.name
            # Add dtype option to prevent pandas from converting stock codes to float
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
    print(f"Starting FindMind-read_company-profile.py (Version {VERSION})")
    
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
            "行業類別": "",
            "類型": ""
        }
        
        # Try to get metrics for this company
        industry_category, company_type = process_company_profile_files(company_code)
        
        # Update the metrics if available
        if industry_category is not None:
            company_entry["行業類別"] = industry_category
        if company_type is not None:
            company_entry["類型"] = company_type
        
        # Add the company entry to our data
        company_data.append(company_entry)
    
    # Write output to Features-Company.csv, even if it already exists
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