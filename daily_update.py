import sys
import os
import json
import duckdb
import requests
import xml.etree.ElementTree as ET
import csv
from datetime import datetime
from pathlib import Path

# Reuse constants and helpers from download_nyc_spending
from download_nyc_spending import make_api_request, parse_transactions, load_progress, save_progress, save_chunk

# Configuration
API_URL = "https://www.checkbooknyc.com/api"

def get_current_fiscal_year():
    """Calculate NYC Fiscal Year (July 1st starts new FY)"""
    today = datetime.now()
    if today.month >= 7:
        return today.year + 1
    return today.year

def daily_update():
    fiscal_year = get_current_fiscal_year()
    print(f"Checking for updates for FY{fiscal_year}...")

    # Paths
    base_dir = Path("data_pipeline/raw")
    parquet_dir = Path("data_pipeline/parquet")
    progress_file = base_dir / f"progress_fy{fiscal_year}.json"
    
    # Ensure directories exist
    chunks_dir = base_dir / f"fy{fiscal_year}_chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)
    
    output_parquet_dir = parquet_dir / f"fiscal_year={fiscal_year}"
    output_parquet_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load Local State
    progress = load_progress(progress_file)
    last_count = progress['last_record_downloaded']
    print(f"Local state: {last_count:,} records downloaded so far.")

    # 2. Check Remote State
    print("Querying API for current record count...")
    response = make_api_request(1, 1, fiscal_year) # Just get 1 record to get the header count
    if response.status_code != 200:
        print(f"Error checking API: {response.status_code}")
        return

    _, total_records_remote = parse_transactions(response.text)
    print(f"Remote state: {total_records_remote:,} records available.")

    # 3. Calculate Delta
    new_records_count = total_records_remote - last_count
    
    if new_records_count <= 0:
        print("No new records found. System is up to date.")
        return

    print(f"Found {new_records_count:,} new records!")

    # 4. Download Delta
    fieldnames = ['agency', 'payee_name', 'check_amount', 'fiscal_year', 'issue_date',
                  'industry', 'spending_category', 'contract_id', 'department', 
                  'expense_category', 'budget_code', 'sub_vendor', 'associated_prime_vendor']

    # We download in one "Daily Chunk" if possible, or split if it's huge
    # For a daily update, 200-1000 records is expected.
    
    start_record = last_count + 1
    end_record = total_records_remote
    
    print(f"Downloading records {start_record:,} to {end_record:,}...")

    chunk_records = []
    current_record = start_record
    
    # Fetch in batches if necessary (API limit)
    while current_record <= end_record:
        batch_size = min(20000, end_record - current_record + 1)
        resp = make_api_request(current_record, batch_size, fiscal_year)
        if resp.status_code == 200:
            transactions, _ = parse_transactions(resp.text)
            if transactions:
                chunk_records.extend(transactions)
                print(f"  Downloaded {len(transactions)} records...")
            else:
                print("  Warning: No records returned in batch.")
        else:
            print(f"  Error downloading batch: {resp.status_code}")
            return
        current_record += batch_size

    # 5. Save Raw CSV
    today_str = datetime.now().strftime('%Y-%m-%d')
    # Use a distinct naming pattern for updates so they don't conflict with bulk chunks
    chunk_filename = f"update_{today_str}_{start_record}-{end_record}.csv"
    chunk_path = chunks_dir / chunk_filename
    
    with open(chunk_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk_records)
    
    print(f"Saved raw update to {chunk_path}")

    # 6. Convert to Parquet (Immediate conversion for availability)
    parquet_filename = f"update_{today_str}_{start_record}-{end_record}.parquet"
    parquet_path = output_parquet_dir / parquet_filename
    
    con = duckdb.connect()
    query = f"""
        COPY (SELECT * FROM read_csv_auto('{chunk_path}', all_varchar=1)) 
        TO '{parquet_path}' (FORMAT 'PARQUET', CODEC 'SNAPPY');
    """
    con.execute(query)
    print(f"Converted to Parquet: {parquet_path}")

    # 7. Update State
    progress['last_record_downloaded'] = end_record
    progress['total_records'] = total_records_remote
    progress['chunks_completed'].append(str(chunk_path))
    save_progress(progress, progress_file)
    
    print("State updated. Daily update complete!")
    print("\nNext: Upload the new parquet file to S3.")
    print(f"aws s3 cp {parquet_path} s3://your-bucket/data/year={fiscal_year}/")

if __name__ == "__main__":
    daily_update()
