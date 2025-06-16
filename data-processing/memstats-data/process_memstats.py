#!/usr/bin/env python3
import sys
import os
import re
import json
import sqlite3
import gzip
from collections import defaultdict

# Configuration 
def setup_database(db_path):
    print(f"INFO: Setting up 'memstats' table in database: {db_path}...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS memstats (
                timestamp           TEXT PRIMARY KEY,
                alloc_bytes         INTEGER,
                sys_bytes           INTEGER,
                num_gc              INTEGER,
                source_file         TEXT
            )
        """)
        conn.commit()
    print("INFO: Database setup complete.")


def process_memstats_file(db_path, log_file_path):
    """
    Processes a single memstats log file by splitting its content by timestamp
    """
    print(f"INFO: Processing file: {log_file_path}")
    
    records_to_insert = []
    source_filename = os.path.basename(log_file_path)

    # This pattern will be used to split the entire file content into records
    log_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})')

    open_func = gzip.open if log_file_path.endswith('.gz') else open
    
    with open_func(log_file_path, 'rt', encoding='utf-8', errors='ignore') as f:
        content = f.read()
        
        # Split the file content by the timestamp pattern.
        entries = log_pattern.split(content)
        
        # The first element is usually an empty string before the first timestamp
        if entries and not entries[0].strip():
            entries = entries[1:]
        
        # Process entries in pairs (timestamp, json_blob)
        for i in range(0, len(entries), 2):
            if i + 1 >= len(entries):
                continue

            timestamp_str = entries[i]
            json_blob = entries[i+1].strip()
            
            try:
                data = json.loads(json_blob)
                
                alloc = data.get("Alloc")
                sys_mem = data.get("Sys")
                num_gc = data.get("NumGC")
                
                if alloc is not None and sys_mem is not None and num_gc is not None:
                    records_to_insert.append((
                        timestamp_str,
                        int(alloc),
                        int(sys_mem),
                        int(num_gc),
                        source_filename
                    ))
            except json.JSONDecodeError:
                continue
    
    if not records_to_insert:
        print(f"INFO: No valid memstats found in {log_file_path}.")
        return

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany("INSERT OR IGNORE INTO memstats VALUES (?, ?, ?, ?, ?)", records_to_insert)
            conn.commit()
            print(f"INFO: Inserted/ignored {len(records_to_insert)} memstats records from {source_filename}.")
    except sqlite3.Error as e:
        print(f"ERROR: Failed to insert data for {log_file_path}. Error: {e}")
        
    print(f"INFO: Finished processing: {log_file_path}")


def create_indexes(db_path):
    print("\nINFO: Creating index on the memstats table...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_memstats_timestamp ON memstats (timestamp);")
        conn.commit()
    print("INFO: Index created successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 process_memstats.py <path_to_log_directory> <sqlite_db_path>")
        print("Example: python3 process_memstats.py ./geth_logs/ geth_memstats.db")
        sys.exit(1)

    log_dir = sys.argv[1]
    db_file_path = sys.argv[2]

    if not os.path.isdir(log_dir):
        print(f"Error: Log directory not found at '{log_dir}'")
        sys.exit(1)

    if os.path.exists(db_file_path):
        os.remove(db_file_path)
        print(f"INFO: Removed existing database file to create a new one: {db_file_path}")

    setup_database(db_file_path)

    # Find and process all relevant log files starting with 'memstats'
    log_files = [os.path.join(log_dir, f) for f in sorted(os.listdir(log_dir)) if f.startswith('memstats')]
    
    print(f"INFO: Found {len(log_files)} log files to process.")
    
    for file_path in log_files:
        process_memstats_file(db_file_path, file_path)
        
    create_indexes(db_file_path)
    
    print("\n--- Processing Complete! ---")
    print(f"Memstats data saved to: {db_file_path}")