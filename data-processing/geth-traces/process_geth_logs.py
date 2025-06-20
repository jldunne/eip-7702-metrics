#!/usr/bin/env python3
import sys
import os
import re
import sqlite3
import gzip
from collections import defaultdict

DEFAULT_YEAR = "2025"
ERROR_MAP = {
    "nonce too low": "invalidation_nonce_low",
    "nonce too high": "invalidation_nonce_high",
    "nonce has max value": "invalidation_nonce_max_value",
    "intrinsic gas too low": "invalidation_intrinsic_gas",
    "insufficient funds for gas * price + value": "invalidation_insufficient_funds",
    "transaction size": "invalidation_oversized_data",
    "transaction gas price below minimum": "gas_price_below_min",
    "max priority fee per gas higher than max fee per gas": "max_fee_too_high",
    "insufficient gas for floor data gas cost": "insufficient_gas_for_floor",
    "exceeds block gas limit": "exceeds_block_gas",
    "set code tx must have at least one authorization tuple": "invalidation_auth_list_empty",
    "EIP-7702 transaction with empty auth list": "invalidation_auth_list_empty_7702",
    "EIP-7702 transaction cannot be used to create contract": "invalidation_setcode_tx_create",
    "EIP-7702 authorization chain ID mismatch": "invalidation_auth_wrong_chain_id",
    "EIP-7702 authorization has invalid signature": "invalidation_auth_sig_invalid",
    "EIP-7702 authorization destination is a contract": "invalidation_auth_destination_is_contract",
    "EIP-7702 authorization nonce does not match current account nonce": "invalidation_auth_nonce_mismatch",
    "gapped-nonce tx from delegated accounts": "invalidation_gapped_nonce",
    "in-flight transaction limit reached for delegated accounts": "invalidation_tx_limit_reached",
    "authority already reserved": "invalidation_authority_reserved"
}

def setup_database(db_path):
    print(f"INFO: Setting up SQLite database at {db_path}...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS geth_metrics (
                timestamp           TEXT,
                metric_category     TEXT,
                metric_name         TEXT,
                count               INTEGER,
                source_file         TEXT,
                PRIMARY KEY (timestamp, metric_name, source_file)
            )
        """)
        conn.commit()
    print("INFO: Database setup complete.")


def process_log_file(db_path, log_file_path):
    """
    Processes a Geth log file line-by-line and inserts aggregated metrics
    into the SQLite database.
    """
    print(f"INFO: Processing file: {log_file_path}")
    
    counts = defaultdict(lambda: defaultdict(int))
    
    year_match = re.search(r'(\d{4})-\d{2}-\d{2}', log_file_path)
    year = year_match.group(1) if year_match else DEFAULT_YEAR

    log_pattern = re.compile(r'\[(\d{2}-\d{2})\|(\d{2}:\d{2}):\d{2}\.\d{3}\]\s*(.*)')

    open_func = gzip.open if log_file_path.endswith('.gz') else open
    
    with open_func(log_file_path, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = log_pattern.search(line)
            if not match:
                continue

            minute_ts_str = f"{year}-{match.group(1).replace('-', '-')} {match.group(2)}"
            log_content = match.group(3).strip()

            # Categorize and Count Events
            if "Discarding invalid transaction" in log_content:
                metric_found = False
                for err_string, metric_name in ERROR_MAP.items():
                    if err_string in log_content:
                        counts[minute_ts_str][metric_name] += 1
                        metric_found = True
                        break
                if not metric_found:
                    counts[minute_ts_str]["invalidation_other"] += 1
            
            elif "Discarding freshly underpriced transaction" in log_content:
                counts[minute_ts_str]["mempool_underpriced"] += 1
            
            elif "Discarding future transaction replacing pending tx" in log_content:
                counts[minute_ts_str]["mempool_replaced"] += 1

    if not counts:
        print(f"INFO: No relevant metrics found in {log_file_path}.")
        return

    records_to_insert = []
    source_filename = os.path.basename(log_file_path)
    for ts, metric_dict in counts.items():
        for metric, count in metric_dict.items():
            category = metric.split('_')[0]
            records_to_insert.append((ts, category, metric, count, source_filename))
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.executemany("INSERT OR IGNORE INTO geth_metrics VALUES (?, ?, ?, ?, ?)", records_to_insert)
            conn.commit()
            print(f"INFO: Inserted/ignored {len(records_to_insert)} aggregated metric records for {source_filename}.")
    except sqlite3.Error as e:
        print(f"ERROR: Failed to insert data for {log_file_path}. Error: {e}")
        
    print(f"INFO: Finished processing: {log_file_path}")


def create_final_summary_and_indexes(db_path):
    """Creates a final aggregated summary table and adds indexes for faster querying."""
    print("\nINFO: Creating final aggregated summary table and indexes...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        cursor.execute("DROP TABLE IF EXISTS geth_metrics_summary;")
        cursor.execute("""
            CREATE TABLE geth_metrics_summary AS
            SELECT
                timestamp,
                metric_category,
                metric_name,
                SUM(count) as total_count
            FROM geth_metrics
            GROUP BY 1, 2, 3;
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_summary_timestamp ON geth_metrics_summary (timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_summary_metric_name ON geth_metrics_summary (metric_name);")
        
        conn.commit()
    print("INFO: Final summary and indexes created successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 process_geth_logs.py <path_to_log_directory> <sqlite_db_path>")
        sys.exit(1)

    log_dir = sys.argv[1]
    db_file_path = sys.argv[2]

    if not os.path.isdir(log_dir):
        print(f"Error: Log directory not found at '{log_dir}'")
        sys.exit(1)
        
    if os.path.exists(db_file_path):
        os.remove(db_file_path)
        print(f"INFO: Removed existing database file: {db_file_path}")

    setup_database(db_file_path)

    # Find and process all relevant log files starting with 'geth'
    log_files = [os.path.join(log_dir, f) for f in sorted(os.listdir(log_dir)) if f.startswith('geth')] 
    
    print(f"INFO: Found {len(log_files)} log files to process.")
    
    for file_path in log_files:
        process_log_file(db_file_path, file_path)
        
    create_final_summary_and_indexes(db_file_path)
    
    print("\n--- Processing Complete! ---")
    print(f"Database saved to: {db_file_path}")
