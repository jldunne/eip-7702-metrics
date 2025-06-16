import json
import gzip
import os
import sys
from datetime import datetime

def process_single_log_file_local(input_file_path, 
                                  output_dir_transactions, 
                                  output_dir_snapshots):
    """
    Reads a single JSON log file from local disk, 
    shreds it, and writes individual transactions and snapshot summaries 
    to new local gzipped JSON files.
    """
    print(f"Processing local file: {input_file_path}")
    
    base_filename = os.path.basename(input_file_path)
    date_str_for_filename = "unknown_date" 
    partition_date_folder_name = "snapshot_date=unknown_date" 

    try:
        name_without_gz = base_filename.replace(".gz", "")
        possible_date_str = os.path.splitext(name_without_gz)[0]
        if os.path.splitext(possible_date_str)[1]:
             possible_date_str = os.path.splitext(possible_date_str)[0]
        
        datetime.strptime(possible_date_str, '%Y-%m-%d') 
        date_str_for_filename = possible_date_str
        partition_date_folder_name = f"snapshot_date={possible_date_str}"
        print(f"Successfully parsed date: {possible_date_str} from filename for partitioning.")
    except ValueError:
        print(f"Could not parse YYYY-MM-DD from filename: {base_filename}. Using default partition: '{partition_date_folder_name}'.")
        
    transactions_partition_path = os.path.join(output_dir_transactions, partition_date_folder_name)
    snapshots_partition_path = os.path.join(output_dir_snapshots, partition_date_folder_name)
    os.makedirs(transactions_partition_path, exist_ok=True)
    os.makedirs(snapshots_partition_path, exist_ok=True)

    safe_base_filename_part = "".join(c if c.isalnum() or c in ('_','-') else '_' for c in base_filename)
    transactions_output_file = os.path.join(transactions_partition_path, f"part-{safe_base_filename_part}.jsonl.gz")
    snapshots_output_file = os.path.join(snapshots_partition_path, f"part-{safe_base_filename_part}.jsonl.gz")

    transactions_written = 0
    snapshots_written = 0

    try:
        open_func = gzip.open if input_file_path.endswith('.gz') else open
        
        with open_func(input_file_path, 'rt', encoding='utf-8') as infile:
            with gzip.open(transactions_output_file, 'wt', encoding='utf-8') as tf, \
                 gzip.open(snapshots_output_file, 'wt', encoding='utf-8') as sf:

                for line_number, line_str in enumerate(infile, 1):
                    line_str = line_str.strip()
                    if not line_str:
                        continue

                    if len(line_str) > 50 * 1024 * 1024:
                        print(f"WARNING: Line {line_number} is very large: {len(line_str) / (1024*1024):.2f} MB.")

                    try:
                        raw_log_entry = json.loads(line_str)
                    except json.JSONDecodeError as e:
                        print(f"Skipping malformed JSON line {line_number}: {e} - Line (start): {line_str[:200]}...")
                        continue

                    outer_timestamp = raw_log_entry.get("timestamp")
                    snapshot_field_value = raw_log_entry.get("snapshot")
                    pending_count_raw = raw_log_entry.get("pending_count", 0)
                    queued_count_raw = raw_log_entry.get("queued_count", 0)
                    
                    try:
                        pending_count = int(pending_count_raw)
                    except (ValueError, TypeError):
                        pending_count = 0
                    try:
                        queued_count = int(queued_count_raw)
                    except (ValueError, TypeError):
                        queued_count = 0
                        
                    snapshot_summary = {
                        "snapshot_timestamp": outer_timestamp,
                        "pending_count": pending_count,
                        "queued_count": queued_count,
                        "original_source_file": input_file_path
                    }
                    sf.write(json.dumps(snapshot_summary) + '\n')
                    snapshots_written += 1

                    snapshot_data = None
                    if isinstance(snapshot_field_value, str): # Check if it's a string that needs parsing
                        if len(snapshot_field_value) > 200 * 1024 * 1024: 
                             print(f"  WARNING: Inner snapshot JSON STRING at line {line_number} is extremely large: {len(snapshot_field_value) / (1024*1024):.2f} MB. Skipping parsing.")
                        else:
                            try:
                                snapshot_data = json.loads(snapshot_field_value)
                            except json.JSONDecodeError as e:
                                print(f"  Skipping malformed inner snapshot JSON STRING on line {line_number}: {e} - Snapshot (start): {snapshot_field_value[:200]}...")
                    elif isinstance(snapshot_field_value, dict): 
                        snapshot_data = snapshot_field_value
                    else:
                        print(f"  Line {line_number}: snapshot field is neither STRING nor DICT (type: {type(snapshot_field_value)}). Skipping snapshot processing.")
                        pass


                    if snapshot_data: # proceed only if snapshot_data was successfully obtained
                        try:
                            def extract_and_write_txs(tx_group, status):
                                nonlocal transactions_written
                                if tx_group and isinstance(tx_group, dict):
                                    for _sender, nonces in tx_group.items():
                                        if isinstance(nonces, dict):
                                            for _nonce_key, tx_details in nonces.items():
                                                if isinstance(tx_details, dict):
                                                    tx_details["_snapshot_timestamp"] = outer_timestamp
                                                    tx_details["_original_source_file"] = input_file_path
                                                    tx_details["_pool_status"] = status
                                                    tf.write(json.dumps(tx_details) + '\n')
                                                    transactions_written +=1
                            
                            extract_and_write_txs(snapshot_data.get('pending'), 'pending')
                            extract_and_write_txs(snapshot_data.get('queued'), 'queued')
                        except Exception as e_inner:
                            print(f"  Error processing inner snapshot data on line {line_number}: {e_inner} - Snapshot (type: {type(snapshot_field_value)}), Data (start): {str(snapshot_field_value)[:200]}...")
        
        print(f"Finished processing. Wrote {transactions_written} transactions to {transactions_output_file}")
        print(f"Wrote {snapshots_written} snapshot summaries to {snapshots_output_file}")

    except Exception as e:
        print(f"Error processing file {input_file_path}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python your_local_shredder_script.py <local_input_file_path> <local_output_dir_transactions> <local_output_dir_snapshots>")
        print("Example: python your_local_shredder_script.py ./small-test.log ./shredded_output/transactions ./shredded_output/snapshots")
        sys.exit(1)

    local_input_file_arg = sys.argv[1].strip()
    local_output_transactions_arg = sys.argv[2].strip()
    local_output_snapshots_arg = sys.argv[3].strip()
    
    os.makedirs(local_output_transactions_arg, exist_ok=True)
    os.makedirs(local_output_snapshots_arg, exist_ok=True)

    process_single_log_file_local(local_input_file_arg, local_output_transactions_arg, local_output_snapshots_arg)
    
    print("Local shredding process finished.")