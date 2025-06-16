# README

## 1. Overview

This repository provides a collection of scripts to collect and process Ethereum node data relating to EIP-7702. This document provides the instructions to replicate the data collection and processing workflow.

### Scripts

* `data-collection/mempool_dump.py`: Captures periodic snapshots of the mempool.
* `data-collection/log_memstats.sh`: Logs geth memory statistics.
* `data-processing/mempool-data/shredder_script.py`: Pre-processes the raw mempool dump files, breaking down large nested objects into one object per transaction per line. This pre-processing step is necessary to avoid memory errors as the raw files are very large.
* `data-processing/mempool-data/shredded_to_parquet_converter.py`: A PySpark script that reads the simple "shredded" JSON files and converts them into Parquet format.
* `data-processing/geth-traces/process_geth_logs.py`: Parses verbose trace logs to produce aggregated metrics on transaction invalidations and mempool events in a SQLite database.
* `data-processing/memstats-data/process_memstats.py`: Parses Geth memory statistics into a SQLite database.


## 2. Data Collection Instructions

To replicate this pipeline, three types of logs must be collected from a running geth node.

### a. Mempool Dumps

These files contain periodic snapshots of the entire transaction pool.

* **Script:** `mempool_dump.py`
* **Dependencies:** `pip install web3`
* **Setup:**
    1.  Modify the `IPC_PATH` in the script to point to your geth node's `geth.ipc` file.
    2.  Modify the `OUTPUT_DIR` to your desired log location.
* **Execution:** Run as a long-running background process. Using a `systemd` service is recommended. This generates daily log files.

### b. Verbose geth Logs (Trace)

These logs provide detailed information on why transactions are accepted or rejected.

* **Geth Flags:** When launching your geth node, include the following flags:
    ```bash
    --verbosity 5 --vmodule "txpool=5"
    ```
    * `--verbosity 5`: Sets the general log level.
    * `--vmodule "txpool=5"`: Sets the `txpool` module to the most verbose `TRACE` level, which is necessary to capture all discard/rejection reasons.
* **Output:** Ensure the geth output is redirected to a log file.

### c. Geth Memory Statistics

These logs provide periodic snapshots of the geth client's memory usage.

* **Script:** `log_memstats.sh`
* **Dependencies:** `curl`, `jq`
* **Setup:**
    1.  Modify the `RPC_URL` in the script if your geth node's HTTP RPC endpoint is not at the default location.
    2.  Modify the `LOGFILE` variable to your desired output path.
* **Execution:** Make the script executable and run it as a background process:

---

## 3. Part 2: Data Processing Pipeline

This pipeline should be run on a machine with sufficient RAM (e.g., 32GB+) and CPU cores. It converts the raw data into formats more suitable for analysis.

### Phase 1: Processing Mempool Dumps (JSON -> Parquet)

This is a two-step process to handle the large nested JSON from `mempool_dump.py`.

**Step 1.1: Shred Raw Mempool Logs**
This step uses the Python script `shredder_script.py` to break down the large snapshot JSONs into a simpler format.

* **Command:**
    ```bash
    # Usage: python3 shredder_script.py <path_to_input_file> <output_dir> <output_snapshot_dirs>
    python3 shredder_script.py /path/to/raw_mempool_dumps/ ./shredded_output/transactions ./shredded_output/snapshots
    ```
* **Input:** The directory containing the daily log files from `mempool_dump.py`.
* **Output:** Two directories (`transactions`, `snapshots`) containing gzipped JSON line files, ready for Spark.

**Step 1.2: Convert Shredded JSON to Parquet**
This step uses the PySpark script `shredded_to_parquet_converter.py` to create the final Parquet files.

* **Dependencies:** `pip install pyspark`
* **Command:**
    ```bash
    # Usage: spark-submit [spark_options] script.py <shredded_tx_path> <shredded_snapshot_path> <parquet_output_path>
    spark-submit   --master local[*]   --driver-memory 4g   shredded_to_parquet_converter.py shredded_output_local/transactions/snapshot_date=2025-05-04/   ./shredded_output_local/snapshots/snapshot_date=2025-05-04/   ./parquet_output/processed-logs
    ```
* **Output:** Creates partitioned Parquet datasets for `snapshots`, `transactions`, and `authorizations` in the `./final_parquet_output/` directory.

### Phase 2: Processing Geth and Memstats Logs (to SQLite)

This phase processes the text-based Geth logs into queryable databases.

**Step 2.1: Process Geth Trace Logs**
The `process_geth_logs.py` script parses the verbose Geth logs to generate aggregated time-series metrics.

* **Command:**
    ```bash
    # Usage: python3 process_geth_logs.py <geth_log_dir> <sqlite_db_name>
    python3 process_geth_logs.py /var/log/geth geth_full_analysis.db
    ```
* **Output:** A SQLite database file (`geth_metrics.db`) containing a `geth_metrics_summary` table.

**Step 2.2: Process Geth Memstats Logs**
The `process_memstats.py` script parses the memory statistics logs.

* **Command:**
    ```bash
    # Usage: python3 process_memstats.py <memstats_log_dir> <sqlite_db_name>
    python3 python3 process_memstats.py /var/log/geth/ memstats.db
    ```
* **Output:** A separate SQLite database file (`memstats.db`) containing a `memstats` table.

---

## 4. Part 3: Inspection

The processed data can be inspected using various tools. For local analysis I used DuckDB (for Parquet) and the `sqlite3` CLI.

**Example: Inspecting Parquet Data with DuckDB**
```bash
# Launch DuckDB
./duckdb

# Query transaction data, reading all partitions
SELECT tx_hash, authorization_count 
FROM read_parquet('./final_parquet_output/transactions/**/*.parquet', hive_partitioning=0) 
WHERE is_eip7702 = TRUE 
LIMIT 10;
```

**Example: Inspecting SQLite Data**
```bash
# Launch SQLite CLI
sqlite3 geth_metrics.db

# Make output readable
.headers on
.mode column

# Run a query for the top invalidation reasons
SELECT metric_name, SUM(total_count) AS total
FROM geth_metrics_summary 
WHERE metric_category = 'invalidation' 
GROUP BY metric_name 
ORDER BY total DESC;
```
