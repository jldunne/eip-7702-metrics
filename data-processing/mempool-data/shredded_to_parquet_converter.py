import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, posexplode, lit, to_date, input_file_name, expr, when
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    ArrayType, TimestampType, DateType, IntegerType 
)

# Define schemas for pre-processed data

# Schema for an individual authorization tuple (object/dictionary)
# This should match the structure of objects within the "authorizationList" array
AUTHORIZATION_TUPLE_SCHEMA = StructType([
    StructField("chainId", StringType(), True), 
    StructField("address", StringType(), True),
    StructField("nonce", StringType(), True),   
    StructField("yParity", StringType(), True),
    StructField("r", StringType(), True),
    StructField("s", StringType(), True)
])

# Schema for a single transaction object from shredder script
SHREDDED_TRANSACTION_SCHEMA = StructType([
    StructField("blockHash", StringType(), True),
    StructField("blockNumber", StringType(), True),
    StructField("from", StringType(), True),
    StructField("gas", StringType(), True),
    StructField("gasPrice", StringType(), True),
    StructField("maxFeePerGas", StringType(), True),
    StructField("maxPriorityFeePerGas", StringType(), True),
    StructField("hash", StringType(), True),
    StructField("input", StringType(), True),
    StructField("nonce", StringType(), True),
    StructField("to", StringType(), True),
    StructField("transactionIndex", StringType(), True),
    StructField("value", StringType(), True),
    StructField("type", StringType(), True), 
    StructField("accessList", ArrayType(StringType()), True), 
    StructField("chainId", StringType(), True),
    StructField("v", StringType(), True),
    StructField("r", StringType(), True),
    StructField("s", StringType(), True),
    StructField("yParity", StringType(), True),
    StructField("authorizationList", ArrayType(AUTHORIZATION_TUPLE_SCHEMA), True), 
    # Fields added by shredder script
    StructField("_snapshot_timestamp", StringType(), True),
    StructField("_original_source_file", StringType(), True),
    StructField("_pool_status", StringType(), True)
])

# Schema for a single snapshot summary object from shredder script
SHREDDED_SNAPSHOT_SUMMARY_SCHEMA = StructType([
    StructField("snapshot_timestamp", StringType(), True), 
    StructField("pending_count", LongType(), True),
    StructField("queued_count", LongType(), True),
    StructField("original_source_file", StringType(), True)
])

# Helper  functions for hex conversion
def parse_hex_to_long(hex_str):
    if hex_str and isinstance(hex_str, str) and hex_str.startswith('0x'):
        try: return int(hex_str, 16)
        except ValueError: return None
    return None
parse_hex_to_long_udf = udf(parse_hex_to_long, LongType())

def parse_hex_to_int(hex_str):
    if hex_str and isinstance(hex_str, str) and hex_str.startswith('0x'):
        try: return int(hex_str, 16)
        except ValueError: return None
    return None
parse_hex_to_int_udf = udf(parse_hex_to_int, IntegerType())


# Main processing loop
def main(shredded_transactions_input_path, shredded_snapshots_input_path, output_base_path):
    spark = SparkSession.builder \
        .appName("ShreddedJsonToParquetConverter_AuthFix") \
        .config("spark.sql.legacy.json.allowEmptyString.enabled", "true") \
        .config("spark.sql.caseSensitive", "false").getOrCreate()

    print(f"DEBUG: Reading shredded transaction logs from: {shredded_transactions_input_path}")
    transactions_df = spark.read.schema(SHREDDED_TRANSACTION_SCHEMA).json(shredded_transactions_input_path)
    
    print("DEBUG: Schema of transactions_df (directly after reading shredded JSON):")
    transactions_df.printSchema(level=3) 
    print("DEBUG: Showing Type 4 transactions from transactions_df BEFORE further processing (up to 5):")
    transactions_df.filter(col("type") == "0x4").select("hash", "type", "authorizationList").show(5, truncate=False)

    # Process Individual Transactions
    print("DEBUG: Processing individual transaction details from shredded data...")
    processed_transactions_df = transactions_df.select(
        col("hash").alias("tx_hash"), 
        col("_snapshot_timestamp").cast(TimestampType()).alias("snapshot_timestamp"),
        col("type").alias("tx_type_hex"), 
        col("from").alias("from_address"),
        col("to").alias("to_address"),
        parse_hex_to_long_udf(col("gas")).alias("gas_limit"),
        parse_hex_to_long_udf(col("gasPrice")).alias("gas_price"), 
        parse_hex_to_long_udf(col("maxFeePerGas")).alias("max_fee_per_gas"),
        parse_hex_to_long_udf(col("maxPriorityFeePerGas")).alias("max_priority_fee_per_gas"),
        col("value").alias("value_hex"),
        parse_hex_to_long_udf(col("nonce")).alias("nonce"),
        col("input").alias("input_data"),
        col("authorizationList").alias("raw_authorization_list"),
        col("_pool_status").alias("pool_status"),
        col("_original_source_file").alias("original_source_file") 
    ).withColumn("snapshot_date", to_date(col("snapshot_timestamp")))

    processed_transactions_df = processed_transactions_df.withColumn(
        "tx_type", parse_hex_to_int_udf(col("tx_type_hex"))
    )
    processed_transactions_df = processed_transactions_df.withColumn(
        "is_eip7702", col("tx_type") == 4
    )
    processed_transactions_df = processed_transactions_df.withColumn(
        "authorization_count",
        when(col("is_eip7702") & col("raw_authorization_list").isNotNull(), 
             expr("size(raw_authorization_list)")
        ).otherwise(0)
    )
    processed_transactions_df = processed_transactions_df.withColumn(
        "input_data_size_bytes",
        when(col("input_data").isNotNull() & (col("input_data") != "0x"),
             (expr("length(substring(input_data, 3))") / 2).cast(IntegerType())
        ).otherwise(0)
    )
    
    print("DEBUG: Sample of Type 4 transactions from processed_transactions_df (showing raw_authorization_list and count):")
    type4_tx_sample_df = processed_transactions_df.filter(col("is_eip7702") == True)
    type4_tx_sample_df.select("tx_hash", "tx_type_hex", "raw_authorization_list", "authorization_count").show(20, truncate=False)
    print(f"DEBUG: Count of Type 4 transactions found in processed_transactions_df: {type4_tx_sample_df.count()}")

    processed_tx_count = processed_transactions_df.count()
    print(f"DEBUG: Final count of processed transactions: {processed_tx_count}")

    # Create Authorizations DataFrame
    print("Processing EIP-7702 authorizations...")
    eip7702_txs_with_auths_df = processed_transactions_df.filter(
        col("is_eip7702") & (col("authorization_count") > 0)
    )
    
    authorizations_df = None 
    if not eip7702_txs_with_auths_df.rdd.isEmpty():
        # Explode the array of authorization structs
        authorizations_df_exploded = eip7702_txs_with_auths_df.select(
            col("tx_hash"), col("snapshot_timestamp"), col("snapshot_date"),
            posexplode(col("raw_authorization_list")).alias("auth_index", "auth_struct") 
        )
        
        # Select fields from the exploded authorization struct
        authorizations_df = authorizations_df_exploded.select(
            "tx_hash", "snapshot_timestamp", "snapshot_date", "auth_index",
            parse_hex_to_long_udf(col("auth_struct.chainId")).alias("auth_chain_id"),
            col("auth_struct.address").alias("auth_address_delegating_to"),
            parse_hex_to_long_udf(col("auth_struct.nonce")).alias("auth_nonce")
        )
        auth_count = authorizations_df.count()
        print(f"DEBUG: Extracted {auth_count} authorization entries.")
    else:
        print("DEBUG: No EIP-7702 transactions with authorization_count > 0 found to populate authorizations_df.")
        auth_schema = StructType([
            StructField("tx_hash", StringType(), True), StructField("snapshot_timestamp", TimestampType(), True),
            StructField("snapshot_date", DateType(), True), StructField("auth_index", IntegerType(), True),
            StructField("auth_chain_id", LongType(), True), StructField("auth_address_delegating_to", StringType(), True),
            StructField("auth_nonce", LongType(), True)
        ])
        authorizations_df = spark.createDataFrame([], schema=auth_schema)

    # Process Snapshot Summaries and add type4_tx_count
    print(f"DEBUG: Reading shredded snapshot summary logs from: {shredded_snapshots_input_path}")
    snapshot_summary_input_df = spark.read.schema(SHREDDED_SNAPSHOT_SUMMARY_SCHEMA).json(shredded_snapshots_input_path)
    
    snapshot_summary_df = snapshot_summary_input_df.select(
        col("snapshot_timestamp").cast(TimestampType()).alias("snapshot_timestamp_ts"),
        col("pending_count"),
        col("queued_count"),
        col("original_source_file")
    ).withColumn("snapshot_date", to_date(col("snapshot_timestamp_ts"))) \
     .withColumnRenamed("snapshot_timestamp_ts", "snapshot_timestamp")

    print("Calculating Type 4 transaction counts for snapshots from processed transactions...")
    type4_counts_per_snapshot = processed_transactions_df \
        .filter(col("is_eip7702")) \
        .groupBy("snapshot_timestamp") \
        .count() \
        .withColumnRenamed("count", "type4_tx_count_agg") \
        .withColumnRenamed("snapshot_timestamp", "ts_for_join")

    snapshot_summary_final_df = snapshot_summary_df.join(
        type4_counts_per_snapshot,
        snapshot_summary_df.snapshot_timestamp == type4_counts_per_snapshot.ts_for_join,
        "left_outer"
    ).select(
        snapshot_summary_df["snapshot_timestamp"],
        snapshot_summary_df["pending_count"],
        snapshot_summary_df["queued_count"],
        col("type4_tx_count_agg").alias("type4_tx_count"),
        snapshot_summary_df["original_source_file"],
        snapshot_summary_df["snapshot_date"]
    ).fillna(0, subset=["type4_tx_count"])

    # Write Transformed Data as Partitioned Parquet
    snapshots_output_path = f"{output_base_path.rstrip('/')}/snapshots"
    transactions_output_path = f"{output_base_path.rstrip('/')}/transactions"
    authorizations_output_path = f"{output_base_path.rstrip('/')}/authorizations"

    final_snapshots_df_to_write = snapshot_summary_final_df.select(
        "snapshot_timestamp", "pending_count", "queued_count", 
        "type4_tx_count", "original_source_file", "snapshot_date"
    )
    final_transactions_df_to_write = processed_transactions_df.select(
        "tx_hash", "snapshot_timestamp", "tx_type_hex", "tx_type", "from_address", "to_address",
        "gas_limit", "gas_price", "max_fee_per_gas", "max_priority_fee_per_gas",
        "value_hex", "nonce", "input_data_size_bytes",
        "is_eip7702", "raw_authorization_list", "authorization_count",
        "snapshot_date", "original_source_file", "pool_status" 
    )

    def write_df_to_parquet(df_to_write, path, df_name, partition_col="snapshot_date"):
        print(f"\n--- Preparing to write: {df_name} to {path} ---")
        df_to_write.printSchema()
        print(f"Sample data for {df_name} (up to 2 rows):")
        df_to_write.show(2, truncate=False)
        
        count = df_to_write.count()
        
        if count > 0:
            print(f"Attempting to write {count} records for {df_name} to {path} partitioned by {partition_col}")
            try:
                df_to_write.write.mode("overwrite") \
                    .partitionBy(partition_col) \
                    .parquet(path, compression="snappy")
                print(f"SUCCESS: Wrote {df_name} to {path}")
            except Exception as e_write:
                print(f"ERROR during write of {df_name} to {path}: {e_write}")
                import traceback
                traceback.print_exc()
        else:
            print(f"DataFrame for {df_name} ({path}) is empty (count={count}). Skipping write.")

    write_df_to_parquet(final_snapshots_df_to_write, snapshots_output_path, "Snapshots")
    write_df_to_parquet(final_transactions_df_to_write, transactions_output_path, "Transactions")
    write_df_to_parquet(authorizations_df, authorizations_output_path, "Authorizations")

    print("Processing complete.")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4: 
        print("Usage: spark-submit your_script_name.py <shredded_transactions_input_path> <shredded_snapshots_input_path> <output_base_directory_path>")
        sys.exit(-1)

    shredded_tx_input_path_arg = sys.argv[1].strip()
    shredded_sn_input_path_arg = sys.argv[2].strip()
    output_base_path_arg = sys.argv[3].strip()
    
    main(shredded_tx_input_path_arg, shredded_sn_input_path_arg, output_base_path_arg)