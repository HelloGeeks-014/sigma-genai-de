import csv
import os
import shutil
import sys
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sample_data import SAMPLE_MERCHANTS, SAMPLE_TRANSACTIONS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, broadcast, row_number, when
from pyspark.sql.types import FloatType, DateType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: Ingest Bronze] Starting ingestion")
        
        # Read CSV with all columns as strings
        bronze_df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_path)
        
        # Add required columns
        bronze_df = bronze_df.withColumn("ingestion_timestamp", current_timestamp()) \
                             .withColumn("source_file", lit(input_path)) \
                            .withColumn("pipeline_run_id", lit(run_id))
        
        # Log row counts
        input_count = bronze_df.count()
        logging.info(f"[Stage: Ingest Bronze] Input count: {input_count:,} rows")
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write Parquet partitioned by date
        bronze_df.write.mode("overwrite").parquet(output_path, partitionBy="transaction_date")
        
        # Log output count
        output_count = spark.read.parquet(output_path).where(col("transaction_date") == run_date).count()
        logging.info(f"[Stage: Ingest Bronze] Output count: {output_count:,} rows")
        
    except Exception as e:
        logging.error(f"[Stage: Ingest Bronze] Error: {e}")
        logging.error(f"[Stage: Ingest Bronze] Input count at failure: {input_count:,} rows")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: Transform Silver] Starting transformation")
        
        # Read Bronze Parquet with partition pruning on run_date
        bronze_df = spark.read.parquet(bronze_path).where(col("transaction_date") == run_date)
        
        # Log input count
        input_count = bronze_df.count()
        logging.info(f"[Stage: Transform Silver] Input count: {input_count:,} rows")
        
        # Cast columns to correct types
        bronze_df = bronze_df.withColumn("amount", col("amount").cast(FloatType())) \
                             .withColumn("transaction_date", col("transaction_date").cast(DateType())) \
                            .withColumn("transaction_id", col("transaction_id").cast("string")) \
                            .withColumn("merchant_id", col("merchant_id").cast("string"))
        
        # Filter NULL transaction_id and negative amounts
        bronze_df = bronze_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        
        # Log after filter count
        after_filter_count = bronze_df.count()
        logging.info(f"[Stage: Transform Silver] After filter count: {after_filter_count:,} rows")
        
        # Deduplicate on transaction_id keeping latest ingestion_timestamp
        latest_txn = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        bronze_df = bronze_df.withColumn("rank", row_number().over(latest_txn)) \
                            .filter(col("rank") == 1) \
                            .drop("rank")
        
        # Log after dedup count
        after_dedup_count = bronze_df.count()
        logging.info(f"[Stage: Transform Silver] After dedup count: {after_dedup_count:,} rows")
        
        # Read merchants and broadcast hint, cache merchants
        merchants_df = spark.read.parquet(merchants_path)
        merchants_df = broadcast(merchants_df)
        merchants_df.cache()
        
        # Join with merchants
        silver_df = bronze_df.join(merchants_df, "merchant_id", "left_outer")
        
        # Add quality_flag column
        silver_df = silver_df.withColumn("quality_flag", 
                                         when(col("merchant_name").isNotNull(), "CLEAN").otherwise("UNMATCHED"))
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/transaction_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write Parquet partitioned by date
        silver_df.write.mode("overwrite").parquet(output_path, partitionBy="transaction_date")
        
        # Log output count
        output_count = spark.read.parquet(output_path).where(col("transaction_date") == run_date).count()
        logging.info(f"[Stage: Transform Silver] Output count: {output_count:,} rows")
        
    except Exception as e:
        logging.error(f"[Stage: Transform Silver] Error: {e}")
        logging.error(f"[Stage: Transform Silver] Input count at failure: {input_count:,} rows")
        logging.error(f"[Stage: Transform Silver] After filter count at failure: {after_filter_count:,} rows")
        logging.error(f"[Stage: Transform Silver] After dedup count at failure: {after_dedup_count:,} rows")
        raise

def prepare_local_sample_inputs(spark, data_dir):
    os.makedirs(data_dir, exist_ok=True)

    transactions_path = os.path.join(data_dir, "transactions.csv")
    with open(transactions_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SAMPLE_TRANSACTIONS[0].keys())
        writer.writeheader()
        writer.writerows(SAMPLE_TRANSACTIONS)

    merchants_path = os.path.join(data_dir, "merchants")
    spark.createDataFrame(SAMPLE_MERCHANTS).write.mode("overwrite").parquet(merchants_path)

    return transactions_path, merchants_path


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("FintechPipeline").getOrCreate()
    
    # Define local paths and run date for this lab run
    local_dir = os.path.join(os.path.dirname(__file__), "local_run")
    input_path, merchants_path = prepare_local_sample_inputs(spark, os.path.join(local_dir, "input"))
    output_path_bronze = os.path.join(local_dir, "bronze")
    output_path_silver = os.path.join(local_dir, "silver")
    run_date = "2024-01-15"
    run_id = "local_run_001"
    
    # Execute bronze stage
    ingest_bronze(spark, input_path, output_path_bronze, run_date, run_id)
    
    # Execute silver stage
    transform_silver(spark, output_path_bronze, merchants_path, output_path_silver, run_date)


if __name__ == "__main__":
    main()


# SECTION 2: GOLD AGGREGATION LAYER
from pyspark.sql.functions import avg, col, count, countDistinct, max, min, mode, sum

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Merchant Performance] Starting aggregation")
        
        # Read the Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)
        
        # Log input count
        input_count = silver_df.count()
        logging.info(f"[Stage: Build Merchant Performance] Input count: {input_count:,} rows")
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate required metrics
        merchant_performance_df = completed_txns.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
            .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(col("status").isin("FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/merchant_performance/run_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write the Gold layer data
        merchant_performance_df.repartition("transaction_date") \
            .write.mode("overwrite").parquet(f"{output_path}/merchant_performance/run_date={run_date}")
        
        # Log output count
        output_count = merchant_performance_df.count()
        logging.info(f"[Stage: Build Merchant Performance] Output count: {output_count:,} rows")
        
    except Exception as e:
        logging.error(f"[Stage: Build Merchant Performance] Error: {e}")
        logging.error(f"[Stage: Build Merchant Performance] Input count at failure: {input_count:,} rows")
        raise

def build_customer_ltv(spark, silver_path):
    try:
        logging.info("[Stage: Build Customer LTV] Starting aggregation")
        
        # Read the Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == "2024-01-15")
        
        # Log input count
        input_count = silver_df.count()
        logging.info(f"[Stage: Build Customer LTV] Input count: {input_count:,} rows")
        
        # Filter for completed transactions
        completed_txns = silver_df.filter(col("status") == "COMPLETED")
        
        # Calculate required metrics
        customer_ltv_df = completed_txns.groupBy("customer_id") \
           .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                mode("payment_method").alias("preferred_payment_method")
            )
        
        # Write the Gold layer data
        customer_ltv_df.write.mode("overwrite").parquet("path_to_gold/customer_ltv")
        
        # Log output count
        output_count = customer_ltv_df.count()
        logging.info(f"[Stage: Build Customer LTV] Output count: {output_count:,} rows")
        
    except Exception as e:
        logging.error(f"[Stage: Build Customer LTV] Error: {e}")
        logging.error(f"[Stage: Build Customer LTV] Input count at failure: {input_count:,} rows")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: Build Daily Summary] Starting aggregation")
        
        # Read the Silver layer data with partition pruning
        silver_df = spark.read.parquet(silver_path).where(col("transaction_date") == run_date)
        
        # Log input count
        input_count = silver_df.count()
        logging.info(f"[Stage: Build Daily Summary] Input count: {input_count:,} rows")
        
        # Calculate required metrics
        daily_summary_df = silver_df.groupBy("transaction_date") \
           .agg(
                sum("amount").alias("total_revenue"),
                count("*").alias("total_txns"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants"),
                (count(col("status").isin("FAILED")) / count("*") * 100).alias("failure_rate_pct")
            )
        
        # Delete existing partition before writing
        partition_path = f"{output_path}/daily_summary/run_date={run_date}"
        shutil.rmtree(partition_path, ignore_errors=True)
        
        # Write the Gold layer data
        daily_summary_df.repartition("transaction_date") \
           .write.mode("overwrite").parquet(f"{output_path}/daily_summary/run_date={run_date}")
        
        # Log output count
        output_count = daily_summary_df.count()
        logging.info(f"[Stage: Build Daily Summary] Output count: {output_count:,} rows")
        
    except Exception as e:
        logging.error(f"[Stage: Build Daily Summary] Error: {e}")
        logging.error(f"[Stage: Build Daily Summary] Input count at failure: {input_count:,} rows")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: Run Gold] Starting gold layer aggregations")
        
        # Build the Gold layer components
        build_merchant_performance(spark, silver_path, f"{gold_output_dir}/merchant_performance", run_date)
        build_customer_ltv(spark, silver_path)
        build_daily_summary(spark, silver_path, f"{gold_output_dir}/daily_summary", run_date)
        
        # Write run metadata to JSON
        run_metadata = {
            "run_date": run_date,
            "silver_path": silver_path,
            "gold_output_dir": gold_output_dir,
            "components": ["merchant_performance", "customer_ltv", "daily_summary"],
            "run_status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        
    except Exception as e:
        logging.error(f"[Stage: Run Gold] Error: {e}")
        run_metadata = {
            "run_date": run_date,
            "silver_path": silver_path,
            "gold_output_dir": gold_output_dir,
            "components": ["merchant_performance", "customer_ltv", "daily_summary"],
            "run_status": "FAILED",
            "error_message": str(e),
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        raise

# Initialize Spark session
spark = SparkSession.builder.appName("GoldPipeline").getOrCreate()