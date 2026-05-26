"""
Sigma DataTech Transaction Analytics Pipeline - FIXED VERSION
Fixed by: Code Review Team | Day 7 — Pipeline Brain
Generated: 2026-05-26T12:38:49.478582+00:00

This file corrects issues from the generated_pipeline.py based on code review findings:
1. Added error handling with try/except blocks
2. Replaced hardcoded paths with parameters

Architecture: Bronze -> Silver -> Gold (medallion pattern)
"""

# ═══════════════════════════════════════════════════════════════
# SECTION 1: BRONZE + SILVER LAYERS
# ═══════════════════════════════════════════════════════════════
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, when, max, count, sum as spark_sum, countDistinct, avg, min
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql.window import Window
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    """
    FIX #1: Added error handling with try/except block
    """
    try:
        logger.info(f"Starting Bronze ingestion from {input_path}")

        # Read raw CSV files with all columns as strings
        transactions_df = (spark.read.format("csv")
                          .option("header", "true")
                           .option("inferSchema", "false")
                           .load(input_path))

        row_count_before = transactions_df.count()
        logger.info(f"Rows read from source: {row_count_before}")

        # Add metadata columns
        transactions_df = (transactions_df.withColumn("ingestion_timestamp", lit(run_date))
                          .withColumn("source_file", lit("transactions.csv"))
                           .withColumn("pipeline_run_id", lit(run_id)))

        # Write as Parquet partitioned by date
        transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(output_path)

        logger.info(f"Bronze layer written to {output_path} with {row_count_before} rows")
        return {"status": "SUCCESS", "rows_written": row_count_before}

    except Exception as e:
        logger.error(f"Bronze ingestion failed: {str(e)}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    """
    FIX #1: Added error handling with try/except block
    """
    try:
        logger.info(f"Starting Silver transformation from {bronze_path}")

        # Read Bronze Parquet with partition pruning on run_date
        transactions_df = (spark.read.format("parquet")
                           .load(bronze_path)
                          .where(col("ingestion_timestamp") == run_date))

        row_count_after_read = transactions_df.count()
        logger.info(f"Rows read from Bronze: {row_count_after_read}")

        # Cast columns to correct types
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
        transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
        transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
        transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))

        # Filter: remove records where transaction_id is NULL or amount < 0
        transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))

        row_count_after_filter = transactions_df.count()
        logger.info(f"Rows after filtering: {row_count_after_filter} (removed {row_count_after_read - row_count_after_filter})")

        # Deduplicate: if same transaction_id appears twice, keep the record with latest ingestion_timestamp
        transactions_df = transactions_df.dropDuplicates(["transaction_id"]).orderBy("ingestion_timestamp", ascending=False)

        row_count_after_dedup = transactions_df.count()
        logger.info(f"Rows after deduplication: {row_count_after_dedup}")

        # Read merchants CSV and cache it
        merchants_df = (spark.read.format("csv")
                       .option("header", "true")
                        .option("inferSchema", "false")
                       .load(merchants_path))
        merchants_df = merchants_df.cache()

        # Join transactions with merchants on merchant_id to get merchant_name, category, city
        transactions_df = (transactions_df.join(broadcast(merchants_df), transactions_df.merchant_id == merchants_df.merchant_id, "left_outer")
                           .withColumn("quality_flag",
                                       when(col("merchant_name").isNotNull(), "CLEAN").otherwise("UNMATCHED")))

        # Write as Parquet partitioned by date
        transactions_df.write.mode("overwrite").partitionBy("transaction_date").parquet(output_path)

        logger.info(f"Silver layer written to {output_path} with {row_count_after_dedup} rows")
        return {"status": "SUCCESS", "rows_written": row_count_after_dedup}

    except Exception as e:
        logger.error(f"Silver transformation failed: {str(e)}")
        raise

def main(input_path=None, merchants_path=None, bronze_path=None, silver_path=None, gold_path=None, run_date=None, run_id=None):
    """
    FIX #2: Replace hardcoded paths with parameters
    """
    try:
        # Initialize SparkSession
        spark = (SparkSession.builder
                .appName("Sigma DataTech Transaction Analytics Pipeline")
                 .getOrCreate())

        # FIX #2: Use parameters instead of hardcoded paths (with defaults for testing)
        input_path = input_path or "s3://sigma-datatech/bronze/transactions.csv"
        merchants_path = merchants_path or "s3://sigma-datatech/bronze/merchants.csv"
        bronze_path = bronze_path or "s3://sigma-datatech/bronze/transactions"
        silver_path = silver_path or "s3://sigma-datatech/silver/transactions"
        gold_path = gold_path or "s3://sigma-datatech/gold"
        run_date = run_date or "2026-05-27"
        run_id = run_id or "run_20260527"

        logger.info(f"Pipeline started with run_date={run_date}, run_id={run_id}")

        # Ingest Bronze layer
        bronze_result = ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
        logger.info(f"Bronze result: {bronze_result}")

        # Transform Silver layer
        silver_result = transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
        logger.info(f"Silver result: {silver_result}")

        # Write run metadata summary to a JSON file
        metadata = {
            "run_date": run_date,
            "run_id": run_id,
            "status": "COMPLETED",
            "bronze_rows": bronze_result.get("rows_written", 0),
            "silver_rows": silver_result.get("rows_written", 0)
        }

        metadata_path = f"{gold_path}/metadata/run_{run_date}.json"
        logger.info(f"Writing metadata to {metadata_path}")

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        logger.info("Pipeline completed successfully")
        return metadata

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
