# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant information, and processes it into clean, enriched, and aggregated layers for analytical purposes.

## Data Flow Diagram

```
+---------------------+      +--------------------+      +--------------------+      +--------------------+
|     Source          |      |     Bronze Layer   |      |     Silver Layer   |      |     Gold Layer     |
| (TRANSACTIONS_CLEAN  | ---> | (bronze_transactions) | ---> | (silver_transactions) | ---> | (gold_merchant_performance, |
| & TRANSACTIONS_DIRTY)|      |                     |      |                     |      | gold_daily_summary)|
+---------------------+      +--------------------+      +--------------------+      +--------------------+
```

## Key Design Decisions
- **Layered Approach**: The pipeline uses a three-tier architecture (Bronze, Silver, Gold) to ensure data quality and analytical readiness.
- **Data Enrichment**: Merchant information is joined with transaction data in the Silver layer to provide context.
- **Aggregative Computations**: The Gold layer performs aggregations and metrics calculations for business insights.
- **Timestamps**: Each layer captures the ingestion timestamp to track data freshness.

## Known Limitations
- **Data Quality**: The pipeline assumes that the source data is either clean or dirty but does not handle partial data issues.
- **Single Source**: The pipeline currently supports only one source of transaction data.
- **No Error Handling**: The pipeline does not handle exceptions beyond basic try-except blocks.
- **Static Merchant Data**: Merchant data is loaded once and not updated unless the pipeline is rerun.

## Dependencies
- **DuckDB**: The database engine used to store and query data.
- **MERCHANTS**: A predefined list of merchant data.
- **TRANSACTIONS_CLEAN & TRANSACTIONS_DIRTY**: Source data files containing transaction records.