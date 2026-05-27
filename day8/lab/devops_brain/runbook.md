# Transaction Pipeline Runbook

## Pipeline Overview
This pipeline ingests transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs nightly to process the previous day's transactions. If it fails, downstream reports and dashboards will be inaccurate.

## Pipeline Steps
1. Connect to DuckDB database using `get_connection()`
2. Setup required tables using `setup_tables()`
3. Load merchants data using `load_merchants()`
4. Load transactions into bronze table using `load_bronze()`
5. Transform bronze to silver using `transform_bronze_to_silver()`
6. Load silver table using `load_silver()`
7. Compute merchant performance metrics using `compute_merchant_performance()`
8. Compute daily summary metrics using `compute_daily_summary()`
9. Load gold tables using `load_gold()`

## Schedule / Trigger
This pipeline is scheduled to run nightly at 2 AM using a cron job.

## Failure Modes
1. **DuckDB connection failure**  
   *Symptom:* Pipeline logs show "ConnectionError"  
   *Root Cause:* Database is down or credentials are invalid

2. **Table creation failure**  
   *Symptom:* Pipeline logs show "ExecutionError" during `setup_tables()`  
   *Root Cause:* SQL syntax error or permissions issue

3. **Merchants data load failure**  
   *Symptom:* Pipeline logs show "DataError" during `load_merchants()`  
   *Root Cause:* Invalid merchants data

4. **Bronze load failure**  
   *Symptom:* Pipeline logs show "DataError" during `load_bronze()`  
   *Root Cause:* Invalid transaction data

5. **Silver transform failure**  
   *Symptom:* Pipeline logs show "KeyError" during `transform_bronze_to_silver()`  
   *Root Cause:* Missing merchant data

## Recovery Actions
1. **DuckDB connection failure**  
   - Verify DB is running  
   - Check credentials in code  
   - Restart DB if needed

2. **Table creation failure**  
   - Review SQL in `setup_tables()`  
   - Fix syntax errors  
   - Ensure user has CREATE TABLE permissions

3. **Merchants data load failure**  
   - Validate merchants data  
   - Fix any invalid rows  
   - Retry pipeline

4. **Bronze load failure**  
   - Validate transaction data  
   - Fix any invalid rows  
   - Retry pipeline

5. **Silver transform failure**  
   - Ensure all merchants have required fields  
   - Fix any missing data  
   - Retry pipeline

## Known Bugs
- Hardcoded AWS credentials in code
- No null handling for merchant fields
- No deduplication of bronze transactions

## Escalation Contacts
- **Severity 1:** Priya Nair (+91-98400-11111)
- **Severity 2:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
- **Severity 3:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks
- Verify bronze table has expected number of rows
- Verify silver table has expected number of rows
- Verify gold tables have today's date
- Spot check a few merchant and daily summary rows