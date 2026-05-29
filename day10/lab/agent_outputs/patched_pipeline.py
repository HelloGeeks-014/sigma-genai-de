# Patched by Self-Healing Agent — 2026-05-29T17:24:28.919000
# Attempts needed: 5

import duckdb
import os
import pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "sigma_platform.duckdb")

def run_merchant_report():
    conn = duckdb.connect(DB_PATH)
    df = conn.execute("SELECT * FROM silver_transactions WHERE amount > 0").fetchdf()

    total = df["amount"].sum()

    df2 = df.groupby("merchant_id")["amount"].mean().reset_index()
    df2.columns = ["merchant_id", "avg_amount"]

    conn.close()
    print(f"Done. Total: {total:.2f}, Merchants: {len(df2)}")

    if not df2.empty:
        top = df2.iloc[0]["merchant_id"]
        print(f"Top merchant by avg amount: {top}")

if __name__ == "__main__":
    run_merchant_report()