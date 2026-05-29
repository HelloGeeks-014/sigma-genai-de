# Patched by Self-Healing Agent — 2026-05-29T17:51:40.960630
# Attempts needed: 2

import duckdb, os, pandas as pd

DB_PATH = os.path.join(os.path.dirname(__file__), "sigma_platform.duckdb")

def run_merchant_report():
    if not os.path.exists(DB_PATH):
        print(f"Database {DB_PATH} does not exist.")
        return

    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM silver_transactions WHERE amount > 0").fetchdf()

    total = df["amount"].sum()

    df2 = df.groupby("merchant_id").agg({"amount": "mean"}).reset_index()
    df2.columns = ["merchant_id", "avg_amount"]

    conn.close()

    print(f"Done. Total: {total:.2f}, Merchants: {len(df2)}")

    top = df2.iloc[0]["merchant_id"]
    print(f"Top merchant by avg amount: {top}")

if __name__ == "__main__":
    run_merchant_report()