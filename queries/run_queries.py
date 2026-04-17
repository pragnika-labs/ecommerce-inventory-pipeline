import pandas as pd
from sqlalchemy import create_engine
import os

engine = create_engine("sqlite:///database/sales.db")

# list of all query files to run in order
query_files = [
    ("Revenue by Category",  "queries/revenue_by_category.sql"),
    ("Top Products",         "queries/top_products.sql"),
    ("Lost Revenue",         "queries/lost_revenue.sql"),
    ("City Demand",          "queries/city_demand.sql"),
    ("Reorder Alerts",       "queries/reorder_alert.sql"),
]

for title, filepath in query_files:

    print()
    print("=" * 60)
    print(f"  {title.upper()}")
    print(f"  File: {filepath}")
    print("=" * 60)

    # check the file exists before trying to read it
    if not os.path.exists(filepath):
        print(f"  File not found — skipping")
        continue

    # open and read the .sql file as a text string
    with open(filepath, "r") as f:
        query = f.read()

    # run the query against the database
    df = pd.read_sql(query, con=engine)

    # print the full result — no row or column limits
    pd.set_option("display.max_rows",    None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width",       120)
    pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

    print(df.to_string(index=False))
    print()

print("=" * 60)
print("  ALL QUERIES COMPLETE")
print("=" * 60)