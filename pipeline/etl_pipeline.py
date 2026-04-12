import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine

#Funtion 1: extract()

def extract():
    csv_path = "data/today.csv"

    if not os.path.exists(csv_path):
        print("Error: File not found.")
        return None
    
    df = pd.read_csv(csv_path)

    print(f"Extracted {len(df)} raw rows from today.csv")
    return df

#Funtion 2: transform(df)

def transform(df):

    print("Starting Transformation.....")

    #Converting datatypes
    df["date"] = pd.to_datetime(df["date"]).strftime('%Y-%m-%d')

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["fulfilled"] = pd.to_numeric(df["fulfilled"], errors="coerce")
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    df["lost_sales"] = pd.to_numeric(df["lost_sales"], errors="coerce")
    df["lost_revenue"] = pd.to_numeric(df["lost_revenue"], errors="coerce")

    #Removing duplicates
    rows_before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    rows_after = len(df)

    dupes_removed = rows_before - rows_after
    if dupes_removed > 0:
        print(f"Removed {dupes_removed} duplicate order(s)")

    #Fix missing values
    df["customer_name"] = df["customer_name"].fillna("Unknown")
    df["customer_phone"] = df["customer_phone"].fillna("Unknown")
    df["city"] = df["city"].fillna("Unknown")

    df["lost_sales"] = df["lost_sales"].fillna(0)
    df["lost_revenue"] = df["lost_revenue"].fillna(0)

    #Fixing cutomer name and phone number
    name_mask = df["customer_name"] != "Unknown"
    df.loc[name_mask, "customer_name"] = (df.loc[name_mask, "customer_name"]
                                     .str.strip()
                                     .str.replace(r'(?<=[a-z])(?=[A-Z])', ' ', regex=True)
                                     .str.title()
                                     )
    
    phone_mask = df["customer_phone"] != "Unknown"
    df.loc[phone_mask, "customer_phone"] = (df.loc[phone_mask, "customer_phone"]
                                            .str.replace("-", "", regex = False)
                                            .str.replace(" ", "", regex = False)
                                            .str.strip()
                                            )
    
    df["phone_valid"] = df["customer_phone"].apply(
        lambda x: len(str(x)) == 10 if x != "Unknown" else False
    )

    #Fixing city
    city_mask = df["city"] != "Unknown"
    df.loc[city_mask, "city"] = (df.loc[city_mask, "city"]
                                 .str.strip()
                                 .str.title()
    )

    df["revenue"] = df["fulfilled"] * df["price"]

    rows_before = len(df)
    df = df.dropna(subset = ["product_id", "price", "quantity"])
    rows_after = len(df)

    if rows_before != rows_after:
        print(f"Dropped {rows_before - rows_after} rows with missing values")
    
    print(f"Transform complete: {len(df)} clean rows ready")
    return df


#Funtion 3: load(df)
def load(df):
    os.makedirs("database", exist_ok=True)

    #create sqlalchemy engine
    engine = create_engine("sqlite:///database/sales.db")

    df.to_sql(
        name = "sales_clean",
        con = engine, 
        if_exists = "append",
        index = False
    )

    print(f"Loaded {len(df)} rows into sales.db - sales_clean table")

#Funtion 4: delete_csv()
def delete_csv():
    csv_path = "data/today.csv"
    
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print("today.csv deleted")
    else:
        print("today.csv is already removed")


#Function 5: verify()
def verify():
    conn = sqlite3.connect("database/sales.db")
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sales_clean")
    total_rows = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(day_number), MAX(day_number) FROM sales_clean")
    min_day, max_day = cursor.fetchone()

    cursor.execute("SELECT SUM(revenue) FROM sales_clean")
    total_revenue = cursor.fetchone()[0]

    conn.close()

    print()
    print(" -"*22)
    print("DATABASE SUMMARY")
    print(" -"*22)
    print(f"Total rows: {total_rows:,}")
    print(f"Days in database: Day {min_day} to Day {max_day}")
    print(f"Total revenue: {total_revenue:,.0f}")
    print(" -"*22)

#Function 6: run_etl()
def run_etl():
    print("ETL PIPELINE STARTING.....")
    
    print("\n[STEP 1] EXTRACTING....")
    df = extract()

    if df is None:
        print("Pipeline stopped - couldn't find today.csv")
        return
    
    print("\n[STEP 2] TRANSFORMING....")
    df = transform(df)

    print("\n[STEP 3] LOADING....")
    load(df)

    print("\n[STEP 4] CLEANING UP....")
    delete_csv()

    print("\n[STEP 5] VERIFYING....")
    verify()

    print("\n ETL PIPELINE COMPLETE")

if __name__ == "__main__":
    run_etl()