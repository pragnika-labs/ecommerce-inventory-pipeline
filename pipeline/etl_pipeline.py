import pandas as pd
import sqlite3
import os
from sqlalchemy import create_engine

from data.generate_data import main as generate_todays_data

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
    df["date"] = pd.to_datetime(df["date"])

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
                                     .str.replace(r'(?<!^)(?=[A-Z])', ' ', regex = True)
                                     .str.title()
                                     )
    
    phone_mask = df["customer_phone"] != "Unknown"
    df.loc[phone_mask, "customer_phone"] = (df.loc[phone_mask, "customer_phone"]
                                            .str.replace("-", "", regex = False)
                                            .str.replace(" ", "", regex = False)
                                            .str.strip()
                                            )
    






    
