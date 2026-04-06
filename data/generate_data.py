import pandas as pd
import random
import sqlite3
import os
from datetime import datetime, timedelta

#Product Catalogue
products = [
    #Electronics
    {"product_id": "P01", "product_name": "Wireless Earbuds", "category": "Electronics", "price": 1399, "initial_stock":200},
    {"product_id": "P02", "product_name": "Bluetooth Speaker", "category": "Electronics", "price": 1899, "initial_stock":170},
    {"product_id": "P03", "product_name": "Wireless Mouse", "category": "Electronics", "price": 900, "initial_stock":220},
    {"product_id": "P04", "product_name": "Power Bank", "category": "Electronics", "price": 2799, "initial_stock":130},
    {"product_id": "P05", "product_name": "Headphones", "category": "Electronics", "price": 2200, "initial_stock":210},

    #Fitness
    {"product_id": "P06", "product_name": "Yoga Mat", "category": "Fitness", "price": 799, "initial_stock":140},
    {"product_id": "P07", "product_name": "Jump Rope", "category": "Fitness", "price": 349, "initial_stock":220},
    {"product_id": "P08", "product_name": "Gym Gloves", "category": "Fitness", "price": 459, "initial_stock":180},
    {"product_id": "P09", "product_name": "Resistance Bands", "category": "Fitness", "price": 499, "initial_stock":250},
    {"product_id": "P10", "product_name": "Gym Ball", "category": "Fitness", "price": 879, "initial_stock":120},

    #Beauty
    {"product_id": "P11", "product_name": "Sunscreen SPF 50", "category": "Beauty", "price": 559, "initial_stock":230},
    {"product_id": "P12", "product_name": "Face Serum", "category": "Beauty", "price": 569, "initial_stock":210},
    {"product_id": "P13", "product_name": "Jade Roller", "category": "Beauty", "price": 1189, "initial_stock":300},
    {"product_id": "P14", "product_name": "Skin Tint", "category": "Beauty", "price": 1199, "initial_stock":220},
    {"product_id": "P15", "product_name": "Sheet Mask Pack", "category": "Beauty", "price": 579, "initial_stock":300},

    #Health
    {"product_id": "P16", "product_name": "Creatine Powder", "category": "Health", "price": 899, "initial_stock":130},
    {"product_id": "P17", "product_name": "Fish Oil Capsules", "category": "Health", "price": 639, "initial_stock":270},
    {"product_id": "P18", "product_name": "Multivitamin Tablets", "category": "Health", "price": 709, "initial_stock":310},
    {"product_id": "P19", "product_name": "Green Tea box", "category": "Health", "price": 459, "initial_stock":250},
    {"product_id": "P20", "product_name": "Whey Protein Bar", "category": "Health", "price": 299, "initial_stock":200},
]

#Cities, Payment methods and their respective weights
cities = ["Hyderabad", "Chennai", "Bengaluru", "Mumbai", "Punjab", "Kolkata"]
city_weights = [0.30, 0.08, 0.25, 0.20, 0.05, 0.12]

payment_methods = ["UPI", "COD", "Card", "Wallet", "Net Banking"]
payment_weights = [0.42, 0.15, 0.28, 0.05, 0.10]


#Function 1: get_next_day_number()
#To find out which day to generate

def get_next_day_number():
    db_path = "database/sales.db"

    if not os.path.exists(db_path):
        return 1
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
                       SELECT name FROM sqlite_master
                       WHERE type='table' AND name='sales_clean'
                       """)
        
        table_exists = cursor.fetchone()

        if not table_exists:
            conn.close()
            return 1
        

        cursor.execute("SELECT MAX(day_number) FROM sales_clean")

        result = cursor.fetchone()[0]

        conn.close()

        if result is None:
            return 1
        
        return result + 1
    
    except Exception as e:
        conn.close()
        print(f"Warning: {e}")
        return 1
    
#Funtion 2: get_current_stock_levels()
#To find out how much stock each product has right now

def get_current_stock_levels():
    db_path = "database/sales.db"

    stock = {p["product_id"]: p["initial_stock"] for p in products}

    if not os.path.exists(db_path):
        return stock
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
                       SELECT name FROM sqlite_master
                       WHERE type='table' AND name='sales_clean'
                       """)
        
        if not cursor.fetchone():
            conn.close()
            return stock
        

        cursor.execute("""
                       SELECT product_id, stock_remaining
                       FROM sales_clean
                       WHERE (product_id, day_number) IN
                       SELECT product_id, MAX(day_number)
                       FROM sales_clan
                       GROUP BY product_id)
                       """)
        
        rows = cursor.fetchall()
        conn.close()

        for product_id, stock_remaining in rows:
            stock[product_id] = stock_remaining

        return stock
    
    except Exception as e:
        conn.close()
        print(f"Warning: {e}")
        return stock
    
#Funtion 3: generate_day(day_number, stock_levels)