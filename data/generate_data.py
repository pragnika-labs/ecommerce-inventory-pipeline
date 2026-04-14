import pandas as pd
import random
import sqlite3
import os
from datetime import datetime, timedelta

# Product Catalogue (20 products with prices and starting warehouse stock)
products = [
    # Electronics
    {"product_id": "P01", "product_name": "Wireless Earbuds", "category": "Electronics", "price": 1399, "initial_stock":200},
    {"product_id": "P02", "product_name": "Bluetooth Speaker", "category": "Electronics", "price": 1899, "initial_stock":170},
    {"product_id": "P03", "product_name": "Wireless Mouse", "category": "Electronics", "price": 900, "initial_stock":220},
    {"product_id": "P04", "product_name": "Power Bank", "category": "Electronics", "price": 2799, "initial_stock":130},
    {"product_id": "P05", "product_name": "Headphones", "category": "Electronics", "price": 2200, "initial_stock":210},

    # Fitness
    {"product_id": "P06", "product_name": "Yoga Mat", "category": "Fitness", "price": 799, "initial_stock":140},
    {"product_id": "P07", "product_name": "Jump Rope", "category": "Fitness", "price": 349, "initial_stock":220},
    {"product_id": "P08", "product_name": "Gym Gloves", "category": "Fitness", "price": 459, "initial_stock":180},
    {"product_id": "P09", "product_name": "Resistance Bands", "category": "Fitness", "price": 499, "initial_stock":250},
    {"product_id": "P10", "product_name": "Gym Ball", "category": "Fitness", "price": 879, "initial_stock":120},

    # Beauty
    {"product_id": "P11", "product_name": "Sunscreen SPF 50", "category": "Beauty", "price": 559, "initial_stock":230},
    {"product_id": "P12", "product_name": "Face Serum", "category": "Beauty", "price": 569, "initial_stock":210},
    {"product_id": "P13", "product_name": "Jade Roller", "category": "Beauty", "price": 1189, "initial_stock":300},
    {"product_id": "P14", "product_name": "Skin Tint", "category": "Beauty", "price": 1199, "initial_stock":220},
    {"product_id": "P15", "product_name": "Sheet Mask Pack", "category": "Beauty", "price": 579, "initial_stock":300},

    # Health
    {"product_id": "P16", "product_name": "Creatine Powder", "category": "Health", "price": 899, "initial_stock":130},
    {"product_id": "P17", "product_name": "Fish Oil Capsules", "category": "Health", "price": 639, "initial_stock":270},
    {"product_id": "P18", "product_name": "Multivitamin Tablets", "category": "Health", "price": 709, "initial_stock":310},
    {"product_id": "P19", "product_name": "Green Tea box", "category": "Health", "price": 459, "initial_stock":250},
    {"product_id": "P20", "product_name": "Whey Protein Bar", "category": "Health", "price": 299, "initial_stock":200},
]

# Demographics (Weighted to make data look realistic, mostly Hyderabad and UPI)
cities = ["Hyderabad", "Chennai", "Bengaluru", "Mumbai", "Punjab", "Kolkata"]
city_weights = [0.30, 0.08, 0.25, 0.20, 0.05, 0.12]

payment_methods = ["UPI", "COD", "Card", "Wallet", "Net Banking"]
payment_weights = [0.42, 0.15, 0.28, 0.05, 0.10]

#Customer first and last names
first_names = [
    "Aarav", "Pooja", "Yogesh", "Meera", "Nikhil",
    "Mrudula", "Snehith", "Rohan", "Abhiram", "Arjun",
    "Kavya", "Nisha", "Rishika", "Chitra", "Pragnya"
]

last_names = [
    "Sharma", "Singh", "Iyer", "Reddy", "Gupta",
    "Naidu", "Kumar", "Patel", "Varma", "Rao",
    "Menon", "Goud", "Shetty", "Mehta", "Choudary"
]

# Function 1: get_next_day_number()
# Checks the database to figure out what day we need to generate next.
def get_next_day_number():
    db_path = "database/sales.db"

    # If no database exists, it's the first day
    if not os.path.exists(db_path):
        return 1
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if the clean sales table actually exists yet
        cursor.execute("""
                       SELECT name FROM sqlite_master
                       WHERE type='table' AND name='sales_clean'
                       """)
        table_exists = cursor.fetchone()

        if not table_exists:
            conn.close()
            return 1
        
        # Find the highest day saved so far, and add 1 for today
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
    
# Funtion 2: get_current_stock_levels()
# Looks at yesterday's data to see exactly how much stock is left today.
def get_current_stock_levels():
    db_path = "database/sales.db"
    
    # Default to initial stock assuming it's Day 1
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
        
        # Grab the leftover stock count from the absolute last day each product was sold
        cursor.execute("""
                       SELECT product_id, stock_remaining
                       FROM sales_clean
                       WHERE (product_id, day_number) IN (
                           SELECT product_id, MAX(day_number)
                           FROM sales_clean
                           GROUP BY product_id
                       )
                       """)
        
        rows = cursor.fetchall()
        conn.close()

        # Update our dictionary with the actual leftovers
        for product_id, stock_remaining in rows:
            stock[product_id] = stock_remaining

        return stock
    
    except Exception as e:
        conn.close()
        print(f"Warning: {e}")
        return stock
    
# Funtion 3: generate_day(day_number, stock_levels)
# Generates the actual random orders, does the math for lost revenue and introduces realistic messy customer data.
def generate_day(day_number, stock_levels):
    start_date = datetime(2024,3,1)
    current_date = start_date + timedelta(days=day_number - 1)
    rows = []

    # Create unique order IDs that don't overlap across days
    order_num = (day_number - 1) * 50 + 1000
    
    # Decide how many people buy something today
    num_orders = random.randint(30,60)

    for _ in range(num_orders):
        product = random.choice(products)
        pid = product["product_id"]
        price = product["price"]

        # Viral logic: P01 explodes in popularity on Day 8
        if pid == "P01":
            if day_number <= 7:
                qty = random.randint(2,8)
            elif day_number == 8:
                qty = random.randint(35,50)
            elif day_number == 9:
                qty = random.randint(20,30)
            elif day_number == 10:
                qty = random.randint(10,15)
            else:
                qty = random.randint(2,8)
        else:
            qty = random.randint(2,8)

        # Calculate if we have enough stock, or if we lose the sale
        current_stock = stock_levels[pid]

        if current_stock <= 0:
            fulfilled = 0
            lost_sales = qty
            lost_revenue = qty * price
            stock_levels[pid] = 0

        elif current_stock < qty:
            fulfilled = current_stock
            lost_sales = qty - current_stock
            lost_revenue = lost_sales * price
            stock_levels[pid] = 0

        else:
            fulfilled = qty
            lost_sales = 0
            lost_revenue = 0
            stock_levels[pid] -= qty    

        revenue = fulfilled * price
        stock_remaining = stock_levels[pid]
        
        city = random.choices(cities, weights = city_weights)[0]
        payment_method = random.choices(payment_methods, weights = payment_weights)[0]

        #Simulate messy customer data
        #Customer name errors
        first = random.choice(first_names)
        last = random.choice(last_names)
        customer_name = f"{first} {last}"

        if random.random() < 0.07:
            customer_name = None

        elif random.random() < 0.10:
            customer_name = customer_name.lower()

        elif random.random() < 0.08:
            customer_name = customer_name.upper()

        elif random.random() < 0.08:
            customer_name = f"{first}{last}"

        #Customer phone number errors
        first_digit = str(random.randint(6,9))
        remaining_digits = "".join([str(random.randint(0,9)) for _ in range(9)])
        customer_phone = first_digit + remaining_digits

        if random.random() < 0.08:
            customer_phone = None

        elif random.random() < 0.10:
            customer_phone = f"{customer_phone[:3]}-{customer_phone[3:6]}-{customer_phone[6:]}"

        elif random.random() < 0.07:
            customer_phone = f"{customer_phone[:3]} {customer_phone[3:6]} {customer_phone[6:]}"

        elif random.random() < 0.05:
            customer_phone = customer_phone[:8]

        #City inconsistencies
        if random.random() < 0.08:
            city = None

        elif random.random() < 0.15:
            casing = random.choice(["lower","upper"])
            if casing == "lower":
                city = city.lower()
            else:
                city = city.upper()

        #Duplicate orders
        if len(rows)> 0 and random.random() < 0.05:
            order_num = int(rows[-1]["order_id"].split("-")[1])

        # Save this receipt
        rows.append({
            "order_id" : f"ORD-{order_num}",
            "date" : current_date.strftime("%Y-%m-%d"),
            "day_number" : day_number,
            "product_id" : pid,
            "product_name" : product["product_name"],
            "category" : product["category"],
            "price" : price,
            "quantity" : qty,
            "fulfilled" : fulfilled,
            "revenue" : revenue,
            "city" : city,
            "payment_method" : payment_method,
            "customer_name" : customer_name,
            "customer_phone" : customer_phone,
            "stock_remaining" : stock_remaining,
            "lost_sales" : lost_sales,
            "lost_revenue" : lost_revenue,
        })

        order_num += 1

    # Convert all receipts into a clean Pandas table
    return pd.DataFrame(rows)

# Function 4: main()
# Wakes up the script, runs the daily tasks, and saves the output.
def main():
    day_number = get_next_day_number()
    stock_levels = get_current_stock_levels()

    print(f"Generating data for Day {day_number}...\n")

    df = generate_day(day_number, stock_levels)

    # Save today's temporary raw data for the ETL pipeline to find later
    df.to_csv("data/today.csv", index=False)

    # Print a nice summary to the terminal
    print("=" * 30)
    print(f"Day {day_number} complete")
    print(f"Orders generated: {len(df)}")
    print(f"Date: {df['date'].iloc[0]}")
    print(f"Revenue today: Rs.{df['revenue'].sum():,.0f}")
    print(f"Lost revenue: Rs.{df['lost_revenue'].sum():,.0f}")
    print(f"Saved to: data/today.csv")
    print("=" * 30)

if __name__ == "__main__":
    main()