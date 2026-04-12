import schedule
import time
import subprocess
import sqlite3
import os

target_days = 30

def get_current_day():
    db_path = "database/sales.db"

    if not os.path.exists(db_path):
        return 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:

        cursor.execute("""
                       SELECT name FROM sqlite_master
                       WHERE type='table' AND name='sales_clean'
                       """)
        
        if not cursor.fetchone():
            conn.close()
            return 0
        
        cursor.execute("SELECT MAX(day_number) FROM sales_clean")
        result = cursor.fetchone()[0]
        conn.close()

        return result if result is not None else 0
    
    except:
        conn.close()
        return 0
    
def generate_job():
    current_day = get_current_day()

    if current_day >= target_days:
        print(f"Target of {target_days} days reached.")
        return
    
    print(f"\n[SCHEDULER] Running generate_data.py - building Day {current_day + 1}....")
    subprocess.run(["python", "data/generate_data.py"])

def etl_job():
    if not os.path.exists("data/today.csv"):
        print("[SCHEDULER] today.csv not found")
        return
    
    current_day = get_current_day()

    if current_day >= target_days:
        print(f"Target of {target_days} days reached.")
        return
    
    print(f"[SCHEDULER] Running etl_pipeline.py - cleaning and loading {current_day + 1}....")
    subprocess.run(["python", "pipeline/etl_pipeline.py"])

    

      

