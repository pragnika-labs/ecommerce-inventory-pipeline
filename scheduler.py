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
    
def daily_pipeline():
    current_day = get_current_day()
    if current_day >= target_days:
        print(f"[SCHEDULER] All {target_days} day complete - press Ctrl+C to stop")
        return
    print()
    print("-"*30)
    print(f"[SCHEDULER] Starting day {current_day + 1} of {target_days}")
    print("-"*30)

    print("[STEP 1] Running generate_data.py....")
    result_generate = subprocess.run(["python", "data/generate_data.py"])

    if result_generate.returncode != 0:
        print("[ERROR] generate_data.py failed")
        return
    
    if not os.path.exists("data/today.csv"):
        print("[ERROR] today.csv not found")
        return
    
    print("[STEP 2] Running etl_pipeline.py...")
    result_etl = subprocess.run(["python", "pipeline/etl_pipeline.py"])

    if result_etl.returncode != 0:
        print("[ERROR] etl_pipeline.py failed")
        return
    
    

