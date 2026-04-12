import schedule
import time
import subprocess
import sqlite3
import os

target_days = 30

def current_day():
    db_path = "database/sales.db"

    if not os.path.exists(db_path):
        return 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
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
    

