# E-Commerce Inventory & Sales Analytics Pipeline

## Business Problem
E-commerce businesses lose revenue when products run out of stock and there is no system in place to detect it early enough. This project builds a data pipeline that simulates 30 days of daily sales across 20 products and 6 cities, stores clean data in a SQL database and analyses stockout impact so the business can make smarter restocking decisions.

## How the Pipeline Works

graph LR
generate_data.py -> today.csv -> etl_pipeline.py -> sales.db

1. `generate_data.py` checks the database for the last recorded day.
2. Generates that day's orders with realistic messy customer data.
3. Saves to `today.csv` — a temporary handoff file.
4. `etl_pipeline.py` reads `today.csv`, cleans all data quality issues, appends clean rows to `sales.db`, then deletes `today.csv`.
5. `scheduler.py` runs both scripts automatically on a daily schedule.
6. `insights.ipynb` is run manually by the analyst to review the latest data, charts, and business insights.

## Key Findings
- Wireless Earbuds generated Rs.646,506 in revenue but caused Rs.593,730 in lost sales after going viral on Day 8.
- First stockout occurred on 2024-03-10 --> stock depleted within 2 days.
- Hyderabad drives 35.6% of all orders --> highest demand city.
- Electronics accounts for 55% of total revenue.
- 18 out of 20 products flagged for reorder after 30 days.
- Total lost revenue across 30 days: Rs.1,115,274.
- UPI is the dominant payment method at 41.2% of transactions.

## Recommendations
- Implement dynamic reorder threshold at average daily sales × 2.
- Prioritise Hyderabad and Bengaluru for restocking - together they account for over 55% of total revenue.
- Increase initial stock for Electronics category by 30%.
- Monitor high-demand products weekly - viral spikes deplete stock within days.

## Data Quality Issues Handled by ETL
| Issue | How it was fixed |
|---|---|
| Missing customer names | Filled with "Unknown" |
| Missing phone numbers | Filled with "Unknown" |
| Missing city values | Filled with "Unknown" |
| Wrong text casing | Fixed with `.str.title()` |
| Phone formatting (dashes, spaces) | Cleaned with `.str.replace()` |
| Duplicate orders | Removed with `drop_duplicates()` |
| Invalid phone lengths | Flagged in `phone_valid` column |

## Tech Stack
- **Python** - core language
- **Pandas** - data cleaning and transformation
- **SQLite** - database storage
- **SQLAlchemy** - Python to SQL connection
- **Matplotlib + Seaborn** - charts and visualizations
- **Schedule** - Data generation and ETL pipeline automation

## Project Structure
ecommerce-inventory-pipeline/
├── analysis/
│   ├── charts/
│   │   ├── chart1_daily_revenue.png
│   │   ├── chart2_revenue_by_category.png
│   │   ├── chart3_top_products.png
│   │   ├── chart4_city_demand.png
│   │   ├── chart5_lost_revenue.png
│   │   └── payment_methods.png
│   └── insights.ipynb
├── data/
│   └── generate_data.py
├── database/
│   └── sales.db
├── pipeline/
│   └── etl_pipeline.py
├── queries/
│   ├── city_demand.sql
│   ├── lost_revenue.sql
│   ├── reorder_alert.sql
│   ├── revenue_by_category.sql
│   └── top_products.sql
├── .gitattributes
├── .gitignore
├── README.md
└── scheduler.py

## How to Run

### Install dependencies
```bash
pip install pandas numpy sqlalchemy matplotlib seaborn schedule jupyter
```

### Run the scheduler to simulate all 30 days
```bash
python scheduler.py
```

### Open analysis notebook manually
```bash
jupyter notebook analysis/insights.ipynb
```
