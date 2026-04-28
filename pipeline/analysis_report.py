import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

db_path = os.path.join(base_dir, "database", "sales.db").replace("\\", "/")
engine = create_engine(f"sqlite:///{db_path}")

def read_sql_file(filename):
    path = os.path.join(base_dir, "queries", filename)
    with open(path, "r") as f:
        return f.read()
    
sns.set_theme(style="whitegrid")
plt.rcParams["font.size"] = 11

day_number = pd.read_sql(
    "SELECT MAX(day_number) FROM sales_clean", engine).iloc[0, 0]

base_date = datetime(2024, 3, 1)
simulated_date = base_date + timedelta(days=int(day_number) - 1)
today = simulated_date.strftime("%Y-%m-%d")


charts_dir = os.path.join(
    base_dir, "analysis", "auto_charts",
    f"day_{day_number:02d}")
reports_dir = os.path.join(
    base_dir, "analysis", "reports")

os.makedirs(charts_dir, exist_ok=True)
os.makedirs(reports_dir, exist_ok=True)

#STOCKOUT ALERT
print("\n[1] Running proactive reorder alert check...\n")

reorder_df = pd.read_sql(
    read_sql_file("reorder_alert.sql"), engine)

critical_df = reorder_df[
    reorder_df["alert_status"] == "REORDER NOW"].copy()

lost_df = pd.read_sql(
    read_sql_file("lost_revenue.sql"), engine)

already_stocked_out = set(lost_df["product_name"].tolist())

if len(critical_df) == 0:
    alert_status = "ALL CLEAR"
    print("STATUS: ALL CLEAR")
    print("All products have healthy stock levels\n")

else:
    alert_status = "ALERT"
    print(f"STATUS: ALERT - {len(critical_df)} product(s) need attention\n")

    for _, row in critical_df.iterrows():
        product = row["product_name"]

        if product in already_stocked_out:
            lost_row = lost_df[lost_df["product_name"] == product]
            lost_amount = lost_row.iloc[0]["total_lost_revenue"]
            
            print(f"[CRITICAL] {product}")
            print(f"Current stock     : {int(row['current_stock'])} units")
            print(f"Reorder threshold : {int(row['reorder_threshold'])} units")
            print(f"Lost revenue so far: Rs.{lost_amount:,.0f}")
            print(f"Action: Stock depleted - reorder immediately\n")

        else:
            print(f"[WARNING] {product}")
            print(f"Current stock     : {int(row['current_stock'])} units")
            print(f"Reorder threshold : {int(row['reorder_threshold'])} units")
            print(f"Action: Stock below 2-day buffer - reorder before stockout occurs\n")

#KPI SUMMARIES
print("[2] KPI summary...")

total_revenue = pd.read_sql(
    "SELECT SUM(revenue) FROM sales_clean",
    engine).iloc[0, 0]

total_orders = pd.read_sql(
    "SELECT COUNT(DISTINCT order_id) FROM sales_clean",
    engine).iloc[0, 0]

total_lost = pd.read_sql(
    "SELECT SUM(lost_revenue) FROM sales_clean",
    engine).iloc[0, 0]

top_city = pd.read_sql("""
                       SELECT city FROM sales_clean
                       WHERE city != 'Unknown'
                       GROUP BY city
                       ORDER BY SUM(revenue) DESC
                       LIMIT 1
                       """, engine).iloc[0, 0]

print(f"Total revenue : Rs.{total_revenue:,.0f}")
print(f"Total orders  : {int(total_orders):,}")
print(f"Lost revenue  : Rs.{total_lost:,.0f}")
print(f"Top city      : {top_city}")
print(f"Days of data  : {day_number}")

#CHARTS
print("\n[3] Generating charts...")

#Chart 1 - Daily revenue vs lost revenue over time

daily = pd.read_sql("""
                    SELECT day_number,
                    SUM(revenue) AS revenue,
                    SUM(lost_revenue) AS lost_revenue
                    FROM sales_clean
                    GROUP BY day_number
                    ORDER BY day_number
                    """, engine)

fig, ax = plt.subplots(figsize = (12, 5))

ax.plot(daily["day_number"], daily["revenue"],
        color = "#468432", linewidth = 2.5,
        marker = "o", markersize = 4,
        label = "Revenue Earned")

ax.plot(daily["day_number"], daily["lost_revenue"],
        color = "red", linewidth = 2,
        linestyle = "--", marker = "o", markersize = 4,
        label = "Lost Revenue")

ax.axvline(x = 8, color = "#1A3263", linestyle = ":",
           linewidth = 3, label = "Viral Spike Day 8")

ax.set_xlim(1, day_number + 0.5)

ax.set_title(
    f"Daily Revenue Trend - Day 1 to Day {day_number}",
    fontsize = 13, fontweight = "bold")
ax.set_xlabel("Day Number")
ax.set_ylabel("Revenue (Rs.)")
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))
ax.legend()
plt.tight_layout()
plt.savefig(os.path.join(charts_dir, "chart1_daily_revenue.png"),
            dpi=150, bbox_inches = "tight")
plt.close()
print("Saved chart1_daily_revenue.png")

#Chart 2 - Revenue by Category
category_df = pd.read_sql(
    read_sql_file("revenue_by_category.sql"), engine)

my_colors = sns.color_palette("colorblind",5)

fig, ax = plt.subplots(figsize = (8, 5))
bars = ax.bar(
    category_df["category"],
    category_df["total_revenue"],
    color = my_colors,
    edgecolor = "white")

for bar in bars:
    h = bar.get_height()
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        h + 5000, f"Rs.{h:,.0f}",
        ha = "center", va = "bottom", fontsize = 9)

ax.set_title("Revenue by Category",
             fontsize = 13, fontweight = "bold")
ax.set_ylabel("Revenue (Rs.)")
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))
plt.tight_layout()
plt.savefig(os.path.join(charts_dir, "chart2_category.png"),
            dpi = 150, bbox_inches = "tight")
plt.close()
print("Saved chart2_category.png")

#Chart 3 - Revenue by City
cities_df = pd.read_sql(
    read_sql_file("city_demand.sql"), engine)

fig, ax = plt.subplots(figsize = (10, 5))
bars = ax.barh(
    cities_df["city"][::-1],
    cities_df["total_revenue"][::-1],
    color = "#B35656", edgecolor = "white")

for bar in bars:
    w = bar.get_width()
    ax.text(
        w + 3000,
        bar.get_y() + bar.get_height() / 2,
        f"Rs.{w:,.0f}", va = "center", fontsize = 9)

ax.set_title("Revenue by City",
             fontsize = 13, fontweight = "bold")
ax.set_xlabel("Revenue (Rs.)")
ax.xaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))
plt.tight_layout()
plt.savefig(os.path.join(charts_dir, "chart3_city.png"),
            dpi = 150, bbox_inches = "tight")
plt.close()
print("Saved chart3_city.png")

#Chart 4 - Lost Revenue by Product
if len(lost_df) > 0:
    fig, ax = plt.subplots(figsize=(10, max(5, len(lost_df) * 0.4)))
    bars = ax.barh(
        lost_df["product_name"][::-1],
        lost_df["total_lost_revenue"][::-1],
        color = "#5F4444", edgecolor = "white")
    
    if len(lost_df) < 5:
        ax.set_ylim(-0.5, 4.5)

    for bar in bars:
        w = bar.get_width()
        if w > 0:
            ax.text(
                w + 1000,
                bar.get_y() + bar.get_height() / 2,
                f"Rs.{w:,.0f}", va = "center", fontsize = 9)

    ax.set_title("Lost Revenue by Product (Stockout Impact)",
                 fontsize = 13, fontweight = "bold")
    ax.set_xlabel("Lost Revenue (Rs.)")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"Rs.{x:,.0f}"))
    plt.tight_layout()
    plt.savefig(os.path.join(charts_dir, "chart4_lost_revenue.png"),
                dpi = 150, bbox_inches = "tight")
    plt.close()
    print("Saved chart4_lost_revenue.png")
else:
    print("Chart 4 skipped - no stockouts yet")


#TEXT REPORT
print("\n[4] Saving report...")

report_name = f"report_day{day_number:02d}.txt"
report_path = os.path.join(reports_dir, report_name)

with open(report_path, "w") as f:
    f.write("=" * 50 + "\n")
    f.write("AUTOMATED ANALYSIS REPORT\n")
    f.write(f"Day  : {day_number}\n")
    f.write(f"Date : {today}\n")
    f.write("=" * 50 + "\n\n")

    f.write(f"ALERT STATUS: {alert_status}\n\n")

    if len(critical_df) > 0:
        f.write("PRODUCTS NEEDING REORDER\n")
        f.write("-" * 30 + "\n")

        for _, row in critical_df.iterrows():
            product = row["product_name"]
            if product in already_stocked_out:
                lost_row = lost_df[lost_df["product_name"] == product]
                lost_amount = lost_row.iloc[0]["total_lost_revenue"]
                
                f.write(f"[CRITICAL] {product}\n")
                f.write(f"Current stock     : {int(row['current_stock'])}\n")
                f.write(f"Reorder threshold : {int(row['reorder_threshold'])}\n")
                f.write(f"Lost so far       : Rs.{lost_amount:,.0f}\n\n")
            
            else:
                f.write(f"[WARNING] {product}\n")
                f.write(f"Current stock     : {int(row['current_stock'])}\n")
                f.write(f"Reorder threshold : {int(row['reorder_threshold'])}\n\n")

    f.write("KPI SUMMARY\n")
    f.write("-" * 30 + "\n")
    f.write(f"Total revenue       : Rs.{total_revenue:,.0f}\n")
    f.write(f"Total orders        : {int(total_orders):,}\n")
    f.write(f"Total lost revenue  : Rs.{total_lost:,.0f}\n")
    f.write(f"Top city            : {top_city}\n")
    f.write(f"Days of data        : {day_number}\n\n")
    f.write(f"Charts saved to:\n")
    f.write(f"analysis/auto_charts/day_{day_number:02d}/\n")

print(f"Saved {report_name}")

print()
print("=" * 50)
print(f"REPORT COMPLETE")
print(f"Status  : {alert_status}")
print(f"Charts  : analysis/auto_charts/day_{day_number:02d}/")
print(f"Report  : analysis/reports/{report_name}")
print("=" * 50)
print()