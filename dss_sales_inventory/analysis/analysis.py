import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Logger configuration (assumed configured at project level)
dss_logger = logging.getLogger('dss_logger')

def run_analysis(features_data: dict, correlation_id: str) -> None:
    """
    Generate descriptive statistics and diagnostic plots from featured data,
    and save summary and plots to reporting/outputs.
    """
    if features_data is None or 'sales' not in features_data or 'inventory' not in features_data:
        raise ValueError("Invalid or missing features_data")

    sales_df = features_data['sales'].copy()
    inventory_df = features_data['inventory'].copy()

    rows_in_sales = len(sales_df)
    rows_in_inventory = len(inventory_df)
    rows_in_total = rows_in_sales + rows_in_inventory

    dss_logger.info(
        "Analysis started",
        extra={"run_id": correlation_id, "stage": "ANALYSIS", "function": "run_analysis",
               "rows_in": rows_in_total, "rows_out": None, "status": "STARTED"}
    )

    # ======================
    # Descriptive statistics
    # ======================
    summary_sales = sales_df[['daily_quantity_sold', 'daily_revenue']].describe().T
    summary_inventory = inventory_df[['stock_ratio', 'stock_on_hand']].describe().T

    summary_df = pd.concat([summary_sales, summary_inventory], axis=0)
    summary_df.index.name = 'metric'

    # ======================
    # Save summary CSV
    # ======================
    output_dir = r"C:\Data_Analysis\dss_sales_inventory\reporting\outputs"
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, "analysis_summary.csv")
    summary_df.to_csv(summary_path)

    # ======================
    # Generate plots
    # ======================
    plots_dir = os.path.join(output_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    # Histogram: daily_quantity_sold
    plt.figure(figsize=(8,6))
    sns.histplot(sales_df['daily_quantity_sold'], bins=30, kde=False)
    plt.title("Histogram of Daily Quantity Sold")
    plt.xlabel("Daily Quantity Sold")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "histogram_daily_quantity.png"))
    plt.close()

    # Line plot: daily_revenue trend
    revenue_trend = sales_df.groupby('date')['daily_revenue'].sum().reset_index()
    plt.figure(figsize=(10,6))
    sns.lineplot(data=revenue_trend, x='date', y='daily_revenue')
    plt.title("Daily Revenue Trend")
    plt.xlabel("Date")
    plt.ylabel("Total Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, "trend_daily_revenue.png"))
    plt.close()

    # ======================
    # Validation
    # ======================
    if summary_df.empty:
        raise ValueError("Analysis summary is empty")
    if not os.path.exists(summary_path):
        raise ValueError("Summary CSV was not saved successfully")

    # Log completion
    dss_logger.info(
        "Analysis completed successfully",
        extra={"run_id": correlation_id, "stage": "ANALYSIS", "function": "run_analysis",
               "rows_in": rows_in_total, "rows_out": len(summary_df), "status": "SUCCESS"}
    )
