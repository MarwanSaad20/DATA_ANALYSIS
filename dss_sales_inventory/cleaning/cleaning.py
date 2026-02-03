import os
import pandas as pd
import logging

# Logger configuration (assumed configured at project level)
dss_logger = logging.getLogger('dss_logger')

def run_cleaning(data: dict, correlation_id: str) -> dict:
    """
    Clean and validate raw sales and inventory DataFrames, convert types, handle critical nulls,
    remove duplicates, perform referential and time series validation, and save cleaned CSVs.
    """
    if data is None or 'sales' not in data or 'inventory' not in data or data['sales'] is None or data['inventory'] is None:
        error_msg = "Invalid or missing data from ingestion stage"
        dss_logger.error(
            error_msg,
            extra={"run_id": correlation_id, "stage": "CLEANING", "function": "run_cleaning", "rows_in": 0, "rows_out": 0, "status": "FAILED"}
        )
        raise ValueError(error_msg)

    # Log start
    rows_in_sales = len(data['sales'])
    rows_in_inventory = len(data['inventory'])
    rows_in_total = rows_in_sales + rows_in_inventory
    dss_logger.info(
        "Cleaning started",
        extra={"run_id": correlation_id, "stage": "CLEANING", "function": "run_cleaning", "rows_in": rows_in_total, "rows_out": None, "status": "STARTED"}
    )

    # Extract DataFrames
    sales_df = data['sales'].copy()
    inventory_df = data['inventory'].copy()

    # ======================
    # Convert 'date' columns
    # ======================
    try:
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        inventory_df['date'] = pd.to_datetime(inventory_df['date'])
    except Exception as e:
        raise ValueError(f"Date conversion failed: {e}")

    # ======================
    # Handle critical nulls
    # ======================
    critical_sales_cols = ['sale_id', 'product_id', 'date', 'quantity', 'unit_price']
    critical_inventory_cols = ['product_id', 'date', 'stock_on_hand', 'reorder_point', 'lead_time_days', 'unit_cost']

    if sales_df[critical_sales_cols].isnull().any().any():
        raise ValueError("Critical nulls found in sales data")
    if inventory_df[critical_inventory_cols].isnull().any().any():
        raise ValueError("Critical nulls found in inventory data")

    # Fill missing revenue with quantity * unit_price if revenue column exists
    if 'revenue' in sales_df.columns:
        sales_df['revenue'] = sales_df['revenue'].fillna(sales_df['quantity'] * sales_df['unit_price'])

    # ======================
    # Remove duplicates
    # ======================
    sales_df = sales_df.drop_duplicates(subset=['sale_id'])
    inventory_df = inventory_df.drop_duplicates(subset=['product_id', 'date'])

    # ======================
    # Time series validation
    # ======================
    for df, name in [(sales_df, 'sales'), (inventory_df, 'inventory')]:
        grouped = df.groupby('product_id')
        for product_id, group in grouped:
            group_sorted = group.sort_values('date')
            if not group_sorted['date'].is_monotonic_increasing:
                raise ValueError(f"Dates not monotonic increasing for product_id {product_id} in {name}")
            if (group_sorted['date'].diff().dt.days[1:] > 30).any():
                raise ValueError(f"Gap >30 days for product_id {product_id} in {name}")

    # ======================
    # Referential integrity
    # ======================
    sales_products = set(sales_df['product_id'].unique())
    inventory_products = set(inventory_df['product_id'].unique())
    active_products = sales_products

    missing_in_inventory = sales_products - inventory_products
    if missing_in_inventory:
        raise ValueError(f"Products in sales missing in inventory: {missing_in_inventory}")

    missing_in_sales = active_products - sales_products
    # optional: raise if inventory has products never sold? can skip

    # ======================
    # Save cleaned CSVs
    # ======================
    processed_dir = r"C:\Data_Analysis\dss_sales_inventory\data\processed"
    os.makedirs(processed_dir, exist_ok=True)
    sales_cleaned_path = os.path.join(processed_dir, "sales_cleaned.csv")
    inventory_cleaned_path = os.path.join(processed_dir, "inventory_cleaned.csv")

    sales_df.to_csv(sales_cleaned_path, index=False)
    inventory_df.to_csv(inventory_cleaned_path, index=False)

    # Compute rows_out for logging
    rows_out_sales = len(sales_df)
    rows_out_inventory = len(inventory_df)
    rows_out_total = rows_out_sales + rows_out_inventory

    dss_logger.info(
        "Cleaning completed successfully",
        extra={"run_id": correlation_id, "stage": "CLEANING", "function": "run_cleaning", "rows_in": rows_in_total, "rows_out": rows_out_total, "status": "SUCCESS"}
    )

    return {'sales': sales_df, 'inventory': inventory_df}
