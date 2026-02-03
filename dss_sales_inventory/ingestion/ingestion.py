import os
import pandas as pd
import logging

# Logger configuration (assumed configured at project level)
dss_logger = logging.getLogger('dss_logger')

class SchemaValidationError(Exception):
    pass

def run_ingestion(correlation_id: str) -> dict:
    """
    Load raw sales and inventory data from CSV files and perform initial validation.

    Parameters
    ----------
    correlation_id : str
        Unique run identifier passed from pipeline for logging traceability.

    Returns
    -------
    dict
        Dictionary containing two validated DataFrames:
        {'sales': pd.DataFrame, 'inventory': pd.DataFrame}

    Raises
    ------
    FileNotFoundError
        If any raw CSV file is missing.
    SchemaValidationError
        If required columns are missing or DataFrames are empty.
    """
    # Log start of ingestion
    dss_logger.info(
        "Ingestion started",
        extra={
            "run_id": correlation_id,
            "stage": "INGESTION",
            "function": "run_ingestion",
            "rows_in": 0,
            "rows_out": None,
            "status": "STARTED"
        }
    )

    # Define file paths (use os.path.join for flexibility)
    base_path = r'C:\Data_Analysis\dss_sales_inventory\data\raw'
    sales_path = os.path.join(base_path, 'sales.csv')
    inventory_path = os.path.join(base_path, 'inventory.csv')

    # Check file existence
    if not os.path.exists(sales_path):
        error_msg = f"Sales file not found: {sales_path}"
        dss_logger.error(error_msg, extra={"run_id": correlation_id, "stage": "INGESTION", "function": "run_ingestion", "status": "FAILED"})
        raise FileNotFoundError(error_msg)

    if not os.path.exists(inventory_path):
        error_msg = f"Inventory file not found: {inventory_path}"
        dss_logger.error(error_msg, extra={"run_id": correlation_id, "stage": "INGESTION", "function": "run_ingestion", "status": "FAILED"})
        raise FileNotFoundError(error_msg)

    # Load CSVs
    sales_df = pd.read_csv(sales_path)
    inventory_df = pd.read_csv(inventory_path)

    # Validate required columns
    required_sales_cols = ['sale_id', 'product_id', 'date', 'quantity', 'unit_price', 'revenue']
    required_inventory_cols = ['product_id', 'date', 'stock_on_hand', 'reorder_point', 'lead_time_days', 'unit_cost']

    missing_sales = [col for col in required_sales_cols if col not in sales_df.columns]
    missing_inventory = [col for col in required_inventory_cols if col not in inventory_df.columns]

    if missing_sales:
        raise SchemaValidationError(f"Missing columns in sales.csv: {missing_sales}")
    if missing_inventory:
        raise SchemaValidationError(f"Missing columns in inventory.csv: {missing_inventory}")

    # Convert 'date' columns to datetime
    try:
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        inventory_df['date'] = pd.to_datetime(inventory_df['date'])
    except Exception as e:
        raise SchemaValidationError(f"Error converting 'date' columns to datetime: {str(e)}")

    # Validate numeric columns (must be >=0 or >0)
    numeric_checks_sales = {
        'sale_id': lambda x: x > 0,
        'product_id': lambda x: x > 0,
        'quantity': lambda x: x >= 0,
        'unit_price': lambda x: x > 0
    }

    for col, check in numeric_checks_sales.items():
        if not sales_df[col].apply(check).all():
            raise SchemaValidationError(f"Validation failed for sales column: {col}")

    numeric_checks_inventory = {
        'product_id': lambda x: x > 0,
        'stock_on_hand': lambda x: x >= 0,
        'reorder_point': lambda x: x > 0,
        'lead_time_days': lambda x: x >= 0,
        'unit_cost': lambda x: x > 0
    }

    for col, check in numeric_checks_inventory.items():
        if not inventory_df[col].apply(check).all():
            raise SchemaValidationError(f"Validation failed for inventory column: {col}")

    # Compute rows_out for logging
    rows_out_sales = len(sales_df)
    rows_out_inventory = len(inventory_df)
    rows_out_total = rows_out_sales + rows_out_inventory

    # Log successful completion
    dss_logger.info(
        "Ingestion completed successfully",
        extra={
            "run_id": correlation_id,
            "stage": "INGESTION",
            "function": "run_ingestion",
            "rows_in": 0,
            "rows_out": rows_out_total,
            "status": "SUCCESS"
        }
    )

    return {
        'sales': sales_df,
        'inventory': inventory_df
    }
