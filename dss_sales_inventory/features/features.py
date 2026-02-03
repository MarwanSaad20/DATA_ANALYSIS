import os
import pandas as pd
import logging

# Logger configuration (assumed configured at project level)
dss_logger = logging.getLogger('dss_logger')

def run_features(cleaned_data: dict, correlation_id: str) -> dict:
    """
    Compute daily features for sales, join with inventory, calculate stock_ratio,
    validate, and save feature CSVs.

    Enhancements:
    - Stock_ratio outliers are detected using IQR and clipped instead of raising an error.
    - Added logging warning for clipped stock_ratio values.
    """
    if cleaned_data is None or 'sales' not in cleaned_data or 'inventory' not in cleaned_data:
        raise ValueError("Invalid or missing cleaned_data")

    sales_df = cleaned_data['sales'].copy()
    inventory_df = cleaned_data['inventory'].copy()

    # Log start
    rows_in_sales = len(sales_df)
    rows_in_inventory = len(inventory_df)
    rows_in_total = rows_in_sales + rows_in_inventory
    dss_logger.info(
        "Features started",
        extra={"run_id": correlation_id, "stage": "FEATURES", "function": "run_features", 
               "rows_in": rows_in_total, "rows_out": None, "status": "STARTED"}
    )

    # ======================
    # Compute daily sales features
    # ======================
    sales_df['revenue'] = sales_df.apply(
        lambda x: x['revenue'] if 'revenue' in x and pd.notnull(x['revenue']) else x['quantity'] * x['unit_price'],
        axis=1
    )

    daily_sales = sales_df.groupby(['product_id', 'date'], as_index=False).agg(
        daily_quantity_sold=pd.NamedAgg(column='quantity', aggfunc='sum'),
        daily_revenue=pd.NamedAgg(column='revenue', aggfunc='sum')
    )

    # ======================
    # Join with inventory
    # ======================
    inventory_features = inventory_df.merge(
        daily_sales,
        on=['product_id', 'date'],
        how='left'
    )

    # Fill NaN daily values with 0 (no sales)
    inventory_features['daily_quantity_sold'] = inventory_features['daily_quantity_sold'].fillna(0)
    inventory_features['daily_revenue'] = inventory_features['daily_revenue'].fillna(0)

    # ======================
    # Calculate stock_ratio
    # ======================
    inventory_features['stock_ratio'] = inventory_features.apply(
        lambda x: x['stock_on_hand'] / max(x['daily_quantity_sold'], 1), axis=1
    )

    # ======================
    # Detect outliers using IQR and clip
    # ======================
    Q1 = inventory_features['stock_ratio'].quantile(0.25)
    Q3 = inventory_features['stock_ratio'].quantile(0.75)
    IQR = Q3 - Q1
    upper_limit = Q3 + 1.5 * IQR

    outliers = inventory_features[inventory_features['stock_ratio'] > upper_limit]
    if not outliers.empty:
        dss_logger.warning(
            f"{len(outliers)} stock_ratio values exceeded IQR upper limit ({upper_limit:.2f}) and were clipped",
            extra={"run_id": correlation_id, "stage": "FEATURES", "function": "run_features",
                   "rows_in": rows_in_total, "rows_out": len(inventory_features), "status": "WARNING"}
        )
        inventory_features['stock_ratio'] = inventory_features['stock_ratio'].clip(upper=upper_limit)

    # ======================
    # Validation
    # ======================
    if (daily_sales['daily_quantity_sold'] < 0).any() or (daily_sales['daily_revenue'] < 0).any():
        raise ValueError("Negative values found in sales features")
    if (inventory_features['stock_ratio'] < 0).any():
        raise ValueError("Negative stock_ratio values found")
    if daily_sales.duplicated(subset=['product_id','date']).any():
        raise ValueError("Duplicate rows found in daily_sales aggregation")
    if inventory_features.duplicated(subset=['product_id','date']).any():
        raise ValueError("Duplicate rows found in inventory_features")

    # ======================
    # Save feature CSVs
    # ======================
    processed_dir = r"C:\Data_Analysis\dss_sales_inventory\data\processed"
    os.makedirs(processed_dir, exist_ok=True)
    sales_features_path = os.path.join(processed_dir, "sales_features.csv")
    inventory_features_path = os.path.join(processed_dir, "inventory_features.csv")

    daily_sales.to_csv(sales_features_path, index=False)
    inventory_features.to_csv(inventory_features_path, index=False)

    # Log completion
    rows_out_sales = len(daily_sales)
    rows_out_inventory = len(inventory_features)
    rows_out_total = rows_out_sales + rows_out_inventory

    dss_logger.info(
        "Features completed successfully",
        extra={"run_id": correlation_id, "stage": "FEATURES", "function": "run_features",
               "rows_in": rows_in_total, "rows_out": rows_out_total, "status": "SUCCESS"}
    )

    return {'sales': daily_sales, 'inventory': inventory_features}
