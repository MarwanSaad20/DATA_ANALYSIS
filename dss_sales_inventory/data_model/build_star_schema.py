"""
Week 10: Data Modeling Layer (Star Schema Builder) - FINAL
Description: 
    - Implements strict Star Schema with unified surrogate key handling.
    - Performs pre-load validation and referential integrity checks.
    - Enforces Grain Protection: (Product x Date x Region).
    - Includes explicit lookup completeness checks.
    - Updated to handle 'daily_quantity_sold' and 'daily_revenue' inputs.
    
Author: Data Engineer
Version: 1.5
"""

import os
import sys
import sqlite3
import pandas as pd
import uuid
import logging
import numpy as np
from datetime import datetime

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'analysis', 'analytics.db')
SQL_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'data_model', 'data_model.sql')
ERD_OUTPUT_PATH = os.path.join(PROJECT_ROOT, 'data_model', 'erd_diagram')

INPUT_SALES = os.path.join(PROJECT_ROOT, 'data', 'processed', 'sales_cleaned.csv')
INPUT_INVENTORY = os.path.join(PROJECT_ROOT, 'data', 'processed', 'inventory_features.csv')

# ==============================================================================
# LOGGING SETUP
# ==============================================================================
def setup_logging(correlation_id):
    """Configures logging to match project standards."""
    log_format = f'%(asctime)s - %(levelname)s - [{correlation_id}] - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger(__name__)

# ==============================================================================
# DATA INGESTION
# ==============================================================================
def load_data(logger):
    """Loads required CSV files with strict type enforcement."""
    logger.info("Step 1: Loading raw data from Processed directory.")
    
    if not os.path.exists(INPUT_SALES) or not os.path.exists(INPUT_INVENTORY):
        logger.error(f"Missing input files. Checked: {INPUT_SALES}, {INPUT_INVENTORY}")
        raise FileNotFoundError("Critical input files missing.")

    try:
        sales_df = pd.read_csv(INPUT_SALES)
        inv_df = pd.read_csv(INPUT_INVENTORY)
        
        # Enforce Date Type
        sales_df['date'] = pd.to_datetime(sales_df['date'])
        
        logger.info(f"Loaded Sales: {sales_df.shape[0]} rows.")
        logger.info(f"Loaded Inventory: {inv_df.shape[0]} rows.")
        
        return {'sales': sales_df, 'inventory': inv_df}
    
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise

# ==============================================================================
# DIMENSION BUILDERS (Python-Managed Keys)
# ==============================================================================
def build_dim_region(logger):
    """Creates the Dim_Region with explicit ID."""
    logger.info("Building Dim_Region.")
    df = pd.DataFrame([{
        'region_id': 1,
        'region_name': 'ALL'
    }])
    return df

def build_dim_date(sales_df, logger):
    """Creates Dim_Date with Python-generated Surrogate Keys."""
    logger.info("Building Dim_Date.")
    
    # 1. Extract Unique Dates
    dates = sales_df['date'].dropna().unique()
    dim_date = pd.DataFrame({'full_date_dt': dates})
    dim_date = dim_date.sort_values('full_date_dt').reset_index(drop=True)
    
    # 2. Generate Surrogate Key (Python Sequence)
    # Starting at 1000 to clearly distinguish from simple counters
    dim_date['date_id'] = range(1000, 1000 + len(dim_date))
    
    # 3. Format Attributes (Strict YYYY-MM-DD for SQLite)
    dim_date['full_date'] = dim_date['full_date_dt'].dt.strftime('%Y-%m-%d')
    dim_date['day'] = dim_date['full_date_dt'].dt.day
    dim_date['month'] = dim_date['full_date_dt'].dt.month
    dim_date['quarter'] = dim_date['full_date_dt'].dt.quarter
    dim_date['year'] = dim_date['full_date_dt'].dt.year
    
    # Check for duplicates
    if dim_date['full_date'].duplicated().any():
        raise ValueError("Duplicate dates generated in Dim_Date.")
        
    # Return strict schema columns
    return dim_date[['date_id', 'full_date', 'day', 'month', 'quarter', 'year']]

def build_dim_product(sales_df, inv_df, logger):
    """Creates Dim_Product. Validates uniqueness of Business Key."""
    logger.info("Building Dim_Product.")
    
    # 1. Derive Attributes
    # Calculate Unit Price from Sales (Daily Revenue / Daily Quantity)
    # Use a copy to avoid SettingWithCopyWarning on the input dataframe
    sales_temp = sales_df.copy()
    
    # Avoid division by zero
    sales_temp['unit_price_calc'] = np.where(
        sales_temp['daily_quantity_sold'] > 0,
        sales_temp['daily_revenue'] / sales_temp['daily_quantity_sold'],
        0
    )
    
    prices = sales_temp.groupby('product_id')['unit_price_calc'].mean().reset_index()
    prices.rename(columns={'unit_price_calc': 'unit_price'}, inplace=True)
    
    # Unit Cost from Inventory
    costs = inv_df[['product_id', 'unit_cost']].drop_duplicates()
    
    # 2. Merge (Business Key = product_id)
    dim_product = pd.merge(prices, costs, on='product_id', how='inner')
    
    # 3. Validation: Unique Primary Key
    if dim_product['product_id'].duplicated().any():
        logger.error("Duplicate product_ids found in source data.")
        raise ValueError("Dim_Product Primary Key Constraint Violated.")
        
    if dim_product.isnull().any().any():
        logger.warning("NULL values detected in Dim_Product. Filling with 0.")
        dim_product = dim_product.fillna(0.0)
        
    return dim_product

# ==============================================================================
# FACT TABLE BUILDER
# ==============================================================================
def build_fact_sales(sales_df, dim_date, dim_product, logger):
    """
    Builds Fact_Sales.
    Grain: One row per Product per Date per Region.
    """
    logger.info("Building Fact_Sales.")
    
    # 1. Aggregate Sales to Grain
    # Updated to use 'daily_quantity_sold' and 'daily_revenue'
    fact = sales_df.groupby(['product_id', 'date']).agg({
        'daily_quantity_sold': 'sum',
        'daily_revenue': 'sum'
    }).reset_index()
    
    # Rename to match Schema requirements (quantity, revenue)
    fact.rename(columns={
        'daily_quantity_sold': 'quantity',
        'daily_revenue': 'revenue'
    }, inplace=True)
    
    # 2. Add Region (Constant FK)
    fact['region_id'] = 1
    
    # 3. Lookup Date FK (date_id)
    # Prepare merge key
    fact['date_str'] = fact['date'].dt.strftime('%Y-%m-%d')
    
    fact_merged = pd.merge(
        fact,
        dim_date[['full_date', 'date_id']],
        left_on='date_str',
        right_on='full_date',
        how='left'
    )

    # CHECK 1: Date lookup completeness check
    if fact_merged['date_id'].isnull().any():
        logger.error("Data Integrity Error: Failed to resolve date_id for some sales records.")
        raise ValueError("Date lookup failed for some fact rows.")
    
    # 4. Lookup Product Cost (for Fact Calculation)
    fact_final = pd.merge(
        fact_merged,
        dim_product[['product_id', 'unit_cost']],
        on='product_id',
        how='left'
    )
    
    # CHECK 2: Product cost lookup completeness check
    if fact_final['unit_cost'].isnull().any():
        logger.error("Data Integrity Error: Missing unit_cost for some products.")
        raise ValueError("Missing unit_cost for some products in fact table.")
    
    # 5. Compute Metrics
    fact_final['cost'] = fact_final['quantity'] * fact_final['unit_cost']
    
    # 6. Generate Fact Surrogate Key (Python Sequence)
    # NOTE: sales_id is a technical row identifier, NOT part of the analytical grain.
    fact_final['sales_id'] = range(1, len(fact_final) + 1)
    
    # 7. Final Selection
    output_cols = ['sales_id', 'product_id', 'date_id', 'region_id', 'quantity', 'revenue', 'cost']
    
    # Filter valid columns
    df_output = fact_final[output_cols].copy()
    
    return df_output

# ==============================================================================
# VALIDATION & QA
# ==============================================================================
def validate_schema(df_fact, df_product, df_date, df_region, logger):
    """
    Comprehensive QA:
    1. Referential Integrity (FKs exist in Dims)
    2. Data Hygiene (No negatives, no nulls)
    3. Grain Protection (No duplicates on analytical keys)
    """
    logger.info("Performing Schema Validation.")
    errors = []
    
    # 1. Grain Protection: Check for Duplicate Analytical Keys
    # The grain is (product_id, date_id, region_id)
    duplicate_grain = df_fact.duplicated(subset=['product_id', 'date_id', 'region_id'])
    if duplicate_grain.any():
        logger.error(f"GRAIN VIOLATION: Found {duplicate_grain.sum()} duplicate rows for (product, date, region).")
        errors.append("Fact Table Grain Violation: Duplicates detected.")
    
    # 2. Referential Integrity Checks
    missing_products = ~df_fact['product_id'].isin(df_product['product_id'])
    if missing_products.any():
        errors.append(f"RI Violation: {missing_products.sum()} rows in Fact have invalid product_id.")
        
    missing_dates = ~df_fact['date_id'].isin(df_date['date_id'])
    if missing_dates.any():
        errors.append(f"RI Violation: {missing_dates.sum()} rows in Fact have invalid date_id.")
        
    missing_regions = ~df_fact['region_id'].isin(df_region['region_id'])
    if missing_regions.any():
        errors.append(f"RI Violation: {missing_regions.sum()} rows in Fact have invalid region_id.")

    # 3. Data Hygiene
    if (df_fact['quantity'] < 0).any():
        errors.append("Negative Quantity found.")
    if (df_fact['revenue'] < 0).any():
        errors.append("Negative Revenue found.")
    if (df_fact['cost'] < 0).any():
        errors.append("Negative Cost found.")
    if df_fact.isnull().any().any():
        errors.append("NULL values found in Fact Table.")

    if errors:
        for err in errors:
            logger.error(f"Validation Failure: {err}")
        raise ValueError("Schema Validation Failed. Check logs.")
    else:
        logger.info("Validation Passed: Referential Integrity, Grain Uniqueness, and Data Hygiene verified.")

# ==============================================================================
# DATABASE LOADER
# ==============================================================================
def load_to_sqlite(data_dict, logger):
    """Executes DDL and loads DataFrames to SQLite."""
    logger.info(f"Connecting to database: {DB_PATH}")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # 1. Execute DDL
        with open(SQL_SCRIPT_PATH, 'r') as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        logger.info("DDL executed successfully.")
        
        # 2. Load DataFrames (Using 'append' as tables are freshly created by DDL)
        # Note: index=False because we generated explicit PKs in the DataFrames
        data_dict['dim_region'].to_sql('dim_region', conn, if_exists='append', index=False)
        data_dict['dim_date'].to_sql('dim_date', conn, if_exists='append', index=False)
        data_dict['dim_product'].to_sql('dim_product', conn, if_exists='append', index=False)
        data_dict['fact_sales'].to_sql('fact_sales', conn, if_exists='append', index=False)
        
        logger.info("All tables populated successfully.")
        conn.commit()
        
    except sqlite3.Error as e:
        logger.error(f"Database Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

# ==============================================================================
# ERD GENERATION (PIL Implementation)
# ==============================================================================
def generate_erd(logger):
    """
    Generates ERD using PIL (Pillow) to remove Graphviz dependency.
    Creates a PNG showing the Star Schema relationship.
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
        logger.info("Generating ERD Diagram using PIL...")

        # Configuration
        IMG_WIDTH, IMG_HEIGHT = 1200, 800
        BG_COLOR = 'white'
        FACT_COLOR = '#ADD8E6'  # Light Blue
        DIM_COLOR = '#E0E0E0'   # Light Gray
        OUTLINE_COLOR = 'black'
        
        # Create Canvas
        img = Image.new('RGB', (IMG_WIDTH, IMG_HEIGHT), color=BG_COLOR)
        draw = ImageDraw.Draw(img)
        
        # Font Selection (Try system fonts, fallback to default)
        try:
            # Try to load Arial (Windows) or DejaVu (Linux)
            try:
                font = ImageFont.truetype("arial.ttf", 20)
            except IOError:
                font = ImageFont.truetype("DejaVuSans.ttf", 20)
        except IOError:
            # Fallback to PIL default (may be small, but works)
            font = ImageFont.load_default()
            logger.warning("System fonts not found. Using PIL default font.")

        # Coordinates (Center Point of the image)
        cx, cy = IMG_WIDTH // 2, IMG_HEIGHT // 2
        
        # Node Definitions
        # Logic: Fact in Center, Dims surrounding it (Left, Top, Right)
        nodes = {
            'FACT': {
                'label': 'fact_sales\n(PK: sales_id)\n(UQ: prod, date, reg)',
                'color': FACT_COLOR,
                'pos': (cx, cy),
                'size': (320, 120)
            },
            'PROD': {
                'label': 'dim_product\n(PK: product_id)',
                'color': DIM_COLOR,
                'pos': (cx - 400, cy), # Left
                'size': (250, 100)
            },
            'DATE': {
                'label': 'dim_date\n(PK: date_id)',
                'color': DIM_COLOR,
                'pos': (cx, cy - 250), # Top
                'size': (250, 100)
            },
            'REG': {
                'label': 'dim_region\n(PK: region_id)',
                'color': DIM_COLOR,
                'pos': (cx + 400, cy), # Right
                'size': (250, 100)
            }
        }

        # 1. Draw Edges (Lines) first (so they are behind the boxes)
        connections = [('PROD', 'FACT'), ('DATE', 'FACT'), ('REG', 'FACT')]
        
        for start_key, end_key in connections:
            start_node = nodes[start_key]
            end_node = nodes[end_key]
            
            # Draw Line
            draw.line([start_node['pos'], end_node['pos']], fill='black', width=3)
            
            # Add cardinality label "1..n" near the Fact table
            # Calculate 65% distance towards Fact
            sx, sy = start_node['pos']
            ex, ey = end_node['pos']
            lbl_x = sx + (ex - sx) * 0.65
            lbl_y = sy + (ey - sy) * 0.65
            
            # Background for label to make it readable
            draw.rectangle([lbl_x-20, lbl_y-12, lbl_x+20, lbl_y+12], fill='white')
            draw.text((lbl_x, lbl_y), "1..n", fill='black', font=font, anchor="mm")

        # 2. Draw Nodes (Boxes and Text)
        for key, node in nodes.items():
            cx_node, cy_node = node['pos']
            w, h = node['size']
            
            # Calculate Bounding Box
            x0 = cx_node - w // 2
            y0 = cy_node - h // 2
            x1 = cx_node + w // 2
            y1 = cy_node + h // 2
            
            # Draw Box
            draw.rectangle([x0, y0, x1, y1], fill=node['color'], outline=OUTLINE_COLOR, width=2)
            
            # Draw Text Centered
            draw.multiline_text(
                (cx_node, cy_node), 
                node['label'], 
                fill='black', 
                font=font, 
                anchor="mm", 
                align="center"
            )

        # 3. Save Image
        # Ensure output directory exists
        erd_dir = os.path.dirname(ERD_OUTPUT_PATH)
        if not os.path.exists(erd_dir):
            os.makedirs(erd_dir)

        # Ensure filename ends with .png
        final_path = ERD_OUTPUT_PATH
        if not final_path.lower().endswith('.png'):
            final_path += '.png'
            
        img.save(final_path)
        logger.info(f"ERD saved successfully to: {final_path}")

    except ImportError:
        logger.warning("PIL (Pillow) library not found. Install via 'pip install Pillow'. Skipping ERD.")
    except Exception as e:
        logger.error(f"Failed to generate ERD: {e}")

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================
def main(data=None, correlation_id=None):
    """
    Main orchestrator for Star Schema Build.
    Accepts optional 'data' dict from pipeline and optional 'correlation_id'.
    """
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
    logger = setup_logging(correlation_id)

    logger.info("Starting Week 10: Star Schema Build Pipeline (Refined)")

    try:
        # 1. Load Source (only if data not passed)
        if data is None:
            raw_data = load_data(logger)
        else:
            logger.info("Using data passed from upstream pipeline.")
            raw_data = data
        
        # 2. Build Dimensions (with explicit SKs)
        dim_region = build_dim_region(logger)
        dim_date = build_dim_date(raw_data['sales'], logger)
        dim_product = build_dim_product(raw_data['sales'], raw_data['inventory'], logger)
        
        # 3. Build Fact
        fact_sales = build_fact_sales(
            raw_data['sales'], 
            dim_date, 
            dim_product, 
            logger
        )
        
        # 4. Strict Validation (Including Grain Check)
        validate_schema(fact_sales, dim_product, dim_date, dim_region, logger)
        
        # 5. Pack Data
        schema_data = {
            'dim_region': dim_region,
            'dim_date': dim_date,
            'dim_product': dim_product,
            'fact_sales': fact_sales
        }
        
        # 6. Load to DB
        load_to_sqlite(schema_data, logger)
        
        # 7. Documentation
        generate_erd(logger)
        
        logger.info("Pipeline completed successfully.")
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()