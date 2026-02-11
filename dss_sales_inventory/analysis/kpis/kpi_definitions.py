import os
import logging
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logger = logging.getLogger("dss.kpi_layer")

# ---------------------------------------------------------------------
# Constants & Thresholds
# ---------------------------------------------------------------------
RISK_THRESHOLD = 70.0
PRESSURE_THRESHOLD = 75.0
PROFIT_THRESHOLD = 40.0

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

INVENTORY_STATUS_VIEW = os.path.join(
    PROJECT_ROOT, "reporting", "outputs", "inventory_status_view.csv"
)

INVENTORY_FEATURES = os.path.join(
    PROJECT_ROOT, "data", "processed", "inventory_features.csv"
)

FORECAST_RESULTS = os.path.join(
    PROJECT_ROOT, "analysis", "forecast", "forecast_results.csv"
)

RISK_RESULTS = os.path.join(
    PROJECT_ROOT, "analysis", "risk", "product_risk_scores.csv"
)

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "analysis", "kpis")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "product_kpis.csv")
DOC_REPORT_FILE = os.path.join(OUTPUT_DIR, "kpi_documentation.md")


# ---------------------------------------------------------------------
# Helpers: Normalization & Calculation
# ---------------------------------------------------------------------
def _normalize_0_100(series: pd.Series) -> pd.Series:
    """Normalizes a numeric series to a 0-100 scale."""
    series = pd.to_numeric(series, errors="coerce")
    min_v = series.min()
    max_v = series.max()

    if pd.isna(min_v) or pd.isna(max_v) or max_v == min_v:
        return pd.Series(np.zeros(len(series)), index=series.index)

    return ((series - min_v) / (max_v - min_v)) * 100.0


def _resolve_column(df: pd.DataFrame, candidates: list) -> str:
    """Returns the first column name from candidates that exists in df."""
    for col in candidates:
        if col in df.columns:
            return col
    return None


# ---------------------------------------------------------------------
# Modular KPI Logic
# ---------------------------------------------------------------------
def compute_inventory_risk_score(df: pd.DataFrame) -> pd.Series:
    """
    Calculates Inventory Risk Score.
    Formula: 0.6 * Risk_Score + 0.4 * Operational_Risk
    Where Operational_Risk = 1 / (1 + stock_ratio)
    """
    # Operational risk grows when stock_ratio is low (low coverage)
    # Ensure stock_ratio is numeric and fill NaNs
    stock_ratio = pd.to_numeric(df["stock_ratio"], errors="coerce").fillna(0.0)
    risk_score = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)

    operational_risk = 1.0 / (1.0 + stock_ratio)
    
    raw_score = (0.6 * risk_score) + (0.4 * operational_risk)
    return _normalize_0_100(raw_score)


def compute_demand_pressure_index(df: pd.DataFrame, forecast_col: str) -> pd.Series:
    """
    Calculates Demand Pressure Index.
    Formula: Forecast_Quantity / Stock
    """
    forecast = pd.to_numeric(df[forecast_col], errors="coerce").fillna(0.0)
    stock = pd.to_numeric(df["stock"], errors="coerce") # NaNs should remain NaNs here
    
    # Division (inf/nan handled after)
    pressure_raw = forecast / stock
    
    # Replace Infinity (div by 0) with NaN, then fill 0
    pressure_raw = pressure_raw.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    
    return _normalize_0_100(pressure_raw)


def compute_profitability_margin(df: pd.DataFrame, profit_col: str) -> pd.Series:
    """
    Calculates Profitability Margin (Normalized Profit).
    """
    profit = pd.to_numeric(df[profit_col], errors="coerce").fillna(0.0)
    return _normalize_0_100(profit)


# ---------------------------------------------------------------------
# Documentation Report Generator
# ---------------------------------------------------------------------
def _generate_documentation(df: pd.DataFrame, source_stats: dict, col_map: dict):
    """
    Generates a Markdown report summarizing the KPI run.
    """
    kpis = ["inventory_risk_score", "demand_pressure_index", "profitability_margin"]
    
    with open(DOC_REPORT_FILE, "w") as f:
        f.write("# KPI Layer Documentation Report\n\n")
        
        # 1. Overview
        f.write("## 1. Execution Overview\n")
        f.write(f"- **Total Products Processed:** {len(df)}\n")
        f.write("- **Status:** Success\n\n")
        
        # 2. Data Source Stats
        f.write("## 2. Data Sources & Deduplication\n")
        f.write("| Dataset | Raw Rows | Unique Products | Status |\n")
        f.write("|---|---|---|---|\n")
        for name, stats in source_stats.items():
            f.write(f"| {name} | {stats['raw']} | {stats['final']} | {stats['status']} |\n")
        f.write("\n")

        # 3. Column Mapping Used
        f.write("## 3. Column Resolutions\n")
        f.write(f"- **Stock Source:** `{col_map['stock']}`\n")
        f.write(f"- **Forecast Source:** `{col_map['forecast']}`\n")
        f.write(f"- **Profit Source:** `{col_map['profit']}`\n\n")

        # 4. KPI Statistics
        f.write("## 4. KPI Statistics\n")
        if not df.empty:
            stats = df[kpis].describe().T[["min", "mean", "50%", "max"]]
            stats.columns = ["Min", "Mean", "Median", "Max"]
            f.write(stats.to_markdown())
        else:
            f.write("No data available.")
        f.write("\n\n")

        # 5. Executive Decisions
        f.write("## 5. Decision Flag Distribution\n")
        if "decision_flag" in df.columns:
            counts = df["decision_flag"].value_counts()
            for label, count in counts.items():
                f.write(f"- **{label}:** {count}\n")
        f.write("\n")

        # 6. Top Risky Products
        f.write("## 6. Top 10 Products by Risk & Pressure\n")
        if not df.empty:
            top_10 = df.nlargest(10, ["inventory_risk_score", "demand_pressure_index"])
            cols_show = ["product_id", "inventory_risk_score", "demand_pressure_index", "decision_flag"]
            f.write(top_10[cols_show].to_markdown(index=False))


# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------
def run_kpi_layer(data: dict, correlation_id: str = None) -> dict:
    """
    Main entry point for KPI calculation.
    """
    run_meta = {
        "run_id": correlation_id,
        "stage": "KPIS",
        "function": "run_kpi_layer"
    }

    logger.info("KPI layer started", extra={**run_meta, "status": "STARTED"})

    # ---------------------------------------------------------
    # 1. Validation: Check Files
    # ---------------------------------------------------------
    required_files = [INVENTORY_STATUS_VIEW, INVENTORY_FEATURES, FORECAST_RESULTS, RISK_RESULTS]
    for f in required_files:
        if not os.path.exists(f):
            raise FileNotFoundError(f"[KPI layer] Missing file: {f}")

    # ---------------------------------------------------------
    # 2. Load & Normalize Columns
    # ---------------------------------------------------------
    inv_view = pd.read_csv(INVENTORY_STATUS_VIEW)
    inv_feat = pd.read_csv(INVENTORY_FEATURES)
    forecast = pd.read_csv(FORECAST_RESULTS)
    risk = pd.read_csv(RISK_RESULTS)

    for d in [inv_view, inv_feat, forecast, risk]:
        d.columns = [c.lower().strip() for c in d.columns]

    source_stats = {}

    # ---------------------------------------------------------
    # 3. Data Cleaning & Deduplication
    # ---------------------------------------------------------
    
    # A. Inventory View
    raw_inv = len(inv_view)
    if "date" in inv_view.columns:
        inv_view["date"] = pd.to_datetime(inv_view["date"], errors="coerce")
        # Sort by date and keep last per product
        inv_view = inv_view.sort_values("date").groupby("product_id", as_index=False).tail(1)
    
    # Strict deduplication on product_id
    inv_view = inv_view.drop_duplicates(subset=["product_id"])
    source_stats["Inventory View"] = {"raw": raw_inv, "final": len(inv_view), "status": "Deduped"}

    # B. Features
    raw_feat = len(inv_feat)
    inv_feat = inv_feat.drop_duplicates(subset=["product_id"])
    source_stats["Inventory Features"] = {"raw": raw_feat, "final": len(inv_feat), "status": "Deduped"}

    # C. Forecast (Aggregation)
    raw_fore = len(forecast)
    # Determine forecast col for aggregation
    fc_col_candidates = ["forecast_quantity", "predicted_demand", "mean_forecast"]
    fc_col = _resolve_column(forecast, fc_col_candidates)
    
    if fc_col:
        forecast_agg = forecast.groupby("product_id", as_index=False)[fc_col].sum()
    else:
        logger.warning("No forecast column found. Creating empty forecast data.")
        forecast_agg = pd.DataFrame(columns=["product_id", "forecast_quantity"])
        fc_col = "forecast_quantity"

    forecast_agg = forecast_agg.drop_duplicates(subset=["product_id"])
    source_stats["Forecast"] = {"raw": raw_fore, "final": len(forecast_agg), "status": "Aggregated"}

    # D. Risk
    raw_risk = len(risk)
    risk = risk.drop_duplicates(subset=["product_id"])
    source_stats["Risk Results"] = {"raw": raw_risk, "final": len(risk), "status": "Deduped"}

    # ---------------------------------------------------------
    # 4. Column Resolution (Fallbacks)
    # ---------------------------------------------------------
    
    # Stock
    stock_candidates = ["stock_on_hand", "on_hand_quantity", "quantity", "current_stock", "stock"]
    stock_col = _resolve_column(inv_view, stock_candidates)
    if not stock_col:
        raise KeyError(f"Could not resolve stock column. Checked: {stock_candidates}")

    # Profit
    # Check if profit exists directly in Risk or Needs computation
    profit_candidates = ["profit", "expected_profit", "expected_profit_mean", "profit_mean"]
    profit_col = _resolve_column(risk, profit_candidates)
    
    # If not found, try to compute
    profit_source = "Direct"
    if not profit_col:
        # Check if we can compute from inventory view or features (wherever price/cost exist)
        # Note: Usually price/cost are in features or view. Let's check view first then merge.
        # We will handle this logic after merge to have all columns available.
        profit_source = "Computed"
        profit_col = "calculated_profit"

    col_map_report = {"stock": stock_col, "forecast": fc_col, "profit": profit_col if profit_source == "Direct" else "Computed (Price - Cost)"}

    # ---------------------------------------------------------
    # 5. Merging
    # ---------------------------------------------------------
    try:
        # Base: Inventory View (Product + Stock)
        df = inv_view[["product_id", stock_col]].rename(columns={stock_col: "stock"})
        
        # Merge Features
        df = df.merge(inv_feat, on="product_id", how="left", validate="one_to_one")
        
        # Merge Forecast
        df = df.merge(forecast_agg, on="product_id", how="left", validate="one_to_one")
        
        # Merge Risk
        df = df.merge(risk, on="product_id", how="left", validate="one_to_one")
        
    except Exception as e:
        logger.error(f"Merge failed: {str(e)}")
        raise e

    # ---------------------------------------------------------
    # 6. Post-Merge Calculation (Profit Fallback)
    # ---------------------------------------------------------
    if profit_source == "Computed":
        # Look for price/cost columns
        price_col = _resolve_column(df, ["unit_price", "price", "selling_price"])
        cost_col = _resolve_column(df, ["unit_cost", "cost", "buying_price"])
        
        if price_col and cost_col:
            df[profit_col] = pd.to_numeric(df[price_col], errors='coerce') - pd.to_numeric(df[cost_col], errors='coerce')
        else:
            logger.warning("Cannot compute profit: missing price/cost columns. Defaulting to 0.")
            df[profit_col] = 0.0

    # ---------------------------------------------------------
    # 7. Safe Numeric Conversion & Handling
    # ---------------------------------------------------------
    # Convert core columns to numeric
    numeric_cols = ["stock", "stock_ratio", "risk_score", fc_col, profit_col]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Handle Stock: 0 -> NaN to prevent div/0 issues later
    df["stock"] = df["stock"].replace(0, np.nan)
    
    # Fill NaNs for additive/multiplicative metrics (not stock)
    df["stock_ratio"] = df["stock_ratio"].fillna(0.0)
    df["risk_score"] = df["risk_score"].fillna(0.0)
    df[fc_col] = df[fc_col].fillna(0.0)
    df[profit_col] = df[profit_col].fillna(0.0)

    # ---------------------------------------------------------
    # 8. Compute KPIs
    # ---------------------------------------------------------
    df["inventory_risk_score"] = compute_inventory_risk_score(df)
    df["demand_pressure_index"] = compute_demand_pressure_index(df, fc_col)
    df["profitability_margin"] = compute_profitability_margin(df, profit_col)

    # ---------------------------------------------------------
    # 9. Decision Logic
    # ---------------------------------------------------------
    # "Requires Intervention" if (Risk >= 70 OR Pressure >= 75) AND Profit >= 40
    is_risky_or_pressured = (
        (df["inventory_risk_score"] >= RISK_THRESHOLD) | 
        (df["demand_pressure_index"] >= PRESSURE_THRESHOLD)
    )
    is_profitable = (df["profitability_margin"] >= PROFIT_THRESHOLD)
    
    df["decision_flag"] = np.where(
        is_risky_or_pressured & is_profitable,
        "Requires Intervention",
        "Safe"
    )

    # ---------------------------------------------------------
    # 10. Finalizing Output
    # ---------------------------------------------------------
    kpi_df = df[[
        "product_id",
        "inventory_risk_score",
        "demand_pressure_index",
        "profitability_margin",
        "decision_flag"
    ]].copy()

    # Sort descending by risk and pressure
    kpi_df = kpi_df.sort_values(
        by=["inventory_risk_score", "demand_pressure_index"],
        ascending=[False, False]
    )

    # Save to CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    kpi_df.to_csv(OUTPUT_FILE, index=False)

    # Generate Documentation
    _generate_documentation(kpi_df, source_stats, col_map_report)

    # Attach to pipeline data
    data["kpis"] = kpi_df

    logger.info(
        "KPI layer completed successfully",
        extra={
            **run_meta,
            "rows_out": len(kpi_df),
            "output": OUTPUT_FILE,
            "status": "SUCCESS"
        }
    )

    return data