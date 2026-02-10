import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
import os
import sys
from math import pi
from pathlib import Path

# ==========================================
# 1. CONFIGURATION & CONSTANTS
# ==========================================
SENSITIVITY_CONFIG = {
    "change_ratios": [-0.2, 0.2],  # +/- 20%
    
    # KPIs to measure impact against (Dependent Variables)
    "target_metrics": [
        "expected_profit", 
        "remaining_stock", 
        "total_units_sold", 
        "total_revenue"
    ],
    
    # Priority drivers to always include if found (Independent Variables)
    "key_drivers": [
        "total_sales", "price", "unit_cost", "current_stock", 
        "risk_score", "avg_daily_revenue", "profit_std", "lead_time_days"
    ],
    
    # Columns to exclude from perturbation
    "ignore_columns": [
        "product_id", "scenario_name", "run_id", "date", "simulation_id",
        "is_stockout", "demand_std", "abc_class"
    ],
    
    # File paths (Dynamic & Extensible)
    "paths": {
        "base_dir": r"C:\Data_Analysis\dss_sales_inventory\analysis\sensitivity",
        "output_dir": r"C:\Data_Analysis\dss_sales_inventory\analysis\sensitivity\outputs",
        "report_file": "sensitivity_findings.md",
        
        # Source files for automatic enrichment
        "sources": {
            "scenarios": r"C:\Data_Analysis\dss_sales_inventory\analysis\scenarios\scenarios_comparison.xlsx",
            "risk": r"C:\Data_Analysis\dss_sales_inventory\analysis\risk\product_risk_scores.csv",
            "forecast": r"C:\Data_Analysis\dss_sales_inventory\analysis\forecast\short_term_forecast.csv",
            "inventory": r"C:\Data_Analysis\dss_sales_inventory\analysis\inventory\inventory_status_view.csv",
            "demand": r"C:\Data_Analysis\dss_sales_inventory\analysis\demand\demand_pressure.csv",
            "performance": r"C:\Data_Analysis\dss_sales_inventory\analysis\performance\product_performance.csv"
        }
    }
}

# ==========================================
# 2. LOGGING SETUP
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(correlation_id)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
base_logger = logging.getLogger("sensitivity_layer")

def setup_logger(correlation_id):
    """Returns a logger adapter with correlation ID context."""
    return logging.LoggerAdapter(base_logger, {'correlation_id': correlation_id})

# ==========================================
# 3. DATA INTEGRATION & PREPARATION
# ==========================================

def load_and_enrich_data(data_dict: dict, logger) -> pd.DataFrame:
    """
    Automatically detects and merges all available pipeline source files.
    Aligns data on 'product_id'.
    """
    # 1. Initialize Base Data (Scenarios)
    if "scenarios" in data_dict and isinstance(data_dict["scenarios"], pd.DataFrame):
        df = data_dict["scenarios"].copy()
        logger.info(f"Loaded {len(df)} base rows from pipeline memory.")
    elif os.path.exists(SENSITIVITY_CONFIG["paths"]["sources"]["scenarios"]):
        df = pd.read_excel(SENSITIVITY_CONFIG["paths"]["sources"]["scenarios"])
        logger.info(f"Loaded {len(df)} base rows from disk.")
    else:
        logger.error("Critical: Base scenario data not found.")
        return pd.DataFrame()

    # 2. Iterate and Merge Additional Sources
    for source_name, path in SENSITIVITY_CONFIG["paths"]["sources"].items():
        if source_name == "scenarios": continue # Skip base
        
        # Check pipeline memory first, then disk
        source_df = data_dict.get(f"{source_name}_data")
        
        if source_df is None and os.path.exists(path):
            try:
                if path.endswith('.csv'):
                    source_df = pd.read_csv(path)
                elif path.endswith('.xlsx'):
                    source_df = pd.read_excel(path)
            except Exception as e:
                logger.warning(f"Failed to load {source_name} from disk: {e}")

        if source_df is not None and "product_id" in source_df.columns:
            # Select numeric columns only + Key
            cols_to_use = source_df.select_dtypes(include=[np.number]).columns.tolist()
            if "product_id" not in cols_to_use: cols_to_use.append("product_id")
            
            # Remove duplicate columns that already exist in base to avoid _x _y clutter
            cols_to_use = [c for c in cols_to_use if c not in df.columns or c == "product_id"]
            
            if len(cols_to_use) > 1:
                df = df.merge(source_df[cols_to_use], on="product_id", how="left")
                logger.info(f"Enriched data with {len(cols_to_use)-1} vars from {source_name}.")

    return df

def clean_and_prepare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans types, fills NaNs, and computes implied metrics for fallback logic.
    """
    # Convert object columns to numeric where possible
    for col in df.columns:
        if col not in SENSITIVITY_CONFIG["ignore_columns"] and df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='ignore')

    # Fill NaNs in numeric columns with 0
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    # Calculate Implied PPU (Profit Per Unit) for fallback calculations
    # Used when Price/Cost columns are missing or zero
    if "total_sales" in df.columns and "expected_profit" in df.columns:
        df["_implied_ppu"] = np.where(
            df["total_sales"] > 0, 
            df["expected_profit"] / df["total_sales"], 
            0
        )
    else:
        df["_implied_ppu"] = 0.0

    # Ensure all target columns exist for calculation storage
    for target in SENSITIVITY_CONFIG["target_metrics"]:
        if target not in df.columns:
            df[target] = 0.0
            
    return df

# ==========================================
# 4. VECTORIZED SENSITIVITY CORE
# ==========================================

def vectorized_recalculation(df: pd.DataFrame) -> dict:
    """
    Re-computes ALL target KPIs based on current state of input variables.
    Returns a dictionary of summed metrics for the entire portfolio.
    """
    # 1. Derived Sales (in case 'total_sales' variable was perturbed)
    sales = df["total_sales"] if "total_sales" in df.columns else np.zeros(len(df))
    
    # 2. Derived Profit
    if "price" in df.columns and "unit_cost" in df.columns:
        profit = (df["price"] - df["unit_cost"]) * sales
        revenue = df["price"] * sales
    else:
        # Fallback
        profit = sales * df["_implied_ppu"]
        revenue = sales * df["_implied_ppu"] # Approximation if price missing

    # 3. Derived Stock
    if "current_stock" in df.columns:
        remaining = np.maximum(0, df["current_stock"] - sales)
    else:
        remaining = df["remaining_stock"]

    # Return Aggregates
    return {
        "expected_profit": profit.sum(),
        "remaining_stock": remaining.sum(),
        "total_units_sold": sales.sum(),
        "total_revenue": revenue.sum()
    }

def get_candidate_variables(df: pd.DataFrame) -> list:
    """Identifies suitable numeric variables for perturbation."""
    candidates = []
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        if col in SENSITIVITY_CONFIG["ignore_columns"]: continue
        if col in SENSITIVITY_CONFIG["target_metrics"]: continue # Don't perturb outputs
        if col.startswith("_"): continue # Internal helpers
        
        # Include if Variance > 0 OR explicitly listed as driver
        if col in SENSITIVITY_CONFIG["key_drivers"] or df[col].nunique() > 1:
            candidates.append(col)
            
    return sorted(list(set(candidates)))

def run_oat_analysis(df: pd.DataFrame, variables: list, logger) -> pd.DataFrame:
    """
    Executes One-At-A-Time (OAT) perturbation.
    Uses in-place modification and reversion for memory efficiency.
    """
    # 1. Baseline Calculation
    baseline = vectorized_recalculation(df)
    logger.info(f"Baseline Profit: ${baseline['expected_profit']:,.2f}")
    
    results = []
    
    for var in variables:
        for ratio in SENSITIVITY_CONFIG["change_ratios"]:
            try:
                # 2. Store Original & Perturb
                original_values = df[var].to_numpy()
                df[var] = original_values * (1 + ratio)
                
                # 3. Recalculate
                new_metrics = vectorized_recalculation(df)
                
                # 4. Calculate Deltas
                row = {
                    "variable": var,
                    "change_ratio": ratio,
                    "impact_magnitude": 0.0 # Will be max of normalized impacts
                }
                
                # Compute delta for each KPI
                for kpi, base_val in baseline.items():
                    delta = new_metrics[kpi] - base_val
                    row[f"delta_{kpi}"] = delta
                    row[f"raw_{kpi}"] = new_metrics[kpi]
                
                # Compute composite impact magnitude (Weighted mostly to Profit)
                profit_impact = abs(row["delta_expected_profit"])
                stock_impact = abs(row["delta_remaining_stock"]) * 10 # heuristic weight
                row["impact_magnitude"] = profit_impact + stock_impact
                
                results.append(row)
                
                # 5. Revert
                df[var] = original_values
                
            except Exception as e:
                logger.warning(f"Error perturbing {var}: {e}")
                if 'original_values' in locals():
                    df[var] = original_values

    results_df = pd.DataFrame(results)
    
    # 6. Normalize Sensitivity Score (0-1)
    if not results_df.empty:
        max_imp = results_df["impact_magnitude"].max()
        if max_imp == 0: max_imp = 1
        results_df["sensitivity_score"] = results_df["impact_magnitude"] / max_imp
        
    return results_df

# ==========================================
# 5. VISUALIZATION
# ==========================================

def generate_tornado_chart(results_df: pd.DataFrame, output_dir: str):
    """Top 15 Tornado Chart for Expected Profit."""
    try:
        pivot = results_df.pivot(index="variable", columns="change_ratio", values="delta_expected_profit")
        pivot["range"] = pivot.max(axis=1) - pivot.min(axis=1)
        pivot = pivot.sort_values("range", ascending=True).iloc[-15:] # Top 15
        
        plt.figure(figsize=(12, 8))
        y_pos = np.arange(len(pivot))
        
        ratios = sorted(SENSITIVITY_CONFIG["change_ratios"])
        low_vals = pivot[ratios[0]]
        high_vals = pivot[ratios[1]]
        
        plt.barh(y_pos, high_vals, color='#2ca02c', alpha=0.8, label=f"+{int(ratios[1]*100)}%")
        plt.barh(y_pos, low_vals, color='#d62728', alpha=0.8, label=f"{int(ratios[0]*100)}%")
        
        plt.yticks(y_pos, pivot.index, fontsize=10)
        plt.xlabel("Impact on Expected Profit ($)", fontsize=10)
        plt.title("Sensitivity Tornado Chart (Top 15 Drivers)", fontsize=14)
        plt.axvline(0, color='black', linewidth=0.8)
        plt.legend()
        plt.grid(axis='x', linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "sensitivity_tornado.png"), dpi=150)
        plt.close()
    except Exception:
        pass

def generate_heatmap(results_df: pd.DataFrame, output_dir: str):
    """Heatmap showing Variable Perturbation vs KPI Impact."""
    try:
        # Filter for top variables
        top_vars = results_df.groupby("variable")["sensitivity_score"].max().nlargest(10).index
        df_sub = results_df[results_df["variable"].isin(top_vars) & (results_df["change_ratio"] > 0)].copy()
        
        # Normalize deltas for heatmap (Z-score ish)
        metrics = [f"delta_{k}" for k in SENSITIVITY_CONFIG["target_metrics"]]
        heatmap_data = df_sub.set_index("variable")[metrics]
        
        # Simple normalization for visual clarity
        heatmap_data = (heatmap_data - heatmap_data.mean()) / (heatmap_data.std() + 1e-9)

        plt.figure(figsize=(10, 8))
        plt.imshow(heatmap_data.T, cmap='coolwarm', aspect='auto')
        
        plt.xticks(range(len(heatmap_data.index)), heatmap_data.index, rotation=45, ha='right')
        plt.yticks(range(len(metrics)), [m.replace("delta_", "").replace("_", " ").title() for m in metrics])
        
        plt.colorbar(label="Standardized Impact")
        plt.title("Variable Impact Correlation Heatmap (Top 10)")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "sensitivity_heatmap.png"), dpi=150)
        plt.close()
    except Exception:
        pass

def generate_spider_chart(results_df: pd.DataFrame, output_dir: str):
    """Radar Chart for Top 8 Drivers."""
    try:
        scores = results_df.groupby("variable")["sensitivity_score"].max().nlargest(8)
        
        # Prepare Data
        categories = scores.index.tolist()
        values = scores.values.tolist()
        values += values[:1]
        categories += categories[:1]
        
        angles = [n / float(len(categories)-1) * 2 * pi for n in range(len(categories))]
        
        plt.figure(figsize=(8, 8))
        ax = plt.subplot(111, polar=True)
        plt.xticks(angles[:-1], categories[:-1], size=10)
        ax.plot(angles, values, linewidth=2, linestyle='solid', color='#1f77b4')
        ax.fill(angles, values, '#1f77b4', alpha=0.25)
        
        plt.title("Relative Sensitivity Score (Top 8)", size=14, y=1.05)
        plt.savefig(os.path.join(output_dir, "sensitivity_spider.png"), dpi=150)
        plt.close()
    except Exception:
        pass

# ==========================================
# 6. REPORTING
# ==========================================

def generate_report(results_df: pd.DataFrame, baseline: dict, output_path: str, sources_used: list):
    """Generates a professional Markdown report."""
    
    summary = results_df.groupby("variable").agg({
        "delta_expected_profit": lambda x: x.abs().max(),
        "delta_remaining_stock": lambda x: x.abs().mean(),
        "sensitivity_score": "max"
    }).sort_values("sensitivity_score", ascending=False)
    
    top_driver = summary.index[0] if not summary.empty else "None"
    
    md = f"""# Sensitivity Analysis Report

**Date:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}
**Methodology:** OAT Perturbation (±{int(SENSITIVITY_CONFIG["change_ratios"][1]*100)}%)

## 1. Executive Summary
This analysis evaluated the robustness of the DSS model by testing **{len(summary)}** input variables.

### Baseline KPIs
| Metric | Value |
|:-------|:------|
| **Expected Profit** | ${baseline['expected_profit']:,.2f} |
| **Remaining Stock** | {baseline['remaining_stock']:,.0f} units |
| **Total Revenue** | ${baseline['total_revenue']:,.2f} |

### Key Findings
* **Dominant Driver:** **{top_driver}**
* **Top 3 Risks:** {', '.join(summary.index[:3].tolist())}

## 2. Impact Ranking
| Rank | Variable | Sensitivity Score | Max Profit Impact | Avg Stock Impact |
|:--:|:---|:--:|:--:|:--:|
"""
    for i, (var, row) in enumerate(summary.head(10).iterrows(), 1):
        md += f"| {i} | {var} | {row['sensitivity_score']:.3f} | ${row['delta_expected_profit']:,.0f} | {row['delta_remaining_stock']:.1f} |\n"
        
    md += f"""
## 3. Visualizations
*  **Tornado Chart:** `outputs/sensitivity_tornado.png` (Upside/Downside risk).
* 

[Image of Radar Chart]
 **Spider Chart:** `outputs/sensitivity_spider.png` (Relative magnitude).
*  **Heatmap:** `outputs/sensitivity_heatmap.png` (Variable/KPI correlation).

## 4. Technical Notes
* **Data Sources:** Merged {len(sources_used)} files ({', '.join(sources_used)}).
* **Logic:** Vectorized Numpy calculation. Profit derived via `(Price - Cost) * Sales`.
* **Fallback:** Implied PPU used where unit economics were missing.

---
*Generated by DSS Sensitivity Layer*
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)

# ==========================================
# 7. ORCHESTRATOR
# ==========================================

def run_sensitivity_analysis(data: dict, correlation_id: str) -> dict:
    """Main execution entry point."""
    logger = setup_logger(correlation_id)
    logger.info("Starting Sensitivity Analysis Layer (Professional Edition)")
    
    # 1. Setup
    Path(SENSITIVITY_CONFIG["paths"]["output_dir"]).mkdir(parents=True, exist_ok=True)
    
    try:
        # 2. Load & Enrich
        df = load_and_enrich_data(data, logger)
        if df.empty: return data
        
        # 3. Prepare
        df_clean = clean_and_prepare(df)
        baseline = vectorized_recalculation(df_clean)
        
        # 4. Detect & Analyze
        variables = get_candidate_variables(df_clean)
        logger.info(f"Analyzing {len(variables)} variables.")
        
        if not variables: return data
        
        results_df = run_oat_analysis(df_clean, variables, logger)
        
        # 5. Visualize & Report
        generate_tornado_chart(results_df, SENSITIVITY_CONFIG["paths"]["output_dir"])
        generate_spider_chart(results_df, SENSITIVITY_CONFIG["paths"]["output_dir"])
        generate_heatmap(results_df, SENSITIVITY_CONFIG["paths"]["output_dir"])
        
        sources = [k for k, v in SENSITIVITY_CONFIG["paths"]["sources"].items() if os.path.exists(v) or f"{k}_data" in data]
        generate_report(results_df, baseline, 
                       os.path.join(SENSITIVITY_CONFIG["paths"]["base_dir"], SENSITIVITY_CONFIG["paths"]["report_file"]),
                       sources)
        
        # 6. Finalize
        data["sensitivity_results"] = results_df
        data["sensitivity_ranking"] = results_df.groupby("variable")["sensitivity_score"].max().sort_values(ascending=False)
        
        logger.info("Sensitivity Analysis completed successfully.")
        return data

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        return data

if __name__ == "__main__":
    print("Run via pipeline integration.")