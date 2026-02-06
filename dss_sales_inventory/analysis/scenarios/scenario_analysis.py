import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List

# ------------------------------------------------------------------
# استخدام نفس dss_logger الموحد من المشروع
# ------------------------------------------------------------------
dss_logger = logging.getLogger("dss_logger")

# ------------------------------------------------------------------
# إعداد المسارات النسبية
# ------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

FORECAST_INPUT = os.path.join(PROJECT_ROOT, "reporting", "outputs", "forecast_results.csv")
INVENTORY_INPUT = os.path.join(PROJECT_ROOT, "data", "processed", "inventory_features.csv")

EXCEL_OUTPUT = os.path.join(PROJECT_ROOT, "analysis", "scenarios", "scenarios_comparison.xlsx")
MD_OUTPUT = os.path.join(PROJECT_ROOT, "analysis", "scenarios", "scenario_insights.md")
SCENARIOS_DIR = os.path.join(PROJECT_ROOT, "analysis", "scenarios")

os.makedirs(SCENARIOS_DIR, exist_ok=True)

# ------------------------------------------------------------------
# دالة Logging موحدة
# ------------------------------------------------------------------
def log_message(message: str, level: str = "INFO", correlation_id: str = "", stage: str = "SCENARIO_ANALYSIS", function: str = "run_scenario_analysis"):
    extra = {
        "run_id": correlation_id,
        "stage": stage,
        "function": function,
        "rows_in": None,
        "rows_out": None,
        "status": level
    }
    if level == "INFO":
        dss_logger.info(message, extra=extra)
    elif level == "WARNING":
        dss_logger.warning(message, extra=extra)
    elif level == "ERROR":
        dss_logger.error(message, extra=extra)

# ------------------------------------------------------------------
# الدوال الأساسية
# ------------------------------------------------------------------

def simulate_scenario(product_data: Dict, variables: Dict) -> Dict:
    """
    محاكاة سيناريو واحد لمنتج معين.
    """
    demand_mult = variables.get("demand_mult", 1.0)
    price_mult = variables.get("price_mult", 1.0)
    supply_add = variables.get("supply_add", 0.0)
    
    # تعديل الكميات الأسبوعية حسب الطلب
    forecast_qty = [q * demand_mult for q in product_data["weekly_quantities"]]
    
    # تعديل السعر
    price = product_data["base_price"] * price_mult
    
    # حساب الربح المتوقع
    total_sales = sum(forecast_qty)
    expected_profit = sum(q * (price - product_data["cost_per_unit"]) for q in forecast_qty)
    
    # المخزون المتبقي
    remaining_stock = product_data["current_stock"] + supply_add - total_sales
    
    # حالة المخزون
    if remaining_stock < 0:
        stock_status = "Out of Stock"
        risk_level = "High"
    elif remaining_stock < total_sales * 0.2:
        stock_status = "Low Stock"
        risk_level = "Medium"
    else:
        stock_status = "Safe"
        risk_level = "Low"
    
    return {
        "expected_profit": round(expected_profit, 2),
        "total_sales": round(total_sales, 2),
        "remaining_stock": max(0, round(remaining_stock, 2)),
        "stock_status": stock_status,
        "risk_level": risk_level
    }

def compare_scenarios(scenarios: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(scenarios)
    if not df.empty:
        df = df.sort_values(by=["expected_profit", "risk_level"], ascending=[False, True])
    return df

def generate_insights_md(comparison_df: pd.DataFrame) -> None:
    with open(MD_OUTPUT, "w", encoding="utf-8") as f:
        f.write("# Scenario Analysis Insights Report\n\n")
        f.write(f"**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        
        if comparison_df.empty:
            f.write("**No scenarios generated due to missing data.**\n")
            return
        
        f.write("## Summary of Top Scenarios\n\n")
        for _, row in comparison_df.head(15).iterrows():
            f.write(f"### Product {row['product_id']} - Scenario: {row['scenario_name']}\n")
            f.write(f"- Expected Profit: ${row['expected_profit']:,}\n")
            f.write(f"- Total Sales (4 weeks): {row['total_sales']:,} units\n")
            f.write(f"- Remaining Stock: {row['remaining_stock']:,} units ({row['stock_status']})\n")
            f.write(f"- Risk Level: {row['risk_level']}\n")
            
            recommendation = ""
            if row['risk_level'] == "High":
                recommendation = "🚨 Immediate Restock Required"
            elif row['risk_level'] == "Medium":
                recommendation = "⚠️ Plan Replenishment & Monitor Closely"
            elif "Price" in row['scenario_name']:
                recommendation = "💰 Consider Price Adjustment for Higher Margin"
            else:
                recommendation = "✅ Healthy Scenario – Maintain Current Strategy"
                
            f.write(f"- **Recommendation**: {recommendation}\n\n")
        
        f.write("## Overall Recommendations\n\n")
        f.write("- Prioritize scenarios with **High Profit** and **Low Risk**.\n")
        f.write("- For High Risk scenarios: Increase supply or reduce promotional demand.\n")
        f.write("- Use Optimistic scenarios to set stretch targets for sales teams.\n")
        f.write("- Pessimistic scenarios help in contingency planning for inventory.\n")

# ------------------------------------------------------------------
# الدالة الرئيسية
# ------------------------------------------------------------------

def run_scenario_analysis(correlation_id: str) -> None:
    log_message("Scenario Analysis Layer started", "INFO", correlation_id)
    
    if not os.path.exists(FORECAST_INPUT):
        log_message(f"Forecast file not found: {FORECAST_INPUT}", "ERROR", correlation_id)
        return
    
    if not os.path.exists(INVENTORY_INPUT):
        log_message(f"Inventory file not found: {INVENTORY_INPUT}", "ERROR", correlation_id)
        return
    
    try:
        forecast_df = pd.read_csv(FORECAST_INPUT)
        inventory_df = pd.read_csv(INVENTORY_INPUT)
    except Exception as e:
        log_message(f"Error reading input files: {str(e)}", "ERROR", correlation_id)
        return
    
    required_cols = ["product_id", "forecast_week", "forecast_quantity"]
    missing = [col for col in required_cols if col not in forecast_df.columns]
    if missing:
        log_message(f"Missing required columns in forecast file: {missing}. Available: {list(forecast_df.columns)}", "ERROR", correlation_id)
        return
    
    # تجميع الكميات أسبوعياً
    weekly_forecast = forecast_df.groupby(["product_id", "forecast_week"])["forecast_quantity"].sum().unstack(fill_value=0)
    weekly_forecast.columns = [f"forecast_week_{int(col)}" for col in weekly_forecast.columns]
    weekly_forecast = weekly_forecast.reset_index()
    
    # افتراضات
    BASE_PRICE = 100.0
    COST_PER_UNIT = 60.0
    
    # السيناريوهات
    SCENARIOS = [
        {"name": "Base Case", "demand_mult": 1.0, "price_mult": 1.0, "supply_add": 0},
        {"name": "Optimistic Demand", "demand_mult": 1.2, "price_mult": 1.1, "supply_add": 0},
        {"name": "Pessimistic Demand", "demand_mult": 0.8, "price_mult": 0.9, "supply_add": 0},
        {"name": "Price Promotion", "demand_mult": 1.3, "price_mult": 0.85, "supply_add": 100},
        {"name": "Supply Boost", "demand_mult": 1.0, "price_mult": 1.0, "supply_add": 200},
    ]
    
    all_results = []
    
    for product_id in weekly_forecast["product_id"].unique():
        product_row = weekly_forecast[weekly_forecast["product_id"] == product_id]
        if product_row.empty:
            continue
        
        weekly_quantities = [
            product_row["forecast_week_1"].iloc[0],
            product_row["forecast_week_2"].iloc[0],
            product_row["forecast_week_3"].iloc[0],
            product_row["forecast_week_4"].iloc[0]
        ]
        
        inv_row = inventory_df[inventory_df["product_id"] == product_id]
        current_stock = inv_row["stock_on_hand"].iloc[0] if not inv_row.empty and "stock_on_hand" in inv_row.columns else 1000  # fallback
        
        product_data = {
            "weekly_quantities": weekly_quantities,
            "current_stock": current_stock,
            "base_price": BASE_PRICE,
            "cost_per_unit": COST_PER_UNIT
        }
        
        for scen in SCENARIOS:
            result = simulate_scenario(product_data, scen)
            all_results.append({
                "product_id": product_id,
                "scenario_name": scen["name"],
                "expected_profit": result["expected_profit"],
                "total_sales": result["total_sales"],
                "remaining_stock": result["remaining_stock"],
                "stock_status": result["stock_status"],
                "risk_level": result["risk_level"]
            })
        
        log_message(f"Scenarios simulated for product {product_id}", "INFO", correlation_id)
    
    if not all_results:
        log_message("No scenarios generated – check input data", "WARNING", correlation_id)
        generate_insights_md(pd.DataFrame())  # تقرير فارغ
        return
    
    comparison_df = compare_scenarios(all_results)
    
    log_message(
        f"Writing Excel rows={comparison_df.shape[0]} cols={comparison_df.shape[1]}",
        "INFO",
        correlation_id
    )

    try:
        try:
            writer = pd.ExcelWriter(EXCEL_OUTPUT, engine="openpyxl")
        except Exception:
            writer = pd.ExcelWriter(EXCEL_OUTPUT, engine="xlsxwriter")

        comparison_df.to_excel(
            writer,
            index=False,
            sheet_name="Scenarios_Comparison"
        )

        writer.close()

        log_message(
            f"Scenarios comparison saved to {EXCEL_OUTPUT}",
            "INFO",
            correlation_id
        )

    except Exception as e:
        log_message(f"Error writing Excel: {str(e)}", "ERROR", correlation_id)
    
    generate_insights_md(comparison_df)
    log_message(f"Scenario insights report saved to {MD_OUTPUT}", "INFO", correlation_id)
    
    log_message("Scenario Analysis Layer completed", "INFO", correlation_id)