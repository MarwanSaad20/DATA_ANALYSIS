# decision_brief.py
# ---------------------------------------------------------
# Week 13 – Decision Brief Export (Narrative Dashboard)
#
# Project  : DSS – Sales & Inventory Decision Support System
# Location : C:\Data_Analysis\dss_sales_inventory\reporting\outputs\python_dash
#
# This script generates an executive narrative decision brief
# by consuming:
#   - Star Schema (SQLite)
#   - KPI layer outputs
#   - Risk simulation outputs
#   - Time-series trend narrative
#
# Output:
#   C:\Data_Analysis\dss_sales_inventory\reporting\outputs\python_dash\decision_brief.md
# ---------------------------------------------------------

import sqlite3
import pandas as pd
from pathlib import Path
import re
from datetime import datetime, UTC


# ---------------------------------------------------------
# Project Paths (STRICT – based on DSS v3.7)
# ---------------------------------------------------------

PROJECT_ROOT = Path(r"C:\Data_Analysis\dss_sales_inventory")

DB_PATH = PROJECT_ROOT / "analysis" / "analytics.db"

KPI_PATH = PROJECT_ROOT / "analysis" / "kpis" / "product_kpis.csv"
RISK_PATH = PROJECT_ROOT / "analysis" / "risk" / "product_risk_scores.csv"

TREND_PATH = PROJECT_ROOT / "analysis" / "time_series" / "trend_insights.md"

# Unified output location
OUTPUT_MD = (
    PROJECT_ROOT
    / "reporting"
    / "outputs"
    / "python_dash"
    / "decision_brief.md"
)


# ---------------------------------------------------------
# SQL QUERIES (validated against real schema)
# ---------------------------------------------------------

SQL_TOTAL_FINANCIALS = """
SELECT
    SUM(f.revenue)                     AS total_revenue,
    SUM(f.cost)                        AS total_cost,
    SUM(f.revenue - f.cost)            AS total_profit
FROM fact_sales f;
"""

SQL_PRODUCT_FINANCIALS = """
SELECT
    f.product_id,
    SUM(f.revenue)          AS total_revenue,
    SUM(f.revenue - f.cost) AS total_profit
FROM fact_sales f
GROUP BY f.product_id;
"""

SQL_DATE_RANGE = """
SELECT
    MIN(d.full_date) AS min_date,
    MAX(d.full_date) AS max_date
FROM fact_sales f
JOIN dim_date d
    ON f.date_id = d.date_id;
"""


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


# ---------------------------------------------------------
# Trend file reader (based on the real file structure)
# ---------------------------------------------------------

def read_trend_insights(trend_path: Path) -> dict:
    if not trend_path.exists():
        raise FileNotFoundError(f"trend_insights.md not found at {trend_path}")

    text = trend_path.read_text(encoding="utf-8")

    # ---- Header fields
    analysis_date = None
    metric = None
    rolling_window = None

    m = re.search(r"\*\*Analysis Date:\*\*\s*(.+)", text)
    if m:
        analysis_date = m.group(1).strip()

    m = re.search(r"\*\*Metric:\*\*\s*(.+)", text)
    if m:
        metric = m.group(1).strip()

    m = re.search(r"\*\*Rolling Window:\*\*\s*(\d+)", text)
    if m:
        rolling_window = int(m.group(1))

    # ---- Executive Summary block
    exec_block_match = re.search(
        r"## Executive Summary(.*)$", text, re.S
    )

    exec_summary = {
        "analyzed_products": None,
        "upward": None,
        "downward": None,
        "stable": None
    }

    if exec_block_match:
        block = exec_block_match.group(1)

        m = re.search(r"Analyzed\s+(\d+)\s+products", block, re.I)
        if m:
            exec_summary["analyzed_products"] = int(m.group(1))

        m = re.search(r"\*\*Upward trends:\*\*\s*(\d+)", block, re.I)
        if m:
            exec_summary["upward"] = int(m.group(1))

        m = re.search(r"\*\*Downward trends:\*\*\s*(\d+)", block, re.I)
        if m:
            exec_summary["downward"] = int(m.group(1))

        m = re.search(r"\*\*Stable trends:\*\*\s*(\d+)", block, re.I)
        if m:
            exec_summary["stable"] = int(m.group(1))

    # ---- Per-product sections (only for sampled products)
    product_blocks = re.findall(
        r"## Product\s+(\d+)(.*?)(?=## Product|\Z)", text, re.S
    )

    sampled_products = []

    for pid, block in product_blocks:
        trend = None
        avg_val = None
        latest_val = None

        m = re.search(r"\*\*Trend:\*\*\s*([A-Z]+)", block)
        if m:
            trend = m.group(1).strip()

        m = re.search(r"\*\*Average avg_daily_sales:\*\*\s*([0-9\.]+)", block)
        if m:
            avg_val = _safe_float(m.group(1))

        m = re.search(r"\*\*Latest Value:\*\*\s*([0-9\.]+)", block)
        if m:
            latest_val = _safe_float(m.group(1))

        sampled_products.append({
            "product_id": int(pid),
            "trend": trend,
            "avg": avg_val,
            "latest": latest_val
        })

    return {
        "analysis_date": analysis_date,
        "metric": metric,
        "rolling_window": rolling_window,
        "executive_summary": exec_summary,
        "sampled_products": sampled_products
    }


# ---------------------------------------------------------
# Core compiler
# ---------------------------------------------------------

def compile_decision_summary() -> dict:

    # ---------------------------
    # Load KPI and Risk layers
    # ---------------------------
    kpi_df = pd.read_csv(KPI_PATH)
    risk_df = pd.read_csv(RISK_PATH)

    # ---------------------------
    # SQLite queries
    # ---------------------------
    with sqlite3.connect(DB_PATH) as conn:
        financials = pd.read_sql(SQL_TOTAL_FINANCIALS, conn).iloc[0].to_dict()
        product_financials = pd.read_sql(SQL_PRODUCT_FINANCIALS, conn)
        date_range = pd.read_sql(SQL_DATE_RANGE, conn).iloc[0].to_dict()

    # ---------------------------
    # Portfolio KPIs
    # ---------------------------
    portfolio_kpis = {
        "avg_inventory_risk_score": float(kpi_df["inventory_risk_score"].mean()),
        "avg_profitability_margin": float(kpi_df["profitability_margin"].mean()),
        "requires_intervention_count": int(
            (kpi_df["decision_flag"] == "Requires Intervention").sum()
        ),
        "total_products": int(kpi_df["product_id"].nunique())
    }

    # ---------------------------
    # Risk exposure summary
    # ---------------------------
    high_risk_count = int((risk_df["risk_level"] == "High").sum())

    top_risk = (
        risk_df.sort_values("risk_score", ascending=False)
        .head(5)
        .loc[:, ["product_id", "risk_score", "risk_level"]]
        .to_dict(orient="records")
    )

    worst_expected_profit = (
        risk_df.sort_values("expected_profit_mean", ascending=True)
        .head(5)
        .loc[:, ["product_id", "expected_profit_mean"]]
        .to_dict(orient="records")
    )

    # ---------------------------
    # Priority intervention list
    # ---------------------------
    merged = pd.merge(
        kpi_df,
        risk_df,
        on="product_id",
        how="left"
    )

    interventions = (
        merged[merged["decision_flag"] == "Requires Intervention"]
        .loc[:, [
            "product_id",
            "inventory_risk_score",
            "demand_pressure_index",
            "profitability_margin",
            "risk_level",
            "expected_profit_mean"
        ]]
        .sort_values("inventory_risk_score", ascending=False)
        .to_dict(orient="records")
    )

    # ---------------------------
    # Executive recommendations (rule-based)
    # ---------------------------
    recommendations = []

    for row in interventions:
        if (
            row["risk_level"] == "High"
            and row["profitability_margin"] is not None
            and row["profitability_margin"] >= portfolio_kpis["avg_profitability_margin"]
        ):
            rec = "Review supply and pricing strategy rather than stopping the product."
        elif (
            row["profitability_margin"] is not None
            and row["profitability_margin"] < portfolio_kpis["avg_profitability_margin"]
            and row["expected_profit_mean"] is not None
            and row["expected_profit_mean"] < 0
        ):
            rec = "Consider product de-prioritization, scale-down, or discontinuation analysis."
        else:
            rec = "Monitor closely and reassess replenishment and demand signals."

        recommendations.append({
            "product_id": row["product_id"],
            "recommendation": rec
        })

    # ---------------------------
    # Trend insights
    # ---------------------------
    trends = read_trend_insights(TREND_PATH)

    # ---------------------------
    # Final unified summary object
    # ---------------------------
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "financials": financials,
        "date_range": date_range,
        "portfolio_kpis": portfolio_kpis,
        "risk_exposure": {
            "high_risk_count": high_risk_count,
            "top_risk_products": top_risk,
            "worst_expected_profit_products": worst_expected_profit
        },
        "interventions": interventions,
        "recommendations": recommendations,
        "trends": trends
    }

    return summary


# ---------------------------------------------------------
# Markdown exporter
# ---------------------------------------------------------

def export_to_md(summary: dict, output_path: Path):

    fin = summary["financials"]
    dr = summary["date_range"]
    pk = summary["portfolio_kpis"]
    risk = summary["risk_exposure"]
    trends = summary["trends"]

    lines = []

    # -----------------------------------------------------
    # Title
    # -----------------------------------------------------
    lines.append("# Executive Decision Brief\n")
    lines.append(f"**Generated at (UTC):** {summary['generated_at']}\n")

    # -----------------------------------------------------
    # 1. Executive Snapshot
    # -----------------------------------------------------
    lines.append("## 1. Executive Snapshot\n")
    lines.append(f"- Total Revenue: {fin['total_revenue']:.2f}")
    lines.append(f"- Total Cost: {fin['total_cost']:.2f}")
    lines.append(f"- Total Profit: {fin['total_profit']:.2f}")
    lines.append(f"- Data Coverage Period: {dr['min_date']} → {dr['max_date']}\n")

    # -----------------------------------------------------
    # 2. Portfolio KPI Overview
    # -----------------------------------------------------
    lines.append("## 2. Portfolio KPI Overview\n")
    lines.append(f"- Total Products: {pk['total_products']}")
    lines.append(f"- Average Inventory Risk Score: {pk['avg_inventory_risk_score']:.2f}")
    lines.append(f"- Average Profitability Margin: {pk['avg_profitability_margin']:.4f}")
    lines.append(f"- Products Requiring Intervention: {pk['requires_intervention_count']}\n")

    # -----------------------------------------------------
    # 3. Risk Exposure Summary
    # -----------------------------------------------------
    lines.append("## 3. Risk Exposure Summary\n")
    lines.append(f"- High Risk Products Count: {risk['high_risk_count']}\n")

    lines.append("### Top 5 Products by Risk Score")
    for r in risk["top_risk_products"]:
        lines.append(
            f"- Product {r['product_id']} | Risk Score: {r['risk_score']:.4f} | Level: {r['risk_level']}"
        )

    lines.append("\n### Worst 5 Products by Expected Profit Mean")
    for r in risk["worst_expected_profit_products"]:
        lines.append(
            f"- Product {r['product_id']} | Expected Profit Mean: {r['expected_profit_mean']:.4f}"
        )

    lines.append("")

    # -----------------------------------------------------
    # 4. Trend & Market Direction
    # -----------------------------------------------------
    lines.append("## 4. Trend & Market Direction\n")

    lines.append(f"- Analysis Date: {trends['analysis_date']}")
    lines.append(f"- Metric: {trends['metric']}")
    lines.append(f"- Rolling Window: {trends['rolling_window']}\n")

    es = trends["executive_summary"]

    lines.append("**Trend Summary:**")
    lines.append(f"- Analyzed Products: {es['analyzed_products']}")
    lines.append(f"- Upward Trends: {es['upward']}")
    lines.append(f"- Downward Trends: {es['downward']}")
    lines.append(f"- Stable Trends: {es['stable']}\n")

    if es["upward"] == 0 and es["downward"] == 0:
        narrative = (
            "Demand behavior across the monitored products is currently stable, "
            "with no statistically detected upward or downward shifts in average daily sales."
        )
    else:
        narrative = (
            "Mixed demand dynamics are observed across the monitored products, "
            "indicating the presence of upward and/or downward demand movements."
        )

    lines.append(narrative + "\n")

    # -----------------------------------------------------
    # 5. Priority Intervention List
    # -----------------------------------------------------
    lines.append("## 5. Priority Intervention List\n")

    if len(summary["interventions"]) == 0:
        lines.append("No products currently flagged for intervention.\n")
    else:
        for r in summary["interventions"]:
            lines.append(
                f"- Product {r['product_id']} | "
                f"Inventory Risk: {r['inventory_risk_score']:.2f} | "
                f"Demand Pressure: {r['demand_pressure_index']:.4f} | "
                f"Profit Margin: {r['profitability_margin']:.4f} | "
                f"Risk Level: {r['risk_level']} | "
                f"Expected Profit Mean: {r['expected_profit_mean']:.4f}"
            )
        lines.append("")

    # -----------------------------------------------------
    # 6. Executive Recommendations
    # -----------------------------------------------------
    lines.append("## 6. Executive Recommendations\n")

    if len(summary["recommendations"]) == 0:
        lines.append("No specific recommendations generated.\n")
    else:
        for r in summary["recommendations"]:
            lines.append(
                f"- Product {r['product_id']}: {r['recommendation']}"
            )
        lines.append("")

    # -----------------------------------------------------
    # 7. Data Limitations
    # -----------------------------------------------------
    lines.append("## 7. Data Limitations\n")
    lines.append(
        "- This report is a narrative executive summary only.\n"
        "- No additional analytical models were executed during its generation.\n"
        "- All conclusions depend strictly on the latest available dashboard and pipeline outputs.\n"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

def main():
    summary = compile_decision_summary()
    export_to_md(summary, OUTPUT_MD)
    print(f"Decision brief generated successfully at:\n{OUTPUT_MD}")


if __name__ == "__main__":
    main()