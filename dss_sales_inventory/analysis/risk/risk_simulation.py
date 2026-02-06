# analysis/risk/risk_simulation.py
"""
Risk Simulation Layer – Week 7
--------------------------------
Lightweight Monte Carlo simulation for product-level risk scoring.

Aligned with DSS v3.0:
- Dict-passing architecture
- correlation_id propagated
- Read-only on existing artifacts
- Generates:
    - analysis/risk/product_risk_scores.csv
    - analysis/risk/risk_assessment_report.md
"""

from __future__ import annotations

import os
import uuid
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd


# -------------------------------------------------------
# Logging
# -------------------------------------------------------

LOGGER = logging.getLogger("risk_simulation")
if not LOGGER.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(message)s]"
    )


# -------------------------------------------------------
# Paths
# -------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# UPDATED: Path changed to analysis/forecast/ per new structure
FORECAST_PATH = os.path.join(
    BASE_DIR, "analysis", "forecast", "forecast_results.csv"
)

# REMAINS: Path stays in data/processed/ as requested
FEATURES_PATH = os.path.join(
    BASE_DIR, "data", "processed", "inventory_features.csv"
)

# REMAINS: Outputs stay in analysis/risk/
OUTPUT_DIR = os.path.join(BASE_DIR, "analysis", "risk")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "product_risk_scores.csv")
OUTPUT_REPORT = os.path.join(OUTPUT_DIR, "risk_assessment_report.md")


# -------------------------------------------------------
# Utilities
# -------------------------------------------------------

def _log(correlation_id: str, msg: str, level=logging.INFO):
    LOGGER.log(level, f"{correlation_id} - {msg}")


# -------------------------------------------------------
# Core API (required by spec)
# -------------------------------------------------------

def define_distributions(
    product_forecast: pd.DataFrame,
    product_features: pd.Series,
    correlation_id: str,
) -> Dict[str, Dict[str, float]]:
    """
    Build simple distribution parameters for one product.

    Returns a lightweight param dict (not scipy objects on purpose)
    to keep the layer dependency-light and reproducible.
    """

    _log(correlation_id, f"define_distributions() - product {product_features['product_id']}")

    # ---- Demand distribution (Normal, truncated at 0)
    demand_mean = product_forecast["forecast_quantity"].mean()

    # Conservative default: 15% coefficient of variation
    demand_std = max(demand_mean * 0.15, 1e-6)

    # ---- Cost distribution (Normal, truncated at >0)
    unit_cost = product_features.get("unit_cost", np.nan)

    if pd.isna(unit_cost):
        unit_cost = product_features.get("cost", np.nan)

    if pd.isna(unit_cost):
        # absolute fallback – keeps pipeline alive
        unit_cost = 1.0

    cost_std = max(unit_cost * 0.05, 1e-6)

    # ---- Lead time distribution (Discrete uniform fallback)
    lead_time = product_features.get("lead_time_days", np.nan)
    if pd.isna(lead_time):
        lead_time = 7.0  # engineering default

    lead_time_low = max(int(round(lead_time - 2)), 1)
    lead_time_high = max(int(round(lead_time + 2)), lead_time_low + 1)

    # ---- Price (deterministic unless column exists)
    price = product_features.get("price", np.nan)
    if pd.isna(price):
        price = product_features.get("unit_price", np.nan)
    if pd.isna(price):
        price = product_features.get("selling_price", np.nan)
    if pd.isna(price):
        # fallback – allows profit computation
        price = unit_cost * 1.3

    # ---- Available stock
    available_stock = product_features.get("current_stock", np.nan)
    if pd.isna(available_stock):
        available_stock = product_features.get("stock", np.nan)
    if pd.isna(available_stock):
        # conservative fallback
        available_stock = demand_mean * 7

    # ---- Holding cost per day
    holding_cost = product_features.get("holding_cost_per_day", np.nan)
    if pd.isna(holding_cost):
        holding_cost = 0.01 * unit_cost

    return {
        "demand": {
            "mean": float(demand_mean),
            "std": float(demand_std),
        },
        "cost": {
            "mean": float(unit_cost),
            "std": float(cost_std),
        },
        "lead_time": {
            "low": int(lead_time_low),
            "high": int(lead_time_high),
        },
        "price": float(price),
        "available_stock": float(available_stock),
        "holding_cost_per_day": float(holding_cost),
    }


def run_monte_carlo(
    distributions: Dict[str, Dict[str, float]],
    n_simulations: int,
    correlation_id: str,
) -> np.ndarray:
    """
    Executes Monte Carlo simulation and returns
    an array of simulated profits.
    """

    _log(correlation_id, "run_monte_carlo() - start")

    rng = np.random.default_rng()

    # ---- Demand
    demand = rng.normal(
        loc=distributions["demand"]["mean"],
        scale=distributions["demand"]["std"],
        size=n_simulations
    )
    demand = np.clip(demand, 0, None)

    # ---- Cost
    cost = rng.normal(
        loc=distributions["cost"]["mean"],
        scale=distributions["cost"]["std"],
        size=n_simulations
    )
    cost = np.clip(cost, 0, None)

    # ---- Lead time
    lead_time = rng.integers(
        low=distributions["lead_time"]["low"],
        high=distributions["lead_time"]["high"] + 1,
        size=n_simulations
    )

    price = distributions["price"]
    available_stock = distributions["available_stock"]
    holding_cost_per_day = distributions["holding_cost_per_day"]

    # ---- Realized sales
    realized_sales = np.minimum(demand, available_stock)

    # ---- Profit model (simple & stable)
    revenue = realized_sales * price
    variable_cost = realized_sales * cost
    holding_cost = lead_time * holding_cost_per_day

    profit = revenue - variable_cost - holding_cost

    _log(correlation_id, "run_monte_carlo() - end")

    return profit


# -------------------------------------------------------
# Risk computation
# -------------------------------------------------------

def compute_risk_metrics(
    profits: np.ndarray
) -> Dict[str, float]:

    mean_profit = float(np.mean(profits))
    std_profit = float(np.std(profits))

    var_95 = float(np.percentile(profits, 5))
    ci_lower = float(np.percentile(profits, 2.5))
    ci_upper = float(np.percentile(profits, 97.5))

    eps = 1e-6

    raw_risk = abs(var_95) / (abs(mean_profit) + eps)

    return {
        "expected_profit_mean": mean_profit,
        "profit_std": std_profit,
        "var_95": var_95,
        "ci_lower": ci_lower,
        "ci_upper": ci_upper,
        "raw_risk_score": raw_risk,
    }


# -------------------------------------------------------
# Report generation
# -------------------------------------------------------

def generate_risk_report(
    df: pd.DataFrame,
    correlation_id: str,
):
    _log(correlation_id, "Generating risk_assessment_report.md")

    total_products = len(df)
    high_risk_count = int((df["risk_level"] == "High").sum())
    avg_score = float(df["risk_score"].mean())

    top10 = df.sort_values("risk_score", ascending=False).head(10)

    lines = []
    lines.append("# Risk Assessment Report\n")
    lines.append("## Executive Summary\n")
    lines.append(f"- Total products analysed: **{total_products}**\n")
    lines.append(f"- High risk products: **{high_risk_count}**\n")
    lines.append(f"- Average risk score: **{avg_score:.4f}**\n")

    lines.append("\n## Top 10 High-Risk Products\n")
    lines.append("| product_id | risk_score | VaR(95%) | CI |\n")
    lines.append("|------------|-----------:|---------:|----|\n")

    for _, r in top10.iterrows():
        ci_range = f"[{r['ci_lower']:.2f}, {r['ci_upper']:.2f}]"
        lines.append(
            f"| {int(r['product_id'])} | {r['risk_score']:.4f} | {r['var_95']:.2f} | {ci_range} |\n"
        )

    lines.append("\n## Decision Interpretation\n")
    lines.append(
        "Products classified as **High risk** with strongly negative VaR should be reviewed for:\n"
        "- safety stock adjustments\n"
        "- reorder policy revisions\n"
        "- pricing buffers or promotion controls\n"
    )

    lines.append("\n## Methodological Warning\n")
    lines.append(
        "This Monte Carlo simulation is based on simplified and independent probability distributions. "
        "It does not capture tail risks, systemic shocks, supplier failures, or extreme market disruptions.\n"
    )

    with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
        f.writelines(lines)


# -------------------------------------------------------
# Main orchestration
# -------------------------------------------------------

def run_risk_simulation(
    data: Dict | None = None,
    correlation_id: str | None = None,
    n_simulations: int = 2000,
) -> pd.DataFrame:
    """
    Entry point compatible with the DSS orchestrator.
    """

    if correlation_id is None:
        correlation_id = str(uuid.uuid4())

    _log(correlation_id, "Risk simulation layer started")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(FORECAST_PATH):
        raise FileNotFoundError(FORECAST_PATH)

    if not os.path.exists(FEATURES_PATH):
        raise FileNotFoundError(FEATURES_PATH)

    forecast_df = pd.read_csv(FORECAST_PATH)
    features_df = pd.read_csv(FEATURES_PATH)

    # ---- Required columns from your real forecast output
    required_cols = {"product_id", "forecast_quantity"}
    if not required_cols.issubset(forecast_df.columns):
        raise ValueError(
            f"forecast_results.csv must contain columns {required_cols}"
        )

    results = []

    for product_id, f_group in forecast_df.groupby("product_id"):

        product_features = features_df[
            features_df["product_id"] == product_id
        ]

        if product_features.empty:
            _log(
                correlation_id,
                f"Skipping product {product_id} (missing features)",
                logging.WARNING
            )
            continue

        product_features = product_features.iloc[0].copy()
        product_features["product_id"] = product_id

        try:
            distributions = define_distributions(
                f_group,
                product_features,
                correlation_id
            )

            profits = run_monte_carlo(
                distributions,
                n_simulations=n_simulations,
                correlation_id=correlation_id
            )

            metrics = compute_risk_metrics(profits)

            results.append({
                "product_id": product_id,
                "expected_profit_mean": metrics["expected_profit_mean"],
                "profit_std": metrics["profit_std"],
                "var_95": metrics["var_95"],
                "ci_lower": metrics["ci_lower"],
                "ci_upper": metrics["ci_upper"],
                "raw_risk_score": metrics["raw_risk_score"],
            })

        except Exception as exc:
            _log(
                correlation_id,
                f"Risk simulation failed for product {product_id}: {exc}",
                logging.ERROR
            )

    if not results:
        raise RuntimeError("No products were processed in risk simulation layer.")

    result_df = pd.DataFrame(results)

    # ---- Min–max normalization
    min_r = result_df["raw_risk_score"].min()
    max_r = result_df["raw_risk_score"].max()

    if max_r - min_r < 1e-9:
        result_df["risk_score"] = 0.0
    else:
        result_df["risk_score"] = (
            (result_df["raw_risk_score"] - min_r) / (max_r - min_r)
        )

    # ---- Risk level classification
    def classify(x):
        if x < 0.33:
            return "Low"
        elif x < 0.66:
            return "Medium"
        else:
            return "High"

    result_df["risk_level"] = result_df["risk_score"].apply(classify)

    final_cols = [
        "product_id",
        "expected_profit_mean",
        "profit_std",
        "var_95",
        "ci_lower",
        "ci_upper",
        "risk_score",
        "risk_level",
    ]

    result_df = result_df[final_cols].sort_values(
        "risk_score", ascending=False
    )

    result_df.to_csv(OUTPUT_CSV, index=False)

    generate_risk_report(result_df, correlation_id)

    _log(correlation_id, "Risk simulation layer completed")

    return result_df


# -------------------------------------------------------
# Standalone execution
# -------------------------------------------------------

if __name__ == "__main__":
    run_risk_simulation()