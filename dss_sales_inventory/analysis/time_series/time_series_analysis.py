# C:\Data_Analysis\dss_sales_inventory\analysis\time_series\time_series_analysis.py

import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # اجعل الرسم بدون واجهة رسومية
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse
import itertools

# ========================
# Helper Functions
# ========================

def load_csv_safely(path: Path) -> pd.DataFrame:
    """Load CSV safely with warnings for missing or empty files."""
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")
    df = pd.read_csv(path)
    if df.empty:
        print(f"Warning: {path.name} is empty.")
    return df

def find_value_column(df: pd.DataFrame) -> str:
    """Select primary numeric column for trend analysis."""
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    priority = ['avg_daily_sales', 'total_revenue']
    for col in priority:
        if col in numeric_cols:
            return col
    if numeric_cols:
        return numeric_cols[0]
    raise ValueError("No suitable numeric column found for analysis.")

def calculate_rolling_mean(df: pd.DataFrame, value_col: str, window: int) -> pd.DataFrame:
    """Calculate rolling mean per product_id."""
    df = df.copy()
    df['rolling_mean'] = df.groupby('product_id')[value_col].transform(
        lambda x: x.rolling(window=window, min_periods=1).mean()
    )
    return df

def detect_trend(series: pd.Series) -> str:
    """Determine trend direction: up, down, or stable."""
    if len(series) < 2:
        return "stable"
    x = np.arange(len(series))
    y = series.values.astype(float)
    slope = np.polyfit(x, y, 1)[0]
    threshold = np.std(y) * 0.05 if np.std(y) > 0 else 0.01
    if slope > threshold:
        return "up"
    elif slope < -threshold:
        return "down"
    return "stable"

def generate_insight(product_id: int, trend: str, demand_df: pd.DataFrame, inventory_df: pd.DataFrame, value_col: str) -> str:
    """Generate product-specific recommendation based on trend, demand, and inventory."""
    demand_row = demand_df[demand_df['product_id'] == product_id]
    inventory_rows = inventory_df[inventory_df['product_id'] == product_id]

    inventory_row = inventory_rows.iloc[-1] if not inventory_rows.empty else pd.Series()

    demand_level = demand_row['demand_pressure_level'].iloc[-1] if not demand_row.empty and 'demand_pressure_level' in demand_row.columns else "N/A"
    inventory_status = inventory_row.get('inventory_status', 'N/A')
    current_stock = inventory_row.get('stock_on_hand', 'N/A')
    reorder_point = inventory_row.get('reorder_point', 'N/A')

    base = f"{trend.upper()} trend in {value_col}."
    if trend == "up":
        return f"{base} Growing demand (Level: {demand_level}). Current stock: {current_stock} (vs reorder {reorder_point}). Status: {inventory_status}. **Recommendation: Increase stock to avoid shortages.**"
    elif trend == "down":
        return f"{base} Declining demand (Level: {demand_level}). Current stock: {current_stock} (vs reorder {reorder_point}). Status: {inventory_status}. **Recommendation: Reduce inventory to optimize holding costs.**"
    else:
        return f"{base} Stable pattern (Level: {demand_level}). Current stock: {current_stock} (vs reorder {reorder_point}). Status: {inventory_status}. **Recommendation: Maintain current stocking policy.**"

# ========================
# Main Function
# ========================

def main(root_dir: str, rolling_window: int = 7, max_sample_products: int = 10):
    ROOT = Path(root_dir)

    PRODUCT_PERF_VIEW = ROOT / 'reporting' / 'outputs' / 'product_performance_view.csv'
    DEMAND_PRESSURE_VIEW = ROOT / 'reporting' / 'outputs' / 'demand_pressure_view.csv'
    INVENTORY_STATUS_VIEW = ROOT / 'reporting' / 'outputs' / 'inventory_status_view.csv'

    PLOTS_DIR = ROOT / 'reporting' / 'outputs' / 'plots'
    REPORT_PATH = ROOT / 'analysis' / 'time_series' / 'trend_insights.md'
    CSV_PATH = ROOT / 'reporting' / 'outputs' / 'time_series_summary.csv'

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load DataFrames
    perf_df = load_csv_safely(PRODUCT_PERF_VIEW)
    demand_df = load_csv_safely(DEMAND_PRESSURE_VIEW)
    inventory_df = load_csv_safely(INVENTORY_STATUS_VIEW)

    value_col = find_value_column(perf_df)
    print(f"Using '{value_col}' as primary metric for analysis.")

    # إنشاء تواريخ وهمية إذا لم يكن موجود
    if 'date' not in perf_df.columns:
        perf_df['date'] = pd.to_datetime('2025-01-01') + pd.to_timedelta(
            perf_df.groupby('product_id').cumcount(), unit='D'
        )
    perf_df = perf_df.sort_values(['product_id', 'date']).reset_index(drop=True)
    perf_df = calculate_rolling_mean(perf_df, value_col, rolling_window)

    trends = {"up": 0, "down": 0, "stable": 0}
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    markers = ['o', '^', 's', 'D', 'v', '<', '>', 'p', '*', 'h']

    report_lines = [
        "# Time Series Trend Insights Report\n\n",
        f"**Analysis Date:** {pd.Timestamp('now').strftime('%Y-%m-%d')}\n",
        f"**Metric:** {value_col}\n",
        f"**Rolling Window:** {rolling_window} periods\n\n",
        "> **Note:** Synthetic dates used if no native date column. Trends via linear regression slope.\n\n"
    ]

    # ========================
    # Combined Plot (All Products)
    # ========================
    plt.figure(figsize=(14, 8))
    color_cycle = itertools.cycle(colors)
    marker_cycle = itertools.cycle(markers)
    for pid in perf_df['product_id'].unique():
        data = perf_df[perf_df['product_id'] == pid]
        color = next(color_cycle)
        marker = next(marker_cycle)
        plt.plot(data['date'], data[value_col], color=color, alpha=0.6)
        plt.plot(data['date'], data['rolling_mean'], color=color, linestyle='--', alpha=0.5)
    plt.title(f"Combined Trends - All Products by {value_col}")
    plt.xlabel("Date (Synthetic Index)")
    plt.ylabel(value_col)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    combined_path = PLOTS_DIR / "combined_all_products_trend.png"
    plt.savefig(combined_path, dpi=300, bbox_inches='tight')
    plt.close()

    report_lines.extend([
        "## Combined View\n\n",
        f"![Combined Trend]({combined_path.relative_to(ROOT)})\n\n",
        "---\n\n"
    ])

    # ========================
    # Individual Product Analysis (Sample Top Products)
    # ========================
    top_products = perf_df.groupby('product_id')[value_col].mean().nlargest(max_sample_products).index
    summary_list = []

    color_cycle = itertools.cycle(colors)
    marker_cycle = itertools.cycle(markers)
    for i, pid in enumerate(top_products):
        product_data = perf_df[perf_df['product_id'] == pid].copy()
        if product_data.empty:
            continue

        trend = detect_trend(product_data[value_col])
        trends[trend] += 1

        avg_value = product_data[value_col].mean()
        last_value = product_data[value_col].iloc[-1]
        insight = generate_insight(pid, trend, demand_df, inventory_df, value_col)

        # جمع بيانات CSV
        demand_row = demand_df[demand_df['product_id'] == pid]
        inventory_rows = inventory_df[inventory_df['product_id'] == pid]
        inventory_row = inventory_rows.iloc[-1] if not inventory_rows.empty else pd.Series()
        demand_level = demand_row['demand_pressure_level'].iloc[-1] if not demand_row.empty and 'demand_pressure_level' in demand_row.columns else "N/A"
        current_stock = inventory_row.get('stock_on_hand', 'N/A')
        reorder_point = inventory_row.get('reorder_point', 'N/A')
        inventory_status = inventory_row.get('inventory_status', 'N/A')

        summary_list.append({
            "product_id": pid,
            "trend": trend,
            "avg_value": avg_value,
            "last_value": last_value,
            "demand_level": demand_level,
            "current_stock": current_stock,
            "reorder_point": reorder_point,
            "inventory_status": inventory_status,
            "recommendation": insight
        })

        # الرسم الفردي
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]

        plt.figure(figsize=(12, 7))
        plt.plot(product_data['date'], product_data[value_col], label='Observed Values', color=color, marker=marker, markersize=8, linestyle='-')
        plt.plot(product_data['date'], product_data['rolling_mean'], label=f'{rolling_window}-Period Rolling Mean', color=color, linestyle='--', linewidth=2.5, alpha=0.8)
        plt.text(0.02, 0.98, f'Trend: {trend.upper()}', transform=plt.gca().transAxes, fontsize=14, fontweight='bold', verticalalignment='top', bbox=dict(facecolor='white', alpha=0.8, edgecolor=color))
        plt.annotate(f'Last: {last_value:.2f}', xy=(product_data['date'].iloc[-1], last_value), xytext=(10, 10), textcoords='offset points', fontsize=10, bbox=dict(facecolor='white', alpha=0.7))
        plt.title(f"Product {pid} — {value_col} Trend ({trend.upper()})")
        plt.xlabel("Date (Synthetic Index)")
        plt.ylabel(value_col)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        plot_path = PLOTS_DIR / f"trend_product_{pid}.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()

        report_lines.extend([
            f"## Product {pid}\n\n",
            f"- **Trend:** {trend.upper()}\n",
            f"- **Average {value_col}:** {avg_value:.2f}\n",
            f"- **Latest Value:** {last_value:.2f}\n",
            f"- **Insight:** {insight}\n\n",
            f"![Product {pid} Trend]({plot_path.relative_to(ROOT)})\n\n",
            "---\n\n"
        ])

    # ========================
    # Export CSV Summary
    # ========================
    summary_df = pd.DataFrame(summary_list)
    summary_df.to_csv(CSV_PATH, index=False, encoding='utf-8')
    print(f"CSV summary saved to: {CSV_PATH}")

    # ========================
    # Executive Summary
    # ========================
    total = len(perf_df['product_id'].unique())
    report_lines.extend([
        "## Executive Summary\n\n",
        f"- Analyzed {total} products.\n",
        f"- **Upward trends:** {trends['up']} product(s)\n",
        f"- **Downward trends:** {trends['down']} product(s)\n",
        f"- **Stable trends:** {trends['stable']} product(s)\n\n",
        "Recommendations are integrated per product above based on trend, current demand pressure, and latest inventory status.\n"
    ])

    REPORT_PATH.write_text("".join(report_lines), encoding="utf-8")

    print("Analysis complete.")
    print(f"Plots saved to: {PLOTS_DIR}")
    print(f"Markdown report generated: {REPORT_PATH}")

# ========================
# CLI Entrypoint
# ========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Time Series Trend Analysis Script")
    parser.add_argument('--root_dir', type=str, default=r'C:\Data_Analysis\dss_sales_inventory', help='Root directory of the project')
    parser.add_argument('--rolling_window', type=int, default=7, help='Rolling mean window size')
    parser.add_argument('--max_sample_products', type=int, default=10, help='Maximum number of products to plot individually')
    args = parser.parse_args()

    main(root_dir=args.root_dir, rolling_window=args.rolling_window, max_sample_products=args.max_sample_products)
