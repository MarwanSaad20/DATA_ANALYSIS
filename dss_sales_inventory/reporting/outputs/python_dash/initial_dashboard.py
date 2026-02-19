import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
import os
from pathlib import Path

# ================================
# Project Configuration & Root Detection
# ================================
def get_project_root() -> str:
    """
    Dynamically determine the project root directory relative to this script.
    Adjusts automatically even لو تم نقل السكربت لمجلد آخر داخل المشروع.
    """
    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        if (parent / "analysis").exists():
            return str(parent)
    return str(script_path.parent.parent)

ROOT = get_project_root()
st.write(f"Project ROOT detected at: {ROOT}")

# ================================
# Data Loading & Preprocessing
# ================================
@st.cache_data(ttl=3600, show_spinner="Loading and processing data from SQLite and CSVs...")
def fetch_and_preprocess_data() -> pd.DataFrame:
    db_path = os.path.join(ROOT, "analysis", "analytics.db")
    kpis_path = os.path.join(ROOT, "analysis", "kpis", "product_kpis.csv")
    risk_path = os.path.join(ROOT, "analysis", "risk", "product_risk_scores.csv")

    for path, name in [
        (db_path, "SQLite database (analysis/analytics.db)"),
        (kpis_path, "product_kpis.csv"),
        (risk_path, "product_risk_scores.csv")
    ]:
        if not os.path.exists(path):
            st.error(f"{name} not found at expected path: {path}")
            st.stop()

    try:
        conn = sqlite3.connect(db_path)
        fact_sales = pd.read_sql_query("SELECT * FROM fact_sales", conn)
        dim_product = pd.read_sql_query("SELECT * FROM dim_product", conn)
        dim_date = pd.read_sql_query("SELECT * FROM dim_date", conn)
        dim_region = pd.read_sql_query("SELECT * FROM dim_region", conn)
        conn.close()

        product_kpis = pd.read_csv(kpis_path)
        product_risk = pd.read_csv(risk_path)

        # Validation (updated to actual columns)
        validation_rules = [
            ("fact_sales", fact_sales, ["product_id", "date_id", "region_id", "quantity", "revenue", "cost"]),
            ("dim_product", dim_product, ["product_id", "unit_cost", "unit_price"]),
            ("dim_date", dim_date, ["date_id", "full_date", "year", "month", "quarter"]),
            ("dim_region", dim_region, ["region_id", "region_name"]),
            ("product_kpis", product_kpis, ["product_id", "inventory_risk_score", "decision_flag", "demand_pressure_index", "profitability_margin"]),
            ("product_risk", product_risk, ["product_id", "risk_level", "expected_profit_mean", "profit_std", "risk_score", "var_95"])
        ]
        for source, df_obj, required in validation_rules:
            missing = [col for col in required if col not in df_obj.columns]
            if missing:
                st.error(f"Missing required columns in {source}: {missing}")
                st.stop()

        # Merge tables
        df = (fact_sales
              .merge(dim_product, on="product_id", how="left")
              .merge(dim_date, on="date_id", how="left")
              .merge(dim_region, on="region_id", how="left"))

        df["profit"] = df["revenue"] - df["cost"]
        df["full_date"] = pd.to_datetime(df["full_date"])
        df = df.merge(product_kpis, on="product_id", how="left")
        df = df.merge(product_risk, on="product_id", how="left")

        # Handle missing values
        fill_zero_cols = ["inventory_risk_score", "profitability_margin", "demand_pressure_index",
                          "expected_profit_mean", "profit_std", "risk_score", "var_95",
                          "unit_cost", "unit_price", "quarter"]
        for col in fill_zero_cols:
            df[col] = df[col].fillna(0)

        df["decision_flag"] = df["decision_flag"].fillna("No Flag")
        df["risk_level"] = df["risk_level"].fillna("Unknown")

        return df

    except Exception as e:
        st.error(f"Failed to load or process data: {e}")
        st.stop()

# ================================
# Portfolio-Level Metrics
# ================================
def compute_portfolio_metrics(full_df: pd.DataFrame) -> dict:
    unique_products = full_df.drop_duplicates(subset="product_id")
    portfolio_avg_risk = round(unique_products["inventory_risk_score"].mean(), 2)
    portfolio_avg_margin = round(unique_products["profitability_margin"].mean(), 2)
    product_risk_map = unique_products.set_index("product_id")["inventory_risk_score"].to_dict()
    return {
        "avg_risk": portfolio_avg_risk,
        "avg_margin": portfolio_avg_margin,
        "risk_map": product_risk_map
    }

# ================================
# Filtered KPI Computation
# ================================
def compute_financial_kpis(filtered_df: pd.DataFrame) -> dict:
    if filtered_df.empty:
        return {"total_revenue": 0, "total_cost": 0, "total_profit": 0}
    return {
        "total_revenue": filtered_df["revenue"].sum(),
        "total_cost": filtered_df["cost"].sum(),
        "total_profit": filtered_df["profit"].sum()
    }

# ================================
# Visualization Functions
# ================================
def build_gauge(value: float, title: str) -> go.Figure:
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "green"},
                {'range': [50, 80], 'color': "orange"},
                {'range': [80, 100], 'color': "red"}
            ],
            'threshold': {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 70}
        }
    ))
    return fig

def build_visualizations(filtered_df: pd.DataFrame):
    if filtered_df.empty:
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="No data matches the selected filters",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16)
        )
        empty_fig.update_layout(title="No Data Available")
        return empty_fig, empty_fig, empty_fig

    trend_df = filtered_df.groupby("full_date", as_index=False)["profit"].sum().sort_values("full_date")
    fig_trend = px.line(trend_df, x="full_date", y="profit", title="Profit Trend Over Time",
                        labels={"profit": "Profit", "full_date": "Date"})
    fig_trend.update_layout(showlegend=False)

    product_df = filtered_df.groupby("product_id", as_index=False).agg(
        profit=("profit", "sum"),
        inventory_risk_score=("inventory_risk_score", "first"),
        risk_level=("risk_level", "first"),
        profitability_margin=("profitability_margin", "first"),
        decision_flag=("decision_flag", "first"),
        demand_pressure_index=("demand_pressure_index", "first"),
        expected_profit_mean=("expected_profit_mean", "first"),
        profit_std=("profit_std", "first"),
        risk_score=("risk_score", "first"),
        var_95=("var_95", "first"),
        unit_cost=("unit_cost", "first"),
        unit_price=("unit_price", "first")
    )

    hover_columns = ["decision_flag", "demand_pressure_index", "expected_profit_mean",
                     "profit_std", "risk_score", "var_95", "unit_cost", "unit_price"]

    # Bar Chart
    product_df_bar = product_df.sort_values("profit", ascending=False)
    fig_bar = px.bar(product_df_bar, x="profit", y="product_id", orientation="h",
                     color="risk_level", hover_data=hover_columns,
                     title="Product Profitability Comparison",
                     labels={"profit": "Total Profit", "product_id": "Product ID"})

    # Scatter / Decision Matrix
    fig_scatter = px.scatter(product_df, x="profit", y="inventory_risk_score",
                             color="risk_level", size="profitability_margin", size_max=40,
                             hover_name="product_id", hover_data=hover_columns,
                             title="Decision Matrix: Profit vs. Inventory Risk Score",
                             labels={"profit": "Total Profit", "inventory_risk_score": "Inventory Risk Score"})
    fig_scatter.update_layout(legend_title="Risk Level")

    return fig_trend, fig_bar, fig_scatter

# ================================
# Streamlit App Layout
# ================================
st.set_page_config(page_title="DSS Executive Dashboard", layout="wide")
st.title("Decision Support System – Executive Dashboard")

full_df = fetch_and_preprocess_data()
portfolio = compute_portfolio_metrics(full_df)

st.sidebar.header("Filters")
product_options = sorted(full_df["product_id"].dropna().unique())
selected_products = st.sidebar.multiselect("Products", product_options, default=product_options)

region_options = sorted(full_df["region_name"].dropna().unique())
selected_regions = st.sidebar.multiselect("Regions", region_options, default=region_options)

year_options = sorted(full_df["year"].unique())
selected_years = st.sidebar.multiselect("Years", year_options, default=year_options)

quarter_options = sorted(full_df["quarter"].unique())
selected_quarters = st.sidebar.multiselect("Quarters", quarter_options, default=quarter_options)

month_options = sorted(full_df["month"].unique())
selected_months = st.sidebar.multiselect("Months", month_options, default=month_options)

gauge_options = ["Portfolio Average"] + product_options
selected_gauge = st.sidebar.selectbox("Risk Gauge Focus", gauge_options, index=0)

filtered_df = full_df[
    full_df["product_id"].isin(selected_products) &
    full_df["region_name"].isin(selected_regions) &
    full_df["year"].isin(selected_years) &
    full_df["quarter"].isin(selected_quarters) &
    full_df["month"].isin(selected_months)
]

financial_kpis = compute_financial_kpis(filtered_df)

gauge_value = portfolio["avg_risk"] if selected_gauge == "Portfolio Average" \
    else portfolio["risk_map"].get(selected_gauge, 0)
gauge_title = "Portfolio Average Inventory Risk Score" if selected_gauge == "Portfolio Average" \
    else f"Inventory Risk Score – {selected_gauge}"

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Revenue", f"{financial_kpis['total_revenue']:,.0f}")
col2.metric("Total Cost", f"{financial_kpis['total_cost']:,.0f}")
col3.metric("Total Profit", f"{financial_kpis['total_profit']:,.0f}")
col4.metric("Portfolio Avg. Inventory Risk Score", portfolio["avg_risk"])
col5.metric("Portfolio Avg. Profitability Margin", f"{portfolio['avg_margin']:.2f}")

fig_trend, fig_bar, fig_scatter = build_visualizations(filtered_df)
fig_gauge = build_gauge(gauge_value, gauge_title)

st.plotly_chart(fig_trend, use_container_width=True)
st.caption("Profit trend over time – updates dynamically with selected filters.")

st.plotly_chart(fig_bar, use_container_width=True)
st.caption("Total profit per product, colored by risk level (highest performers at top). Hover for detailed metrics (decision flag, demand pressure, VaR, etc.).")

st.plotly_chart(fig_scatter, use_container_width=True)
st.caption("Decision matrix: ideal products top-right (high profit, low risk). Bubble size = profitability margin. Hover for detailed product metrics.")

st.plotly_chart(fig_gauge, use_container_width=True)
st.caption("Inventory risk gauge (0–100). Green ≤50, Orange 50–80, Red ≥80. Threshold at 70.")

st.caption(
    "Financial KPIs and charts update in real time with filters. "
    "Portfolio-level metrics reflect the full dataset."
)

# ================================
# Production Deployment Recommendations
# ================================
"""
Production Deployment Recommendations:
- Host on Streamlit Community Cloud, AWS, GCP, or a dedicated server for reliability.
- For sensitive data, add authentication (e.g., streamlit-authenticator or reverse proxy).
- For large datasets, consider pre-aggregating fact tables in SQL or using DuckDB for faster queries.
- Monitor cache memory usage; adjust ttl or clear cache periodically in production.
- Add logging (e.g., structlog) for usage analytics and error tracking.
- Enable Streamlit's built-in theming and consider dark mode support.
"""
