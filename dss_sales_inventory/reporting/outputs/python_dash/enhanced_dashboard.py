import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path
import os

# -------------------------------------------------
# Database Configuration and Root Discovery
# -------------------------------------------------

def find_project_root(start: Path):
    """
    Locate DSS project root by searching upwards for analysis/analytics.db
    starting from the script's directory.
    """
    start_dir = start.parent
    for p in [start_dir] + list(start_dir.parents):
        if (p / "analysis" / "analytics.db").exists():
            return p

    raise FileNotFoundError(
        "Could not locate DSS project root (analysis/analytics.db not found)."
    )

# Optional environment variable override for DB path
ENV_DB_PATH = os.getenv("DSS_DB_PATH")

if ENV_DB_PATH:
    DB_PATH = Path(ENV_DB_PATH).expanduser().resolve()
else:
    BASE_DIR = find_project_root(Path(__file__).resolve())
    DB_PATH = BASE_DIR / "analysis" / "analytics.db"

if not DB_PATH.exists():
    raise FileNotFoundError(f"SQLite database not found at: {DB_PATH}")

# -------------------------------------------------
# Database Connection Utilities
# -------------------------------------------------

def get_connection():
    """Return a SQLite connection using string path."""
    return sqlite3.connect(str(DB_PATH))

# -------------------------------------------------
# Data Loading – SQL Layer
# -------------------------------------------------

def load_sales_time_series(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Load time series data aggregated by product_id and region_name,
    including revenue, quantity, and profit.
    """
    query = """
    SELECT
        d.full_date AS date,
        f.product_id,
        r.region_name,
        SUM(f.revenue) AS revenue,
        SUM(f.quantity) AS quantity,
        SUM(f.revenue - f.cost) AS profit
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_region r ON f.region_id = r.region_id
    WHERE d.full_date BETWEEN ? AND ?
    GROUP BY d.full_date, f.product_id, r.region_name
    ORDER BY d.full_date
    """
    with get_connection() as conn:
        df = pd.read_sql_query(query, conn, params=[start_date, end_date])

    df["date"] = pd.to_datetime(df["date"])
    return df

# -------------------------------------------------
# Data Preparation Utilities
# -------------------------------------------------

def fill_missing_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill missing dates with zeros for product-region combinations.
    Create a readable 'identifier' column for charting.
    """
    if df.empty:
        return df

    all_dates = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
    combos = df[["product_id", "region_name"]].drop_duplicates().assign(key=1)
    date_df = pd.DataFrame({"date": all_dates}).assign(key=1)
    full_index = date_df.merge(combos, on="key").drop("key", axis=1)
    base = full_index.merge(df, on=["date", "product_id", "region_name"], how="left")
    base[["revenue", "quantity", "profit"]] = base[["revenue", "quantity", "profit"]].fillna(0)
    base["identifier"] = base["product_id"].astype(str) + " (" + base["region_name"] + ")"
    return base

# -------------------------------------------------
# Time Intelligence Calculations
# -------------------------------------------------

def compute_rolling_avg(df: pd.DataFrame, window: int) -> pd.DataFrame:
    """Compute rolling average revenue per product-region."""
    df = df.sort_values(["identifier", "date"])
    df["rolling_revenue"] = df.groupby("identifier")["revenue"].transform(
        lambda s: s.rolling(window=window, min_periods=1).mean()
    )
    return df

def compute_cumulative_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative revenue per product-region."""
    df = df.sort_values(["identifier", "date"])
    df["cumulative_revenue"] = df.groupby("identifier")["revenue"].cumsum()
    return df

def calculate_mom_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Compute month-over-month revenue growth."""
    if df.empty:
        return pd.DataFrame(columns=["date", "identifier", "revenue", "quantity", "year_month", "mom_growth_pct"])

    monthly = (
        df.set_index("date")
        .groupby("identifier")[["revenue", "quantity"]]
        .resample("M")
        .sum()
        .reset_index()
    )
    monthly["year_month"] = monthly["date"].dt.strftime("%Y-%m")
    monthly["mom_growth_pct"] = monthly.groupby("identifier")["revenue"].pct_change() * 100
    return monthly

# -------------------------------------------------
# Charts Preparation
# -------------------------------------------------

def prepare_time_charts(df: pd.DataFrame, rolling_window: int):
    rolling_df = compute_rolling_avg(df.copy(), window=rolling_window)
    cumulative_df = compute_cumulative_revenue(df.copy())
    mom_df = calculate_mom_growth(df.copy())
    return rolling_df, cumulative_df, mom_df

# -------------------------------------------------
# Streamlit Application
# -------------------------------------------------

def main():
    st.set_page_config(page_title="DSS – Time Intelligence Dashboard", layout="wide")
    st.title("Week 12 – Dashboard Enhancement")
    st.caption("Date Intelligence & Growth Analysis")

    # Load date range from dim_date
    with st.spinner("Loading date boundaries..."):
        with get_connection() as conn:
            date_bounds = pd.read_sql_query(
                "SELECT MIN(full_date) AS min_d, MAX(full_date) AS max_d FROM dim_date", conn
            )

    min_date = pd.to_datetime(date_bounds["min_d"].iloc[0])
    max_date = pd.to_datetime(date_bounds["max_d"].iloc[0])

    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("End date", value=max_date, min_value=min_date, max_value=max_date)
    with col3:
        rolling_window = st.slider("Rolling window (days)", min_value=3, max_value=30, value=7)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        return

    # Load data
    with st.spinner("Loading sales data..."):
        raw_df = load_sales_time_series(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

    if raw_df.empty:
        st.warning("No data for selected period.")
        return

    base_df = fill_missing_dates(raw_df)

    # Sidebar filters
    st.sidebar.header("Filters")
    regions = ["All"] + sorted(base_df["region_name"].unique().tolist())
    selected_region = st.sidebar.selectbox("Region", regions)
    df_filtered = base_df if selected_region == "All" else base_df[base_df["region_name"] == selected_region]

    product_ids = sorted(df_filtered["product_id"].unique())
    selected_product_ids = st.sidebar.multiselect(
        "Product IDs",
        product_ids,
        default=product_ids[:5] if len(product_ids) > 5 else product_ids
    )

    if not selected_product_ids:
        st.info("Select at least one product ID.")
        return

    df_for_charts = df_filtered[df_filtered["product_id"].isin(selected_product_ids)].copy()

    # Prepare chart DataFrames
    rolling_df, cumulative_df, mom_df = prepare_time_charts(df_for_charts, rolling_window)

    # Rolling Average Chart
    st.subheader("Rolling Average – Revenue Trend")
    fig_roll = px.line(
        rolling_df, x="date", y="rolling_revenue", color="identifier",
        hover_data=["revenue", "quantity", "profit"],
        labels={"rolling_revenue": "Rolling Avg Revenue", "date": "Date", "identifier": "Product (Region)"}
    )
    st.plotly_chart(fig_roll, use_container_width=True)

    # Cumulative Revenue Chart
    st.subheader("Cumulative Revenue")
    fig_cum = px.bar(
        cumulative_df, x="date", y="cumulative_revenue", color="identifier", barmode="group",
        hover_data=["revenue", "quantity", "profit"],
        labels={"cumulative_revenue": "Cumulative Revenue", "date": "Date", "identifier": "Product (Region)"}
    )
    st.plotly_chart(fig_cum, use_container_width=True)

    # Month-over-Month Growth Chart
    st.subheader("Month-over-Month Growth (%)")
    fig_mom = px.line(
        mom_df, x="year_month", y="mom_growth_pct", color="identifier", markers=True,
        hover_data=["revenue", "quantity"],
        labels={"mom_growth_pct": "MoM Growth %", "year_month": "Month", "identifier": "Product (Region)"}
    )
    st.plotly_chart(fig_mom, use_container_width=True)

    # Analytical Notes
    with st.expander("Analytical Notes for Decision Makers"):
        st.markdown(
            """
- Rolling Average يقلل ضجيج التذبذب اليومي ويُظهر الاتجاه الحقيقي.
- Cumulative Revenue يوضح قوة المنتج على المدى الزمني.
- MoM Growth يكشف تسارع أو تراجع الطلب شهريًا.
- اختيار الفترات الزمنية يسمح بمقارنة مراحل تشغيل مختلفة بدقة.
"""
        )

if __name__ == "__main__":
    main()