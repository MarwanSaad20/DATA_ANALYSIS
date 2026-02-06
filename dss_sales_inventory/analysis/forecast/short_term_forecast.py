import os
import logging
import numpy as np
import pandas as pd
from datetime import timedelta
from typing import List

# ------------------------------------------------------------------
# استخدام نفس dss_logger الموحد من المشروع (دون إعادة تهيئة basicConfig)
# ------------------------------------------------------------------
dss_logger = logging.getLogger("dss_logger")

# ------------------------------------------------------------------
# إعداد المسارات النسبية (محدث للـ View الجديد)
# ------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

VIEW_PATH = os.path.join(PROJECT_ROOT, "reporting", "outputs", "daily_product_sales_view.csv")
FORECAST_OUTPUT_PATH = os.path.join(PROJECT_ROOT, "reporting", "outputs", "forecast_results.csv")
EVAL_MD_PATH = os.path.join(PROJECT_ROOT, "analysis", "forecast", "forecast_evaluation.md")
FORECAST_DIR = os.path.join(PROJECT_ROOT, "analysis", "forecast")

os.makedirs(os.path.dirname(FORECAST_OUTPUT_PATH), exist_ok=True)
os.makedirs(FORECAST_DIR, exist_ok=True)

# ------------------------------------------------------------------
# دالة Logging موحدة تماماً مع باقي المشروع
# ------------------------------------------------------------------
def log_message(message: str, level: str = "INFO", correlation_id: str = "", stage: str = "SHORT_TERM_FORECAST", function: str = "run_short_term_forecast"):
    extra = {
        "run_id": correlation_id,
        "stage": stage,
        "function": function,
        "rows_in": None,
        "rows_out": None,
        "status": "INFO" if level == "INFO" else level
    }
    if level == "INFO":
        dss_logger.info(message, extra=extra)
    elif level == "WARNING":
        dss_logger.warning(message, extra=extra)
    elif level == "ERROR":
        dss_logger.error(message, extra=extra)

# ------------------------------------------------------------------
# الدوال الإلزامية
# ------------------------------------------------------------------

def fit_model(series: pd.Series) -> dict:
    """
    Holt's Linear Exponential Smoothing (Double Exponential Smoothing)
    مع grid search بسيط لأفضل alpha و beta بناءً على in-sample MAE.
    """
    if len(series) < 2:
        raise ValueError("Series too short for fitting Holt's model")

    alphas = np.round(np.arange(0.1, 1.0, 0.2), 2)
    betas = np.round(np.arange(0.05, 0.51, 0.1), 2)

    best_mae = np.inf
    best_alpha = 0.3
    best_beta = 0.1
    best_level = float(series.iloc[0])
    best_trend = 0.0

    for alpha in alphas:
        for beta in betas:
            level = float(series.iloc[0])
            trend = 0.0
            forecasts = []

            for i in range(1, len(series)):
                prev_level = level
                level = alpha * series.iloc[i] + (1 - alpha) * (level + trend)
                trend = beta * (level - prev_level) + (1 - beta) * trend
                forecast = prev_level + trend
                forecasts.append(forecast)

            mae = np.mean(np.abs(series.iloc[1:].values - np.array(forecasts)))

            if mae < best_mae:
                best_mae = mae
                best_alpha = alpha
                best_beta = beta
                # Refit final level/trend
                level = float(series.iloc[0])
                trend = 0.0
                for i in range(1, len(series)):
                    prev_level = level
                    level = best_alpha * series.iloc[i] + (1 - best_alpha) * (level + trend)
                    trend = best_beta * (level - prev_level) + (1 - best_beta) * trend
                best_level = level
                best_trend = trend

    return {
        "alpha": best_alpha,
        "beta": best_beta,
        "level": best_level,
        "trend": best_trend,
        "mae": best_mae
    }

def generate_forecast(model: dict, steps: int = 28) -> List[float]:
    """
    توليد توقع 28 يومًا باستخدام Holt's Linear (لا قيم سالبة).
    """
    forecasts = []
    level = model["level"]
    trend = model["trend"]
    for h in range(1, steps + 1):
        f = level + h * trend
        forecasts.append(max(0.0, f))
    return forecasts

def evaluate_error(actual: pd.Series, predicted: pd.Series) -> float:
    """
    Mean Absolute Error (MAE)
    """
    return float(np.mean(np.abs(actual.values - predicted.values)))

# ------------------------------------------------------------------
# الدالة الرئيسية
# ------------------------------------------------------------------

def run_short_term_forecast(correlation_id: str) -> None:
    """
    طبقة التوقع قصير المدى (28 يومًا) بناءً على daily_product_sales_view.csv
    """
    log_message("Short-Term Forecast Layer started", "INFO", correlation_id)

    if not os.path.exists(VIEW_PATH):
        log_message(f"View file not found: {VIEW_PATH}", "ERROR", correlation_id)
        return

    try:
        df = pd.read_csv(VIEW_PATH)
    except Exception as e:
        log_message(f"Failed to read view file: {str(e)}", "ERROR", correlation_id)
        return

    REQUIRED_COLUMNS = ["product_id", "date", "quantity_sold"]
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        log_message(f"Missing required columns: {missing_cols}. Available columns: {list(df.columns)}", "ERROR", correlation_id)
        return

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["product_id", "date"])

    results_dfs = []
    eval_entries = []
    skipped_products = []

    for product_id, group in df.groupby("product_id"):
        series_df = group.set_index("date")["quantity_sold"]
        series = series_df.sort_index().asfreq("D").fillna(0)  # ملء الأيام المفقودة بـ 0

        if len(series) < 14:
            skipped_products.append((product_id, len(series)))
            log_message(f"Skipping product {product_id} - insufficient data ({len(series)} days)", "WARNING", correlation_id)
            continue

        try:
            model = fit_model(series)
            last_date = series.index.max()
            future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=28, freq="D")
            forecasts = generate_forecast(model, steps=28)

            weeks = [(i // 7) + 1 for i in range(28)]

            product_df = pd.DataFrame({
                "product_id": [product_id] * 28,
                "forecast_week": weeks,
                "forecast_date": future_dates,
                "forecast_quantity": forecasts,
                "model_type": ["Holt's Linear Exponential Smoothing"] * 28
            })

            results_dfs.append(product_df)

            mean_quantity = series.mean()
            reliability = ("Good reliability for short-term" 
                           if model["mae"] < mean_quantity * 0.3 
                           else "Limited reliability - monitor closely")

            eval_entries.append({
                "product_id": product_id,
                "points": len(series),
                "alpha": model["alpha"],
                "beta": model["beta"],
                "mae": model["mae"],
                "reliability": reliability
            })

            log_message(f"Forecast completed for product {product_id} (MAE={model['mae']:.2f})", "INFO", correlation_id)

        except Exception as e:
            log_message(f"Soft-fail for product {product_id}: {str(e)}", "WARNING", correlation_id)
            continue

    # كتابة النتائج
    if results_dfs:
        final_results = pd.concat(results_dfs, ignore_index=True)
        final_results.to_csv(FORECAST_OUTPUT_PATH, index=False)
        log_message(f"Forecast results saved to {FORECAST_OUTPUT_PATH}", "INFO", correlation_id)
    else:
        log_message("No products processed - forecast_results.csv not created", "WARNING", correlation_id)

    # كتابة تقرير التقييم (يبقى كما هو)
    with open(EVAL_MD_PATH, "w", encoding="utf-8") as f:
        f.write("# Short-Term Forecast Evaluation Report\n\n")
        f.write("## Processed Products\n\n")
        for entry in eval_entries:
            f.write(f"### Product {entry['product_id']}\n")
            f.write(f"- Number of historical data points: {entry['points']}\n")
            f.write(f"- Model: Holt's Linear Exponential Smoothing (α={entry['alpha']:.2f}, β={entry['beta']:.2f})\n")
            f.write(f"- In-sample MAE: {entry['mae']:.2f}\n")
            f.write(f"- Reliability comment: {entry['reliability']}\n\n")

        if skipped_products:
            f.write("## Skipped Products (Insufficient Data)\n\n")
            for pid, count in skipped_products:
                f.write(f"- Product {pid}: only {count} days available (< 14)\n")

        f.write("## Model Limitations\n\n")
        f.write("- Holt's Linear method captures level and linear trend but assumes no seasonality.\n")
        f.write("- May over/under-estimate if recent data shows non-linear changes or external shocks.\n")
        f.write("- Forecast quantity forced ≥ 0; negative trend extrapolations are clipped.\n")
        f.write("- Short-term (28 days) only - not suitable for long-term strategic planning.\n")

    log_message(f"Evaluation report saved to {EVAL_MD_PATH}", "INFO", correlation_id)
    log_message("Short-Term Forecast Layer completed", "INFO", correlation_id)