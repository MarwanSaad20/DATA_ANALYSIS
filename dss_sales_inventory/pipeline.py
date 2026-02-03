import uuid
import logging
import sys

from ingestion.ingestion import run_ingestion
from cleaning.cleaning import run_cleaning
from features.features import run_features
from analysis.analysis import run_analysis

# ========================
# Logger configuration
# ========================
dss_logger = logging.getLogger('dss_logger')
dss_logger.setLevel(logging.INFO)

# Console handler لإظهار الرسائل على الشاشة
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# Formatter بسيط للعرض
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(stage)s] [%(run_id)s] [%(function)s] %(message)s"
)
console_handler.setFormatter(formatter)

# إضافة الـ handler للـ logger إذا لم يكن موجوداً
if not dss_logger.handlers:
    dss_logger.addHandler(console_handler)

# ========================
# Main pipeline
# ========================
if __name__ == "__main__":
    """
    Main entry point for the pipeline. Orchestrates execution of all stages in strict sequential order,
    passes correlation_id for traceability, handles errors globally, and ensures complete logging.
    """
    correlation_id = str(uuid.uuid4())

    # Log pipeline start
    dss_logger.info(
        "Pipeline started",
        extra={
            "run_id": correlation_id,
            "stage": "PIPELINE",
            "function": "main",
            "rows_in": None,
            "rows_out": None,
            "status": "STARTED"
        }
    )

    data = None

    try:
        # ========================
        # Stage 1: Ingestion
        # ========================
        dss_logger.info(
            "Ingestion stage started",
            extra={
                "run_id": correlation_id,
                "stage": "INGESTION",
                "function": "run_ingestion",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        data = run_ingestion(correlation_id)
        dss_logger.info(
            "Ingestion stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "INGESTION",
                "function": "run_ingestion",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 2: Cleaning
        # ========================
        dss_logger.info(
            "Cleaning stage started",
            extra={
                "run_id": correlation_id,
                "stage": "CLEANING",
                "function": "run_cleaning",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        data = run_cleaning(data, correlation_id)
        dss_logger.info(
            "Cleaning stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "CLEANING",
                "function": "run_cleaning",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 3: Features
        # ========================
        dss_logger.info(
            "Features stage started",
            extra={
                "run_id": correlation_id,
                "stage": "FEATURES",
                "function": "run_features",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        data = run_features(data, correlation_id)
        dss_logger.info(
            "Features stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "FEATURES",
                "function": "run_features",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # ========================
        # Stage 4: Analysis
        # ========================
        dss_logger.info(
            "Analysis stage started",
            extra={
                "run_id": correlation_id,
                "stage": "ANALYSIS",
                "function": "run_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "STARTED"
            }
        )
        run_analysis(data, correlation_id)
        dss_logger.info(
            "Analysis stage completed",
            extra={
                "run_id": correlation_id,
                "stage": "ANALYSIS",
                "function": "run_analysis",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

        # Log pipeline success
        dss_logger.info(
            "Pipeline completed successfully",
            extra={
                "run_id": correlation_id,
                "stage": "PIPELINE",
                "function": "main",
                "rows_in": None,
                "rows_out": None,
                "status": "SUCCESS"
            }
        )

    except Exception as e:
        dss_logger.error(
            f"Pipeline failed with error: {str(e)}",
            extra={
                "run_id": correlation_id,
                "stage": "PIPELINE",
                "function": "main",
                "rows_in": None,
                "rows_out": None,
                "status": "FAILED"
            },
            exc_info=True
        )
        sys.exit(1)
