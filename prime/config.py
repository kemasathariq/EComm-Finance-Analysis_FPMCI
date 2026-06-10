from pathlib import Path

BASE_DIR  = Path(__file__).parent.parent
DATA_PROC = BASE_DIR / "data" / "processed"
DATA_RAW  = BASE_DIR / "data" / "raw"
OUTPUT_DIR = Path(__file__).parent / "output"
MODEL_DIR  = Path(__file__).parent / "models"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MARTS = {
    "seller_acquisition":  DATA_PROC / "mart_seller_acquisition.csv",
    "customer_profile":    DATA_PROC / "mart_customer_payment_profile.csv",
    "seller_performance":  DATA_PROC / "mart_seller_performance.csv",
    "hv_category":         DATA_PROC / "mart_hv_category_preference.csv",
    "repeat_customer":     DATA_PROC / "mart_repeat_customer.csv",
    "payment_leakage":     DATA_PROC / "mart_payment_leakage.csv",
    "order_detail":        DATA_PROC / "mart_order_payment_detail.csv",
    "geo_summary":         DATA_PROC / "mart_geo_payment_summary.csv",
    "monthly_revenue":     DATA_PROC / "mart_monthly_revenue.csv",
    "product_price_range": DATA_PROC / "mart_product_price_range.csv",
    "review_by_segment":   DATA_PROC / "mart_review_by_segment.csv",
}

MQL_RAW = DATA_RAW / "mql.csv"

HVC_TOP_CATEGORIES = [
    "watches_gifts", "health_beauty", "computers_accessories",
    "sports_leisure", "bed_bath_table",
]

FRONTIER_STATES = {"SP", "MG", "RS", "PR", "RJ"}

HVC_SPEND_THRESHOLD    = 180.52
NATIONAL_AVG_ORDER_VALUE = 160.99
LEAKAGE_ALERT_THRESHOLD  = 5.0


def validate():
    missing = [name for name, path in MARTS.items() if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing mart files: {missing}")
