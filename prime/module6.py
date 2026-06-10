"""
PRIME Module 6 - Market Timing Engine
======================================
Input  : mart_monthly_revenue    (monthly order totals by payment type)
         mart_product_price_range (monthly revenue by category + price bucket)

Outputs:
  campaign_calendar.csv   - 12-month intensity calendar with pre-launch windows
  category_timing.csv     - best campaign months per product category
  flash_sale_windows.csv  - high-intensity months flagged for flash sale execution

Logic:
  1. Aggregate total monthly revenue across all payment types
  2. Score each calendar month 1-10 via percentile rank across the 24-month dataset
  3. Map Brazilian seasonal events per calendar month (Jan-Dec)
  4. Pre-launch window: 21 days (score >=8), 14 days (score 5-7), 7 days (<5)
  5. Flash sale candidates: months with intensity score >= 7

Run: python prime/module6.py
"""

import pandas as pd
import numpy as np
from config import MARTS, OUTPUT_DIR, HVC_TOP_CATEGORIES, validate

# Brazilian e-commerce seasonal events mapped to calendar month (1-12)
BR_EVENTS = {
    1:  "Liquidação de Janeiro / Summer clearance",
    2:  "Carnaval season - electronics + fashion pre-event",
    3:  "Back-to-school close / Easter prep",
    4:  "Páscoa (Easter) - chocolate, home, gifts",
    5:  "Dia das Mães (2nd Sunday) - biggest gifting event after BF",
    6:  "Dia dos Namorados (June 12) - beauty, jewelry, electronics",
    7:  "Férias de julho - travel, toys, leisure",
    8:  "Dia dos Pais (2nd Sunday) - electronics, tools, sports",
    9:  "Volta às aulas / Spring promo",
    10: "Dia das Crianças (Oct 12) - toys, games, apparel",
    11: "Black Friday - highest sales event of the year",
    12: "Natal / Christmas + end-of-year clearance",
}

# How many days before peak to launch campaign per intensity band
LAUNCH_WINDOW_DAYS = {
    "HIGH":   21,
    "MEDIUM": 14,
    "LOW":    7,
}

FLASH_SALE_THRESHOLD = 7   # intensity score >= this gets flash sale flag


def score_months(monthly_totals: pd.Series) -> pd.Series:
    """
    Scale monthly revenue to 1-10 intensity score using percentile rank.
    Uses the observed 24-month distribution so the top month always = 10.
    """
    ranks = monthly_totals.rank(pct=True)
    return (ranks * 9 + 1).round(1)


def intensity_band(score: float) -> str:
    if score >= 8:
        return "HIGH"
    if score >= 5:
        return "MEDIUM"
    return "LOW"


def build_campaign_calendar(df_monthly: pd.DataFrame) -> pd.DataFrame:
    # Aggregate across payment types to get total monthly revenue
    monthly = (
        df_monthly.groupby("year_month", as_index=False)
        .agg(total_revenue=("total_revenue", "sum"), order_count=("order_count", "sum"))
    )
    monthly["year_month_dt"] = pd.to_datetime(monthly["year_month"] + "-01")
    monthly["calendar_month"] = monthly["year_month_dt"].dt.month

    # Score by percentile across all observed months
    monthly["intensity_score"] = score_months(monthly["total_revenue"])
    monthly["intensity_band"]  = monthly["intensity_score"].apply(intensity_band)

    # Aggregate by calendar month (average across years for a repeatable annual pattern)
    cal = (
        monthly.groupby("calendar_month")
        .agg(
            avg_revenue      = ("total_revenue",    "mean"),
            avg_orders       = ("order_count",      "mean"),
            avg_intensity    = ("intensity_score",   "mean"),
            data_points      = ("year_month",        "count"),
        )
        .reset_index()
        .round({"avg_revenue": 0, "avg_orders": 0, "avg_intensity": 1})
    )

    cal["intensity_score"]    = cal["avg_intensity"].apply(lambda x: round(x, 1))
    cal["intensity_band"]     = cal["intensity_score"].apply(intensity_band)
    cal["seasonal_event"]     = cal["calendar_month"].map(BR_EVENTS)
    cal["pre_launch_days"]    = cal["intensity_band"].map(LAUNCH_WINDOW_DAYS)
    cal["flash_sale_flag"]    = cal["intensity_score"] >= FLASH_SALE_THRESHOLD
    cal["month_name"]         = pd.to_datetime(cal["calendar_month"].astype(str), format="%m").dt.strftime("%B")
    cal["campaign_budget_weight"] = (cal["intensity_score"] / cal["intensity_score"].sum() * 100).round(1)

    cols = [
        "calendar_month", "month_name", "intensity_score", "intensity_band",
        "seasonal_event", "pre_launch_days", "flash_sale_flag",
        "avg_revenue", "avg_orders", "campaign_budget_weight", "data_points",
    ]
    return cal[cols].sort_values("calendar_month").reset_index(drop=True)


def build_category_timing(df_price: pd.DataFrame) -> pd.DataFrame:
    """
    For each HVC category, find the top 3 months by total revenue.
    This tells the marketing team WHEN to push each category.
    """
    df = df_price.copy()
    df["calendar_month"] = pd.to_datetime(df["year_month"] + "-01").dt.month

    cat_monthly = (
        df.groupby(["product_category_en", "calendar_month"])
        .agg(total_revenue=("total_revenue", "sum"), order_count=("order_count", "sum"))
        .reset_index()
    )

    rows = []
    for cat in HVC_TOP_CATEGORIES:
        sub = cat_monthly[cat_monthly["product_category_en"] == cat].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("total_revenue", ascending=False)
        peak_months  = sub.head(3)["calendar_month"].tolist()
        peak_months_named = [pd.Timestamp(f"2000-{m:02d}-01").strftime("%B") for m in peak_months]
        rows.append({
            "category":          cat,
            "peak_months":       ", ".join(peak_months_named),
            "peak_month_nums":   peak_months,
            "top_month_revenue": round(sub.iloc[0]["total_revenue"], 0),
            "top_month":         pd.Timestamp(f"2000-{sub.iloc[0]['calendar_month']:02d}-01").strftime("%B"),
            "seasonal_event":    BR_EVENTS.get(peak_months[0], ""),
        })

    return pd.DataFrame(rows)


def build_flash_sale_windows(calendar: pd.DataFrame) -> pd.DataFrame:
    flash = calendar[calendar["flash_sale_flag"]].copy()
    flash["flash_sale_type"]    = flash["intensity_score"].apply(
        lambda s: "Major Flash Event (48-72h)" if s >= 9 else "Standard Flash Sale (24h)"
    )
    flash["recommended_window"] = flash["intensity_band"].map({
        "HIGH":   "Week 2 and Week 4 of the month",
        "MEDIUM": "Week 3 of the month",
    })
    flash["channel_priority"] = flash["intensity_score"].apply(
        lambda s: "Push + Email + Display + Retargeting" if s >= 9
                  else "Email + Display"
    )
    return flash[[
        "calendar_month", "month_name", "intensity_score",
        "seasonal_event", "flash_sale_type",
        "recommended_window", "channel_priority", "campaign_budget_weight",
    ]].reset_index(drop=True)


def run():
    validate()
    df_monthly = pd.read_csv(MARTS["monthly_revenue"])
    df_price   = pd.read_csv(MARTS["product_price_range"])

    print("=" * 65)
    print("  PRIME MODULE 6 - MARKET TIMING ENGINE")
    print("=" * 65)
    print(f"\n  Monthly revenue rows : {len(df_monthly)}")
    print(f"  Date range           : {df_monthly['year_month'].min()} to {df_monthly['year_month'].max()}")
    print(f"  Price range rows     : {len(df_price)}")
    print(f"  HVC categories       : {HVC_TOP_CATEGORIES}")

    calendar = build_campaign_calendar(df_monthly)

    print("\nCAMPAIGN INTENSITY CALENDAR (annual pattern)")
    print("-" * 65)
    display_cols = [
        "calendar_month", "month_name", "intensity_score", "intensity_band",
        "pre_launch_days", "flash_sale_flag", "campaign_budget_weight", "seasonal_event",
    ]
    print(calendar[display_cols].to_string(index=False))

    category_timing = build_category_timing(df_price)
    print("\nHVC CATEGORY TIMING (top 3 peak months per category)")
    print("-" * 65)
    print(category_timing[["category", "top_month", "peak_months", "seasonal_event"]].to_string(index=False))

    flash_windows = build_flash_sale_windows(calendar)
    print("\nFLASH SALE WINDOWS")
    print("-" * 65)
    print(flash_windows[["month_name", "intensity_score", "flash_sale_type",
                          "recommended_window", "channel_priority"]].to_string(index=False))

    print("\nBUDGET ALLOCATION SUMMARY")
    print("-" * 65)
    for band in ["HIGH", "MEDIUM", "LOW"]:
        sub = calendar[calendar["intensity_band"] == band]
        months = ", ".join(sub["month_name"].tolist())
        budget = sub["campaign_budget_weight"].sum()
        print(f"  {band:6s}: {len(sub)} months | {budget:.1f}% budget | {months}")

    calendar.to_csv(OUTPUT_DIR / "campaign_calendar.csv", index=False)
    category_timing.to_csv(OUTPUT_DIR / "category_timing.csv", index=False)
    flash_windows.to_csv(OUTPUT_DIR / "flash_sale_windows.csv", index=False)

    print(f"\nSaved to {OUTPUT_DIR}/")
    return calendar, category_timing, flash_windows


if __name__ == "__main__":
    run()
