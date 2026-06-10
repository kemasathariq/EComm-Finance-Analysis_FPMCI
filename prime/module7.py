"""
PRIME Module 7 - User-Based Marketing Engine
=============================================
Input  : mart_customer_payment_profile (94,401 customers)
         mart_repeat_customer          (repeat + recency flags)
         mart_hv_category_preference   (HVC customers top category)

Outputs:
  trigger_segments.csv  - 5 short-term behavioral triggers per customer
  audience_segments.csv - 4 long-term audience buckets with strategy
  segment_summary.csv   - aggregate size + strategy per segment

Short-term triggers (event-based, fire at checkout or post-order):
  T1  post_purchase_review   : order completed, no review follow-up signal
  T2  installment_upgrade     : boleto user with avg order above HVC threshold
  T3  payment_anomaly         : voucher/unknown payment on high-value orders
  T4  re_engagement           : no order in last 90 days, single-purchase
  T5  hvc_upsell              : HVC + single-pay + avg order > threshold

Long-term audiences (feed to CRM / email campaign tool):
  A1  hvc_loyal               : is_high_value=1, is_repeat=1
  A2  hvc_one_time            : is_high_value=1, is_repeat=0
  A3  rc_upgrade_path         : is_repeat=1, not high value
  A4  rc_single_pay           : is_repeat=0, uses_installments=0, not high value

Run: python prime/module7.py
"""

import pandas as pd
import numpy as np
from config import MARTS, OUTPUT_DIR, HVC_SPEND_THRESHOLD, HVC_TOP_CATEGORIES, validate

# Days with no new order before re-engagement trigger fires
RE_ENGAGEMENT_DAYS = 90

# Audience strategies (for output documentation)
AUDIENCE_STRATEGIES = {
    "hvc_loyal": (
        "Exclusive early access to Black Friday + Dia das Mães campaigns. "
        "12-installment option pre-surfaced. Dedicated retention email cadence (bi-weekly)."
    ),
    "hvc_one_time": (
        "Win-back campaign at 30d post-purchase. "
        "Personalized category recommendation based on first-order category. "
        "Incentivize second order with 10% category voucher."
    ),
    "rc_upgrade_path": (
        "Upgrade to installment plan at next checkout. "
        "Show HVC threshold progress ('R$X away from HVC status'). "
        "Target with mid-tier category upsell (C. 150-500 bucket)."
    ),
    "rc_single_pay": (
        "Credit card + installment education email. "
        "Low-value promotional entry offer to encourage repeat. "
        "Re-engagement trigger at 90 days inactivity."
    ),
}

TRIGGER_ACTIONS = {
    "post_purchase_review": (
        "Send review request email 5 days after estimated delivery. "
        "If review score >= 4, surface upsell recommendation."
    ),
    "installment_upgrade": (
        "At next checkout: display credit card + 6-installment option side-by-side with boleto. "
        "Show monthly installment value prominently."
    ),
    "payment_anomaly": (
        "Flag to customer ops within 24h. "
        "Send payment method clarification email. "
        "Offer credit card as preferred fallback."
    ),
    "re_engagement": (
        "Day 90: re-engagement email with top category deals. "
        "Day 105: SMS/push with discount code. "
        "Day 120: final win-back offer before suppression."
    ),
    "hvc_upsell": (
        "Surface 12-installment plan at checkout. "
        "Add loyalty point multiplier badge to product page. "
        "Invite to HVC early-access sale event."
    ),
}


# TRIGGER SEGMENTATION
def build_triggers(df_profile: pd.DataFrame, df_repeat: pd.DataFrame) -> pd.DataFrame:
    # Merge on customer_unique_id for recency and repeat signals
    df = df_profile.merge(
        df_repeat[["customer_unique_id", "is_repeat_customer", "days_active", "last_order_date"]],
        on="customer_unique_id",
        how="left",
    )

    # T1: post-purchase review - all completed orders (proxy: anyone with total_orders >= 1)
    # In production this would be triggered per order event; here we flag eligible customers
    df["T1_post_purchase_review"] = (df["total_orders"] >= 1).astype(int)

    # T2: boleto user with avg order above HVC threshold - likely better off on credit card installment
    df["T2_installment_upgrade"] = (
        (df["preferred_payment_type"] == "boleto")
        & (df["avg_order_value"] > HVC_SPEND_THRESHOLD)
    ).astype(int)

    # T3: payment anomaly - voucher or unknown payment type used on orders with above-avg spend
    anomaly_types = {"voucher"}
    df["T3_payment_anomaly"] = (
        df["preferred_payment_type"].isin(anomaly_types)
        & (df["avg_order_value"] > df["avg_order_value"].median())
    ).astype(int)

    # T4: re-engagement - single purchase, no activity in RE_ENGAGEMENT_DAYS days
    df["T4_re_engagement"] = (
        (df["is_repeat_customer"] == 0)
        & (df["days_active"] == 0)
        & (df["total_orders"] == 1)
    ).astype(int)

    # T5: HVC upsell - high value, single-pay (no installments), above threshold
    df["T5_hvc_upsell"] = (
        (df["is_high_value"] == 1)
        & (df["uses_installments"] == 0)
        & (df["avg_order_value"] > HVC_SPEND_THRESHOLD)
    ).astype(int)

    trigger_cols = [
        "T1_post_purchase_review", "T2_installment_upgrade",
        "T3_payment_anomaly", "T4_re_engagement", "T5_hvc_upsell",
    ]
    df["active_triggers"]     = df[trigger_cols].sum(axis=1)
    df["primary_trigger"]     = df[trigger_cols].idxmax(axis=1).where(df["active_triggers"] > 0, "none")
    df["trigger_action"]      = df["primary_trigger"].map(
        lambda t: TRIGGER_ACTIONS.get(t.replace("T1_", "").replace("T2_", "").replace("T3_", "")
                                       .replace("T4_", "").replace("T5_", ""), "No action")
    )

    return df, trigger_cols


# AUDIENCE SEGMENTATION
def build_audiences(df_profile: pd.DataFrame, df_repeat: pd.DataFrame,
                    df_hvc_cat: pd.DataFrame) -> pd.DataFrame:
    df = df_profile.merge(
        df_repeat[["customer_unique_id", "is_repeat_customer"]],
        on="customer_unique_id",
        how="left",
    )

    # Top category per HVC customer (for personalized recommendations)
    top_cat = (
        df_hvc_cat.sort_values("total_spend", ascending=False)
        .drop_duplicates("customer_unique_id")
        [["customer_unique_id", "product_category_en"]]
        .rename(columns={"product_category_en": "top_category"})
    )
    df = df.merge(top_cat, on="customer_unique_id", how="left")
    df["top_category"] = df["top_category"].fillna("unknown")

    # Mutually exclusive audience assignment (priority order: A1 > A2 > A3 > A4)
    conditions = [
        (df["is_high_value"] == 1) & (df["is_repeat_customer"] == 1),
        (df["is_high_value"] == 1) & (df["is_repeat_customer"] == 0),
        (df["is_repeat_customer"] == 1) & (df["is_high_value"] == 0),
    ]
    choices = ["hvc_loyal", "hvc_one_time", "rc_upgrade_path"]
    df["audience_segment"] = np.select(conditions, choices, default="rc_single_pay")

    df["audience_strategy"] = df["audience_segment"].map(AUDIENCE_STRATEGIES)

    # HVC category flag: is their top category one of the HVC categories?
    df["is_hvc_category"] = df["top_category"].isin(HVC_TOP_CATEGORIES).astype(int)

    return df


# MAIN
def run():
    validate()
    df_profile = pd.read_csv(MARTS["customer_profile"])
    df_repeat  = pd.read_csv(MARTS["repeat_customer"])
    df_hvc_cat = pd.read_csv(MARTS["hv_category"])

    print("=" * 65)
    print("  PRIME MODULE 7 - USER-BASED MARKETING ENGINE")
    print("=" * 65)
    print(f"\n  Customers          : {len(df_profile):,}")
    print(f"  Repeat customers   : {df_repeat['is_repeat_customer'].sum():,}")
    print(f"  HV customers       : {df_profile['is_high_value'].sum():,}")
    print(f"  HV category rows   : {len(df_hvc_cat):,}")

    # TRIGGERS
    df_triggers, trigger_cols = build_triggers(df_profile, df_repeat)

    print("\nSHORT-TERM TRIGGER SUMMARY")
    print("-" * 65)
    trigger_labels = {
        "T1_post_purchase_review": "T1 Post-purchase review",
        "T2_installment_upgrade":  "T2 Installment upgrade",
        "T3_payment_anomaly":      "T3 Payment anomaly",
        "T4_re_engagement":        "T4 Re-engagement",
        "T5_hvc_upsell":           "T5 HVC upsell",
    }
    for col, label in trigger_labels.items():
        cnt = df_triggers[col].sum()
        print(f"  {label:30s}: {cnt:>8,} ({cnt/len(df_triggers)*100:.1f}%)")

    print(f"\n  Customers with >= 1 trigger : {(df_triggers['active_triggers'] > 0).sum():,}")
    print(f"  Customers with >= 2 triggers: {(df_triggers['active_triggers'] > 1).sum():,}")

    # AUDIENCES
    df_audiences = build_audiences(df_profile, df_repeat, df_hvc_cat)

    print("\nLONG-TERM AUDIENCE SEGMENTS")
    print("-" * 65)
    seg_dist = df_audiences["audience_segment"].value_counts()
    for seg in ["hvc_loyal", "hvc_one_time", "rc_upgrade_path", "rc_single_pay"]:
        cnt = seg_dist.get(seg, 0)
        print(f"  {seg:20s}: {cnt:>8,} ({cnt/len(df_audiences)*100:.1f}%)")

    print("\nSEGMENT STRATEGIES")
    print("-" * 65)
    for seg, strategy in AUDIENCE_STRATEGIES.items():
        print(f"\n  [{seg.upper()}]")
        print(f"  {strategy[:110]}")

    print("\nHVC LOYAL - TOP CATEGORIES")
    print("-" * 65)
    hvc_loyal = df_audiences[df_audiences["audience_segment"] == "hvc_loyal"]
    cat_dist = hvc_loyal["top_category"].value_counts().head(5)
    for cat, cnt in cat_dist.items():
        print(f"  {cat:35s}: {cnt:>5,}")

    # OUTPUTS
    trigger_output_cols = [
        "customer_unique_id", "customer_state", "is_high_value",
        "preferred_payment_type", "avg_order_value", "total_orders",
    ] + trigger_cols + ["active_triggers", "primary_trigger", "trigger_action"]

    audience_output_cols = [
        "customer_unique_id", "customer_state", "is_high_value",
        "is_repeat_customer", "total_spend", "avg_order_value",
        "preferred_payment_type", "uses_installments",
        "top_category", "is_hvc_category",
        "audience_segment", "audience_strategy",
    ]

    # Segment summary for reporting
    segment_summary = (
        df_audiences.groupby("audience_segment")
        .agg(
            customer_count     = ("customer_unique_id", "count"),
            avg_spend          = ("total_spend",        "mean"),
            avg_order_value    = ("avg_order_value",    "mean"),
            pct_credit_card    = ("preferred_payment_type",
                                  lambda x: (x == "credit_card").mean() * 100),
            pct_installments   = ("uses_installments",  "mean"),
            pct_hvc_category   = ("is_hvc_category",    "mean"),
        )
        .round({"avg_spend": 0, "avg_order_value": 1, "pct_credit_card": 1,
                "pct_installments": 3, "pct_hvc_category": 3})
        .reset_index()
        .sort_values("customer_count", ascending=False)
    )
    segment_summary["strategy"] = segment_summary["audience_segment"].map(AUDIENCE_STRATEGIES)

    print("\nSEGMENT SUMMARY TABLE")
    print("-" * 65)
    print(segment_summary[[
        "audience_segment", "customer_count", "avg_spend",
        "avg_order_value", "pct_credit_card", "pct_installments",
    ]].to_string(index=False))

    df_triggers[trigger_output_cols].to_csv(OUTPUT_DIR / "trigger_segments.csv", index=False)
    df_audiences[audience_output_cols].to_csv(OUTPUT_DIR / "audience_segments.csv", index=False)
    segment_summary.to_csv(OUTPUT_DIR / "segment_summary.csv", index=False)

    print(f"\nSaved to {OUTPUT_DIR}/")
    return df_triggers, df_audiences, segment_summary


if __name__ == "__main__":
    run()
