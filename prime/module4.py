import pandas as pd
import numpy as np
from config import (MARTS, OUTPUT_DIR, HVC_TOP_CATEGORIES,
                    FRONTIER_STATES, HVC_SPEND_THRESHOLD, validate)

CUSTOMER_TIERS   = [(5000, "Platinum"), (2000, "Gold"), (500, "Silver"), (0, "Bronze")]
SELLER_TIERS     = [(50_000, "Platinum"), (20_000, "Gold"), (5_000, "Silver"), (0, "Bronze")]
INSTALLMENT_MULT = {"1": 1.0, "2-6": 1.0, "7-12": 1.0, "12+": 2.0}
REVIEW_BONUS_PTS = 50


def assign_tier(points: float, tiers: list) -> str:
    for threshold, label in tiers:
        if points >= threshold:
            return label
    return tiers[-1][1]


def build_customer_points(df_profile, df_hv_cat, df_repeat):
    df = df_profile[[
        "customer_unique_id", "customer_state", "customer_city",
        "total_spend", "is_high_value", "preferred_payment_type",
        "installment_bucket", "max_installments_used",
        "uses_installments", "avg_review_score", "total_orders",
    ]].copy()

    df["base_points"]      = df["total_spend"].round(2)
    df["installment_mult"] = df["installment_bucket"].map(INSTALLMENT_MULT).fillna(1.0)

    hvc_cids = set(
        df_hv_cat[df_hv_cat["product_category_en"].isin(HVC_TOP_CATEGORIES)]["customer_unique_id"].unique()
    )
    df["category_mult"] = df["customer_unique_id"].apply(lambda cid: 1.5 if cid in hvc_cids else 1.0)
    df["frontier_mult"] = df["customer_state"].apply(lambda s: 1.2 if s in FRONTIER_STATES else 1.0)

    df["review_bonus"] = df.apply(
        lambda r: REVIEW_BONUS_PTS if r["avg_review_score"] > 0 and r["total_orders"] >= 1 else 0,
        axis=1,
    )

    df["total_points"] = (
        df["base_points"] * df["installment_mult"] * df["category_mult"] * df["frontier_mult"]
        + df["review_bonus"]
    ).round(0)

    df["tier"] = df["total_points"].apply(lambda p: assign_tier(p, CUSTOMER_TIERS))

    repeat_map = df_repeat.set_index("customer_unique_id")["is_repeat_customer"]
    df["is_repeat_customer"] = df["customer_unique_id"].map(repeat_map).fillna(0).astype(int)

    def audience(row):
        hv, rep, ib = int(row["is_high_value"]), int(row["is_repeat_customer"]), row["installment_bucket"]
        if hv == 1 and rep == 1: return "HVC Loyal"
        if hv == 1 and rep == 0: return "HVC One-Time"
        if hv == 0 and ib != "1": return "RC Upgrade Path"
        return "RC Single-Pay"

    df["marketing_audience"] = df.apply(audience, axis=1)

    tier_map = {"Bronze": 500, "Silver": 2000, "Gold": 5000, "Platinum": 5000}
    next_map  = {"Bronze": "Silver", "Silver": "Gold", "Gold": "Platinum", "Platinum": "Platinum"}
    df["next_tier"]      = df["tier"].map(next_map)
    df["points_to_next"] = df.apply(
        lambda r: max(0, tier_map[r["next_tier"]] - r["total_points"]), axis=1
    ).round(0)

    return df.sort_values("total_points", ascending=False).reset_index(drop=True)


def build_seller_points(df_seller):
    df = df_seller.copy()

    df["base_points"]   = (df["avg_review_score"] * 100 * df["total_orders"]).round(0)
    df["category_bonus"] = df["avg_order_value"].apply(lambda v: 1.3 if v > HVC_SPEND_THRESHOLD else 1.0)
    df["total_points"]   = (df["base_points"] * df["category_bonus"]).round(0)
    df["tier"]           = df["total_points"].apply(lambda p: assign_tier(p, SELLER_TIERS))

    df["revenue_tier"] = pd.cut(
        df["total_revenue"],
        bins=[-np.inf, 848, 3520, np.inf],
        labels=["Starter", "Growth", "Premium"],
    ).astype(str)

    ad_map = {
        "Starter": "Basic listing + onboarding analytics + category page placement",
        "Growth":  "Featured product slots + keyword-targeted display + enhanced analytics",
        "Premium": "Banner ads + HVC segment targeting + cross-sell placements + account manager",
    }
    df["ad_package"]         = df["revenue_tier"].map(ad_map)
    df["ad_credit_value_R$"] = (df["total_points"] * 0.10).round(2)

    next_map  = {"Bronze": "Silver", "Silver": "Gold", "Gold": "Platinum", "Platinum": "Platinum"}
    tier_thrs = {"Bronze": 5000, "Silver": 20000, "Gold": 50000, "Platinum": 50000}
    df["next_tier"]      = df["tier"].map(next_map)
    df["points_to_next"] = df.apply(
        lambda r: max(0, tier_thrs[r["next_tier"]] - r["total_points"]), axis=1
    ).round(0)

    return df.sort_values("total_points", ascending=False).reset_index(drop=True)


def build_tier_summary(cust_df, sell_df):
    rows = []
    for tier in ["Platinum", "Gold", "Silver", "Bronze"]:
        c = cust_df[cust_df["tier"] == tier]
        s = sell_df[sell_df["tier"] == tier]
        rows.append({
            "tier":                    tier,
            "customer_count":          len(c),
            "customer_pct":            round(len(c) / len(cust_df) * 100, 1),
            "avg_customer_points":     round(c["total_points"].mean(), 0) if len(c) else 0,
            "hvc_in_tier_pct":         round(c["is_high_value"].mean() * 100, 1) if len(c) else 0,
            "seller_count":            len(s),
            "seller_pct":              round(len(s) / len(sell_df) * 100, 1),
            "avg_seller_points":       round(s["total_points"].mean(), 0) if len(s) else 0,
            "avg_seller_ad_credit_R$": round(s["ad_credit_value_R$"].mean(), 2) if len(s) else 0,
        })
    return pd.DataFrame(rows)


def run():
    validate()
    df_profile = pd.read_csv(MARTS["customer_profile"])
    df_seller  = pd.read_csv(MARTS["seller_performance"])
    df_hv_cat  = pd.read_csv(MARTS["hv_category"])
    df_repeat  = pd.read_csv(MARTS["repeat_customer"])

    print("=" * 65)
    print("  PRIME MODULE 4 - RETENTION AND POINTS ENGINE")
    print("=" * 65)

    cust_df = build_customer_points(df_profile, df_hv_cat, df_repeat)
    sell_df = build_seller_points(df_seller)
    summary = build_tier_summary(cust_df, sell_df)

    print(f"\nCUSTOMER TIERS (n={len(cust_df):,})")
    for tier, cnt in cust_df["tier"].value_counts().reindex(["Platinum", "Gold", "Silver", "Bronze"]).items():
        print(f"  {tier:10s}: {cnt:>8,}  ({cnt / len(cust_df) * 100:>5.1f}%)")

    print("\nMARKETING AUDIENCES")
    for aud, cnt in cust_df["marketing_audience"].value_counts().items():
        print(f"  {aud:20s}: {cnt:>8,}  ({cnt / len(cust_df) * 100:>5.1f}%)")

    print("\nINSTALLMENT MULTIPLIER IMPACT")
    print(cust_df.groupby("installment_bucket").agg(
        customers        = ("customer_unique_id", "count"),
        avg_base_points  = ("base_points",        "mean"),
        avg_total_points = ("total_points",        "mean"),
    ).round(1).to_string())

    print(f"\nSELLER TIERS (n={len(sell_df):,})")
    for tier, cnt in sell_df["tier"].value_counts().reindex(["Platinum", "Gold", "Silver", "Bronze"]).items():
        avg_cr = sell_df[sell_df["tier"] == tier]["ad_credit_value_R$"].mean()
        print(f"  {tier:10s}: {cnt:>5}  ({cnt / len(sell_df) * 100:>5.1f}%)  avg ad credit: R${avg_cr:>8.2f}")

    print("\nCOMBINED TIER SUMMARY")
    print(summary.to_string(index=False))

    cust_df.to_csv(OUTPUT_DIR / "customer_points.csv", index=False)
    sell_df.to_csv(OUTPUT_DIR / "seller_points.csv",   index=False)
    summary.to_csv(OUTPUT_DIR / "tier_summary.csv",    index=False)
    print(f"\nSaved to {OUTPUT_DIR}/")
    return cust_df, sell_df, summary


if __name__ == "__main__":
    run()
