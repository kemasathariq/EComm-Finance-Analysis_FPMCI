"""
PRIME Module 3 - K-means Seller Tier Segmenter
===============================================
Input  : mart_seller_performance  (2,978 sellers)
         mart_seller_acquisition  (lead behaviour profile -> onboarding map)
Steps  :
  1. Log-transform total_revenue (right-skewed: mean R$4,490, max R$229,237)
  2. StandardScaler on all four clustering features
  3. Validate k=3 with elbow (inertia) + silhouette score for k=2..6
  4. K-means k=3, map clusters to Starter/Growth/Premium by revenue centroid
  5. Assign ad package per tier, HVC alignment flag
  6. Map lead_behaviour_profile -> onboarding package by avg days_to_close

Run: python prime/module3.py
"""

import warnings
import pickle

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

warnings.filterwarnings("ignore")

from config import MARTS, OUTPUT_DIR, MODEL_DIR, HVC_TOP_CATEGORIES, HVC_SPEND_THRESHOLD, validate

CLUSTER_FEATURES = [
    "log_revenue",       # log1p of total_revenue (reduces right-skew influence)
    "avg_review_score",
    "log_orders",        # log1p of total_orders
    "unique_categories",
]

N_CLUSTERS = 3
SEED       = 42
K_RANGE    = range(2, 7)

AD_PACKAGES = {
    "Starter": "Basic product listing | Onboarding analytics dashboard | Category page placement",
    "Growth":  "Featured product slots | Keyword-targeted display ads | Enhanced analytics | Category targeting",
    "Premium": "Banner ads | HVC segment targeting | Cross-sell placements | Dedicated account manager | Priority in watches/health_beauty/computers",
}

# Onboarding package assigned by lead_behaviour_profile from mart_seller_acquisition.
# Days-to-close observed averages: eagle=36, cat=41, shark=75, wolf=84, mixed=149-341.
ONBOARDING_PACKAGES = {
    "eagle":       "Fast-Track - dedicated SDR, 7-day onboarding, priority listing",
    "cat":         "Fast-Track - structured 2-week onboarding, category advisor",
    "wolf":        "Standard Nurture - 90-day drip sequence, bi-weekly check-in",
    "shark":       "High-Touch Support - 60-day intensive, legal/compliance review",
    "cat, wolf":   "Extended Nurture - 120-day sequence, category + ops support",
    "eagle, wolf": "Extended Nurture - 120-day sequence, revenue-focused advisor",
    "eagle, cat":  "Fast-Track Plus - 3-week onboarding, early ad credit",
    "shark, cat":  "High-Touch Support - compliance-first, 45-day structured path",
    "shark, wolf": "High-Touch Support - longest cycle expected, executive sponsor",
}


# FEATURE ENGINEERING
def engineer_features(df):
    df = df.copy()
    df["log_revenue"] = np.log1p(df["total_revenue"])
    df["log_orders"]  = np.log1p(df["total_orders"])
    return df


# CLUSTER VALIDATION
def validate_k(X_scaled):
    # Silhouette in [-1, 1]: higher = better-separated clusters.
    # Inertia (within-cluster SS): look for the elbow point.
    rows = []
    for k in K_RANGE:
        km     = KMeans(n_clusters=k, random_state=SEED, n_init=10)
        labels = km.fit_predict(X_scaled)
        sil    = silhouette_score(X_scaled, labels) if k > 1 else None
        rows.append({
            "k":                k,
            "inertia":          round(km.inertia_, 2),
            "silhouette_score": round(sil, 4) if sil else None,
        })
    return pd.DataFrame(rows)


# CLUSTER -> TIER MAPPING
def map_clusters_to_tiers(df, cluster_col="cluster"):
    # Map cluster index to tier label by ranking mean revenue per cluster
    revenue_by_cluster = df.groupby(cluster_col)["total_revenue"].mean()
    tier_order         = revenue_by_cluster.rank().astype(int)
    tier_map           = {idx: ["Starter", "Growth", "Premium"][rank - 1] for idx, rank in tier_order.items()}
    return df[cluster_col].map(tier_map)


# MAIN
def run():
    validate()
    df_seller = pd.read_csv(MARTS["seller_performance"])
    df_acq    = pd.read_csv(MARTS["seller_acquisition"])

    print("=" * 65)
    print("  PRIME MODULE 3 - K-MEANS SELLER TIER SEGMENTER")
    print("=" * 65)
    print(f"\n  Sellers       : {len(df_seller):,}")
    print(f"  Revenue range : R${df_seller['total_revenue'].min():.0f} - R${df_seller['total_revenue'].max():,.0f}")

    df = engineer_features(df_seller)

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(df[CLUSTER_FEATURES])

    print(f"\nCLUSTER VALIDATION (k = {list(K_RANGE)})")
    print("-" * 65)
    val_df = validate_k(X_scaled)
    print(val_df.to_string(index=False))

    best_k = int(val_df.loc[val_df["silhouette_score"].idxmax(), "k"])
    print(f"\n  Best silhouette: k={best_k}  |  Using k={N_CLUSTERS} (business tier design)")

    kmeans       = KMeans(n_clusters=N_CLUSTERS, random_state=SEED, n_init=20)
    df["cluster"] = kmeans.fit_predict(X_scaled)
    df["revenue_tier"] = map_clusters_to_tiers(df, "cluster")

    print("\nCLUSTER CENTROIDS (original feature scale)")
    print("-" * 65)
    centroid_df = df.groupby("revenue_tier").agg(
        count            = ("seller_id",        "count"),
        avg_revenue      = ("total_revenue",    "mean"),
        median_revenue   = ("total_revenue",    "median"),
        avg_review_score = ("avg_review_score", "mean"),
        avg_orders       = ("total_orders",     "mean"),
        avg_categories   = ("unique_categories","mean"),
    ).round(1)
    print(centroid_df.to_string())

    df["hvc_aligned"]         = (df["avg_order_value"] > HVC_SPEND_THRESHOLD).astype(int)
    df["ad_package"]          = df["revenue_tier"].map(AD_PACKAGES)
    df["ad_credit_value_R$"]  = (
        df["avg_review_score"] * 100 * df["total_orders"]
        * df["hvc_aligned"].map({1: 1.3, 0: 1.0})
        * 0.10
    ).round(2)

    print("\nSELLER TIER DISTRIBUTION")
    print("-" * 65)
    for tier in ["Starter", "Growth", "Premium"]:
        sub = df[df["revenue_tier"] == tier]
        print(f"  {tier:8s}: {len(sub):>5,} sellers ({len(sub)/len(df)*100:>5.1f}%)  "
              f"avg revenue R${sub['total_revenue'].mean():>10,.0f}")

    print("\nLEAD BEHAVIOUR PROFILE -> ONBOARDING PACKAGE")
    print("-" * 65)
    profile_stats = (
        df_acq.groupby("lead_behaviour_profile")
        .agg(count=("seller_id", "count"), avg_days=("days_to_close", "mean"))
        .round(1)
        .sort_values("avg_days")
        .reset_index()
    )
    profile_stats["onboarding_package"] = (
        profile_stats["lead_behaviour_profile"]
        .map(ONBOARDING_PACKAGES)
        .fillna("Standard Nurture - default 60-day sequence")
    )
    print(profile_stats.to_string(index=False))

    print("\nTOP 10 SELLERS BY REVENUE")
    print("-" * 65)
    print(df.sort_values("total_revenue", ascending=False)[[
        "seller_id", "seller_state", "total_revenue", "avg_review_score",
        "revenue_tier", "hvc_aligned", "ad_credit_value_R$",
    ]].head(10).to_string(index=False))

    output_cols = [
        "seller_id", "seller_state", "seller_city",
        "total_revenue", "avg_review_score", "total_orders", "unique_categories",
        "cluster", "revenue_tier", "hvc_aligned", "ad_package", "ad_credit_value_R$",
    ]
    df[output_cols].to_csv(OUTPUT_DIR / "seller_tiers.csv", index=False)
    val_df.to_csv(OUTPUT_DIR / "cluster_validation.csv", index=False)
    profile_stats.to_csv(OUTPUT_DIR / "profile_onboarding_packages.csv", index=False)

    artifacts = {
        "kmeans":               kmeans,
        "scaler":               scaler,
        "cluster_features":     CLUSTER_FEATURES,
        "tier_mapping":         dict(zip(df["cluster"], df["revenue_tier"])),
        "ad_packages":          AD_PACKAGES,
        "onboarding_packages":  ONBOARDING_PACKAGES,
    }
    with open(MODEL_DIR / "seller_kmeans.pkl", "wb") as f:
        pickle.dump(artifacts, f)

    print(f"\nSaved to {OUTPUT_DIR}/ and {MODEL_DIR}/")
    return df, kmeans, scaler, val_df


if __name__ == "__main__":
    run()
