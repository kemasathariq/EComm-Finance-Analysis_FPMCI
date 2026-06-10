import math
import pandas as pd
from config import MARTS, MQL_RAW, OUTPUT_DIR, validate


def load_mql_funnel() -> pd.DataFrame:
    mql = pd.read_csv(MQL_RAW)
    acq = pd.read_csv(MARTS["seller_acquisition"])
    mql["origin"] = mql["origin"].fillna("unknown")
    merged = mql.merge(acq[["mql_id"]], on="mql_id", how="left")
    merged["converted"] = merged["mql_id"].isin(acq["mql_id"]).astype(int)
    return merged


def compute_channel_efficiency(funnel: pd.DataFrame) -> pd.DataFrame:
    grp = funnel.groupby("origin").agg(
        total_leads     = ("mql_id",    "count"),
        converted_leads = ("converted", "sum"),
    ).reset_index()
    grp["conversion_rate_pct"] = (grp["converted_leads"] / grp["total_leads"] * 100).round(2)
    grp["log_volume"]          = grp["total_leads"].apply(lambda v: round(math.log(v + 1), 4))
    grp["efficiency_score"]    = (grp["converted_leads"] / grp["total_leads"] * grp["log_volume"]).round(6)

    grp = grp.sort_values("efficiency_score", ascending=False).reset_index(drop=True)
    grp["rank"]                  = grp.index + 1
    grp["budget_allocation_pct"] = (grp["efficiency_score"] / grp["efficiency_score"].sum() * 100).round(1)
    grp["action_priority"]       = grp.apply(_action_priority, axis=1)
    grp["recommendation"]        = grp.apply(_recommendation, axis=1)
    return grp


def _action_priority(row) -> str:
    if row["origin"] == "unknown":
        return "P0 - Fix UTM tracking (Week 1)"
    match row["rank"]:
        case 1 | 2: return "P1 - Scale now"
        case 3 | 4: return "P2 - Maintain and monitor"
        case _:     return "P3 - Reduce and reallocate"


def _recommendation(row) -> str:
    if row["origin"] == "unknown":
        return ("FIX UTM FIRST - 16.7% conversion but 0% attribution. "
                "Enforce UTM params on all landing pages before any spend increase.")
    if row["efficiency_score"] == 0:
        return "PAUSE - Zero efficiency. Investigate before any new spend."
    if row["rank"] <= 2:
        return f"INCREASE - Rank {row['rank']} by efficiency. Scale budget 20-30%."
    if row["rank"] <= 4:
        return "MAINTAIN - Solid efficiency. Review quarterly."
    return ("REDUCE - Below-median efficiency. "
            "Reallocate to Rank 1-2 channels. A/B test creative before cutting.")


def compute_profile_close_time(df_acq: pd.DataFrame) -> pd.DataFrame:
    return (
        df_acq.groupby("lead_behaviour_profile", as_index=False)
        .agg(
            seller_count          = ("seller_id",    "count"),
            avg_days_to_close     = ("days_to_close","mean"),
            median_days_to_close  = ("days_to_close","median"),
            min_days              = ("days_to_close","min"),
            max_days              = ("days_to_close","max"),
        )
        .round(1)
        .sort_values("avg_days_to_close")
        .assign(onboarding_package=lambda d: d["avg_days_to_close"].apply(
            lambda x: "Fast-Track (<45 days)"       if x < 45
                 else "Standard Nurture (45-90 days)" if x < 90
                 else "High-Touch Support (>90 days)"
        ))
    )


def compute_segment_distribution(df_acq: pd.DataFrame) -> pd.DataFrame:
    return (
        df_acq.groupby("business_segment", as_index=False)
        .agg(
            converted_count = ("seller_id",    "count"),
            avg_days        = ("days_to_close","mean"),
            pct_resellers   = ("business_type", lambda x: (x == "reseller").mean() * 100),
        )
        .round(1)
        .sort_values("converted_count", ascending=False)
        .assign(platform_share_pct=lambda d:
            (d["converted_count"] / d["converted_count"].sum() * 100).round(1))
    )


def run():
    validate()
    funnel = load_mql_funnel()
    df_acq = pd.read_csv(MARTS["seller_acquisition"])

    print("=" * 65)
    print("  PRIME MODULE 2 - MARKETING CHANNEL PRIORITIZATION")
    print("=" * 65)

    channel_df = compute_channel_efficiency(funnel)
    print("\nCHANNEL EFFICIENCY RANKING")
    print(channel_df[[
        "rank", "origin", "total_leads", "conversion_rate_pct",
        "efficiency_score", "budget_allocation_pct", "action_priority",
    ]].to_string(index=False))

    print("\nBUDGET RECOMMENDATIONS")
    for _, row in channel_df.iterrows():
        print(f"  [{row['rank']:2d}] {row['origin']:22s} -> {row['recommendation'][:80]}")

    profile_df = compute_profile_close_time(df_acq)
    print("\nLEAD BEHAVIOUR PROFILE - DAYS TO CLOSE")
    print(profile_df.to_string(index=False))

    seg_df = compute_segment_distribution(df_acq)
    print("\nTOP BUSINESS SEGMENTS (converted leads)")
    print(seg_df.head(10).to_string(index=False))

    channel_df.to_csv(OUTPUT_DIR / "channel_priority_report.csv", index=False)
    profile_df.to_csv(OUTPUT_DIR / "profile_close_time.csv",      index=False)
    seg_df.to_csv(    OUTPUT_DIR / "segment_distribution.csv",     index=False)
    print(f"\nSaved to {OUTPUT_DIR}/")
    return channel_df, profile_df, seg_df


if __name__ == "__main__":
    run()
