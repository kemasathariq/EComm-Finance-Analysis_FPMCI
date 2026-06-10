import pandas as pd
from datetime import date
from config import MARTS, OUTPUT_DIR, NATIONAL_AVG_ORDER_VALUE, LEAKAGE_ALERT_THRESHOLD, validate

CRITICAL_PAYMENT_TYPES = {"not_defined", "unknown"}
SEVERITY_BANDS = [(2000, "CRITICAL"), (1000, "HIGH"), (500, "MEDIUM"), (0, "LOW")]


def classify_severity(score: float) -> str:
    for threshold, label in SEVERITY_BANDS:
        if score >= threshold:
            return label
    return "LOW"


def pct_below(local_avg: float, national_avg: float) -> float:
    if national_avg == 0 or local_avg == 0:
        return 0.0
    return round((national_avg - local_avg) / national_avg * 100, 2)


def get_remediation(row: pd.Series) -> str:
    state, ptype, score, pct = (
        row["state"], row["payment_type"], row["leakage_score"], row["pct_below_national_avg"]
    )
    if ptype in CRITICAL_PAYMENT_TYPES:
        return (f"CRITICAL ROUTING ERROR - {ptype} linked to review scores 1.0-1.67. "
                f"Escalate to payment ops. Manual review within 24h SLA.")
    if ptype == "credit_card" and score > 1000:
        return (f"Push 12-installment plan for {state} credit card orders above R$ 180.52. "
                f"{pct:.1f}% below national avg across {int(row['order_count']):,} transactions.")
    if ptype == "boleto" and score > 500:
        return (f"Offer {state} boleto customers credit-card-with-installment upgrade at checkout. "
                f"{pct:.1f}% below national avg.")
    if pct > 10:
        return (f"{state} {ptype}: {pct:.1f}% below national avg. "
                f"Investigate checkout flow for friction points.")
    return f"{state} {ptype}: {pct:.1f}% below national avg. Monitor weekly."


def build_leakage_alerts(df_leakage: pd.DataFrame, national_avg: float) -> pd.DataFrame:
    df = df_leakage.copy()
    df["pct_below_national_avg"] = df["avg_value"].apply(lambda v: pct_below(v, national_avg))
    df["severity"]               = df["leakage_score"].apply(classify_severity)
    df["alert_flag"]             = df["pct_below_national_avg"] > LEAKAGE_ALERT_THRESHOLD
    df["revenue_at_risk_R$"]     = (
        (df["avg_value"] - national_avg).clip(upper=0).abs() * df["order_count"]
    ).round(2)
    df["remediation"] = df.apply(get_remediation, axis=1)

    alerts = df[df["leakage_score"] > 0].sort_values("leakage_score", ascending=False).reset_index(drop=True)
    alerts["alert_rank"] = alerts.index + 1
    return alerts


def build_state_summary(alerts: pd.DataFrame) -> pd.DataFrame:
    return (
        alerts.groupby("state", as_index=False)
        .agg(
            total_leakage_score   = ("leakage_score",          "sum"),
            total_orders_affected = ("order_count",            "sum"),
            total_revenue_at_risk = ("revenue_at_risk_R$",     "sum"),
            num_payment_types     = ("payment_type",           "count"),
            worst_payment_type    = ("payment_type",           "first"),
            highest_pct_below     = ("pct_below_national_avg", "max"),
        )
        .sort_values("total_leakage_score", ascending=False)
        .round(2)
        .reset_index(drop=True)
        .assign(state_rank=lambda d: d.index + 1)
    )


def run():
    validate()
    df_leakage = pd.read_csv(MARTS["payment_leakage"])
    df_orders  = pd.read_csv(MARTS["order_detail"], usecols=["total_payment_value", "payment_type"])

    computed_avg = df_orders["total_payment_value"].mean()

    print("=" * 65)
    print("  PRIME MODULE 5 - PAYMENT LEAKAGE MONITOR")
    print("=" * 65)
    print(f"\n  Analysis date    : {date.today()}")
    print(f"  National avg     : R$ {NATIONAL_AVG_ORDER_VALUE:.2f}  (config)")
    print(f"  Validated avg    : R$ {computed_avg:.2f}  (live from mart)")
    print(f"  Alert threshold  : >{LEAKAGE_ALERT_THRESHOLD}% below national avg")
    print(f"  Total rows       : {len(df_leakage)}")
    print(f"  Active leakage   : {len(df_leakage[df_leakage['leakage_score'] > 0])}")

    alerts    = build_leakage_alerts(df_leakage, NATIONAL_AVG_ORDER_VALUE)
    state_sum = build_state_summary(alerts)

    print("\nTOP 15 LEAKAGE ALERTS")
    print(alerts[[
        "alert_rank", "state", "payment_type", "order_count", "avg_value",
        "pct_below_national_avg", "leakage_score", "severity", "alert_flag",
    ]].head(15).to_string(index=False))

    print("\nSTATE LEAKAGE SUMMARY (top 10)")
    print(state_sum.head(10)[[
        "state_rank", "state", "total_leakage_score",
        "total_orders_affected", "total_revenue_at_risk",
        "worst_payment_type", "highest_pct_below",
    ]].to_string(index=False))

    print("\nREMEDIATION - TOP 10 ALERTS")
    for _, row in alerts.head(10).iterrows():
        print(f"\n  [{row['alert_rank']:2d}] {row['state']} / {row['payment_type']:12s}"
              f" | Score: {row['leakage_score']:>8.1f} | {row['severity']}")
        print(f"       -> {row['remediation'][:100]}")

    critical = alerts[alerts["payment_type"].isin(CRITICAL_PAYMENT_TYPES)]
    if not critical.empty:
        print(f"\nCRITICAL ROUTING FAILURES ({len(critical)} found)")
        print(critical[["state", "payment_type", "order_count", "avg_value", "remediation"]].to_string(index=False))

    alerts.to_csv(   OUTPUT_DIR / "leakage_alerts.csv",        index=False)
    state_sum.to_csv(OUTPUT_DIR / "leakage_state_summary.csv", index=False)
    print(f"\nSaved to {OUTPUT_DIR}/")
    return alerts, state_sum


if __name__ == "__main__":
    run()
