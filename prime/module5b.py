"""
PRIME Module 5b - Installment Recommendation Engine (ML Layer)
==============================================================
Input  : mart_customer_payment_profile (94,401 customers)
         mart_payment_leakage          (for leakage-state context)

At checkout, for a given customer + current order, returns:
  recommend_installments  (bool)
  recommended_tier        ("1", "2-6", "7-12", "12+")
  confidence_score        (float 0-1)
  reason                  (human-readable string)

Model: Logistic Regression predicting uses_installments (binary)
  Class balance: ~51.7% positive vs 48.3% negative -> near-perfect balance.
  No class weighting needed. Accuracy and AUC-ROC are valid metrics here.

Features at checkout (available without historical data for new customers):
  state_encoded         : label-encoded state (27 states)
  log_avg_order_value   : log1p of historical or current order value
  log_total_orders      : log1p of past order count (use 1 for new customers)
  is_credit_card        : 1 if preferred_payment_type == credit_card
  is_high_value         : HVC flag from mart_customer_payment_profile
  leakage_state         : 1 if customer_state is in the top-5 leakage states

Integration with Module 5 (rule layer):
  final_recommend = (rule_layer.leakage_flag AND ml_layer.confidence > 0.5)
                    OR (ml_layer.confidence > 0.75)

Run: python prime/module5b.py
"""

import warnings
import pickle

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import classification_report

warnings.filterwarnings("ignore")

from config import (MARTS, OUTPUT_DIR, MODEL_DIR,
                    HVC_SPEND_THRESHOLD, FRONTIER_STATES, validate)

# Top-5 leakage states from Module 5 rule layer (mart_payment_leakage output)
LEAKAGE_STATES = FRONTIER_STATES

MODEL_FEATURES = [
    "state_encoded",
    "log_avg_order_value",
    "log_total_orders",
    "is_credit_card",
    "is_high_value",
    "leakage_state",
]
TARGET = "uses_installments"

LR_PARAMS = {
    "C":            1.0,
    "max_iter":     500,
    "solver":       "lbfgs",
    "random_state": 42,
}
N_FOLDS = 5
SEED    = 42

# Confidence -> installment tier mapping (applied at inference)
CONFIDENCE_TIERS = [
    (0.80, "12+",  "High confidence: proactively surface 12-installment option"),
    (0.65, "7-12", "Medium confidence: show 7-12 installment option"),
    (0.50, "2-6",  "Low confidence: show basic 2-6 installment option"),
    (0.00, "1",    "Unlikely to use installments: do not surface installment prompt"),
]


# FEATURE ENGINEERING
def fit_state_encoder(df):
    le = LabelEncoder()
    le.fit(df["customer_state"].fillna("SP"))
    return le


def build_features(df, state_encoder):
    X = pd.DataFrame(index=df.index)

    # Unknown state at inference time defaults to SP (most common state)
    known = set(state_encoder.classes_)
    state_col = df["customer_state"].fillna("SP").apply(lambda s: s if s in known else "SP")
    X["state_encoded"]       = state_encoder.transform(state_col)
    X["log_avg_order_value"] = np.log1p(df["avg_order_value"].fillna(df["avg_order_value"].median()))
    X["log_total_orders"]    = np.log1p(df["total_orders"].fillna(1))
    X["is_credit_card"]      = (df["preferred_payment_type"] == "credit_card").astype(int)
    X["is_high_value"]       = df["is_high_value"].astype(int)
    X["leakage_state"]       = df["customer_state"].isin(LEAKAGE_STATES).astype(int)
    return X


# INFERENCE
def get_recommended_tier(confidence):
    for threshold, tier, reason in CONFIDENCE_TIERS:
        if confidence >= threshold:
            return tier, reason
    return "1", "Insufficient signal"


def score_checkout(customer_state, avg_order_value, total_orders,
                   preferred_payment_type, is_high_value,
                   model, scaler, state_encoder):
    """
    Score a single checkout event. Called by the checkout API in production.
    For new customers: set total_orders=1, is_high_value=0.
    """
    row = pd.DataFrame([{
        "customer_state":         customer_state,
        "avg_order_value":        avg_order_value,
        "total_orders":           total_orders,
        "preferred_payment_type": preferred_payment_type,
        "is_high_value":          is_high_value,
    }])

    X_raw = build_features(row, state_encoder)
    prob  = model.predict_proba(scaler.transform(X_raw))[0, 1]
    tier, reason = get_recommended_tier(prob)

    # Rule override: leakage state + credit card + order above HVC threshold
    # -> always surface 12-installment regardless of model confidence
    if (customer_state in LEAKAGE_STATES
            and preferred_payment_type == "credit_card"
            and avg_order_value > HVC_SPEND_THRESHOLD):
        tier   = "12+"
        reason = (f"LEAKAGE OVERRIDE: {customer_state} credit card order > R${HVC_SPEND_THRESHOLD:.2f}. "
                  f"Always surface 12-installment per Module 5 rule layer.")
        prob   = max(prob, 0.80)

    return {
        "customer_state":         customer_state,
        "order_value":            avg_order_value,
        "recommend_installments": tier != "1",
        "recommended_tier":       tier,
        "confidence_score":       round(float(prob), 4),
        "reason":                 reason,
    }


# MAIN
def run():
    validate()
    df = pd.read_csv(MARTS["customer_profile"])

    print("=" * 65)
    print("  PRIME MODULE 5B - INSTALLMENT RECOMMENDATION ENGINE")
    print("=" * 65)
    print(f"\n  Customers        : {len(df):,}")
    print(f"  uses_installments: {df[TARGET].sum():,} positive ({df[TARGET].mean()*100:.1f}%)  <- near-balanced")
    print(f"  Leakage states   : {sorted(LEAKAGE_STATES)}")

    state_encoder = fit_state_encoder(df)
    X_raw         = build_features(df, state_encoder)
    y             = df[TARGET].astype(int)

    scaler   = StandardScaler()
    X_scaled = scaler.fit_transform(X_raw)

    print(f"\nRunning {N_FOLDS}-fold cross-validation (Logistic Regression)...")
    lr     = LogisticRegression(**LR_PARAMS)
    cv     = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=SEED)
    cv_acc = cross_val_score(lr, X_scaled, y, cv=cv, scoring="accuracy")
    cv_auc = cross_val_score(lr, X_scaled, y, cv=cv, scoring="roc_auc")
    cv_f1  = cross_val_score(lr, X_scaled, y, cv=cv, scoring="f1")
    print(f"  CV Accuracy : {cv_acc.mean():.4f} ± {cv_acc.std():.4f}")
    print(f"  CV AUC-ROC  : {cv_auc.mean():.4f} ± {cv_auc.std():.4f}")
    print(f"  CV F1       : {cv_f1.mean():.4f} ± {cv_f1.std():.4f}")

    lr.fit(X_scaled, y)
    y_pred = lr.predict(X_scaled)
    y_prob = lr.predict_proba(X_scaled)[:, 1]

    print("\nCLASSIFICATION REPORT (in-sample)")
    print("-" * 65)
    print(classification_report(y, y_pred, target_names=["No installments", "Uses installments"]))

    coef_df = pd.DataFrame({
        "feature":     MODEL_FEATURES,
        "coefficient": lr.coef_[0],
        "abs_coef":    np.abs(lr.coef_[0]),
    }).sort_values("abs_coef", ascending=False)
    print("LOGISTIC REGRESSION COEFFICIENTS (|coef| ranked)")
    print("-" * 65)
    print(coef_df[["feature", "coefficient", "abs_coef"]].to_string(index=False))
    print("\n  Positive coef -> increases P(uses_installments)")

    df["installment_confidence"] = y_prob.round(4)
    df["recommended_tier"]       = pd.Series(y_prob).apply(lambda p: get_recommended_tier(p)[0])
    df["recommendation_reason"]  = pd.Series(y_prob).apply(lambda p: get_recommended_tier(p)[1])

    leakage_mask = (
        df["customer_state"].isin(LEAKAGE_STATES)
        & (df["preferred_payment_type"] == "credit_card")
        & (df["avg_order_value"] > HVC_SPEND_THRESHOLD)
    )
    df.loc[leakage_mask, "recommended_tier"]      = "12+"
    df.loc[leakage_mask, "recommendation_reason"] = "LEAKAGE OVERRIDE: leakage state + credit card + order > HVC threshold"
    df.loc[leakage_mask, "installment_confidence"] = df.loc[leakage_mask, "installment_confidence"].clip(lower=0.80)

    print("\nRECOMMENDED TIER DISTRIBUTION")
    print("-" * 65)
    tier_dist = df["recommended_tier"].value_counts().reindex(["12+", "7-12", "2-6", "1"], fill_value=0)
    for tier, cnt in tier_dist.items():
        print(f"  {tier:6s}: {cnt:>8,} ({cnt/len(df)*100:>5.1f}%)")
    print(f"\n  Leakage override applied: {leakage_mask.sum():,} customers ({leakage_mask.sum()/len(df)*100:.1f}%)")

    print("\nDEMO: SCORE INDIVIDUAL CHECKOUTS")
    print("-" * 65)
    demo_cases = [
        ("SP", 210.00, 1, "credit_card", 0, "New customer SP, credit card, order > threshold"),
        ("SP", 150.00, 3, "credit_card", 1, "HVC in SP, below threshold"),
        ("PB", 268.00, 1, "credit_card", 1, "HVC in non-leakage state PB"),
        ("SC", 85.00,  1, "boleto",      0, "New customer, boleto, low order"),
        ("RJ", 320.00, 5, "credit_card", 1, "HVC in RJ (leakage), repeat buyer"),
    ]
    for state, val, orders, ptype, hv, label in demo_cases:
        result = score_checkout(state, val, orders, ptype, hv, lr, scaler, state_encoder)
        print(f"\n  [{label}]")
        print(f"  Input : state={state}, order=R${val:.0f}, ptype={ptype}, hv={hv}")
        print(f"  Result: recommend={result['recommend_installments']}, "
              f"tier={result['recommended_tier']}, confidence={result['confidence_score']:.3f}")
        print(f"  Reason: {result['reason'][:90]}")

    output_cols = [
        "customer_unique_id", "customer_state", "total_spend", "is_high_value",
        "preferred_payment_type", "installment_bucket", "avg_order_value",
        "installment_confidence", "recommended_tier", "recommendation_reason",
    ]
    df[output_cols].to_csv(OUTPUT_DIR / "installment_recommendations.csv", index=False)
    coef_df.to_csv(OUTPUT_DIR / "installment_model_coefficients.csv", index=False)

    artifacts = {
        "model":         lr,
        "scaler":        scaler,
        "state_encoder": state_encoder,
        "features":      MODEL_FEATURES,
        "leakage_states": LEAKAGE_STATES,
        "cv_metrics": {
            "accuracy_mean": cv_acc.mean(), "accuracy_std": cv_acc.std(),
            "auc_mean":      cv_auc.mean(), "auc_std":      cv_auc.std(),
            "f1_mean":       cv_f1.mean(),  "f1_std":       cv_f1.std(),
        },
    }
    with open(MODEL_DIR / "installment_recommender.pkl", "wb") as f:
        pickle.dump(artifacts, f)

    print(f"\nSaved to {OUTPUT_DIR}/ and {MODEL_DIR}/")
    return lr, scaler, state_encoder, df


if __name__ == "__main__":
    run()
