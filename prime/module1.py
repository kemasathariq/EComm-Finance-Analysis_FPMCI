"""
PRIME Module 1 - XGBoost Lead Scorer
=====================================
Model A: XGBoost REGRESSOR - predicts days_to_close (runs now)
  Input  : 842 converted leads (mart_seller_acquisition)
  Target : days_to_close (clipped to >=0, log-transformed for training)
  Output : priority_score (0-1, higher = close sooner)
           estimated_days_to_close, onboarding_package

Model B: XGBoost CLASSIFIER - predicts conversion probability (stub)
  Requires raw_mql.csv with negative class (non-converted leads).
  Without it we have no negatives with matching qualification features.
  See classifier_stub() for the full pipeline once raw_mql.csv is available.

Why regressor instead of classifier:
  We only have CONVERTED leads. days_to_close is a real quality signal -
  eagle profiles close in 36 days on average, wolf in 84 days.
  This gives a usable SDR priority ranking without needing negative examples.

Run: python prime/module1.py
"""

import warnings
import pickle

import numpy as np
import pandas as pd
from sklearn.model_selection import KFold, cross_val_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb
import shap

warnings.filterwarnings("ignore")

from config import MARTS, OUTPUT_DIR, MODEL_DIR, validate

CAT_FEATURES = [
    "lead_type",
    "lead_behaviour_profile",
    "business_segment",
    "business_type",
]
NUM_FEATURES = ["log_revenue", "has_revenue"]
TARGET       = "days_to_close"
N_FOLDS      = 5
SEED         = 42

XGB_PARAMS = {
    "n_estimators":     400,
    "max_depth":        4,
    "learning_rate":    0.05,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "reg_alpha":        0.1,
    "reg_lambda":       1.0,
    "random_state":     SEED,
    "tree_method":      "hist",
    "objective":        "reg:squarederror",
}


# FEATURE ENGINEERING
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if TARGET in df.columns:
        # A few source records have days_to_close = -2 (data quality issue)
        df[TARGET] = df[TARGET].clip(lower=0)

    # declared_monthly_revenue is 94% zero; log1p compresses scale,
    # has_revenue captures the binary "did they declare?" signal separately
    df["log_revenue"] = np.log1p(df["declared_monthly_revenue"].fillna(0))
    df["has_revenue"]  = (df["declared_monthly_revenue"] > 0).astype(int)

    for col in CAT_FEATURES:
        df[col] = df[col].fillna("unknown").astype(str)

    return df


# TARGET ENCODING (K-FOLD, NO LEAKAGE)
def kfold_target_encode(df_train, col, target, n_folds=5, smoothing=10):
    """
    Smoothed target encoding within K-fold splits to avoid train/val leakage.
    Smoothing formula: (n_i * mean_i + k * global_mean) / (n_i + k)
    Returns: encoded series for training + full-data mapping for inference.
    """
    global_mean = df_train[target].mean()
    encoded     = pd.Series(index=df_train.index, dtype=float)

    kf = KFold(n_splits=n_folds, shuffle=True, random_state=SEED)
    for train_idx, val_idx in kf.split(df_train):
        fold_train = df_train.iloc[train_idx]
        fold_val   = df_train.iloc[val_idx]
        stats = fold_train.groupby(col)[target].agg(["mean", "count"])
        stats["smoothed"] = (
            (stats["mean"] * stats["count"] + global_mean * smoothing)
            / (stats["count"] + smoothing)
        )
        encoded.iloc[val_idx] = fold_val[col].map(stats["smoothed"]).fillna(global_mean).values

    full_stats = df_train.groupby(col)[target].agg(["mean", "count"])
    full_stats["smoothed"] = (
        (full_stats["mean"] * full_stats["count"] + global_mean * smoothing)
        / (full_stats["count"] + smoothing)
    )
    mapping = full_stats["smoothed"].to_dict()
    mapping["__global_mean__"] = global_mean
    return encoded, mapping


def apply_target_encoding(df, col, mapping):
    return df[col].map(mapping).fillna(mapping.get("__global_mean__", 0.0))


# TRAINING
def train(df):
    df = engineer_features(df)

    # Log-transform target: right-skewed (median 14 days, max 427 days)
    y_raw = df[TARGET].values
    y     = np.log1p(y_raw)

    X         = pd.DataFrame(index=df.index)
    encodings = {}
    for col in CAT_FEATURES:
        X[col], encodings[col] = kfold_target_encode(df, col, TARGET, n_folds=N_FOLDS)
    for col in NUM_FEATURES:
        X[col] = df[col].values

    print(f"\n  Running {N_FOLDS}-fold cross-validation...")
    model_cv = xgb.XGBRegressor(**XGB_PARAMS)
    cv_rmse  = cross_val_score(model_cv, X, y, cv=N_FOLDS, scoring="neg_root_mean_squared_error")
    cv_r2    = cross_val_score(model_cv, X, y, cv=N_FOLDS, scoring="r2")
    print(f"  CV RMSE (log scale): {-cv_rmse.mean():.4f} ± {cv_rmse.std():.4f}")
    print(f"  CV R2              : {cv_r2.mean():.4f} ± {cv_r2.std():.4f}")

    model_final = xgb.XGBRegressor(**XGB_PARAMS)
    model_final.fit(X, y)

    y_pred     = np.expm1(model_final.predict(X))
    rmse_days  = np.sqrt(mean_squared_error(y_raw, y_pred))
    mae_days   = mean_absolute_error(y_raw, y_pred)
    r2_days    = r2_score(y_raw, y_pred)
    print(f"\n  In-sample (original scale): RMSE={rmse_days:.1f}d  MAE={mae_days:.1f}d  R2={r2_days:.3f}")

    cv_metrics = {
        "cv_rmse_log_mean": -cv_rmse.mean(), "cv_rmse_log_std": cv_rmse.std(),
        "cv_r2_mean": cv_r2.mean(),          "cv_r2_std":       cv_r2.std(),
        "insample_rmse_days": rmse_days,     "insample_mae_days": mae_days,
        "insample_r2": r2_days,
    }
    return model_final, encodings, X, y_raw, y_pred, cv_metrics


# SHAP
def compute_shap(model, X):
    explainer = shap.TreeExplainer(model)
    shap_vals = explainer.shap_values(X)
    importance = pd.DataFrame({
        "feature":       X.columns.tolist(),
        "mean_abs_shap": np.abs(shap_vals).mean(axis=0),
        "mean_shap":     shap_vals.mean(axis=0),
    }).sort_values("mean_abs_shap", ascending=False).reset_index(drop=True)
    importance["rank"] = importance.index + 1
    return importance, shap_vals


# SCORING
def priority_score(days):
    # Invert days_to_close: shorter close = higher priority score
    clipped = np.clip(days, 0, None)
    mn, mx  = clipped.min(), clipped.max()
    if mx == mn:
        return np.ones_like(clipped)
    return 1.0 - (clipped - mn) / (mx - mn)


def score_leads(df_new, model, encodings, top_segment_by_profile):
    df = engineer_features(df_new)
    X  = pd.DataFrame(index=df.index)
    for col in CAT_FEATURES:
        X[col] = apply_target_encoding(df, col, encodings[col])
    for col in NUM_FEATURES:
        X[col] = df[col].values

    days_pred = np.expm1(model.predict(X)).clip(0)
    p_score   = priority_score(days_pred)

    result = df_new.copy()
    result["predicted_days_to_close"] = days_pred.round(1)
    result["priority_score"]          = p_score.round(4)
    result["priority_tier"]           = pd.cut(
        p_score, bins=[0, 0.4, 0.7, 1.0],
        labels=["LOW", "MEDIUM", "HIGH"], include_lowest=True,
    )
    result["predicted_segment"] = df["lead_behaviour_profile"].map(top_segment_by_profile).fillna("unknown")
    result["onboarding_package"] = pd.Series(days_pred.clip(0)).map(
        lambda d: "Fast-Track (<=45 days)"      if d <= 45
             else "Standard Nurture (<=90 days)" if d <= 90
             else "High-Touch Support (>90 days)"
    )
    return result.sort_values("priority_score", ascending=False).reset_index(drop=True)


# CLASSIFIER STUB
def classifier_stub():
    """
    Binary classifier for conversion probability.
    Run this when raw_mql.csv is available from the Airflow pipeline.

    Setup:
      df_mql    = pd.read_csv("data/raw/mql.csv")          # 8,000 leads, origin only
      df_closed = pd.read_csv("data/raw/closed_deals.csv") # 842 converted
      df = df_mql.merge(df_closed, on="mql_id", how="left")
      df["is_converted"] = df["seller_id"].notna().astype(int)

    Feature gap: converted leads have full profile; non-converted only have origin.
    Fill non-converted categoricals with "unknown" - XGBoost handles this.

    Key params:
      scale_pos_weight = (8000 - 842) / 842 = 8.5  (class imbalance)
      eval_metric = "aucpr"  (AUPRC, not AUC-ROC - more honest for imbalanced data)
      baseline AUPRC = 0.105 (positive rate); any score above that adds value
    """
    print("\n  [STUB] Binary classifier requires raw_mql.csv with negative class.")
    print("  See classifier_stub() docstring for the full pipeline.")


# MAIN
def run():
    validate()
    df = pd.read_csv(MARTS["seller_acquisition"])

    print("=" * 65)
    print("  PRIME MODULE 1 - XGBOOST LEAD SCORER")
    print("=" * 65)
    print(f"\n  Training samples : {len(df):,} converted leads")
    print(f"  days_to_close    : min={df['days_to_close'].min()}, "
          f"median={df['days_to_close'].median():.0f}, "
          f"max={df['days_to_close'].max()}")

    model, encodings, X_train, y_raw, y_pred, cv_metrics = train(df)

    top_seg = (
        df.groupby("lead_behaviour_profile")["business_segment"]
        .agg(lambda x: x.value_counts().index[0])
        .to_dict()
    )

    print("\nSHAP Feature Importance (mean |SHAP| on log scale)")
    print("-" * 65)
    shap_df, _ = compute_shap(model, X_train)
    print(shap_df.to_string(index=False))

    scored = score_leads(df, model, encodings, top_seg)

    print("\nPREDICTED DAYS-TO-CLOSE BY LEAD BEHAVIOUR PROFILE")
    print("-" * 65)
    profile_summary = (
        scored.groupby("lead_behaviour_profile")
        .agg(
            count              = ("mql_id",                 "count"),
            avg_actual_days    = ("days_to_close",           "mean"),
            avg_predicted_days = ("predicted_days_to_close", "mean"),
            avg_priority_score = ("priority_score",          "mean"),
            pct_high_priority  = ("priority_tier",           lambda x: (x == "HIGH").mean() * 100),
        )
        .round(1)
        .sort_values("avg_predicted_days")
    )
    print(profile_summary.to_string())

    print("\nPRIORITY TIER DISTRIBUTION")
    print("-" * 65)
    for tier, cnt in scored["priority_tier"].value_counts().items():
        print(f"  {tier:8s}: {cnt:>4} leads ({cnt/len(scored)*100:.1f}%)")

    print("\nTOP 10 LEADS BY PRIORITY SCORE")
    print("-" * 65)
    preview_cols = [
        "mql_id", "lead_behaviour_profile", "business_segment",
        "priority_score", "predicted_days_to_close", "priority_tier", "onboarding_package",
    ]
    print(scored[preview_cols].head(10).to_string(index=False))

    classifier_stub()

    artifacts = {
        "model":     model,
        "encodings": encodings,
        "top_segment_by_profile": top_seg,
        "feature_cols": CAT_FEATURES + NUM_FEATURES,
        "cv_metrics":  cv_metrics,
        "shap_importance": shap_df,
    }
    with open(MODEL_DIR / "lead_scorer_regressor.pkl", "wb") as f:
        pickle.dump(artifacts, f)

    scored.to_csv(OUTPUT_DIR / "lead_scores.csv", index=False)
    shap_df.to_csv(OUTPUT_DIR / "lead_shap_importance.csv", index=False)

    print(f"\nSaved to {MODEL_DIR}/ and {OUTPUT_DIR}/")
    return model, encodings, scored, shap_df


if __name__ == "__main__":
    run()
