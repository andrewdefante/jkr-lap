"""
MLB Pitch Quality Model — Stuff+ Layer

Trains a CSW (Called Strike + Whiff) prediction model per pitch type.
Normalized to 100 = league average, higher = better.

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/stuff_model.py --train
    PYTHONPATH=/app python3 /pipeline/mlb/stuff_model.py --train --season 2023 2024 2025
    PYTHONPATH=/app python3 /pipeline/mlb/stuff_model.py --evaluate
    PYTHONPATH=/app python3 /pipeline/mlb/stuff_model.py --score --season 2025
"""

import sys
import os
import argparse
import pickle
import numpy as np
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from sqlalchemy import text

# Pitch types to model individually
PITCH_TYPES = ['FF', 'SI', 'SL', 'CH', 'FC', 'ST', 'CU', 'FS', 'KC']

# Minimum pitches to include a pitcher in scoring
MIN_PITCHES = 50

# Where to save trained models
MODEL_DIR = "/pipeline/mlb/models"

FEATURES = [
    'start_speed',
    'pfx_x',
    'pfx_z', 
    'spin_rate',
    'x0',           # release point horizontal
    'z0',           # release point height
    'px',           # plate location horizontal
    'pz',           # plate location vertical
    'balls',
    'strikes',
    'same_hand',    # 1 if pitcher/batter same handedness
    'velo_minus_type_avg',   # velocity relative to pitch type average
    'break_total',           # sqrt(pfx_x^2 + pfx_z^2) — total movement magnitude
]


def load_training_data(seasons: list, db) -> pd.DataFrame:
    """
    Pull aggregated pitcher-level pitch data.
    One row per pitcher per pitch type per season.
    """
    season_list = ','.join(str(s) for s in seasons)
    print(f"  Loading aggregated pitch data for seasons: {season_list}")

    sql = text(f"""
        SELECT
            p.pitcher_id,
            p.pitch_type_code,
            EXTRACT(YEAR FROM g.game_date::date) as season,
            COUNT(*) as pitches,
            AVG(p.start_speed) as avg_velo,
            STDDEV(p.start_speed) as std_velo,
            AVG(p.pfx_x) as avg_hmov,
            AVG(p.pfx_z) as avg_vmov,
            AVG(p.spin_rate) as avg_spin,
            AVG(p.x0) as avg_x0,
            AVG(p.z0) as avg_z0,
            STDDEV(p.x0) as std_x0,
            STDDEV(p.z0) as std_z0,
            AVG(SQRT(p.pfx_x^2 + p.pfx_z^2)) as avg_break_total,
            AVG(CASE WHEN p.call_code IN ('S','W','T','C') 
                THEN 1.0 ELSE 0.0 END) as csw_rate,
            SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
            NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                THEN 1.0 ELSE 0.0 END), 0) as whiff_rate,
            AVG(CASE WHEN ab.bat_side = ab.pitch_hand 
                THEN 1.0 ELSE 0.0 END) as same_hand_pct,
            AVG(CASE WHEN ab.bat_side = ab.pitch_hand
                AND p.call_code IN ('S','W','T','C')
                THEN 1.0 ELSE NULL END) as csw_same_hand,
            AVG(CASE WHEN ab.bat_side != ab.pitch_hand
                AND p.call_code IN ('S','W','T','C')
                THEN 1.0 ELSE NULL END) as csw_opp_hand
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE EXTRACT(YEAR FROM g.game_date::date) IN ({season_list})
        AND p.pitch_type_code IN ('FF','SI','SL','CH','FC','ST','CU','FS','KC')
        AND p.start_speed IS NOT NULL
        AND p.pfx_x IS NOT NULL
        AND p.pfx_z IS NOT NULL
        AND p.spin_rate IS NOT NULL
        AND p.x0 IS NOT NULL
        AND p.z0 IS NOT NULL
        AND ab.bat_side IS NOT NULL
        GROUP BY p.pitcher_id, p.pitch_type_code,
            EXTRACT(YEAR FROM g.game_date::date)
        HAVING COUNT(*) >= 100
    """)

    print("  Executing query...")
    df = pd.read_sql(sql, db.bind)
    print(f"  Loaded {len(df):,} pitcher-pitch type seasons")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all features relative to pitch type average."""

    # Velocity above pitch type average
    type_avg_velo = df.groupby('pitch_type_code')['avg_velo'].transform('mean')
    type_std_velo = df.groupby('pitch_type_code')['avg_velo'].transform('std')
    df['velo_z'] = (df['avg_velo'] - type_avg_velo) / type_std_velo

    # Horizontal movement z-score
    type_avg_hmov = df.groupby('pitch_type_code')['avg_hmov'].transform('mean')
    type_std_hmov = df.groupby('pitch_type_code')['avg_hmov'].transform('std')
    df['hmov_z'] = (df['avg_hmov'] - type_avg_hmov) / type_std_hmov

    # Vertical movement z-score
    type_avg_vmov = df.groupby('pitch_type_code')['avg_vmov'].transform('mean')
    type_std_vmov = df.groupby('pitch_type_code')['avg_vmov'].transform('std')
    df['vmov_z'] = (df['avg_vmov'] - type_avg_vmov) / type_std_vmov

    # Spin z-score
    type_avg_spin = df.groupby('pitch_type_code')['avg_spin'].transform('mean')
    type_std_spin = df.groupby('pitch_type_code')['avg_spin'].transform('std')
    df['spin_z'] = (df['avg_spin'] - type_avg_spin) / type_std_spin

    # Break z-score
    type_avg_break = df.groupby('pitch_type_code')['avg_break_total'].transform('mean')
    type_std_break = df.groupby('pitch_type_code')['avg_break_total'].transform('std')
    df['break_z'] = (df['avg_break_total'] - type_avg_break) / type_std_break

    # Release point consistency (lower std = more consistent = better command)
    df['release_consistency'] = 1 / (
        df['std_x0'].fillna(0.1) + df['std_z0'].fillna(0.1) + 0.01
    )
    type_avg_rc = df.groupby('pitch_type_code')['release_consistency'].transform('mean')
    type_std_rc = df.groupby('pitch_type_code')['release_consistency'].transform('std')
    df['release_consistency_z'] = (
        df['release_consistency'] - type_avg_rc
    ) / type_std_rc.replace(0, 1)

    # Platoon split — how much does pitch play differently vs same/opp hand
    #df['platoon_split'] = (
    #    df['csw_same_hand'].fillna(df['csw_rate']) -
    #    df['csw_opp_hand'].fillna(df['csw_rate'])
    #)

    return df


def train_models(df: pd.DataFrame) -> dict:
    """
    Train Ridge regression per pitch type.
    Target: next season CSW rate (predictive validation).
    Falls back to same-season if insufficient cross-season data.
    """
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import Pipeline
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import r2_score
    import numpy as np

    feature_cols = [
    'velo_z',
    'hmov_z',
    'vmov_z',
    'spin_z',
    'break_z',
    'release_consistency_z',
    ]

    models = {}

    for pitch_type in PITCH_TYPES:
        pt_df = df[df['pitch_type_code'] == pitch_type].copy().dropna(
            subset=feature_cols + ['csw_rate']
        )

        if len(pt_df) < 30:
            print(f"  Skipping {pitch_type} — only {len(pt_df)} pitcher-seasons")
            continue

        print(f"\n  Training {pitch_type} ({len(pt_df):,} pitcher-seasons)...")

        # Try cross-season validation: train on year N, predict year N+1
        seasons = sorted(pt_df['season'].unique())
        cross_season_scores = []

        if len(seasons) >= 2:
            for i in range(len(seasons) - 1):
                train_season = seasons[i]
                test_season = seasons[i + 1]

                # Match pitchers who appear in both seasons
                train = pt_df[pt_df['season'] == train_season]
                test = pt_df[pt_df['season'] == test_season]
                common = set(train['pitcher_id']) & set(test['pitcher_id'])

                if len(common) < 20:
                    continue

                train_common = train[train['pitcher_id'].isin(common)]
                test_common = test[test['pitcher_id'].isin(common)]

                # Align on pitcher_id
                train_common = train_common.set_index('pitcher_id')
                test_common = test_common.set_index('pitcher_id')
                common_idx = list(common)

                X_train = train_common.loc[common_idx, feature_cols].fillna(0)
                y_train = train_common.loc[common_idx, 'csw_rate']
                X_test = test_common.loc[common_idx, feature_cols].fillna(0)
                y_test = test_common.loc[common_idx, 'csw_rate']

                model_cv = Pipeline([
                    ('scaler', StandardScaler()),
                    ('ridge', Ridge(alpha=5.0))
                ])
                model_cv.fit(X_train, y_train)
                score = model_cv.score(X_test, y_test)
                cross_season_scores.append(score)
                print(f"    {int(train_season)}→{int(test_season)} R²: {score:.4f} "
                      f"(n={len(common_idx)})")

        if cross_season_scores:
            print(f"    Avg cross-season R²: {np.mean(cross_season_scores):.4f}")

        # Fit final model on all data
        X = pt_df[feature_cols].fillna(0)
        y = pt_df['csw_rate']

        model = Pipeline([
            ('scaler', StandardScaler()),
            ('ridge', Ridge(alpha=5.0))
        ])

        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        model.fit(X, y)
        train_r2 = r2_score(y, model.predict(X))

        print(f"    CV R²: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
        print(f"    Train R²: {train_r2:.4f}")

        # Show coefficients
        ridge = model.named_steps['ridge']
        coef_pairs = sorted(
            zip(feature_cols, ridge.coef_),
            key=lambda x: abs(x[1]), reverse=True
        )
        print(f"    Coefficients: {', '.join(f'{n}({v:+.4f})' for n,v in coef_pairs[:5])}")

        league_avg = float(y.mean())
        print(f"    League avg CSW: {league_avg:.3f}")

        models[pitch_type] = {
            'model': model,
            'feature_cols': feature_cols,
            'league_avg_csw': league_avg,
            'cv_r2': float(cv_scores.mean()),
            'cross_season_r2': float(np.mean(cross_season_scores)) if cross_season_scores else None,
            'n_pitcher_seasons': len(pt_df),
        }

    return models


def score_pitchers(models: dict, df: pd.DataFrame, season: int) -> pd.DataFrame:
    """Score pitchers and normalize to 100 scale."""
    results = []

    for pitch_type, model_data in models.items():
        pt_df = df[df['pitch_type_code'] == pitch_type].copy()
        feature_cols = model_data['feature_cols']
        league_avg = model_data['league_avg_csw']

        pt_df = pt_df.dropna(subset=feature_cols)
        if len(pt_df) == 0:
            continue

        X = pt_df[feature_cols].fillna(0)
        pt_df = pt_df.copy()
        pt_df['predicted_csw'] = model_data['model'].predict(X)

        # Normalize to 100 scale
        pt_df['stuff_plus'] = (
            pt_df['predicted_csw'] / league_avg * 100
        ).round(1)

        pt_df['pitch_type'] = pitch_type
        pt_df['season'] = season

        results.append(pt_df[[
            'pitcher_id', 'pitch_type', 'season', 'pitches',
            'stuff_plus', 'predicted_csw', 'csw_rate',
            'avg_velo', 'avg_spin', 'avg_hmov', 'avg_vmov', 'whiff_rate'
        ]])

    if not results:
        return pd.DataFrame()

    return pd.concat(results, ignore_index=True)


def save_models(models: dict):
    """Save trained models to disk."""
    os.makedirs(MODEL_DIR, exist_ok=True)
    for pitch_type, model_data in models.items():
        path = os.path.join(MODEL_DIR, f"stuff_{pitch_type}.pkl")
        with open(path, 'wb') as f:
            pickle.dump(model_data, f)
        print(f"  Saved {pitch_type} model → {path}")


def load_models() -> dict:
    """Load trained models from disk."""
    models = {}
    for pitch_type in PITCH_TYPES:
        path = os.path.join(MODEL_DIR, f"stuff_{pitch_type}.pkl")
        if os.path.exists(path):
            with open(path, 'rb') as f:
                models[pitch_type] = pickle.load(f)
            print(f"  Loaded {pitch_type} model")
    return models


def main():
    parser = argparse.ArgumentParser(description="MLB Stuff+ pitch quality model")
    parser.add_argument("--train", action="store_true")
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--score", action="store_true")
    parser.add_argument("--season", type=int, nargs="+", default=[2023, 2024, 2025])
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.train:
            print(f"\n=== Training Stuff+ Model ===")
            print(f"Seasons: {args.season}")

            df = load_training_data(args.season, db)
            df = engineer_features(df)

            print(f"\n  CSW rate by pitch type (training data):")
            csw_by_type = df.groupby('pitch_type_code')['csw_rate'].mean()
            for pt, rate in csw_by_type.items():
                print(f"    {pt}: {rate:.3f}")

            print(f"\n  Training models...")
            models = train_models(df)

            save_models(models)
            print(f"\n  Trained {len(models)} models")

        if args.score:
            print(f"\n=== Scoring Pitchers ===")
            models = load_models()
            if not models:
                print("No models found — run --train first")
                return

            score_season = args.season[-1]
            df = load_training_data([score_season], db)
            df = engineer_features(df)

            scores = score_pitchers(models, df, score_season)

            if scores.empty:
                print("No scores generated")
                return

            print(f"\n  Top 20 pitchers by Stuff+ (min {MIN_PITCHES} pitches per type):")
            print(f"\n  {'Pitcher ID':>10} {'Type':>4} {'Stuff+':>8} "
                  f"{'Pitches':>8} {'Velo':>6} {'Whiff%':>7}")
            print(f"  {'-'*10} {'-'*4} {'-'*8} {'-'*8} {'-'*6} {'-'*7}")

            top = scores.sort_values('stuff_plus', ascending=False).head(20)
            for _, row in top.iterrows():
                print(f"  {int(row['pitcher_id']):>10} {row['pitch_type']:>4} "
                      f"{row['stuff_plus']:>8.1f} {int(row['pitches']):>8,} "
                      f"{row['avg_velo']:>6.1f} {row['whiff_rate']:>7.3f}")

        if args.evaluate:
            print(f"\n=== Model Evaluation ===")
            models = load_models()
            for pt, m in models.items():
                print(f"  {pt}: AUC={m['auc']:.4f}, n={m['n_pitches']:,}, "
                      f"league_avg_csw={m['league_avg_csw']:.3f}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
