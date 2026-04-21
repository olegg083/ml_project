import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, median_absolute_error, r2_score
from catboost import CatBoostRegressor, Pool

DATA_PATH = ""
MODEL_PATH = "models/catboost_prm2.cbm"
RANDOM_SEED = 42
TEST_SIZE = 0.2
N_SPLITS = 5

CB_PARAMS = {
    "iterations": 2500,
    "learning_rate": 0.03,
    "depth": 7,
    "loss_function": "RMSE",
    "eval_metric": "MAPE",
    "random_seed": RANDOM_SEED,
    "od_type": "Iter",
    "od_wait": 100,
}


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "price_mln" not in df.columns:
        df["price_mln"] = df["price_total"] / 1_000_000
    return df


def prepare_data(df: pd.DataFrame):
    leakage_cols = ['price_total', 'price_mln', 'price_per_m2_calc', 'target_log_prm2']
    y = df['target_log_prm2']
    X = df.drop(columns=[c for c in leakage_cols if c in df.columns])
    cat_features = [c for c in ['building_material', 'condition', 'geo_cluster'] if c in X.columns]
    return X, y, cat_features


def calc_metrics(y_true_mln, y_pred_log_prm2, area_m2):
    y_pred_prm2 = np.expm1(y_pred_log_prm2)
    y_pred_total_mln = (y_pred_prm2 * area_m2) / 1_000_000

    return {
        "mae": mean_absolute_error(y_true_mln, y_pred_total_mln),
        "medae": median_absolute_error(y_true_mln, y_pred_total_mln),
        "mape": mean_absolute_percentage_error(y_true_mln, y_pred_total_mln) * 100,
        "r2": r2_score(y_true_mln, y_pred_total_mln)
    }


def print_metrics(title: str, metrics: dict):
    print(f"\n{title}")
    print(f"  MAE:   {metrics['mae']:.3f} млн руб.")
    print(f"  MedAE: {metrics['medae']:.3f} млн руб.")
    print(f"  MAPE:  {metrics['mape']:.2f}%")
    print(f"  R2:    {metrics['r2']:.4f}")


def run_cross_validation(df_train: pd.DataFrame, X_train: pd.DataFrame, y_train: pd.Series, cat_features: list):
    kf = KFold(n_splits=N_SPLITS, shuffle=True, random_state=RANDOM_SEED)
    cv_metrics = {"mae": [], "medae": [], "mape": [], "r2": []}

    print(f"🔄 Запуск {N_SPLITS}-Fold Cross-Validation...")

    for fold, (train_idx, val_idx) in enumerate(kf.split(X_train), 1):
        X_tr, y_tr = X_train.iloc[train_idx], y_train.iloc[train_idx]
        X_val, y_val = X_train.iloc[val_idx], y_train.iloc[val_idx]
        df_val = df_train.iloc[val_idx]

        model = CatBoostRegressor(**CB_PARAMS, verbose=False)
        model.fit(
            Pool(X_tr, y_tr, cat_features=cat_features),
            eval_set=Pool(X_val, y_val, cat_features=cat_features),
            use_best_model=True
        )

        preds_log = model.predict(X_val)
        metrics = calc_metrics(df_val["price_mln"], preds_log, df_val["area_total_m2"])

        for k in cv_metrics:
            cv_metrics[k].append(metrics[k])

        print(f"   Fold {fold}: MAPE = {metrics['mape']:.2f}% | R2 = {metrics['r2']:.4f}")

    avg_metrics = {k: np.mean(v) for k, v in cv_metrics.items()}
    print_metrics("СРЕДНИЕ МЕТРИКИ КРОСС-ВАЛИДАЦИИ:", avg_metrics)


def train_final_model(X_train, y_train, X_test, y_test, cat_features):
    print("\n🚀 Обучение финальной модели на всем Train...")
    model = CatBoostRegressor(**CB_PARAMS, verbose=500)
    model.fit(
        Pool(X_train, y_train, cat_features=cat_features),
        eval_set=Pool(X_test, y_test, cat_features=cat_features),
        use_best_model=True
    )
    return model


def main():
    df = load_data(DATA_PATH)
    print(f"Исходный датасет: {df.shape[0]} строк, {df.shape[1]} колонок")

    df_train, df_test = train_test_split(df, test_size=TEST_SIZE, random_state=RANDOM_SEED)

    X_train, y_train, cat_features = prepare_data(df_train)
    X_test, y_test, _ = prepare_data(df_test)

    run_cross_validation(df_train, X_train, y_train, cat_features)

    model = train_final_model(X_train, y_train, X_test, y_test, cat_features)

    preds_log = model.predict(X_test)
    final_metrics = calc_metrics(df_test["price_mln"], preds_log, df_test["area_total_m2"])
    print_metrics("ФИНАЛЬНЫЙ ТЕСТ (REAL MONEY):", final_metrics)

    os.makedirs(os.path.dirname(MODEL_PATH) or '.', exist_ok=True)
    model.save_model(MODEL_PATH)
    print(f"\n💾 Модель сохранена в: {MODEL_PATH}")


if __name__ == "__main__":
    main()