import os
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, median_absolute_error, r2_score
from catboost import CatBoostRegressor, Pool

DATA_PATH = "../data/features/features_10000_test3.parquet"
RANDOM_SEED = 42
TEST_SIZE = 0.2

CB_PARAMS = {
    "iterations": 2500,
    "learning_rate": 0.03,
    "depth": 7,
    "eval_metric": "MAPE",
    "random_seed": RANDOM_SEED,
    "od_type": "Iter",
    "od_wait": 100,
    "task_type": "GPU",
}


def load_data(path):
    df = pd.read_parquet(path)
    df["price_mln"] = df["price_total"] / 1_000_000
    if "description_full" in df.columns:
        df["description_full"] = df["description_full"].fillna("Нет").astype(str)
    return df


def prepare_xy(df):
    price_prm2 = df["price_total"] / df["area_total_m2"]
    y = np.log1p(price_prm2)
    drop_cols = ['price_total', 'price_mln', 'price_per_m2_calc', 'target_log_prm2', 'price_log']
    X = df.drop(columns=[c for c in drop_cols if c in df.columns])
    cat = [c for c in ['building_material', 'condition', 'geo_cluster'] if c in X.columns]
    txt = ['description_full'] if 'description_full' in X.columns else []
    return X, y, cat, txt


def get_metrics(y_true_mln, y_pred_log_prm2, area_m2):
    y_pred_total_mln = (np.expm1(y_pred_log_prm2) * area_m2) / 1_000_000
    return {
        "mae": mean_absolute_error(y_true_mln, y_pred_total_mln),
        "medae": median_absolute_error(y_true_mln, y_pred_total_mln),
        "mape": mean_absolute_percentage_error(y_true_mln, y_pred_total_mln) * 100,
        "r2": r2_score(y_true_mln, y_pred_total_mln)
    }


def main():
    df = load_data(DATA_PATH)

    # ВАЛИДАЦИЯ (ЧТОБЫ УЗНАТЬ МЕТРИКИ) ===
    print("ЭТАП 1: Оценка качества...")
    train_df, test_df = train_test_split(df, test_size=TEST_SIZE, random_state=RANDOM_SEED)

    X_train, y_train, cat, txt = prepare_xy(train_df)
    X_test, y_test, _, _ = prepare_xy(test_df)

    val_model = CatBoostRegressor(**CB_PARAMS, verbose=False)
    val_model.fit(Pool(X_train, y_train, cat, txt), eval_set=Pool(X_test, y_test, cat, txt))

    # Считаем и выводим метрики (это реальная сила твоей модели)
    preds = val_model.predict(X_test)
    m = get_metrics(test_df["price_mln"], preds, test_df["area_total_m2"])

    print("\nМЕТРИКИ КАЧЕСТВА (на отложенной выборке):")
    print(f"   MAPE:  {m['mape']:.2f}%")
    print(f"   MedAE: {m['medae']:.3f} млн руб.")
    print(f"   R2:    {m['r2']:.4f}")
    print(f"   MAE:   {m['mae']:.3f} млн руб.")

    best_iter = val_model.get_best_iteration()

    #(ОБУЧЕНИЕ НА 100% ДАННЫХ)
    print(f"\nЭТАП 2: Обучение финальной модели на всех {len(df)} строках...")
    X_full, y_full, cat, txt = prepare_xy(df)

    prod_params = CB_PARAMS.copy()
    prod_params["iterations"] = best_iter  # Используем лучшее число деревьев
    prod_params.pop("od_wait")
    prod_params.pop("od_type")  # Убираем лишнее

    final_model = CatBoostRegressor(**prod_params, verbose=500)
    final_model.fit(Pool(X_full, y_full, cat, txt))

    # Сохраняем
    os.makedirs("models", exist_ok=True)
    final_model.save_model("models/house_model_final.cbm")
    print(f"Метрики зафиксированы, модель обучена на максимуме данных и сохранена.")


if __name__ == "__main__":
    main()

'''МЕТРИКИ КАЧЕСТВА (на отложенной выборке):
   MAPE:  14.85%
   MedAE: 2.364 млн руб.
   R2:    0.8596
   MAE:   8.348 млн руб.'''