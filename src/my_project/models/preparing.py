# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans  # <--- ДОБАВИЛИ ДЛЯ КЛАСТЕРОВ

KREMLIN_LAT = 55.7558
KREMLIN_LON = 37.6173


def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = log2 = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * (2 * np.arcsin(np.sqrt(a)))


def add_geo_clusters(df: pd.DataFrame, n_clusters: int = 70) -> pd.DataFrame:
    """Разбивает координаты на микрорайоны (кластеры)"""
    print(f"🗺 Создаем {n_clusters} гео-кластеров...")
    coords = df[['lat', 'lon']].copy()

    # Обучаем K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df['geo_cluster'] = kmeans.fit_predict(coords).astype(str)
    return df


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    print("🛠 Начинаем Feature Engineering...")
    data = df.copy()

    # 1. ГЕО-ПРИЗНАКИ И КЛАСТЕРЫ (Делаем ДО удаления lat/lon)
    if 'lat' in data.columns and 'lon' in data.columns:
        data['distance_to_center_km'] = haversine_distance(data['lat'], data['lon'], KREMLIN_LAT, KREMLIN_LON)
        data = add_geo_clusters(data, n_clusters=100)  # <--- РАЗБИЛИ НА РАЙОНЫ
        data = data.drop(columns=['lat', 'lon'])  # Теперь удаляем координаты

    # 2. ЛОГИЧЕСКИЕ ПРИЗНАКИ
    data['floor_ratio'] = data['floor'] / data['floors_total']
    data['living_to_total_ratio'] = data['living_area_m2'] / data['area_total_m2']
    data['kitchen_to_total_ratio'] = data['kitchen_area_m2'] / data['area_total_m2']

    if 'year_built' in data.columns:
        data['building_age'] = 2026 - data['year_built']
        data = data.drop(columns=['year_built'])

    # 3. Обработка NaN в категориях (ДОБАВИЛИ geo_cluster)
    cat_cols = ['building_material', 'condition', 'geo_cluster']
    for col in cat_cols:
        if col in data.columns:
            data[col] = data[col].fillna("Не указано").astype(str)

    # 4. НОВЫЙ ТАРГЕТ (Цена за квадратный метр)
    data['price_mln'] = data['price_total'] / 1_000_000
    data['price_per_m2_calc'] = data['price_total'] / data['area_total_m2']
    data['target_log_prm2'] = np.log1p(data['price_per_m2_calc'])  # <--- ЛОГАРИФМ ЦЕНЫ ЗА МЕТР

    # 5. УДАЛЯЕМ МУСОР (НО ОСТАВЛЯЕМ price_total и area_total_m2 для оценки!)
    cols_to_drop = ['price_per_m2', 'url', 'title', 'photos_s3', 'city','flat_id']
    data = data.drop(columns=[c for c in cols_to_drop if c in data.columns])

    print(f"✅ Признаки созданы. Итоговая размерность: {data.shape}")
    return data


if __name__ == "__main__":
    # Загружаем
    input_path = "../../../data/processed/mirkvartir_moscow_flats_10000.parquet"
    output_path = "../../../data/features/features_10000_final.parquet"

    raw_df = pd.read_parquet(input_path)
    features_df = build_features(raw_df)

    import os

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    features_df.to_parquet(output_path, index=False)
    print(f"💾 Данные с новыми фичами сохранены в {output_path}")