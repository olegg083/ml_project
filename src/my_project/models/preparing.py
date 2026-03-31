# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# Координаты нулевого километра (Кремль)
KREMLIN_LAT = 55.7558
KREMLIN_LON = 37.6173


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Вычисляет расстояние (в км) между двумя точками на Земле.
    """
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * (2 * np.arcsin(np.sqrt(a)))


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    print("🛠 Начинаем Feature Engineering...")
    data = df.copy()  # Делаем копию, чтобы не портить оригинал

    # 1. Удаляем утечку таргета и текст (ВСЕ ИЗМЕНЕНИЯ ДЕЛАЕМ С data)
    cols_to_drop = ['price_per_m2', 'url', 'flat_id', 'title', 'photos_s3', 'city', 'description_full']
    data = data.drop(columns=[c for c in cols_to_drop if c in data.columns])

    # 2. ГЕО-ПРИЗНАКИ И УДАЛЕНИЕ ДУБЛИКАТОВ (lat/lon)
    if 'lat' in data.columns and 'lon' in data.columns:
        data['distance_to_center_km'] = haversine_distance(data['lat'], data['lon'], KREMLIN_LAT, KREMLIN_LON)
        data = data.drop(columns=['lat', 'lon'])  # Удалили координаты

    # 3. ЛОГИЧЕСКИЕ ПРИЗНАКИ И УДАЛЕНИЕ ДУБЛИКАТА (year_built)
    data['floor_ratio'] = data['floor'] / data['floors_total']
    data['living_to_total_ratio'] = data['living_area_m2'] / data['area_total_m2']
    data['kitchen_to_total_ratio'] = data['kitchen_area_m2'] / data['area_total_m2']

    if 'year_built' in data.columns:
        data['building_age'] = 2024 - data['year_built']
        data = data.drop(columns=['year_built'])  # Удалили год постройки

    # 4. Обработка NaN в категориях
    cat_cols = ['building_material', 'condition']
    for col in cat_cols:
        if col in data.columns:
            data[col] = data[col].fillna("Не указано").astype(str)

    # 5. ЦЕЛЕВАЯ ПЕРЕМЕННАЯ (ТАРГЕТ)
    # Переводим цену в миллионы и ЛОГАРИФМИРУЕМ
    data['price_mln'] = data['price_total'] / 1_000_000
    data['price_log'] = np.log1p(data['price_mln'])  # log(1 + x)

    print(f"✅ Признаки созданы. Итоговая размерность: {data.shape}")
    return data  # Возвращаем ИЗМЕНЕННЫЙ датасет


if __name__ == "__main__":
    # Загружаем
    input_path = "../../../data/processed/mirkvartir_moscow_flats_10000.parquet"
    output_path = "../../../data/features/features_10000.parquet"

    print(f"Загружаем сырые данные из {input_path}")
    raw_df = pd.read_parquet(input_path)

    # Применяем фичи
    features_df = build_features(raw_df)

    # Сохраняем
    features_df.to_parquet(output_path, index=False)
    print(f"💾 Данные с новыми фичами сохранены в {output_path}")

    # Проверяем результат (должны увидеть distance_to_center_km и price_log)
    print(features_df.head(3))