# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np  # <--- ДОБАВИЛИ NUMPY ДЛЯ ПРАВИЛЬНЫХ ПРОПУСКОВ
import pandera as pa
from pandera.errors import SchemaErrors

from flats_schema import FLATS_CLEAN_SCHEMA


def optimize_and_clean(input_path, output_path):
    print(f"🚀 Читаем данные из: {input_path}")
    df = pd.read_csv(input_path, encoding="utf-8-sig")

    # --- 1. ЧИСТИМ КАТЕГОРИИ ---
    allowed_materials = ["монолит", "кирпич", "блок"]
    if "building_material" in df.columns:
        df["building_material"] = df["building_material"].astype(str).str.lower().str.strip()
        df.loc[~df["building_material"].isin(allowed_materials), "building_material"] = pd.NA

    allowed_conditions = ["евроремонт", "требует ремонта", "дизайнерский ремонт"]
    if "condition" in df.columns:
        df["condition"] = df["condition"].astype(str).str.lower().str.strip()
        df.loc[~df["condition"].isin(allowed_conditions), "condition"] = pd.NA

    # --- 2. ЛЕЧИМ НУЛИ (ИСПРАВЛЕНО!) ---

    # 2.1 Площади: 0.0 - это ошибка парсера (кухня не может быть 0 кв.м).
    # Заменяем на np.nan, чтобы Pandera (float64) не ругалась на coerce_dtype!
    float_cols_with_zeros = ["living_area_m2", "kitchen_area_m2"]
    for col in float_cols_with_zeros:
        if col in df.columns:
            df[col] = df[col].replace([0, 0.0], np.nan)

    # 2.2 ВНИМАНИЕ: Колонку 'rooms' мы из этого списка УБРАЛИ!
    # 0 комнат = студия. Это абсолютно легальное значение, мы его сохраняем!

    # Этажи и год: 0 - это тоже ошибка, но для типа Int64 подходит pd.NA
    int_cols_with_zeros = ["floor", "floors_total", "year_built"]
    for col in int_cols_with_zeros:
        if col in df.columns:
            df[col] = df[col].replace([0, 0.0], pd.NA)

    # --- 3. ПРИВОДИМ ТИПЫ К Int64 ---
    int_columns = ["rooms", "floor", "floors_total", "year_built"]
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype("Int64")

        # --- 4. ВАЛИДАЦИЯ PANDERA (С ПОДРОБНЫМ ОТЧЕТОМ ПО ID) ---
        print("💎 Начинаем валидацию по схеме...")
        try:
            clean_df = FLATS_CLEAN_SCHEMA.validate(df, lazy=True)
            print("✅ Все данные идеальны, ошибок нет!")
        except SchemaErrors as err:
            report = err.failure_cases

            print("\n❌ НАЙДЕНЫ ОШИБКИ (АНОМАЛИИ) В ДАННЫХ:")

            if 'column' in report.columns and 'check' in report.columns:
                summary = report.groupby(['column', 'check']).size().reset_index(name='count')
                print(summary.to_string(index=False))

                # --- НОВОЕ: ВЫВОДИМ КОНКРЕТНЫЕ ПРИМЕРЫ С ID ---
                print("\n🔍 ПРИМЕРЫ АНОМАЛИЙ (первые 10):")
                # Если в отчете есть индекс проблемной строки, мы можем достать ее flat_id из исходного df
                if 'index' in report.columns and 'flat_id' in df.columns:
                    # Объединяем отчет об ошибках с исходной таблицей, чтобы подтянуть flat_id и url
                    detailed_report = report.merge(
                        df[['flat_id', 'url']],
                        left_on='index',
                        right_index=True,
                        how='left'
                    )

                    # Показываем самые важные колонки
                    cols_to_show = ['flat_id', 'column', 'check', 'failure_case']
                    print(detailed_report[cols_to_show].head(10).to_string(index=False))

                    # --- НОВОЕ: СОХРАНЯЕМ ПОЛНЫЙ ОТЧЕТ С ID ---
                    os.makedirs(output_path.parent, exist_ok=True)
                    error_file = output_path.parent / "validation_errors_detailed.csv"
                    detailed_report.to_csv(error_file, index=False, encoding="utf-8-sig")
                    print(f"\n📂 Полный отчет с ссылками и ID сохранен в: {error_file}")
                else:
                    print(report.head(10).to_string(index=False))

            # Удаляем аномальные строки
            if 'index' in report.columns:
                invalid_indices = report['index'].dropna().unique()
                print(f"\n🗑️ Удаляем {len(invalid_indices)} аномальных строк...")
                clean_df = df.drop(index=invalid_indices)
            else:
                clean_df = df

    # --- 5. СОРТИРОВКА КОЛОНОК ---
    order = [
        "flat_id", "city", "building_material", "url", "price_total",
        "area_total_m2", "living_area_m2", "kitchen_area_m2",
        "rooms", "floor", "floors_total", "year_built",
        "lat", "lon", "condition", "description_full", "photos_s3"
    ]
    existing_order = [c for c in order if c in clean_df.columns]
    other_cols = [c for c in clean_df.columns if c not in existing_order]
    clean_df = clean_df[existing_order + other_cols]

    # --- 6. СОХРАНЕНИЕ ---
    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_parquet(output_path, index=False, engine="fastparquet")
    csv_output_path = output_path.with_suffix(".csv")
    clean_df.to_csv(csv_output_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ ГОТОВО! Исходно: {len(df)} строк. Осталось чистых: {len(clean_df)} строк.")


if __name__ == "__main__":
    from config import OUTPUT_DIR, FILE_NAME, PROCESSED_DIR, PROCESSED_PARQUET_NAME

    input_file = OUTPUT_DIR / FILE_NAME
    output_file = PROCESSED_DIR / PROCESSED_PARQUET_NAME
    optimize_and_clean(input_file, output_file)