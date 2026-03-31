# -*- coding: utf-8 -*-
from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

# Разумные границы для вторички / новостроек в Москве и области (руб, м², этажи)
PRICE_TOTAL_MIN = 300_000
PRICE_TOTAL_MAX = 10_000_000_000

PRICE_M2_MIN = 20_000
PRICE_M2_MAX = 3_000_000

AREA_MIN = 5.0
AREA_MAX = 10_000.0

ROOMS_MIN = 0
ROOMS_MAX = 20


# --- ПРАВИЛЬНЫЕ ФУНКЦИИ ПРОВЕРКИ (Безопасны для пустых значений NaN) ---

def _floor_le_total_floors(df):
    # Если этаж ИЛИ этажность дома неизвестны (NaN) - пропускаем проверку (True)
    # Иначе проверяем, что этаж <= этажности
    return df["floor"].isna() | df["floors_total"].isna() | (df["floor"] <= df["floors_total"])


def _areas_sum_check(df):
    # Заменяем пустые значения на 0 только на момент сложения
    living = df["living_area_m2"].fillna(0)
    kitchen = df["kitchen_area_m2"].fillna(0)

    # Добавляем погрешность в 0.1 кв.м (прощаем микро-ошибки округления)
    return (living + kitchen) <= (df["area_total_m2"] + 0.1)


FLATS_CLEAN_SCHEMA = DataFrameSchema(
    columns={
        "flat_id": Column(str, nullable=False, coerce=True),
        "city": Column(str, nullable=True, coerce=True),
        "building_material": Column(str, nullable=True, coerce=True),
        "url": Column(str, nullable=False, coerce=True),

        # Обязательные поля (удаляем строку, если тут пусто)
        "price_total": Column(float, Check.between(PRICE_TOTAL_MIN, PRICE_TOTAL_MAX), nullable=False, coerce=True),
        "area_total_m2": Column(float, Check.between(AREA_MIN, AREA_MAX), nullable=True, coerce=True),
        "lat": Column(float, Check.between(54.0, 57.0), nullable=False, coerce=True),
        "lon": Column(float, Check.between(36.0, 42.5), nullable=False, coerce=True),

        # Площади (nullable=True, потому что часто не указаны)
        "living_area_m2": Column(float, nullable=True, coerce=True),
        "kitchen_area_m2": Column(float, nullable=True, coerce=True),

        # Целочисленные поля типа Int64 (nullable=True - разрешаем пропуски!)
        "rooms": Column("Int64", Check.between(0, 30), nullable=False, coerce=True),
        "floor": Column("Int64", Check.between(1, 150, ignore_na=True), nullable=True, coerce=True),
        "floors_total": Column("Int64", Check.between(1, 150, ignore_na=True), nullable=True, coerce=True),
        "year_built": Column("Int64", Check.between(1800, 2030), nullable=False, coerce=True),

        "photos_s3": Column(str, nullable=True, coerce=True),
        "description_full": Column(str, nullable=True, coerce=True),
    },
    checks=[
        pa.Check(_floor_le_total_floors, error="Этаж выше этажности дома"),
        pa.Check(_areas_sum_check, error="Сумма жилой и кухни больше общей площади"),
    ],
    strict=False,
    coerce=True,
)