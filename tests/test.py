# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# Прямой запуск: ``python scraper.py`` — нет пакета, подключаем каталог ``src``.
if __package__ in (None, ""):
    _src_root = Path(__file__).resolve().parents[2]
    _src_s = str(_src_root)
    if _src_s not in sys.path:
        sys.path.insert(0, _src_s)

import json
import re
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from functools import partial
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from my_project.data.config import (
    BASE_DOMAIN,
    BASE_LIST_URL,
    CITY_NAME,
    DETAIL_SLEEP_SEC,
    FILE_NAME,
    FIRST_PAGE,
    LIST_RSC_PARAM,
    LIST_SLEEP_SEC,
    MAX_EMPTY_PAGES,
    MAX_IMAGES,
    MAX_IMAGE_UPLOAD_WORKERS,
    MAX_PAGES,
    MAX_WORKERS,
    MIN_ITEMS,
    OUTPUT_DIR,
    load_s3_config,
)
from my_project.data.http_client import fetch_html, get_thread_session
from my_project.data.s3_storage import create_s3_client, upload_image_to_s3


_RE_METRO = re.compile(r"(?:м\.|метро)\s*([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z0-9\- ]{1,60})", re.IGNORECASE)
_RE_DISTRICT_1 = re.compile(r"(?:р-?н|район)\s*([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z0-9\- ]{2,80})", re.IGNORECASE)
_RE_DISTRICT_2 = re.compile(r"\b([А-ЯЁA-Z][А-ЯЁA-Zа-яёa-z0-9\- ]{2,80})\s+район\b", re.IGNORECASE)


def _clean_loc_token(value: str) -> str:
    v = normalize_spaces(value)
    v = re.sub(r"[\s,;:.]+$", "", v)
    return v


def parse_location_from_text(text: str) -> Dict[str, Any]:
    """
    Локация из текста карточки (листинг): метро/район + сырой кусок location_text.
    Хрупко к вёрстке, но работает как быстрый heuristics.
    """
    t = normalize_spaces(text)
    metro = [_clean_loc_token(m.group(1)) for m in _RE_METRO.finditer(t)]
    district = None
    if m := _RE_DISTRICT_1.search(t):
        district = _clean_loc_token(m.group(1))
    elif m := _RE_DISTRICT_2.search(t):
        district = _clean_loc_token(m.group(1))

    metro_u: List[str] = []
    seen = set()
    for s in metro:
        if not s:
            continue
        key = s.lower()
        if key in seen:
            continue
        seen.add(key)
        metro_u.append(s)

    location_text = None
    if ("м." in t.lower()) or ("метро" in t.lower()) or ("район" in t.lower()) or ("р-н" in t.lower()):
        location_text = t

    return {
        "location_text": location_text,
        "metro": ", ".join(metro_u) if metro_u else None,
        "district": district,
    }


def _upload_flat_images_to_s3(
    raw_images: List[str],
    flat_id: str,
    s3_client: Any,
    bucket_name: str,
) -> List[str]:
    if not raw_images:
        return []

    workers = min(MAX_IMAGE_UPLOAD_WORKERS, len(raw_images))
    if workers <= 1:
        session = get_thread_session()
        out: List[str] = []
        for i, img_url in enumerate(raw_images):
            s3_url = upload_image_to_s3(session, s3_client, bucket_name, CITY_NAME, img_url, flat_id, i)
            if s3_url:
                out.append(s3_url)
        return out

    def worker(idx_url: tuple[int, str]) -> tuple[int, Optional[str]]:
        i, img_url = idx_url
        session = get_thread_session()
        s3_url = upload_image_to_s3(session, s3_client, bucket_name, CITY_NAME, img_url, flat_id, i)
        return i, s3_url

    indexed: List[tuple[int, Optional[str]]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(worker, (i, u)) for i, u in enumerate(raw_images)]
        for fut in as_completed(futures):
            indexed.append(fut.result())

    indexed.sort(key=lambda x: x[0])
    return [u for _, u in indexed if u]


def normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\xa0", " ").replace("\u202f", " ")).strip()


def extract_number(pattern: str, text: str, is_float: bool = False) -> Optional[float]:
    if not text or not (match := re.search(pattern, text.lower())):
        return None

    val_str = re.sub(r"[^\d.,]", "", match.group(1)).replace(",", ".")
    try:
        return float(val_str) if is_float else int(float(val_str))
    except ValueError:
        return None


def extract_entrypoint_data(html: str) -> Optional[Dict[str, Any]]:
    marker = "ReactDOM.hydrate(React.createElement(EstateOfferCardComponents.EntryPoint,"
    if marker not in html:
        return None

    json_part = html.split(marker, 1)[1].lstrip()
    try:
        data, _ = json.JSONDecoder().raw_decode(json_part.replace("\\/", "/"))
        return data
    except json.JSONDecodeError:
        return None


def get_candidate_blocks(soup: BeautifulSoup) -> List[Any]:
    selectors = "article, div[class*='card'], div[class*='item'], div[class*='offer'], div[class*='listing']"
    seen = set()
    blocks = [b for b in soup.select(selectors) if id(b) not in seen and not seen.add(id(b))]
    return blocks or soup.select("article, div")


def parse_list_page(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards: List[Dict[str, Any]] = []
    seen_urls = set()

    for block in get_candidate_blocks(soup):
        text_block = normalize_spaces(block.get_text(" ", strip=True))
        text_l = text_block.lower()
        if not re.search(r"м[²2]", text_l) or "этаж" not in text_l or not re.search(r"[₽]|руб", text_l):
            continue

        title_el = next(
            (
                el
                for el in block.select("a, h3, h2")
                if re.search(r"квартира|студия|\d+\s*[- ]*комн|м[²2]", el.get_text().lower())
            ),
            None,
        )
        title = normalize_spaces(title_el.get_text(" ", strip=True)) if title_el else None
        if not title:
            continue

        url = None
        if title_el and title_el.has_attr("href"):
            href = title_el["href"]
            url = href if href.startswith("http") else f"{BASE_DOMAIN}{href}"

        price_text = normalize_spaces(
            next(
                (
                    c.get_text(" ", strip=True)
                    for c in block.find_all(["div", "span"])
                    if re.search(r"₽|руб", c.get_text().lower())
                ),
                text_block,
            )
        )

        loc = parse_location_from_text(text_block)
        card = {
            "title": title,
            "url": url,
            "location_text": loc.get("location_text"),
            "district": loc.get("district"),
            "metro": loc.get("metro"),
            "price_total": extract_number(r"([\d\s]+)\s*(?:₽|руб)", price_text),
            "price_per_m2": extract_number(r"([\d\s]+)\s*(?:₽/м|руб\s*/\s*м)", price_text),
            "rooms": 0 if "студия" in title.lower() else extract_number(r"(\d+)\s*[- ]*комн", title),
            "area_total_m2": extract_number(r"(\d+[.,]?\d*)\s*м[²2]\s*площад", text_l, True)
            or extract_number(r"(\d+[.,]?\d*)\s*м[²2]", title, True),
            "kitchen_area_m2": extract_number(r"(\d+[.,]?\d*)\s*м[²2]\s*кух", text_l, True),
            "floor": extract_number(r"(\d+)\s*/", title),
            "floors_total": extract_number(r"/\s*(\d+)\s*этаж", title),
            "city": CITY_NAME,
            "ceiling_height_m": None,
        }

        if url and url not in seen_urls:
            seen_urls.add(url)
            cards.append(card)

    return cards


def parse_detail_page(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")
    card_data = extract_entrypoint_data(html) or {}
    offer = card_data.get("offerProperties") or {}
    coord = card_data.get("coordinate") or {}

    info_container = soup.select_one(
        "[data-testid*='info'], .object-info, .offer__info, .object-params, .b-object-info"
    )
    info_text = info_container.get_text("\n", strip=True) if info_container else soup.get_text("\n", strip=True)

    desc_full = card_data.get("description", "")
    h_pattern = (
        r"(?:высота потолков|высота потолка|потолки)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*м"
        r"|(\d+(?:[.,]\d+)?)\s*м\s*(?:высот[аи]\s+потолк|потолк[аи])"
    )

    bad_img_words = {"icon", "logo", "sprite", "placeholder", "/320x240/", "newbuildings"}
    raw_images = [img.get("data-src") or img.get("src", "") for img in soup.find_all("img")]
    image_urls = list(
        dict.fromkeys(
            (
                src
                if src.startswith("http")
                else f"https:{src}"
                if src.startswith("//")
                else f"{BASE_DOMAIN}{src}"
            )
            for src in raw_images
            if src
            and src.startswith(("http", "//", "/"))
            and "media.mirkvartir.me/custom/" in src
            and not any(w in src.lower() for w in bad_img_words)
        )
    )

    return {
        "description_full": desc_full,
        "raw_image_urls": image_urls,
        "year_built": offer.get("buildingYear"),
        "price_total": offer.get("price") or card_data.get("price"),
        "price_per_m2": offer.get("priceM2"),
        "area_total_m2": offer.get("area"),
        "kitchen_area_m2": offer.get("areaKitchen"),
        "living_area_m2": offer.get("areaLive"),
        "floor": offer.get("floor"),
        "floors_total": offer.get("floorsTotal"),
        "lat": coord.get("lat"),
        "lon": coord.get("lon"),
        "city": CITY_NAME,
        "ceiling_height_m": extract_number(h_pattern, desc_full, True)
        or extract_number(h_pattern, soup.get_text(" ", strip=True), True),
        "condition": (m.group(1).strip() if (m := re.search(r"Состояние\s+([^\n]+)", info_text)) else None),
    }


def process_single_flat(
    item: Dict[str, Any],
    index: int,
    total: int,
    *,
    s3_client,
    bucket_name: str,
) -> Dict[str, Any]:
    url = item.get("url")
    if not url:
        return item

    flat_id_match = re.search(r"/(\d+)/?$", url)
    flat_id = flat_id_match.group(1) if flat_id_match else None
    if not flat_id:
        print(f"[{index}/{total}] Не удалось извлечь ID из URL: {url}, пропускаем...")
        return item

    try:
        detail_html = fetch_html(url, sleep_sec=DETAIL_SLEEP_SEC)
        detail_data = parse_detail_page(detail_html)
        raw_images = detail_data.pop("raw_image_urls", [])[:MAX_IMAGES]

        s3_links = _upload_flat_images_to_s3(raw_images, flat_id, s3_client, bucket_name)

        item["flat_id"] = flat_id
        item["photos_s3"] = ", ".join(s3_links) if s3_links else None
        item.update({k: v for k, v in detail_data.items() if v is not None})

        print(f"[{index}/{total}] Готово: {url} (ID: {flat_id}, Фото: {len(s3_links)})")
    except Exception as e:
        print(f"[{index}/{total}] Ошибка при разборе {url}: {e}")

    return item


def scrape_mirkvartir_moscow(
    max_pages: int = 10,
    min_items: int = 300,
    with_details: bool = True,
    start_page: int = FIRST_PAGE,
) -> pd.DataFrame:
    all_items: List[Dict[str, Any]] = []
    seen_urls = set()
    empty_pages = 0

    for page in range(start_page, max_pages + 1):
        print(f"Скачиваю страницу списка p={page}...")
        html = fetch_html(
            BASE_LIST_URL,
            params={"_rsc": LIST_RSC_PARAM, "p": page},
            sleep_sec=LIST_SLEEP_SEC,
        )
        cards = parse_list_page(html)
        print(f"  Найдено карточек на странице: {len(cards)}")

        new_items = 0
        for it in cards:
            if (u := it.get("url")) and u not in seen_urls:
                seen_urls.add(u)
                all_items.append(it)
                new_items += 1

        empty_pages = 0 if new_items else empty_pages + 1
        print(f"  Всего уникальных объявлений: {len(all_items)}")

        if len(all_items) >= min_items or empty_pages >= MAX_EMPTY_PAGES:
            print("Достигли нужного числа объявлений или пустых страниц. Переходим к деталям.")
            break

    if with_details:
        s3_cfg = load_s3_config()
        if not s3_cfg:
            raise RuntimeError(
                "Missing S3 environment variables (S3_ENDPOINT_URL, S3_ACCESS_KEY, "
                "S3_SECRET_KEY, S3_BUCKET_NAME). They are required when with_details=True."
            )
        s3_client = create_s3_client(s3_cfg)
        bucket_name = s3_cfg.bucket_name

        print(f"\n🚀 Запускаем многопоточный парсинг деталей и загрузку в S3 ({MAX_WORKERS} потоков)...")
        worker = partial(process_single_flat, s3_client=s3_client, bucket_name=bucket_name)
        future_to_index: Dict[Future, int] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i, item in enumerate(all_items, 1):
                fut = executor.submit(worker, item, i, len(all_items))
                future_to_index[fut] = i - 1

            results: List[Optional[Dict[str, Any]]] = [None] * len(all_items)
            for future in as_completed(future_to_index):
                idx = future_to_index[future]
                results[idx] = future.result()
        all_items = [r for r in results if r is not None]

    return pd.DataFrame(all_items)


def main() -> None:
    df = scrape_mirkvartir_moscow(max_pages=MAX_PAGES, min_items=MIN_ITEMS)

    print(f"\nИтого собрано объявлений: {len(df)}")

    desired_order = [
        "flat_id",
        "city",
        "url",
        "title",
        "location_text",
        "district",
        "metro",
        "price_total",
        "price_per_m2",
        "area_total_m2",
        "living_area_m2",
        "kitchen_area_m2",
        "rooms",
        "floor",
        "floors_total",
        "year_built",
        "ceiling_height_m",
        "condition",
        "lat",
        "lon",
        "description_full",
        "photos_s3",
    ]
    other_cols = [col for col in df.columns if col not in desired_order]
    final_order = desired_order + other_cols
    df = df.reindex(columns=final_order)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / FILE_NAME
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"Данные сохранены в файл: {output_file}")


if __name__ == "__main__":
    main()
