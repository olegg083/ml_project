# -*- coding: utf-8 -*-
import time
import re
import json
import random
from typing import List, Dict, Optional, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd

CITY_NAME = "Москва"
CITY_PATH = "Москва"
BASE_LIST_URL = f"https://www.mirkvartir.ru/{CITY_PATH}/"
BASE_DOMAIN = "https://www.mirkvartir.ru"
REQUEST_TIMEOUT = (5, 20)
LIST_SLEEP_SEC = 1.2
DETAIL_SLEEP_SEC = 2.0
MAX_EMPTY_PAGES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
}


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_SESSION = build_session()


def fetch_html(url: str, params: Optional[dict] = None, sleep_sec: float = 1.5,
               session: Optional[requests.Session] = None) -> str:
    if sleep_sec > 0:
        time.sleep(sleep_sec + random.uniform(0.0, 0.4))
    sess = session or _SESSION
    resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s.replace("\xa0", " ").replace("\u202f", " ")).strip()


def extract_number(pattern: str, text: str, is_float: bool = False) -> Optional[float]:
    """Универсальная функция для поиска и конвертации числа из строки по регулярке."""
    if not text or not (match := re.search(pattern, text.lower())):
        return None
    val_str = re.sub(r"[^\d.,]", "", match.group(1)).replace(",", ".")
    try:
        return float(val_str) if is_float else int(float(val_str))
    except ValueError:
        return None


def extract_entrypoint_data(html: str) -> Optional[Dict[str, Any]]:
    """Извлекает JSON из React hydrate без ручного перебора скобок."""
    marker = "ReactDOM.hydrate(React.createElement(EstateOfferCardComponents.EntryPoint,"
    if marker not in html:
        return None

    json_part = html.split(marker, 1)[1].lstrip()
    try:
        # raw_decode автоматически находит конец валидного JSON-объекта
        data, _ = json.JSONDecoder().raw_decode(json_part.replace("\\/", "/"))
        return data
    except json.JSONDecodeError:
        return None


def get_candidate_blocks(soup: BeautifulSoup) -> List[Any]:
    selectors = "article, div[class*='card'], div[class*='item'], div[class*='offer'], div[class*='listing']"
    seen = set()
    # Собираем блоки, попутно отсеивая дубликаты по ID объекта в памяти
    blocks = [b for b in soup.select(selectors) if id(b) not in seen and not seen.add(id(b))]
    return blocks or soup.select("article, div")


def parse_list_page(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    cards = []
    seen_urls = set()

    for block in get_candidate_blocks(soup):
        text_block = normalize_spaces(block.get_text(" ", strip=True))
        text_l = text_block.lower()

        # Быстрые фильтры
        if not re.search(r"м[²2]", text_l) or "этаж" not in text_l or not re.search(r"[₽]|руб", text_l):
            continue

        # Поиск заголовка
        title_el = next((el for el in block.select("a, h3, h2")
                         if re.search(r"квартира|студия|\d+\s*[- ]*комн|м[²2]", el.get_text().lower())), None)
        title = normalize_spaces(title_el.get_text(" ", strip=True)) if title_el else None
        if not title:
            continue

        url = None
        if title_el and title_el.has_attr("href"):
            href = title_el["href"]
            url = href if href.startswith("http") else f"{BASE_DOMAIN}{href}"

        # Парсинг цен и площадей одной строкой через хелпер
        price_text = normalize_spaces(
            next((c.get_text(" ", strip=True) for c in block.find_all(["div", "span"])
                  if re.search(r"₽|руб", c.get_text().lower())), text_block)
        )

        rooms = 0 if "студия" in title.lower() else extract_number(r"(\d+)\s*[- ]*комн", title)
        floor = extract_number(r"(\d+)\s*/", title)
        floors_total = extract_number(r"/\s*(\d+)\s*этаж", title)

        card = {
            "title": title,
            "url": url,
            "price_total": extract_number(r"([\d\s]+)\s*(?:₽|руб)", price_text),
            "price_per_m2": extract_number(r"([\d\s]+)\s*(?:₽/м|руб\s*/\s*м)", price_text),
            "rooms": rooms,
            "area_total_m2": extract_number(r"(\d+[.,]?\d*)\s*м[²2]\s*площад", text_l, True) or extract_number(
                r"(\d+[.,]?\d*)\s*м[²2]", title, True),
            "kitchen_area_m2": extract_number(r"(\d+[.,]?\d*)\s*м[²2]\s*кух", text_l, True),
            "floor": floor,
            "floors_total": floors_total,
            "location_text": next((normalize_spaces(c.get_text()) for c in block.find_all(["div", "span"])
                                   if re.search(r"москва|ул\.|пр\.", c.get_text().lower())), None),
            "description_short": next((normalize_spaces(c.get_text()) for c in block.find_all(["p", "div"])
                                       if len(c.get_text()) > 60 and "₽" not in c.get_text()), None),
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
        "[data-testid*='info'], .object-info, .offer__info, .object-params, .b-object-info")
    info_text = info_container.get_text("\n", strip=True) if info_container else soup.get_text("\n", strip=True)

    desc_full = card_data.get("description", "")
    h_pattern = r"(?:высота потолков|высота потолка|потолки)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*м|(\d+(?:[.,]\d+)?)\s*м\s*(?:высот[аи]\s+потолк|потолк[аи])"

    # Сбор и фильтрация изображений
    bad_img_words = {"icon", "logo", "sprite", "placeholder", "/320x240/", "newbuildings"}
    raw_images = [img.get("data-src") or img.get("src", "") for img in soup.find_all("img")]
    image_urls = list(dict.fromkeys(
        (src if src.startswith("http") else f"https:{src}" if src.startswith("//") else f"{BASE_DOMAIN}{src}")
        for src in raw_images if src and src.startswith(("http", "//", "/"))
        and "media.mirkvartir.me/custom/" in src
        and not any(w in src.lower() for w in bad_img_words)
    ))

    return {
        "description_full": desc_full,
        "image_urls": image_urls,
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
        "ceiling_height_m": extract_number(h_pattern, desc_full, True) or extract_number(h_pattern,
                                                                                         soup.get_text(" ", strip=True),
                                                                                         True),
        "house_info": re.sub(r"[,\s]*(19|20)\d{2}\s*г\.?", "", offer.get("material", "")).strip(" ,;") or None,
        "condition": (m.group(1).strip() if (m := re.search(r"Состояние\s+([^\n]+)", info_text)) else None),
    }


def scrape_mirkvartir_moscow(max_pages: int = 10, min_items: int = 300, with_details: bool = True) -> pd.DataFrame:
    all_items = []
    seen_urls = set()
    empty_pages = 0

    for page in range(1, max_pages + 1):
        print(f"Скачиваю страницу списка p={page}...")
        html = fetch_html(BASE_LIST_URL, params={"_rsc": "140g3", "p": page}, sleep_sec=LIST_SLEEP_SEC)
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
            print("Достигли нужного числа объявлений или пустых страниц. Останавливаемся.")
            break

    if with_details:
        print("Загружаю подробности по каждому объявлению (описание и фото)...")
        for i, item in enumerate(all_items, start=1):
            if not (url := item.get("url")): continue
            print(f"[{i}/{len(all_items)}] Детальная страница: {url}")
            try:
                detail_html = fetch_html(url, sleep_sec=DETAIL_SLEEP_SEC)
                detail_data = parse_detail_page(detail_html)
                if images := detail_data.pop("image_urls", []):
                    item["image_urls"] = json.dumps(images, ensure_ascii=False)
                item.update({k: v for k, v in detail_data.items() if v is not None})
            except Exception as e:
                print(f"  Ошибка при разборе {url}: {e}")

    return pd.DataFrame(all_items)


def main():
    df = scrape_mirkvartir_moscow(max_pages=100, min_items=30, with_details=True)
    print(f"Итого собрано объявлений: {len(df)}")
    output_file = "../data/raw/mirkvartir_moscow_flats_test.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Данные сохранены в файл: {output_file}")
#Скачиваю страницу списка p=30...

if __name__ == "__main__":
    main()