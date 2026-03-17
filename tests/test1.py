# -*- coding: utf-8 -*-
import time
import re
import json
import random
from typing import List, Dict, Optional

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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_SESSION = build_session()


def fetch_html(
    url: str,
    params: Optional[dict] = None,
    sleep_sec: float = 1.5,
    session: Optional[requests.Session] = None,
) -> str:
    if sleep_sec and sleep_sec > 0:
        time.sleep(sleep_sec + random.uniform(0.0, 0.4))
    sess = session or _SESSION
    resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def normalize_spaces(s: str) -> str:
    s = s.replace("\xa0", " ").replace("\u202f", " ").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip()


def strip_year_from_text(value: str) -> str:
    value = re.sub(r"[,\s]*(19|20)\d{2}\s*г\.?", "", value)
    return value.strip(" ,;")


def extract_ceiling_height(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    t = text.lower()
    patterns = [
        r"(?:высота потолков|высота потолка|потолки)\s*[:\-]?\s*(\d+(?:[.,]\d+)?)\s*м",
        r"(\d+(?:[.,]\d+)?)\s*м\s*(?:высот[аи]\s+потолк|потолк[аи])",
    ]
    for pat in patterns:
        m = re.search(pat, t)
        if m:
            return parse_float(m.group(1))
    return None




def extract_entrypoint_data(html: str) -> Optional[Dict]:
    marker = "ReactDOM.hydrate(React.createElement(EstateOfferCardComponents.EntryPoint,"
    idx = html.find(marker)
    if idx == -1:
        return None
    start = html.find("{", idx)
    if start == -1:
        return None
    depth = 0
    in_str = False
    escape = False
    end = None
    for i in range(start, len(html)):
        ch = html[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end is None:
        return None
    json_text = html[start:end].replace("\\/", "/")
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        return None


def parse_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = normalize_spaces(s)
    s = re.sub(r"[^\d.,]", "", s)
    s = s.replace(",", ".")
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    if not nums:
        return None
    try:
        return float(nums[0])
    except ValueError:
        return None


def parse_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = normalize_spaces(s)
    nums = re.findall(r"\d+", s)
    if not nums:
        return None
    try:
        number_str = "".join(nums)
        return int(number_str)
    except ValueError:
        return None


def parse_rooms_from_title(title: str) -> Optional[int]:
    title = title.lower()
    if "студия" in title:
        return 0
    m = re.search(r"(\d+)\s*[- ]*комн", title)
    if m:
        return int(m.group(1))
    return None


def parse_area_from_title(title: str) -> Optional[float]:
    m = re.search(r"(\d+[.,]?\d*)\s*м[²2]", title)
    if not m:
        return None
    return parse_float(m.group(1))


def parse_floor_from_title(title: str):
    m = re.search(r"(\d+)\s*/\s*(\d+)\s*этаж", title.lower())
    if not m:
        return None, None
    floor = parse_int(m.group(1))
    floors_total = parse_int(m.group(2))
    return floor, floors_total


def get_candidate_blocks(soup: BeautifulSoup):
    selectors = [
        "article",
        "div[class*='card']",
        "div[class*='item']",
        "div[class*='offer']",
        "div[class*='listing']",
    ]
    blocks = []
    seen = set()
    for sel in selectors:
        for block in soup.select(sel):
            bid = id(block)
            if bid in seen:
                continue
            seen.add(bid)
            blocks.append(block)
    if not blocks:
        blocks = soup.select("article, div")
    return blocks


def parse_list_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    cards: List[Dict] = []
    raw_cards = get_candidate_blocks(soup)
    seen_urls = set()

    for block in raw_cards:
        text_block = block.get_text(" ", strip=True)
        text_block = normalize_spaces(text_block) if text_block else ""
        if not text_block:
            continue
        text_block_l = text_block.lower()
        if "м²" not in text_block_l and "м2" not in text_block_l:
            continue
        if "этаж" not in text_block_l:
            continue
        if "₽" not in text_block_l and not re.search(r"\bруб", text_block_l):
            continue

        total_area = None
        kitchen_area = None

        m_tot = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*площад", text_block_l)
        if m_tot:
            total_area = parse_float(m_tot.group(1))

        m_k = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*кух", text_block_l)
        if m_k:
            kitchen_area = parse_float(m_k.group(1))

        title_el = None
        for sel in ["a", "h3", "h2"]:
            cand = block.find(sel)
            if cand and (
                "квартира" in cand.get_text().lower()
                or "студия" in cand.get_text().lower()
                or re.search(r"\d+\s*[- ]*комн", cand.get_text().lower())
            ):
                title_el = cand
                break

        if not title_el:
            for a in block.find_all("a"):
                if "м²" in a.get_text() or "м2" in a.get_text():
                    title_el = a
                    break

        title = normalize_spaces(title_el.get_text(" ", strip=True)) if title_el else None
        if not title:
            continue

        url = None
        if title_el and title_el.has_attr("href"):
            href = title_el["href"]
            if href.startswith("http"):
                url = href
            else:
                url = BASE_DOMAIN + href

        price_total = None
        price_el = None
        for cand in block.find_all(["div", "span"]):
            txt = cand.get_text(" ", strip=True)
            if "₽" in txt or "руб" in txt.lower():
                price_el = cand
                break
        price_text = normalize_spaces(price_el.get_text(" ", strip=True)) if price_el else text_block
        if price_text:
            m_price = re.search(r"([\d\s]+)\s*₽", price_text)
            if m_price:
                price_total = parse_int(m_price.group(1))
            if price_total is None:
                m_price = re.search(r"([\d\s]+)\s*руб", price_text.lower())
                if m_price:
                    price_total = parse_int(m_price.group(1))

        price_per_m2 = None
        if price_text:
            m_m2 = re.search(r"([\d\s]+)\s*₽/м", price_text)
            if m_m2:
                price_per_m2 = parse_int(m_m2.group(1))
            if price_per_m2 is None:
                m_m2 = re.search(r"([\d\s]+)\s*руб\s*/\s*м", price_text.lower())
                if m_m2:
                    price_per_m2 = parse_int(m_m2.group(1))

        description_short = None
        for cand in block.find_all(["p", "div"]):
            txt = normalize_spaces(cand.get_text(" ", strip=True)) if cand else ""
            if not txt:
                continue
            if len(txt) > 60 and "₽" not in txt and "м²" not in txt:
                description_short = txt
                break

        location_text = None
        for cand in block.find_all(["div", "span"]):
            txt = normalize_spaces(cand.get_text(" ", strip=True)) if cand else ""
            if not txt:
                continue
            if "москва" in txt.lower() or "ул." in txt.lower() or "пр." in txt.lower():
                location_text = txt
                break

        rooms = parse_rooms_from_title(title)
        if total_area is None:
            total_area = parse_area_from_title(title)
        floor, floors_total = parse_floor_from_title(title)

        card = {
            "title": title,
            "url": url,
            "price_total": price_total,
            "price_per_m2": price_per_m2,
            "rooms": rooms,
            "area_total_m2": total_area,
            "kitchen_area_m2": kitchen_area,
            "floor": floor,
            "floors_total": floors_total,
            "location_text": location_text,
            "description_short": description_short,
            "city": CITY_NAME,
            "ceiling_height_m": None,
        }

        if url and url not in seen_urls:
            seen_urls.add(url)
            cards.append(card)

    return cards


def parse_detail_page(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")

    card_data = extract_entrypoint_data(html) or {}
    offer = card_data.get("offerProperties") or {}
    lat = None
    lon = None
    coord = card_data.get("coordinate")
    if isinstance(coord, dict):
        lat = coord.get("lat")
        lon = coord.get("lon")
    city = CITY_NAME

    description_full = card_data.get("description")

    total_area = offer.get("area")
    kitchen_area = offer.get("areaKitchen")
    living_area = offer.get("areaLive")

    floor = offer.get("floor")
    floors_total = offer.get("floorsTotal")

    house_info = offer.get("material")
    year_built = offer.get("buildingYear")
    if house_info:
        house_info = strip_year_from_text(house_info)

    price_total = offer.get("price") or card_data.get("price")
    price_per_m2 = offer.get("priceM2")

    full_text = soup.get_text(" ", strip=True)
    ceiling_height_m = extract_ceiling_height(description_full)
    if ceiling_height_m is None:
        ceiling_height_m = extract_ceiling_height(full_text)

    info_text = full_text
    info_container = soup.select_one(
        "[data-testid*='info'], .object-info, .offer__info, .object-params, .b-object-info"
    )
    if info_container:
        info_text = info_container.get_text("\n", strip=True)

    condition = None
    m_cond = re.search(r"Состояние\s+([^\n]+)", info_text)
    if m_cond:
        condition = m_cond.group(1).strip()
    if condition and ("м²" in condition or "₽" in condition):
        condition = None
    if condition is None and description_full:
        cond_map = [
            (r"требует\s+ремонт", "требует ремонта"),
            (r"евроремонт", "евроремонт"),
            (r"дизайнерск[а-я]*\s+ремонт", "дизайнерский ремонт"),
        ]
        for pat, label in cond_map:
            if re.search(pat, description_full, re.IGNORECASE):
                condition = label
                break

    image_urls: List[str] = []
    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src")
        if not src:
            continue
        lname = src.lower()
        if any(bad in lname for bad in ["icon", "logo", "sprite", "placeholder"]):
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            src = BASE_DOMAIN + src
        if not src.startswith("https://media.mirkvartir.me/custom/"):
            continue
        if "/320x240/" in src:
            continue
        if "newbuildings" in src:
            continue
        image_urls.append(src)

    image_urls = list(dict.fromkeys(image_urls))

    result = {
        "description_full": description_full,
        "image_urls": image_urls,
        "year_built": year_built,
        "price_total": price_total,
        "price_per_m2": price_per_m2,
        "area_total_m2": total_area,
        "kitchen_area_m2": kitchen_area,
        "living_area_m2": living_area,
        "floor": floor,
        "floors_total": floors_total,
        "lat": lat,
        "lon": lon,
        "city": city,
        "ceiling_height_m": ceiling_height_m,
        "house_info": house_info,
        "condition": condition,
    }
    return result


def scrape_mirkvartir_moscow(
    max_pages: int = 10,
    min_items: int = 300,
    with_details: bool = True,
    session: Optional[requests.Session] = None,
) -> pd.DataFrame:
    all_items: List[Dict] = []
    seen_urls = set()
    empty_pages = 0
    sess = session or _SESSION

    for page in range(1, max_pages + 1):
        print(f"Скачиваю страницу списка p={page}...")
        html = fetch_html(
            BASE_LIST_URL,
            params={"_rsc": "140g3", "p": page},
            sleep_sec=LIST_SLEEP_SEC,
            session=sess,
        )
        cards = parse_list_page(html)
        print(f"  Найдено карточек на странице: {len(cards)}")

        new_items = 0
        for it in cards:
            u = it.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                all_items.append(it)
                new_items += 1
        if new_items == 0:
            empty_pages += 1
        else:
            empty_pages = 0

        print(f"  Всего уникальных объявлений: {len(all_items)}")

        if len(all_items) >= min_items:
            print("Достигли нужного числа объявлений, останавливаемся по списку.")
            break
        if empty_pages >= MAX_EMPTY_PAGES:
            print("Несколько пустых страниц подряд, останавливаемся по списку.")
            break

    if with_details:
        print("Загружаю подробности по каждому объявлению (описание и фото)...")
        for i, item in enumerate(all_items, start=1):
            url = item.get("url")
            if not url:
                continue
            print(f"[{i}/{len(all_items)}] Детальная страница: {url}")
            try:
                detail_html = fetch_html(url, sleep_sec=DETAIL_SLEEP_SEC, session=sess)
                detail_data = parse_detail_page(detail_html)
                image_urls = detail_data.pop("image_urls", None)
                if image_urls is not None:
                    item["image_urls"] = json.dumps(image_urls, ensure_ascii=False)
                for k, v in detail_data.items():
                    if v is not None:
                        item[k] = v
            except Exception as e:
                print(f"  Ошибка при разборе {url}: {e}")
                continue

    df = pd.DataFrame(all_items)
    return df


def main():
    max_pages = 1
    min_items = 500
    with_details = True

    df = scrape_mirkvartir_moscow(
        max_pages=max_pages,
        min_items=min_items,
        with_details=with_details,
    )

    print(f"Итого собрано объявлений: {len(df)}")
    output_file = "mirkvartir_moscow_flats_14.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Данные сохранены в файл: {output_file}")


if __name__ == "__main__":
    main()

