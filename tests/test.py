import time
import re
import json
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd


BASE_LIST_URL = "https://www.mirkvartir.ru/Москва/"
BASE_DOMAIN = "https://www.mirkvartir.ru"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}


def fetch_html(url: str, params: Optional[dict] = None, sleep_sec: float = 1.5) -> str:
    time.sleep(sleep_sec)
    resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", " ")
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
    s = s.replace("\xa0", " ").replace(" ", " ")
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


def clean_text(el) -> Optional[str]:
    if not el:
        return None
    txt = el.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt) if txt else None


def parse_list_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    cards: List[Dict] = []
    raw_cards = soup.select("article, div")

    for block in raw_cards:
        text_block = block.get_text(" ", strip=True).lower()
        if not text_block:
            continue
        if "м²" not in text_block and "м2" not in text_block:
            continue
        if "этаж" not in text_block:
            continue
        if " ₽" not in text_block:
            continue

        # извлекаем общую и кухонную площадь из текста карточки
        total_area = None
        kitchen_area = None

        m_tot = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*площад", text_block)
        if m_tot:
            total_area = parse_float(m_tot.group(1))

        m_k = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*кух", text_block)
        if m_k:
            kitchen_area = parse_float(m_k.group(1))

        title_el = None
        for sel in ["a", "h3", "h2"]:
            cand = block.find(sel)
            if cand and ("квартира" in cand.get_text().lower() or "студия" in cand.get_text().lower()):
                title_el = cand
                break

        if not title_el:
            for a in block.find_all("a"):
                if "м²" in a.get_text() or "м2" in a.get_text():
                    title_el = a
                    break

        title = clean_text(title_el)
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
            if "₽" in txt:
                price_el = cand
                break
        price_text = clean_text(price_el)
        if price_text:
            m_price = re.search(r"([\d\s]+)\s*₽", price_text)
            if m_price:
                price_total = parse_int(m_price.group(1))

        price_per_m2 = None
        if price_text:
            m_m2 = re.search(r"([\d\s]+)\s*₽/м", price_text)
            if m_m2:
                price_per_m2 = parse_int(m_m2.group(1))

        description_short = None
        for cand in block.find_all(["p", "div"]):
            txt = clean_text(cand)
            if not txt:
                continue
            if len(txt) > 60 and "₽" not in txt and "м²" not in txt:
                description_short = txt
                break

        location_text = None
        for cand in block.find_all(["div", "span"]):
            txt = clean_text(cand)
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
        }

        if url and not any(c.get("url") == url for c in cards):
            cards.append(card)

    return cards


def parse_detail_page(html: str) -> Dict:
    soup = BeautifulSoup(html, "lxml")

    # ---------- ТЕКСТОВОЕ ОПИСАНИЕ ----------
    description_full = None
    desc_el = soup.select_one('[itemprop="description"]')
    if desc_el:
        description_full = clean_text(desc_el)
    else:
        # Собираем несколько подходящих абзацев и склеиваем, чтобы текст не обрезался
        candidates = soup.find_all(["div", "section", "p"])
        blocks = []
        for cand in candidates:
            txt = clean_text(cand)
            if not txt:
                continue
            if len(txt) >= 150 and "подробнее" not in txt.lower():
                blocks.append(txt)
        if blocks:
            description_full = "\n\n".join(blocks)

    # Для структурных признаков берём только блок "Информация о квартире",
    # чтобы не ловить "шум" из рекомендованных объявлений.
    full_text = soup.get_text("\n", strip=True)
    info_text = full_text
    start_idx = full_text.find("Информация о квартире")
    if start_idx != -1:
        info_text = full_text[start_idx:]
        end_idx = len(info_text)
        for marker in [
            "История цены этого объявления",
            "История цены",
            "Связаться с продавцом",
        ]:
            m_pos = info_text.find(marker)
            if m_pos != -1 and m_pos < end_idx:
                end_idx = m_pos
        info_text = info_text[:end_idx]

    info_text_lower = info_text.lower()

    # ---------- ПЛОЩАДИ ----------
    total_area = None
    kitchen_area = None
    living_area = None

    m_tot = re.search(r"Площадь\s+(\d+[.,]?\d*)\s*м[²2]", info_text)
    if m_tot:
        total_area = parse_float(m_tot.group(1))

    # кухня 11 м²  ИЛИ  11 м² кухня
    m_k = re.search(r"кухн[яи]\s+(\d+[.,]?\d*)\s*м[²2]", info_text_lower, re.IGNORECASE)
    if not m_k:
        m_k = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*кухн[яи]", info_text_lower, re.IGNORECASE)
    if m_k:
        kitchen_area = parse_float(m_k.group(1))

    # жилая 18 м²  ИЛИ  18 м² жилая
    m_l = re.search(r"жил[а-я]*\s+(\d+[.,]?\d*)\s*м[²2]", info_text_lower, re.IGNORECASE)
    if not m_l:
        m_l = re.search(r"(\d+[.,]?\d*)\s*м[²2]\s*жил[а-я]*", info_text_lower, re.IGNORECASE)
    if m_l:
        living_area = parse_float(m_l.group(1))

    # ---------- ЭТАЖ / ЭТАЖНОСТЬ ----------
    floor = None
    floors_total = None
    m_floor = re.search(r"Этаж\s+(\d+)\s+из\s+(\d+)", info_text)
    if m_floor:
        floor = parse_int(m_floor.group(1))
        floors_total = parse_int(m_floor.group(2))

    # ---------- ДОМ / ТИП / ГОД ПОСТРОЙКИ ----------
    house_info = None
    year_built = None

    m_house_line = re.search(r"Дом\s+([^\n]+)", info_text)
    if m_house_line:
        house_info_raw = m_house_line.group(1).strip()
        # отделяем тип дома от года: "кирпич, 1958 г." -> "кирпич"
        house_type = house_info_raw
        m_split = re.search(r"(.+?)[, ]+\d{4}", house_info_raw)
        if m_split:
            house_type = m_split.group(1).strip()
        house_info = house_type

        m_year = re.search(r"(\d{4})", house_info_raw)
        if m_year:
            try:
                y = int(m_year.group(1))
                if 1800 <= y <= 2100:
                    year_built = y
            except ValueError:
                year_built = None

    # если по строке "Дом ..." год не нашёлся, пробуем общий блок характеристик
    if year_built is None:
        m_year2 = re.search(r"(\d{4})\s*г\.?\s*Год постройки", full_text)
        if m_year2:
            try:
                y = int(m_year2.group(1))
                if 1800 <= y <= 2100:
                    year_built = y
            except ValueError:
                year_built = None

    # ---------- СОСТОЯНИЕ ----------
    condition = None
    m_cond = re.search(r"Состояние\s+([^\n]+)", info_text)
    if m_cond:
        condition = m_cond.group(1).strip()

    # ---------- ПЛАНИРОВКА ----------
    layout = None
    m_layout = re.search(r"Планировка\s+([^\n]+)", info_text)
    if m_layout:
        layout = m_layout.group(1).strip()

    # ---------- ИЗОБРАЖЕНИЯ ----------
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
        image_urls.append(src)

    image_urls = list(dict.fromkeys(image_urls))

    return {
        "description_full": description_full,
        "image_urls": image_urls,
        "year_built": year_built,
        "total_area_m2_detail": total_area,
        "kitchen_area_m2_detail": kitchen_area,
        "living_area_m2_detail": living_area,
        "floor_detail": floor,
        "floors_total_detail": floors_total,
        "house_info": house_info,
        "condition": condition,
        "layout": layout,
    }


def scrape_mirkvartir_moscow(
    max_pages: int = 10,
    min_items: int = 300,
    with_details: bool = True,
) -> pd.DataFrame:
    all_items: List[Dict] = []

    for page in range(1, max_pages + 1):
        print(f"Скачиваю страницу списка p={page}...")
        html = fetch_html(BASE_LIST_URL, params={"_rsc": "140g3", "p": page})
        cards = parse_list_page(html)
        print(f"  Найдено карточек на странице: {len(cards)}")

        all_items.extend(cards)
        seen = set()
        unique_items = []
        for it in all_items:
            u = it.get("url")
            if u and u not in seen:
                seen.add(u)
                unique_items.append(it)
        all_items = unique_items

        print(f"  Всего уникальных объявлений: {len(all_items)}")

        if len(all_items) >= min_items:
            print("Достигли нужного числа объявлений, останавливаемся по списку.")
            break

    if with_details:
        print("Загружаю подробности по каждому объявлению (описание и фото)...")
        for i, item in enumerate(all_items, start=1):
            url = item.get("url")
            if not url:
                continue
            print(f"[{i}/{len(all_items)}] Детальная страница: {url}")
            try:
                detail_html = fetch_html(url, sleep_sec=2.0)
                detail_data = parse_detail_page(detail_html)
                item["description_full"] = detail_data["description_full"]
                item["image_urls"] = json.dumps(detail_data["image_urls"], ensure_ascii=False)

                # Структурные признаки из деталки: при наличии переопределяем базовые
                if detail_data["total_area_m2_detail"] is not None:
                    item["area_total_m2"] = detail_data["total_area_m2_detail"]
                if detail_data["kitchen_area_m2_detail"] is not None:
                    item["kitchen_area_m2"] = detail_data["kitchen_area_m2_detail"]
                if detail_data["living_area_m2_detail"] is not None:
                    item["living_area_m2"] = detail_data["living_area_m2_detail"]
                if detail_data["floor_detail"] is not None:
                    item["floor"] = detail_data["floor_detail"]
                if detail_data["floors_total_detail"] is not None:
                    item["floors_total"] = detail_data["floors_total_detail"]

                item["year_built"] = detail_data["year_built"]
                item["house_info"] = detail_data["house_info"]
                item["condition"] = detail_data["condition"]
                item["layout"] = detail_data["layout"]
            except Exception as e:
                print(f"  Ошибка при разборе {url}: {e}")
                continue

    df = pd.DataFrame(all_items)
    return df


def main():
    max_pages = 1
    min_items = 400
    with_details = True

    df = scrape_mirkvartir_moscow(
        max_pages=max_pages,
        min_items=min_items,
        with_details=with_details,
    )

    print(f"Итого собрано объявлений: {len(df)}")
    output_file = "mirkvartir_moscow_flats_3.csv"
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Данные сохранены в файл: {output_file}")


if __name__ == "__main__":
    main()

