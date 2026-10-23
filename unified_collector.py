import asyncio
import json
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests as cureq
from playwright.async_api import async_playwright
import psycopg2
from psycopg2.extras import execute_values
import requests

# 1. Параметры подключения к Neon DB
DB_CONFIG = {
    "host": "ep-lucky-pine-b4moh6wp-pooler.c-6.us-east-2.aws.neon.tech",
    "database": "neondb",
    "user": "neondb_owner",
    "password": "npg_T7EktyDSN3Aa",
    "port": 5432,
    "sslmode": "require",
    "channel_binding": "require",
}

MONTHS_MAP = {
    "января": "01",
    "февраля": "02",
    "марта": "03",
    "апреля": "04",
    "мая": "05",
    "июня": "06",
    "июля": "07",
    "августа": "08",
    "сентября": "09",
    "октября": "10",
    "ноября": "11",
    "декабря": "12",
}

# --- Вспомогательные функции нормализации данных ---


def clean_district(val):
  return re.sub(r"(ском|ом|ий)$", "ский район", val.strip()) if val else None


def extract_locality(address):
  if not address:
    return "г. Томск"
  m = re.search(
      r"(село|пос\.|деревня|д\.|п\.|с\.|ст\.|ст\b)\s*([^,]+)",
      address,
      re.IGNORECASE,
  )
  return f"{m.group(1)} {m.group(2).strip()}" if m else "г. Томск"


def extract_category(text):
  t = str(text).lower()
  if any(k in t for k in ["сельхоз", "снт", "днт", "садовод"]):
    return "земли сельскохозяйственного назначения"
  if any(k in t for k in ["населен", "ижс", "лпх"]):
    return "земли населённых пунктов"
  if any(k in t for k in ["пром", "производ"]):
    return "земли промышленности"
  return "не указано"


def extract_vri(text):
  t = str(text).lower()
  vri = []
  if "ижс" in t:
    vri.append("ИЖС")
  if "лпх" in t:
    vri.append("ЛПХ")
  if any(k in t for k in ["садовод", "снт", "дач"]):
    vri.append("садоводство")
  if any(k in t for k in ["пром", "коммерч"]):
    vri.append("коммерческое использование")
  return ", ".join(vri) if vri else "не указано"


def check_feature(text, keywords):
  t = str(text).lower()
  return "да" if any(k in t for k in keywords) else "нет"


def save_batch_to_neon(rows):
  if not rows:
    return 0
  conn = psycopg2.connect(**DB_CONFIG)
  cur = conn.cursor()
  insert_query = """
    INSERT INTO land_plots_report (
        source, source_url, date_published, parsed_at, region, district,
        locality, address, price, price_per_unit, area, area_unit,
        category_land, vri, cadastral_number, has_gas, has_electricity,
        has_water, has_house, description, contact_name
    ) VALUES %s
    ON CONFLICT (source_url) DO NOTHING;
    """
  execute_values(cur, insert_query, rows)
  conn.commit()
  cur.close()
  conn.close()
  return len(rows)


# --- Модуль 1: tomsk.ru09.ru (requests по HTTP) ---


def scrape_ru09(max_pages=3):
  print(f"\n[1/3] Запуск сбора с tomsk.ru09.ru ({max_pages} страниц)...")
  base_url = "http://www.tomsk.ru09.ru"
  catalog_url = "http://www.tomsk.ru09.ru/realty/?type=3"
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      )
  }
  session = requests.Session()
  now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  rows = []

  for p in range(1, max_pages + 1):
    try:
      r = session.get(f"{catalog_url}&page={p}", headers=headers, timeout=8)
      if r.status_code != 200:
        break
      r.encoding = r.apparent_encoding
      soup = BeautifulSoup(r.text, "html.parser")

      for a in soup.find_all("a", href=True):
        if "subaction=detail" in a["href"] and "id=" in a["href"]:
          card_url = urljoin(base_url, a["href"])
          parent = a.find_parent("tr") or a.find_parent("div")
          raw_text = (
              parent.get_text(separator=" ", strip=True) if parent else ""
          )

          area = None
          m_area = re.search(r"(\d+([.,]\d+)?)\s*сот", raw_text)
          if m_area:
            area = float(m_area.group(1).replace(",", "."))

          price = None
          m_price = re.search(
              r"(\d[\d\s]{2,})\s*(тыс\.?\s*руб|т\.р|руб)",
              raw_text,
              re.IGNORECASE,
          )
          if m_price:
            cleaned = float(re.sub(r"[^\d]", "", m_price.group(1)))
            price = (
                cleaned * 1000
                if any(x in m_price.group(2).lower() for x in ["тыс", "т.р"])
                else cleaned
            )

          price_unit = (
              round(price / area, 2) if (price and area and area > 0) else None
          )
          addr = a.get_text(strip=True) or "Томская область"

          # Получаем точную дату публикации внутри объявления
          date_pub = None
          try:
            resp_card = session.get(card_url, headers=headers, timeout=6)
            if resp_card.status_code == 200:
              resp_card.encoding = resp_card.apparent_encoding
              m_date = re.search(
                  r"Опубликовано\s*:\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})",
                  resp_card.text,
                  re.I,
              )
              if m_date and m_date.group(2).lower() in MONTHS_MAP:
                date_pub = f"{m_date.group(3)}-{MONTHS_MAP[m_date.group(2).lower()]}-{m_date.group(1).zfill(2)}"
          except Exception:
            pass

          rows.append((
              "tomsk.ru09",
              card_url,
              date_pub or datetime.now().strftime("%Y-%m-%d"),
              now_str,
              "Томская область",
              "Томский район",
              extract_locality(addr),
              addr,
              price,
              price_unit,
              area,
              "сотка",
              extract_category(raw_text),
              extract_vri(raw_text),
              None,
              check_feature(raw_text, ["газ", "газификац"]),
              check_feature(raw_text, ["электр", "свет", "квт"]),
              check_feature(raw_text, ["водопровод", "скважин", "вода"]),
              check_feature(raw_text, ["дом", "домик", "баня"]),
              raw_text[:600],
              "ru09 продавец",
          ))
      time.sleep(0.3)
    except Exception as e:
      print(f"  [ru09] Ошибка страницы {p}: {e}")
      break

  print(f"  [ru09] Собрано: {len(rows)} строк.")
  return rows


# --- Модуль 2: cian.ru (curl_cffi через TLS Chrome) ---


def scrape_cian(max_pages=3):
  print(f"\n[2/3] Запуск сбора с CIAN ({max_pages} страниц)...")
  base_url = "https://tomsk.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=suburban&region=5011&type=4"
  session = cureq.Session(impersonate="chrome120")
  now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  rows = []

  for p in range(1, max_pages + 1):
    try:
      resp = session.get(f"{base_url}&p={p}", timeout=15)
      if resp.status_code != 200:
        break

      soup = BeautifulSoup(resp.text, "html.parser")
      cards = soup.select('article[data-name="CardComponent"]')
      if not cards:
        break

      for card in cards:
        link = card.select_one('a[href*="/suburban/deal/"]')
        if not link:
          continue

        full_url = link["href"]
        text = card.get_text(separator=" ", strip=True)

        # Цена
        price_el = card.select_one('[data-mark="MainPrice"]')
        price = float(re.sub(r"[^\d]", "", price_el.text)) if price_el else None

        # Площадь
        m_a = re.search(r"(\d+([.,]\d+)?)\s*сот", text)
        area = float(m_a.group(1).replace(",", ".")) if m_a else None
        price_unit = (
            round(price / area, 2) if (price and area and area > 0) else None
        )

        geo = card.select_one('[data-name="Geo"]')
        address = geo.get_text(strip=True) if geo else "Томская область"

        rows.append((
            "cian",
            full_url,
            datetime.now().strftime("%Y-%m-%d"),
            now_str,
            "Томская область",
            "Томский район",
            extract_locality(address),
            address,
            price,
            price_unit,
            area,
            "сотка",
            extract_category(text),
            extract_vri(text),
            None,
            check_feature(text, ["газ", "газификац"]),
            check_feature(text, ["электр", "свет", "квт"]),
            check_feature(text, ["водопровод", "скважин", "вода"]),
            check_feature(text, ["дом", "домик", "баня"]),
            text[:600],
            "Циан",
        ))
      time.sleep(1.5)
    except Exception as e:
      print(f"  [CIAN] Ошибка страницы {p}: {e}")
      break

  print(f"  [CIAN] Собрано: {len(rows)} строк.")
  return rows


# --- Модуль 3: avito.ru (Playwright) ---


async def scrape_avito(max_pages=2):
  print(f"\n[3/3] Запуск сбора с Avito ({max_pages} страниц)...")
  rows = []
  now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  base_url = "https://www.avito.ru/tomskaya_oblast/zemelnye_uchastki"

  async with async_playwright() as p:
    browser = await p.chromium.launch(
        headless=False, args=["--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(
        locale="ru-RU", viewport={"width": 1280, "height": 720}
    )
    page = await context.new_page()

    for p_idx in range(1, max_pages + 1):
      try:
        url = f"{base_url}?p={p_idx}" if p_idx > 1 else base_url
        await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(2000)

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select('div[data-marker="item"]')
        if not items:
          break

        for card in items:
          link = card.select_one('a[data-marker="item-title"]')
          if not link:
            continue
          href = link.get("href")
          card_url = (
              f"https://www.avito.ru{href}" if href.startswith("/") else href
          )
          title = link.get_text(strip=True)

          # Цена
          p_meta = card.select_one('meta[itemprop="price"]')
          price = float(p_meta.get("content")) if p_meta else None

          # Площадь
          m_a = re.search(r"(\d+([.,]\d+)?)\s*сот", title, re.I)
          area = float(m_a.group(1).replace(",", ".")) if m_a else None
          price_unit = (
              round(price / area, 2) if (price and area and area > 0) else None
          )

          geo_el = card.select_one('[class*="geo-root"]')
          address = (
              geo_el.get_text(strip=True) if geo_el else "Томская область"
          )
          full_ctx = f"{title} {address}"

          rows.append((
              "avito",
              card_url,
              datetime.now().strftime("%Y-%m-%d"),
              now_str,
              "Томская область",
              "Томский район",
              extract_locality(address),
              address,
              price,
              price_unit,
              area,
              "сотка",
              extract_category(full_ctx),
              extract_vri(full_ctx),
              None,
              check_feature(full_ctx, ["газ", "газификац"]),
              check_feature(full_ctx, ["электр", "свет", "квт"]),
              check_feature(full_ctx, ["водопровод", "скважин", "вода"]),
              check_feature(full_ctx, ["дом", "домик", "баня"]),
              full_ctx[:600],
              "Авито Продавец",
          ))
        await page.wait_for_timeout(2000)
      except Exception as e:
        print(f"  [Avito] Ошибка страницы {p_idx}: {e}")
        break

    await browser.close()

  print(f"  [Avito] Собрано: {len(rows)} строк.")
  return rows


# --- Оркестратор запуска ---


def main():
  total_added = 0

  # 1. Запуск сбора с ru09
  ru09_rows = scrape_ru09(max_pages=3)
  total_added += save_batch_to_neon(ru09_rows)

  # 2. Запуск сбора с CIAN
  cian_rows = scrape_cian(max_pages=3)
  total_added += save_batch_to_neon(cian_rows)

  # 3. Запуск сбора с Avito
  avito_rows = asyncio.run(scrape_avito(max_pages=2))
  total_added += save_batch_to_neon(avito_rows)

  print(f"\n[ИТОГ] Сбор завершен! Всего передано в базу: {total_added} записей.")


if __name__ == "__main__":
  main()