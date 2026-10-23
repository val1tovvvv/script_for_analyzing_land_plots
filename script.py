import asyncio
import concurrent.futures
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

# 1. Параметры базы Neon
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

# --- Вспомогательные функции нормализации ---


def sanitize_text(text):
  if not text:
    return ""
  # Убираем разрывы строк и множественные пробелы, ломающие CSV
  return re.sub(r"\s+", " ", str(text)).strip()


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


def extract_district(address):
  if not address:
    return "Томский район"
  m = re.search(r"([А-Яа-я]+(?:\s+[А-Яа-я]+)?)\s+р-н", address, re.IGNORECASE)
  if m:
    return clean_district(m.group(1))
  for d in ["Кировский", "Советский", "Ленинский", "Октябрьский"]:
    if d.lower() in address.lower():
      return f"{d} район"
  return "Томский район"


def extract_category(text):
  t = str(text).lower()
  if any(k in t for k in ["сельхоз", "снт", "днт", "садовод"]):
    return "земли сельскохозяйственного назначения"
  if any(k in t for k in ["населен", "ижс", "лпх"]):
    return "земли населённых пунктов"
  if any(k in t for k in ["пром", "производ"]):
    return "земли промышленности"
  return "земли населённых пунктов"


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
  return ", ".join(vri) if vri else "ИЖС"


def check_feature(text, keywords):
  t = str(text).lower()
  return "да" if any(k in t for k in keywords) else "нет"


# --- Поток 1: tomsk.ru09 (с жестким фильтром квартир) ---


def worker_ru09(max_pages=30):
  print("[Старт] Поток tomsk.ru09 запущен...")
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
          low_t = raw_text.lower()

          # Фильтр: если это квартира или нет упоминания соток — строго пропускаем
          if any(
              bad in low_t
              for bad in ["квартир", "комнатн", "кухня", "этаж", "панель"]
          ):
            continue

          m_area = re.search(r"(\d+([.,]\d+)?)\s*сот", raw_text, re.IGNORECASE)
          if not m_area:
            continue
          area = float(m_area.group(1).replace(",", "."))

          # Извлекаем цену
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
          clean_desc = sanitize_text(raw_text)[:400]

          rows.append((
              "tomsk.ru09",
              card_url,
              datetime.now().strftime("%Y-%m-%d"),
              now_str,
              "Томская область",
              extract_district(raw_text),
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
              clean_desc,
              "Собственник на ru09",
          ))
      time.sleep(0.1)
    except Exception as e:
      print(f"[ru09] Ошибка: {e}")
      break

  print(f"[Финиш] Поток tomsk.ru09 завершен: найдено {len(rows)} чистых участков.")
  return rows


# --- Поток 2: CIAN (надежный сбор через curl_cffi) ---


def worker_cian(max_pages=20):
  print("[Старт] Поток CIAN запущен...")
  base_url = "https://tomsk.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=suburban&region=5011&type=4"
  session = cureq.Session(impersonate="chrome120")
  now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  rows = []

  for p in range(1, max_pages + 1):
    try:
      resp = session.get(
          f"{base_url}&p={p}",
          headers={
              "Accept-Language": "ru-RU,ru;q=0.9",
              "Referer": "https://tomsk.cian.ru/",
          },
          timeout=15,
      )
      if resp.status_code != 200:
        print(f"[CIAN] Код ответа: {resp.status_code}")
        break

      offers = []
      # Поиск данных в _cianConfig
      match = re.search(
          r"window\._cianConfig\['frontend-serp'\]\s*=\s*(\[.*?\]);", resp.text
      )
      if match:
        try:
          data = json.loads(match.group(1))
          for item in data:
            if item.get("key") == "initialState":
              offers = (
                  item.get("value", {}).get("results", {}).get("offers", [])
              )
              break
        except Exception:
          pass

      # Резервный поиск initialState
      if not offers:
        match_alt = re.search(
            r'\{"key":"initialState"[^>]*?,"value":(\{.*?\})\};', resp.text
        )
        if match_alt:
          try:
            data = json.loads(match_alt.group(1))
            offers = data.get("results", {}).get("offers", [])
          except Exception:
            pass

      if offers:
        for off in offers:
          c_url = off.get("fullUrl")
          price = off.get("bargainTerms", {}).get("priceRur")
          area = off.get("land", {}).get("area")
          price_unit = (
              round(price / area, 2) if (price and area and area > 0) else None
          )

          geo = off.get("geo", {})
          addr = geo.get("userInput") or "Томская область"
          desc = sanitize_text(off.get("description", ""))[:500]
          cadastral = off.get("land", {}).get("cadastralNumber")
          date_pub = (
              off.get("creationDate", "")[:10]
              if off.get("creationDate")
              else datetime.now().strftime("%Y-%m-%d")
          )
          full_t = f"{addr} {desc}"

          rows.append((
              "cian",
              c_url,
              date_pub,
              now_str,
              "Томская область",
              extract_district(addr),
              extract_locality(addr),
              addr,
              price,
              price_unit,
              area,
              "сотка",
              extract_category(full_t),
              extract_vri(full_t),
              cadastral,
              check_feature(full_t, ["газ", "газификац"]),
              check_feature(full_t, ["электр", "свет", "квт", "220"]),
              check_feature(full_t, ["водопровод", "скважин", "вода"]),
              check_feature(full_t, ["дом", "домик", "баня"]),
              desc,
              "Собственник на Циан",
          ))
      else:
        # Парсинг по HTML-карточкам
        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select(
            'article[data-name="CardComponent"], div[data-name="OfferSnippet"]'
        )
        if not cards:
          break
        for card in cards:
          link = card.select_one('a[href*="/suburban/deal/"]')
          if not link:
            continue
          c_url = link["href"]
          c_text = sanitize_text(card.get_text(separator=" ", strip=True))

          p_el = card.select_one(
              '[data-mark="MainPrice"], span[color="text-primary-default"]'
          )
          price = (
              float(re.sub(r"[^\d]", "", p_el.text))
              if p_el and re.sub(r"[^\d]", "", p_el.text)
              else None
          )

          m_a = re.search(r"(\d+([.,]\d+)?)\s*сот", c_text, re.I)
          area = float(m_a.group(1).replace(",", ".")) if m_a else None
          price_unit = (
              round(price / area, 2) if (price and area and area > 0) else None
          )

          geo = card.select_one('[data-name="Geo"]')
          addr = geo.get_text(strip=True) if geo else "Томская область"

          rows.append((
              "cian",
              c_url,
              datetime.now().strftime("%Y-%m-%d"),
              now_str,
              "Томская область",
              extract_district(addr),
              extract_locality(addr),
              addr,
              price,
              price_unit,
              area,
              "сотка",
              extract_category(c_text),
              extract_vri(c_text),
              None,
              check_feature(c_text, ["газ", "газификац"]),
              check_feature(c_text, ["электр", "свет", "квт"]),
              check_feature(c_text, ["водопровод", "скважин", "вода"]),
              check_feature(c_text, ["дом", "домик", "баня"]),
              c_text[:500],
              "Циан",
          ))
      time.sleep(1.0)
    except Exception as e:
      print(f"[CIAN] Ошибка: {e}")
      break

  print(f"[Финиш] Поток CIAN завершен: найдено {len(rows)} объектов.")
  return rows


# --- Поток 3: Avito (Playwright с глубоким сбором) ---


async def _async_worker_avito(max_pages=8):
  base_url = "https://www.avito.ru/tomskaya_oblast/zemelnye_uchastki"
  now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  rows = []

  async with async_playwright() as p:
    browser = await p.chromium.launch(
        headless=False, args=["--disable-blink-features=AutomationControlled"]
    )
    ctx = await browser.new_context(
        locale="ru-RU", viewport={"width": 1366, "height": 768}
    )
    page = await ctx.new_page()

    for p_idx in range(1, max_pages + 1):
      try:
        url = f"{base_url}?p={p_idx}" if p_idx > 1 else base_url
        await page.goto(url, wait_until="domcontentloaded", timeout=22000)
        await page.mouse.wheel(0, 3200)
        await page.wait_for_timeout(1500)

        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        items = soup.select('div[data-marker="item"]')
        if not items:
          break

        for card in items:
          link_el = card.select_one('a[data-marker="item-title"]')
          if not link_el:
            continue
          href = link_el.get("href")
          c_url = f"https://www.avito.ru{href}" if href.startswith("/") else href
          title = sanitize_text(link_el.get_text(strip=True))

          p_meta = card.select_one('meta[itemprop="price"]')
          price = float(p_meta.get("content")) if p_meta else None
          if not price:
            p_box = card.select_one('[data-marker="item-price"]')
            if p_box:
              dig = re.sub(r"[^\d]", "", p_box.get_text())
              price = float(dig) if dig else None

          m_a = re.search(r"(\d+([.,]\d+)?)\s*сот", title, re.I)
          area = float(m_a.group(1).replace(",", ".")) if m_a else None
          price_unit = (
              round(price / area, 2) if (price and area and area > 0) else None
          )

          geo_el = card.select_one('[class*="geo-root"]')
          addr = (
              geo_el.get_text(strip=True) if geo_el else "Томская область"
          )
          full_ctx = sanitize_text(f"{title} {addr}")

          rows.append((
              "avito",
              c_url,
              datetime.now().strftime("%Y-%m-%d"),
              now_str,
              "Томская область",
              extract_district(addr),
              extract_locality(addr),
              addr,
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
              full_ctx[:500],
              "Продавец на Авито",
          ))
        await page.wait_for_timeout(1200)
      except Exception as e:
        print(f"[Avito] Предупреждение: {e}")
        break

    await browser.close()

  return rows


def worker_avito(max_pages=8):
  print("[Старт] Поток Avito запущен...")
  rows = asyncio.run(_async_worker_avito(max_pages))
  print(f"[Финиш] Поток Avito завершен: найдено {len(rows)} объектов.")
  return rows


# --- Оркестратор запуска ---


def main():
  start = time.time()
  print("=" * 60)
  print("ПАРАЛЛЕЛЬНЫЙ СБОР ЧИСТЫХ ДАННЫХ (tomsk.ru09 + CIAN + Avito)")
  print("=" * 60)

  all_results = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    f_ru09 = executor.submit(worker_ru09, max_pages=30)
    f_cian = executor.submit(worker_cian, max_pages=20)
    f_avito = executor.submit(worker_avito, max_pages=8)

    for future in concurrent.futures.as_completed([f_ru09, f_cian, f_avito]):
      try:
        data = future.result()
        all_results.extend(data)
      except Exception as exc:
        print(f"Ошибка в потоке: {exc}")

  print(
      f"\nСбор завершен за {round(time.time() - start, 1)} сек. Всего получено:"
      f" {len(all_results)} записей."
  )

  if all_results:
    print(f"Сохранение в Neon DB...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    query = """
        INSERT INTO land_plots_report (
            source, source_url, date_published, parsed_at, region, district,
            locality, address, price, price_per_unit, area, area_unit,
            category_land, vri, cadastral_number, has_gas, has_electricity,
            has_water, has_house, description, contact_name
        ) VALUES %s
        ON CONFLICT (source_url) DO NOTHING;
        """
    execute_values(cur, query, all_results)
    conn.commit()
    cur.close()
    conn.close()
    print("[V] Готово! Все новые участки добавлены без дубликатов.")
  else:
    print("Данные не получены.")


if __name__ == "__main__":
  main()