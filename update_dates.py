import concurrent.futures
from datetime import datetime
import re
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_batch
import requests

DB_CONFIG = {
    "host": "ep-lucky-pine-b4moh6wp-pooler.c-6.us-east-2.aws.neon.tech",
    "database": "neondb",
    "user": "neondb_owner",
    "password": "npg_T7EktyDSN3Aa",
    "port": 5432,
    "sslmode": "require",
    "channel_binding": "require",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
        " like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Connection": "keep-alive",
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

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(
    pool_connections=20, pool_maxsize=20, max_retries=2
)
session.mount("http://", adapter)
session.headers.update(HEADERS)


def extract_published_date(html):
  if not html:
    return None

  # Точный поиск блока "Опубликовано: 14 августа 2026"
  m = re.search(
      r"Опубликовано\s*:\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})", html, re.IGNORECASE
  )
  if m:
    day = m.group(1).zfill(2)
    month_str = m.group(2).lower()
    year = m.group(3)
    if month_str in MONTHS_MAP:
      return f"{year}-{MONTHS_MAP[month_str]}-{day}"

  # Запасной вариант: дата в формате дд.мм.гггг сразу после слова Опубликовано
  m_num = re.search(
      r"Опубликовано\s*:\s*(\d{2})\.(\d{2})\.(\d{4})", html, re.IGNORECASE
  )
  if m_num:
    return f"{m_num.group(3)}-{m_num.group(2)}-{m_num.group(1)}"

  return None


def fetch_card_date(record):
  rec_id, url = record
  try:
    resp = session.get(url, timeout=7)
    if resp.status_code == 200:
      resp.encoding = resp.apparent_encoding
      parsed_date = extract_published_date(resp.text)
      if parsed_date:
        return (parsed_date, rec_id)
  except Exception:
    pass
  return None


def run_updater():
  conn = psycopg2.connect(**DB_CONFIG)
  cur = conn.cursor()

  cur.execute(
      "SELECT id, source_url FROM land_plots_report WHERE date_published IS"
      " NULL;"
  )
  records = cur.fetchall()
  total = len(records)
  print(f"Найдено записей без даты: {total}")

  if total == 0:
    print("Все записи уже содержат дату!")
    cur.close()
    conn.close()
    return

  print("Сканирование дат в 15 потоков...")
  updates = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(fetch_card_date, rec) for rec in records]
    for i, f in enumerate(concurrent.futures.as_completed(futures), 1):
      res = f.result()
      if res:
        updates.append(res)

      if len(updates) >= 50 or i == total:
        if updates:
          execute_batch(
              cur,
              "UPDATE land_plots_report SET date_published = %s WHERE id = %s;",
              updates,
          )
          conn.commit()
          print(f"  --> Зафиксировано в Neon: {len(updates)} дат (всего проверено {i}/{total})")
          updates = []

  cur.close()
  conn.close()
  print("\nГотово! Все даты зафиксированы в таблице.")


if __name__ == "__main__":
  run_updater()