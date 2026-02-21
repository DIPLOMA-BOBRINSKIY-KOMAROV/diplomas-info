import os
import re
import time
import logging
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ----------------------------
# config
# ----------------------------
BASE_URL = "https://www.pogodaiklimat.ru/weather.php"
DEFAULT_STATION_ID = "27612"  # Москва (можешь менять)
OUT_CSV = "pogodaiklimat_archive.csv"

YEAR_FROM = 2011
YEAR_TO = 2026

WAIT_TIMEOUT = 25


# ----------------------------
# logging
# ----------------------------
def setup_logger():
    logger = logging.getLogger("pogodaiklimat")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(ch)
    return logger


log = setup_logger()


# ----------------------------
# selenium helpers
# ----------------------------
def make_driver():
    options = webdriver.ChromeOptions()
    # headless выключен специально (как ты просил)
    options.add_argument("--start-maximized")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver


def safe_click(driver, by, selector, timeout=4):
    try:
        el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, selector)))
        el.click()
        return True
    except Exception:
        return False


def close_garbage(driver):
    """
    Пытаемся закрыть всё, что мешает: cookie-баннеры, модалки, кривые крестики.
    Тут нет “идеального” селектора, поэтому набор эвристик.
    """
    # cookie / consent
    safe_click(driver, By.XPATH, "//button[contains(., 'Соглас') or contains(., 'Accept') or contains(., 'OK')]", timeout=2)
    safe_click(driver, By.XPATH, "//a[contains(., 'Соглас') or contains(., 'Accept') or contains(., 'OK')]", timeout=2)

    # типовые крестики
    xpaths = [
        "//button[contains(., '×') or contains(., '✕') or contains(., 'Закрыть')]",
        "//div[contains(@class,'close') or contains(@class,'Close')]/a",
        "//div[contains(@class,'close') or contains(@class,'Close')]/button",
        "//*[@aria-label='Close' or @aria-label='Закрыть']",
    ]
    for xp in xpaths:
        safe_click(driver, By.XPATH, xp, timeout=1)

    # иногда помогает кликнуть по якорю #close (у них реально используется)
    try:
        driver.execute_script("window.location.hash = 'close';")
    except Exception:
        pass


def save_debug(driver, prefix):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"{prefix}_{ts}.html"
    png_path = f"{prefix}_{ts}.png"

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        log.info(f"[debug] saved: {html_path}")
    except Exception as e:
        log.info(f"[debug] failed save html: {e}")

    try:
        driver.save_screenshot(png_path)
        log.info(f"[debug] saved: {png_path}")
    except Exception as e:
        log.info(f"[debug] failed save png: {e}")


def open_month(driver, station_id, year, month):
    # bot=2 — режим “таблица за период” (у них так)
    # bday/fday — берём весь месяц. fday=31 ок, сайт сам нормализует.
    url = (
        f"{BASE_URL}?id={station_id}"
        f"&bday=1&fday=31&amonth={month}&ayear={year}&bot=2"
    )
    log.info(f"open month: {year}-{month:02d} | {url}")
    driver.get(url)
    time.sleep(0.8)
    close_garbage(driver)

    # Скролл: не обязателен для DOM, но иногда помогает догрузить блоки/скрипты.
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.35);")
    except Exception:
        pass

    # Ждём именно контейнер архива, а не th
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.archive-table"))
        )
    except Exception as e:
        log.error(f"timeout waiting archive-table: {year}-{month:02d} | {type(e).__name__}: {e}")
        log.info(f"[debug] url = {driver.current_url}")
        save_debug(driver, f"fail_{year}_{month:02d}")
        return None

    return driver.page_source


# ----------------------------
# parsing logic (важная часть)
# ----------------------------
def norm_text(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_archive_tables(html: str) -> pd.DataFrame:
    """
    На странице архив — это несколько <table> внутри div.archive-table.
    Каждая таблица даёт 1+ колонок. Склеиваем по номеру строки.
    """
    soup = BeautifulSoup(html, "html.parser")
    wrap = soup.select_one("div.archive-table")
    if not wrap:
        raise ValueError("div.archive-table not found")

    tables = wrap.find_all("table")
    if not tables:
        raise ValueError("no tables inside div.archive-table")

    parts = []
    max_rows = 0

    for ti, table in enumerate(tables):
        trs = table.find_all("tr")
        if len(trs) < 2:
            continue

        # header row (обычно первая строка)
        header_cells = [norm_text(td.get_text(" ", strip=True)) for td in trs[0].find_all(["td", "th"])]
        header_cells = [h for h in header_cells if h]

        # data rows
        data = []
        for tr in trs[1:]:
            tds = tr.find_all(["td", "th"])
            row = [norm_text(td.get_text(" ", strip=True)) for td in tds]
            row = [x for x in row if x != ""]
            # иногда бывают пустые строки/разделители — пропускаем
            if not row:
                continue
            data.append(row)

        if not data:
            continue

        max_cols = max(len(r) for r in data)
        max_rows = max(max_rows, len(data))

        # делаем имена колонок
        if len(header_cells) == max_cols:
            colnames = header_cells
        elif len(header_cells) == 1:
            base = header_cells[0]
            colnames = [base] if max_cols == 1 else [f"{base} #{i+1}" for i in range(max_cols)]
        else:
            # несколько заголовков, но не совпадает с числом колонок
            base = " | ".join(header_cells) if header_cells else f"table_{ti}"
            colnames = [base] if max_cols == 1 else [f"{base} #{i+1}" for i in range(max_cols)]

        # выравниваем строки по числу колонок
        fixed = []
        for r in data:
            if len(r) < max_cols:
                r = r + [None] * (max_cols - len(r))
            elif len(r) > max_cols:
                r = r[:max_cols]
            fixed.append(r)

        df_part = pd.DataFrame(fixed, columns=colnames)

        # защита от дублей названий
        df_part.columns = make_unique_columns(df_part.columns.tolist(), prefix=f"t{ti}")

        parts.append(df_part)

    if not parts:
        raise ValueError("no parsable tables produced any data")

    # склейка по индексу строки (это и есть “строка наблюдения”)
    out = pd.concat(parts, axis=1)

    # иногда какие-то таблицы короче — добиваем до max_rows
    if len(out) < max_rows:
        out = out.reindex(range(max_rows))

    return out


def make_unique_columns(cols, prefix="col"):
    seen = {}
    out = []
    for c in cols:
        key = c if c else prefix
        if key not in seen:
            seen[key] = 1
            out.append(key)
        else:
            seen[key] += 1
            out.append(f"{key} ({seen[key]})")
    return out


# ----------------------------
# main loop
# ----------------------------
def append_to_csv(df: pd.DataFrame, path: str):
    file_exists = os.path.exists(path)
    df.to_csv(path, index=False, mode="a", header=not file_exists, encoding="utf-8-sig")


def main():
    station_id = os.environ.get("STATION_ID", DEFAULT_STATION_ID)

    driver = make_driver()
    try:
        log.info(f"open: {BASE_URL}")
        driver.get(BASE_URL)
        time.sleep(1.0)
        close_garbage(driver)

        # прогрев: открываем страницу станции (так проще, чем искать город каждый раз)
        station_url = f"{BASE_URL}?id={station_id}#close"
        log.info(f"open station: {station_url}")
        driver.get(station_url)
        time.sleep(1.0)
        close_garbage(driver)

        # цикл по месяцам
        for y in range(YEAR_FROM, YEAR_TO + 1):
            for m in range(1, 13):
                try:
                    page = open_month(driver, station_id, y, m)
                    if not page:
                        log.error(f"MONTH FAIL: {y}-{m:02d} (page is None)")
                        continue

                    df = parse_archive_tables(page)

                    # добавляем мета-поля
                    df.insert(0, "station_id", station_id)
                    df.insert(1, "year", y)
                    df.insert(2, "month", m)

                    append_to_csv(df, OUT_CSV)
                    log.info(f"saved: {y}-{m:02d} | rows={len(df)} | cols={len(df.columns)} -> {OUT_CSV}")

                    # небольшая пауза, чтобы сайт не нервничал
                    time.sleep(0.7)

                except Exception as e:
                    log.error(f"MONTH FAIL: {y}-{m:02d} | {type(e).__name__}: {e}")
                    log.info(f"[debug] url = {driver.current_url}")
                    save_debug(driver, f"fail_{y}_{m:02d}")
                    continue

        log.info("done")

    finally:
        # не закрываю мгновенно, чтобы ты успел глазами увидеть финал
        time.sleep(2.0)
        driver.quit()


if __name__ == "__main__":
    main()