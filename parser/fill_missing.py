import os
import re
import time
import calendar
import datetime as dt
from typing import List, Tuple, Optional, Set

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# -------------------- CONFIG --------------------
BASE_URL = "https://www.pogodaiklimat.ru/weather.php"
STATION_ID = os.environ.get("STATION_ID", "27612")  # Москва по умолчанию

# твой текущий датасет (куда уже наскапало)
IN_CSV = "pogodaiklimat_archive.csv"

# куда сохранить обновлённую версию
OUT_CSV = "pogodaiklimat_archive_filled_fixed.csv"

YEAR_FROM = 2011
YEAR_TO = 2026  # верхняя граница скрапа (для поиска недостающих месяцев)

WAIT_TIMEOUT = 25
POLITE_SLEEP = 0.7

TIME_COL = "Время (UTC), дата #1"
DM_COL = "Время (UTC), дата #2"


# -------------------- SELENIUM --------------------
def make_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--lang=ru-RU")
    options.add_argument("--disable-popup-blocking")
    return webdriver.Chrome(options=options)


def safe_click(driver, by, selector, timeout=2) -> bool:
    try:
        el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, selector)))
        el.click()
        return True
    except Exception:
        return False


def close_garbage(driver):
    # cookie/consent
    safe_click(driver, By.ID, "cookie_close", timeout=1)
    safe_click(driver, By.XPATH, "//button[contains(.,'Принять') or contains(.,'Соглас')]", timeout=1)
    safe_click(driver, By.XPATH, "//a[contains(.,'Принять') or contains(.,'Соглас')]", timeout=1)

    # типовые крестики
    for xp in [
        "//button[contains(.,'×') or contains(.,'✕') or contains(.,'Закрыть')]",
        "//*[@aria-label='Close' or @aria-label='Закрыть']",
        "//*[contains(@class,'close') and (self::a or self::button)]",
    ]:
        safe_click(driver, By.XPATH, xp, timeout=1)

    # их якорь закрытия
    try:
        driver.execute_script("window.location.hash = 'close';")
    except Exception:
        pass


def open_month(driver, station_id: str, year: int, month: int) -> Optional[str]:
    # ✅ ВОТ ТУТ ФИКС: реальный последний день месяца
    last_day = calendar.monthrange(year, month)[1]

    url = (
        f"{BASE_URL}?id={station_id}"
        f"&bday=1&fday={last_day}&amonth={month}&ayear={year}&bot=2"
    )

    driver.get(url)
    time.sleep(0.8)
    close_garbage(driver)

    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.archive-table"))
        )
    except Exception:
        return None

    return driver.page_source


# -------------------- HTML PARSING --------------------
def norm_text(s: str) -> str:
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


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


def parse_archive_tables(html: str) -> pd.DataFrame:
    """
    ВАЖНО: не удаляем пустые ячейки внутри строки,
    иначе столбцы типа Tmin/Tmax/R/R24/S будут "съезжать".
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

        header_cells = [norm_text(td.get_text(" ", strip=True)) for td in trs[0].find_all(["td", "th"])]
        header_cells = [h for h in header_cells if h]

        data = []
        for tr in trs[1:]:
            tds = tr.find_all(["td", "th"])
            # ✅ НЕ ФИЛЬТРУЕМ пустые значения тут!
            row = [norm_text(td.get_text(" ", strip=True)) for td in tds]
            # строка может быть вся пустая (разделитель) — тогда пропускаем
            if all(x == "" for x in row):
                continue
            data.append(row)

        if not data:
            continue

        max_cols = max(len(r) for r in data)
        max_rows = max(max_rows, len(data))

        if len(header_cells) == max_cols:
            colnames = header_cells
        elif len(header_cells) == 1:
            base = header_cells[0]
            colnames = [base] if max_cols == 1 else [f"{base} #{i+1}" for i in range(max_cols)]
        else:
            base = " | ".join(header_cells) if header_cells else f"table_{ti}"
            colnames = [base] if max_cols == 1 else [f"{base} #{i+1}" for i in range(max_cols)]

        fixed = []
        for r in data:
            if len(r) < max_cols:
                r = r + [None] * (max_cols - len(r))
            elif len(r) > max_cols:
                r = r[:max_cols]
            fixed.append(r)

        df_part = pd.DataFrame(fixed, columns=colnames)
        df_part.columns = make_unique_columns(df_part.columns.tolist(), prefix=f"t{ti}")
        parts.append(df_part)

    if not parts:
        raise ValueError("no parsable tables produced any data")

    out = pd.concat(parts, axis=1)
    if len(out) < max_rows:
        out = out.reindex(range(max_rows))

    return out


# -------------------- DATASET HELPERS --------------------
def parse_day_month(s: str) -> Tuple[Optional[int], Optional[int]]:
    if s is None:
        return None, None
    s = str(s).strip()
    m = re.match(r"^(\d{1,2})\.(\d{1,2})$", s)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def add_dt_utc(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[TIME_COL] = df[TIME_COL].astype(str).str.strip()
    df[DM_COL] = df[DM_COL].astype(str).str.strip()

    hour = pd.to_numeric(df[TIME_COL], errors="coerce")
    dm = df[DM_COL].apply(parse_day_month)
    day = dm.apply(lambda x: x[0])
    mon = dm.apply(lambda x: x[1])

    year = pd.to_numeric(df["year"], errors="coerce")

    df["dt_utc"] = pd.to_datetime(dict(year=year, month=mon, day=day), errors="coerce") + pd.to_timedelta(hour, unit="h")
    df["dt_utc"] = pd.to_datetime(df["dt_utc"], errors="coerce")
    return df


def months_in_range(y1: int, m1: int, y2: int, m2: int) -> List[Tuple[int, int]]:
    cur = dt.date(y1, m1, 1)
    end = dt.date(y2, m2, 1)
    out = []
    while cur <= end:
        out.append((cur.year, cur.month))
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out


def find_missing_months(df: pd.DataFrame) -> List[Tuple[int, int]]:
    df = add_dt_utc(df)
    max_dt = df["dt_utc"].dropna().max()
    if pd.isna(max_dt):
        # если вообще dt_utc не построился — просто считаем всё отсутствующим
        return months_in_range(YEAR_FROM, 1, YEAR_TO, 12)

    end_y, end_m = int(max_dt.year), int(max_dt.month)

    present = set((p.year, p.month) for p in df["dt_utc"].dropna().dt.to_period("M").unique())
    expected = set(months_in_range(YEAR_FROM, 1, end_y, end_m))

    missing = sorted(expected - present)
    return missing


# -------------------- MAIN --------------------
def main():
    if not os.path.exists(IN_CSV):
        raise FileNotFoundError(f"Не найден {IN_CSV}. Положи его рядом со скриптом.")

    base = pd.read_csv(IN_CSV, encoding="utf-8-sig", dtype=str)

    missing = find_missing_months(base)
    print(f"missing months: {len(missing)}")
    if missing[:15]:
        print("first missing:", ", ".join([f"{y}-{m:02d}" for y, m in missing[:15]]))

    if not missing:
        print("Нечего добивать — месяцы на месте.")
        base.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return

    driver = make_driver()
    new_parts = []

    try:
        # прогрев
        driver.get(BASE_URL)
        time.sleep(1.0)
        close_garbage(driver)

        driver.get(f"{BASE_URL}?id={STATION_ID}#close")
        time.sleep(1.0)
        close_garbage(driver)

        for y, m in missing:
            html = open_month(driver, STATION_ID, y, m)
            if not html:
                print(f"FAIL {y}-{m:02d} (no html)")
                continue

            dfm = parse_archive_tables(html)
            dfm.insert(0, "station_id", STATION_ID)
            dfm.insert(1, "year", y)
            dfm.insert(2, "month", m)

            # чтобы 1.10 не превращалось в 1.1
            dfm[TIME_COL] = dfm[TIME_COL].astype(str)
            dfm[DM_COL] = dfm[DM_COL].astype(str)

            new_parts.append(dfm)
            print(f"OK {y}-{m:02d} rows={len(dfm)}")
            time.sleep(POLITE_SLEEP)

    finally:
        driver.quit()

    if not new_parts:
        print("Ничего не удалось доскрапить.")
        base.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        return

    new_df = pd.concat(new_parts, ignore_index=True)

    # merge + dedupe by (station_id, dt_utc)
    base_dt = add_dt_utc(base)
    new_dt = add_dt_utc(new_df)

    merged = pd.concat([base_dt, new_dt], ignore_index=True)
    merged = merged.sort_values("dt_utc").drop_duplicates(subset=["station_id", "dt_utc"], keep="last")

    # dt_utc оставляем — он полезен для анализа
    merged.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"saved: {OUT_CSV} rows={len(merged)}")

if __name__ == "__main__":
    main()