#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import gzip
import io
import os
import re
import sys
import time
import argparse
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ------------------------
# Конфиг и источники зон
# ------------------------

BASE_DIR = Path(__file__).resolve().parent
ALL_DOMAINS_FILE = BASE_DIR / "all_domains.csv"   # TSV с таб-разделителем
RESULT_FILE      = BASE_DIR / "result.csv"        # TSV с таб-разделителем

# Наборы источников .gz. Скрипт попробует пройтись по каждому словарю по очереди.
SOURCES = [
    # 1) ru-tld.ru (как в твоём check_data_file)
    {
        'net.ru': 'https://ru-tld.ru/files/NET-RU_Domains_ru-tld.ru.gz',
        'org.ru': 'https://ru-tld.ru/files/ORG-RU_Domains_ru-tld.ru.gz',
        'pp.ru':  'https://ru-tld.ru/files/PP-RU_Domains_ru-tld.ru.gz',
        '*.ru':  'https://ru-tld.ru/files/3d_domains_ru-tld.ru.gz',
        'ru':    'https://ru-tld.ru/files/RU_Domains_ru-tld.ru.gz',
        'rf':    'https://ru-tld.ru/files/RF_Domains_ru-tld.ru.gz',
        'su':    'https://ru-tld.ru/files/SU_Domains_ru-tld.ru.gz',
    },
    # 2) partner.r01.ru (как в твоём GetDomains.__main__)
    {
        'ru': 'https://partner.r01.ru/zones/ru_domains.gz',
        'rf': 'https://partner.r01.ru/zones/rf_domains.gz',
        'su': 'https://partner.r01.ru/zones/su_domains.gz',
    },
]

HEADER = ["domain", "registered", "date_created", "paid_till", "date_free", "delegated"]

UA = "Mozilla/5.0 (all-in-one-domains-script)"
TIMEOUT = 60


# ------------------------
# HTTP-сессия с ретраями
# ------------------------

def make_session():
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = make_session()


# ------------------------
# Загрузка и распаковка
# ------------------------

def fetch_gz(url: str) -> io.BytesIO:
    """Скачать .gz и вернуть BytesIO (в памяти)."""
    r = SESSION.get(url, timeout=TIMEOUT, stream=True)
    r.raise_for_status()
    buf = io.BytesIO(r.content)
    return buf


def append_gz_tsv_to_file(gz_bytes: io.BytesIO, out_path: Path):
    """Прочитать gz как TSV и дописать строки в файл."""
    with gzip.GzipFile(fileobj=gz_bytes) as gz, open(out_path, "a", encoding="utf-8", newline="") as res:
        writer = csv.writer(res, delimiter="\t")
        for raw_line in gz:
            line = raw_line.decode("utf-8", errors="ignore").strip()
            if not line:
                continue
            writer.writerow(line.split("\t"))


def build_all_domains(out_path: Path):
    """Собрать all_domains.csv из доступных источников (по очереди)."""
    # обнуляем файл и пишем заголовок
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as res:
        csv.writer(res, delimiter="\t").writerow(HEADER)

    total_rows = 0
    for source_idx, links in enumerate(SOURCES, 1):
        print(f"\nИсточник #{source_idx}: {len(links)} архивов")
        for zone, url in links.items():
            try:
                print(f"  → скачиваю {zone}: {url}")
                gz_buf = fetch_gz(url)
                before = out_path.stat().st_size if out_path.exists() else 0
                append_gz_tsv_to_file(gz_buf, out_path)
                after = out_path.stat().st_size
                added_bytes = after - before
                print(f"    OK (+{added_bytes:,} байт)")
                time.sleep(0.5)
            except Exception as e:
                print(f"    ПРОПУЩЕНО ({zone}): {e.__class__.__name__}: {e}")
                time.sleep(0.5)

    # Подсчитаем строки
    try:
        df = pd.read_csv(out_path, sep="\t", dtype=str, on_bad_lines="skip", low_memory=False)
        total_rows = len(df)
    except Exception:
        pass
    print(f"\nГотово: {out_path} (строк: {total_rows:,})")


# ------------------------
# Фильтрация и поиск
# ------------------------

def normalize_domain(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"^(?:https?://)?(?:www\.)?", "", s)
    s = s.split("/")[0].split(":")[0]
    return s


def filter_days_and_mask(in_path: Path, out_path: Path, days: int, mask: str):
    """Фильтрация по дням до date_free и маске. Запись result.csv (TSV)."""
    # читаем TSV, мягко парсим даты
    df = pd.read_csv(in_path, sep="\t", dtype=str, on_bad_lines="skip", low_memory=False)
    need_cols = {"domain", "date_free", "date_created"}
    if not need_cols.issubset(set(df.columns)):
        raise ValueError(f"Ожидались колонки {need_cols}, а получили: {list(df.columns)}")

    df["domain"] = df["domain"].astype(str).map(normalize_domain)
    df["date_free"] = pd.to_datetime(df["date_free"], dayfirst=True, errors="coerce")
    df["date_created"] = pd.to_datetime(df["date_created"], dayfirst=True, errors="coerce")

    df = df.dropna(subset=["date_free"])  # без даты освобождения — не фильтруем по дням

    today = pd.Timestamp.today().normalize()
    df["days_left"] = (df["date_free"] - today).dt.days
    df = df.loc[df["days_left"] < days].copy()

    # возраст домена
    df["domain_age"] = (today - df["date_created"]).dt.days

    # маска/регекс — если пустая, оставляем всё
    if mask.strip():
        df = df.loc[df["domain"].astype(str).str.contains(mask, flags=re.I, regex=True, na=False)].copy()

    # сортировка: сначала ближе к освобождению
    df = df.sort_values(["days_left", "domain"]).reset_index(drop=True)

    # сохраняем result.csv (TSV)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, sep="\t", index=False)

    print(f"\nСохранено: {out_path} (строк: {len(df):,})")
    if not df.empty:
        print("\nПримеры доменов:")
        print("\n".join(df["domain"].head(25).tolist()))


# ------------------------
# Запрос параметров
# ------------------------

def ask_yes_no(prompt: str) -> bool:
    while True:
        ans = input(prompt).strip().lower()
        if ans in ("y", "yes", "д", "да"):
            return True
        if ans in ("n", "no", "н", "нет"):
            return False
        print("Введите YES/NO")


def main_cli():
    parser = argparse.ArgumentParser(description="Скачать зоны и отфильтровать домены")
    parser.add_argument("--days", type=int, default=None, help="Фильтр: дней до освобождения (например 30)")
    parser.add_argument("--mask", type=str, default=None, help="Маска/RegEx по домену (например 'mebel|шкаф')")
    parser.add_argument("--force-update", action="store_true", help="Принудительно пересобрать all_domains.csv")
    args = parser.parse_args()

    need_update = args.force_update
    if not ALL_DOMAINS_FILE.exists() and not need_update:
        need_update = True

    if ALL_DOMAINS_FILE.exists() and not args.force_update:
        print(f"Найден файл: {ALL_DOMAINS_FILE}")
        if ask_yes_no("Обновить файл зон сейчас? (YES/NO): "):
            need_update = True

    if need_update:
        print("Собираю all_domains.csv из .gz-архивов...")
        build_all_domains(ALL_DOMAINS_FILE)
    else:
        print("Оставляю текущий all_domains.csv.")

    # параметры фильтрации
    days = args.days if args.days is not None else int(input("Enter days left number: ").strip())
    mask = args.mask if args.mask is not None else input("Enter domains search mask (word or RegEx): ").strip()

    filter_days_and_mask(ALL_DOMAINS_FILE, RESULT_FILE, days=days, mask=mask)


if __name__ == "__main__":
    try:
        main_cli()
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        sys.exit(1)
