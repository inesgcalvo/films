"""
Parallel FilmAffinity scraper using Selenium.

Features
--------
- multiprocessing
- uses pre-generated film IDs
- random delays to avoid blocking
- retry logic
- chunked parquet saving
- extracts synopsis
"""

import os
import time
import random
import re
import logging
import pandas as pd

from multiprocessing import Pool, cpu_count
from tqdm import tqdm

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup


# =============================
# CONFIG
# =============================

IDS_FILE = "dataset/ids.txt"

BASE_URL = "https://www.filmaffinity.com/es/film{}.html"

OUTPUT_DIR = "dataset/chunks"

CHUNK_SIZE = 100

WORKERS = 2

RETRIES = 3


os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO)


# =============================
# DRIVER
# =============================

def create_driver():

    options = Options()

    options.add_argument("--width=1400")
    options.add_argument("--height=1000")

    driver = webdriver.Firefox(options=options)

    return driver


# =============================
# PARSER
# =============================

def parse_movie(html, film_id):

    soup = BeautifulSoup(html, "lxml")

    title_el = soup.select_one("#main-title span")

    if not title_el:
        return None

    title_es = title_el.text.strip()

    synopsis_el = soup.select_one("#synopsis")

    synopsis = synopsis_el.text.strip() if synopsis_el else None

    rating_el = soup.select_one("#movie-rat-avg")

    rating = float(rating_el.text) if rating_el else None

    votes_el = soup.select_one("#movie-count-rat")

    votes = None
    if votes_el:
        votes = int(votes_el.text.replace(".", "").replace(",", ""))

    country_el = soup.select_one("#country-img img")

    country = country_el["alt"] if country_el else None
    flag = country_el["src"] if country_el else None

    poster_el = soup.select_one("#movie-main-image-container img")

    poster = poster_el["src"] if poster_el else None

    duration = None

    duration_el = soup.find("dd", string=re.compile("min"))

    if duration_el:
        m = re.search(r"\d+", duration_el.text)
        if m:
            duration = float(m.group())

    return {
        "film_id": film_id,
        "title_es": title_es,
        "country": country,
        "flag": flag,
        "rating": rating,
        "votes": votes,
        "duration": duration,
        "poster": poster,
        "synopsis": synopsis
    }


# =============================
# SCRAPE SINGLE FILM
# =============================

def scrape_film(film_id):

    driver = create_driver()

    url = BASE_URL.format(film_id)

    for attempt in range(RETRIES):

        try:

            driver.get(url)

            time.sleep(random.uniform(2, 4))

            html = driver.page_source

            movie = parse_movie(html, film_id)

            driver.quit()

            return movie

        except Exception:

            if attempt == RETRIES - 1:
                driver.quit()
                return None

            time.sleep(2)


# =============================
# SAVE
# =============================

def save_chunk(data, chunk_id):

    df = pd.DataFrame(data)

    path = f"{OUTPUT_DIR}/movies_{chunk_id:05d}.parquet"

    df.to_parquet(path)

    logging.info(f"Saved chunk {chunk_id}")


# =============================
# MAIN
# =============================

def main():

    logging.info("Loading IDs")

    with open(IDS_FILE) as f:
        ids = [x.strip() for x in f]

    logging.info(f"{len(ids)} IDs loaded")

    pool = Pool(WORKERS)

    chunk = []

    chunk_id = 0

    for result in tqdm(pool.imap(scrape_film, ids), total=len(ids)):

        if result:
            chunk.append(result)

        if len(chunk) >= CHUNK_SIZE:

            save_chunk(chunk, chunk_id)

            chunk = []

            chunk_id += 1

    if chunk:
        save_chunk(chunk, chunk_id)

    pool.close()
    pool.join()


# =============================
# ENTRY
# =============================

if __name__ == "__main__":

    main()