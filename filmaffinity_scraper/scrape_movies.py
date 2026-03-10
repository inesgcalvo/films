"""
scrape_movies.py

Scraper asíncrono de páginas de FilmAffinity.

Características:
- scraping concurrente
- retry automático
- guardado incremental en parquet
- logging
"""

import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
import os
import re
import logging

# ==========================
# CONFIG
# ==========================

MAX_CONCURRENT = 100
CHUNK_SIZE = 1000
RETRIES = 3

BASE_URL = "https://www.filmaffinity.com/es/film{}.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}

IDS_FILE = "dataset/ids.txt"
OUTPUT_FILE = "dataset/movies.parquet"

os.makedirs("dataset", exist_ok=True)

# ==========================
# LOGGING
# ==========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ==========================
# HTML PARSER
# ==========================

def parse_movie(html, film_id):
    """
    Extrae la información de una página HTML de película.
    """

    soup = BeautifulSoup(html, "lxml")

    title_el = soup.select_one("#main-title span")

    # si no hay título la película probablemente no existe
    if not title_el:
        return None

    title_es = title_el.text.strip()

    def get_field(label):
        """
        Extrae campos tipo:
        <dt>Título original</dt>
        <dd>...</dd>
        """
        el = soup.find("dt", string=label)
        if not el:
            return None
        return el.find_next("dd").text.strip()

    title_original = get_field("Título original")

    # país
    country_el = soup.select_one("#country-img img")
    country = country_el["alt"] if country_el else None
    flag = country_el["src"] if country_el else None

    # listas de personas
    director = [x.text for x in soup.select("dd.directors a")]
    writer = [x.text for x in soup.select("dd.screenwriters a")]
    cast = [x.text for x in soup.select("#cast a")]

    # duración
    duration = get_field("Duración")
    if duration:
        m = re.search(r"\d+", duration)
        duration = float(m.group()) if m else None

    # rating
    rating_el = soup.select_one("#movie-rat-avg")
    rating = float(rating_el.text) if rating_el else None

    votes_el = soup.select_one("#movie-count-rat")
    votes = int(votes_el.text.replace(".", "")) if votes_el else None

    # poster
    poster_el = soup.select_one("#movie-main-image-container img")
    poster = poster_el["src"] if poster_el else None

    return {
        "id": film_id,
        "title_es": title_es,
        "title_original": title_original,
        "country": country,
        "flag": flag,
        "director": director,
        "writer": writer,
        "cast": cast,
        "duration": duration,
        "rating": rating,
        "votes": votes,
        "poster": poster
    }

# ==========================
# HTTP FETCH
# ==========================

semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def fetch_movie(session, film_id):
    """
    Descarga una página con retry automático.
    """

    url = BASE_URL.format(film_id)

    async with semaphore:

        for attempt in range(RETRIES):

            try:

                async with session.get(url, headers=HEADERS, timeout=20) as r:

                    if r.status != 200:
                        return None

                    html = await r.text()

                    return parse_movie(html, film_id)

            except Exception:

                if attempt == RETRIES - 1:
                    return None

                await asyncio.sleep(1)

# ==========================
# SAVE CHUNKS
# ==========================

def save_chunk(data):

    """
    Guarda un chunk de datos en parquet.
    """

    df = pd.DataFrame(data)

    if os.path.exists(OUTPUT_FILE):
        df.to_parquet(OUTPUT_FILE, engine="pyarrow", compression="snappy", append=True)
    else:
        df.to_parquet(OUTPUT_FILE, engine="pyarrow", compression="snappy")

# ==========================
# MAIN
# ==========================

async def main():

    with open(IDS_FILE) as f:
        ids = [int(x.strip()) for x in f]

    connector = aiohttp.TCPConnector(limit=200)

    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = [fetch_movie(session, i) for i in ids]

        chunk = []

        for future in asyncio.as_completed(tasks):

            result = await future

            if result:
                chunk.append(result)

            if len(chunk) >= CHUNK_SIZE:

                save_chunk(chunk)

                logging.info(f"Saved {len(chunk)} movies")

                chunk = []

        if chunk:
            save_chunk(chunk)

    logging.info("Scraping finished")


if __name__ == "__main__":
    asyncio.run(main())