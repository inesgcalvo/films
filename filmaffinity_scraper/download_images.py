"""
download_images.py

Descarga imágenes (posters y banderas)
a partir del dataset generado.
"""

import asyncio
import aiohttp
import pandas as pd
import os

DATASET = "dataset/movies.parquet"

POSTER_DIR = "posters"
FLAG_DIR = "flags"

os.makedirs(POSTER_DIR, exist_ok=True)
os.makedirs(FLAG_DIR, exist_ok=True)

MAX_CONCURRENT = 50

semaphore = asyncio.Semaphore(MAX_CONCURRENT)

async def download(session, url, path):

    if not url:
        return

    if os.path.exists(path):
        return

    async with semaphore:

        try:

            async with session.get(url) as r:

                if r.status == 200:

                    data = await r.read()

                    with open(path, "wb") as f:
                        f.write(data)

        except:
            pass


async def main():

    df = pd.read_parquet(DATASET)

    connector = aiohttp.TCPConnector(limit=100)

    async with aiohttp.ClientSession(connector=connector) as session:

        tasks = []

        for _, row in df.iterrows():

            poster_path = f"{POSTER_DIR}/{row['id']}.jpg"
            flag_path = f"{FLAG_DIR}/{row['id']}.png"

            tasks.append(download(session, row["poster"], poster_path))
            tasks.append(download(session, row["flag"], flag_path))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())