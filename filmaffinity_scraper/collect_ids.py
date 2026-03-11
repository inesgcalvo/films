"""
collect_ids.py

Generate a list of potential ID's starting from 000001 to 999999

This step allows to distribute the work, restart the scraping and avoid to calculate ID's again.
"""

import os

MAX_ID = 999999
OUTPUT_FILE = "dataset/ids.txt"

os.makedirs("dataset", exist_ok=True)

with open(OUTPUT_FILE, "w") as f:
    for film_id in range(1, MAX_ID + 1):
        f.write(f"{film_id:06d}\n")

print(f"{MAX_ID} IDs guardados en {OUTPUT_FILE}")