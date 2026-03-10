"""
collect_ids.py

Genera una lista de IDs potenciales de FilmAffinity
y los guarda en dataset/ids.txt.

Separar esta fase permite:
- reanudar scraping
- distribuir trabajo
- evitar recalcular IDs
"""

import os

MAX_ID = 900000
OUTPUT_FILE = "dataset/ids.txt"

os.makedirs("dataset", exist_ok=True)

with open(OUTPUT_FILE, "w") as f:
    for film_id in range(1, MAX_ID + 1):
        f.write(f"{film_id}\n")

print(f"{MAX_ID} IDs guardados en {OUTPUT_FILE}")