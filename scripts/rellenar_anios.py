#!/usr/bin/env python3
"""Rellena anio, pais y director desde OMDb (usando imdb_id ya obtenido)."""
import os, csv, json, time, urllib.parse, urllib.request
from pathlib import Path

API_KEY = os.environ.get("OMDB_API_KEY", "")
CSV_PATH = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")
CACHE = Path("/Users/inesg.calvo/reporte_videos/omdb_cache.json")
SLEEP = 0.2

def omdb_by_id(imdb_id: str) -> dict:
    url = "https://www.omdbapi.com/?" + urllib.parse.urlencode({"apikey": API_KEY, "i": imdb_id})
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"Response": "False", "Error": str(e)}

# Mapa país (nombre OMDb -> código 2-3 letras)
PAIS_MAP = {
    "United States": "US", "United Kingdom": "UK", "France": "FR", "Germany": "DE",
    "Italy": "IT", "Spain": "ES", "Japan": "JP", "Canada": "CA", "Australia": "AU",
    "Sweden": "SE", "Denmark": "DK", "Netherlands": "NL", "Belgium": "BE",
    "Russia": "RU", "China": "CN", "South Korea": "KR", "Brazil": "BR",
    "Mexico": "MX", "Argentina": "AR", "India": "IN", "Poland": "PL",
    "Ireland": "IE", "Norway": "NO", "Finland": "FI", "Austria": "AT",
    "Switzerland": "CH", "Portugal": "PT", "Greece": "GR", "Iran": "IR",
    "Turkey": "TR", "Hungary": "HU", "Czech Republic": "CZ", "Romania": "RO",
    "New Zealand": "NZ", "Hong Kong": "HK", "Taiwan": "TW", "Thailand": "TH",
    "Israel": "IL", "West Germany": "DE", "East Germany": "DE",
    "Soviet Union": "SU", "Yugoslavia": "YU",
}

def norm_pais(c: str) -> str:
    if not c: return ""
    first = c.split(",")[0].strip()
    return PAIS_MAP.get(first, first[:3].upper())

def main():
    with CSV_PATH.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader); fieldnames = reader.fieldnames

    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    id_cache: dict[str, dict] = {}

    filled_anio = filled_pais = filled_dir = 0

    for r in rows:
        imdb = r.get("imdb_id", "")
        if not imdb or imdb == "N/A": continue

        need_anio = not r.get("anio")
        need_pais = not r.get("pais")
        need_dir  = not r.get("director")
        if not (need_anio or need_pais or need_dir): continue

        # Busca en cache primero (por título+año) — ya tenemos las respuestas
        found = None
        for v in cache.values():
            if isinstance(v, dict) and v.get("imdbID") == imdb:
                found = v; break

        if not found:
            if imdb in id_cache:
                found = id_cache[imdb]
            else:
                found = omdb_by_id(imdb)
                id_cache[imdb] = found
                time.sleep(SLEEP)

        if found.get("Response") != "True": continue

        if need_anio and found.get("Year"):
            r["anio"] = found["Year"][:4]; filled_anio += 1
        if need_pais and found.get("Country"):
            r["pais"] = norm_pais(found["Country"]); filled_pais += 1
        if need_dir and found.get("Director") and found["Director"] != "N/A":
            r["director"] = found["Director"].split(",")[0].strip(); filled_dir += 1

    with CSV_PATH.open("w", newline="") as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    print(f"Rellenados: anio={filled_anio}  pais={filled_pais}  director={filled_dir}")

if __name__ == "__main__":
    main()
