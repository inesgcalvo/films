#!/usr/bin/env python3
"""Consulta OMDb para cada peli/doc/corto del catalogo colapsado.
Guarda cache incremental por si se corta. Rellena imdb_id, nota_imdb,
titulo_original (corregido si OMDb dice otro) y titulo_es (si difiere)."""
import os, csv, json, sys, time, urllib.parse, urllib.request
from pathlib import Path

API_KEY = os.environ.get("OMDB_API_KEY", "")
IN  = Path("/Users/inesg.calvo/reporte_videos/catalogo_v2_colapsado.csv")
OUT = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")
CACHE = Path("/Users/inesg.calvo/reporte_videos/omdb_cache.json")
LOG   = Path("/Users/inesg.calvo/reporte_videos/omdb.log")

TARGET_CATS = {"LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES"}
SLEEP = 0.25  # ~4 req/s

def load_cache():
    if CACHE.exists():
        return json.loads(CACHE.read_text())
    return {}

def save_cache(c):
    CACHE.write_text(json.dumps(c, ensure_ascii=False, indent=1))

def omdb_lookup(title: str, year: str) -> dict:
    """Busca por título + año. Prueba con año, sin año, y quitando artículos."""
    for t, y in [(title, year), (title, ''), (strip_leading_article(title), year)]:
        if not t: continue
        params = {"apikey": API_KEY, "t": t, "type": "movie"}
        if y: params["y"] = y
        url = "https://www.omdbapi.com/?" + urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                data = json.loads(r.read())
        except Exception as e:
            return {"Response": "False", "Error": str(e)}
        if data.get("Response") == "True":
            return data
        time.sleep(SLEEP)
    return data

def strip_leading_article(t: str) -> str:
    for art in ("The ", "El ", "La ", "Los ", "Las ", "Un ", "Una ", "L'", "Le ", "Les "):
        if t.startswith(art):
            return t[len(art):]
    return t

def main():
    with IN.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)
        fieldnames = reader.fieldnames

    cache = load_cache()
    targets = [r for r in rows if r["categoria"] in TARGET_CATS]
    print(f"Total pelis/docs/cortos a consultar: {len(targets)}")

    log_lines = []
    hit = miss = cached = 0

    for i, r in enumerate(targets, 1):
        title = r["titulo_original"].strip()
        year  = r["anio"].strip()
        key = f"{title}|{year}"

        if key in cache:
            data = cache[key]; cached += 1
        else:
            data = omdb_lookup(title, year)
            cache[key] = data
            time.sleep(SLEEP)
            if i % 25 == 0:
                save_cache(cache)
                print(f"[{i}/{len(targets)}] hit={hit} miss={miss} cached={cached}")

        if data.get("Response") == "True":
            hit += 1
            omdb_title = data.get("Title", "")
            r["imdb_id"]   = data.get("imdbID", "")
            r["nota_imdb"] = data.get("imdbRating", "")
            # Si OMDb devuelve un título distinto, el nuestro es probablemente la traducción
            if omdb_title and omdb_title.lower() != title.lower():
                r["titulo_es"] = title           # el que teníamos era la traducción
                r["titulo_original"] = omdb_title # OMDb da el original
                log_lines.append(f"RENAME: {title} -> {omdb_title}")
        else:
            miss += 1
            r["notas"] = "OMDb no encontrado"
            log_lines.append(f"MISS: {title} ({year})")

    save_cache(cache)
    LOG.write_text("\n".join(log_lines))

    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    print(f"\nCompletado: hit={hit}  miss={miss}  cached={cached}")
    print(f"Escrito: {OUT}")
    print(f"Log:     {LOG}")

if __name__ == "__main__":
    main()
