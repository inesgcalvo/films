#!/usr/bin/env python3
"""Segundo pase OMDb:
1) Detecta y REVIERTE false positives del primer pase comparando similitud.
2) Reintenta MISS con búsqueda alternativa (search endpoint, sin acentos, sin subtitulos)."""
import os, csv, json, re, sys, time, unicodedata, urllib.parse, urllib.request
from difflib import SequenceMatcher
from pathlib import Path

API_KEY = os.environ.get("OMDB_API_KEY", "")
IN  = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")
OUT = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")  # sobrescribimos
CACHE = Path("/Users/inesg.calvo/reporte_videos/omdb_cache.json")
LOG   = Path("/Users/inesg.calvo/reporte_videos/omdb_refinar.log")

TARGET_CATS = {"LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES"}
SLEEP = 0.25
SIM_THRESHOLD = 0.65   # bajo este umbral consideramos false positive

def norm(s: str) -> str:
    s = unicodedata.normalize('NFKD', s.lower())
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9]+', ' ', s).strip()
    return s

def sim(a: str, b: str) -> float:
    return SequenceMatcher(None, norm(a), norm(b)).ratio()

def clean_for_search(t: str) -> str:
    # Quita extensiones y sufijos residuales
    t = re.sub(r'\s+(avi|mp4|mkv|mpg)\s*$', '', t, flags=re.I)
    t = re.sub(r'\s*\([^)]*(dvdrip|divx|dvd|hdtv|spanish|latino|dual)[^)]*\)\s*', ' ', t, flags=re.I)
    t = re.sub(r'\s{2,}', ' ', t).strip()
    return t

def omdb_get(params: dict) -> dict:
    params = {"apikey": API_KEY, **params}
    url = "https://www.omdbapi.com/?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"Response": "False", "Error": str(e)}

def search_and_match(title: str, year: str) -> dict | None:
    """Prueba varias estrategias: t=título+año, t=título, s=título+año filtrando por año."""
    cleaned = clean_for_search(title)
    candidates = []

    # Estrategia 1: t= con año
    for t in [cleaned, norm(cleaned)]:
        if not t: continue
        for y in [year, '']:
            params = {"t": t, "type": "movie"}
            if y: params["y"] = y
            d = omdb_get(params)
            time.sleep(SLEEP)
            if d.get("Response") == "True":
                candidates.append(d)

    # Estrategia 2: s= (search) filtrar por año
    d = omdb_get({"s": cleaned, "type": "movie"})
    time.sleep(SLEEP)
    if d.get("Response") == "True":
        for item in d.get("Search", [])[:5]:
            if year and item.get("Year", "").startswith(year):
                detail = omdb_get({"i": item["imdbID"]})
                time.sleep(SLEEP)
                if detail.get("Response") == "True":
                    candidates.append(detail)

    # Elige el candidato con mayor similitud al título original + año coincidente
    best = None; best_score = 0
    for c in candidates:
        s = sim(title, c.get("Title", ""))
        # bonus si el año coincide exactamente
        if year and c.get("Year", "").startswith(year):
            s += 0.15
        if s > best_score:
            best_score = s; best = c

    if best and best_score >= SIM_THRESHOLD:
        return best
    return None

def main():
    with IN.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader); fieldnames = reader.fieldnames

    log = []
    fixed_fp = 0    # false positives revertidos
    fixed_miss = 0  # miss ahora resueltos
    kept_miss = 0

    for r in rows:
        if r["categoria"] not in TARGET_CATS: continue

        titulo_actual   = r["titulo_original"]
        titulo_es       = r["titulo_es"]
        anio            = r["anio"]
        imdb_id         = r["imdb_id"]
        buscar          = titulo_es if titulo_es else titulo_actual  # buscar por el título más probable

        # -- CASO 1: teníamos match en pase 1 -> validar similitud
        if imdb_id:
            # Recuperar el que se buscó originalmente
            orig_search = titulo_es if titulo_es else titulo_actual
            s = sim(orig_search, titulo_actual)
            if s < SIM_THRESHOLD:
                # False positive: revertir
                log.append(f"FP-REVERT sim={s:.2f}: buscado='{orig_search}' -> OMDb='{titulo_actual}' (imdb={imdb_id})")
                r["titulo_original"] = orig_search
                r["titulo_es"] = ""
                r["imdb_id"] = ""
                r["nota_imdb"] = ""
                r["notas"] = "OMDb devolvió título distinto (revertido)"
                fixed_fp += 1
                # Intentar de nuevo con búsqueda más rigurosa
                match = search_and_match(orig_search, anio)
                if match:
                    r["titulo_original"] = match["Title"]
                    if norm(match["Title"]) != norm(orig_search):
                        r["titulo_es"] = orig_search
                    r["imdb_id"] = match.get("imdbID", "")
                    r["nota_imdb"] = match.get("imdbRating", "")
                    r["notas"] = ""
                    fixed_miss += 1
                    log.append(f"  -> reencontrado: {match['Title']} ({match.get('Year')}) imdb={match['imdbID']}")
            continue

        # -- CASO 2: era MISS -> reintentar
        if not imdb_id:
            match = search_and_match(buscar, anio)
            if match:
                r["titulo_original"] = match["Title"]
                if norm(match["Title"]) != norm(buscar):
                    r["titulo_es"] = buscar
                r["imdb_id"] = match.get("imdbID", "")
                r["nota_imdb"] = match.get("imdbRating", "")
                r["notas"] = ""
                fixed_miss += 1
                log.append(f"MISS-FIX: {buscar} ({anio}) -> {match['Title']} imdb={match['imdbID']}")
            else:
                kept_miss += 1
                if not r.get("notas"):
                    r["notas"] = "OMDb no encontrado (2 pases)"

    LOG.write_text("\n".join(log))

    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    print(f"False positives revertidos: {fixed_fp}")
    print(f"Miss resueltos en 2º pase:  {fixed_miss}")
    print(f"Miss que siguen sin match:  {kept_miss}")
    print(f"Log: {LOG}")

if __name__ == "__main__":
    main()
