#!/usr/bin/env python3
"""
Re-parsea el catalogo, limpiando titulos y extrayendo año/pais/director
con reglas mucho mas laxas. Añade columnas titulo_original, titulo_es,
nota_imdb, imdb_id (vacias por ahora, se rellenan luego con OMDb).
"""
import os, csv, re, sys
from pathlib import Path

IN  = Path("/Users/inesg.calvo/reporte_videos/catalogo.csv")
OUT = Path("/Users/inesg.calvo/reporte_videos/catalogo_v2.csv")

# Scene tags / calidad / codecs / release groups que hay que quitar del título
SCENE_TAGS = [
    r'\b(?:1080p|720p|2160p|480p|576p|4k|uhd)\b',
    r'\b(?:bluray|blu-ray|brrip|bdrip|bdremux|remux|webrip|web-dl|webdl|hdtv|hdtvrip|dvdrip|dvdscr|dvd|hdrip|hd|bd|vhs)\b',
    r'\b(?:x264|x265|h264|h265|hevc|avc|xvid|divx|mpeg[24]?)\b',
    r'\b(?:ac3|aac|dts(?:-hd|-ma)?|mp3|flac|truehd|atmos|opus|5\.1|7\.1|2\.0)\b',
    r'\b(?:dual|multi|latino|castellano|spanish|english|vose|vos|subs?|subtitles)\b',
    r'\b(?:extended|unrated|directors?\.?cut|remastered|criterion|imax|proper|repack|internal|complete)\b',
    # release groups conocidos (bien con -, o sueltos)
    r'\b(?:yify|rarbg|sparks|anoxmous|axxo|fgt|evo|ctrlhd|amiable|geckos|deflate|ntb|mkvcage|joy|kingdom|reward|karina|noir|vector|dexzaery|bhdstudio|fxg|donizzz|hive-cm8)\b',
    r'-[A-Z0-9]{2,}$',                              # release group tail en MAYUSCULAS: -SPARKS, -JYK
    r'\[[^\]]*\]',                                  # cualquier cosa entre corchetes
    r'\{[^}]*\}',                                   # cualquier cosa entre llaves
    r'www\.[^\s.]+\.[a-z]+',                        # www.xxx.com
    r'\.[a-z0-9]{2,4}$',                            # extensión sobrante
]
SCENE_RE = re.compile('|'.join(SCENE_TAGS), re.IGNORECASE)

# Año 4 dígitos entre 1900-2099 con delimitador o entre paréntesis/corchetes
YEAR_RE = re.compile(r'(?:^|[\s\.\[\(_-])((?:19|20)\d{2})(?:[\s\.\]\)_-]|$)')

# País: código en mayúsculas 2-4 letras que aparece justo después del año
COUNTRY_RE = re.compile(r'\[(?:19|20)\d{2}\]\s+([A-Z]{2,4})(?:\s|$)')

def clean_title(raw: str) -> str:
    """Quita scene tags, extensión, separadores raros y colapsa espacios."""
    s = raw
    # Sustituye separadores comunes por espacio
    s = re.sub(r'[._]', ' ', s)
    # Aplica limpieza scene-tag iterativa (hasta que no cambia)
    for _ in range(4):
        s = SCENE_RE.sub(' ', s)
    # Quita años sueltos al final (los devolvemos separados)
    s = YEAR_RE.sub(' ', s)
    # Quita paréntesis/corchetes vacíos
    s = re.sub(r'\(\s*\)|\[\s*\]|\{\s*\}', '', s)
    # Colapsa espacios y separadores sueltos
    s = re.sub(r'\s*-\s*$', '', s)
    s = re.sub(r'^\s*-\s*', '', s)
    s = re.sub(r'\s{2,}', ' ', s).strip(' -_.')
    return s

def extract_year(raw: str) -> str:
    m = YEAR_RE.search(raw)
    return m.group(1) if m else ''

def extract_country(raw: str) -> str:
    # Solo si viene en el patrón "[AAAA] XXX"
    m = COUNTRY_RE.search(raw)
    return m.group(1) if m else ''

def is_probably_scene_tail(s: str) -> bool:
    """Detecta si un candidato a director es en realidad un scene tag residual."""
    if not s: return True
    if re.fullmatch(r'[A-Z0-9]{2,}', s): return True
    if re.search(r'\b(?:brrip|1080p|720p|bluray|dvdrip|hdtv|x264|xvid|yify|rarbg|sparks|anoxmous)\b', s.lower()): return True
    return False

def extract_director(raw: str, titulo_limpio: str, anio: str, pais: str) -> str:
    """Intenta extraer director de patrones típicos:
       'Título [AAAA] PAÍS Director'
       'Título - Director AAAA'
    """
    # Patrón 1: tras corchetes de año y país
    m = re.search(r'\[(?:19|20)\d{2}\]\s+[A-Z]{2,4}\s+(.+?)$', raw)
    if m:
        cand = m.group(1).strip()
        if not is_probably_scene_tail(cand) and cand.lower() != titulo_limpio.lower():
            return cand
    # Patrón 2: "Título - Director AAAA"
    m = re.match(r'^(.+?)\s+-\s+(.+?)\s+(?:19|20)\d{2}\s*$', raw)
    if m:
        cand = m.group(2).strip()
        titulo_pref = m.group(1).strip()
        if not is_probably_scene_tail(cand) and cand.lower() != titulo_pref.lower():
            return cand
    # Patrón 3: tras corchetes de año sin país
    m = re.search(r'\[(?:19|20)\d{2}\]\s+(.+?)$', raw)
    if m:
        cand = m.group(1).strip()
        if re.fullmatch(r'[A-Z]{2,4}', cand):
            return ''
        if is_probably_scene_tail(cand): return ''
        if cand.lower() == titulo_limpio.lower(): return ''
        return cand
    return ''

def reparse(carpeta: str, ruta: str, cat: str):
    """Devuelve (titulo, anio, pais, director)."""
    if cat == 'SERIES':
        return carpeta, '', '', ''
    if cat == 'VIDEOS_MUSICALES':
        return Path(ruta).stem, '', '', ''

    # Fuente principal: nombre de la carpeta de nivel 2
    src = carpeta
    # Si la carpeta no tiene año pero el archivo sí, usa el archivo como pista
    if not YEAR_RE.search(src):
        nombre_fich = Path(ruta).stem
        if YEAR_RE.search(nombre_fich):
            src = f"{carpeta} {nombre_fich}"

    anio = extract_year(src)
    pais = extract_country(src)
    director = extract_director(src, '', anio, pais)

    # Título = carpeta sin corchetes año/país ni director ni scene tags
    titulo = carpeta
    # Quita `[AAAA] PAÍS Director` completo si presente
    titulo = re.sub(r'\s*\[(?:19|20)\d{2}\](?:\s+[A-Z]{2,4})?(?:\s+.+)?$', '', titulo)
    # Quita `- Director AAAA` completo
    titulo = re.sub(r'\s+-\s+.+\s+(?:19|20)\d{2}\s*$', '', titulo)
    # Quita año inline y todo lo que le sigue (scene tags al final)
    titulo = re.sub(r'\s+(?:19|20)\d{2}\b.*$', '', titulo)
    # Limpia scene tags del residuo
    titulo = clean_title(titulo)
    # Recalcula director si el que sacamos coincide con titulo limpio
    if director and director.lower() == titulo.lower():
        director = ''

    return titulo, anio, pais, director

def main():
    with IN.open() as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)
        rows = list(reader)

    # Columnas: mantenemos las 20 originales y añadimos 4 nuevas al final
    new_header = header + ['titulo_es', 'imdb_id', 'nota_imdb', 'notas']
    # Renombramos "titulo" existente a "titulo_original" (posición 1)
    new_header[1] = 'titulo_original'

    out_rows = []
    for r in rows:
        if len(r) < 20:
            continue
        cat, titulo_old, anio_old, pais_old, director_old = r[0], r[1], r[2], r[3], r[4]
        ruta = r[19]

        # Reparse desde la ruta original (mas fiable que el titulo ya extraido)
        parts = ruta.split('/')
        carpeta = parts[1] if len(parts) >= 2 else ruta

        titulo, anio, pais, director = reparse(carpeta, ruta, cat)

        # Solo mantener valores viejos si el reparse NO devolvió nada útil (titulo vacío)
        if not titulo:
            titulo = titulo_old
            if not anio: anio = anio_old
            if not pais: pais = pais_old
            if not director: director = director_old

        r[1] = titulo
        r[2] = anio
        r[3] = pais
        r[4] = director
        # Añade columnas nuevas
        r += ['', '', '', '']
        out_rows.append(r)

    with OUT.open('w', newline='') as f:
        w = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        w.writerow(new_header)
        w.writerows(out_rows)

    print(f"Escrito: {OUT}  ({len(out_rows)} filas)")

if __name__ == '__main__':
    main()
