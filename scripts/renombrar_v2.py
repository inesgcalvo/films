#!/usr/bin/env python3
"""Segunda pasada de renombrado: para pelis cuya ruta apunta a un archivo,
renombra la CARPETA padre. Colecciona nombres deseados por carpeta y renombra
solo una vez por carpeta física."""
import os, csv, re
from collections import defaultdict
from pathlib import Path

CSV = Path("/Users/inesg.calvo/reporte_videos/recomendaciones.csv")
ROOT = Path("/Volumes/INES_6TB/VIDEOS")
LOG = Path("/Users/inesg.calvo/reporte_videos/renombrados_v2.log")
TARGET_CATS = {"LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES"}

PAIS_3L = {
    "US":"USA","USA":"USA","UNITED STATES":"USA",
    "UK":"GBR","GB":"GBR","GBR":"GBR","UNITED KINGDOM":"GBR",
    "FR":"FRA","FRA":"FRA","FRANCE":"FRA",
    "ES":"ESP","ESP":"ESP","SPAIN":"ESP",
    "DE":"DEU","GER":"DEU","DEU":"DEU","GERMANY":"DEU","WEST GERMANY":"DEU","EAST GERMANY":"DEU",
    "IT":"ITA","ITA":"ITA","ITALY":"ITA",
    "JP":"JPN","JPN":"JPN","JAP":"JPN","JAPAN":"JPN",
    "CA":"CAN","CAN":"CAN","CANADA":"CAN",
    "AU":"AUS","AUS":"AUS","AUSTRALIA":"AUS",
    "SE":"SWE","SWE":"SWE","SWEDEN":"SWE",
    "DK":"DNK","DEN":"DNK","DNK":"DNK","DENMARK":"DNK",
    "NL":"NLD","NED":"NLD","NLD":"NLD","NETHERLANDS":"NLD",
    "BE":"BEL","BEL":"BEL","BELGIUM":"BEL",
    "RU":"RUS","RUS":"RUS","RUSSIA":"RUS",
    "CN":"CHN","CHN":"CHN","CHINA":"CHN",
    "KR":"KOR","KOR":"KOR","SOUTH KOREA":"KOR",
    "BR":"BRA","BRA":"BRA","BRAZIL":"BRA",
    "MX":"MEX","MEX":"MEX","MEXICO":"MEX",
    "AR":"ARG","ARG":"ARG","ARGENTINA":"ARG",
    "IN":"IND","IND":"IND","INDIA":"IND",
    "PL":"POL","POL":"POL","POLAND":"POL",
    "IE":"IRL","IRL":"IRL","IRELAND":"IRL",
    "NO":"NOR","NOR":"NOR","NORWAY":"NOR",
    "FI":"FIN","FIN":"FIN","FINLAND":"FIN",
    "AT":"AUT","AUT":"AUT","AUSTRIA":"AUT",
    "CH":"CHE","CHE":"CHE","SWITZERLAND":"CHE",
    "PT":"PRT","PRT":"PRT","PORTUGAL":"PRT",
    "GR":"GRC","GRC":"GRC","GREECE":"GRC",
    "IR":"IRN","IRN":"IRN","IRAN":"IRN",
    "TR":"TUR","TUR":"TUR","TURKEY":"TUR",
    "HU":"HUN","HUN":"HUN","HUNGARY":"HUN",
    "CZ":"CZE","CZE":"CZE","CZECH REPUBLIC":"CZE",
    "RO":"ROU","ROM":"ROU","RUM":"ROU","ROU":"ROU","ROMANIA":"ROU",
    "NZ":"NZL","NZL":"NZL","NEW ZEALAND":"NZL",
    "HK":"HKG","HKG":"HKG","HONG KONG":"HKG",
    "TW":"TWN","TWN":"TWN","TAIWAN":"TWN",
    "TH":"THA","THA":"THA","THAILAND":"THA",
    "IL":"ISR","ISR":"ISR","ISRAEL":"ISR",
    "SU":"SUN","SUN":"SUN","SOVIET UNION":"SUN",
    "YU":"YUG","YUG":"YUG","YUGOSLAVIA":"YUG",
    "PH":"PHL","PHL":"PHL","PHILIPPINES":"PHL",
    "PE":"PER","PER":"PER","PERU":"PER",
    "CL":"CHL","CHL":"CHL","CHI":"CHL","CHILE":"CHL",
    "CO":"COL","COL":"COL","COLOMBIA":"COL",
    "EG":"EGY","EGY":"EGY","EGYPT":"EGY",
    "UA":"UKR","UKR":"UKR","UKRAINE":"UKR",
    "ZA":"ZAF","ZAF":"ZAF","SOUTH AFRICA":"ZAF",
    "N/A":"","":"",
}

def norm_pais(c: str) -> str:
    if not c: return ""
    c = c.strip().upper()
    c = re.split(r'[,/]', c)[0].strip()
    return PAIS_3L.get(c, c[:3] if len(c) >= 3 else c)

def safe_name(s: str) -> str:
    s = s.replace('/', ' - ').replace(':', ' -')
    s = re.sub(r'\s{2,}', ' ', s).strip(' .-')
    return s

def build_dirname(t, a, p, d):
    partes = [safe_name(t)]
    if a: partes.append(f"[{a}]")
    if p: partes.append(p)
    if d: partes.append(safe_name(d))
    return ' '.join(partes)

def main():
    with CSV.open() as f:
        rows = [r for r in csv.DictReader(f, delimiter=';') if r["categoria"] in TARGET_CATS
                and r["recomendacion"] != "DELETE"]

    log = []; renamed = 0; skipped_no_data = 0; skipped_missing = 0; collisions = 0
    ya_procesadas = set()

    for r in rows:
        titulo = r["titulo_original"].strip()
        anio   = r["anio"].strip()
        pais3  = norm_pais(r["pais"])
        director = r["director"].strip()

        if not (titulo and anio):
            skipped_no_data += 1
            log.append(f"SIN DATOS: {r['ruta_relativa']}")
            continue

        # La CARPETA a renombrar = nivel 2 de la ruta (LARGOMETRAJES/<carpeta>/...)
        rel = Path(r["ruta_relativa"])
        parts = rel.parts
        if len(parts) < 2:
            skipped_missing += 1; continue

        carpeta_actual = ROOT / parts[0] / parts[1]
        if carpeta_actual in ya_procesadas: continue
        ya_procesadas.add(carpeta_actual)

        if not carpeta_actual.exists():
            skipped_missing += 1
            log.append(f"NO EXISTE (movida?): {carpeta_actual.name}")
            continue

        # Si es archivo suelto (no carpeta), no renombrar carpeta (podríamos renombrar el archivo)
        if not carpeta_actual.is_dir():
            log.append(f"SUELTO (archivo, no carpeta): {carpeta_actual.name}")
            skipped_missing += 1; continue

        nuevo_nombre = build_dirname(titulo, anio, pais3, director)
        if carpeta_actual.name == nuevo_nombre:
            continue

        dst = carpeta_actual.parent / nuevo_nombre
        if dst.exists():
            collisions += 1
            log.append(f"COLISION: '{carpeta_actual.name}' -> '{nuevo_nombre}' (destino ya existe)")
            continue

        try:
            carpeta_actual.rename(dst)
            renamed += 1
            log.append(f"OK: {carpeta_actual.name}\n    -> {nuevo_nombre}")
        except Exception as e:
            log.append(f"ERROR {carpeta_actual.name}: {e}")
            skipped_missing += 1

    LOG.write_text("\n".join(log))
    print(f"Renombrados: {renamed}")
    print(f"Sin datos suficientes (falta titulo/anio): {skipped_no_data}")
    print(f"No existen o no son carpeta: {skipped_missing}")
    print(f"Colisiones: {collisions}")
    print(f"Log: {LOG}")

if __name__ == "__main__":
    main()
