#!/usr/bin/env python3
"""Reorganiza LARGOMETRAJES/DOCUMENTALES/CORTOMETRAJES:
1) Detecta duplicados por (titulo_original, anio). El de mejor calidad se queda;
   los demás se mueven a /Volumes/INES_6TB/VIDEOS/_DUPLICADOS/.
2) Renombra cada carpeta al patrón 'Titulo [AAAA] PAI Director' (código país 3 letras)."""
import os, csv, re, shutil
from collections import defaultdict
from pathlib import Path

CSV = Path("/Users/inesg.calvo/reporte_videos/recomendaciones.csv")
ROOT = Path("/Volumes/INES_6TB/VIDEOS")
DUP  = ROOT / "_DUPLICADOS"
LOG_DUP  = Path("/Users/inesg.calvo/reporte_videos/duplicados.log")
LOG_REN  = Path("/Users/inesg.calvo/reporte_videos/renombrados.log")

TARGET_CATS = {"LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES"}

# Mapa a ISO 3166-1 alpha-3
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
    "BEL":"BEL","BEL":"BEL","BELGIUM":"BEL",
    "NAM":"NAM","NAMIBIA":"NAM",
    "PER":"PER","PERU":"PER","PE":"PER",
    "CHL":"CHL","CHILE":"CHL","CL":"CHL","CHI":"CHL",
    "CO":"COL","COL":"COL","COLOMBIA":"COL",
    "EG":"EGY","EGY":"EGY","EGYPT":"EGY",
    "AL":"ALB","ALB":"ALB",
    "BG":"BGR","BGR":"BGR",
    "HR":"HRV","HRV":"HRV",
    "IS":"ISL","ISL":"ISL",
    "SK":"SVK","SVK":"SVK",
    "SI":"SVN","SVN":"SVN",
    "UA":"UKR","UKR":"UKR",
    "VN":"VNM","VNM":"VNM",
    "ZA":"ZAF","ZAF":"ZAF",
    "AF":"AFG","AFG":"AFG",
    "PK":"PAK","PAK":"PAK",
    "MY":"MYS","MYS":"MYS",
    "SG":"SGP","SGP":"SGP",
    "ID":"IDN","IDN":"IDN",
    "N/A":"","":"",
}

def norm_pais(c: str) -> str:
    if not c: return ""
    c = c.strip().upper()
    # Coge solo el primer país si vienen múltiples
    c = re.split(r'[,/]', c)[0].strip()
    return PAIS_3L.get(c, c[:3] if len(c) >= 3 else c)

def to_float(s, default=0.0):
    try: return float(s.replace(',', '.'))
    except (ValueError, AttributeError): return default

def to_int(s, default=0):
    try: return int(s)
    except (ValueError, TypeError): return default

def safe_name(s: str) -> str:
    """Limpia nombre para filesystem: sin /, colapsa espacios."""
    s = s.replace('/', ' - ').replace(':', ' -')
    s = re.sub(r'\s{2,}', ' ', s).strip(' .-')
    return s

def build_dirname(titulo: str, anio: str, pais3: str, director: str) -> str:
    partes = [safe_name(titulo)]
    if anio: partes.append(f"[{anio}]")
    if pais3: partes.append(pais3)
    if director: partes.append(safe_name(director))
    return ' '.join(partes)

def main():
    with CSV.open() as f:
        rows = [r for r in csv.DictReader(f, delimiter=';') if r["categoria"] in TARGET_CATS
                and r["recomendacion"] != "DELETE"]

    # ============ FASE 1: Duplicados ============
    grupos = defaultdict(list)
    for r in rows:
        titulo_key = re.sub(r'[^a-z0-9]+', '', r["titulo_original"].lower())
        anio = r["anio"] or ""
        if titulo_key and anio:  # solo agrupamos si hay título+año fiables
            grupos[(titulo_key, anio)].append(r)

    duplicados = {k: v for k, v in grupos.items() if len(v) > 1}
    print(f"Grupos con duplicados: {len(duplicados)}")

    log_dup = []; dup_moved = 0
    perdedores = set()  # rutas que serán movidas
    for (t, a), items in duplicados.items():
        # Ordena de mejor a peor: alto DESC, bitrate DESC, tamaño DESC
        items.sort(key=lambda r: (to_int(r["alto"]), to_float(r["bitrate_Mbps"]), to_float(r["tamano_GB"])), reverse=True)
        ganador = items[0]
        log_dup.append(f"\n== {ganador['titulo_original']} [{a}] ==")
        log_dup.append(f"  GANADOR: {ganador['ruta_relativa']} ({ganador['calidad']}, {ganador['tamano_GB']} GB)")
        for perd in items[1:]:
            log_dup.append(f"  A DUP:   {perd['ruta_relativa']} ({perd['calidad']}, {perd['tamano_GB']} GB)")
            perdedores.add(perd["ruta_relativa"])
            src = ROOT / perd["ruta_relativa"]
            dst = DUP / perd["ruta_relativa"]
            if not src.exists():
                log_dup.append(f"    ! no existe: {src}")
                continue
            if dst.exists():
                log_dup.append(f"    ! destino ya existe, skip")
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(src), str(dst))
                dup_moved += 1
            except Exception as e:
                log_dup.append(f"    ! ERROR: {e}")

    LOG_DUP.write_text("\n".join(log_dup))
    print(f"Movidos a _DUPLICADOS: {dup_moved}")

    # ============ FASE 2: Renombrar ============
    log_ren = []; renamed = 0; skipped = 0
    for r in rows:
        if r["ruta_relativa"] in perdedores:  # ya movido
            continue

        rel = Path(r["ruta_relativa"])
        src = ROOT / rel
        if not src.exists() or not src.is_dir():
            # Sólo renombramos carpetas de peli (no ficheros sueltos)
            skipped += 1; continue

        titulo = r["titulo_original"].strip()
        anio   = r["anio"].strip()
        pais3  = norm_pais(r["pais"])
        director = r["director"].strip()

        # Requiere al menos titulo + anio para renombrar
        if not (titulo and anio):
            skipped += 1
            log_ren.append(f"SKIP (falta titulo/anio): {rel}")
            continue

        nuevo_nombre = build_dirname(titulo, anio, pais3, director)
        if src.name == nuevo_nombre:
            continue  # ya está bien

        dst = src.parent / nuevo_nombre
        if dst.exists():
            skipped += 1
            log_ren.append(f"COLISION: {src.name} -> {nuevo_nombre} ya existe")
            continue

        try:
            src.rename(dst)
            renamed += 1
            log_ren.append(f"{src.name}  ->  {nuevo_nombre}")
        except Exception as e:
            log_ren.append(f"ERROR: {src.name} -> {e}")
            skipped += 1

    LOG_REN.write_text("\n".join(log_ren))
    print(f"Renombrados: {renamed}")
    print(f"Skipped: {skipped}")
    print(f"Log duplicados: {LOG_DUP}")
    print(f"Log renombrados: {LOG_REN}")

if __name__ == "__main__":
    main()
