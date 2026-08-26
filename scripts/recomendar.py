#!/usr/bin/env python3
"""Calcula recomendación KEEP/DELETE/REVIEW combinando nota IMDb × calidad × edad.

Reglas (pelis/docs/cortos):
  KEEP    = nota >= 7.0
          | (nota >= 6.0 y año pre-1970)   # clásico irremplazable
          | (nota >= 6.0 y alto >= 1000)   # buena peli en Full HD
  DELETE  = nota < 5.0 y alto < 700       # mala peli en mala calidad
          | nota < 6.0 y alto < 480        # peli mediocre en calidad basura
  REVIEW  = todo lo demás (sin nota, casos límite, etc.)

Series se recomiendan solo por calidad global (no tenemos nota IMDb):
  KEEP    = alto >= 720
  DELETE  = alto < 480 y bitrate < 1
  REVIEW  = resto

Videos musicales quedan como REVIEW (decisión personal).
"""
import os, csv
from collections import defaultdict
from pathlib import Path

CSV_IN  = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")
CSV_OUT = Path("/Users/inesg.calvo/reporte_videos/recomendaciones.csv")
MD_OUT  = Path("/Users/inesg.calvo/reporte_videos/RECOMENDACIONES.md")

def to_float(s, default=0.0):
    if not s: return default
    try: return float(s.replace(',', '.'))
    except ValueError: return default

def to_int(s, default=0):
    try: return int(s)
    except (ValueError, TypeError): return default

def recomendar(r):
    cat = r["categoria"]
    alto = to_int(r["alto"])
    br   = to_float(r["bitrate_Mbps"])
    nota = to_float(r["nota_imdb"], -1)
    anio = to_int(r["anio"])

    razones = []

    if cat in ("LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES"):
        if nota < 0:
            return "REVIEW", "sin nota IMDb"
        if nota >= 7.0:
            return "KEEP", f"nota alta {nota}"
        if nota >= 6.0 and anio and anio < 1970:
            return "KEEP", f"clásico pre-1970 ({anio}, {nota})"
        if nota >= 6.0 and alto >= 1000:
            return "KEEP", f"nota {nota} en Full HD"
        if nota < 5.0 and alto < 700:
            return "DELETE", f"nota baja {nota} y calidad {alto}p"
        if nota < 6.0 and alto < 480:
            return "DELETE", f"nota mediocre {nota} y calidad basura {alto}p"
        return "REVIEW", f"caso límite (nota {nota}, {alto}p)"

    if cat == "SERIES":
        if alto >= 720: return "KEEP", f"HD ({alto}p)"
        if alto < 480 and br < 1: return "DELETE", f"SD basura ({alto}p, {br} Mbps)"
        return "REVIEW", f"SD medio ({alto}p, {br} Mbps)"

    return "REVIEW", "requiere decisión personal"

def main():
    with CSV_IN.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader); fields = reader.fieldnames + ["recomendacion", "razon"]

    stats = defaultdict(lambda: {"n": 0, "gb": 0.0})
    for r in rows:
        rec, razon = recomendar(r)
        r["recomendacion"] = rec; r["razon"] = razon
        gb = to_float(r["tamano_GB"])
        stats[(r["categoria"], rec)]["n"] += 1
        stats[(r["categoria"], rec)]["gb"] += gb
        stats[("TOTAL", rec)]["n"] += 1
        stats[("TOTAL", rec)]["gb"] += gb

    with CSV_OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    # Markdown resumen
    def sort_key(r):
        # Ordena las de DELETE por "más peso liberado" primero
        return -to_float(r["tamano_GB"])

    md = []
    md.append("# Recomendaciones borrar/mantener")
    md.append("")
    md.append("Criterio: **nota IMDb × resolución × antigüedad**. Clásicos pre-1970 con nota ≥ 6 se protegen aunque la calidad sea mala (no existen mejores copias). Series se juzgan solo por calidad (no consultamos IMDb por serie).")
    md.append("")
    md.append("## Impacto por categoría")
    md.append("")
    md.append("| Categoría | KEEP | DELETE | REVIEW | GB liberados si borras DELETE |")
    md.append("|---|---:|---:|---:|---:|")
    for cat in ("LARGOMETRAJES", "SERIES", "DOCUMENTALES", "CORTOMETRAJES", "VIDEOS_MUSICALES"):
        k = stats.get((cat, "KEEP"), {"n":0,"gb":0}); d = stats.get((cat, "DELETE"), {"n":0,"gb":0}); rv = stats.get((cat, "REVIEW"), {"n":0,"gb":0})
        md.append(f"| {cat} | {k['n']} | {d['n']} | {rv['n']} | {d['gb']:.0f} |")
    tk = stats.get(("TOTAL","KEEP"),{"n":0,"gb":0}); td = stats.get(("TOTAL","DELETE"),{"n":0,"gb":0}); tr = stats.get(("TOTAL","REVIEW"),{"n":0,"gb":0})
    md.append(f"| **TOTAL** | **{tk['n']}** | **{td['n']}** | **{tr['n']}** | **{td['gb']:.0f}** |")
    md.append("")

    # Top 30 DELETE
    md.append("## Top 30 candidatos claros a borrar (más GB liberados)")
    md.append("")
    md.append("| Título | Año | Nota IMDb | Calidad | GB | Razón |")
    md.append("|---|---:|---:|---|---:|---|")
    dels = sorted([r for r in rows if r["recomendacion"] == "DELETE"], key=sort_key)[:30]
    for r in dels:
        md.append(f"| {r['titulo_original']} | {r['anio']} | {r['nota_imdb'] or '-'} | {r['calidad']} | {to_float(r['tamano_GB']):.1f} | {r['razon']} |")

    # Top 30 KEEP mejor calidad+nota
    md.append("")
    md.append("## Top 30 KEEP (joyas de la colección)")
    md.append("")
    md.append("| Título | Año | Nota IMDb | Calidad | GB |")
    md.append("|---|---:|---:|---|---:|")
    def keep_score(r):
        return -(to_float(r["nota_imdb"]) * 100 + to_int(r["alto"]) * 0.01)
    keeps = sorted([r for r in rows if r["recomendacion"] == "KEEP" and r["categoria"]!="SERIES"], key=keep_score)[:30]
    for r in keeps:
        md.append(f"| {r['titulo_original']} | {r['anio']} | {r['nota_imdb'] or '-'} | {r['calidad']} | {to_float(r['tamano_GB']):.1f} |")

    # Casos REVIEW notables (nota alta pero calidad malísima → candidatos a reemplazar)
    md.append("")
    md.append("## Candidatos a REEMPLAZAR (joyas en muy baja calidad)")
    md.append("_Nota ≥ 7 pero resolución < 480p — merece la pena buscar copia mejor._")
    md.append("")
    md.append("| Título | Año | Nota | Alto | GB |")
    md.append("|---|---:|---:|---:|---:|")
    joyas_baja = sorted([r for r in rows if r["categoria"] in ("LARGOMETRAJES","DOCUMENTALES","CORTOMETRAJES")
                         and to_float(r["nota_imdb"]) >= 7.0 and to_int(r["alto"]) > 0 and to_int(r["alto"]) < 480],
                         key=lambda r: -to_float(r["nota_imdb"]))
    for r in joyas_baja:
        md.append(f"| {r['titulo_original']} | {r['anio']} | {r['nota_imdb']} | {r['alto']} | {to_float(r['tamano_GB']):.1f} |")

    md.append("")
    md.append("---")
    md.append(f"_CSV completo con columna `recomendacion`: `{CSV_OUT}`_")
    MD_OUT.write_text("\n".join(md))

    print(f"Escrito: {CSV_OUT}")
    print(f"Escrito: {MD_OUT}")
    print(f"KEEP: {tk['n']} archivos ({tk['gb']:.0f} GB)")
    print(f"DELETE: {td['n']} archivos ({td['gb']:.0f} GB) <- espacio recuperable")
    print(f"REVIEW: {tr['n']} archivos ({tr['gb']:.0f} GB)")

if __name__ == "__main__":
    main()
