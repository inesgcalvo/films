#!/usr/bin/env python3
"""Mueve todo lo marcado como DELETE a /Volumes/INES_6TB/VIDEOS/_BORRAR/
preservando la estructura de carpetas. Reversible: no borra nada."""
import os, csv, shutil, sys
from pathlib import Path

CSV = Path("/Users/inesg.calvo/reporte_videos/recomendaciones.csv")
ROOT = Path("/Volumes/INES_6TB/VIDEOS")
DEST = ROOT / "_BORRAR"
LOG = Path("/Users/inesg.calvo/reporte_videos/movidos_a_borrar.log")

def main():
    with CSV.open() as f:
        rows = [r for r in csv.DictReader(f, delimiter=';') if r["recomendacion"] == "DELETE"]

    print(f"A mover: {len(rows)} elementos")

    moved = 0; failed = 0; total_gb = 0.0
    log = []

    for r in rows:
        rel = Path(r["ruta_relativa"])
        src = ROOT / rel
        dst = DEST / rel

        if not src.exists():
            failed += 1
            log.append(f"NO EXISTE: {src}")
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            if dst.exists():
                log.append(f"SKIP (destino ya existe): {rel}")
                failed += 1; continue
            shutil.move(str(src), str(dst))
            gb = float(r["tamano_GB"].replace(',', '.') or 0)
            total_gb += gb
            moved += 1
            log.append(f"OK {gb:.2f} GB: {rel}")
        except Exception as e:
            failed += 1
            log.append(f"ERROR: {rel} -> {e}")

    LOG.write_text("\n".join(log))
    print(f"Movidos: {moved}")
    print(f"Fallos:  {failed}")
    print(f"Espacio liberado del árbol principal: {total_gb:.1f} GB")
    print(f"Ubicación: {DEST}")
    print(f"Log: {LOG}")

if __name__ == "__main__":
    main()
