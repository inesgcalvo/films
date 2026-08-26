#!/usr/bin/env python3
"""Fusiona los datos OMDb + traducciones + notas del CSV de BACKUP
en el catalogo_v2_colapsado.csv recién generado. Escribe catalogo_v3_omdb.csv.
Matching por (categoria, ruta_relativa)."""
import os, csv
from pathlib import Path

FRESH = Path("/Users/inesg.calvo/reporte_videos/catalogo_v2_colapsado.csv")
BACKUP = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.BACKUP.csv")
OUT = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")

PRESERVE = ['titulo_original', 'titulo_es', 'anio', 'pais', 'director',
            'imdb_id', 'nota_imdb', 'notas']

def main():
    with FRESH.open() as f:
        fresh_rows = list(csv.DictReader(f, delimiter=';'))
        fields = list(fresh_rows[0].keys()) if fresh_rows else []

    with BACKUP.open() as f:
        backup_rows = list(csv.DictReader(f, delimiter=';'))

    # Index backup por ruta_relativa
    backup_idx = {r['ruta_relativa']: r for r in backup_rows}

    merged = 0; new = 0
    for r in fresh_rows:
        b = backup_idx.get(r['ruta_relativa'])
        if not b:
            # Prueba también sin sufijo de archivo (si backup tenía carpeta y ahora fichero)
            for k, v in backup_idx.items():
                if k in r['ruta_relativa'] or r['ruta_relativa'] in k:
                    b = v; break
        if b:
            for field in PRESERVE:
                if b.get(field): r[field] = b[field]
            merged += 1
        else:
            new += 1

    with OUT.open('w', newline='') as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(fresh_rows)

    print(f"Fusionadas con backup: {merged}")
    print(f"Sin match en backup (nuevas): {new}")
    print(f"Escrito: {OUT}")

if __name__ == '__main__':
    main()
