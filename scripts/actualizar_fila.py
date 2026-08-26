#!/usr/bin/env python3
"""Actualiza campos de una fila del catalogo_v3_omdb.csv por coincidencia de ruta_relativa.
Uso: actualizar_fila.py <ruta_parcial> [--titulo T] [--titulo-es E] [--anio A] [--pais P]
                       [--director D] [--imdb ID] [--nota N] [--notas TXT]"""
import argparse, csv, sys
from pathlib import Path

CSV = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("ruta", help="Substring de ruta_relativa que identifique la fila")
    p.add_argument("--titulo")
    p.add_argument("--titulo-es", dest="titulo_es")
    p.add_argument("--anio")
    p.add_argument("--pais")
    p.add_argument("--director")
    p.add_argument("--imdb")
    p.add_argument("--nota")
    p.add_argument("--notas")
    args = p.parse_args()

    with CSV.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader); fields = reader.fieldnames

    matches = [r for r in rows if args.ruta in r["ruta_relativa"]]
    if len(matches) == 0:
        print(f"NO MATCH: {args.ruta}"); sys.exit(1)
    if len(matches) > 1:
        print(f"MULTIPLE MATCHES ({len(matches)}):")
        for m in matches: print(f"  - {m['ruta_relativa']}")
        sys.exit(1)

    r = matches[0]
    if args.titulo:    r["titulo_original"] = args.titulo
    if args.titulo_es: r["titulo_es"] = args.titulo_es
    if args.anio:      r["anio"] = args.anio
    if args.pais:      r["pais"] = args.pais
    if args.director:  r["director"] = args.director
    if args.imdb:      r["imdb_id"] = args.imdb
    if args.nota:      r["nota_imdb"] = args.nota
    if args.notas:     r["notas"] = args.notas

    with CSV.open("w", newline="") as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    print(f"OK: {r['ruta_relativa']}")
    print(f"  titulo={r['titulo_original']} | anio={r['anio']} | pais={r['pais']} | director={r['director']}")
    print(f"  imdb={r['imdb_id']} nota={r['nota_imdb']} titulo_es={r['titulo_es']}")

if __name__ == "__main__":
    main()
