#!/usr/bin/env python3
"""Colapsa catalogo_v2.csv en filas por película (una fila por carpeta en
LARGOMETRAJES/DOCUMENTALES/CORTOMETRAJES; SERIES y VIDEOS_MUSICALES 1:1)."""
import os, csv
from pathlib import Path

IN  = Path("/Users/inesg.calvo/reporte_videos/catalogo_v2.csv")
OUT = Path("/Users/inesg.calvo/reporte_videos/catalogo_v2_colapsado.csv")

def to_float(s: str) -> float:
    if not s: return 0.0
    return float(s.replace(',', '.'))

def fmt(f: float, dec: int) -> str:
    return f"{f:.{dec}f}".replace('.', ',')

def main():
    with IN.open() as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)

    fieldnames = list(rows[0].keys()) + ['n_ficheros']

    groups: dict[str, list] = {}
    for r in rows:
        cat = r['categoria']
        ruta_parts = r['ruta_relativa'].split('/')
        if cat in ('LARGOMETRAJES', 'DOCUMENTALES', 'CORTOMETRAJES') and len(ruta_parts) >= 2:
            key = f"{cat}|{ruta_parts[1]}"
        else:
            key = f"{cat}|{r['ruta_relativa']}"
        groups.setdefault(key, []).append(r)

    out = []
    for key, group in groups.items():
        if len(group) == 1:
            r = dict(group[0]); r['n_ficheros'] = '1'
            out.append(r); continue

        # Elige representante = mejor calidad (alto, luego bitrate)
        def score(r):
            return (int(r['alto'] or 0), to_float(r['bitrate_Mbps']))
        rep = max(group, key=score)
        r = dict(rep)

        # Sumas
        r['duracion_min'] = fmt(sum(to_float(x['duracion_min']) for x in group), 1)
        r['tamano_GB']    = fmt(sum(to_float(x['tamano_GB'])    for x in group), 2)

        # Concatena únicos
        def uniq_join(field):
            seen, order = set(), []
            for x in group:
                v = x[field]
                if v and v not in seen:
                    seen.add(v); order.append(v)
            return '+'.join(order)
        r['extension']    = uniq_join('extension')
        r['idioma_audio'] = uniq_join('idioma_audio')

        # Ruta = carpeta si es peli/doc/corto
        cat = r['categoria']
        parts = group[0]['ruta_relativa'].split('/')
        if cat in ('LARGOMETRAJES', 'DOCUMENTALES', 'CORTOMETRAJES') and len(parts) >= 2:
            r['ruta_relativa'] = f"{cat}/{parts[1]}"

        r['n_ficheros'] = str(len(group))
        out.append(r)

    # Ordena por categoría, luego título
    out.sort(key=lambda r: (r['categoria'], r['titulo_original'].lower()))

    with OUT.open('w', newline='') as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(out)

    print(f"Entrada: {len(rows)} filas")
    print(f"Salida:  {len(out)} filas")
    print(f"Colapso: {len(rows)-len(out)} filas fusionadas")
    print(f"Escrito: {OUT}")

if __name__ == '__main__':
    main()
