#!/usr/bin/env python3
"""Fase A: arregla X-Men (2000).
Fase B: para cada archivo suelto en LARGOMETRAJES/DOCUMENTALES/CORTOMETRAJES,
crea una carpeta con nombre estándar y mueve el archivo dentro. Actualiza CSV."""
import os, csv, re, shutil, unicodedata
from pathlib import Path

CSV = Path("/Users/inesg.calvo/reporte_videos/catalogo_v3_omdb.csv")
ROOT = Path("/Volumes/INES_6TB/VIDEOS")
TARGET_CATS = ("LARGOMETRAJES", "DOCUMENTALES", "CORTOMETRAJES")

PAIS_3L_KNOWN = {"USA","GBR","FRA","ESP","DEU","ITA","JPN","CAN","AUS","SWE","DNK","NLD",
    "BEL","RUS","CHN","KOR","BRA","MEX","ARG","IND","POL","IRL","NOR","FIN","AUT","CHE",
    "PRT","GRC","IRN","TUR","HUN","CZE","ROU","NZL","HKG","TWN","THA","ISR","SUN","YUG",
    "PHL","PER","CHL","COL","EGY","UKR","ZAF"}

def safe(s: str) -> str:
    s = s.replace('/', ' - ').replace(':', ' -').replace('?', '')
    # Quita extensiones de video pegadas por si acaso
    s = re.sub(r'\.(avi|mkv|mp4|mpg|mpeg|mov|wmv|flv|m4v|vob)$', '', s, flags=re.I)
    s = re.sub(r'\s{2,}', ' ', s).strip(' .-')
    return s

def build_dirname(r):
    t = safe(r['titulo_original'])
    a = r['anio'].strip()
    p = r['pais'].strip().upper()
    if p not in PAIS_3L_KNOWN: p = ''
    d = safe(r['director'])
    partes = [t]
    if a: partes.append(f"[{a}]")
    if p: partes.append(p)
    if d: partes.append(d)
    return ' '.join(partes)

def main():
    with CSV.open() as f:
        reader = csv.DictReader(f, delimiter=';'); rows = list(reader); fields = reader.fieldnames

    # ---- FASE A: X-Men 2000 ----
    xmen_fixed = 0
    for r in rows:
        if r['ruta_relativa'].startswith('LARGOMETRAJES/X [2000]/') or r['ruta_relativa'] == 'LARGOMETRAJES/X [2000]':
            r['titulo_original'] = 'X-Men'
            r['titulo_es'] = 'X-Men'
            r['anio'] = '2000'; r['pais'] = 'USA'; r['director'] = 'Bryan Singer'
            r['imdb_id'] = 'tt0120903'; r['nota_imdb'] = '7.4'
            r['notas'] = ''
            new_dir = 'X-Men [2000] USA Bryan Singer'
            r['ruta_relativa'] = r['ruta_relativa'].replace('X [2000]', new_dir)
            xmen_fixed += 1

    # También X-Men 2 (falta director)
    for r in rows:
        if 'X-Men 2 [2003]' in r['ruta_relativa']:
            r['director'] = 'Bryan Singer'; r['pais'] = 'USA'
            r['imdb_id'] = 'tt0290334'; r['nota_imdb'] = '7.4'
            r['notas'] = ''
            new_dir = 'X-Men 2 [2003] USA Bryan Singer'
            r['ruta_relativa'] = r['ruta_relativa'].replace('X-Men 2 [2003]', new_dir)

    # Renombrar carpetas reales
    xmen_dir_old = ROOT / 'LARGOMETRAJES' / 'X [2000]'
    xmen_dir_new = ROOT / 'LARGOMETRAJES' / 'X-Men [2000] USA Bryan Singer'
    if xmen_dir_old.exists() and not xmen_dir_new.exists():
        xmen_dir_old.rename(xmen_dir_new); print(f'Fixed X-Men (2000)')

    x2_old = ROOT / 'LARGOMETRAJES' / 'X-Men 2 [2003]'
    x2_new = ROOT / 'LARGOMETRAJES' / 'X-Men 2 [2003] USA Bryan Singer'
    if x2_old.exists() and not x2_new.exists():
        x2_old.rename(x2_new); print(f'Fixed X-Men 2')

    # ---- FASE B: mover ficheros sueltos ----
    moved = 0; skipped = 0; garbage = 0
    for r in rows:
        if r['categoria'] not in TARGET_CATS: continue
        rel = Path(r['ruta_relativa'])
        if len(rel.parts) != 2: continue  # ya está en carpeta

        f_path = ROOT / rel
        if not f_path.exists() or not f_path.is_file():
            skipped += 1; continue

        # Basura macOS ._*
        if f_path.name.startswith('._'):
            trash = ROOT / '_BORRAR' / rel.parts[0] / f_path.name
            trash.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(f_path), str(trash))
            garbage += 1; continue

        # Nombre de carpeta: usa titulo/año si hay; si no, limpieza mínima del nombre del archivo
        if r['titulo_original']:
            dirname = build_dirname(r)
        else:
            dirname = safe(f_path.stem)

        # Salvaguarda: si la "carpeta" acaba igual que el fichero, algo va mal
        if dirname == f_path.name or dirname + f_path.suffix == f_path.name:
            print(f'  SKIP colisión nombre carpeta=archivo: {rel}'); skipped += 1; continue

        dest_dir = f_path.parent / dirname
        dest_file = dest_dir / f_path.name

        if dest_dir.exists() and dest_dir.is_dir():
            # Si ya existe la carpeta, meter el archivo dentro
            if not (dest_dir / f_path.name).exists():
                shutil.move(str(f_path), str(dest_dir / f_path.name))
                r['ruta_relativa'] = f"{rel.parts[0]}/{dirname}/{f_path.name}"
                moved += 1
            else:
                skipped += 1
        else:
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(f_path), str(dest_file))
            r['ruta_relativa'] = f"{rel.parts[0]}/{dirname}/{f_path.name}"
            moved += 1
            print(f'  {rel.parts[0]}/{f_path.name}  ->  {dirname}/')

    with CSV.open('w', newline='') as f:
        w = csv.DictWriter(f, delimiter=';', fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(rows)

    print(f"\nSueltos movidos a carpeta: {moved}")
    print(f"Basura macOS movida a _BORRAR: {garbage}")
    print(f"Skipped: {skipped}")

if __name__ == '__main__':
    main()
