#!/bin/bash
# Regeneración segura: reescanea disco + preserva las correcciones manuales
# de IMDb/título ES en catalogo_v3_omdb.csv
set -e
cd /Users/inesg.calvo

echo "[1/5] Guardando backup del CSV actual con correcciones..."
cp reporte_videos/catalogo_v3_omdb.csv reporte_videos/catalogo_v3_omdb.BACKUP.csv

echo "[2/5] Catalogando videos desde disco (~6 min)..."
./catalogar_videos.sh

echo "[3/5] Parseando titulos limpios..."
python3 parsear_titulos.py

echo "[4/5] Colapsando por pelicula y fusionando con backup..."
python3 colapsar_v2.py
python3 fusionar_omdb.py

echo "[5/5] Generando recomendaciones..."
cp reporte_videos/catalogo_v3_omdb.csv reporte_videos/catalogo_v3_omdb.tmp
python3 recomendar.py

echo "Listo. Ficheros en /Users/inesg.calvo/reporte_videos/"
