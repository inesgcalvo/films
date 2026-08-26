#!/bin/bash
# Genera resumen agregado a partir de videos.csv

CSV_RAW="/Users/inesg.calvo/reporte_videos/videos.csv"
CSV="/Users/inesg.calvo/reporte_videos/videos_filtrado.csv"
OUT="/Users/inesg.calvo/reporte_videos/RESUMEN.md"

# Filtra ficheros AppleDouble (._*) que no son videos reales
awk -F';' 'NR==1 || $3 !~ /^\._/' "$CSV_RAW" > "$CSV"
DESCARTADOS=$(($(wc -l < "$CSV_RAW") - $(wc -l < "$CSV")))

TOTAL=$(($(wc -l < "$CSV") - 1))
TAM_TOTAL=$(awk -F';' 'NR>1 {s+=$12} END {printf "%.1f", s}' "$CSV")
DUR_TOTAL_H=$(awk -F';' 'NR>1 {s+=$11} END {printf "%.1f", s/60}' "$CSV")

{
echo "# Reporte de calidad de videos"
echo ""
echo "**Ruta analizada:** \`/Volumes/INES_6TB/VIDEOS\`"
echo "**Total archivos:** $TOTAL (excluidos $DESCARTADOS ficheros basura \`._*\` de macOS)"
echo "**Tamaño total:** ${TAM_TOTAL} GB"
echo "**Duración total:** ${DUR_TOTAL_H} h"
echo ""
echo "## Distribución por calidad (resolución vertical)"
echo ""
echo "| Calidad | Archivos | % | Tamaño (GB) |"
echo "|---|---:|---:|---:|"
awk -F';' -v tot="$TOTAL" 'NR>1 {c[$8]++; g[$8]+=$12} END {
  for (k in c) printf "| %s | %d | %.1f%% | %.1f |\n", k, c[k], 100*c[k]/tot, g[k]
}' "$CSV" | sort -t'|' -k3 -nr

echo ""
echo "## Distribución por calidad × categoría"
echo ""
echo "| Categoría | 4K/UHD | 1440p | 1080p | 720p | 576p | 480p | <480p | ? | Total |"
echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
awk -F';' 'NR>1 {
  cat=$2; q=$8;
  cats[cat]=1;
  m[cat,q]++;
  t[cat]++;
}
END {
  for (c in cats) {
    printf "| %s | %d | %d | %d | %d | %d | %d | %d | %d | %d |\n", c,
      m[c,"4K/UHD"]+0, m[c,"1440p (2K)"]+0, m[c,"1080p (Full HD)"]+0,
      m[c,"720p (HD)"]+0, m[c,"576p (SD PAL)"]+0, m[c,"480p (SD NTSC)"]+0,
      m[c,"<480p (baja)"]+0, m[c,"?"]+0, t[c]
  }
}' "$CSV" | sort

echo ""
echo "## Códecs de vídeo más comunes"
echo ""
echo "| Códec | Archivos |"
echo "|---|---:|"
awk -F';' 'NR>1 {c[$5]++} END {for (k in c) printf "| %s | %d |\n", k, c[k]}' "$CSV" | sort -t'|' -k3 -nr | head -10

echo ""
echo "## Contenedores"
echo ""
echo "| Contenedor | Archivos |"
echo "|---|---:|"
awk -F';' 'NR>1 {c[$4]++} END {for (k in c) printf "| %s | %d |\n", k, c[k]}' "$CSV" | sort -t'|' -k3 -nr | head -10

echo ""
echo "## Top 10 archivos MAS pesados"
echo ""
echo "| Tamaño (GB) | Calidad | Bitrate (Mbps) | Ruta |"
echo "|---:|---|---:|---|"
awk -F';' 'NR>1 {printf "%s|%s|%s|%s\n", $12, $8, $10, $1}' "$CSV" | sort -t'|' -k1 -nr | head -10 | awk -F'|' '{printf "| %s | %s | %s | %s |\n", $1, $2, $3, $4}'

echo ""
echo "## Top 10 archivos con peor calidad (más pequeños con duración > 30 min)"
echo ""
echo "| Alto | Calidad | Duración (min) | Bitrate (Mbps) | Ruta |"
echo "|---:|---|---:|---:|---|"
awk -F';' 'NR>1 && $11>30 && $7!="" {printf "%s|%s|%s|%s|%s\n", $7, $8, $11, $10, $1}' "$CSV" | sort -t'|' -k1 -n | head -10 | awk -F'|' '{printf "| %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5}'

echo ""
echo "## Archivos sin metadatos legibles (posibles corruptos o formatos raros)"
echo ""
NOMETA=$(awk -F';' 'NR>1 && $7=="" {print}' "$CSV" | wc -l | tr -d ' ')
echo "Total: **$NOMETA** archivos sin resolución detectable."
if [ "$NOMETA" -gt 0 ] && [ "$NOMETA" -le 30 ]; then
  echo ""
  awk -F';' 'NR>1 && $7=="" {print "- `" $1 "`"}' "$CSV"
fi

echo ""
echo "---"
echo "_CSV completo: \`/Users/inesg.calvo/reporte_videos/videos.csv\` (importable en Excel/Numbers/LibreOffice con separador \`;\`)_"
} > "$OUT"

echo "Escrito: $OUT"
