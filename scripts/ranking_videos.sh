#!/bin/bash
# Genera un ranking de calidad: tiers ordenados, categorias ordenadas,
# y un CSV con TODOS los videos ordenados de mejor a peor calidad.

CSV="/Users/inesg.calvo/reporte_videos/videos_filtrado.csv"
OUT_MD="/Users/inesg.calvo/reporte_videos/RANKING.md"
OUT_CSV="/Users/inesg.calvo/reporte_videos/videos_ordenados.csv"

# Puntuacion de calidad = altura*1000 + bitrate_kbps (asi la resolucion manda
# y el bitrate desempata dentro de la misma resolucion)
# Coma decimal española -> punto para awk
awk -F';' 'NR==1 {print "puntuacion;" $0; next}
  {
    alto=$7+0
    br=$10; gsub(",",".",br); brnum=br+0
    score = alto*1000 + brnum*100
    printf "%.0f;%s\n", score, $0
  }' "$CSV" | (read header; echo "$header"; sort -t';' -k1 -nr) > "$OUT_CSV"

{
echo "# Ranking de calidad de la videoteca"
echo ""
echo "_Criterio: **resolución vertical** como eje principal, **bitrate** como desempate._"
echo ""

echo "## 1) Tiers de calidad (mejor → peor)"
echo ""
echo "| # | Tier | Archivos | Tamaño (GB) | Bitrate medio (Mbps) |"
echo "|---:|---|---:|---:|---:|"
awk -F';' 'NR>1 {
  q=$8;
  # asigna orden numerico a cada tier
  if      (q=="4K/UHD")           r=1
  else if (q=="1440p (2K)")       r=2
  else if (q=="1080p (Full HD)")  r=3
  else if (q=="720p (HD)")        r=4
  else if (q=="576p (SD PAL)")    r=5
  else if (q=="480p (SD NTSC)")   r=6
  else if (q=="<480p (baja)")     r=7
  else                            r=9
  c[r]++; g[r]+=$12
  br=$10; gsub(",",".",br); s[r]+=br+0; n[r]++
  name[r]=q
}
END {
  for (i=1; i<=9; i++) if (c[i]>0)
    printf "| %d | %s | %d | %.1f | %.2f |\n", i, name[i], c[i], g[i], (n[i]?s[i]/n[i]:0)
}' "$CSV" | sort -t'|' -k2 -n

echo ""
echo "## 2) Categorías ordenadas por calidad media"
echo ""
echo "_Puntuación = altura media ponderada por archivo._"
echo ""
echo "| # | Categoría | Altura media (px) | % Full HD | % HD | % SD o peor | Archivos |"
echo "|---:|---|---:|---:|---:|---:|---:|"
awk -F';' 'NR>1 && $7>0 {
  c=$2; a=$7
  cnt[c]++; sum[c]+=a
  if (a>=1000) fhd[c]++
  else if (a>=700) hd[c]++
  else sd[c]++
}
END {
  for (k in cnt) {
    med=sum[k]/cnt[k]
    printf "%d|%s|%d|%.1f|%.1f|%.1f|%d\n",
      med, k, med, 100*fhd[k]/cnt[k], 100*hd[k]/cnt[k], 100*sd[k]/cnt[k], cnt[k]
  }
}' "$CSV" | sort -t'|' -k1 -nr | awk -F'|' 'BEGIN{i=0} {i++; printf "| %d | %s | %s | %s%% | %s%% | %s%% | %s |\n", i, $2, $3, $4, $5, $6, $7}'

echo ""
echo "## 3) Top 20 mejor calidad"
echo ""
echo "| # | Calidad | Alto | Bitrate (Mbps) | Tamaño (GB) | Ruta |"
echo "|---:|---|---:|---:|---:|---|"
awk -F';' 'NR>1 {printf "| %s | %s | %s | %s | %s |\n", $9, $8, $11, $13, $2}' "$OUT_CSV" \
  | head -20 | awk 'BEGIN{i=0} {i++; sub(/\| /, "| " i " | ")}1'

echo ""
echo "## 4) Bottom 20 peor calidad (con duración > 30 min)"
echo ""
echo "| # | Calidad | Alto | Bitrate (Mbps) | Tamaño (GB) | Ruta |"
echo "|---:|---|---:|---:|---:|---|"
awk -F';' 'NR>1 && $12>30 && $8>0 {printf "| %s | %s | %s | %s | %s |\n", $9, $8, $11, $13, $2}' "$OUT_CSV" \
  | tail -20 | awk 'BEGIN{i=0} {i++; sub(/\| /, "| " i " | ")}1'

echo ""
echo "---"
echo "_Ranking completo: \`/Users/inesg.calvo/reporte_videos/videos_ordenados.csv\` (columna \`puntuacion\` = altura·1000 + bitrate_kbps)_"
} > "$OUT_MD"

echo "Escrito: $OUT_MD"
echo "Escrito: $OUT_CSV"
