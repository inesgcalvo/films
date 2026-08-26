#!/bin/bash
# Simula qué se borraría con cada umbral de calidad, sin borrar nada.

CSV="/Users/inesg.calvo/reporte_videos/videos_filtrado.csv"
OUT="/Users/inesg.calvo/reporte_videos/UMBRAL_SIMULACION.md"

{
echo "# Simulación de umbrales de calidad"
echo ""
echo "_Nada se ha borrado. Esto es solo una previsualización de qué se eliminaría con cada regla._"
echo ""
echo "**Total actual:** 1 453 archivos · 1 101 GB"
echo ""

echo "## Opción A — Umbral por RESOLUCIÓN (altura mínima)"
echo ""
echo "| Umbral | Se borran | GB liberados | Se quedan | Categorías más afectadas |"
echo "|---|---:|---:|---:|---|"
for MIN in 240 360 480 576 720 1080; do
  awk -F';' -v min=$MIN '
    NR>1 && $7+0>0 && $7+0<min {
      del++; gb+=$12
      cat[$2]++
    }
    NR>1 && ($7+0>=min || $7+0==0) { keep++ }
    END {
      afect=""
      for (c in cat) {
        if (afect) afect=afect", "
        afect=afect c":"cat[c]
      }
      printf "| ≥ %dp | %d | %.0f | %d | %s |\n", min, del, gb, keep, afect
    }' "$CSV"
done

echo ""
echo "## Opción B — Umbral COMBINADO (resolución **Y** bitrate mínimo)"
echo ""
echo "_Elimina también los archivos con bitrate tan bajo que la imagen es basura aunque el nombre diga alta resolución._"
echo ""
echo "| Regla | Se borran | GB liberados | Se quedan |"
echo "|---|---:|---:|---:|"
awk -F';' '
  function tonum(s){ gsub(",",".",s); return s+0 }
  NR>1 { total++
    alto=$7+0; br=tonum($10); size=tonum($12)
    total_gb+=size
    # Regla 1: alto>=480 y bitrate>=1 Mbps
    if (alto>=480 && br>=1) { r1_keep++; r1_gb+=size } else { r1_del++; r1_delgb+=size }
    # Regla 2: alto>=576 y bitrate>=1.5
    if (alto>=576 && br>=1.5) { r2_keep++; r2_gb+=size } else { r2_del++; r2_delgb+=size }
    # Regla 3: alto>=720 y bitrate>=2
    if (alto>=720 && br>=2) { r3_keep++; r3_gb+=size } else { r3_del++; r3_delgb+=size }
    # Regla 4: alto>=720 y bitrate>=3
    if (alto>=720 && br>=3) { r4_keep++; r4_gb+=size } else { r4_del++; r4_delgb+=size }
    # Regla 5: alto>=1080 y bitrate>=4
    if (alto>=1080 && br>=4) { r5_keep++; r5_gb+=size } else { r5_del++; r5_delgb+=size }
  }
  END {
    printf "| ≥ 480p Y ≥ 1 Mbps | %d | %.0f | %d |\n", r1_del, r1_delgb, r1_keep
    printf "| ≥ 576p Y ≥ 1,5 Mbps | %d | %.0f | %d |\n", r2_del, r2_delgb, r2_keep
    printf "| ≥ 720p Y ≥ 2 Mbps | %d | %.0f | %d |\n", r3_del, r3_delgb, r3_keep
    printf "| ≥ 720p Y ≥ 3 Mbps | %d | %.0f | %d |\n", r4_del, r4_delgb, r4_keep
    printf "| ≥ 1080p Y ≥ 4 Mbps | %d | %.0f | %d |\n", r5_del, r5_delgb, r5_keep
  }' "$CSV"

echo ""
echo "## Contenido irremplazable con calidad baja (aviso)"
echo ""
echo "Algunas películas históricas **solo existen** en resolución baja porque son cine mudo o pre-restauraciones digitales. Borrar por umbral \"a ciegas\" te haría perderlas."
echo ""
echo "**Ejemplos en tu colección con altura < 480 pero anteriores a 1960:**"
echo ""
awk -F';' 'NR>1 && $7+0>0 && $7+0<480 { print $1 }' "$CSV" \
  | grep -Ei '\[19[0-5][0-9]\]' | head -15 | sed 's/^/- /'

echo ""
echo "## Series con calidad mezclada dentro de misma temporada"
echo ""
echo "Al borrar por umbral pueden desaparecer episodios sueltos, dejando temporadas incompletas. Series con calidad mixta:"
echo ""
awk -F';' 'NR>1 && $2=="SERIES" && $7+0>0 {
  split($1, p, "/"); serie=p[2]
  alto=$7+0
  if (alto<480) baja[serie]++
  else          alta[serie]++
}
END {
  for (s in baja) if (alta[s]>0)
    printf "- %s (baja: %d, alta: %d)\n", s, baja[s], alta[s]
}' "$CSV" | head -15

echo ""
echo "---"
echo "Cuando decidas el umbral, dime cuál y te preparo el borrado (con lista previa para revisar antes de ejecutar)."
} > "$OUT"

echo "Escrito: $OUT"
