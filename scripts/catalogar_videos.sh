#!/bin/bash
# Cataloga videos: parsea metadata desde el path + mediainfo (incluye idioma audio)

ROOT="/Volumes/INES_6TB/VIDEOS"
OUT_DIR="/Users/inesg.calvo/reporte_videos"
CSV="$OUT_DIR/catalogo.csv"
LOG="$OUT_DIR/progreso_cat.log"

mkdir -p "$OUT_DIR"

echo "categoria;titulo;anio;pais;director;extension;idioma_audio;contenedor;codec_video;ancho;alto;calidad;fps;bitrate_Mbps;duracion_min;tamano_GB;codec_audio;canales_audio;filmaffinity;ruta_relativa" > "$CSV"

TOTAL=$(find "$ROOT" \( -path "*/_BORRAR" -o -path "*/_DUPLICADOS" \) -prune -o -type f \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.avi" -o -iname "*.mov" -o -iname "*.wmv" -o -iname "*.flv" -o -iname "*.webm" -o -iname "*.m4v" -o -iname "*.mpg" -o -iname "*.mpeg" -o -iname "*.ts" -o -iname "*.vob" -o -iname "*.3gp" -o -iname "*.divx" \) ! -name "._*" -print 2>/dev/null | wc -l | tr -d ' ')
echo "Total archivos: $TOTAL" > "$LOG"

TEMPLATE=$'General;G:%Format%|%Duration%|%FileSize%\\n\nVideo;V:%Format%|%Width%|%Height%|%FrameRate%|%BitRate%\\n\nAudio;A:%Format%|%Channels%|%Language/String%\\n'

I=0
find "$ROOT" \( -path "*/_BORRAR" -o -path "*/_DUPLICADOS" \) -prune -o -type f \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.avi" -o -iname "*.mov" -o -iname "*.wmv" -o -iname "*.flv" -o -iname "*.webm" -o -iname "*.m4v" -o -iname "*.mpg" -o -iname "*.mpeg" -o -iname "*.ts" -o -iname "*.vob" -o -iname "*.3gp" -o -iname "*.divx" \) ! -name "._*" -print 2>/dev/null | while IFS= read -r f; do
  I=$((I+1))
  if [ $((I % 50)) -eq 0 ]; then
    echo "[$(date +%H:%M:%S)] $I / $TOTAL" >> "$LOG"
  fi

  rel="${f#$ROOT/}"
  cat=$(echo "$rel" | awk -F/ '{print $1}')
  nombre=$(basename "$f")
  ext="${nombre##*.}"
  ext=$(echo "$ext" | tr '[:upper:]' '[:lower:]')

  # Carpeta de peli/serie = segundo nivel bajo root
  carpeta=$(echo "$rel" | awk -F/ '{print $2}')

  # Parser de titulo/anio/pais/director del nombre de carpeta
  # Formatos soportados (para LARGOMETRAJES/DOCUMENTALES/CORTOMETRAJES):
  #   1) "Titulo [AAAA] PAIS Director"
  #   2) "Titulo (AAAA)"
  #   3) "Titulo - Director AAAA"
  #   4) "Titulo AAAA Director"  (mas laxo)
  titulo=""; anio=""; pais=""; director=""

  if [[ "$carpeta" =~ ^(.+)[[:space:]]+\[([12][0-9]{3})\][[:space:]]+([A-Z]{2,4})[[:space:]]+(.+)$ ]]; then
    titulo="${BASH_REMATCH[1]}"; anio="${BASH_REMATCH[2]}"; pais="${BASH_REMATCH[3]}"; director="${BASH_REMATCH[4]}"
  elif [[ "$carpeta" =~ ^(.+)[[:space:]]+\[([12][0-9]{3})\][[:space:]]+([A-Z]{2,4})[[:space:]]*$ ]]; then
    titulo="${BASH_REMATCH[1]}"; anio="${BASH_REMATCH[2]}"; pais="${BASH_REMATCH[3]}"
  elif [[ "$carpeta" =~ ^(.+)[[:space:]]+\[([12][0-9]{3})\][[:space:]]*$ ]]; then
    titulo="${BASH_REMATCH[1]}"; anio="${BASH_REMATCH[2]}"
  elif [[ "$carpeta" =~ ^(.+)[[:space:]]+\(([12][0-9]{3})\)[[:space:]]*$ ]]; then
    titulo="${BASH_REMATCH[1]}"; anio="${BASH_REMATCH[2]}"
  elif [[ "$carpeta" =~ ^(.+)[[:space:]]-[[:space:]]+(.+)[[:space:]]+([12][0-9]{3})[[:space:]]*$ ]]; then
    titulo="${BASH_REMATCH[1]}"; director="${BASH_REMATCH[2]}"; anio="${BASH_REMATCH[3]}"
  elif [[ "$carpeta" =~ ^([12][0-9]{3})[[:space:]]+(.+)$ ]]; then
    # video suelto sin carpeta con formato "AAAA Titulo (director).ext"
    anio="${BASH_REMATCH[1]}"; titulo="${BASH_REMATCH[2]}"
    titulo=$(echo "$titulo" | sed 's/\.[a-z0-9]*$//')
  else
    titulo="$carpeta"
  fi

  # Para SERIES: la carpeta de nivel 2 es la serie, sin año en el nombre
  if [ "$cat" = "SERIES" ]; then
    titulo="$carpeta"; anio=""; pais=""; director=""
  fi
  if [ "$cat" = "VIDEOS_MUSICALES" ]; then
    titulo=$(echo "$nombre" | sed 's/\.[a-z0-9]*$//')
    anio=""; pais=""; director=""
  fi

  # Metadatos con mediainfo
  info=$(mediainfo --Output="$TEMPLATE" "$f" 2>/dev/null)
  gen=$(echo "$info" | grep '^G:' | head -1 | sed 's/^G://')
  vid=$(echo "$info" | grep '^V:' | head -1 | sed 's/^V://')
  aud=$(echo "$info" | grep '^A:' | head -1 | sed 's/^A://')

  contenedor=$(echo "$gen" | cut -d'|' -f1)
  dur_ms=$(echo "$gen"    | cut -d'|' -f2)
  size_b=$(echo "$gen"    | cut -d'|' -f3)

  codec_v=$(echo "$vid" | cut -d'|' -f1)
  ancho=$(echo "$vid"   | cut -d'|' -f2)
  alto=$(echo "$vid"    | cut -d'|' -f3)
  fps=$(echo "$vid"     | cut -d'|' -f4)
  br=$(echo "$vid"      | cut -d'|' -f5)

  codec_a=$(echo "$aud"  | cut -d'|' -f1)
  canales=$(echo "$aud"  | cut -d'|' -f2)
  idioma=$(echo "$aud"   | cut -d'|' -f3)

  # Inferir idioma del nombre si no viene en metadatos
  if [ -z "$idioma" ]; then
    low=$(echo "$rel" | tr '[:upper:]' '[:lower:]')
    case "$low" in
      *vose*|*"v.o.s.e"*|*"v.o. sub"*) idioma="VOSE" ;;
      *dual*)                          idioma="Dual" ;;
      *spanish*|*castellano*|*.spa.*|*"[spa]"*) idioma="Español" ;;
      *english*|*.eng.*|*"[eng]"*)     idioma="Inglés" ;;
      *french*|*.fre.*|*.fra.*|*français*) idioma="Francés" ;;
      *german*|*.ger.*|*deutsch*)      idioma="Alemán" ;;
      *italian*|*.ita.*)               idioma="Italiano" ;;
      *japanese*|*.jpn.*)              idioma="Japonés" ;;
    esac
  fi

  calidad="?"
  if [ -n "$alto" ] && [ "$alto" -gt 0 ] 2>/dev/null; then
    if   [ "$alto" -ge 2000 ]; then calidad="4K/UHD"
    elif [ "$alto" -ge 1400 ]; then calidad="1440p (2K)"
    elif [ "$alto" -ge 1000 ]; then calidad="1080p (Full HD)"
    elif [ "$alto" -ge 700  ]; then calidad="720p (HD)"
    elif [ "$alto" -ge 550  ]; then calidad="576p (SD PAL)"
    elif [ "$alto" -ge 460  ]; then calidad="480p (SD NTSC)"
    else                            calidad="<480p (baja)"
    fi
  fi

  dur_min=""
  [ -n "$dur_ms" ] && dur_min=$(awk -v x="$dur_ms" 'BEGIN{printf "%.1f", x/60000}')
  size_gb=""
  [ -n "$size_b" ] && size_gb=$(awk -v x="$size_b" 'BEGIN{printf "%.2f", x/1073741824}')
  br_mbps=""
  [ -n "$br" ] && br_mbps=$(awk -v x="$br" 'BEGIN{printf "%.2f", x/1000000}')
  fps_out=""
  [ -n "$fps" ] && fps_out=$(awk -v x="$fps" 'BEGIN{printf "%.2f", x}')

  # Sanitiza campos que puedan tener ;
  titulo=$(echo "$titulo" | tr ';' ',')
  director=$(echo "$director" | tr ';' ',')
  rel_safe=$(echo "$rel" | tr ';' ',')

  echo "$cat;$titulo;$anio;$pais;$director;$ext;$idioma;$contenedor;$codec_v;$ancho;$alto;$calidad;$fps_out;$br_mbps;$dur_min;$size_gb;$codec_a;$canales;;$rel_safe" >> "$CSV"
done

echo "[$(date +%H:%M:%S)] TERMINADO" >> "$LOG"
