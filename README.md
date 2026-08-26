# FILMS

Herramientas para catalogar, evaluar la calidad y organizar una videoteca personal masiva (LARGOMETRAJES / DOCUMENTALES / CORTOMETRAJES / SERIES / VIDEOS_MUSICALES).

## Objetivo

Partiendo de una carpeta con miles de archivos de vídeo:

1. Extrae metadatos técnicos con `mediainfo` (resolución, códec, bitrate, duración, audio).
2. Parsea títulos, año, país y director desde nombres de carpeta y archivo.
3. Consulta OMDb (IMDb) para enriquecer con nota, ID y título original correcto.
4. Detecta duplicados y clasifica cada película como KEEP / DELETE / REVIEW combinando **nota × resolución × antigüedad**.
5. Renombra todo al patrón estándar `Título [AAAA] PAI Director`.

## Estructura

```
films/
├── scripts/          # Scripts .py y .sh del pipeline
├── data/             # CSVs generados (catálogo, recomendaciones)
│   ├── logs/         # Logs de las ejecuciones
│   └── cache/        # Cache local de respuestas OMDb
├── reports/          # Reportes en Markdown
├── filmaffinity_scraper/  # Scraper original (WIP, bloqueado por Cloudflare)
├── src/              # Utilidades comunes
├── notebooks/        # Análisis exploratorio en Jupyter
├── .env.example      # Copiar a `.env` y rellenar
└── requirements.txt
```

## Requisitos

```bash
brew install mediainfo
pip install -r requirements.txt
cp .env.example .env
# Editar .env: añadir OMDB_API_KEY (gratis en https://www.omdbapi.com/apikey.aspx)
```

## Pipeline completo

```bash
cd scripts
./regenerar_v2.sh        # Corre todos los pasos en orden
```

## Pasos individuales

| Script | Qué hace |
|---|---|
| `catalogar_videos.sh` | Escanea el disco con `mediainfo` → `catalogo.csv` |
| `parsear_titulos.py` | Limpia scene tags y extrae título/año/país/director → `catalogo_v2.csv` |
| `colapsar_v2.py` | Colapsa DVDs (VIDEO_TS) y CD1/CD2 en una fila por peli → `catalogo_v2_colapsado.csv` |
| `omdb_lookup.py` | Consulta OMDb → añade `imdb_id`, `nota_imdb`, `titulo_original` |
| `omdb_refinar.py` | 2º pase: detecta false positives por similitud y reintenta miss con `search` endpoint |
| `rellenar_anios.py` | Completa año/país/director desde OMDb usando `imdb_id` |
| `recomendar.py` | Clasifica cada peli como KEEP/DELETE/REVIEW → `recomendaciones.csv` |
| `mover_a_borrar.py` | Mueve los DELETE a `_BORRAR/` (reversible) |
| `organizar.py` | Detecta duplicados y renombra al patrón estándar |
| `mover_a_carpetas.py` | Mete archivos sueltos dentro de carpetas propias |

## Reglas de clasificación

- **KEEP**: nota ≥ 7.0 · o clásico pre-1970 con nota ≥ 6.0 · o nota ≥ 6.0 en Full HD
- **DELETE**: nota < 5.0 con calidad < 700p · o nota < 6.0 con calidad < 480p
- **REVIEW**: todo lo demás (incluye sin nota IMDb)
- Los clásicos irremplazables (pre-1970) se protegen de borrado.
- SERIES se juzgan solo por calidad (no consultamos IMDb por episodio).

## Patrón de nombrado

Cada carpeta de película:

```
Título original [AAAA] PAÍS Director
```

Donde `PAÍS` es el código ISO 3166-1 alpha-3 (USA, GBR, FRA, ESP, DEU, ITA, JPN…).

## Notas técnicas

- `filmaffinity.com` está detrás de Cloudflare y bloquea scraping HTTP directo → usamos OMDb (IMDb) como fuente de rating.
- OMDb tiene límite de 1000 requests/día en tier gratuito; el cache local evita repetir consultas.
