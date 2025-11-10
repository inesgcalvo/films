from pathlib import Path
import csv

# ======================================================
# CONFIGURACIÓN PRINCIPAL
# ======================================================
root = Path(r"E:\VIDEOS\FILM")  # Carpeta raíz con tus películas
out_path = Path(r"E:\_code_\films\data\films_folder_report.csv")  # Archivo CSV de salida
# ======================================================

# Extensiones de vídeo reconocidas
VIDEO_EXTS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.mpg', '.mpeg', '.m4v', '.3gp', '.ts', '.rmvb'
}


def human_readable_size(nbytes: int) -> str:
    """Convierte bytes a formato legible (KB/MB/GB)."""
    if nbytes < 1024:
        return f"{nbytes} B"
    for unit in ("KB", "MB", "GB", "TB"):
        nbytes /= 1024.0
        if nbytes < 1024.0:
            return f"{nbytes:3.1f} {unit}"
    return f"{nbytes:.1f} PB"


def get_folder_size(folder: Path) -> int:
    """Calcula el tamaño total (en bytes) de una carpeta y su contenido."""
    total = 0
    for f in folder.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except OSError:
                pass
    return total


def examine_films(root: Path):
    """Explora las carpetas de películas y genera la información requerida."""
    results = []
    folders_with_subfolders = 0

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"La carpeta raíz indicada no existe o no es un directorio: {root}")

    # Iterar sobre las subcarpetas dentro de FILM
    for entry in sorted(root.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_dir():
            continue

        carpeta = entry
        archivos = [f for f in carpeta.iterdir() if f.is_file()]
        subcarpetas = [d for d in carpeta.iterdir() if d.is_dir()]

        num_docs = len(archivos)
        nombres_docs = [f.name for f in archivos]

        # --- Archivos de vídeo ---
        video_files = [f for f in archivos if f.suffix.lower() in VIDEO_EXTS]
        num_videos = len(video_files)
        nombres_videos = [f.name for f in video_files]

        # --- Verificar carpeta VIDEO_TS ---
        videots_folder = None
        for d in subcarpetas:
            if d.name.lower() == "video_ts":
                videots_folder = d
                break

        # --- Calcular tamaños y formato ---
        if videots_folder:
            formato_field = "DVD"
            total_size_bytes = get_folder_size(videots_folder)
        else:
            total_size_bytes = sum((f.stat().st_size for f in video_files if f.exists()), 0)
            formatos = sorted({f.suffix.lower().lstrip('.') for f in video_files})
            formato_field = "; ".join(formatos) if formatos else ""

        total_size_human = human_readable_size(total_size_bytes)

        # --- Subcarpetas ---
        num_subcarpetas = len(subcarpetas)

        nombres_subcarpetas = [d.name for d in subcarpetas]
        if num_subcarpetas > 0:
            folders_with_subfolders += 1

        # --- Agregar registro ---
        results.append({
            "CARPETA": carpeta.name,
            "Numero de documentos": num_docs,
            "Nombres de documentos": "; ".join(nombres_docs),
            "Número de documentos de tipo video": num_videos,
            "Nombre de documentos de tipo video": "; ".join(nombres_videos),
            "Tamaño (bytes)": total_size_bytes,
            "Tamaño (legible)": total_size_human,
            "Formato de video": formato_field,
            "Numero de subcarpetas": num_subcarpetas,
            "Nombre de subcarpetas": "; ".join(nombres_subcarpetas),
        })

    return results, folders_with_subfolders


def write_csv(rows, out_path: Path, delimiter="|"):
    """Escribe los resultados en un archivo CSV."""
    headers = [
        "CARPETA",
        "Numero de documentos",
        "Nombres de documentos",
        "Número de documentos de tipo video",
        "Nombre de documentos de tipo video",
        "Tamaño (bytes)",
        "Tamaño (legible)",
        "Formato de video",
        "Numero de subcarpetas",
        "Nombre de subcarpetas",
    ]

    # Asegurarse de que exista la carpeta destino
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)

# ======================================================
# EJECUCIÓN DIRECTA (Visual Studio / VS Code / etc.)
# ======================================================
if __name__ == "__main__":
    try:
        rows, folders_with_subfolders = examine_films(root)
        write_csv(rows, out_path)
        print(f"\n✅ CSV generado en: {out_path.resolve()}")
        print(f"📁 Carpetas con subcarpetas: {folders_with_subfolders}")
    except Exception as e:
        print("❌ Error:", e)