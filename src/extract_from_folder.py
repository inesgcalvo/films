from pathlib import Path
import csv
import sys

# --------------- Ruta Modificable Aquí --------------
root = Path(r"E:\VIDEOS\FILM")
out_path = Path("..\data\film_folder_report.csv")
# ----------------------------------------------------

VIDEO_EXTS = {
    '.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm',
    '.mpg', '.mpeg', '.m4v', '.3gp', '.ts', '.rmvb'
}


def human_readable_size(nbytes: int) -> str:
    if nbytes < 1024:
        return f"{nbytes} B"
    for unit in ("KB", "MB", "GB", "TB"):
        nbytes /= 1024.0
        if nbytes < 1024.0:
            return f"{nbytes:3.1f} {unit}"
    return f"{nbytes:.1f} PB"


def get_folder_size(folder: Path) -> int:
    """Suma recursivamente el tamaño de todos los archivos dentro de una carpeta."""
    total = 0
    for f in folder.rglob("*"):
        if f.is_file():
            try:
                total += f.stat().st_size
            except OSError:
                pass
    return total


def examine_films(root: Path):
    results = []
    folders_with_subfolders = 0

    if not root.exists() or not root.is_dir():
        raise FileNotFoundError(f"La carpeta raíz indicada no existe o no es un directorio: {root}")

    for entry in sorted(root.iterdir(), key=lambda p: p.name.lower()):
        if not entry.is_dir():
            continue

        carpeta = entry
        archivos = [f for f in carpeta.iterdir() if f.is_file()]
        subcarpetas = [d for d in carpeta.iterdir() if d.is_dir()]

        num_docs = len(archivos)
        nombres_docs = [f.name for f in archivos]

        # Detectar archivos de vídeo por extensión
        video_files = [f for f in archivos if f.suffix.lower() in VIDEO_EXTS]
        num_videos = len(video_files)
        nombres_videos = [f.name for f in video_files]

        # Tamaños (bytes y legible)
        tamaños_bytes = []
        tamaños_legibles = []
        formatos = []
        for f in video_files:
            try:
                size = f.stat().st_size
            except OSError:
                size = 0
            tamaños_bytes.append(str(size))
            tamaños_legibles.append(human_readable_size(size))
            formatos.append(f.suffix.lower().lstrip('.'))

        # Detectar carpeta VIDEO_TS y calcular su tamaño total
        has_videots = None
        for d in subcarpetas:
            if d.name.lower() == "video_ts":
                has_videots = d
                break

        tam_videots_bytes = ""
        tam_videots_legible = ""
        if has_videots:
            formato_field = "DVD"
            size_videots = get_folder_size(has_videots)
            tam_videots_bytes = str(size_videots)
            tam_videots_legible = human_readable_size(size_videots)
        else:
            # Unir formatos únicos en orden alfabético
            unique_formats = sorted(set(formatos))
            formato_field = "; ".join(unique_formats) if unique_formats else ""

        num_subcarpetas = len(subcarpetas)
        nombres_subcarpetas = [d.name for d in subcarpetas]

        if num_subcarpetas > 0:
            folders_with_subfolders += 1

        results.append({
            "CARPETA": carpeta.name,
            "Numero de documentos": num_docs,
            "Nombres de documentos": "; ".join(nombres_docs),
            "Número de documentos de tipo video": num_videos,
            "Nombre de documentos de tipo video": "; ".join(nombres_videos),
            "Tamaño (bytes)": "; ".join(tamaños_bytes),
            "Tamaño (legible)": "; ".join(tamaños_legibles),
            "Formato de video": formato_field,
            "Tamaño VIDEO_TS (bytes)": tam_videots_bytes,
            "Tamaño VIDEO_TS (legible)": tam_videots_legible,
            "Numero de subcarpetas": num_subcarpetas,
            "Nombre de subcarpetas": "; ".join(nombres_subcarpetas),
        })

    return results, folders_with_subfolders


def write_csv(rows, out_path: Path, delimiter="|"):
    headers = [
        "CARPETA",
        "Numero de documentos",
        "Nombres de documentos",
        "Número de documentos de tipo video",
        "Nombre de documentos de tipo video",
        "Tamaño (bytes)",
        "Tamaño (legible)",
        "Formato de video",
        "Tamaño VIDEO_TS (bytes)",
        "Tamaño VIDEO_TS (legible)",
        "Numero de subcarpetas",
        "Nombre de subcarpetas",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    try:
        rows, folders_with_subfolders = examine_films(root)
        write_csv(rows, out_path)
        print(f"\n✅ CSV generado en: {out_path.resolve()}")
        print(f"📁 Carpetas con subcarpetas: {folders_with_subfolders}")
    except Exception as e:
        print("❌ Error:", e)