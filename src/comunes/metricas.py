"""Registro de tiempos para la evidencia de escala (resultados/rendimiento.csv)."""
import csv
import time
from contextlib import contextmanager

from .config import RESULTADOS_DIR

RUTA = RESULTADOS_DIR / "rendimiento.csv"


@contextmanager
def medir(componente: str, etapa: str, filas: int | None = None):
    info = {"filas": filas}
    t0 = time.perf_counter()
    try:
        yield info
    finally:
        seg = time.perf_counter() - t0
        RUTA.parent.mkdir(parents=True, exist_ok=True)
        nuevo = not RUTA.exists()
        n = info["filas"]
        with open(RUTA, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if nuevo:
                w.writerow(["componente", "etapa", "filas", "segundos", "filas_por_seg"])
            w.writerow([componente, etapa, n or "", round(seg, 3), round(n / seg) if n and seg else ""])
        print(f"[{componente}] {etapa}: {seg:.2f}s" + (f" ({n:,} filas)" if n else ""))
