"""Verifica que el CSV descargado tenga las columnas esperadas, sin cargarlo completo.

    python scripts/inspeccionar_csv.py [ruta.csv]
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.comunes.columnas import validar  # noqa: E402
from src.comunes.config import CSV_PATH  # noqa: E402
from src.comunes.transformar import normalizar  # noqa: E402

ruta = sys.argv[1] if len(sys.argv) > 1 else CSV_PATH
muestra = pd.read_csv(ruta, nrows=20000, dtype=str)
mapa, faltantes, no_rec = validar(list(muestra.columns))

print(f"Archivo: {ruta}\nColumnas leídas: {len(muestra.columns)}")
print("\nMapeo reconocido:")
for o, c in mapa.items():
    print(f"  {o!r:45} -> {c}")
if no_rec:
    print("\nColumnas NO reconocidas (se ignoran):", no_rec)
if faltantes:
    sys.exit(f"\nERROR: faltan columnas requeridas: {faltantes}. Agrega su alias en src/comunes/columnas.py")

limpio = normalizar(muestra)
if limpio.empty:
    sys.exit("ERROR: 0 filas válidas. Revisa id_caso, fecha_notificacion y cod_municipio (scripts/diagnostico.py).")
print(f"\nFilas válidas en la muestra: {len(limpio):,} de {len(muestra):,}")
print("Fechas parseadas (no nulas):")
for c in ["fecha_notificacion", "fecha_inicio_sintomas", "fecha_diagnostico", "fecha_muerte", "fecha_recuperacion"]:
    print(f"  {c:25} {limpio[c].notna().sum():>7,}")
print("Tipos de contagio:", limpio["tipo_contagio"].value_counts(dropna=False).to_dict())
print("OK: el CSV es compatible con el pipeline.")
