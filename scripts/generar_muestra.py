"""Genera un CSV pequeño con los encabezados reales del portal para probar el pipeline
sin los 6 millones de filas.   python scripts/generar_muestra.py 50000"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

n = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
rng = np.random.default_rng(1)
munis = [("73001", "IBAGUE", "73", "TOLIMA"), ("73449", "MELGAR", "73", "TOLIMA"),
         ("11001", "BOGOTA D.C.", "11", "BOGOTA D.C."), ("05001", "MEDELLIN", "05", "ANTIOQUIA")]
pesos = [0.3, 0.1, 0.4, 0.2]
idx = rng.choice(len(munis), n, p=pesos)
notif = pd.Series(pd.Timestamp("2020-03-06") + pd.to_timedelta(rng.integers(0, 700, n), unit="D"))
sint = notif - pd.to_timedelta(rng.integers(0, 10, n), unit="D")
diag = notif + pd.to_timedelta(rng.integers(0, 8, n), unit="D")
tipo = rng.choice(["Relacionado", "Comunitaria", "En estudio", "Importado"], n, p=[.35, .5, .1, .05])
fallece = rng.random(n) < 0.02
iso = lambda s: s.dt.strftime("%Y-%m-%dT00:00:00.000")  # noqa: E731

df = pd.DataFrame({
    "fecha reporte web": iso(notif + pd.to_timedelta(1, unit="D")),
    "ID de caso": np.arange(1, n + 1),
    "Fecha de notificación": iso(notif),
    "Código DIVIPOLA departamento": [munis[i][2] for i in idx],
    "Nombre departamento": [munis[i][3] for i in idx],
    "Código DIVIPOLA municipio": [munis[i][0] for i in idx],
    "Nombre municipio": [munis[i][1] for i in idx],
    "Edad": rng.integers(0, 95, n),
    "Unidad de medida de edad": 1,
    "Sexo": rng.choice(["F", "M"], n),
    "Tipo de contagio": tipo,
    "Ubicación del caso": "Casa",
    "Estado": np.where(fallece, "Fallecido", "Leve"),
    "Código ISO del país": "",
    "Nombre del país": "",
    "Recuperado": np.where(fallece, "Fallecido", "Recuperado"),
    "Fecha de inicio de síntomas": iso(sint),
    "Fecha de muerte": np.where(fallece, iso(diag + pd.to_timedelta(5, unit="D")), "-   -"),
    "Fecha de diagnóstico": iso(diag),
    "Fecha de recuperación": np.where(fallece, "-   -", iso(diag + pd.to_timedelta(14, unit="D"))),
    "Tipo de recuperación": np.where(fallece, "N/A", "Tiempo"),
    "Pertenencia étnica": "Otros",
    "Nombre del grupo étnico": "",
})
destino = Path(__file__).resolve().parents[1] / "data/raw/muestra.csv"
df.to_csv(destino, index=False)
print(f"{n:,} filas -> {destino}")
