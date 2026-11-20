"""Genera la red de contactos (aristas CONTAGIO_A) para el subconjunto del grafo.

IMPORTANTE: el dataset del INS NO trae quién contagió a quién. Estas aristas son
SINTÉTICAS y reproducibles (semilla fija); los casos, municipios y fechas sí son reales.
Reglas (documentadas en el informe):
  * Cada caso puede tener a lo sumo UN contagiador (el grafo resultante es un bosque).
  * El contagiador es un caso del MISMO municipio notificado en los VENTANA_DIAS previos.
  * Probabilidad de tener contagiador según tipo de contagio:
    relacionado 0.90 | comunitaria 0.25 | en estudio 0.10 | importado 0.00
  * FRAC_HUBS de los casos son "super-propagadores"; con prob. P_HUB el contagiador
    se elige entre ellos (genera la cola pesada típica de redes de contagio).

Entrada: parquet silver (salida de Spark).  Salida: data/grafo/{nodos_caso,aristas_contacto}.csv

    python -m src.grafo.generar_contactos
"""
import json

import numpy as np
import pandas as pd

from src.comunes.config import DEPARTAMENTOS_GRAFO, GRAFO_DIR, SEMILLA, SILVER_DIR
from src.comunes.metricas import medir

VENTANA_DIAS = 14
P_TIPO = {"relacionado": 0.90, "comunitaria": 0.25, "en estudio": 0.10, "importado": 0.0}
P_DEFECTO = 0.10
FRAC_HUBS = 0.01
P_HUB = 0.35

COLS_NODO = ["id_caso", "fecha_notificacion", "edad", "sexo", "estado", "tipo_contagio",
             "cod_municipio", "nombre_municipio", "cod_departamento", "nombre_departamento"]


def generar_aristas(df: pd.DataFrame, semilla: int = SEMILLA) -> pd.DataFrame:
    """df requiere: id_caso, cod_municipio, fecha_notificacion, tipo_contagio."""
    rng = np.random.default_rng(semilla)
    aristas = []
    for _, g in df.sort_values(["cod_municipio", "fecha_notificacion", "id_caso"]).groupby("cod_municipio", sort=True):
        n = len(g)
        if n < 2:
            continue
        ids = g["id_caso"].to_numpy(dtype="int64")
        dias = g["fecha_notificacion"].to_numpy(dtype="datetime64[D]").astype("int64")
        probs = g["tipo_contagio"].map(P_TIPO).fillna(P_DEFECTO).to_numpy(dtype="float64")
        lo = np.searchsorted(dias, dias - VENTANA_DIAS, side="left")
        r_tiene, r_hub, r_pos = rng.random(n), rng.random(n), rng.random(n)
        hubs = np.flatnonzero(rng.random(n) < FRAC_HUBS)
        for i in range(1, n):
            if lo[i] >= i or r_tiene[i] >= probs[i]:
                continue
            src = None
            if r_hub[i] < P_HUB:
                a, b = np.searchsorted(hubs, lo[i], "left"), np.searchsorted(hubs, i, "left")
                if b > a:
                    src = hubs[a + int(r_pos[i] * (b - a))]
            if src is None:
                src = lo[i] + int(r_pos[i] * (i - lo[i]))
            aristas.append((ids[src], ids[i], int(dias[i] - dias[src])))
    return pd.DataFrame(aristas, columns=["origen_id", "destino_id", "dias"])


def main():
    GRAFO_DIR.mkdir(parents=True, exist_ok=True)
    with medir("grafo", "leer_silver") as m:
        df = pd.read_parquet(SILVER_DIR, columns=COLS_NODO)
        if DEPARTAMENTOS_GRAFO:
            df = df[df["nombre_departamento"].isin(DEPARTAMENTOS_GRAFO)]
        df["fecha_notificacion"] = pd.to_datetime(df["fecha_notificacion"])
        m["filas"] = len(df)
    if df.empty:
        raise SystemExit(f"Sin casos para {DEPARTAMENTOS_GRAFO}. Revisa DEPARTAMENTOS_GRAFO en .env")

    with medir("grafo", "generar_aristas", len(df)):
        aristas = generar_aristas(df)

    df["fecha_notificacion"] = df["fecha_notificacion"].dt.strftime("%Y-%m-%d")
    df.to_csv(GRAFO_DIR / "nodos_caso.csv", index=False)
    aristas.to_csv(GRAFO_DIR / "aristas_contacto.csv", index=False)
    (GRAFO_DIR / "metadatos_generador.json").write_text(json.dumps({
        "semilla": SEMILLA, "ventana_dias": VENTANA_DIAS, "p_tipo": P_TIPO, "frac_hubs": FRAC_HUBS,
        "p_hub": P_HUB, "departamentos": DEPARTAMENTOS_GRAFO, "nodos": len(df), "aristas": len(aristas),
        "nota": "Aristas sintéticas: el dataset no contiene relaciones de contagio."}, indent=2, ensure_ascii=False))
    print(f"Nodos: {len(df):,} | Aristas: {len(aristas):,} -> {GRAFO_DIR}")


if __name__ == "__main__":
    main()
