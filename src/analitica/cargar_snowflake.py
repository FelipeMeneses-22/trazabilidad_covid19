"""Carga las tablas gold (y opcionalmente los 6M de casos) a Snowflake vía stage interno.

Variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE,
           SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA  (y opcional SNOWFLAKE_ROLE)

    python -m src.analitica.cargar_snowflake [--completo]
"""
import argparse
import os
from pathlib import Path

import snowflake.connector

from src.comunes.config import GOLD_DIR, RAIZ, SILVER_DIR
from src.comunes.metricas import medir

TABLAS_GOLD = {
    "indicadores_depto_mes": "gold_indicadores_depto_mes",
    "tiempo_deteccion": "gold_tiempo_deteccion",
    "r_efectivo_mes": "gold_r_efectivo_mes",
    "alcance_por_origen": "gold_alcance_por_origen",
    "nodos_criticos": "gold_nodos_criticos",
}
COPY = "FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = ABORT_STATEMENT"


def conectar():
    cfg = {k.lower().replace("snowflake_", ""): v for k, v in os.environ.items() if k.startswith("SNOWFLAKE_")}
    return snowflake.connector.connect(**cfg)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--completo", action="store_true", help="también carga fact_casos (todo el silver)")
    args = ap.parse_args()

    con = conectar()
    cur = con.cursor()
    for stmt in (RAIZ / "sql/snowflake_ddl.sql").read_text().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    cur.execute("CREATE STAGE IF NOT EXISTS stg_covid")

    for carpeta, tabla in TABLAS_GOLD.items():
        with medir("snowflake", f"carga_{tabla}") as m:
            cur.execute(f"REMOVE @stg_covid/{carpeta}/")
            cur.execute(f"PUT file://{(GOLD_DIR / carpeta).as_posix()}/*.parquet @stg_covid/{carpeta}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            cur.execute(f"TRUNCATE TABLE {tabla}")
            cur.execute(f"COPY INTO {tabla} FROM @stg_covid/{carpeta}/ {COPY}")
            m["filas"] = cur.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]

    if args.completo:
        with medir("snowflake", "carga_fact_casos") as m:
            cur.execute("REMOVE @stg_covid/casos/")
            for part in sorted(Path(SILVER_DIR).glob("periodo=*")):
                cur.execute(f"PUT file://{part.as_posix()}/*.parquet @stg_covid/casos/{part.name}/ AUTO_COMPRESS=FALSE PARALLEL=8")
            cur.execute("TRUNCATE TABLE fact_casos")
            cur.execute(f"COPY INTO fact_casos FROM @stg_covid/casos/ {COPY}")
            m["filas"] = cur.execute("SELECT COUNT(*) FROM fact_casos").fetchone()[0]
    con.close()


if __name__ == "__main__":
    main()
