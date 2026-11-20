"""Spark: indicadores epidemiológicos + simulación de propagación sobre el grafo de contactos.

Entradas : silver (parquet) y data/grafo/aristas_contacto.csv (generar_contactos.py)
Salidas  : gold/* (parquet, 1 archivo cada uno) listos para cargar en Snowflake

  indicadores_depto_mes : casos, fallecidos y tiempos de detección por departamento y mes
  tiempo_deteccion      : p50/p90/promedio de días síntomas->diagnóstico por departamento
  r_efectivo_mes        : contagios generados por caso (R efectivo aproximado) por mes
  alcance_por_origen    : casos alcanzados por generación desde cada super-propagador (BFS)
  nodos_criticos        : super-propagadores con hijos directos y alcance total

    python -m src.procesamiento.analisis_propagacion [--semillas 500] [--max-gen 8]
"""
import argparse

from pyspark.sql import SparkSession, functions as F

from src.comunes.config import GOLD_DIR, GRAFO_DIR, SILVER_DIR
from src.comunes.metricas import medir


def guardar(df, nombre):
    with medir("spark", f"gold_{nombre}") as m:
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(str(GOLD_DIR / nombre))
        m["filas"] = df.count()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--semillas", type=int, default=500, help="top-N nodos por hijos directos")
    ap.add_argument("--max-gen", type=int, default=8)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("analisis_propagacion")
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.driver.memory", "4g")
             .getOrCreate())

    casos = spark.read.parquet(str(SILVER_DIR))
    casos.cache()

    # ---- 1. Indicadores por departamento y mes -------------------------------------------
    es_fallecido = (F.col("fecha_muerte").isNotNull() | (F.col("estado") == "fallecido")).cast("int")
    indic = (casos.groupBy("periodo", "cod_departamento", "nombre_departamento")
             .agg(F.count("*").alias("casos"), F.sum(es_fallecido).alias("fallecidos"),
                  F.round(F.avg("dias_sintomas_diag"), 2).alias("dias_sint_diag_prom"),
                  F.round(F.avg("dias_notif_diag"), 2).alias("dias_notif_diag_prom")))
    guardar(indic, "indicadores_depto_mes")

    deteccion = (casos.where(F.col("dias_sintomas_diag").isNotNull())
                 .groupBy("cod_departamento", "nombre_departamento")
                 .agg(F.count("*").alias("casos_con_dato"),
                      F.expr("percentile_approx(dias_sintomas_diag, 0.5)").alias("p50_dias"),
                      F.expr("percentile_approx(dias_sintomas_diag, 0.9)").alias("p90_dias"),
                      F.round(F.avg("dias_sintomas_diag"), 2).alias("promedio_dias")))
    guardar(deteccion, "tiempo_deteccion")

    # ---- 2. Propagación sobre el grafo -------------------------------------------------
    aristas = (spark.read.option("header", True).csv(str(GRAFO_DIR / "aristas_contacto.csv"))
               .select(F.col("origen_id").cast("long").alias("origen"), F.col("destino_id").cast("long").alias("destino")))
    aristas.cache()

    hijos = aristas.groupBy("origen").agg(F.count("*").alias("hijos"))
    nodos = casos.select("id_caso", "periodo").join(hijos, casos.id_caso == hijos.origen, "left").fillna({"hijos": 0})
    r_mes = (nodos.groupBy("periodo")
             .agg(F.count("*").alias("casos"), F.sum("hijos").alias("contagios_generados"))
             .withColumn("r_efectivo", F.round(F.col("contagios_generados") / F.col("casos"), 4)))
    guardar(r_mes, "r_efectivo_mes")

    # BFS por generaciones desde los N mayores propagadores. Cada caso tiene un solo
    # contagiador (bosque) -> no hay ciclos y no hace falta deduplicar.
    semillas = hijos.orderBy(F.desc("hijos")).limit(args.semillas).select(F.col("origen").alias("semilla"), "hijos")
    frontera = semillas.select("semilla", F.col("semilla").alias("nodo"), F.lit(0).alias("generacion"))
    niveles = [frontera.groupBy("semilla", "generacion").agg(F.count("*").alias("casos"))]
    for gen in range(1, args.max_gen + 1):
        frontera = (frontera.join(aristas, frontera.nodo == aristas.origen)
                    .select("semilla", F.col("destino").alias("nodo"), F.lit(gen).alias("generacion")))
        frontera = frontera.localCheckpoint()  # corta el linaje en cada nivel
        nivel = frontera.groupBy("semilla", "generacion").agg(F.count("*").alias("casos"))
        niveles.append(nivel)
    alcance = niveles[0]
    for n in niveles[1:]:
        alcance = alcance.unionByName(n)
    alcance = alcance.where(F.col("casos") > 0)
    guardar(alcance, "alcance_por_origen")

    criticos = (alcance.where(F.col("generacion") > 0).groupBy("semilla")
                .agg(F.sum("casos").alias("alcance_total"), F.max("generacion").alias("max_generacion"))
                .join(semillas, "semilla").select(F.col("semilla").alias("id_caso"), "hijos", "alcance_total", "max_generacion")
                .orderBy(F.desc("alcance_total")))
    guardar(criticos, "nodos_criticos")

    print("\nTop 10 nodos críticos:")
    criticos.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
