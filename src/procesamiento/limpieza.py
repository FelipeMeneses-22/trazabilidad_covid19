"""Spark: CSV crudo -> Parquet 'silver' limpio, particionado por periodo (YYYY-MM).

Mismas reglas que src/comunes/transformar.py. Lee el CSV local completo (6M filas).

    python -m src.procesamiento.limpieza [--csv ruta.csv]
"""
import argparse

from pyspark.sql import SparkSession, functions as F

from src.comunes.columnas import CANONICAS, FECHAS, REQUERIDAS, mapa_columnas
from src.comunes.config import CSV_PATH, SILVER_DIR
from src.comunes.metricas import medir

VACIOS = ["", "nan", "none", "n/a", "-", "-   -", "sin dato", "nat"]
FORMATOS_FECHA = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS",
                  "yyyy-MM-dd'T'HH:mm:ss", "MM/dd/yyyy hh:mm:ss a", "yyyy-MM-dd", "dd/MM/yyyy"]


def texto(c):
    t = F.trim(F.col(c))
    return F.when(F.lower(t).isin(VACIOS), None).otherwise(t)


def numerico(c):
    """Quita separadores de miles y espacios: '1,556,979' -> '1556979'."""
    return F.regexp_replace(F.regexp_replace(texto(c), r"[,\s]", ""), r"\.0$", "")


def fecha(c):
    return F.to_date(F.coalesce(*[F.to_timestamp(texto(c), f) for f in FORMATOS_FECHA]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=CSV_PATH)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("limpieza_covid")
             .config("spark.sql.shuffle.partitions", "64")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.driver.memory", "4g")
             .getOrCreate())

    df = spark.read.option("header", True).csv(args.csv)
    mapa = mapa_columnas(df.columns)
    faltan = [c for c in REQUERIDAS if c not in mapa.values()]
    if faltan:
        raise SystemExit(f"Faltan columnas requeridas {faltan}. Ejecuta scripts/inspeccionar_csv.py")
    df = df.select([F.col(f"`{o}`").alias(c) for o, c in mapa.items()])
    for c in CANONICAS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))

    for c in FECHAS:
        df = df.withColumn(c, fecha(c))
    df = (df.select(*CANONICAS)
          .withColumn("id_caso", numerico("id_caso").cast("long"))
          .withColumn("edad", numerico("edad").cast("int"))
          .withColumn("cod_departamento", F.lpad(numerico("cod_departamento"), 2, "0"))
          .withColumn("cod_municipio", F.lpad(numerico("cod_municipio"), 5, "0"))
          .withColumn("nombre_departamento", F.upper(texto("nombre_departamento")))
          .withColumn("nombre_municipio", F.upper(texto("nombre_municipio")))
          .withColumn("sexo", F.upper(texto("sexo"))))
    for c in ["tipo_contagio", "estado", "recuperado", "ubicacion_caso", "tipo_recuperacion"]:
        df = df.withColumn(c, F.lower(texto(c)))

    df = df.dropna(subset=["id_caso", "fecha_notificacion", "cod_municipio"]).dropDuplicates(["id_caso"])

    def dias(fin, ini):  # intervalos plausibles; fuera de rango = dato sucio -> null
        d = F.datediff(F.col(fin), F.col(ini))
        return F.when((d >= 0) & (d <= 120), d)

    df = (df.withColumn("dias_sintomas_diag", dias("fecha_diagnostico", "fecha_inicio_sintomas"))
            .withColumn("dias_notif_diag", dias("fecha_diagnostico", "fecha_notificacion"))
            .withColumn("periodo", F.date_format("fecha_notificacion", "yyyy-MM")))

    with medir("spark", "limpieza_csv_a_parquet") as m:
        (df.repartition("periodo").write.mode("overwrite").partitionBy("periodo")
           .option("compression", "snappy").parquet(str(SILVER_DIR)))
        m["filas"] = spark.read.parquet(str(SILVER_DIR)).count()
    print(f"Silver -> {SILVER_DIR} ({m['filas']:,} filas)")
    spark.stop()


if __name__ == "__main__":
    main()
