"""Sube a S3 el CSV crudo (bronze) y el Parquet limpio + gold (silver/gold).

Requiere credenciales AWS (aws configure o variables de entorno) y S3_BUCKET en .env.

    python -m src.analitica.subir_s3 [--sin-bronze]
"""
import argparse
import os
from pathlib import Path

import boto3

from src.comunes.config import CSV_PATH, LAKE_DIR
from src.comunes.metricas import medir


def subir_directorio(s3, bucket, base: Path, prefijo: str) -> int:
    n = 0
    for f in base.rglob("*"):
        if f.is_file() and not f.name.startswith(("_", ".")) and not f.name.endswith(".crc"):
            s3.upload_file(str(f), bucket, f"{prefijo}/{f.relative_to(base).as_posix()}")
            n += 1
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-bronze", action="store_true")
    args = ap.parse_args()
    bucket = os.environ["S3_BUCKET"]
    s3 = boto3.client("s3")

    if not args.sin_bronze:
        with medir("s3", "subir_bronze_csv"):
            s3.upload_file(CSV_PATH, bucket, f"bronze/casos_covid/{Path(CSV_PATH).name}")
    for capa in ("silver", "gold"):
        with medir("s3", f"subir_{capa}") as m:
            m["filas"] = subir_directorio(s3, bucket, LAKE_DIR / capa, capa)  # 'filas' = archivos subidos
    print(f"Datos en s3://{bucket}/ (bronze/ silver/ gold/)")


if __name__ == "__main__":
    main()
