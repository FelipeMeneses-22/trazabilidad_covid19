"""Consume eventos de Kafka y los escribe en Cassandra (dos tablas, una por consulta).

    python -m src.ingesta.consumidor_cassandra --max-eventos 0 --inactividad 20
"""
import argparse
import json
import time
from datetime import date
from pathlib import Path

from cassandra.concurrent import execute_concurrent_with_args
from confluent_kafka import Consumer

from src.comunes.cassandra_cliente import conectar
from src.comunes.config import KAFKA_SERVERS, KAFKA_TOPIC
from src.comunes.metricas import medir

SCHEMA = Path(__file__).resolve().parents[2] / "cassandra/schema.cql"


def preparar_sesion():
    sesion = conectar()
    for stmt in SCHEMA.read_text().split(";"):
        if stmt.strip():
            sesion.execute(stmt)
    sesion.set_keyspace("trazabilidad")
    return sesion


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-eventos", type=int, default=0, help="0 = hasta quedar inactivo")
    ap.add_argument("--inactividad", type=int, default=20, help="segundos sin mensajes para terminar")
    ap.add_argument("--lote", type=int, default=1000)
    ap.add_argument("--grupo", default="cassandra-sink")
    args = ap.parse_args()

    sesion = preparar_sesion()
    p_caso = sesion.prepare("INSERT INTO eventos_por_caso (id_caso, fecha_evento, tipo_evento, cod_municipio, "
                            "nombre_municipio, cod_departamento, estado) VALUES (?,?,?,?,?,?,?)")
    p_muni = sesion.prepare("INSERT INTO eventos_por_municipio_dia (cod_municipio, fecha_evento, tipo_evento, id_caso, "
                            "nombre_municipio, cod_departamento, estado, sexo, edad) VALUES (?,?,?,?,?,?,?,?,?)")

    cons = Consumer({"bootstrap.servers": KAFKA_SERVERS, "group.id": args.grupo,
                     "auto.offset.reset": "earliest", "enable.auto.commit": False})
    cons.subscribe([KAFKA_TOPIC])

    total, ultimo_msg = 0, time.time()
    with medir("cassandra", "ingesta_desde_kafka") as m:
        while True:
            msgs = cons.consume(num_messages=args.lote, timeout=1.0)
            msgs = [x for x in msgs if x.error() is None]
            if not msgs:
                if time.time() - ultimo_msg > args.inactividad:
                    break
                continue
            ultimo_msg = time.time()
            evs = [json.loads(x.value()) for x in msgs]
            f = lambda e: date.fromisoformat(e["fecha_evento"])  # noqa: E731
            por_caso = [(e["id_caso"], f(e), e["tipo_evento"], e["cod_municipio"], e["nombre_municipio"],
                         e["cod_departamento"], e["estado"]) for e in evs]
            por_muni = [(e["cod_municipio"], f(e), e["tipo_evento"], e["id_caso"], e["nombre_municipio"],
                         e["cod_departamento"], e["estado"], e["sexo"], e["edad"]) for e in evs]
            for stmt, datos in ((p_caso, por_caso), (p_muni, por_muni)):
                for ok, res in execute_concurrent_with_args(sesion, stmt, datos, concurrency=64, raise_on_first_error=False):
                    if not ok:
                        raise RuntimeError(res)
            cons.commit(asynchronous=False)
            total += len(evs)
            if total % 50_000 < args.lote:
                print(f"  {total:,} eventos")
            if args.max_eventos and total >= args.max_eventos:
                break
        m["filas"] = total
    cons.close()
    print(f"Listo: {total:,} eventos escritos en Cassandra (x2 tablas)")


if __name__ == "__main__":
    main()
