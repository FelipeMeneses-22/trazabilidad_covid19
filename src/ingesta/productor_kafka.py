"""Lee el CSV local por bloques y publica eventos de movimiento del caso en Kafka.

Por cada caso se emiten hasta 3 eventos: NOTIFICACION, DIAGNOSTICO y DESENLACE
(RECUPERADO o FALLECIDO). La clave del mensaje es id_caso: todos los eventos de un
caso van a la misma partición y conservan su orden.

    python -m src.ingesta.productor_kafka --max-casos 500000 --eps 0
"""
import argparse
import json
import time

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.comunes.config import CSV_PATH, KAFKA_SERVERS, KAFKA_TOPIC
from src.comunes.metricas import medir
from src.comunes.transformar import normalizar


def crear_topico(particiones: int = 3):
    admin = AdminClient({"bootstrap.servers": KAFKA_SERVERS})
    if KAFKA_TOPIC not in admin.list_topics(timeout=10).topics:
        for fut in admin.create_topics([NewTopic(KAFKA_TOPIC, particiones, 1)]).values():
            fut.result()
        print(f"Tópico {KAFKA_TOPIC} creado ({particiones} particiones)")


def eventos_de(fila: dict):
    base = {k: fila[k] for k in ("id_caso", "cod_municipio", "nombre_municipio", "cod_departamento",
                                 "nombre_departamento", "sexo", "edad", "tipo_contagio")}
    pares = [("NOTIFICACION", fila["fecha_notificacion"], fila["estado"]),
             ("DIAGNOSTICO", fila["fecha_diagnostico"], fila["estado"])]
    if pd.notna(fila["fecha_muerte"]):
        pares.append(("FALLECIDO", fila["fecha_muerte"], "fallecido"))
    elif pd.notna(fila["fecha_recuperacion"]):
        pares.append(("RECUPERADO", fila["fecha_recuperacion"], "recuperado"))
    for tipo, fecha, estado in pares:
        if pd.isna(fecha):
            continue
        ev = dict(base, tipo_evento=tipo, fecha_evento=fecha.strftime("%Y-%m-%d"),
                  estado=None if pd.isna(estado) else estado)
        yield {k: (None if pd.isna(v) else (int(v) if k in ("id_caso", "edad") else v)) for k, v in ev.items()}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=CSV_PATH)
    ap.add_argument("--max-casos", type=int, default=0, help="0 = todo el archivo")
    ap.add_argument("--eps", type=int, default=0, help="eventos por segundo (0 = sin límite)")
    ap.add_argument("--bloque", type=int, default=100_000)
    args = ap.parse_args()

    crear_topico()
    prod = Producer({"bootstrap.servers": KAFKA_SERVERS, "linger.ms": 50, "compression.type": "lz4",
                     "queue.buffering.max.messages": 500_000})
    casos = eventos = 0
    with medir("kafka", "producir") as m:
        for bloque in pd.read_csv(args.csv, chunksize=args.bloque, dtype=str):
            df = normalizar(bloque)
            if args.max_casos:
                df = df.head(args.max_casos - casos)
            for fila in df.to_dict("records"):
                for ev in eventos_de(fila):
                    while True:
                        try:
                            prod.produce(KAFKA_TOPIC, key=str(ev["id_caso"]), value=json.dumps(ev))
                            break
                        except BufferError:
                            prod.poll(0.2)
                    eventos += 1
                    prod.poll(0)
                    if args.eps and eventos % args.eps == 0:
                        time.sleep(1)
                casos += 1
            print(f"  {casos:,} casos / {eventos:,} eventos")
            if args.max_casos and casos >= args.max_casos:
                break
        prod.flush()
        m["filas"] = eventos
    print(f"Listo: {casos:,} casos -> {eventos:,} eventos en '{KAFKA_TOPIC}'")


if __name__ == "__main__":
    main()
