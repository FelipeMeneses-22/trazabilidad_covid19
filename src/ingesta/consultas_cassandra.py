"""Consultas CQL de trazabilidad con su tiempo de respuesta.

    python -m src.ingesta.consultas_cassandra [id_caso] [cod_municipio] [YYYY-MM-DD]
"""
import json
import sys
import time
from datetime import date

from src.comunes.cassandra_cliente import conectar
from src.comunes.config import CASSANDRA_KEYSPACE, RESULTADOS_DIR


def cronometrar(sesion, cql, params):
    t0 = time.perf_counter()
    filas = list(sesion.execute(cql, params))
    return filas, (time.perf_counter() - t0) * 1000


def main():
    sesion = conectar(CASSANDRA_KEYSPACE)
    id_caso = int(sys.argv[1]) if len(sys.argv) > 1 else sesion.execute("SELECT id_caso FROM eventos_por_caso LIMIT 1").one().id_caso
    if len(sys.argv) > 3:
        muni, dia = sys.argv[2], date.fromisoformat(sys.argv[3])
    else:
        r = sesion.execute("SELECT cod_municipio, fecha_evento FROM eventos_por_municipio_dia LIMIT 1").one()
        muni, dia = r.cod_municipio, r.fecha_evento.date() if hasattr(r.fecha_evento, "date") else r.fecha_evento

    consultas = {
        "C1_historial_de_un_caso": ("SELECT * FROM eventos_por_caso WHERE id_caso = %s", (id_caso,)),
        "C2_eventos_municipio_dia": ("SELECT tipo_evento, id_caso, estado FROM eventos_por_municipio_dia "
                                     "WHERE cod_municipio = %s AND fecha_evento = %s", (muni, str(dia))),
        "C3_solo_desenlaces_municipio_dia": ("SELECT id_caso, estado FROM eventos_por_municipio_dia "
                                             "WHERE cod_municipio = %s AND fecha_evento = %s AND tipo_evento = 'FALLECIDO'",
                                             (muni, str(dia))),
    }
    salida = {}
    for nombre, (cql, params) in consultas.items():
        filas, ms = cronometrar(sesion, cql, params)
        salida[nombre] = {"cql": cql, "params": [str(p) for p in params], "filas": len(filas), "ms": round(ms, 2)}
        print(f"{nombre}: {len(filas)} filas en {ms:.1f} ms")
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTADOS_DIR / "consultas_cassandra.json").write_text(json.dumps(salida, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
