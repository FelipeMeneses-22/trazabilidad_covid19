"""Carga el grafo en Neo4j desde data/grafo/*.csv.

Modelo:  (:Caso)-[:UBICADO_EN]->(:Municipio)-[:PERTENECE_A]->(:Departamento)
         (:Caso)-[:CONTAGIO_A {dias}]->(:Caso)

    python -m src.grafo.cargar_neo4j [--limpiar]
"""
import argparse

import pandas as pd
from neo4j import GraphDatabase

from src.comunes.config import GRAFO_DIR, NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER
from src.comunes.metricas import medir

LOTE = 10_000

INDICES = [
    "CREATE CONSTRAINT caso_id IF NOT EXISTS FOR (c:Caso) REQUIRE c.id IS UNIQUE",
    "CREATE CONSTRAINT muni_cod IF NOT EXISTS FOR (m:Municipio) REQUIRE m.codigo IS UNIQUE",
    "CREATE CONSTRAINT depto_cod IF NOT EXISTS FOR (d:Departamento) REQUIRE d.codigo IS UNIQUE",
]

Q_DIMS = """
UNWIND $rows AS r
MERGE (d:Departamento {codigo: r.cod_departamento}) SET d.nombre = r.nombre_departamento
MERGE (m:Municipio {codigo: r.cod_municipio}) SET m.nombre = r.nombre_municipio
MERGE (m)-[:PERTENECE_A]->(d)
"""
Q_CASOS = """
UNWIND $rows AS r
MERGE (c:Caso {id: r.id_caso})
SET c.fecha = date(r.fecha_notificacion), c.edad = r.edad, c.sexo = r.sexo,
    c.estado = r.estado, c.tipo_contagio = r.tipo_contagio
WITH c, r
MATCH (m:Municipio {codigo: r.cod_municipio})
MERGE (c)-[:UBICADO_EN]->(m)
"""
Q_ARISTAS = """
UNWIND $rows AS r
MATCH (a:Caso {id: r.origen_id}), (b:Caso {id: r.destino_id})
MERGE (a)-[x:CONTAGIO_A]->(b) SET x.dias = r.dias
"""


def registros(df: pd.DataFrame):
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


def en_lotes(sesion, consulta, df, enteros=()):
    for c in enteros:
        df[c] = pd.array(df[c], dtype="Int64")
    filas = registros(df)
    for i in range(0, len(filas), LOTE):
        sesion.run(consulta, rows=filas[i:i + LOTE]).consume()
    return len(filas)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limpiar", action="store_true", help="borra todo el grafo antes de cargar")
    args = ap.parse_args()

    nodos = pd.read_csv(GRAFO_DIR / "nodos_caso.csv", dtype={"cod_municipio": str, "cod_departamento": str})
    aristas = pd.read_csv(GRAFO_DIR / "aristas_contacto.csv")

    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver, driver.session() as s:
        if args.limpiar:
            s.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 50000 ROWS").consume()
        for q in INDICES:
            s.run(q).consume()
        with medir("neo4j", "cargar_dimensiones"):
            dims = nodos[["cod_departamento", "nombre_departamento", "cod_municipio", "nombre_municipio"]].drop_duplicates()
            en_lotes(s, Q_DIMS, dims)
        with medir("neo4j", "cargar_casos") as m:
            m["filas"] = en_lotes(s, Q_CASOS, nodos[["id_caso", "fecha_notificacion", "edad", "sexo", "estado",
                                                    "tipo_contagio", "cod_municipio"]].copy(), enteros=("id_caso", "edad"))
        with medir("neo4j", "cargar_aristas") as m:
            m["filas"] = en_lotes(s, Q_ARISTAS, aristas.copy(), enteros=("origen_id", "destino_id", "dias"))
        r = s.run("MATCH (c:Caso) WITH count(c) AS casos MATCH ()-[x:CONTAGIO_A]->() RETURN casos, count(x) AS aristas").single()
        print(f"Neo4j: {r['casos']:,} casos, {r['aristas']:,} relaciones CONTAGIO_A")


if __name__ == "__main__":
    main()
