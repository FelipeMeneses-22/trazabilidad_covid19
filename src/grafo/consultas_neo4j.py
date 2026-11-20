"""Ejecuta las consultas de trazabilidad en Neo4j, mide tiempos y guarda la evidencia.

    python -m src.grafo.consultas_neo4j
Salida: resultados/consultas_neo4j.json (consulta, parámetros, filas, ms, primeras filas)
"""
import json
import time

from neo4j import GraphDatabase

from src.comunes.config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER, RESULTADOS_DIR

CONSULTAS = {
    "T1_trazabilidad_hacia_atras": """
        MATCH p = (raiz:Caso)-[:CONTAGIO_A*1..15]->(c:Caso {id: $hoja})
        WHERE NOT ()-[:CONTAGIO_A]->(raiz)
        RETURN [n IN nodes(p) | n.id] AS cadena, length(p) AS generaciones""",
    "T2_trazabilidad_hacia_adelante": """
        MATCH p = (o:Caso {id: $raiz})-[:CONTAGIO_A*1..15]->(c:Caso)
        WITH collect(DISTINCT c) AS afectados, max(length(p)) AS generaciones
        UNWIND afectados AS a
        MATCH (a)-[:UBICADO_EN]->(m:Municipio)
        RETURN size(afectados) AS casos_afectados, generaciones, m.nombre AS municipio, count(*) AS casos
        ORDER BY casos DESC LIMIT 5""",
    "T3_super_propagadores": """
        MATCH (c:Caso)-[x:CONTAGIO_A]->()
        RETURN c.id AS caso, c.estado AS estado, count(x) AS hijos ORDER BY hijos DESC LIMIT 10""",
}
GDS = {
    "T4_tamano_cadenas_wcc": """
        CALL gds.wcc.stream('contagio') YIELD nodeId, componentId
        RETURN componentId, count(*) AS tamano ORDER BY tamano DESC LIMIT 10""",
    "T5_nodos_criticos_pagerank": """
        CALL gds.pageRank.stream('contagio') YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).id AS caso, round(score, 4) AS pagerank ORDER BY pagerank DESC LIMIT 10""",
}


def correr(s, nombre, cql, **params):
    t0 = time.perf_counter()
    filas = [r.data() for r in s.run(cql, **params)]
    ms = (time.perf_counter() - t0) * 1000
    print(f"{nombre}: {len(filas)} filas en {ms:.0f} ms")
    return {"cql": " ".join(cql.split()), "params": params, "filas": len(filas), "ms": round(ms, 1), "muestra": filas[:5]}


def main():
    salida = {}
    with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as d, d.session() as s:
        # caso demo: la raíz con más hijos y un descendiente a >= 3 generaciones
        raiz = s.run("""MATCH (r:Caso)-[:CONTAGIO_A]->() WHERE NOT ()-[:CONTAGIO_A]->(r)
                        RETURN r.id AS id, count(*) AS h ORDER BY h DESC LIMIT 1""").single()["id"]
        hoja = s.run("MATCH (:Caso {id: $r})-[:CONTAGIO_A*3..8]->(c) RETURN c.id AS id LIMIT 1", r=raiz).single()
        hoja = hoja["id"] if hoja else raiz
        params = {"T1_trazabilidad_hacia_atras": {"hoja": hoja}, "T2_trazabilidad_hacia_adelante": {"raiz": raiz}}
        for nombre, cql in CONSULTAS.items():
            salida[nombre] = correr(s, nombre, cql, **params.get(nombre, {}))
        try:
            s.run("CALL gds.graph.drop('contagio', false)").consume()
            s.run("CALL gds.graph.project('contagio', 'Caso', 'CONTAGIO_A')").consume()
            for nombre, cql in GDS.items():
                salida[nombre] = correr(s, nombre, cql)
        except Exception as e:  # GDS no instalado
            print("GDS no disponible, se omiten T4/T5:", str(e)[:120])
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    (RESULTADOS_DIR / "consultas_neo4j.json").write_text(json.dumps(salida, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
