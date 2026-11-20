// ============ Consultas de trazabilidad (ejecutar en Neo4j Browser http://localhost:7474) ============

// T1. Hacia ATRÁS: cadena de contagio desde el caso raíz hasta el caso consultado
MATCH p = (raiz:Caso)-[:CONTAGIO_A*1..15]->(c:Caso {id: $id})
WHERE NOT ()-[:CONTAGIO_A]->(raiz)
RETURN [n IN nodes(p) | n.id] AS cadena, length(p) AS generaciones;

// T2. Hacia ADELANTE: alcance de un caso (casos afectados, generaciones y municipios)
MATCH p = (o:Caso {id: $id})-[:CONTAGIO_A*1..15]->(c:Caso)
WITH collect(DISTINCT c) AS afectados, max(length(p)) AS generaciones
UNWIND afectados AS a
MATCH (a)-[:UBICADO_EN]->(m:Municipio)
RETURN size(afectados) AS casos_afectados, generaciones, m.nombre AS municipio, count(*) AS casos
ORDER BY casos DESC LIMIT 5;

// T3. Super-propagadores: casos con más contagios directos
MATCH (c:Caso)-[x:CONTAGIO_A]->()
RETURN c.id AS caso, c.estado AS estado, count(x) AS hijos
ORDER BY hijos DESC LIMIT 10;

// T4. Tamaño de las cadenas de contagio (componentes conexos, requiere GDS)
CALL gds.graph.project('contagio', 'Caso', 'CONTAGIO_A');
CALL gds.wcc.stream('contagio') YIELD nodeId, componentId
RETURN componentId, count(*) AS tamano ORDER BY tamano DESC LIMIT 10;

// T5. Nodos críticos por PageRank (requiere GDS)
CALL gds.pageRank.stream('contagio') YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS caso, round(score, 4) AS pagerank
ORDER BY pagerank DESC LIMIT 10;
