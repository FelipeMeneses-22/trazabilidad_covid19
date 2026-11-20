# Decisión de diseño principal (para la diapositiva)

**Decisión:** modelar el contagio como grafo dirigido de casos (`CONTAGIO_A`) en Neo4j y mantener
los eventos en Cassandra con una tabla por consulta.

**Alternativa descartada:** guardar todo en una sola base relacional (o solo en Cassandra) y
resolver la trazabilidad con JOINs / consultas recursivas.

**Por qué:** la trazabilidad hacia atrás y hacia adelante son recorridos de profundidad variable;
en un grafo cuestan proporcional a la cadena recorrida, no al tamaño de la tabla. Cassandra, en
cambio, no hace recorridos pero sí lecturas por clave en milisegundos con escrituras masivas, por
eso sirve el historial por caso y por municipio-día.

**Costo asumido:** duplicar datos (Cassandra, Neo4j, S3, Snowflake) y que las aristas sean
sintéticas porque la fuente no registra contagiador.
