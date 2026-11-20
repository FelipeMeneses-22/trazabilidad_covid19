# Reto 5 — Trazabilidad y propagación de casos de COVID-19 (Colombia)

Arquitectura integrada de datos masivos sobre los **casos positivos de COVID-19 en Colombia**
(datos.gov.co, dataset `gt2j-8ykr`, ~6 millones de filas, CSV local).

```
CSV local (6M) ─┬─► Spark (limpieza) ─► Parquet silver ─► S3 (bronze/silver/gold)
                │                          │
                │                          ├─► generar_contactos ─► Neo4j (grafo, subconjunto)
                │                          └─► Spark (propagación, indicadores) ─► gold ─► Snowflake (tablero)
                └─► Productor ─► Kafka ─► Consumidor ─► Cassandra (consulta por caso / municipio-día)
```

| Tecnología | Para qué se usa | Por qué ahí |
|---|---|---|
| Kafka | Eventos de movimiento del caso (notificación, diagnóstico, desenlace) | Log ordenado por `id_caso`, desacopla ingesta y consumo |
| Cassandra | Historial por caso y eventos por municipio-día | Escrituras masivas; tablas diseñadas por consulta |
| S3 | Data lake: bronze (CSV), silver (Parquet por mes), gold | Almacenamiento barato y columnar |
| Spark | Limpieza de 6M filas, indicadores, simulación de propagación (BFS por generaciones) | Procesamiento distribuido |
| Neo4j | Cadenas de contagio hacia atrás/adelante, super-propagadores, PageRank/WCC | Recorridos de grafo en milisegundos |
| Snowflake | Tablero: alcance, tiempo de detección, nodos críticos | SQL analítico sobre hechos + agregados |

## Decisiones importantes (léelas antes de entregar)

1. **CSV local en vez de la API.** 6M de filas por la API Socrata son miles de peticiones paginadas
   con límites de tasa, y el resultado depende de la red y de que el dataset no cambie. Con el CSV
   la corrida es reproducible. Todo el pipeline lee `CSV_PATH`; acepta los encabezados del portal
   ("ID de caso") y los de la API (`id_de_caso`).
2. **Las aristas de contagio son sintéticas.** El dataset **no dice quién contagió a quién**.
   Los casos, municipios y fechas son reales; las relaciones `CONTAGIO_A` las genera
   `src/grafo/generar_contactos.py` con reglas documentadas y semilla fija
   (mismo municipio, ventana de 14 días, probabilidad según tipo de contagio, 1 % de
   super-propagadores). Dilo explícitamente en el informe; el R efectivo y los "nodos críticos"
   son resultado de esos parámetros, no una estimación epidemiológica real.
3. **El grafo usa un subconjunto** (`DEPARTAMENTOS_GRAFO`, por defecto TOLIMA). Los 6M van por
   Spark, S3 y Snowflake; Neo4j y Cassandra reciben un subconjunto para trabajar en un portátil.
   Para Kafka/Cassandra usa `--max-casos` (500 000 casos ≈ 1,5 M eventos).

## Requisitos
Python 3.10–3.12, Java 17+ (Spark), Docker. `pip install -r requirements.txt`, luego copia
`.env.example` a `.env`. Si `cassandra-driver` falla en tu versión de Python, usa 3.11.

## Ejecución (en este orden)
```bash
python scripts/inspeccionar_csv.py                 # 0. valida columnas del CSV (hazlo primero)
python scripts/generar_muestra.py 50000            #    (opcional) muestra para ensayar sin los 6M
docker compose up -d                               # 1. Kafka, Cassandra, Neo4j (+GDS)

python -m src.procesamiento.limpieza               # 2. CSV -> Parquet silver (Spark)
python -m src.ingesta.productor_kafka --max-casos 500000
python -m src.ingesta.consumidor_cassandra         # 3. Kafka -> Cassandra
python -m src.ingesta.consultas_cassandra          #    CQL con tiempos

python -m src.grafo.generar_contactos              # 4. aristas sintéticas
python -m src.grafo.cargar_neo4j --limpiar
python -m src.grafo.consultas_neo4j                #    5 consultas con tiempos

python -m src.procesamiento.analisis_propagacion   # 5. gold (Spark)
python -m src.analitica.subir_s3                   # 6. S3 (requiere S3_BUCKET + credenciales AWS)
python -m src.analitica.cargar_snowflake --completo  # 7. Snowflake; consultas en sql/consultas_snowflake.sql
```
Pruebas sin servicios: `pytest tests -q`.

## Evidencia para el informe
- `resultados/rendimiento.csv`: filas y segundos por etapa (se llena solo). Repite con 100 k, 1 M
  y 6 M filas para mostrar escala.
- `resultados/consultas_neo4j.json` y `consultas_cassandra.json`: consulta, parámetros, filas, ms.
- `src/grafo/consultas.cypher` y `sql/consultas_snowflake.sql`: las consultas a pegar en el informe,
  cada una con su interpretación.
- Parámetros del generador: `data/grafo/metadatos_generador.json`.

## Modelo de datos
- **Neo4j:** `(:Caso)-[:UBICADO_EN]->(:Municipio)-[:PERTENECE_A]->(:Departamento)`,
  `(:Caso)-[:CONTAGIO_A {dias}]->(:Caso)`. Restricciones de unicidad en `id` y `codigo`.
- **Cassandra:** `eventos_por_caso` (partición `id_caso`) y `eventos_por_municipio_dia`
  (partición `(cod_municipio, fecha_evento)`: evita particiones sin límite en Bogotá).
- **S3 / Parquet:** particionado por `periodo` (YYYY-MM), compresión Snappy.
- **Snowflake:** `fact_casos` (cluster por departamento y fecha) + tablas `gold_*`.

## Limitaciones conocidas
- No se probó contra el CSV real de 6M ni contra Docker/Snowflake/S3 reales: se probó la limpieza,
  el generador y los dos scripts Spark con una muestra de 50 000 filas. Ejecuta
  `inspeccionar_csv.py` antes de nada; si algún encabezado no se reconoce, agrégalo en
  `src/comunes/columnas.py`.
- GDS (T4/T5) requiere que Neo4j haya descargado el plugin al primer arranque (necesita internet).
