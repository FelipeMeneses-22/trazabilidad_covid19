-- Modelo "query-first": una tabla por patrón de consulta.
CREATE KEYSPACE IF NOT EXISTS trazabilidad
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Q1: historial completo de un caso (trazabilidad por lote/caso)
CREATE TABLE IF NOT EXISTS trazabilidad.eventos_por_caso (
  id_caso          bigint,
  fecha_evento     date,
  tipo_evento      text,
  cod_municipio    text,
  nombre_municipio text,
  cod_departamento text,
  estado           text,
  PRIMARY KEY ((id_caso), fecha_evento, tipo_evento)
) WITH CLUSTERING ORDER BY (fecha_evento DESC, tipo_evento ASC);

-- Q2: eventos de un municipio en un día (trazabilidad por ubicación).
-- La partición incluye el día para que ningún municipio (p. ej. Bogotá) cree
-- una partición sin límite de tamaño.
CREATE TABLE IF NOT EXISTS trazabilidad.eventos_por_municipio_dia (
  cod_municipio    text,
  fecha_evento     date,
  tipo_evento      text,
  id_caso          bigint,
  nombre_municipio text,
  cod_departamento text,
  estado           text,
  sexo             text,
  edad             int,
  PRIMARY KEY ((cod_municipio, fecha_evento), tipo_evento, id_caso)
);
