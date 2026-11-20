-- Se ejecuta con src/analitica/cargar_snowflake.py (usa DATABASE/SCHEMA/WAREHOUSE del .env)
CREATE TABLE IF NOT EXISTS fact_casos (
  id_caso NUMBER, fecha_reporte DATE, fecha_notificacion DATE,
  cod_departamento STRING, nombre_departamento STRING, cod_municipio STRING, nombre_municipio STRING,
  edad NUMBER, unidad_edad STRING, sexo STRING, tipo_contagio STRING, ubicacion_caso STRING,
  estado STRING, recuperado STRING, fecha_inicio_sintomas DATE, fecha_diagnostico DATE,
  fecha_muerte DATE, fecha_recuperacion DATE, tipo_recuperacion STRING, cod_pais STRING,
  nombre_pais STRING, pertenencia_etnica STRING, grupo_etnico STRING,
  dias_sintomas_diag NUMBER, dias_notif_diag NUMBER
) CLUSTER BY (cod_departamento, fecha_notificacion);

CREATE TABLE IF NOT EXISTS gold_indicadores_depto_mes (
  periodo STRING, cod_departamento STRING, nombre_departamento STRING, casos NUMBER,
  fallecidos NUMBER, dias_sint_diag_prom FLOAT, dias_notif_diag_prom FLOAT);

CREATE TABLE IF NOT EXISTS gold_tiempo_deteccion (
  cod_departamento STRING, nombre_departamento STRING, casos_con_dato NUMBER,
  p50_dias NUMBER, p90_dias NUMBER, promedio_dias FLOAT);

CREATE TABLE IF NOT EXISTS gold_r_efectivo_mes (
  periodo STRING, casos NUMBER, contagios_generados NUMBER, r_efectivo FLOAT);

CREATE TABLE IF NOT EXISTS gold_alcance_por_origen (semilla NUMBER, generacion NUMBER, casos NUMBER);

CREATE TABLE IF NOT EXISTS gold_nodos_criticos (
  id_caso NUMBER, hijos NUMBER, alcance_total NUMBER, max_generacion NUMBER);
