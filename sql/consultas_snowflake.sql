-- ===== Consultas analíticas (tablero de trazabilidad) =====

-- A1. Alcance y letalidad por departamento 
SELECT nombre_departamento, COUNT(*) AS casos,
       COUNT_IF(fecha_muerte IS NOT NULL OR estado = 'fallecido') AS fallecidos,
       ROUND(100 * fallecidos / casos, 2) AS letalidad_pct
FROM fact_casos GROUP BY 1 ORDER BY casos DESC;

-- A2. Tiempo de detección (síntomas -> diagnóstico) por departamento
SELECT nombre_departamento, p50_dias, p90_dias, promedio_dias, casos_con_dato
FROM gold_tiempo_deteccion ORDER BY p90_dias DESC, promedio_dias DESC;

-- A3. Nodos críticos con su municipio de origen (cruce grafo + hechos)
SELECT n.id_caso, f.nombre_municipio, n.hijos, n.alcance_total, n.max_generacion
FROM gold_nodos_criticos n JOIN fact_casos f ON f.id_caso = n.id_caso
ORDER BY n.alcance_total DESC LIMIT 20;

-- A4. Curva mensual de casos y R efectivo aproximado (simulación)
SELECT r.periodo, r.casos, r.contagios_generados, r.r_efectivo
FROM gold_r_efectivo_mes r ORDER BY r.periodo;

-- A5. Top 3 municipios por mes (ventana)
SELECT * FROM (
  SELECT TO_CHAR(fecha_notificacion, 'YYYY-MM') AS periodo, nombre_municipio, COUNT(*) AS casos,
         ROW_NUMBER() OVER (PARTITION BY TO_CHAR(fecha_notificacion, 'YYYY-MM') ORDER BY COUNT(*) DESC) AS pos
  FROM fact_casos GROUP BY 1, 2)
WHERE pos <= 3 ORDER BY periodo, pos;

-- A6. Decaimiento de las cadenas: casos alcanzados por generación (promedio de los super-propagadores)
SELECT generacion, ROUND(AVG(casos), 2) AS casos_promedio, SUM(casos) AS casos_total
FROM gold_alcance_por_origen GROUP BY generacion ORDER BY generacion;
