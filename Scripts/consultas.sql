-- consultas.sql (actualizado)
-- Script para las consultas de las etapas 3 y 4 con esquema

-- Establecer el esquema por defecto
SET search_path TO examen_02;

-- Etapa 3: Completitud de Datos

-- Crear una tabla de calendario con todas las fechas entre 2023-01-01 y 2024-12-31
CREATE TABLE calendario AS
SELECT generate_series('2023-01-01'::date, '2024-12-31'::date, '1 day'::interval) AS fecha;

-- Crear una tabla con todas las combinaciones esperadas de fechas y embalses
CREATE TABLE combinaciones_esperadas AS
SELECT c.fecha, e.id AS embalse_id
FROM calendario c
CROSS JOIN embalses e;

-- Identificar fechas faltantes
SELECT
    ce.fecha,
    ce.embalse_id,
    e.nombre AS embalse_nombre,
    r.nombre AS region_nombre
INTO fechas_faltantes
FROM combinaciones_esperadas ce
LEFT JOIN aportes a ON ce.fecha = a.fecha AND ce.embalse_id = a.embalse_id
JOIN embalses e ON ce.embalse_id = e.id
JOIN regiones r ON e.region_id = r.id
WHERE a.id IS NULL
ORDER BY ce.fecha, ce.embalse_id;

-- Resumen de completitud por embalse
SELECT
    e.id AS embalse_id,
    e.nombre AS embalse_nombre,
    r.nombre AS region_nombre,
    COALESCE(COUNT(a.id), 0) AS dias_con_datos,
    731 AS dias_totales,
    (COALESCE(COUNT(a.id), 0) * 100.0 / 731) AS porcentaje_completitud
INTO completitud
FROM embalses e
JOIN regiones r ON e.region_id = r.id
LEFT JOIN aportes a ON e.id = a.embalse_id
GROUP BY e.id, e.nombre, r.nombre
ORDER BY porcentaje_completitud DESC;

-- Etapa 4: Niveles Mínimos de Aporte Hídrico

-- Calcular el umbral mínimo (percentil 10) por embalse
CREATE TABLE umbrales AS
SELECT
    embalse_id,
    percentile_cont(0.1) WITHIN GROUP (ORDER BY aporte_hidrico) AS umbral_minimo
FROM aportes
GROUP BY embalse_id;

-- Identificar días con aportes por debajo del umbral
SELECT
    a.fecha,
    e.nombre AS embalse_nombre,
    r.nombre AS region_nombre,
    a.aporte_hidrico,
    u.umbral_minimo
INTO dias_criticos
FROM aportes a
JOIN embalses e ON a.embalse_id = e.id
JOIN regiones r ON a.region_id = r.id
JOIN umbrales u ON a.embalse_id = u.embalse_id
WHERE a.aporte_hidrico < u.umbral_minimo
ORDER BY a.fecha, e.nombre;

-- Agrupar por períodos consecutivos
WITH periodos_criticos AS (
    SELECT
        a.fecha,
        e.nombre AS embalse_nombre,
        r.nombre AS region_nombre,
        a.aporte_hidrico,
        u.umbral_minimo,
        SUM(CASE WHEN a.aporte_hidrico < u.umbral_minimo THEN 1 ELSE 0 END) 
            OVER (PARTITION BY e.id ORDER BY a.fecha) AS grupo
    FROM aportes a
    JOIN embalses e ON a.embalse_id = e.id
    JOIN regiones r ON a.region_id = r.id
    JOIN umbrales u ON a.embalse_id = u.embalse_id
)
SELECT
    embalse_nombre,
    region_nombre,
    MIN(fecha) AS inicio_periodo,
    MAX(fecha) AS fin_periodo,
    COUNT(*) AS dias_criticos
INTO periodos_criticos
FROM periodos_criticos
WHERE aporte_hidrico < umbral_minimo
GROUP BY embalse_nombre, region_nombre, grupo
ORDER BY inicio_periodo;

-- Nueva consulta para delta porcentual en 2024 (Etapa 4 - 30%)
WITH aportes_2024 AS (
    SELECT
        e.nombre AS embalse_nombre,
        a.aporte_hidrico,
        ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY a.aporte_hidrico) AS rn_min,
        ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY a.aporte_hidrico DESC) AS rn_max,
        MAX(a.aporte_hidrico) OVER (PARTITION BY e.id) AS max_aporte,
        MIN(a.aporte_hidrico) OVER (PARTITION BY e.id) AS min_aporte
    FROM examen_02.aportes a
    JOIN examen_02.embalses e ON a.embalse_id = e.id
    WHERE EXTRACT(YEAR FROM a.fecha) = 2024
)
SELECT
    embalse_nombre,
    min_aporte AS valor_minimo,
    max_aporte AS valor_maximo,
    CASE
        WHEN max_aporte = 0 THEN 0 -- Evitar división por cero
        ELSE ROUND(((max_aporte - min_aporte) * 100.0 / max_aporte)::numeric, 2)
    END AS delta_porcentual
INTO delta_2024
FROM aportes_2024
WHERE rn_min = 1 OR rn_max = 1
GROUP BY embalse_nombre, min_aporte, max_aporte
ORDER BY embalse_nombre;

-- Generar plan de ejecución para delta_2024
EXPLAIN
WITH aportes_2024 AS (
    SELECT
        e.nombre AS embalse_nombre,
        a.aporte_hidrico,
        ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY a.aporte_hidrico) AS rn_min,
        ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY a.aporte_hidrico DESC) AS rn_max,
        MAX(a.aporte_hidrico) OVER (PARTITION BY e.id) AS max_aporte,
        MIN(a.aporte_hidrico) OVER (PARTITION BY e.id) AS min_aporte
    FROM examen_02.aportes a
    JOIN examen_02.embalses e ON a.embalse_id = e.id
    WHERE EXTRACT(YEAR FROM a.fecha) = 2024
)
SELECT
    embalse_nombre,
    min_aporte AS valor_minimo,
    max_aporte AS valor_maximo,
    CASE
        WHEN max_aporte = 0 THEN 0
        ELSE ROUND(((max_aporte - min_aporte) * 100.0 / max_aporte)::numeric, 2)
    END AS delta_porcentual
FROM aportes_2024
WHERE rn_min = 1 OR rn_max = 1
GROUP BY embalse_nombre, min_aporte, max_aporte
ORDER BY embalse_nombre;

SELECT * FROM examen_02.completitud LIMIT 5;
SELECT * FROM examen_02.fechas_faltantes LIMIT 5;
SELECT * FROM examen_02.dias_criticos LIMIT 5;
SELECT * FROM examen_02.periodos_criticos LIMIT 5;