-- modelo_de_datos.sql
-- Script para crear el modelo de datos inicial y normalizado con esquema

-- Crear la base de datos 
CREATE DATABASE tadb_examen_02;

-- Crear el usuario y otorgar permisos 
CREATE ROLE "JoseBD" WITH LOGIN PASSWORD '1234';
GRANT ALL PRIVILEGES ON DATABASE tadb_examen_02 TO "JoseBD";
\c tadb_examen_02  -- Conectar a la base de datos tadb_examen_02
GRANT ALL ON SCHEMA public TO "JoseBD";

-- Crear el esquema examen_02
CREATE SCHEMA examen_02;
GRANT ALL ON SCHEMA examen_02 TO "JoseBD";

-- Establecer el esquema por defecto para las operaciones siguientes
SET search_path TO examen_02;

-- Crear la tabla de regiones
CREATE TABLE regiones (
    id INTEGER PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL,
    CONSTRAINT uk_regiones_nombre UNIQUE (nombre)
);

-- Crear la tabla de fuentes hídricas
CREATE TABLE fuentes_hidricas (
    id INTEGER PRIMARY KEY,
    nombre VARCHAR(4) NOT NULL,
    CONSTRAINT uk_fuentes_nombre UNIQUE (nombre)
);

-- Crear la tabla de embalses con la UK compuesta y relaciones obligatorias
CREATE TABLE embalses (
    id INTEGER PRIMARY KEY,
    nombre VARCHAR(4) NOT NULL,
    fuente_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    CONSTRAINT fk_embalses_fuente FOREIGN KEY (fuente_id) REFERENCES fuentes_hidricas(id),
    CONSTRAINT fk_embalses_region FOREIGN KEY (region_id) REFERENCES regiones(id),
    CONSTRAINT uk_embalses_compuesta UNIQUE (nombre, fuente_id, region_id)
);

-- Crear la tabla inicial para los datos crudos (sin normalizar)
CREATE TABLE aportes_inicial (
    fecha DATE,
    serie_hidrologica VARCHAR(8),
    region_hidrologica VARCHAR(50), 
    aporte_hidrico BIGINT
);

-- Insertar datos solo si la tabla está vacía (evitar duplicados)
INSERT INTO regiones (id, nombre)
SELECT * FROM (VALUES
(1, 'Antioquia'),
(2, 'Oriente'),
(3, 'Centro'),
(4, 'Caribe'),
(5, 'Colombia'),
(6, 'Caldas'),
(7, 'Valle')
) AS tmp (id, nombre)
WHERE NOT EXISTS (SELECT 1 FROM regiones);

-- Insertar datos solo si la tabla está vacía
INSERT INTO fuentes_hidricas (id, nombre)
SELECT * FROM (VALUES
(1, 'TENC'), (2, 'PORC'), (3, 'IEPM'), (4, 'CAUC'), (5, 'NARE'),
(6, 'GUAT'), (7, 'POR1'), (8, 'RGRD'), (9, 'CALD'), (10, 'GUAD'),
(11, 'GUAR'), (12, 'MANS'), (13, 'MIEL'), (14, 'SINU'), (15, 'BOGO'),
(16, 'AMOY'), (17, 'MAG1'), (18, 'CUCU'), (19, 'SMAR'), (20, 'PRAD'),
(21, 'MAGD'), (22, 'SOGA'), (23, 'BATA'), (24, 'CHIV'), (25, 'CHUZ'),
(26, 'GUAV'), (27, 'ANCH'), (28, 'CALM'), (29, 'NEGR'), (30, 'CAMP'),
(31, 'CHIN'), (32, 'ELLA'), (33, 'RANC')
) AS tmp (id, nombre)
WHERE NOT EXISTS (SELECT 1 FROM fuentes_hidricas);

-- Insertar datos solo si la tabla está vacía
INSERT INTO embalses (id, nombre, fuente_id, region_id)
SELECT * FROM (VALUES
(1, 'BOCA', 1, 1), (2, 'CLLR', 2, 1), (3, 'DESV', 3, 1),
(4, 'ITUA', 4, 1), (5, 'MIRF', 1, 1), (6, 'PENO', 5, 1),
(7, 'PLAY', 6, 1), (8, 'PP-2', 7, 1), (9, 'PP-3', 2, 1),
(10, 'PUNC', 6, 1), (11, 'QUBR', 8, 1), (12, 'RGR2', 8, 1),
(13, 'SLOR', 5, 1), (14, 'SMIG', 9, 1), (15, 'TRON', 10, 1),
(16, 'DESV', 11, 6), (17, 'DESV', 12, 6), (18, 'PTEH', 13, 6),
(19, 'URR1', 14, 4), (20, 'ALIC', 15, 3), (21, 'AMOY', 16, 3),
(22, 'BETA', 17, 3), (23, 'CUCU', 18, 3), (24, 'DESV', 19, 3),
(25, 'EMBA', 20, 3), (26, 'QUIM', 21, 3), (27, 'SOGA', 22, 3),
(28, 'DESV', 23, 2), (29, 'DESV', 24, 2), (30, 'EMBA', 25, 2),
(31, 'EMBA', 26, 2), (32, 'ESME', 23, 2), (33, 'ALTO', 27, 7),
(34, 'BAJO', 27, 7), (35, 'CAL1', 28, 7), (36, 'FLR2', 4, 7),
(37, 'SALV', 4, 7), (38, 'ESCM', 29, 1), (39, 'CAME', 30, 6),
(40, 'CAME', 31, 6), (41, 'ESME', 30, 6), (42, 'ESTR', 32, 6),
(43, 'SANF', 33, 6), (44, 'GUAV', 26, 2)
) AS tmp (id, nombre, fuente_id, region_id)
WHERE NOT EXISTS (SELECT 1 FROM embalses);

-- Crear la tabla normalizada para los aportes
CREATE TABLE aportes (
    id SERIAL PRIMARY KEY,
    fecha DATE NOT NULL,
    embalse_id INTEGER NOT NULL,
    region_id INTEGER NOT NULL,
    aporte_hidrico BIGINT NOT NULL,
    CONSTRAINT fk_aportes_embalse FOREIGN KEY (embalse_id) REFERENCES embalses(id),
    CONSTRAINT fk_aportes_region FOREIGN KEY (region_id) REFERENCES regiones(id)
);

-- La importación
-- Migrar datos de aportes_inicial a aportes (ejecutar después de la importación)
INSERT INTO examen_02.aportes (fecha, embalse_id, region_id, aporte_hidrico)
SELECT
    ai.fecha,
    e.id AS embalse_id,
    r.id AS region_id,
    ai.aporte_hidrico
FROM examen_02.aportes_inicial ai
JOIN examen_02.embalses e ON
    SUBSTRING(ai.serie_hidrologica FROM 1 FOR 4) = e.nombre
    AND (
        SELECT f.id
        FROM examen_02.fuentes_hidricas f
        WHERE f.nombre = SUBSTRING(ai.serie_hidrologica FROM 5 FOR 4)
    ) = e.fuente_id
JOIN examen_02.regiones r ON ai.region_hidrologica = r.nombre;