--=============================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ********************
-- FICHA: INDICADOR N°00 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGOS
-- NOMBRE: Utilización de consultorio externos 
--=============================================================================================

--*******************************************************************************************
-- Creado por           : Edson Donayre Uchuya (OGEI)
-- Motivo               : Indicador Nuevo para el CG 2025
-- Fecha creación       : 05/12/2024
-- Fuente de Datos      : Sistema Electrónico de transferencia de Información de IPRESS SETI – IPRESS (SUSALUD)
--*******************************************************************************************
USE BD_HISINDICADORES
GO

-- RENAES
IF OBJECT_ID('Tempdb..#Renaes') IS NOT NULL DROP TABLE #Renaes
SELECT
	r.diris		,
	r.DESC_DPTO	,
	r.DESC_PROV	,
	r.desc_dist	,
	r.DESC_RED	,
	r.desc_mred	,
	r.cat_estab	,
	r.desc_estab,
	r.COD_ESTAB,
    r.UBIGEO
INTO #Renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK)
WHERE
r.CAT_ESTAB NOT IN('SD','I-1','I-2','I-3')
AND r.COD_ESTAB NOT IN(
    6212,       -- III-E - 00006212: HOSPITAL DE EMERGENCIAS PEDIATRICAS
    6213        -- III-E - 00006213: HOSPITAL DE EMERGENCIAS JOSÉ CASIMIRO ULLOA
    )  
AND r.AMBITO = 1 -- SECTOR MINSA o GOBIERNO REGIONAL

DECLARE @AÑO INT, @MES_FIN INT;

SET @AÑO = 2024     -- AÑO DE EVALUACION
SET @MES_FIN = 10   -- MES HASTA DONDE SE EVALUARÁ

--===============================
-- REPORTE NOMINAL
--===============================
-- Tabla agregada A: Reporte de recursos de salud nombre del campo: consultorios físicos, consultorios funcionales
-- http://datos.susalud.gob.pe/dataset/consulta-recursos-de-salud-por-ipress

-- DENOMINADOR: Número de consultorios físicos de medicina que se encuentren en dicho periodo. (CA_CONSULTORIOS)
-- NUMERADOR: Número total de consultorios funcionales de medicina programados por día, la cual se obtiene de la sumatorio durante el mes. (CA_CONSULTORIOS_FN)

-- ALMACENA EL NOMINAL
IF Object_id(N'tempdb..#DL1153_2025_CG00_UTILIZACION_CONSULTORIO_EXT',N'U') IS NOT NULL DROP TABLE #DL1153_2025_CG00_UTILIZACION_CONSULTORIO_EXT;
SELECT
r.COD_ESTAB,
r.DESC_ESTAB,
s.ANHO [AÑO],
CONVERT(INT, s.MES) [MES],
TRY_CONVERT(int, s.CA_CONSULTORIOS_FN) [NUMERADOR_CONSULTORIO_FUNCIONAL],
TRY_CONVERT(int, s.CA_CONSULTORIOS) [DENOMINADOR_CONSULTORIO_FISICO]
INTO #DL1153_2025_CG00_UTILIZACION_CONSULTORIO_EXT
FROM #Renaes r
LEFT JOIN BD_HISINDICADORES.dbo.SETI_IPRESS_ConsultaA_2024 s ON r.COD_ESTAB = CONVERT(int, s.CO_IPRESS)
WHERE
s.ANHO = @AÑO AND
CONVERT(int,s.MES) <= @MES_FIN

--===============================
-- REPORTE CONSOLIDADO
--===============================

-- SE GENERA EL REPORTE CONSOLIDADO
SELECT
s.ANHO [AÑO],
s.MES [MES],
CASE 
WHEN r.DESC_DPTO = 'LIMA' AND r.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
WHEN r.DESC_DPTO = 'LIMA' AND r.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
ELSE r.DESC_DPTO
END	[DESC_DPTO],
r.DESC_PROV,
r.DESC_DIST,
CASE
WHEN r.DIRIS LIKE '%diris%' THEN r.DIRIS
ELSE r.DESC_RED
END	[RED],
r.DESC_MRED [MICRORED],
r.CAT_ESTAB,
r.DESC_ESTAB [ESTABLECIMIENTO],
r.UBIGEO,
SUM(IIF(TRY_CONVERT(int, s.CA_CONSULTORIOS_FN) IS NULL, 0, s.CA_CONSULTORIOS_FN)) [NUMERADOR_CONSULTORIO_FUNCIONAL],
SUM(IIF(TRY_CONVERT(int, s.CA_CONSULTORIOS) IS NULL, 0, s.CA_CONSULTORIOS)) [DENOMINADOR_CONSULTORIO_FISICO]
FROM #Renaes r
LEFT JOIN BD_HISINDICADORES.dbo.SETI_IPRESS_ConsultaA_2024 s ON r.COD_ESTAB = CONVERT(int, s.CO_IPRESS)
WHERE
s.ANHO = @AÑO AND
CONVERT(int,s.MES) <= @MES_FIN
GROUP BY
s.ANHO,
s.MES,
CASE 
WHEN r.DESC_DPTO = 'LIMA' AND r.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
WHEN r.DESC_DPTO = 'LIMA' AND r.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
ELSE r.DESC_DPTO
END,
r.DESC_PROV,
r.DESC_DIST,
CASE
WHEN r.DIRIS LIKE '%diris%' THEN r.DIRIS
ELSE r.DESC_RED
END,
r.DESC_MRED,
r.CAT_ESTAB,
r.DESC_ESTAB,
r.UBIGEO

-- *****************************************************
-- FIN :D
-- *****************************************************
