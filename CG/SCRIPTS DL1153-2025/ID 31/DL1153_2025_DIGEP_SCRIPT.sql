--====================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ***********
-- FICHA: INDICADOR DIGEP - DL 1153 - 2025
-- NOMBRE: Porcentaje de personal registrado en el aplicativo del Registro Nacional de
-- Personal de la Salud sin inconsistencias de información
--====================================================================================



USE [BD_HISINDICADORES]
GO

--***************************************************
--				BASES DE DATOS.
--***************************************************
-- INFORHUS
IF OBJECT_ID('Tempdb..#INFORHUS') IS NOT NULL DROP TABLE #INFORHUS
SELECT
	d.id_pliego,
	d.Pliego_desc,
	d.ue,
	d.ue_desc,
	d.id_ubigeo,
	d.diresa,
	d.renaes,
	Estab_Desc = d.etab_Desc,
	d.regimen_laboral,
	d.inconsistencias
INTO #INFORHUS
FROM [dbo].[DL1153_2024_DIGEP_Base] d

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
CREATE TABLE #tabla_reporte
(
	Pliego_desc	NVARCHAR(100)	, 
	ue_desc		NVARCHAR(100)	,
	renaes		INT				,
	Estab_Desc	NVARCHAR(200)	,
	año			INT				,
	mes			INT				,
	Den			INT				,
	Num			INT
)

DECLARE @mes_inicio INT	,
		@mes_eval	INT	,
		@año		INT	

SET @mes_inicio = 1		--<========= Mes inicio
SET @mes_eval   = 11	--<========= Mes de evaluación
SET @año		= 2024	--<========= Año de evaluación

--%%%%%%%%%%%%%%%%
--  DENOMINADOR 
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Denominador_DIGEP') IS NOT NULL DROP TABLE #Denominador_DIGEP
SELECT
	i.Pliego_desc	,
	i.ue_desc		,
	i.renaes		,
	i.Estab_Desc	,
	Den				= COUNT(*)
INTO #Denominador_DIGEP
FROM #INFORHUS i
GROUP BY	i.Pliego_desc	,
			i.ue_desc		,
			i.renaes		,
			i.Estab_Desc

--%%%%%%%%%%%%%%%%
--  NUMERADOR 
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Numerador_DIGEP') IS NOT NULL DROP TABLE #Numerador_DIGEP
SELECT
	i.Pliego_desc	,
	i.ue_desc		,
	i.renaes		,
	i.Estab_Desc	,
	Num				= COUNT(*)
INTO #Numerador_DIGEP
FROM #INFORHUS i
WHERE i.inconsistencias = 0
GROUP BY	i.Pliego_desc	,
			i.ue_desc		,
			i.renaes		,
			i.Estab_Desc

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
INSERT INTO #Tabla_Reporte
SELECT
	d.Pliego_desc	,
	d.ue_desc		,
	d.renaes		,	
	d.Estab_Desc	,
	año				= 2024,		
	mes				= 11,			
	Den				= d.Den,			
	Num				= ISNULL(n.Num,0)
FROM #Denominador_DIGEP d
LEFT JOIN #Numerador_DIGEP n ON	d.Pliego_desc	= n.Pliego_desc AND
								d.ue_desc		= n.ue_desc		AND
								d.renaes		= n.renaes

--%%%%%%%%%%%%%%%%
--	REPORTE
--%%%%%%%%%%%%%%%%
SELECT * FROM #Tabla_Reporte