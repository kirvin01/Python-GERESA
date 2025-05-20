--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N° - DL 1153 - 2025
-- NOMBRE: Establecimientos de Salud que aseguran los equipos críticos para los Programas 
-- Presupuestales Seleccionados
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marín (OGEI)
-- Fecha creación      : 27/11/2023
--*******************************************************************************************

USE BD_HISINDICADORES
GO

--***************************************************
--				BASES DE DATOS.
--***************************************************
-- RENAES (Solo EESS del 1er Nivel)
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
SELECT
	r.*
INTO #renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r
WHERE	r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') AND
		r.AMBITO	= 1							 AND
		r.SW_ESTADO = 1

-- SIGA-MP (Proporcionado por DIEM/DGOS: Rene Vasquez - rvasquezs@minsa.gob.pe)
-- Rene Vasquez actualizó la tabla para el 2025
IF OBJECT_ID('Tempdb..#SIGA_MP') IS NOT NULL DROP TABLE #SIGA_MP
SELECT
	renaes		= r.COD_ESTAB,
	s.Pliego	,
	Desc_Pliego = s.NMB_PLIEGO,
	s.Producto
INTO #SIGA_MP
FROM [DL1153_2025_DIEM_Resumen_EquiposCriticos_SIGA] s WITH(NOLOCK) -- 523 355
INNER JOIN #renaes									 r WITH(NOLOCK) ON CONVERT(INT,s.cod_oGEI) = CONVERT(INT,r.cod_estab)
WHERE s.Cond = 'Si' AND
		s.Producto <> ''

--***************************************************
--					SINTAXIS
--***************************************************


DECLARE
	@mes_eval	INT,
	@año		INT 

SET @mes_eval = 10		--<======= Mes de evaluación
SET @año	  = 2024	--<======= Año de evaluación 

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#CantidadEquiposCriticos') IS NOT NULL DROP TABLE #CantidadEquiposCriticos
SELECT
	t.renaes,
	P_CF1 = ISNULL(t.[CF1],0),
	P_CF2 = ISNULL(t.[CF2],0),
	P_CF3 = ISNULL(t.[CF3],0),
	P_CF4 = ISNULL(t.[CF4],0),
	P_CR1 = ISNULL(t.[CR1],0),
	P_CR3 = ISNULL(t.[CR3],0),
	P_CR4 = ISNULL(t.[CR4],0),
	P_CR5 = ISNULL(t.[CR5],0),
	P_CR6 = ISNULL(t.[CR6],0),
	P_PR1 = ISNULL(t.[PR1],0),
	P_PR2 = ISNULL(t.[PR2],0),
	P_PR3 = ISNULL(t.[PR3],0),
	P_PR4 = ISNULL(t.[PR4],0),
	P_PR5 = ISNULL(t.[PR5],0),
	P_PR7 = ISNULL(t.[PR7],0),
	P_PR8 = ISNULL(t.[PR8],0),
	P_PR9 = ISNULL(t.[PR9],0),
	Hmg	  = IIF(t.[CR5] > 0,1,0)
INTO #CantidadEquiposCriticos
FROM
	(SELECT
		s.renaes	,
		s.Producto	,
		Cantidad	= COUNT(*) 
	FROM #SIGA_MP s
	GROUP BY s.renaes	,
			 s.Producto	) t
	PIVOT (SUM(Cantidad)
	FOR Producto IN ([CF1],[CF2],[CF3],[CF4],[CR1],[CR3],[CR4],[CR5],[CR6],[PR1],[PR2],[PR3],[PR4],[PR5],[PR7],[PR8],[PR9])) t

IF OBJECT_ID('Tempdb..#ResultadoEquiposCriticos') IS NOT NULL DROP TABLE #ResultadoEquiposCriticos
SELECT
	e.renaes						,
	r.CAT_ESTAB						,
	e.Hmg							,
	CF1		= CONVERT(FLOAT,IIF(e.P_CF1 > 1,1,0)),
	CF2		= IIF(e.P_CF2 > 1,1,0)	,
	CF3		= IIF(e.P_CF3 > 1,1,0)	,
	CF4		= IIF(e.P_CF4 > 1,1,0)	,
	CR1		= IIF(e.P_CR1 > 1,1,0)	,
	CR2		= IIF(e.P_PR2 > 1,1,0)	,
	CR3		= IIF(e.P_CR3 > 1,1,0)	,
	CR4		= IIF(e.P_CR4 > 1,1,0)	,
	CR5		= IIF(e.P_CR5 > 1,1,0)	,
	CR6		= IIF(e.P_CR6 > 1,1,0)	,
	PR1		= IIF(e.P_PR1 > 1,1,0)	,
	PR2		= IIF(e.P_PR2 > 1,1,0)	,
	PR3		= IIF(e.P_PR3 > 1,1,0)	,
	PR4		= IIF(e.P_PR4 > 1,1,0)	,
	PR5		= IIF(e.P_PR5 > 1,1,0)	,
	PR6		= IIF(e.P_CR3 > 1,1,0)	,
	PR7		= IIF(e.P_PR7 > 1,1,0)	,
	PR8		= IIF(e.P_PR8 > 1,1,0)	,
	PR9		= IIF(e.P_PR9 > 1,1,0)
INTO #ResultadoEquiposCriticos
FROM #CantidadEquiposCriticos	e 
INNER JOIN #renaes				r ON e.renaes = r.COD_ESTAB

IF OBJECT_ID('Tempdb..#IndicadorEquiposCriticos') IS NOT NULL DROP TABLE #IndicadorEquiposCriticos
SELECT
	t.*,
	Den = 1,
	Num = IIF(t.Hmg > 0 AND t.Total >= 0.80,1,0)
INTO #IndicadorEquiposCriticos
FROM
(SELECT
	r.*,
	Total =	CASE
				WHEN r.CAT_ESTAB IN ('I-1','I-2') THEN  ROUND(((r.CF1 + r.CF2 + r.CF3 + r.CR1 + r.CR2 + r.CR3 + r.CR4 + r.CR5 +
														 r.CR6 + r.PR1 + r.PR2 + r.PR3 + r.PR4 + r.PR5 + r.PR6 + r.PR8) / 16),2)
				WHEN r.CAT_ESTAB IN ('I-3','I-4') THEN  ROUND(((r.CF1 + r.CF2 + r.CF3 + r.CF4 + r.CR1 + r.CR2 + r.CR3 + r.CR4 + r.CR5 + r.CR6 +
														 r.PR1 + r.PR2 + r.PR3 + r.PR4 + r.PR5 + r.PR6 + r.PR7 + r.PR8 + r.PR9) / 19),2)
			END
FROM #ResultadoEquiposCriticos r) t

--===================
-- REPORTE
--===================
SELECT
	s.Pliego		,
	s.Desc_Pliego	,
	r.DESC_RED,
	r.DESC_ESTAB	,
	i.*
FROM #IndicadorEquiposCriticos	i -- 7 475
INNER JOIN #renaes				r ON i.renaes = r.COD_ESTAB
LEFT JOIN (SELECT
				DISTINCT
				s.renaes,
				s.Pliego,
				s.Desc_Pliego
			FROM #SIGA_MP s)	s ON i.renaes = s.renaes 




