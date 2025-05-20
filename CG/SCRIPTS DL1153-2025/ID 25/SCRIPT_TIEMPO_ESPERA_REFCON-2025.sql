--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°30 - DL 1153 - 2024
-- ÁREA RESPONSABLE DEL INDICADOR: Dirección General de Operaciones en Salud (DGOS)
-- NOMBRE: Promedio de Espera para la Atención en Consulta Externa de un Paciente Referido
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marín (OGEI)
-- Fecha creación      : 07/11/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 02/12/2024
-- Motivo              : Revisar el código para el 2025
-- Modificado por	   : Deyvis Mego (OGEI)
--*******************************************************************************************
USE BD_HISINDICADORES

--Base de renaes (Establecimientos de Salud nivel I (I-4) y II, Hospitales e Institutos especializados)
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
select
	r.*
	--t.Nivel
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes r
--INNER JOIN dbo.DL1153_2024_IPRESS_Nivel_TiempoEsperaREFCON t ON r.COD_ESTAB = t.COD_ESTAB
where CAT_ESTAB in ('I-2','I-3','I-4','II-1','II-2','II-E','III-1','III-2','III-E')

--Base de REFCON 2023 Y 2022
IF OBJECT_ID('Tempdb..#refcon') IS NOT NULL DROP TABLE #refcon
select
	DISTINCT
	renaes			= convert(int,cod_unico_d),
	id_referencia	,
	fecha_envio		= convert(date,fecha_envio),
	ups_destino		,
	Fecha_aceptacion,
	Fecha_Paciente_recibido = a.fecha_pac_recibido,
	LLEGADA
into #refcon
from [BD_BACKUP_OGEI].[dbo].TramaREFCON a
inner join #renaes b on convert(int,a.cod_unico_d) = convert(int,b.cod_Estab)		-- Establecimiento de Origen
where	tipo_traslado = 'REFERENCIA'			and
		ups_destino like '%CONSULTA%EXTERNA%'	AND			-- Referencias Realizado por EESS y son enviados por emergencia.
		convert(date,fecha_envio) is not null	AND
		desc_estado = 'PACIENTE RECIBIDO'		AND
		YEAR(a.fecha_pac_recibido) = 2024		AND
		YEAR(a.Fecha_aceptacion)  >= 2023		AND
		LLEGADA = 'S'

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
renaes	int,
año		int,
mes		int,
den		int,
num		int
)

declare @mes_eval	int,
		@mes_inicio int,
		@año		int

set @mes_inicio = 1		--<========== Mes de inicio
set @mes_eval	= 10	--<========== Mes de evaluación
set @año		= 2024  --<========== Año de evaluación 

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Tabla_Den') IS NOT NULL DROP TABLE #Tabla_Den
SELECT
	r.renaes,
	Cantidad = COUNT(*)
INTO #Tabla_Den
FROM #refcon r
WHERE	YEAR(r.Fecha_Paciente_recibido)	 = @año			AND
		MONTH(r.Fecha_Paciente_recibido) = @mes_inicio	
GROUP BY	r.renaes
ORDER BY 1, 2 ASC

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Tabla_Num') IS NOT NULL DROP TABLE #Tabla_Num
SELECT
	t.renaes,
	Dias_Espera = SUM(IIF((Cantidad - Domingos) < 0, 0, Cantidad - Domingos))
INTO #Tabla_Num
FROM
(SELECT
	renaes,
	Cantidad = DATEDIFF(DD,r.fecha_aceptacion,r.Fecha_Paciente_recibido),
	Domingos = (DATEDIFF(DAY,r.fecha_aceptacion,r.Fecha_Paciente_recibido) - DATEPART(dw,r.Fecha_Paciente_recibido)+8)/7

FROM #refcon r
WHERE	YEAR(r.Fecha_Paciente_recibido)	 = @año			AND
		MONTH(r.Fecha_Paciente_recibido) = @mes_inicio) t
GROUP BY	t.renaes
ORDER BY 1, 2 ASC

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
INSERT INTO #Tabla_Reporte
SELECT
	d.renaes,
	Año = @año,
	Mes = @mes_inicio,
	Den = d.Cantidad,
	Num = n.Dias_Espera
FROM #Tabla_Den d
INNER JOIN #Tabla_Num n ON n.renaes = d.renaes

SET @mes_inicio = @mes_inicio + 1
END

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
--IF OBJECT_ID(N'dbo.DL1153_2024_CG30_EsperaAtenPacRef',N'U') IS NOT NULL
--DROP TABLE DL1153_2024_CG30_EsperaAtenPacRef
IF OBJECT_ID('Tempdb..#REPORTE_FINAL') IS NOT NULL DROP TABLE #REPORTE_FINAL
select
	Diris		 = B.diris,
	Departamento =	CASE
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
						ELSE B.DESC_DPTO
					END,
	Provincia	 = B.desc_prov,
	Distrito	 = B.desc_dist,
	b.DESC_RED	,
	b.Ubigeo	,
	b.CAT_ESTAB	,
	b.DESC_ESTAB,
	a.*
INTO #REPORTE_FINAL
--INTO DL1153_2024_CG30_EsperaAtenPacRef
from #tabla_reporte a
inner join #renaes b on CONVERT(int,a.renaes)=CONVERT(int,b.COD_ESTAB) 

-- Exportamos el reporte final agrupado
--SELECT * FROM DL1153_2024_CG30_EsperaAtenPacRef

SELECT * FROM #REPORTE_FINAL