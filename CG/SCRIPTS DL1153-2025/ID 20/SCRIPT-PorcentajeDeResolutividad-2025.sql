--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°25 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: Dirección General de Operaciones en Salud (DGOS)
-- NOMBRE: Porcentaje de Resolutividad
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 25/10/2023
-- Motivo              : Revisar el código para el 2024
-- Modificado por	   : Piero Romero Marín (OGEI)
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 29/11/2024
-- Motivo              : Revisar el código para el 2025
-- Modificado por	   : Piero Romero Marín (OGEI)
--*******************************************************************************************
use BD_HISINDICADORES
go

--***************************************************
--				BASES DE DATOS.
--***************************************************
--Base de renaes (Hospital de II nivel con más de 50 camas, hospital general de III nivel, hospital e instituto especializado)
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
select
	b.*
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes b
where	converT(int,AMBITO) = 1 and			-- MINSA y GORE
		CAT_ESTAB in ('II-1','II-2','II-E','III-1','III-2','III-E')	

--Base EMERGENCIAS
IF OBJECT_ID('Tempdb..#seem') IS NOT NULL DROP TABLE #seem
select
	renaes			= converT(int,a.renipress)		,
	Categoria		= b.CAT_ESTAB					,
	fecha_aten		= try_convert(date,a.fecate)	,
	fecha_egreso	= try_convert(date,a.fecegr)	,
	Prioridad		= CONVERT(INT,a.PRIORIDAD)		,
	emergencia		= 1
into #seem
from BD_BACKUP_OGEI.dbo.TramaEmergencias a
inner join #renaes b on convert(int,a.renipress) = convert(int,b.cod_Estab)
WHERE a.PRIORIDAD IN ('1','2')
 
--Base de REFCON
IF OBJECT_ID('Tempdb..#refcon') IS NOT NULL DROP TABLE #refcon
select
	renaes			= convert(int,cod_unico)	,
	Categoria		= b.CAT_ESTAB				,
	id_referencia	,
	fecha_envio		= convert(date,fecha_envio)	,
	ups_destino
into #refcon		
from BD_BACKUP_OGEI.dbo.TramaREFCON a
inner join #renaes b on convert(int,a.cod_unico) = convert(int,b.cod_Estab)		-- Establecimiento de Origen
where	tipo_traslado = 'REFERENCIA'	AND
		ups_destino like '%emergencia%' AND			-- Referencias Realizado por EESS y son enviados por emergencia.
		convert(date,fecha_envio) is not null

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

declare @mes_inicio int,
		@mes_eval	int,
		@año		int 

set @mes_inicio = 1		--<=========== Mes de inicio		
set @mes_eval	= 10		--<=========== Mes de evaluación
set @año		= 2024  --<============= Año de evaluación 

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%

-- 1. NIVEL III:
--N° de atenciones de emergencia en el período
IF OBJECT_ID('Tempdb..#Ind_Den') IS NOT NULL DROP TABLE #Ind_Den
select
	s.renaes,
	den = sum(s.emergencia)
into #Ind_Den
from #seem s
where	year(fecha_aten)  = @año		AND
		month(fecha_aten) = @mes_inicio	AND
		s.PRIORIDAD IN ('1','2')
group by renaes 

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

-- 1. NIVEL III:
--N° Referencias para emergencia enviadas en un periodo
	IF OBJECT_ID('Tempdb..#Ind_Num') IS NOT NULL DROP TABLE #Ind_Num
	select
		r.renaes,
		num_emergencia = count(distinct r.id_referencia)
	into #Ind_Num
	from #refcon r
	where	r.ups_destino like '%emergencia%'	AND
			year(r.fecha_envio)  = @año			AND
			month(r.fecha_envio) = @mes_inicio	
   group by renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #tabla_reporte
select
	a.renaes,
	año		= @año		,
	mes		= @mes_inicio	,
	a.den	,
	num		= isnull(b.num_emergencia,0)
from #Ind_Den a
left join #Ind_Num b on a.renaes = b.renaes

set @mes_inicio = @mes_inicio + 1
end 

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador

select
	Diris			= B.diris,
	Departamento	=	CASE
							WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV  = 'LIMA' THEN 'LIMA METROPOLITANA'
							WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
							ELSE B.DESC_DPTO
						END,
	Provincia		= B.desc_prov,
	Distrito		= B.desc_dist,
	Red				=	case
							when b.DIRIS like '%diris%' then b.diris
							else B.DESC_RED
						end,
	MicroRed		= b.desc_mred,
	b.cat_estab		,
	eess_nombre		= b.desc_estab,
	a.*
INTO #Reporte_2025
from #tabla_reporte a
inner join #renaes b on convert(int,a.renaes) = convert(int,b.COD_ESTAB)

-- Exportamos el reporte final agrupado
select * from #Reporte_2025
