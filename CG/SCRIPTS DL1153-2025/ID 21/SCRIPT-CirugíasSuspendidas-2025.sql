--===========================================================================================
/* **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
FICHA: INDICADOR N° - DL 1153 - 2024
-- ÁREA RESPONSABLE DEL INDICADOR: Dirección General de Operaciones en Salud (DGOS)
NOMBRE: Porcentaje de Cirugías Suspendidas												   */
--===========================================================================================

--*******************************************************************************************
-- Creado por (2023)   : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 25/10/2023
-- Motivo              : Modificaciones en la FICHA para los Indicadores del 2024
-- Modificado por	   : Piero Romero Marín (OGEI)
--*******************************************************************************************
/*==============================================================================*/

--Link descarga de datos:
--http://datos.susalud.gob.pe/dataset/consulta-h-produccion-asistencial-en-intervenciones-quirurgicas-de-las-ipress

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
		CAT_ESTAB in ('II-1','II-2','II-E','III-1','III-2','III-E') AND	
		COD_ESTAB NOT IN (6213,6212,7734,7733,5948,6214) -- Excluir IPRESS según FICHA
						  

--Base de Cirugias 
IF OBJECT_ID('Tempdb..#Base_SetiIpress_Cirugias') IS NOT NULL DROP TABLE #Base_SetiIpress_Cirugias
select
	renaes						= convert(int,h.co_ipress),
	año							= convert(int,h.anho)		,
	mes							= convert(int,h.mes)		,
	total_cirugias_mayores		= isnull(try_convert(int,h.total_ciruj_may),0),
	total_cirugias_menores		= isnull(try_convert(int,h.total_ciruj_men),0),
	total_cirugias_suspendidas	= isnull(try_convert(int,h.ciruj_suspend),0)
into #Base_SetiIpress_Cirugias
from BD_HISINDICADORES.dbo.TBL_ConsultaH_Intervenciones_Quirurgicas_202410 h
INNER JOIN #renaes r ON h.CO_IPRESS = r.COD_ESTAB
where try_convert(int,de_programc) = 1								-- Cirugias Programadas.
	  

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
año int,
mes int,
renaes int,
total_cirugias_programadas int,
total_cirugias_suspendidas int
)

declare @mes_inicio int,
		@mes_eval	int,
		@año		int 

set @mes_inicio	= 1 --<=========== Mes de inicio		
set @mes_eval	= 10 --<=========== Mes de evaluación
set @año		= 2024 --<============= Año de evaluación 

while @mes_inicio <= @mes_eval
begin 

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
--1. - El número de intervenciones quirúrgicas programadas en el mismo período. Una intervención quirúrgica se define como la intervención quirúrgica 
--     planificada con antelación y que no necesita practicarse inmediatamente como la cirugía de emergencia.
IF OBJECT_ID('Tempdb..#Base_SetiIpress_den') IS NOT NULL DROP TABLE #Base_SetiIpress_den
select
	renaes,
	total_cirugias_programadas = sum(s.total_cirugias_menores		+
									 s.total_cirugias_mayores		+
									 s.total_cirugias_suspendidas	)
into #Base_SetiIpress_den
from #Base_SetiIpress_Cirugias s
where	año = @año and
		mes = @mes_inicio and
		(total_cirugias_menores		+ 
		 total_cirugias_mayores		+ 
		 total_cirugias_suspendidas	) > 0
group by renaes

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
--1.El número total de intervenciones quirúrgicas programadas que fueron suspendidas por diferentes motivos en un periodo de tiempo. 
IF OBJECT_ID('Tempdb..#Base_SetiIpress_num') IS NOT NULL DROP TABLE #Base_SetiIpress_num
select
	renaes,
	total_cirugias_suspendidas = sum(total_cirugias_suspendidas)
into #Base_SetiIpress_num
from #Base_SetiIpress_Cirugias
where	año = @año and
		mes = @mes_inicio and
		total_cirugias_suspendidas > 0
group by renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #tabla_reporte
select
	año = @año					,
	mes = @mes_inicio			,
	a.renaes					,
	a.total_cirugias_programadas,
	total_cirugias_suspendidas = isnull(b.total_cirugias_suspendidas,0)
from #Base_SetiIpress_den a
left join #Base_SetiIpress_num b on a.renaes = b.renaes

drop table #Base_SetiIpress_den
drop table #Base_SetiIpress_num

set @mes_inicio=@mes_inicio+1
end 

--===================
-- REPORTE
--===================
IF OBJECT_ID(N'dbo.DL1153_2024_CG28_CirugiasSupendidas',N'U') IS NOT NULL
DROP TABLE DL1153_2024_CG28_CirugiasSupendidas
select 
	B.diris Diris,
	CASE 
	WHEN B.DESC_DPTO='LIMA' AND B.DESC_PROV='LIMA' THEN 'LIMA METROPOLITANA'
	WHEN B.DESC_DPTO='LIMA' AND B.DESC_PROV<>'LIMA' THEN 'LIMA PROVINCIAS'
	ELSE B.DESC_DPTO END Departamento,
	B.desc_prov Provincia,
	B.desc_dist Distrito,
	case when b.DIRIS like '%diris%' then b.diris else B.DESC_RED end Red,
	b.desc_mred MicroRed,
	b.cat_estab,
	b.desc_estab eess_nombre,
	a.*
INTO BD_HISINDICADORES.dbo.DL1153_2024_CG28_CirugiasSupendidas
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes)=convert(int,b.COD_ESTAB)


select * from DL1153_2024_CG28_CirugiasSupendidas

drop table #Base_SetiIpress_Cirugias
drop table #tabla_reporte
-- =D

