--===========================================================================================
/* **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
FICHA: INDICADOR N°29 - DL 1153 - 2024
ÁREA RESPONSABLE DEL INDICADOR: Dirección General de Operaciones en Salud (DGOS)
NOMBRE: Rendimiento de Sala de Operaciones												   */
--===========================================================================================

--*******************************************************************************************
-- Creado por		   : Piero Romero Marín (OGEI)
-- Fecha creación      : 24/11/2023
/*==============================================================================*/

--Link descarga de datos:
--http://datos.susalud.gob.pe/dataset/consulta-h-produccion-asistencial-en-intervenciones-quirurgicas-de-las-ipress
use BD_HISINDICADORES
go

--***************************************************
--				BASES DE DATOS.
--***************************************************
--Base de renaes (Hospitales de II y III nivel de atención e Institutos Especializados)
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
select
	b.*
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes b WITH(NOLOCK)
where	converT(int,b.AMBITO) = 1 and			-- MINSA y GORE
		b.CAT_ESTAB in ('II-1','II-2','II-E','III-1','III-2','III-E') AND
		COD_ESTAB NOT IN (7734,7733,5948,6214)-- Excluir IPRESS según FICHA
						  

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
from BD_HISINDICADORES.dbo.TBL_ConsultaH_Intervenciones_Quirurgicas_202410 h WITH(NOLOCK)
INNER JOIN #renaes r ON h.CO_IPRESS = r.COD_ESTAB
where try_convert(int,de_programc) = 1		-- Cirugias Programadas.

-- Base de Sala de Operaciones: Proveído por Madelaine Sanchez - DGOS /
--								Actualizado por Nelly Ccorahua (dgos031@minsa.gob.pe) - DGOS
-- DROP TABLE DL1153_2024_ReporteSalaOperaciones
IF OBJECT_ID('Tempdb..#Salas_Operacion') IS NOT NULL DROP TABLE #Salas_Operacion
SELECT
	renaes				= CONVERT(INT,so.COD_RENIPRESS),
	Mes					= Mes,
	SOP_Op_Electivas	= so.[N° SOP operativas Electivas]
INTO #Salas_Operacion
FROM DL1153_2024_ReporteSalaOperaciones so WITH(NOLOCK)

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
año		int,
mes		int,
renaes	int,
Den		int,
Num		int
)

declare @mes_inicio int,
		@mes_eval	int,
		@año		int 

set @mes_inicio	= 1		--<=========== Mes de inicio		
set @mes_eval	= 10		--<=========== Mes de evaluación
set @año		= 2024	--<============= Año de evaluación 

while @mes_inicio <= @mes_eval
begin 

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
--= N° de Sala de Operaciones en condiciones operativas en un periodo
IF OBJECT_ID('Tempdb..#Tabla_Den') IS NOT NULL DROP TABLE #Tabla_Den
SELECT
	Año = @año,
	Mes = @mes_inicio,
	s.renaes,
	s.SOP_Op_Electivas
INTO #Tabla_Den
FROM #Salas_Operacion s
WHERE Mes = @mes_inicio

--%%%%%%%%%%%%%%%%
--  NUMERADOR
--%%%%%%%%%%%%%%%%
--= N° de intervenciones quirúrgicas ejecutadas en un periodo
IF OBJECT_ID('Tempdb..#Tabla_Num') IS NOT NULL DROP TABLE #Tabla_Num
SELECT
	t.Año,
	t.Mes,
	t.renaes,
	Total_Cirugias = SUM(Total_Cirugias)
INTO #Tabla_Num
FROM
	(SELECT
		Año				= @año,
		Mes				= @mes_inicio,
		c.renaes,
		Total_Cirugias	= c.total_cirugias_mayores + c.total_cirugias_menores
	FROM #Base_SetiIpress_Cirugias c
	WHERE c.mes = @mes_inicio ) t
GROUP BY	t.Año,
			t.Mes,
			t.renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
INSERT INTO #tabla_reporte
SELECT
	d.Año		,
	d.Mes		,
	d.renaes	,
	Den			= d.SOP_Op_Electivas	,
	Num			= ISNULL(n.Total_Cirugias,0)
FROM #Tabla_Den d
LEFT JOIN #Tabla_Num n ON	d.renaes = n.renaes AND
							d.Mes	 = n.Mes

SET @mes_inicio = @mes_inicio + 1
END

--===================
-- REPORTE
--===================
IF OBJECT_ID(N'dbo.DL1153_2024_CG29_RendimientoSalaOpe',N'U') IS NOT NULL
DROP TABLE DL1153_2024_CG29_RendimientoSalaOpe

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
INTO DL1153_2024_CG29_RendimientoSalaOpe
from #tabla_reporte a
inner join #renaes b on convert(int,a.renaes)=convert(int,b.COD_ESTAB)

SELECT * FROM DL1153_2024_CG29_RendimientoSalaOpe