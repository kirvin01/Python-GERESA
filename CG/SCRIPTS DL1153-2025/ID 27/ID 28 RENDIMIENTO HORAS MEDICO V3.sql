--=============================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ********************
-- FICHA: INDICADOR N°00 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR:  (DGOS)
-- NOMBRE:  Rendimiento Hora Médico en el Primer Nivel de atención
--=============================================================================================

--*******************************************************************************************
-- Creado por		   : Erickson Javier Chuquimarca Bernal
-- Motivo              : Indicador Nuevo para el CG 2025
-- Fecha creación      : 13/12/2024
--*******************************************************************************************

USE [BD_BACKUP_OGEI]
--Base de renaes (Establecimientos de salud del primer nivel de atención )
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
select
	r.*
into #renaes
from BD_BACKUP_OGEI.dbo.Renaes r
where CAT_ESTAB in --FALTA: 'I-1','I-2'
('I-4','II-1','II-2','II-E','III-1','III-2','III-E') or r.est_nombre like 'CENTRO DE SALUD MENTAL COMUNITARIO%'

--EXTRAEMOS TODOS LOS DATOS  DE LOS MEDICOS QUE REGISTRAN ATENCIONES CON EL CUAL SE TRABAJARÁ
IF OBJECT_ID('Tempdb..#HIS_MINSA_EC') IS NOT NULL DROP TABLE #HIS_MINSA_EC
select  id_cita, 
cod_item , 
turno, 
aniomes, 
id_profesional, 
numdoc_pers, 
rownum, renaes,
fecha_registro,
id_tipcond_estab
into #HIS_MINSA_EC
from [BD_BACKUP_OGEI].[dbo].[TramaHisMinsa]
WHERE id_colegio='01' and 
APP IS NULL  
AND LEFT(cod_item,1)  IN  ('A', 	'B', 	'C', 	'D', 	'E', 	'F', 	'G', 	'H', 	'I', 	'J', 	'K', 	'L', 	
'M', 	'N', 	'O', 	'P', 	'Q', 	'R', 	'S', 	'T', 	'V', 	'W', 	'X', 	'Y')


--CAPTURAMOS LAS CITAS UNICAS (SIN INCLUIR DUPLICADAS)
IF OBJECT_ID('Tempdb..#TM_CITAS_UNICAS') IS NOT NULL DROP TABLE #TM_CITAS_UNICAS
SELECT distinct t.id_cita,
t.turno,
t.aniomes, 
t.id_profesional, 
t.numdoc_pers,
t.renaes,
id_tipcond_estab,
cast(t.fecha_registro as date) fecha_registro
INTO #TM_CITAS_UNICAS
FROM #HIS_MINSA_EC t


--IDENTIFICASMOS CITAS N, C Y R
IF OBJECT_ID('Tempdb..#TM_N_C_R') IS NOT NULL DROP TABLE #TM_N_C_R
SELECT DISTINCT *
INTO #TM_N_C_R
FROM (
	SELECT  
	(CONVERT(VARCHAR,renaes) COLLATE Latin1_General_CI_AS +numdoc_pers+aniomes) AS CODIGO,
	numdoc_pers, 
	aniomes,renaes,id_tipcond_estab
	FROM  #TM_CITAS_UNICAS T) AS SourceTable
    PIVOT (
	    count(id_tipcond_estab)  
		FOR id_tipcond_estab IN ([N], [C], [R])
) AS PivotTable;

--=============================TURNOS========================================
--IDENTIFICAMOS TODOS LOS TURNOS REALIZADOS EN EL PERIODO  POR EL PROFESIONAL
IF OBJECT_ID('Tempdb..#TURNOS_AÑO') IS NOT NULL DROP TABLE #TURNOS_AÑO
SELECT DISTINCT numdoc_pers, 
aniomes,
COUNT(TURNO) TURNOS_PERIDO,
renaes
INTO  #TURNOS_AÑO
FROM  
	(select DISTINCT numdoc_pers, 
	turno, 
	CAST ( fecha_registro AS DATE) AS FECHA_TURNO, 
	aniomes, 
	renaes
	from #TM_CITAS_UNICAS) AS Atenciones_Por_Profesional
GROUP BY numdoc_pers, aniomes,renaes

--============================ATENCIONES=======================================
--IDENTIFICAMOS LA CANTIDAD DE ATENCIONES REALIZADAS POR PROFESIONAL EN EL PERIDODO
IF OBJECT_ID('Tempdb..#ATENCIONES_X_PERIODO') IS NOT NULL DROP TABLE #ATENCIONES_X_PERIODO
SELECT DISTINCT numdoc_pers, 
aniomes,
COUNT(id_cita) ATENCIONES,
renaes
INTO #ATENCIONES_X_PERIODO
FROM #TM_CITAS_UNICAS
GROUP BY numdoc_pers, aniomes,renaes

--***************************************************
--					SINTAXIS
--***************************************************

declare @mes_eval	int,
		@mes_inicio int,
		@año		int

set @mes_inicio = 1		--<========== Mes de inicio
set @mes_eval	= 10	--<========== Mes de evaluación
set @año		= 2024  --<========== Año de evaluación 


--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

--REPORTE DE RESULTADOS PRELIMINAR DEL NUMERADOR
IF OBJECT_ID('Tempdb..#REPORTE_PRE_LIMINAR') IS NOT NULL DROP TABLE #REPORTE_PRE_LIMINAR

SELECT 
(CONVERT(VARCHAR,T.renaes) COLLATE Latin1_General_CI_AS +T.numdoc_pers+T.aniomes) AS CODIGO,
T.renaes,
T.numdoc_pers, 
T.aniomes,
LEFT(T.aniomes,4) ANIO, 
RIGHT(T.aniomes,2) MES, 
T.TURNOS_PERIDO,
	(SELECT P.ATENCIONES 
	FROM  #ATENCIONES_X_PERIODO P 
	WHERE P.aniomes=T.aniomes AND P.numdoc_pers=T.numdoc_pers  AND P.renaes=T.renaes) AS ATENCIONES_NUMERADOR
INTO  #REPORTE_PRE_LIMINAR
FROM #TURNOS_AÑO T
WHERE CAST (LEFT(T.aniomes,4) as int)	 = @año			AND
	  cast (RIGHT(T.aniomes,2) as int) BETWEEN @mes_inicio   AND @mes_eval	

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%

--=================TRAEMOS LA DATA DE TUA SALUD
IF OBJECT_ID('Tempdb..#TUA_SALUD') IS NOT NULL DROP TABLE #TUA_SALUD
SELECT ano,
mes,
[COdigo Unico],
Nombre,
[Tipo Documento],
[Numero Documento],
FECHA,
TURNO,
--=================PARA EL CASO DE SALIDA A LAS 00:00 HORAS SE ACTUALIZA A LAS 23:59 HORAS 
INGRESO,
	(case 
	when EGRESOS='00:00' then CONVERT(VARCHAR,'23:59')
	ELSE EGRESOS
	END )AS EGRESO,
	Servicio,
	Especialidad
INTO #TUA_SALUD
FROM [BD_HISINDICADORES].dbo.DGOS_DATA_TURNOS_VF_171224_E AS T

--=================CAULCULAMOS LAS HORAS TRABAJADAS POR DIA
IF OBJECT_ID('Tempdb..#TUA_SALUD_HORAS_X_DIA') IS NOT NULL DROP TABLE #TUA_SALUD_HORAS_X_DIA
SELECT *,
	(CASE
	WHEN  EGRESO='23:59' then (DATEDIFF(HOUR,CONVERT(TIME,INGRESO),CONVERT(TIME, EGRESO)))+1
	ELSE (DATEDIFF(HOUR,CONVERT(TIME,INGRESO),CONVERT(TIME, EGRESO)))
	END) AS Horas_Trabajadas
INTO #TUA_SALUD_HORAS_X_DIA
FROM #TUA_SALUD AS T

--=================SE AGUPAN POR DIA Y TURNO, SIN SUPERAR LAS 4 HORAS 
IF OBJECT_ID('Tempdb..#TUA_HORAS_EFECTIVAS') IS NOT NULL DROP TABLE #TUA_HORAS_EFECTIVAS
SELECT DISTINCT 
(E.[COdigo Unico]+E.[Numero Documento]+E.ANo+RIGHT(('00'+E.MES),2)) CODIGO,
E.ANo AS AÑO,E.Mes AS MES,E.[COdigo Unico] AS Renaes,E.[Numero Documento] as DNI,SUM(HORAS_EFECTIVAS) as h_efectivas
INTO #TUA_HORAS_EFECTIVAS
FROM (SELECT DISTINCT  H.ANo,H.Mes,H.[COdigo Unico],H.FECHA,H.TURNO,H.[Numero Documento],
						(CASE WHEN SUM(Horas_Trabajadas)>=4 THEN 4
						WHEN SUM(Horas_Trabajadas)<4 THEN SUM(Horas_Trabajadas)
						END) HORAS_EFECTIVAS
		FROM  #TUA_SALUD_HORAS_X_DIA H
		GROUP BY  H.ANo,H.Mes,H.[COdigo Unico],H.FECHA,H.TURNO,H.[Numero Documento])AS E
GROUP BY E.ANo,E.Mes,E.[COdigo Unico],E.[Numero Documento]

--=================VALIDAR QUE ESTE PROGRAMADO MINIMO TRES MESES EN TUA SALUD
--CAPTURAMOS LOS MESES ASIGNADOS
IF OBJECT_ID('Tempdb..#MESES_ASIGNADOS') IS NOT NULL DROP TABLE #MESES_ASIGNADOS
SELECT DISTINCT *
INTO #MESES_ASIGNADOS
FROM (
	SELECT DISTINCT T.[DNI], 
	T.[MES] 
	FROM  #TUA_HORAS_EFECTIVAS T) AS SourceTable
    PIVOT (
	    count([MES])  
		FOR [MES] IN ([1], [2], [3],[4], [5], [6],[7], [8], [9],[10], [11], [12])
) AS PivotTable;
--NOTA: LA FICHA NO ESPECIFICA MESES CONTINUOS O DISCONTINUOS, POR LO TANTO SE CONSIDERAN AMBOS CASOS

IF OBJECT_ID('Tempdb..#Horas_Efectivas_FINAL') IS NOT NULL DROP TABLE #Horas_Efectivas_FINAL
SELECT DISTINCT 
T.CODIGO,T.AÑO,T.MES,T.Renaes,T.DNI,T.h_efectivas,([1]+[2]+[3]+[4]+[5]+[6]+[7]+[8]+[9]+[10]+[11]+[12]) M_ASIGNADOS
into #Horas_Efectivas_FINAL
FROM #TUA_HORAS_EFECTIVAS T 
INNER JOIN #MESES_ASIGNADOS M ON T.DNI=M.DNI
WHERE ([1]+[2]+[3]+[4]+[5]+[6]+[7]+[8]+[9]+[10]+[11]+[12])>=3 



--===================
-- REPORTE
--===================
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
	a.renaes,
    b.DESC_ESTAB,
	A.numdoc_pers,
	a.ANIO,
	a.MES,
	a.TURNOS_PERIDO,
	x.N,
	x.C,
	x.R,
	a.ATENCIONES_NUMERADOR ATENCIO_NUM,
	H.h_efectivas  HRS_DEN

from #REPORTE_PRE_LIMINAR a
inner join #renaes b on CONVERT(int,a.renaes)=CONVERT(int,b.COD_ESTAB) 
INNER join #Horas_Efectivas_FINAL H  on a.CODIGO=H.CODIGO
INNER JOIN #TM_N_C_R x on a.CODIGO=x.CODIGO
ORDER BY Diris,Departamento,Provincia,Distrito

