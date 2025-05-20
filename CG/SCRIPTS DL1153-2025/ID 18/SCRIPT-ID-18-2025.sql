--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Dirección de Salud Mental (DSAME)
-- NOMBRE: Rendimiento cama en Unidades de Hospitalización de Salud Mental y Adicciones
--		   (UHSMA) en hospitales
--===========================================================================================

use BD_HISINDICADORES
go

--***************************************************
--				BASES DE DATOS.
--***************************************************
--Base Estadistica de Egresos y Permanencia
IF OBJECT_ID('Tempdb..#Base_Egresos') IS NOT NULL DROP TABLE #Base_Egresos
select
	renaes			= try_convert(int,a.RENIPRESS)	,
	fecha_ingreso	= try_convert(date,a.FECING)	,
	fecha_egreso	= try_convert(date,a.FECEGR)	,
	total_estancia	= a.TOTALEST					,
	--a.ups			,
	--ups_descrip		= b.DESCRIP_UPS02				,
	a.condicion		,
	a.CODDIAG1 dx1	,
	a.CODDIAG2 dx2	,
	a.CODDIAG3 dx3	,
	a.CODDIAG4 dx4	
into #Base_Egresos
from BD_BACKUP_OGEI.dbo.TramaEgresos (nolock) a
where	a.CODDIAG1 is not null and a.CODDIAG1 not in ('','NULL') and
		try_convert(date,a.FECING) is not null and
		try_convert(date,a.FECEGR) is not null and
		try_convert(date,a.FECEGR) >= try_convert(date,a.FECING) AND
		(
			(SUBSTRING(a.CODDIAG1,1,1) = 'F'								OR
			 a.CODDIAG1 = 'T740'											OR
			 SUBSTRING(a.CODDIAG1,1,2) IN ('X6','X7')						OR
			 SUBSTRING(a.CODDIAG1,1,3) IN ('X80','X81','X82','X83','X84')	OR
			 SUBSTRING(a.CODDIAG1,1,3) IN ('Y04','Y05','Y06','Y07','Y08')	
			 )	
			OR
			(SUBSTRING(a.CODDIAG2,1,1) = 'F'								OR
			 a.CODDIAG2 = 'T740'											OR
			 SUBSTRING(a.CODDIAG2,1,2) IN ('X6','X7')						OR
			 SUBSTRING(a.CODDIAG2,1,3) IN ('X80','X81','X82','X83','X84')	OR
			 SUBSTRING(a.CODDIAG2,1,3) IN ('Y04','Y05','Y06','Y07','Y08')
			 )	
		 )

-- Directorio de Hospitales Generales con camas para Salud Mental
IF OBJECT_ID('Tempdb..#Denominador') IS NOT NULL DROP TABLE #Denominador
SELECT
	c.*
INTO #Denominador
FROM BD_HISINDICADORES.dbo.CamasSaludMental_2025 c


--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(	renaes	int,
	año		int,
	mes		int,
	den		int,
	num		int
)

declare @mes_inicio int,
		@mes_eval	int,
		@año		int

set @mes_inicio	= 1		--<===== Mes de inicio		
set @mes_eval	= 10		--<===== Mes de evaluación
set @año		= 2024	--<===== Año de evaluacin 

while @mes_inicio <= @mes_eval
begin 


--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
-- N° de camas disponibles para la atención de salud mental en la UHSMA durante el año 2024

-- Directorio de Hospitales Generales con camas para Salud Mental
--SELECT
--	c.*
--INTO #Denominador
--FROM BD_HISINDICADORES.dbo.CamasSaludMental c


--%%%%%%%%%%%%%%%%
--  NUMERADOR	--
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Numerador') IS NOT NULL DROP TABLE #Numerador
SELECT
	b.renaes,
	Num = COUNT(1)
INTO #Numerador
FROM #Base_Egresos b
WHERE MONTH(b.fecha_egreso) <= @mes_inicio
GROUP BY	b.renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #tabla_reporte
select
	c.renaes,
	Año = @año,
	Mes = @mes_inicio,
	den = c.CantCamas,
	num = ISNULL(n.Num,0)
from #Denominador c
LEFT JOIN #Numerador n on n.renaes = c.renaes

DROP TABLE #Numerador

set @mes_inicio = @mes_inicio + 1
end

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador

select 
	Diris = B.diris,
	Departamento =	CASE 
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV  = 'LIMA' THEN 'LIMA METROPOLITANA'
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
						ELSE B.DESC_DPTO
					END,
	Provincia	= B.desc_prov,
	Distrito	= B.desc_dist,
	Red			= B.desc_red,
	MicroRed	= b.desc_mred,
	b.cat_estab	,
	eess_nombre = b.desc_estab,
	a.*
INTO #Reporte_2025
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes)=convert(int,b.COD_ESTAB)

-- Exportamos el reporte final agrupado
SELECT * FROM #Reporte_2025