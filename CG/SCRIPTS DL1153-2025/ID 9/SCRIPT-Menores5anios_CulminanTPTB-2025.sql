--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Dirección de Prevención y Control de la
--								   Tuberculosis (DPCTB)
-- NOMBRE: Porcentaje de contactos menores de 5 años de edad que culminan Terapia Preventiva
--		   para TB (TPTB)
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marin (OGEI)
-- Fecha creación      : 06/11/2023
---------------------------------------------------------------------------------------------
-- Modificado por	   : Piero Romero Marin (OGEI)
-- Fecha modificación  : 10/12/2024
-- Motivo Modificación : Actualización para indicadores del 2025
--*******************************************************************************************

--***************************************************
--				BASES DE DATOS.
--***************************************************
-- SIGTB: Trama de datos proporcionado por la Lic. Karla Guerra Motta - DPCTB / DGIESP
-- DROP TABLE BD_HISINDICADORES.dbo.DL1153_2024_MenoresCulminanTPTB
USE BD_HISINDICADORES

Set DateFormat DMY

UPDATE t SET
	t.Fecha_Termino_TPTB = NULL
FROM BD_HISINDICADORES.dbo.DL1153_2024_MenoresCulminanTPTB t
WHERE t.Fecha_Termino_TPTB = ''

IF OBJECT_ID('Tempdb..#TPTB') IS NOT NULL DROP TABLE #TPTB
SELECT
	renaes = t.Renaes,
	Fecha_Dx = CONVERT(DATE,t.Fecha_Diagnostico),
	Edad = CONVERT(INT,t.Edad),
	TerminoTPTB = CONVERT(INT,t.TerminoTPTB),
	Fecha_Termino_TPTB = CONVERT(DATE,t.Fecha_Termino_TPTB)
INTO #TPTB
FROM BD_HISINDICADORES.dbo.DL1153_2024_MenoresCulminanTPTB t

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
renaes int,
año int,
mes int,
den int,
num int,
) 

declare @mes_eval	int,
		@mes_inicio int,
		@año		int

set @mes_inicio = 1		--<========== Mes de inicio
set @mes_eval	= 9		--<========== Mes de evaluación
set @año		= 2024  --<========== Año de evaluación 

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- DENOMINADOR
--%%%%%%%%%%%%%%%%
-- Número total de personas afectadas de casos de TB sensible registrados en el SIG TB
IF OBJECT_ID('Tempdb..#Tabla_Den') IS NOT NULL DROP TABLE #Tabla_Den
SELECT
	t.renaes,
	Den = COUNT(*)
INTO #Tabla_Den
FROM #TPTB t
WHERE MONTH(t.Fecha_Dx) <= @mes_inicio
GROUP BY t.renaes 

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
--Total de personas afectadas por TB sensible incluidos en el denominador con resultado de 
--tratamiento: curado + tratamiento completo registrados en el SIG TB
IF OBJECT_ID('Tempdb..#Tabla_Num') IS NOT NULL DROP TABLE #Tabla_Num
SELECT
	t.renaes,
	Num = COUNT(*)
INTO #Tabla_Num
FROM #TPTB t
WHERE	t.TerminoTPTB = 1 AND
		MONTH(t.Fecha_Termino_TPTB) <= @mes_inicio
GROUP BY t.renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
INSERT INTO #tabla_reporte
SELECT
	d.renaes,
	año = @año,
	mes = @mes_inicio,
	d.Den,
	Num = ISNULL(n.Num,0)
FROM #Tabla_Den d
LEFT JOIN #Tabla_Num n ON d.renaes = n.renaes

set @mes_inicio = @mes_inicio + 1
end

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
IF OBJECT_ID (N'dbo.DL1153_2024_CG09_CulminanTPTB', N'U') IS NOT NULL  
DROP TABLE dbo.DL1153_2024_CG09_CulminanTPTB;
GO

select
	Diris		 = B.diris,
	Departamento = CASE 
				 		WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
				 		WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
				 		ELSE B.DESC_DPTO
				   END,
	Provincia	 = B.desc_prov,
	Distrito	 = B.desc_dist,
	Red			 = case
				   	when b.DIRIS like '%diris%' then b.diris
				   	else B.DESC_RED
				   end,
	MicroRed	 = b.desc_mred,
	b.cat_estab	 ,
	eess_nombre	 = b.desc_estab,
	a.*
INTO dbo.DL1153_2024_CG09_CulminanTPTB
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes) = convert(int,b.COD_ESTAB)
where	b.AMBITO = 1 AND
		(b.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') OR
		(b.CAT_ESTAB IN ('II-1','II-2','II-E') AND CAMAS < 50)
		)

-- Exportamos el reporte final agrupado
SELECT * FROM dbo.DL1153_2024_CG09_CulminanTPTB



