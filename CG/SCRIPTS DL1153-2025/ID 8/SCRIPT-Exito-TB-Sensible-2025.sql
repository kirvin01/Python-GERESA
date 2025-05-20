--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Dirección de Prevención y Control de la
--								   Tuberculosis (DPCTB)
-- NOMBRE: Tasa de éxito de tratamiento para TB Sensible
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marin (OGEI)
-- Fecha creación      : 06/11/2023
---------------------------------------------------------------------------------------------
-- Modificado por	   : Piero Romero Marin (OGEI)
-- Fecha modificación  : 10/12/2024
-- Motivo Modificación : Actualización para indicadores del 2025
--*******************************************************************************************
USE BD_HISINDICADORES
GO

Set DateFormat DMY

--***************************************************
--				BASES DE DATOS.
--***************************************************
-- SIGTB: Trama de datos proporcionado por la Lic. Karla Guerra Motta - DPCTB / DGIESP
-- DROP TABLE BD_HISINDICADORES.dbo.DL1153_2024_ExitoTBS


-- Tabla de éxito TB del 2023: Este tabla solo se usa para calcular la simulación, para el 2025 no se debe de usar.
-- Tabla éxito TB del 2023:
UPDATE t SET
	t.Fecha_Resultado_tto = NULL
FROM BD_HISINDICADORES.dbo.DL1153_2023_ExitoTBS t
WHERE t.Fecha_Resultado_tto = ''

-- Tabla éxito TB del 2024:
UPDATE t SET
	t.Fecha_Resultado_tto = NULL
FROM BD_HISINDICADORES.dbo.DL1153_2024_ExitoTBS t
WHERE t.Fecha_Resultado_tto = ''

IF OBJECT_ID('Tempdb..#TBS') IS NOT NULL DROP TABLE #TBS
SELECT
	renaes				= t.[Cod_Renaes],
	Fecha_Dx			= CONVERT(DATE,t.[Fecha de Diagnostico de TB]),
	Fecha_Tto			= CONVERT(DATE,t.[Fecha de inicio de tratamiento]),
	Resultado_Tto		= [Resultado de tratamiento],
	Fecha_Resultado_Tto = CONVERT(DATE,t.Fecha_Resultado_tto)
INTO #TBS
FROM BD_HISINDICADORES.dbo.DL1153_2024_ExitoTBS t
UNION
SELECT
	renaes				= t.[Cod_Renaes],
	Fecha_Dx			= CONVERT(DATE,t.[Fecha de Diagnostico de TB]),
	Fecha_Tto			= CONVERT(DATE,t.[Fecha de inicio de tratamiento]),
	Resultado_Tto		= [Resultado de tratamiento],
	Fecha_Resultado_Tto = CONVERT(DATE,t.Fecha_Resultado_tto)
FROM BD_HISINDICADORES.dbo.DL1153_2023_ExitoTBS t

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
set @mes_eval	= 10		--<========== Mes de evaluación
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
FROM #TBS t
WHERE MONTH(t.Fecha_Dx) <= 6
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
FROM #TBS t
WHERE	t.Resultado_Tto IN ('Curado','Tratamiento completo') AND
		MONTH(t.Fecha_Resultado_Tto) <= @mes_inicio
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
IF OBJECT_ID (N'dbo.DL1153_2024_CG08_ExitoTBS', N'U') IS NOT NULL  
DROP TABLE dbo.DL1153_2024_CG08_ExitoTBS;
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
INTO dbo.DL1153_2024_CG08_ExitoTBS
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes) = convert(int,b.COD_ESTAB)
where	b.AMBITO = 1 AND
		(b.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') OR
		(b.CAT_ESTAB IN ('II-1','II-2','II-E') AND CAMAS < 50)
		)

-- Exportamos el reporte final agrupado
SELECT * FROM dbo.DL1153_2024_CG08_ExitoTBS

