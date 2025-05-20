--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°06 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Dirección Ejecutiva de Inmunizaciones (DMUNI)
-- NOMBRE: Porcentaje de niñas/niños recién nacidos de parto institucional vacunados con BCG
--		   y Anti hepatitis B antes del alta
--===========================================================================================

--*******************************************************************************************
-- Creado por		   : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 25/10/2023
-- Motivo              : Modificaciones en la FICHA para los Indicadores del 2024
-- Modificado por	   : Piero Romero Marín (OGEI)
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 05/12/2024
-- Motivo              : Modificaciones en la FICHA para los Indicadores del 2025
-- Modificado por	   : Edson Donayre Uchuya (OGEI)
--*******************************************************************************************

USE BD_HISINDICADORES
GO

--Padron Nominal
IF Object_id(N'tempdb..#PadronNominal_Prev',N'U') IS NOT NULL DROP TABLE #PadronNominal_Prev;
SELECT pn.*
INTO #PadronNominal_Prev
FROM BD_BACKUP_OGEI.dbo.TramaPadronNominal pn with(nolock)

UPDATE a SET a.TI_DOC_IDENTIDAD = a.TIP_DOC
FROM #PadronNominal_Prev a

UPDATE a SET a.TI_DOC_IDENTIDAD = 1
FROM #PadronNominal_Prev a
WHERE a.TI_DOC_IDENTIDAD = 4

UPDATE a SET a.TI_DOC_IDENTIDAD = 3
FROM #PadronNominal_Prev a
WHERE a.TI_DOC_IDENTIDAD = 2

DELETE FROM #PadronNominal_Prev
WHERE nu_cnv = '3004303430'

--***************************************************
--				BASES DE DATOS.
--***************************************************

--HIS MINSA 2024
SELECT
	id_cita									,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item								,
	valor_lab
INTO #his_minsa
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa WITH(nolock)
WHERE cod_item		IN ('90585',				--Vacuna BCG
						'90744') AND			--Vacuna HVB
	  id_tipo_doc	IN (1,6)	 AND            --TipoDoc: 1=DNI 6=CNV
	  sw = 1

--CNV
SELECT
	distinct
	num_doc		= nu_cnv					,
	fecha_nac	= convert(date,FE_NACIDO)	,
	renaes		= convert(int,CO_LOCAL)
into #base_cnv
from BD_BACKUP_OGEI.dbo.TramaCNV with (nolock)
where					 sw_cnv = 1		AND 
	  convert(int,PESO_NACIDO) >= 1500		--Excluir menores de 1500 gr.

--Padron Nominal.
IF Object_id(N'tempdb..#Padron_Nominal',N'U') IS NOT NULL DROP TABLE #Padron_Nominal;
SELECT
	distinct
	num_cnv = nu_cnv,
	num_dni = NU_DNI_MENOR
into #Padron_Nominal
from (	SELECT
			NU_CNV		,
			NU_DNI_MENOR
		from #PadronNominal_Prev --Antes era BD_BACKUP_OGEI.dbo.TramaPadronNominal
		where (sw_cnv = 1 or sw_pn = 1)	and
			   try_convert(int,nu_cnv) is not null and
			   NU_CNV <> ''
	 ) as t 
where try_convert(int,NU_DNI_MENOR) is not null and
	  NU_DNI_MENOR <> ''


--***************************************************
--					SINTAXIS
--***************************************************
IF Object_id(N'tempdb..#tabla_reporte',N'U') IS NOT NULL DROP TABLE #tabla_reporte;
create TABLE #tabla_reporte
(	renaes		int			,
	num_cnv		nvarchar(12),
	num_dni		nvarchar(15),
	fecha_nac	date		,
	año			int			,
	mes			int			,
	den			int			,
	num			int			,
	vacuna_BCG	int			,
	fecha_BCG	date		,
	cnv_bcg		int			,
	vacuna_HVB	int			,
	fecha_HVB	date		,
	cnv_hvb		int
)

declare @mes_inicio int,
		@mes_eval	int,
		@año		int 

set @mes_inicio = 1		--<========= Mes inicio
set @mes_eval	= 10		--<========= Mes de evaluación
set @año		= 2024  --<========= Año de evaluación 


WHILE @mes_inicio <= @mes_eval
BEGIN

PRINT(CONCAT('PERIODO: ',CONVERT(VARCHAR(4),@año),'-',CONVERT(VARCHAR(2),@mes_inicio)))

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%

--1.Niños /niñas nacidas en Hospital o Instituto, registrados en el CNV en Línea en el periodo de evaluación
SELECT
	distinct
	num_cnv		= a.num_doc					,
	b.num_dni								,
	a.fecha_nac								,
	fecha_corte = dateadd(dd,1,a.fecha_nac)	,
	a.renaes				
into #cnv_den
from #base_cnv a
LEFT JOIN #Padron_Nominal b on a.num_doc = b.num_cnv
where month(a.fecha_nac) = @mes_inicio and
	   year(a.fecha_nac) = @año

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

--1. Niños del denominador que recibieron vacunas HVB y BCG

		--1.1 Recibio vacunas HVB en menos de 24 horas. 

			-- Validacion con Cnv.
			SELECT
				a.num_cnv						,
				a.fecha_nac						,
				a.renaes						,
				fecha_HVB	= b.fecha_atencion	,
				vacuna_HVB	= 1					,
				cnv			= 1
			into #cnv_numHVB_cnv
			from #cnv_den a
			inner join #his_minsa b on a.num_cnv = b.num_doc AND convert(int,a.renaes) = convert(int,b.renaes) 
			where b.cod_item = '90744' and
				 (b.fecha_atencion between a.fecha_nac and a.fecha_corte)

            
            -- Validacion con Dni.
			SELECT
				a.num_cnv						,
				a.fecha_nac						,
				a.renaes						,
				fecha_HVB	= b.fecha_atencion	,
				vacuna_HVB	= 1					,
				cnv			= 0
			into #cnv_numHVB_dni
			from #cnv_den a
			inner join #his_minsa b on a.num_dni = b.num_doc AND convert(int,a.renaes) = convert(int,b.renaes) 
			where	 b.cod_item = '90744' and
					(b.fecha_atencion between a.fecha_nac and a.fecha_corte)


        -- Validacion Final.
		SELECT
			num_cnv		,
			fecha_nac	,
			renaes		,
			fecha_HVB	= max(fecha_HVB)	,
			vacuna_HVB	= max(vacuna_HVB)	,
			cnv_hvb		= max(cnv)
		into #cnv_numHVB
		from(	SELECT *
				from #cnv_numHVB_cnv
				union all 
				SELECT *
				from #cnv_numHVB_dni
			 ) as t
		group by num_cnv, fecha_nac, renaes

        --1.2 Recibio vacunas BCG en menos de 24 horas. 

			-- Validacion con Cnv.
			SELECT
				a.num_cnv	,
				a.fecha_nac	,
				a.renaes	,
				fecha_BCG	= b.fecha_atencion	,
				vacuna_BCG	= 1					,
				cnv			= 1
			into #cnv_numBCG_cnv
			from #cnv_den a
			inner join #his_minsa b on a.num_cnv = b.num_doc AND convert(int,a.renaes) = convert(int,b.renaes) 
			where b.cod_item = '90585' and
				 (b.fecha_atencion between a.fecha_nac and a.fecha_corte)

			-- Validacion con Dni.
			SELECT
				a.num_cnv	,
				a.fecha_nac	,
				a.renaes	,
				fecha_BCG	= b.fecha_atencion	,
				vacuna_BCG	= 1					,
				cnv			= 0
			into #cnv_numBCG_dni
			from #cnv_den a
			inner join #his_minsa b on a.num_dni = b.num_doc AND convert(int,a.renaes) = convert(int,b.renaes) 
			where b.cod_item = '90585' and
				 (b.fecha_atencion between a.fecha_nac and a.fecha_corte)


		-- Validacion Final.
		SELECT
			num_cnv		,
			fecha_nac	,
			renaes		,
			fecha_BCG	= max(fecha_BCG)	,
			vacuna_BCG	= max(vacuna_BCG)	,
			cnv_bcg		= max(cnv)
		into #cnv_numBCG
		from(	SELECT *
				from #cnv_numBCG_cnv
				union all 
				SELECT *
				from #cnv_numBCG_dni
			 ) as t
		group by num_cnv, fecha_nac, renaes

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #tabla_reporte
SELECT
	a.renaes	,
	a.num_cnv	,
	a.num_dni	,
	a.fecha_nac	,
	año			= year(a.fecha_nac)							,
	mes			= month(a.fecha_nac)						,
	den			= 1,
	num			= iif(b.vacuna_BCG=1 and c.vacuna_HVB=1,1,0),
	vacuna_BCG	= isnull(b.vacuna_BCG,0)					,
	b.fecha_BCG	,
	cnv_bcg		= isnull(B.cnv_bcg,0)						,	--Vacuna BCG
	vacuna_HVB	= isnull(c.vacuna_HVB,0)					,
	c.fecha_HVB	,
	cnv_hvb		= isnull(C.cnv_hvb,0)							--Vacuna HVB
from #cnv_den a
LEFT JOIN #cnv_numBCG b on a.num_cnv   = b.num_cnv	 and
						   a.fecha_nac = b.fecha_nac and
						   a.renaes	   = b.renaes
LEFT JOIN #cnv_numHVB c on a.num_cnv   = c.num_cnv	 and
						   a.fecha_nac = c.fecha_nac and
						   a.renaes    = c.renaes

DROP TABLE #cnv_den
DROP TABLE #cnv_numBCG
DROP TABLE #cnv_numHVB
DROP TABLE #cnv_numBCG_cnv
DROP TABLE #cnv_numBCG_dni
DROP TABLE #cnv_numHVB_cnv
DROP TABLE #cnv_numHVB_dni

SET @mes_inicio = @mes_inicio+1
END

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
IF OBJECT_ID (N'dbo.DL1153_2025_CG07_RnVacuna', N'U') IS NOT NULL  
DROP TABLE dbo.DL1153_2025_CG07_RnVacuna;
GO

SELECT 
	Diris			=	B.diris		,
	Departamento	=	CASE 
							WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
							WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
							ELSE B.DESC_DPTO
						END			,
	Provincia		=	B.desc_prov	,
	Distrito		=	B.desc_dist	,
	Red				=	case
							when b.DIRIS like '%diris%' then b.diris
							else B.DESC_RED
						end			,
	MicroRed		= b.desc_mred	,
	b.cat_estab						,
	eess_nombre		= b.desc_estab	,
	a.*
INTO dbo.DL1153_2025_CG07_RnVacuna
FROM #tabla_reporte a
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes) = convert(int,b.COD_ESTAB)
WHERE b.CAT_ESTAB IN ('I-1','I-2','I-3','I-4','II-1','II-2','II-E','III-1','III-2','III-E') AND
	  CONVERT(int,b.ambito) = 1

-- Exportamos el reporte final agrupado
SELECT
	Diris							,
	Departamento					,
	Provincia						,
	Distrito						,
	Red								,
	MicroRed						,
	Cat_estab						,
	eess_nombre						,
	mes								,
	año								,
	den			= sum(den)			,
	num			= sum(num)			,
	Vacuna_BCG	= sum(vacuna_BCG)	,
	Vacuna_HVB	= sum(vacuna_HVB)
FROM dbo.DL1153_2025_CG07_RnVacuna
GROUP BY	Diris		, Departamento	, Provincia	, Distrito	, Red				, MicroRed,
			Cat_estab	, eess_nombre	, mes		, año	


DROP TABLE #base_cnv
DROP TABLE #Padron_Nominal
DROP TABLE #his_minsa