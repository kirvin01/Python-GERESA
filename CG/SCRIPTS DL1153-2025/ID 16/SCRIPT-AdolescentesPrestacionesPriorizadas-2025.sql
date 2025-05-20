--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / DIVICI
-- NOMBRE: Porcentaje de adolescentes que reciben prestaciones priorizadas para el cuidado 
-- de su salud
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marin (OGEI)
-- Fecha creación      : 02/12/2024
--*******************************************************************************************

USE BD_HISINDICADORES

--***************************************************
--				BASES DE DATOS.
--***************************************************
--HIS MINSA 2024
IF OBJECT_ID('Tempdb..#his_minsa_Den') IS NOT NULL DROP TABLE #his_minsa_Den
select
	id_cita			,
	renaes			,
	id_tipcond_estab,
	fecha_atencion	= convert(date,periodo),
	id_tipo_doc		,
	num_doc			,
	id_genero		,
	fecha_registro	,
	id_tipitem		,
	cod_item		,
	valor_lab
into #his_minsa_Den
from BD_BACKUP_OGEI.dbo.TramaHisMinsa with (nolock)
where id_tipo_doc IN (1,2,3,4)
	and sw = 1
	and (convert(int,edad_reg) between 12 and 17)
	and id_tipedad_reg	= 'A'

--1.1 Se excluyen adolescentes gestantes
	delete from #his_minsa_Den
	where num_doc in (	select
							distinct
							num_doc
						from #his_minsa_Den 
						where cod_item in ('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z359','Z3492.2')
					 )

--1.2 Se excluye atenciones de los adolescentes que acuden por vacunas, telemedicina
	delete from #his_minsa_Den
	where id_cita in (	select
							distinct
							id_cita
						from #his_minsa_Den 
						where cod_item in ('90749.01','90749.02','90649','90670','90717',
										   '90658','90688','90746','90714','90749.05')
							OR SUBSTRING(cod_item,1,5) = '99499'
					 )

IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa
select
	id_cita			,
	renaes			,
	id_tipcond_estab,
	fecha_atencion	,
	id_tipo_doc		,
	num_doc			,
	id_genero		,
	fecha_registro	,
	id_tipitem		,
	cod_item		,
	valor_lab
into #his_minsa
from #his_minsa_Den with (nolock)
where cod_item IN ('C8002','99402.03','Z019','99209.04','99403.01',
				   '99402.09','99199.26','85018','85018.01')

--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
	renaes			INT,
	Mes				INT,
	Genero			VARCHAR(1),
	id_tipo_doc		INT,
	num_doc			VARCHAR(20),
	Den				INT,
	Num				INT,
	AtenIntegral	INT,
	ConsejSex		INT,
	EvalNutriAnt	INT,
	ConsejAlimen	INT,
	ConsejPrevSM	INT,
	SuplemHAcFol	INT,
	DosajeHemo		INT
)

declare @mes_final	int,
		@mes_eval	int,
		@año		int

set @mes_eval  = 1		--<=========== Mes inicial
set @mes_final = 10		--<=========== Mes de evaluación
set @año	   = 2024	--<============= Año de evaluación 

while @mes_eval <= @mes_final
begin

--%%%%%%%%%%%%%%%%
-- DENOMINADOR
--%%%%%%%%%%%%%%%%
-- Adolescentes atendidos según HIS MINSA 
IF OBJECT_ID('Tempdb..#Den') IS NOT NULL DROP TABLE #Den
SELECT
	h.*
INTO #Den
FROM
	(SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.id_tipo_doc,h.num_doc		
								   ORDER BY h.fecha_atencion ASC),
		h.*
	FROM
		(select
			distinct
			a.renaes		,
			a.id_genero		,
			a.fecha_atencion,
			Mes				= @mes_eval,
			a.id_tipo_doc	,
			a.num_doc		,
			Den				= 1
		from #his_minsa_Den a 
		WHERE MONTH(a.fecha_atencion) <= @mes_eval) h
	) h
WHERE h.id = 1

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
-- Adolescentes que cumplen con el paquete básico de Prestaciones priorizadas:
IF OBJECT_ID('Tempdb..#Num') IS NOT NULL DROP TABLE #Num
SELECT
	h.Mes			,
	h.renaes		,
	h.id_genero		,
	h.id_tipo_doc	,
	h.num_doc		,
	AtenIntegral	= SUM(h.AtenIntegral)	,
	ConsejSex		= SUM(h.ConsejSex)		,
	EvalNutriAnt	= SUM(h.EvalNutriAnt)	,
	ConsejAlimen	= SUM(h.ConsejAlimen)	,
	ConsejPrevSM	= SUM(h.ConsejPrevSM)	,
	SuplemHAcFol	= SUM(h.SuplemHAcFol)	,
	DosajeHemo		= SUM(h.DosajeHemo)		,
	Num				= CONVERT(INT,0)
INTO #Num
FROM
	(-- Adolescentes varones
	SELECT
		DISTINCT
		a.renaes		,
		Mes				= @mes_eval,
		id_genero		,
		a.id_tipo_doc	,
		a.num_doc		,
		AtenIntegral = IIF(a.cod_item = 'C8002' AND a.valor_lab = 'TA',1,0),
		ConsejSex	 = IIF(a.cod_item = '99402.03',1,0),
		EvalNutriAnt = IIF(a.cod_item IN ('Z019','99209.04'),1,0),
		ConsejAlimen = IIF(a.cod_item IN ('99403.01'),1,0),
		ConsejPrevSM = IIF(a.cod_item IN ('99402.09'),1,0),
		SuplemHAcFol = CONVERT(INT,NULL),
		DosajeHemo	 = CONVERT(INT,NULL)
	FROM #his_minsa a
	WHERE a.id_genero	 = 'M'
		AND a.id_tipitem = 'D'
		AND MONTH(a.fecha_atencion) <= @mes_eval 
		AND (		(a.cod_item = 'C8002' AND a.valor_lab = 'TA')
				OR	 a.cod_item IN ('99402.03','Z019','99209.04','99403.01','99402.09'))
	UNION
	-- Adolescentes mujeres
	SELECT
		DISTINCT
		a.renaes		,
		Mes				= @mes_eval,
		a.id_genero		,
		a.id_tipo_doc	,
		a.num_doc		,
		AtenIntegral = IIF(a.cod_item = 'C8002' AND a.valor_lab = 'TA',1,0),
		ConsejSex	 = IIF(a.cod_item = '99402.03',1,0),
		EvalNutriAnt = IIF(a.cod_item IN ('Z019','99209.04'),1,0),
		ConsejAlimen = IIF(a.cod_item IN ('99403.01'),1,0),
		ConsejPrevSM = IIF(a.cod_item IN ('99402.09'),1,0),
		SuplemHAcFol = IIF(a.cod_item IN ('99199.26'),1,0),
		DosajeHemo	 = IIF(a.cod_item IN ('85018','85018.01'),1,0)
	FROM #his_minsa a
	WHERE a.id_genero	 = 'F'
		AND a.id_tipitem = 'D'
		AND MONTH(a.fecha_atencion) <= @mes_eval 
		AND (		(a.cod_item = 'C8002' AND a.valor_lab = 'TA')
				OR	 a.cod_item IN ('99402.03','Z019','99209.04','99403.01',	
									'99402.09','99199.26','85018','85018.01'))
	) h
GROUP BY	h.Mes			,
			h.renaes		,
			h.id_genero		,
			h.id_tipo_doc	,
			h.num_doc

-- Actualizamos el cumplimiento del numerador para varones
UPDATE n SET
	n.Num = 1
FROM #Num n
WHERE n.id_genero		= 'M'
	AND n.AtenIntegral	= 1
	AND n.ConsejSex		= 1
	AND n.EvalNutriAnt	= 1
	AND n.ConsejAlimen	= 1
	AND n.ConsejPrevSM	= 1

-- Actualizamos el cumplimiento del numerador para varones
UPDATE n SET
	n.Num = 1
FROM #Num n
WHERE n.id_genero		= 'F'
	AND n.AtenIntegral	= 1
	AND n.ConsejSex		= 1
	AND n.EvalNutriAnt	= 1
	AND n.ConsejAlimen	= 1
	AND n.ConsejPrevSM	= 1
	AND n.SuplemHAcFol	= 1
	AND n.DosajeHemo	= 1

--%%%%%%%%%%%%%%%%
-- INDICADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Ind') IS NOT NULL DROP TABLE #Ind
SELECT
	d.renaes		,
	d.Mes			,
	Genero			= d.id_genero,
	d.id_tipo_doc	,
	d.num_doc		,
	d.Den			,
	Num				= CONVERT(INT,0),
	AtenIntegral	= CONVERT(INT,0),
	ConsejSex		= CONVERT(INT,0),
	EvalNutriAnt	= CONVERT(INT,0),
	ConsejAlimen	= CONVERT(INT,0),
	ConsejPrevSM	= CONVERT(INT,0),
	SuplemHAcFol	= CONVERT(INT,0),
	DosajeHemo		= CONVERT(INT,0)
INTO #Ind
FROM #Den d

UPDATE i SET
	i.Num			= n.Num			,
	i.AtenIntegral	= n.AtenIntegral,
	i.ConsejSex		= n.ConsejSex	,	
	i.EvalNutriAnt	= n.EvalNutriAnt,
	i.ConsejAlimen	= n.ConsejAlimen,
	i.ConsejPrevSM	= n.ConsejPrevSM,
	i.SuplemHAcFol	= n.SuplemHAcFol,
	i.DosajeHemo	= n.DosajeHemo
FROM #Ind i
INNER JOIN #Num n ON i.id_tipo_doc = n.id_tipo_doc	AND
						 i.num_doc = n.num_doc		AND
						  i.renaes = n.renaes
					
INSERT INTO #tabla_reporte
SELECT * FROM #Ind

set @mes_eval = @mes_eval + 1
end

--%%%%%%%%%%%%%%%%
-- REPORTE
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Tabla_Fisica_Indicador') IS NOT NULL DROP TABLE #Tabla_Fisica_Indicador
SELECT
	DISA	= r.DESC_DISA	,
	PROV	= r.DESC_PROV	,
	RED		= r.DESC_RED	,
	DIST	= r.DESC_DIST	,
	EESS	= r.DESC_ESTAB	,
	CATEG	= r.CAT_ESTAB	,
	t.*
INTO #Tabla_Fisica_Indicador
FROM #tabla_reporte t
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK) ON t.renaes = r.COD_ESTAB
WHERE r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4')
	OR (r.CAT_ESTAB = 'II-1' AND r.CAMAS <= 50) --Hospitales con Población asignada

--Generamos el reporte agrupado para exportarlo
SELECT
	t.DISA			,
	t.PROV			,
	t.RED			,
	t.DIST			,
	t.EESS			,
	t.CATEG			,
	t.Mes			,
	Den				= SUM(t.Den)		 ,
	Num				= SUM(t.Num)		 ,
	AtenIntegral	= SUM(t.AtenIntegral),
	ConsejSex		= SUM(t.ConsejSex)	 ,	
	EvalNutriAnt	= SUM(t.EvalNutriAnt),
	ConsejAlimen	= SUM(t.ConsejAlimen),
	ConsejPrevSM	= SUM(t.ConsejPrevSM),
	SuplemHAcFol	= SUM(t.SuplemHAcFol),
	DosajeHemo		= SUM(t.DosajeHemo)
FROM #Tabla_Fisica_Indicador t
GROUP BY t.DISA			,
		 t.PROV			,
		 t.RED			,
		 t.DIST			,
		 t.EESS			,
		 t.CATEG		,
		 t.Mes