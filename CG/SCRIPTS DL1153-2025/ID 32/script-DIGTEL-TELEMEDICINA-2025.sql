--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°33 - DL 1153 - 2024
-- ÁREA RESPONSABLE DEL INDICADOR: DIGTEL / Dirección de Telemedicina (DITEL)
-- NOMBRE: Tasa de utilización de los servicios de telemedicina
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 30/11/2023
-- Motivo              : Revisar el código para el 2024
-- Modificado por	   : Piero Romero Marín (OGEI)
--*******************************************************************************************
USE BD_HISINDICADORES
GO

--***************************************************
--				BASES DE DATOS.
--***************************************************
--HIS MINSA 
IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa
select
	id_cita			,
	num_doc			,
	fecha_atencion	= convert(date,periodo)	,
	id_profesion	,
	renaes			= convert(int,renaes)	,
	id_tipcond_estab,
	tipo_dx			= id_tipitem			,
	cod_item		,
	valor_lab
into #his_minsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa h with (nolock)
where			 sw = 1			AND
		-- Telemedicina
		(cod_item in ('99499.11','99499.12','99499.01',
					 '99499.03','99499.10')
		)														OR
		-- Enfermedades y transtornos hipertensivos:
		(SUBSTRING(h.cod_item,1,3) IN ('I10','I11','I12',
	    							   'O10','O13','O14')	OR
						h.cod_item = '016X'
		)														OR
		-- Diabetes Mellitus
		(SUBSTRING(h.cod_item,1,3) IN ('E10','E11','E12',				
		 							   'E13','E14','O24')
	    )														OR
		--== Salud Mental
		--Transtorno y episodio depresivo
		(SUBSTRING(h.cod_item,1,3) IN ('F32','F33')			OR
						h.cod_item = 'F204'
		)														OR
		--Transtorno de la ansiedad
		(SUBSTRING(h.cod_item,1,3) IN ('F40','F41')			OR
						h.cod_item IN ('F064','F930',
									   'F931','F932')
		)														OR
		--Trastornos mentales y del comportamiento debidos al uso de alcohol:
		(SUBSTRING(h.cod_item,1,3) IN ('F10')					
		)														OR
		--Trastorno de consumo de sustancias:
		(SUBSTRING(h.cod_item,1,3) IN ('F11','F19')
		)														OR
		--Transtorno psicótico
		(SUBSTRING(h.cod_item,1,3) IN ('F20','F23')			OR
						h.cod_item IN ('F28X','F29X',
									   'F531')
		)														OR
		--Maltrato Infantil
		(SUBSTRING(h.cod_item,1,3) IN ('T74')				OR
						h.cod_item IN ('Y040','Y041','Y042',
									   'Y048','Y049','Y060',
									   'Y061','Y068','Y070',
									   'Y071','Y078','Y079')
		)														OR
		--Anemia
		(SUBSTRING(h.cod_item,1,3) IN ('D50','D51','D52','D53',
									   'D55','D56','D57','D58',
									   'D59','D60','D61','D62',
									   'D63','D64')
		) 

-- Generamos Tabla RENAES
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
SELECT
	renaes	= r.COD_ESTAB,
	Categ	= r.CAT_ESTAB
INTO #renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r
WHERE r.AMBITO_ESN = 1

-- Atenciones del HIS MINSA
IF OBJECT_ID('Tempdb..#Atenciones_Den') IS NOT NULL DROP TABLE #Atenciones_Den
SELECT
	t.renaes	,
	t.Mes		,
	Cantidad	= COUNT(*)
INTO #Atenciones_Den
FROM
(SELECT
	DISTINCT
	renaes	= convert(int,h.renaes),
	id_cita	,
	Mes		= MONTH(convert(date,h.periodo))
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa h with(nolock)
WHERE			  sw = 1) t
GROUP BY t.renaes,
		 t.Mes	

-- Tabla de Poblacion
-- Tabla remitida por Ing. Dulcinea Zuñiga - dzunigaa@minsa.gob.pe - 16/07 - por correo
-- DROP TABLE DL1153_2024_IPRESS_Telemedicina
IF OBJECT_ID('Tempdb..#Poblacion') IS NOT NULL DROP TABLE #Poblacion
SELECT
	p.*
INTO #Poblacion
FROM dbo.DL1153_2024_IPRESS_Telemedicina p with(nolock)

-- Tabla de Umbrales
-- Tabla remitida por Ing. Dulcinea Zuñiga - dzunigaa@minsa.gob.pe - 16/07 - por correo
-- DROP TABLE DL1153_2024_Umbrales_IPRESS_Telemedicina
IF OBJECT_ID('Tempdb..#Umbrales') IS NOT NULL DROP TABLE #Umbrales
SELECT
	t.Nivel				,
	renaes				= CONVERT(INT,t.renaes)			,
	Teleinterconsulta	= ROUND(t.Teleinterconsulta,2)	,
	teleconsulta		= ROUND(t.teleconsulta,2)		,
	telmonitoreo		= ROUND(t.telmonitoreo,2)
INTO #Umbrales
FROM DL1153_2024_Umbrales_IPRESS_Telemedicina t with(nolock)

--Actualizamos los umbrale que tienen cero de TeleInterconsulta
UPDATE u SET
	u.Teleinterconsulta = 50
FROM #Umbrales u
INNER JOIN #Poblacion p ON u.renaes = p.RENAES
WHERE	u.Teleinterconsulta  = 0	 AND
		p.PoblacionAsignada <= 15000

UPDATE u SET
	u.Teleinterconsulta = 60
FROM #Umbrales u
INNER JOIN #Poblacion p ON u.renaes = p.RENAES
WHERE	u.Teleinterconsulta  = 0	 AND
		p.PoblacionAsignada BETWEEN 15001 AND 30000

UPDATE u SET
	u.Teleinterconsulta = 90
FROM #Umbrales u
INNER JOIN #Poblacion p ON u.renaes = p.RENAES
WHERE	u.Teleinterconsulta  = 0	 AND
		p.PoblacionAsignada >= 30001


--*************************************************--
--					SINTAXIS
--*************************************************--
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(	
	año								int,
	mes								int,
	Nivel							VARCHAR(50),
	renaes							int,
	den								INT,
	Num_TelInt_Ptje					INT,
	teleinterconsulta				FLOAT,
	TeleInterConsultante			FLOAT,
	TeleInterConsultor				FLOAT,
	Num_TelCon_Ptje					INT,
	TeleConsulta					FLOAT,
	Num_TelMon_Ptje					INT,
	TeleMonitoreo					FLOAT,
	num								INT,
)

declare @mes_eval	int,
		@mes_inicio int,
		@año		int 

set @mes_inicio	= 1		--<====== Mes de inicio
set @mes_eval	= 10		--<====== Mes de evaluación
set @año		= 2024	--<====== Año de evaluación 

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%

/* Establecimientos de salud de salud primer nivel de atención*/

-- TeleInterconsulta / TeleConsulta
IF OBJECT_ID('Tempdb..#Den_Tele_Inter_Consul') IS NOT NULL DROP TABLE #Den_Tele_Inter_Consul
SELECT
	renaes	= CONVERT(INT,	t.renaes)			,
	PobAsig = CONVERT(FLOAT,p.PoblacionAsignada)
INTO #Den_Tele_Inter_Consul
FROM #Umbrales	  t WITH(NOLOCK) -- 8 433
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK) ON t.renaes = r.COD_ESTAB -- 3 610
INNER JOIN #Poblacion						  p WITH(NOLOCK) ON CONVERT(INT,t.renaes) = CONVERT(INT,p.RENAES)
WHERE	r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') AND		-- EESS 1er Nivel de atención -- 3 453
		r.AMBITO = 1

-- Telemonitoreo
IF OBJECT_ID('Tempdb..#Den_Telemon') IS NOT NULL DROP TABLE #Den_Telemon
SELECT
	renaes	= CONVERT(INT,	t.renaes)			,
	PobAsig = CONVERT(FLOAT,t.PoblacionAsignada)
INTO #Den_Telemon
FROM #Poblacion	  t WITH(NOLOCK) 
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK) ON t.renaes = r.COD_ESTAB 
WHERE	r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') AND		
		r.AMBITO = 1

/*Establecimientos de salud del segundo y tercer nivel de atención*/
IF OBJECT_ID('Tempdb..#Den_II_III_Nivel') IS NOT NULL DROP TABLE #Den_II_III_Nivel
SELECT
	t.renaes,
	Cantidad = SUM(t.Cantidad)
INTO #Den_II_III_Nivel
FROM
(SELECT
	t.renaes,
	t.Mes,
	t.Cantidad
FROM #Atenciones_Den	  t WITH(NOLOCK) -- 3 612
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK) ON t.renaes = r.COD_ESTAB -- 3 610
WHERE	r.CAT_ESTAB IN ('II-1','II-2','II-E','III-1','III-2','III-E') AND
		r.AMBITO = 1) t	-- EESS 1er Nivel de atención -- 3 453
WHERE t.Mes <= @mes_inicio 
GROUP BY t.renaes

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

/*1. Establecimientos de salud de salud primer nivel de atención*/
	--1.1 Teleinterconsulta - Consultante.
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultante') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultante
	SELECT
		t.renaes	,
		t.num_doc	,
		teleinterconsulta_consultante = SUM(t.teleinterconsulta_consultante)
	INTO #his_minsa_TeleInter_consultante
	FROM
	(select
		distinct
		a.renaes	,
		a.id_cita	,
		a.num_doc	,
		teleinterconsulta_consultante = 1
	from #his_minsa a
	inner join #Den_Tele_Inter_Consul b on a.renaes = b.renaes
	where	a.cod_item in ('99499.11',				-- Teleinterconsulta Síncrona
					       '99499.12')		  and	-- Teleinterconsulta Asíncrona
			try_convert(int,valor_lab) = 1	  and	-- Teleinterconsulta - Consultante.
						 a.tipo_dx	   = 'D'  and
						 CONVERt(inT,id_profesion) IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,
									  16,17,18,19,20,21,22,23,24,25,26,28,
									  29,30,31,33,34,35,36,38,45,46,47,48,
									  49,50,51,54,55,56,57) AND
			year(fecha_atencion)	   = @año and
			month(fecha_atencion)	  <= @mes_inicio)	t
	GROUP BY t.renaes,
			 t.num_doc


	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultante_R') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultante_R
	SELECT
		t.renaes,
		teleinterconsulta_consultante = SUM(t.teleinterconsulta_consultante)
	INTO #his_minsa_TeleInter_consultante_R
	FROM #his_minsa_TeleInter_consultante t
	GROUP BY	t.renaes

	--1.2 Teleinterconsulta - Consultor.
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultor') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultor
	SELECT
		t.renaes,
		t.num_doc,
		teleinterconsulta_consultor = SUM(t.teleinterconsulta_consultor)
	INTO #his_minsa_TeleInter_consultor
	FROM
	(select
		distinct
		a.renaes,
		a.id_cita,
		a.num_doc,
		teleinterconsulta_consultor = 1
	from #his_minsa a
	inner join #Den_Tele_Inter_Consul b on a.renaes = b.renaes
	where	a.cod_item in ('99499.11',				-- Teleinterconsulta Síncrona
					       '99499.12')		  and	-- Teleinterconsulta Asíncrona
			try_convert(int,valor_lab) = 2	  and	-- Teleinterconsulta - Consultor.
						a.tipo_dx	   = 'D'  and
						 CONVERt(inT,id_profesion) IN (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,
									  16,17,18,19,20,21,22,23,24,25,26,28,
									  29,30,31,33,34,35,36,38,45,46,47,48,
									  49,50,51,54,55,56,57) AND
			year(fecha_atencion)	   = @año and
			month(fecha_atencion)	  <= @mes_inicio)	t
	GROUP BY t.renaes,
			 t.num_doc
	
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultor_R') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultor_R
	SELECT
		t.renaes,
		teleinterconsulta_consultor = SUM(t.teleinterconsulta_consultor)
	INTO #his_minsa_TeleInter_consultor_R
	FROM #his_minsa_TeleInter_consultor t
	GROUP BY	t.renaes

	--==Juntamos TeleInterconsulta
	IF OBJECT_ID('Tempdb..#TeleInterConsulta') IS NOT NULL DROP TABLE #TeleInterConsulta
	SELECT
		t.*,
		TeleInterConsulta = t.teleinterconsulta_consultante + t.teleinterconsulta_consultor
	INTO #TeleInterConsulta
	FROM
	(SELECT
		renaes							= IIF(e.renaes IS NULL,r.renaes,e.renaes),
		teleinterconsulta_consultante	= ISNULL(e.teleinterconsulta_consultante,0),
		teleinterconsulta_consultor		= ISNULL(r.teleinterconsulta_consultor,0)
	FROM #his_minsa_TeleInter_consultante_R e
	FULL OUTER JOIN #his_minsa_TeleInter_consultor_R r ON e.renaes = r.renaes) t

	--1.3 TeleConsulta
	IF OBJECT_ID('Tempdb..#his_minsa_TeleConsul') IS NOT NULL DROP TABLE #his_minsa_TeleConsul
	SELECT
		t.renaes,
		t.num_doc,
		teleConsulta = SUM(t.teleConsulta)
	INTO #his_minsa_TeleConsul
	FROM
	(select
		distinct
		a.renaes,
		a.id_cita,
		a.num_doc,
		teleConsulta = 1
	from #his_minsa a
	inner join #Den_Tele_Inter_Consul b on a.renaes = b.renaes
	where	a.cod_item in ('99499.01',				-- TeleConsulta Síncrona
					       '99499.03') and			-- TeleConsulta Asíncrona
			--try_convert(int,valor_lab) = 1	  and	-- Teleinterconsulta - Consultante.
						a.tipo_dx	   = 'D'  and
			year(fecha_atencion)   = @año	  and
			month(fecha_atencion) <= @mes_inicio)	t
	GROUP BY t.renaes,
			 t.num_doc

	IF OBJECT_ID('Tempdb..#his_minsa_TeleConsul_R') IS NOT NULL DROP TABLE #his_minsa_TeleConsul_R
	SELECT
		t.renaes,
		teleConsulta = SUM(t.teleConsulta)
	INTO #his_minsa_TeleConsul_R
	FROM #his_minsa_TeleConsul t
	GROUP BY	t.renaes

	-- 1.5 Telemonitoreo
	IF OBJECT_ID('Tempdb..#his_minsa_TeleMonitoreo') IS NOT NULL DROP TABLE #his_minsa_TeleMonitoreo
	SELECT
		t.renaes,
		t.num_doc,
		TeleMonitoreo = SUM(t.TeleMonitoreo)
	INTO #his_minsa_TeleMonitoreo
	FROM
	(select
		distinct
		a.renaes,
		a.id_cita,
		a.num_doc,
		TeleMonitoreo = 1
	from #his_minsa a
	inner join #Den_Telemon b on a.renaes = b.renaes
	where	a.cod_item in ('99499.10')			AND	
			a.tipo_dx	   = 'D'  and
			year(fecha_atencion)	   = @año	AND
			month(fecha_atencion)	  <= @mes_inicio		AND
			id_cita IN (SELECT
							DISTINCT
							id_cita
						FROM #his_minsa
						WHERE cod_item NOT IN ('99499.11','99499.12','99499.01',
											   '99499.03','99499.10'))
	) t
	GROUP BY t.renaes,
			 t.num_doc
	HAVING (SUM(t.TeleMonitoreo)) <= 24

	IF OBJECT_ID('Tempdb..#his_minsa_TeleMonitoreo_R') IS NOT NULL DROP TABLE #his_minsa_TeleMonitoreo_R
	SELECT
		t.renaes,
		TeleMonitoreo = SUM(t.TeleMonitoreo)
	INTO #his_minsa_TeleMonitoreo_R
	FROM #his_minsa_TeleMonitoreo t
	GROUP BY	t.renaes

/*2. Establecimientos de salud del segundo y tercer nivel de atención*/
--2.1 Teleinterconsulta - Consultor.
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultor_Nivel_II_III') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultor_Nivel_II_III
	SELECT
		t.renaes,
		t.num_doc,
		teleinterconsulta_consultor = SUM(t.teleinterconsulta_consultor)
	INTO #his_minsa_TeleInter_consultor_Nivel_II_III
	FROM
	(select
		distinct
		a.renaes,
		a.id_cita,
		a.num_doc,
		teleinterconsulta_consultor = 1
	from #his_minsa a
	inner join #Den_II_III_Nivel b on a.renaes = b.renaes
	where	a.cod_item in ('99499.11',				-- Teleinterconsulta Síncrona
					       '99499.12')		  and	-- Teleinterconsulta Asíncrona
			try_convert(int,valor_lab) = 2	  and	-- Teleinterconsulta - Consultor.
						a.tipo_dx	   = 'D'  and
			year(fecha_atencion)	   = @año and
			month(fecha_atencion)	  <= @mes_inicio)	t
	GROUP BY t.renaes,
			 t.num_doc
	
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultor_Nivel_II_III_R') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultor_Nivel_II_III_R
	SELECT
		t.renaes,
		teleinterconsulta_consultor = SUM(t.teleinterconsulta_consultor)
	INTO #his_minsa_TeleInter_consultor_Nivel_II_III_R
	FROM #his_minsa_TeleInter_consultor_Nivel_II_III t
	GROUP BY	t.renaes

--2.2 Teleinterconsulta - Consultante
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultante_Nivel_II_III') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultante_Nivel_II_III
	SELECT
		t.renaes,
		t.num_doc,
		teleinterconsulta_consultante = SUM(t.teleinterconsulta_consultante)
	INTO #his_minsa_TeleInter_consultante_Nivel_II_III
	FROM
	(select
		distinct
		a.renaes,
		a.id_cita,
		a.num_doc,
		teleinterconsulta_consultante = 1
	from #his_minsa a
	inner join #Den_II_III_Nivel b on a.renaes = b.renaes
	where	a.cod_item in ('99499.11',				-- Teleinterconsulta Síncrona
					       '99499.12')		  and	-- Teleinterconsulta Asíncrona
			try_convert(int,valor_lab) = 1	  and	-- Teleinterconsulta - Consultante.
						a.tipo_dx	   = 'D'  and
			year(fecha_atencion)	   = @año and
			month(fecha_atencion)	  <= @mes_inicio)	t
	GROUP BY t.renaes,
			 t.num_doc
	
	IF OBJECT_ID('Tempdb..#his_minsa_TeleInter_consultante_Nivel_II_III_R') IS NOT NULL DROP TABLE #his_minsa_TeleInter_consultante_Nivel_II_III_R
	SELECT
		t.renaes,
		teleinterconsulta_consultante = SUM(t.teleinterconsulta_consultante)
	INTO #his_minsa_TeleInter_consultante_Nivel_II_III_R
	FROM #his_minsa_TeleInter_consultante_Nivel_II_III t
	GROUP BY	t.renaes

	--==Juntamos TeleInterconsulta
	IF OBJECT_ID('Tempdb..#TeleInterConsulta_Nivel_II_IIII') IS NOT NULL DROP TABLE #TeleInterConsulta_Nivel_II_IIII
	SELECT
		t.*,
		TeleInterConsulta = t.teleinterconsulta_consultante + t.teleinterconsulta_consultor
	INTO #TeleInterConsulta_Nivel_II_IIII
	FROM
	(SELECT
		renaes							= IIF(e.renaes IS NULL,r.renaes,e.renaes),
		teleinterconsulta_consultante	= ISNULL(e.teleinterconsulta_consultante,0),
		teleinterconsulta_consultor		= ISNULL(r.teleinterconsulta_consultor,0)
	FROM #his_minsa_TeleInter_consultante_Nivel_II_III_R e
	FULL OUTER JOIN #his_minsa_TeleInter_consultor_Nivel_II_III_R r ON e.renaes = r.renaes) t


--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%

IF OBJECT_ID('Tempdb..#Tabla_Reporte_Inicial') IS NOT NULL DROP TABLE #Tabla_Reporte_Inicial
SELECT
	año						= @año	,
	Mes						= @mes_inicio	,
	Nivel					= 'Primer Nivel de Atención',
	a.renaes				,
	r.Categ					,
	ServTM					= IIF(u.renaes IS NULL,'No','Si'),
	den						= a.PobAsig,
	teleinterconsulta		= ISNULL(b.TeleInterConsulta,0),
	Um_TeleInter			= ISNULL(u.Teleinterconsulta,0),
	Num_TelInt				=	CASE
									WHEN a.PobAsig <  1000							THEN ROUND((ISNULL(b.TeleInterConsulta,0)/a.PobAsig)*100   ,2	)
									WHEN a.PobAsig <  10000  AND a.PobAsig >= 1000	THEN ROUND((ISNULL(b.TeleInterConsulta,0)/a.PobAsig)*1000  ,2	)
									WHEN a.PobAsig <  100000 AND a.PobAsig >= 10000 THEN ROUND((ISNULL(b.TeleInterConsulta,0)/a.PobAsig)*10000 ,2	)
									WHEN a.PobAsig >= 100000						THEN ROUND((ISNULL(b.TeleInterConsulta,0)/a.PobAsig)*100000,2	)
								END,
	TeleInterConsultante	= ISNULL(b.teleinterconsulta_consultante,0),
	TeleInterConsultor		= ISNULL(b.teleinterconsulta_consultor,0),
	TeleConsulta			= ISNULL(d.teleConsulta,0),
	Um_TeleConsulta			= ISNULL(u.teleconsulta,0),
	Num_TeleCon				=	CASE
									WHEN a.PobAsig <  1000							THEN ROUND((ISNULL(d.teleConsulta,0)/a.PobAsig)*100   ,2	)
									WHEN a.PobAsig <  10000  AND a.PobAsig >= 1000	THEN ROUND((ISNULL(d.teleConsulta,0)/a.PobAsig)*1000  ,2	)
									WHEN a.PobAsig <  100000 AND a.PobAsig >= 10000 THEN ROUND((ISNULL(d.teleConsulta,0)/a.PobAsig)*10000 ,2	)
									WHEN a.PobAsig >= 100000						THEN ROUND((ISNULL(d.teleConsulta,0)/a.PobAsig)*100000,2	)
								END,
	TeleMonitoreo			= ISNULL(e.TeleMonitoreo,0),
	Um_TeleMonitoreo		= ISNULL(u.telmonitoreo,0),
	Num_TeleMon				=	CASE
									WHEN a.PobAsig <  1000							THEN ROUND((ISNULL(e.TeleMonitoreo,0)/a.PobAsig)*100   ,2	)
									WHEN a.PobAsig <  10000  AND a.PobAsig >= 1000	THEN ROUND((ISNULL(e.TeleMonitoreo,0)/a.PobAsig)*1000  ,2	)
									WHEN a.PobAsig <  100000 AND a.PobAsig >= 10000 THEN ROUND((ISNULL(e.TeleMonitoreo,0)/a.PobAsig)*10000 ,2	)
									WHEN a.PobAsig >= 100000						THEN ROUND((ISNULL(e.TeleMonitoreo,0)/a.PobAsig)*100000,2	)
								END
INTO #Tabla_Reporte_Inicial -- 8368
from #Den_Telemon a
LEFT JOIN #Umbrales						u ON a.renaes = u.renaes
LEFT JOIN #TeleInterConsulta			b ON a.renaes = b.renaes
LEFT JOIN #his_minsa_TeleConsul_R		d ON a.renaes = d.renaes
LEFT JOIN #his_minsa_TeleMonitoreo_R	e ON a.renaes = e.renaes
INNER JOIN #renaes						r ON a.renaes = r.renaes
UNION ALL
SELECT
	año					 = @año							,
	Mes					 = @mes_inicio							,
	Nivel				 = 'II y III Nivel de Atención',
	d.renaes			 ,
	r.Categ				 ,
	ServTM				 = 'Si'							,
	den					 = d.Cantidad,
	teleinterconsulta	 = ISNULL(t.TeleInterConsulta,0),
	Um_TeleInter		 = u.Teleinterconsulta,
	Num_TelInt			 =	CASE
								WHEN d.Cantidad <  1000							  THEN ISNULL(ROUND((CONVERT(FLOAT,t.TeleInterConsulta)/CONVERT(FLOAT,d.Cantidad))*100   ,2),0)
								WHEN d.Cantidad <  10000  AND d.Cantidad >= 1000  THEN ISNULL(ROUND((CONVERT(FLOAT,t.TeleInterConsulta)/CONVERT(FLOAT,d.Cantidad))*1000  ,2),0)
								WHEN d.Cantidad <  100000 AND d.Cantidad >= 10000 THEN ISNULL(ROUND((CONVERT(FLOAT,t.TeleInterConsulta)/CONVERT(FLOAT,d.Cantidad))*10000 ,2),0)
								WHEN d.Cantidad >= 100000						  THEN ISNULL(ROUND((CONVERT(FLOAT,t.TeleInterConsulta)/CONVERT(FLOAT,d.Cantidad))*100000,2),0)
							END,
	TeleInterConsultante = ISNULL(t.teleinterconsulta_consultante,0),
	TeleInterConsultor	 = ISNULL(t.teleinterconsulta_consultor,0),
	TeleConsulta		 = NULL,
	Um_TeleConsulta		 = NULL,
	Num_TeleCon			 = NULL,
	TeleMonitoreo		 = NULL,
	Um_TeleMonitoreo	 = NULL,
	Num_TeleMon			 = NULL
FROM #Den_II_III_Nivel						d
INNER JOIN #Umbrales						u ON d.renaes = u.renaes
LEFT JOIN #TeleInterConsulta_Nivel_II_IIII	t ON d.renaes = t.renaes
INNER JOIN #renaes							r ON d.renaes = r.renaes

INSERT INTO #tabla_reporte
SELECT
	t.*,
	Num = CONVERT(INT,ROUND(t.Num_TelCon_Ptje + T.Num_TelInt_Ptje + t.Num_TelMon_Ptje,0))
FROM 
(SELECT
	t.año,
	t.Mes,
	t.Nivel,
	t.renaes,
	t.den,
	Num_TelInt_Ptje =	CASE 
							WHEN (t.Num_TelInt - t.Um_TeleInter) < 0							THEN 0
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den <= 15000 AND t.Num_TeleCon = 0	AND 
								 (t.Num_TelInt - t.Um_TeleInter) >= 2							THEN 70
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TelInt - t.Um_TeleInter) >= 3	AND t.Num_TeleCon = 0	THEN 70
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den >= 30001 AND t.Num_TeleCon = 0	AND
								 (t.Num_TelInt - t.Um_TeleInter) >= 10							THEN 70
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den <= 15000 AND t.Num_TeleCon = 0	AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 2							THEN ((t.Num_TelInt - t.Um_TeleInter)*70)/2
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 3	AND t.Num_TeleCon = 0	THEN ((t.Num_TelInt - t.Um_TeleInter)*70)/3
							WHEN  t.Nivel = 'Primer Nivel de Atención'	AND t.den <= 30001 AND t.Num_TeleCon = 0	AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 10							THEN ((t.Num_TelInt - t.Um_TeleInter)*70)/10
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000							AND 
								 (t.Num_TelInt - t.Um_TeleInter) >= 2	THEN 60
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TelInt - t.Um_TeleInter) >= 3	THEN 60
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >= 30001							AND
								 (t.Num_TelInt - t.Um_TeleInter) >= 10	THEN 60
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000							AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 2	THEN ((t.Num_TelInt - t.Um_TeleInter)*60)/2
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 3	THEN ((t.Num_TelInt - t.Um_TeleInter)*60)/3
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 30001							AND 
								 (t.Num_TelInt - t.Um_TeleInter) < 10	THEN ((t.Num_TelInt - t.Um_TeleInter)*60)/10
							WHEN  t.Nivel = 'II y III Nivel de Atención' AND t.Categ IN ('II-1','II-2','II-E')		AND
								 (t.Num_TelInt - t.Um_TeleInter) >= 2  THEN 100
							WHEN  t.Nivel = 'II y III Nivel de Atención' AND t.Categ IN ('III-1','III-2','III-E')	AND 
								 (t.Num_TelInt - t.Um_TeleInter) >= 3  THEN 100
							WHEN  t.Nivel = 'II y III Nivel de Atención' AND t.Categ IN ('II-1','II-2','II-E')		AND
								 (t.Num_TelInt - t.Um_TeleInter) <  2  THEN ((t.Num_TelInt - t.Um_TeleInter)*100)/2
							WHEN  t.Nivel = 'II y III Nivel de Atención' AND t.Categ IN ('III-1','III-2','III-E')	AND 
								 (t.Num_TelInt - t.Um_TeleInter) <  3  THEN ((t.Num_TelInt - t.Um_TeleInter)*100)/3
							ELSE 0
						END	,
	t.teleinterconsulta		,
	t.TeleInterConsultante	,
	t.TeleInterConsultor	,
	Num_TelCon_Ptje =	CASE 
							WHEN (t.Num_TeleCon - t.Um_TeleConsulta) < 0 OR t.Num_TeleCon = 0						THEN 0
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND (t.Num_TeleCon - t.Um_TeleConsulta) >= 2	THEN 10
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND (t.Num_TeleCon - t.Um_TeleConsulta) <  2	THEN ((t.Num_TeleCon - t.Um_TeleConsulta)*10)/2
							ELSE 0
						END,
	t.TeleConsulta,
	Num_TelMon_Ptje =	CASE 
							WHEN (t.Num_TeleMon - t.Um_TeleMonitoreo) < 0 						THEN 0
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000 AND t.ServTM = 'No'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 2						THEN 100
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 3 AND t.ServTM = 'No'	THEN 100
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >= 30001 AND t.ServTM = 'No'		AND
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 10						THEN 100
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000 AND t.ServTM = 'No'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 2						THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*100)/2
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 3 AND t.ServTM = 'No'	THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*100)/3
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 30001 AND t.ServTM = 'No'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 10						THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*100)/10
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000 AND t.ServTM = 'Si'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 2						THEN 30
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 3 AND t.ServTM = 'Si'	THEN 30
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >= 30001 AND t.ServTM = 'Si'		AND
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) >= 10						THEN 30
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 15000 AND t.ServTM = 'Si'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 2						THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*30)/2
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den >  15001 AND  t.den <= 30000		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 3 AND t.ServTM = 'Si'	THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*30)/3
							WHEN  t.Nivel = 'Primer Nivel de Atención' AND t.den <= 30001 AND t.ServTM = 'Si'		AND 
								 (t.Num_TeleMon - t.Um_TeleMonitoreo) < 10						THEN ((t.Num_TeleMon - t.Um_TeleMonitoreo)*30)/10
							ELSE 0
						END,
	t.TeleMonitoreo
FROM #Tabla_Reporte_Inicial t) t


set @mes_inicio = @mes_inicio + 1
end

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador

select 
B.diris Diris, CASE 
	WHEN B.DESC_DPTO='LIMA' AND B.DESC_PROV='LIMA' THEN 'LIMA METROPOLITANA'
	WHEN B.DESC_DPTO='LIMA' AND B.DESC_PROV<>'LIMA' THEN 'LIMA PROVINCIAS'
	ELSE B.DESC_DPTO END Departamento
, B.desc_prov Provincia
, B.desc_dist Distrito
, case when b.DIRIS like '%diris%' then b.diris else B.DESC_RED end Red
, b.desc_mred MicroRed
, b.cat_estab
, b.desc_estab eess_nombre
, a.*
INTO #Inkdicador
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes)=convert(int,b.COD_ESTAB)

-- Exportamos el reporte final agrupado
SELECT * FROM #Inkdicador

-- =D