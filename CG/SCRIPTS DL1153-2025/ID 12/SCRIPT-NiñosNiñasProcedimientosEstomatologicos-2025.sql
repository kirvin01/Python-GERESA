--***************************************************
--				BASES DE DATOS.
--***************************************************

-- RENAES:
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
SELECT
	DISTINCT
	DISA		= r.DESC_DISA	,
	Provincia	= r.DESC_PROV	,
	Distrito	= r.DESC_DIST	,
	renaes		= r.COD_ESTAB	,
	EESS		= r.DESC_ESTAB	,
	RED			= r.DESC_RED,
	r.CAT_ESTAB
INTO #renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK)
WHERE r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4')
	OR (r.CAT_ESTAB = 'II-1' AND r.CAMAS <= 50)


--HIS MINSA 
IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa
select
	id_cita									, 
	fecha_registro							, 
	fecha_atencion	= convert(date,periodo)	, 
	renaes			= convert(int,renaes)	, 
	id_tipcond_estab						, 
	id_tipcond_serv							, 
	id_tipo_doc		,
	num_doc									, 
	edad_reg								, 
	id_tipedad_reg,
	tipo_dx			= id_tipitem			, 
	cod_item  								, 
	valor_lab
into #his_minsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa WITH(NOLOCK)
where	id_tipo_doc = 1 AND
				 sw = 1 AND
		substring(rtrim(ltrim(id_ups)),1,4) = '3033' AND				-- UPS servicios Odontologicas.
		(convert(int,edad_reg) between 1 and 6	 AND
		id_tipedad_reg						= 'A') OR
		(convert(int,edad_reg) between 6 and 11	 AND
		id_tipedad_reg						= 'M')


--Excluir Atenciones de Telemedicina.
delete from #his_minsa 
where id_cita in (
					select
						distinct
						id_cita
					from #his_minsa
					where substring(cod_item,1,5)='99499'
				 ) 

--Capturamos Códigos de la ficha
IF OBJECT_ID('Tempdb..#his_minsa_sb') IS NOT NULL DROP TABLE #his_minsa_sb
SELECT
	h.*
INTO #his_minsa_sb
FROM #his_minsa h WITH(NOLOCK)--1 6101 682
INNER JOIN #renaes r ON h.renaes = r.renaes
WHERE id_cita IN (SELECT
					 id_cita
				  FROM #his_minsa 
				  WHERE cod_item IN ('D0150','D1330','D1310','D1206','D1110','D1351','D1352'))

-- DENOMINADOR
IF OBJECT_ID('Tempdb..#Den_Prev') IS NOT NULL DROP TABLE #Den_Prev
SELECT
	t.*
INTO #Den_Prev -- 420 244
FROM
	(
	SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.id_tipo_doc, h.num_doc
								   ORDER BY h.fecha_atencion DESC),
		h.*
	--INTO #Tab1
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.id_tipo_doc	,
			h.num_doc		,
			h.fecha_atencion,
			Mes = MONTH(h.fecha_atencion)
		-- 420 244 dni unicos -- 428 067 dni - renaes 
		FROM #his_minsa_sb h WITH(NOLOCK) -- 4 831 536
		WHERE h.tipo_dx = 'D'
			AND h.cod_item = 'D0150'
			AND h.id_tipcond_serv IN ('N','R')
		) h
	) t
WHERE t.id = 1


-- NUMERADOR
-- Instrucción de Higiene Oral
IF OBJECT_ID('Tempdb..#Num_HO') IS NOT NULL DROP TABLE #Num_HO
SELECT
	h.*
INTO #Num_HO
FROM
	(
	SELECT
		Id = ROW_NUMBER() OVER(PARTITION BY t.num_doc
								   ORDER BY t.fecha_atencion ASC),
		t.*
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.num_doc		,
			h.fecha_atencion,
			Mes				= MONTH(h.fecha_atencion),
			HO				= 1
		FROM #his_minsa_sb h WITH(NOLOCK)
		WHERE h.tipo_dx		= 'D'
			AND h.cod_item	= 'D1330'
			AND h.valor_lab = '2'
		) t
	) h
WHERE Id = 1

-- Asesoría Nutricional para el control de enfermedades dentales
IF OBJECT_ID('Tempdb..#Num_AN') IS NOT NULL DROP TABLE #Num_AN
SELECT
	h.*
INTO #Num_AN
FROM
	(
	SELECT
		Id = ROW_NUMBER() OVER(PARTITION BY t.num_doc
								   ORDER BY t.fecha_atencion ASC),
		t.*
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.num_doc		,
			h.fecha_atencion,
			Mes				= MONTH(h.fecha_atencion),
			AN				= 1
		FROM #his_minsa_sb h WITH(NOLOCK)
		WHERE h.tipo_dx		= 'D'
			AND h.cod_item	= 'D1310'
			AND h.valor_lab = '2'
		) t
	) h
WHERE Id = 1


-- Aplicación de Flúor Barniz
IF OBJECT_ID('Tempdb..#Num_FB') IS NOT NULL DROP TABLE #Num_FB
SELECT
	h.*
INTO #Num_FB
FROM
	(
	SELECT
		Id = ROW_NUMBER() OVER(PARTITION BY t.num_doc
								   ORDER BY t.fecha_atencion ASC),
		t.*
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.num_doc		,
			h.fecha_atencion,
			Mes				= MONTH(h.fecha_atencion),
			FB				= 1
		FROM #his_minsa_sb h WITH(NOLOCK)
		WHERE h.tipo_dx		= 'D'
			AND h.cod_item	= 'D1206'
			AND h.valor_lab = '2'
		) t
	) h
WHERE Id = 1


-- Profilaxis dental 
IF OBJECT_ID('Tempdb..#Num_PD') IS NOT NULL DROP TABLE #Num_PD
SELECT
	h.*
INTO #Num_PD
FROM
	(
	SELECT
		Id = ROW_NUMBER() OVER(PARTITION BY t.num_doc
								   ORDER BY t.fecha_atencion ASC),
		t.*
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.num_doc		,
			h.fecha_atencion,
			Mes				= MONTH(h.fecha_atencion),
			PD				= 1
		FROM #his_minsa_sb h WITH(NOLOCK)
		WHERE h.tipo_dx		= 'D'
			AND h.cod_item	= 'D1110'
			AND h.valor_lab = '2'
		) t
	) h
WHERE Id = 1


-- aplicación de sellantes
IF OBJECT_ID('Tempdb..#Num_AS') IS NOT NULL DROP TABLE #Num_AS
SELECT
	h.*
INTO #Num_AS
FROM
	(
	SELECT
		Id = ROW_NUMBER() OVER(PARTITION BY t.num_doc
								   ORDER BY t.fecha_atencion ASC),
		t.*
	FROM
		(
		SELECT
			DISTINCT
			h.renaes		,
			h.num_doc		,
			h.fecha_atencion,
			Mes				= MONTH(h.fecha_atencion),
			ASs				= 1
		FROM #his_minsa_sb h WITH(NOLOCK)
		WHERE h.tipo_dx		= 'D'
			AND h.cod_item	= 'D1351'
			AND h.valor_lab IN ('1','2','3','4')
			AND h.id_cita IN (SELECT id_cita FROM #his_minsa_sb WHERE cod_item	= 'D1351' AND valor_lab = 'FIN')
			AND convert(int,edad_reg) between 1 and 6	 AND
				id_tipedad_reg	= 'A'
		) t
	) h
WHERE Id = 1


--Unimos los Procedimientos a nivel nominal:
-- Trama Nominal Universo:
IF OBJECT_ID('Tempdb..#Num') IS NOT NULL DROP TABLE #Num
SELECT
	DISTINCT
	h.num_doc	,
	renaes_AN	= CONVERT(INT,NULL)	,
	Mes_AN		= CONVERT(INT,NULL)	,
	AN			= CONVERT(INT,0)	,
	renaes_FB	= CONVERT(INT,NULL)	,
	Mes_FB		= CONVERT(INT,NULL)	,
	FB			= CONVERT(INT,0)	,
	renaes_HO	= CONVERT(INT,NULL)	,
	Mes_HO		= CONVERT(INT,NULL)	,
	HO			= CONVERT(INT,0)	,
	renaes_PD	= CONVERT(INT,NULL)	,
	Mes_PD		= CONVERT(INT,NULL)	,
	PD			= CONVERT(INT,0)	,
	renaes_AS	= CONVERT(INT,NULL)	,
	Mes_AS		= CONVERT(INT,NULL)	,
	ASs			= CONVERT(INT,0)	
INTO #Num
FROM #his_minsa_sb h WITH(NOLOCK)
WHERE h.tipo_dx		= 'D'
	AND h.cod_item	IN ('D1110','D1330','D1206','D1310','D1351')
	AND h.valor_lab = '2'

-- Actualizamos AN
UPDATE n SET
	n.renaes_AN = a.renaes	,
	n.Mes_AN	= a.Mes		,
	n.AN		= a.AN
FROM #Num n --199 533
INNER JOIN #Num_AN a on a.num_doc = n.num_doc

-- Actualizamos FB
UPDATE n SET
	n.renaes_FB = f.renaes	,
	n.Mes_FB	= f.Mes		,
	n.FB		= f.FB
FROM #Num n --199 533
INNER JOIN #Num_FB f on f.num_doc = n.num_doc

-- Actualizamos HO
UPDATE n SET
	n.renaes_HO = h.renaes	,
	n.Mes_HO	= h.Mes		,
	n.HO		= h.HO
FROM #Num n --199 533
INNER JOIN #Num_HO h on h.num_doc = n.num_doc

-- Actualizamos PD
UPDATE n SET
	n.renaes_PD = p.renaes	,
	n.Mes_PD	= p.Mes		,
	n.PD		= p.PD
FROM #Num n --199 533
INNER JOIN #Num_PD p on p.num_doc = n.num_doc

-- Actualizamos AS
UPDATE n SET
	n.renaes_AS = p.renaes	,
	n.Mes_AS	= p.Mes		,
	n.ASs		= p.ASs
FROM #Num n --199 533
INNER JOIN #Num_AS p on p.num_doc = n.num_doc

-- Actualizamos Numerador
IF OBJECT_ID('Tempdb..#Num_Prev') IS NOT NULL DROP TABLE #Num_Prev
SELECT
	n.*,
	reanes = CASE
				WHEN n.Num = 1 AND n.renaes_PD IS NOT NULL THEN n.renaes_PD
				WHEN n.Num = 1 AND n.renaes_HO IS NOT NULL THEN n.renaes_HO
				WHEN n.Num = 1 AND n.renaes_FB IS NOT NULL THEN n.renaes_FB
				WHEN n.Num = 1 AND n.renaes_AN IS NOT NULL THEN n.renaes_AN
				WHEN n.Num = 0							   THEN NULL
			END,
	Mes = IIF(n.Num = 1,n.Mes_PD,NULL)
INTO #Num_Prev
FROM
	(SELECT
		n.*,
		Num = IIF(n.AN = 1 AND n.FB = 1 AND n.HO = 1 AND n.PD = 1, 1,0)
	FROM #Num n --199 533
	) n

--REPORTE:
SELECT
	r.DISA		,
	r.Provincia	,
	r.Distrito	,
	r.RED		,
	r.CAT_ESTAB	,
	d.renaes	,
	r.EESS		,
	d.Mes		,
	--d.num_doc	,
	Den			= COUNT(*),
	Num			= SUM(ISNULL(n.Num,0)),
	AN			= SUM(ISNULL(n.AN,0)),
	FB			= SUM(ISNULL(n.FB,0)),
	HO			= SUM(ISNULL(n.HO,0)),
	PD			= SUM(ISNULL(n.PD,0)),
	Ass			= SUM(ISNULL(n.ASs,0))
FROM #Den_Prev d -- 420 445
LEFT JOIN #Num_Prev n ON d.num_doc = n.num_doc -- 203 853
INNER JOIN #renaes r  ON d.renaes  = r.renaes
GROUP BY r.DISA		,
		 r.Provincia,
		 r.Distrito	,
		 r.RED,
		 r.CAT_ESTAB,
		 d.renaes	,
		 r.EESS		,
		 d.Mes