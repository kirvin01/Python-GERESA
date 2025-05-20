--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DPCAN
-- NOMBRE: Mujeres de 30 a 49 años con tamizaje para la detección de lesiones premalignas e 
--		   incipientes de cáncer de cuello uterino
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marin (OGEI)
-- Fecha creación      : 11/12/2024
--*******************************************************************************************

USE BD_HISINDICADORES

--====================
-- FUENTES DE DATOS --
--====================
-- RENAES (SUSALUD)
-- Capturamos el solo EESS de I Nivel de atención según la Ficha:
IF OBJECT_ID('Tempdb..#renaes') IS NOT NULL DROP TABLE #renaes
SELECT
	DISTINCT
	DISA		= r.DESC_DISA	,
	Provincia	= r.DESC_PROV	,
	Distrito	= r.DESC_DIST	,
	renaes		= r.COD_ESTAB	,
	EESS		= r.DESC_ESTAB	,
	r.CAT_ESTAB
INTO #renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK)
WHERE r.CAT_ESTAB IN ('I-1','I-2','I-3','I-4')

--HIS MINSA 2024
IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa
SELECT
	id_tipedad_reg	= h.id_tipedad_reg COLLATE Latin1_General_CI_AS	,
	h.renaes		,
	h.edad_reg 		,
	id_genero		= h.id_genero COLLATE Latin1_General_CI_AS		,
	Fecha_Atencion	= CONVERT(DATE,h.periodo)	,
	Anio			= YEAR(CONVERT(DATE,h.periodo))					,
	Mes				= MONTH(CONVERT(DATE,h.periodo)),
	id_tipitem		= h.id_tipitem COLLATE Latin1_General_CI_AS		,
	cod_item		= h.cod_item COLLATE Latin1_General_CI_AS		,
	h.id_cita		,
	num_doc			= h.num_doc COLLATE Latin1_General_CI_AS		,
	h.id_tipo_doc	,
	valor_lab		= h.valor_lab COLLATE Latin1_General_CI_AS
INTO #his_minsa
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK)
INNER JOIN #renaes r WITH(NOLOCK) ON h.renaes = r.renaes
WHERE h.cod_item IN ('87621','88141.01')
	AND h.id_genero = 'F'
	AND (h.edad_reg between 30 AND 49 
		AND h.id_tipedad_reg = 'A')

-- Asegurados SIS 2024 --
-- La trama de asegurados del SIS-2024 fue remitido por el SIS a pedido de DPCAN
-- Para el observatorio nacional de Cáncer. Fatima Villar (mvillarp@minsa.gob.pe) realizó la solicitud al SIS
/*Para el 2025 se va a tener que actualizar la trama de SIS,
DPCAN deberá de solicitar dicha trama actualizada al SIS*/
IF OBJECT_ID('Tempdb..#sis') IS NOT NULL DROP TABLE #sis
SELECT
	s.RENAES,
	s.Mes,
	Cantidad = SUM(CONVERT(INT,s.Cantidad))
INTO #sis
FROM Observatorio_DPCAN.SIS_Asegurados_2023 s WITH(NOLOCK)
INNER JOIN #renaes r ON s.RENAES = r.renaes
WHERE s.Genero = 'F'
	AND s.Edad BETWEEN 30 AND 49
	AND s.Mes = 12 -- Se congela el denominador a diciembre del año anterior
	AND s.Año = 2023
GROUP BY s.RENAES,
		 s.Mes


-- Mujeres con entrega de resultados el 2023 (Se tiene que calcular del 2024 para excluir del 2025)
-- Leer ficha
--SELECT * FROM BD_HISINDICADORES.dbo.EntregResultados_2023_IVAA_VPH

-- Excluimos las mujeres con entrega de resultado de un año anterior:
DELETE
FROM #his_minsa
WHERE num_doc IN (SELECT num_doc FROM EntregResultados_2023_IVAA_VPH)

-- Disminuimos el universo de denominador en base a las cantidades de mujeres que tuvieron entrega de resultado
UPDATE s SET
	s.Cantidad = IIF((s.Cantidad - e.Cantidad) <= 0,s.Cantidad,s.Cantidad - e.Cantidad)
FROM #sis s
INNER JOIN (SELECT
				e.renaes,
				Cantidad = COUNT(*)
			FROM EntregResultados_2023_IVAA_VPH e
			GROUP BY e.renaes) e ON s.RENAES = e.renaes

--****************************
--		 Automatizador
--****************************
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(	renaes	int,
	año		int,
	mes		int,
	den		int,
	num		int
)

declare @mes_eval	int,
		@mes_inicio int,
		@año		int

set @mes_inicio = 1
set @mes_eval   = 10
set @año		= 2024

while @mes_inicio <= @mes_eval
begin 

--====================
--	  DENOMINADOR   --
--====================
-- Cantidad de asegurados por mes del sis
IF OBJECT_ID('Tempdb..#Den') IS NOT NULL DROP TABLE #Den
SELECT
	renaes  = CONVERT(INT,s.RENAES),
	Mes		= @mes_inicio,
	Cantidad
INTO #Den
FROM #sis s

--====================
--	   NUMERADOR    --
--====================
-- Toma muestra DM-VPH:
IF OBJECT_ID('Tempdb..#Num_VPH_Muestra') IS NOT NULL DROP TABLE #Num_VPH_Muestra
SELECT
	h.*
INTO #Num_VPH_Muestra
FROM
   (SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.id_tipo_doc, h.num_doc
								   ORDER BY h.Muestra_Fecha_Atencion_VPH ASC),
		h.*
	FROM
	   (SELECT
			DISTINCT		
			h.id_tipo_doc				,
			h.num_doc					,
			Muestra_renaes_VPH			= h.renaes,
			Muestra_Fecha_Atencion_VPH	= h.Fecha_Atencion
		FROM #his_minsa h WITH(NOLOCK)
		WHERE h.cod_item = '87621' -- VPH
			AND (h.valor_lab = '' OR h.valor_lab IS NULL)
			AND (h.edad_reg BETWEEN 30 AND 49)
			AND h.id_tipitem = 'D'
			AND MONTH(h.Fecha_Atencion) <= @mes_inicio ) h
	) h
WHERE h.id = 1


-- Entrega de resultados IVAA o DM-VPH:

IF OBJECT_ID('Tempdb..#Num_VPH') IS NOT NULL DROP TABLE #Num_VPH
SELECT
	h.*
INTO #Num_VPH
FROM
   (SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.id_tipo_doc, h.num_doc
								   ORDER BY h.Fecha_Atencion_VPH ASC),
		h.*
	FROM
	   (SELECT
			DISTINCT		
			h.id_tipo_doc		,
			h.num_doc			,
			renaes_VPH			= h.renaes,
			Fecha_Atencion_VPH	= h.Fecha_Atencion
		FROM #his_minsa h WITH(NOLOCK)
		WHERE h.cod_item = '87621' -- VPH
			AND h.valor_lab IN ('N','A')
			AND (h.edad_reg BETWEEN 30 AND 49)
			AND h.id_tipitem = 'D'
			AND MONTH(h.Fecha_Atencion) <= @mes_inicio ) h
	) h
WHERE h.id = 1

IF OBJECT_ID('Tempdb..#Num_IVAA') IS NOT NULL DROP TABLE #Num_IVAA
SELECT
	h.*
INTO #Num_IVAA
FROM
   (SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.id_tipo_doc, h.num_doc
								   ORDER BY h.Fecha_Atencion_IVAA ASC),
		h.*
	FROM
	   (SELECT
			DISTINCT		
			h.id_tipo_doc		,
			h.num_doc			,
			renaes_IVAA			= h.renaes,
			Fecha_Atencion_IVAA	= h.Fecha_Atencion
		FROM #his_minsa h WITH(NOLOCK)
		WHERE h.cod_item = '88141.01' -- IVAA
			AND h.valor_lab IN ('N','A')
			AND (h.edad_reg BETWEEN 30 AND 49)
			AND h.id_tipitem = 'D'
			AND MONTH(h.Fecha_Atencion) <= @mes_inicio ) h
	) h
WHERE h.id = 1

-- Unimos las 2 tabla y marcamos el numerador según IVAA:
IF OBJECT_ID('Tempdb..#TablaIndicador') IS NOT NULL DROP TABLE #TablaIndicador
SELECT
	id_tipo_doc			= IIF(v.id_tipo_doc IS NULL, i.id_tipo_doc, v.id_tipo_doc),
	num_doc				= IIF(v.num_doc IS NULL, i.num_doc, v.num_doc),
	v.renaes_VPH		,
	v.Fecha_Atencion_VPH,
	i.renaes_IVAA		,
	i.Fecha_Atencion_IVAA,
	Num = IIF(i.Fecha_Atencion_IVAA IS NULL, 0, 1)
INTO #TablaIndicador
FROM #Num_VPH v
FULL OUTER JOIN #Num_IVAA i ON v.id_tipo_doc = i.id_tipo_doc AND
								   v.num_doc = i.num_doc

-- Identificamos el numerador según VPH
UPDATE v SET
	v.Num = 1
FROM #TablaIndicador v
INNER JOIN #Num_VPH_Muestra m ON v.id_tipo_doc = m.id_tipo_doc		AND
									 v.num_doc = m.num_doc			AND
								  v.renaes_VPH = m.Muestra_renaes_VPH
WHERE v.Num = 0 AND
	DATEDIFF(day,m.Muestra_Fecha_Atencion_VPH,v.Fecha_Atencion_VPH) <= 45 AND 
	DATEDIFF(day,m.Muestra_Fecha_Atencion_VPH,v.Fecha_Atencion_VPH) >= 0


-- Generamos la tabla final del numerador
IF OBJECT_ID('Tempdb..#Num') IS NOT NULL DROP TABLE #Num
SELECT
	t.renaes,
	t.Anio	,
	t.Mes	,
	Num		= SUM(t.Num)
INTO #Num
FROM
	(SELECT
		renaes	= n.renaes_IVAA,
		Anio	= 2024	,
		Mes		= @mes_inicio	,
		Num		= SUM(n.Num)
	FROM #TablaIndicador n
	WHERE n.Num = 1 AND
		n.renaes_IVAA IS NOT NULL
	GROUP BY n.renaes_IVAA
	UNION
	SELECT
		renaes	= n.renaes_VPH,
		Anio	= 2024	,
		Mes		= @mes_inicio	,
		Num		= SUM(n.Num)
	FROM #TablaIndicador n
	WHERE n.Num = 1 AND
		n.renaes_VPH IS NOT NULL
	GROUP BY n.renaes_VPH ) t
GROUP BY t.renaes,
		 t.Anio	,
		 t.Mes	


--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
INSERT INTO #tabla_reporte
SELECT
	d.renaes,
	año		= 2024,
	d.Mes 	,
	Den		= d.Cantidad,
	Num		= ISNULL(n.Num,0)
FROM #Den d -- 74 799
LEFT JOIN #Num n ON d.renaes = n.renaes

set @mes_inicio = @mes_inicio + 1
end

--%%%%%%%%%%%%%%%%
--	  REPORTE
--%%%%%%%%%%%%%%%%

select 
	Diris			= r.DESC_DISA,
	Departamento	= r.DESC_DPTO,
	Provincia		= r.DESC_PROV,
	Distrito		= r.DESC_DIST,
	red				= r.DESC_RED ,
	r.DESC_ESTAB,
	r.CAT_ESTAB,
	a.*				
from #tabla_reporte a
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r ON r.COD_ESTAB = a.renaes


