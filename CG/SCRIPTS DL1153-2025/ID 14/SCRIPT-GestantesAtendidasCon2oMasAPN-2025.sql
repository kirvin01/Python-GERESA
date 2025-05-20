--===========================================================================================
-- **************** OFICINA DE GESTION DE LA INFORMACION (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°17 - DL 1153 - 2025
-- AREA RESPONSABLE DEL INDICADOR: DGIESP / Direccion Ejecutiva de Salud Sexual y Reproductiva
--								   (DSARE)
-- NOMBRE: Porcentaje de gestantes atendidas con 2 o mas APN en el hospital, referida por
--		   factores de riesgo
--===========================================================================================

--*******************************************************************************************
-- Creado por (2023)   : Piero Romero Marin (OGEI)
-- Fecha creacion      : 27/03/2024
---------------------------------------------------------------------------------------------
-- Fecha Actualizacion : 26/11/2024
-- Motivo              : Modificaciones de la FICHA para los Indicadores del 2025
-- Modificado por	   : (OGEI)
--*******************************************************************************************
USE BD_HISINDICADORES
GO

--***************************************************
--					SINTAXIS
--***************************************************
DECLARE
	@mes_eval	INT,
	@anio		INT 

SET @mes_eval = 10		--<======= Mes de evaluación
SET @anio	  = 2024	--<======= Año de evaluación 

--***************************************************
--				BASES DE DATOS.
--***************************************************
-- SELECT TOP 1000 * FROM BD_BACKUP_OGEI.dbo.Renaes
-- SELECT DISTINCT CAT_ESTAB FROM BD_BACKUP_OGEI.dbo.Renaes ORDER BY CAT_ESTAB

-- RENAES EXCLUYE LOS QUE NO CUENTEN CON CATEGORIA (SD)
IF OBJECT_ID('Tempdb..#Renaes') IS NOT NULL DROP TABLE #Renaes
SELECT
	r.diris		,
	r.DESC_DPTO	,
	r.DESC_PROV	,
	r.desc_dist	,
	r.DESC_RED	,
	r.desc_mred	,
	r.cat_estab	,
	r.desc_estab,
	r.COD_ESTAB
INTO #Renaes
FROM BD_BACKUP_OGEI.dbo.Renaes r WITH(NOLOCK)
WHERE r.CAT_ESTAB <> 'SD'
-- CATEGORIAS EXISTENTES ('I-1','I-2','I-3','I-4','II-1','II-2','II-E','III-1','III-2','III-E','SD')

--HIS MINSA 2024
IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa
SELECT
	CONVERT(DATE,periodo) [fecha_atencion],
	CONVERT(INT,renaes) [renaes],
	id_tipcond_serv,
	num_doc,
	id_tipitem [tipo_dx],
	cod_item,
	valor_lab
INTO #his_minsa
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa WITH(NOLOCK)
INNER JOIN #Renaes r WITH(NOLOCK) ON CONVERT(INT,r.COD_ESTAB) = CONVERT(INT,renaes)
WHERE
YEAR(periodo) = @anio AND
id_tipo_doc = 1 AND
-- i_rownum_lab = 1 AND -- HACE UNICO EL REGISTRO DE ATENCION (ID_CITA)
sw = 1 AND
cod_item IN ('Z3591','Z3592','Z3593','Z359') --Codigos de Atencion Prenatal
AND r.CAT_ESTAB IN ('II-1','II-2','II-E','III-1','III-2','III-E')

--REFCON 2024
IF OBJECT_ID('Tempdb..#refcon') IS NOT NULL DROP TABLE #refcon
SELECT
num_doc,
fecha_envio [fecha],
cod_unico [renaes_origen],
cod_unico_d [renaes_destino],
ups_destino,
id_ciex,
desc_estado
INTO #refcon
FROM BD_BACKUP_OGEI.dbo.TramaREFCON WITH(NOLOCK)
WHERE
Anio = @anio AND
P_id_sexo = 'FEMENINO' AND -- AGREGADO: FILTRA MUJERES
tipo_traslado = 'REFERENCIA'			AND
ups_destino LIKE '%CONSULTA%EXTERNA%'	AND
tip_doc_pac	  = 'DNI'					AND
sw = 1						AND
(
	(	id_ciex IN (
		'O342',												-- Cesarea anterior
		'O990',												-- Anemia Severa
		'O13X',												-- Hipertension Gestacional
		'O23' ,'O231','O232','O233','O234','O235',			-- ITU
		'O440',												-- Placenta Previa
	    'O987',												-- VIH
		'O241','O242','O243','O244',						-- Diabetes
		'O260',												-- Aumento excesivo de peso en embarazo
		'O300',												-- Embarazo Multiple

		'M320','M321','M328','M329','M32','M3212'			-- Enfermedades Inmunológicas (Observado: 'M32','M3212')
		)										
	)	OR
	( 
		(SUBSTRING(id_ciex, 1,3) = 'M05') OR 					-- Artritis reumatoidea (Observado: 'M05')
		(SUBSTRING(id_ciex, 1,3) BETWEEN 'E00' AND 'E07') OR	-- Enfermedades Tiroideas (Observado: 'E007' ? E00 a E07)
		(SUBSTRING(id_ciex, 1,3) BETWEEN 'Q00' AND 'Q09') OR	-- Cardiopatías (valvulares y congénitas) (Observado: 'E007' ? Q00 a Q99)
		(SUBSTRING(id_ciex, 1,3) BETWEEN 'A15' AND 'A19')		-- Tuberculosis Activa (A15 – A19)
	) OR						
	(	--Gestantes Adolescente
		id_ciex IN('Z3591','Z3592','Z3593')   AND					
		P_edad_actual <= 15 AND P_tipoedad = 'A'
	)
)

-- Quitamos los "OBSERVADO" y "RECHAZADO"
DELETE FROM #refcon
WHERE desc_estado IN ('OBSERVADO','RECHAZADO')


--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
-- Total, de gestantes que han sido referidas del 1° nivel de atención por
-- complicaciones o factores de riesgo específicos
IF OBJECT_ID('Tempdb..#Den') IS NOT NULL DROP TABLE #Den
SELECT
t.*,
Den = 1
INTO #Den
FROM
(	-- LISTA LAS REFERECIAS 
	SELECT DISTINCT
	ROW_NUMBER() OVER(PARTITION BY rf.num_doc ORDER BY rf.fecha ASC) [id],
	rf.num_doc,
	CONVERT(DATE,rf.fecha) [Fecha],
	rf.id_ciex,
	rf.renaes_destino,
	rf.renaes_origen
	FROM #refcon rf WITH(NOLOCK)
	INNER JOIN #Renaes r1 WITH(NOLOCK) ON CONVERT(INT,rf.renaes_destino) = CONVERT(INT,r1.COD_ESTAB)
	INNER JOIN #Renaes r2 WITH(NOLOCK) ON CONVERT(INT,rf.renaes_origen)  = CONVERT(INT,r2.COD_ESTAB)
	WHERE
	r1.CAT_ESTAB IN ('II-1','II-2','II-E','III-1','III-2','III-E') AND
	r2.CAT_ESTAB IN ('I-1','I-2','I-3','I-4') AND
	MONTH(rf.fecha) <= @mes_eval AND
	YEAR(rf.fecha)   = @anio
) t
WHERE t.id = 1

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
-- Total de gestante del denominador con 2 o más Atenciones Prenatales (APN) en el
-- hospital que han sido referidas por factores de riesgo específicos.
IF OBJECT_ID('Tempdb..#Num_Aten') IS NOT NULL DROP TABLE #Num_Aten
SELECT DISTINCT
h.renaes,
h.num_doc,
h.fecha_atencion
INTO #Num_Aten
FROM #his_minsa h WITH(NOLOCK)
INNER JOIN #Den d WITH(NOLOCK) ON h.renaes = d.renaes_destino AND h.num_doc = d.num_doc COLLATE Latin1_General_CI_AS
WHERE
h.fecha_atencion >= d.Fecha AND
MONTH(h.fecha_atencion) <= @mes_eval AND -- ¿...?
YEAR(h.fecha_atencion) = @anio

IF OBJECT_ID('Tempdb..#Num') IS NOT NULL DROP TABLE #Num
SELECT
n.renaes,
n.num_doc,
MIN(fecha_atencion) [Fecha_Aten],
1 [Num],
COUNT(*) [CantAten]
INTO #Num
FROM #Num_Aten n
GROUP BY n.num_doc, n.renaes
HAVING COUNT(*) > 1

--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Tabla_Reporte') IS NOT NULL DROP TABLE #Tabla_Reporte
SELECT
d.renaes_destino [renaes],
d.num_doc,
d.Fecha [Fecha_Referencia],
MONTH(d.Fecha) [Mes],
YEAR(d.Fecha) [Anio],
d.Den,
ISNULL(n.Num,0) [Num],
ISNULL(n.CantAten,0) [CantAten]
INTO #Tabla_Reporte
FROM #Den d WITH(NOLOCK)
LEFT JOIN #Num n WITH(NOLOCK) ON d.renaes_destino = n.renaes AND d.num_doc = n.num_doc COLLATE Modern_Spanish_CI_AS


--===================
-- REPORTE
--===================
IF OBJECT_ID(N'dbo.DL1153_2025_CG17_APNreferencia',N'U') IS NOT NULL
DROP TABLE #DL1153_2025_CG17_APNreferencia -- CAMBIAR AQUI TABLA TEMPORAL

SELECT
B.diris [Diris],
CASE 
	WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV  = 'LIMA' THEN 'LIMA METROPOLITANA'
	WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
	ELSE B.DESC_DPTO
END [Departamento],
B.desc_prov [Provincia],
B.desc_dist [Distrito],
CASE
	WHEN b.DIRIS LIKE '%diris%' THEN b.diris
	ELSE B.DESC_RED
END [Red],
b.desc_mred [MicroRed],
b.cat_estab,
b.desc_estab [eess_nombre],
a.*					
INTO #DL1153_2025_CG17_APNreferencia
FROM #tabla_reporte a
INNER JOIN #Renaes b ON CONVERT(INT,a.renaes) = CONVERT(INT,b.COD_ESTAB)

-- SELECT * from #DL1153_2025_CG17_APNreferencia

SELECT
	t.Diris			,
	t.Departamento	,
	t.Provincia		,
	t.Distrito		,
	t.Red			,
	t.MicroRed		,
	t.renaes		,
	t.CAT_ESTAB		,
	t.eess_nombre	,
	t.Mes			,
	t.Anio			,
	Den				= SUM(t.Den),
	Num				= SUM(t.Num)
FROM #DL1153_2025_CG17_APNreferencia t WITH(NOLOCK)
GROUP BY	t.Diris			,
			t.Departamento	,
			t.Provincia		,
			t.Distrito		,
			t.Red			,
			t.MicroRed		,
			t.renaes		,
			t.CAT_ESTAB		,
			t.eess_nombre	,
			t.Mes			,
			t.Anio


drop table #his_minsa
drop table #refcon
-- =D