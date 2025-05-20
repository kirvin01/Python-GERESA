--=============================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ********************
-- FICHA: INDICADOR N°00 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR:  (UFANS)
-- NOMBRE:  Porcentaje de niños y niñas de 6 a 35 meses con diagnóstico de anemia
--          del Total de los casos esperados de anemia
--=============================================================================================

--*******************************************************************************************
-- Creado por		   : Edson Donayre Uchuya
-- Motivo              : Indicador Nuevo para el CG 2025
-- Fecha creación      : 03/12/2024
--*******************************************************************************************

USE BD_HISINDICADORES
GO
--***************************************************
--				BASES DE DATOS.
--***************************************************

---- PADRON NOMINAL -------^
IF Object_id(N'tempdb..#Tmp_PadronNominal_Preliminar',N'U') IS NOT NULL DROP TABLE #Tmp_PadronNominal_Preliminar;
SELECT
p.CO_UBIGEO_INEI,
CAsE
WHEN IIF(p.NU_DNI_MENOR='',NULL,p.NU_DNI_MENOR) IS NOT NULL THEN 'DNI'
WHEN IIF(p.NU_CNV='',NULL,p.NU_CNV) IS NOT NULL THEN 'CNV'
WHEN IIF(p.NU_CUI='',NULL,p.NU_CUI) IS NOT NULL THEN 'CUI'
ELSE 'CODP'
END [tip_doc_eval],
CASE
WHEN IIF(p.NU_DNI_MENOR='',NULL,p.NU_DNI_MENOR) IS NOT NULL THEN p.NU_DNI_MENOR
WHEN IIF(p.NU_CNV='',NULL,p.NU_CNV) IS NOT NULL THEN p.NU_CNV
WHEN IIF(p.NU_CUI='',NULL,p.NU_CUI) IS NOT NULL THEN p.NU_CUI
ELSE p.CO_PADRON_NOMINAL
END [num_doc_eval],
p.FE_NAC_MENOR,
DATEADD(DAY,180,p.FE_NAC_MENOR) [_01_180D_06M],
DATEADD(DAY,364,p.FE_NAC_MENOR) [_01_364D_11M],
DATEADD(DAY,365,p.FE_NAC_MENOR) [_02_365D_12M],
DATEADD(DAY,729,p.FE_NAC_MENOR) [_02_729D_23M],
DATEADD(DAY,730,p.FE_NAC_MENOR) [_03_730D_35M],
DATEADD(DAY,1079,p.FE_NAC_MENOR) [_03_1079D_35M]
INTO #Tmp_PadronNominal_Preliminar
FROM BD_BACKUP_OGEI.dbo.TramaPadronNominal p
WHERE
(p.sw_cnv=1 OR p.sw_pn=1) AND
p.TI_SEGURO_MENOR = 1 -- SIN SEGURO / SIS

-- ELIMINA NIÑA(O) CON CNV QUE YA CUENTA CON DNI
DELETE FROM #Tmp_PadronNominal_Preliminar
WHERE
num_doc_eval IN(
    SELECT num_doc_eval
    FROM #Tmp_PadronNominal_Preliminar
    GROUP BY num_doc_eval
    HAVING COUNT(num_doc_eval)>1
) AND
tip_doc_eval = 'CNV'

-- DATA HIS MINSA 2024
IF Object_id(N'tempdb..#Data_HisMinsa',N'U') IS NOT NULL DROP TABLE #Data_HisMinsa;
SELECT
renaes,
CONVERT(date,periodo) fecha_atencion,
id_cita,
id_tipo_doc,
num_doc,
id_tipitem,
i_rownum_lab,
valor_lab,
id_corr_diag,
cod_item
INTO #Data_HisMinsa
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa WITH(NOLOCK)
WHERE
sw = 1 AND
id_tipo_doc IN(1,6) AND ---SOLO DNI Y CNV
id_tipitem = 'D' AND
cod_item IN(
    'D509','D649',          -- DX ANEMIA
    '85018','85018.01',     -- DOSAJE HEMOGLOBINA
    '99199.17'              -- TRATAMIENTO
)

-- DATA HIS MINSA 2023
INSERT INTO #Data_HisMinsa
SELECT 
renaes,
CONVERT(date,periodo) fecha_atencion,
id_cita,
id_tipo_doc,
num_doc,
id_tipitem,
i_rownum_lab,
valor_lab,
id_corr_diag,
cod_item
FROM BD_BACKUP_OGEI_2023.dbo.TramaHisMinsa WITH(NOLOCK)
WHERE
sw = 1 AND
id_tipo_doc IN(1,6) AND ---SOLO DNI Y CNV
id_tipitem = 'D' AND
cod_item IN(
    'D509','D649',          -- DX ANEMIA
    '85018','85018.01',     -- DOSAJE HEMOGLOBINA
    '99199.17'              -- TRATAMIENTO
)

-- DATA HIS MINSA 2022
INSERT INTO #Data_HisMinsa
SELECT 
renaes,
CONVERT(date,periodo) fecha_atencion,
id_cita,
id_tipo_doc,
num_doc,
id_tipitem,
i_rownum_lab,
valor_lab,
id_corr_diag,
cod_item
FROM BD_FUENTES_HIS.dbo.TramaHisMinsa_2022_20230302 WITH(NOLOCK)
WHERE
sw = 1 AND
id_tipo_doc IN(1,6) AND ---SOLO DNI Y CNV
id_tipitem = 'D' AND
cod_item IN(
    'D509','D649',          -- DX ANEMIA
    '85018','85018.01',     -- DOSAJE HEMOGLOBINA
    '99199.17'              -- TRATAMIENTO
)

-- DATA HIS MINSA 2021
INSERT INTO #Data_HisMinsa
SELECT
renaes,
CONVERT(date,periodo) fecha_atencion,
id_cita,
id_tipo_doc,
num_doc,
id_tipitem,
ROW_NUMBER() OVER(PARTITION BY id_cita, cod_item ORDER BY id_cita, cod_item) [i_rownum_lab],
valor_lab,
ROW_NUMBER() OVER(PARTITION BY id_cita ORDER BY id_cita) [id_corr_diag],
cod_item
FROM BD_FUENTES_HIS.dbo.TramaHisMinsa_2021_20220308 WITH(NOLOCK)
WHERE
sw = 1 AND
id_tipo_doc IN(1,6) AND ---SOLO DNI Y CNV
id_tipitem = 'D' AND
cod_item IN(
    'D509','D649',          -- DX ANEMIA
    '85018','85018.01',     -- DOSAJE HEMOGLOBINA
    '99199.17'              -- TRATAMIENTO
)

--***************************************************
--	INGRESO DE PARAMETROS PARA PROCESAR EL INDICADOR
--***************************************************

DECLARE @mes_inicio INT, @mes_eval INT, @año INT, @fecha_eval_inicial DATE, @fecha_eval_final DATE

SET @mes_inicio = 1		--<========= Mes inicio
SET @mes_eval	= 10		--<========= Mes de evaluación
SET @año		= 2024  --<========= Año de evaluación 


-- Fecha inicial: Primer día del mes de inicio
SET @fecha_eval_inicial = CONVERT(DATE, CAST(@año AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(@mes_inicio AS VARCHAR(2)), 2) + '-01')

-- Fecha final: Último día del mes de fin
SET @fecha_eval_final = EOMONTH(CONVERT(DATE, CAST(@año AS VARCHAR(4)) + '-' + RIGHT('0' + CAST(@mes_eval AS VARCHAR(2)), 2) + '-01'))

--***************************************************
--				ALMACEN DENOMINADOR.
--***************************************************
IF Object_id(N'tempdb..#Nominal_Denominador_Padron',N'U') IS NOT NULL DROP TABLE #Nominal_Denominador_Padron;
CREATE TABLE #Nominal_Denominador_Padron(
    codigo_seguimiento VARCHAR(15),
    numero_documento VARCHAR(8),
    fecha_nacimiento DATE,
    ubigeo INT,
    categoria_nino VARCHAR(6),
    fecha_min_evaluacion DATE,
    fecha_max_evaluacion DATE,
    denominador INT
)

--***************************************************
--	NIÑOS(AS) PADRON PARA EL PERIODO DE EVALUACION
--***************************************************

-- *****************************************************
-- DENOMINADOR
-- *****************************************************

-- %%%%%%%%%%%%%%%%%%%%
-- CRITERIO 1
-- %%%%%%%%%%%%%%%%%%%%
-- DNI únicos de niños que cumplen entre 6 a 35 meses de edad en el período de evaluación,
-- con SIS, sin datos de seguro y sin seguro (población MINSA y Gobierno Regional)
-- registrados en el padrón nominal 

-- DENOMINADOR: INSERTA NIÑOS PARA EL PERIODO DE EVALUACION
INSERT INTO #Nominal_Denominador_Padron
SELECT DISTINCT
CONCAT(pp.num_doc_eval,'-06-11M'),
pp.num_doc_eval,
pp.FE_NAC_MENOR,
pp.CO_UBIGEO_INEI,
'06-11M' [categoria_nino],
_01_180D_06M,
_01_364D_11M,
'1' [Den]
FROM #Tmp_PadronNominal_Preliminar pp
WHERE
pp.tip_doc_eval IN('DNI','CNV') AND
(pp._01_364D_11M BETWEEN @fecha_eval_inicial AND @fecha_eval_final)

INSERT INTO #Nominal_Denominador_Padron
SELECT DISTINCT
CONCAT(pp.num_doc_eval,'-12-23M'),
pp.num_doc_eval,
pp.FE_NAC_MENOR,
pp.CO_UBIGEO_INEI,
'12-23M' [categoria_nino],
_02_365D_12M,
_02_729D_23M,
'1' [Den]
FROM #Tmp_PadronNominal_Preliminar pp
WHERE
pp.tip_doc_eval IN('DNI','CNV') AND
(pp._02_729D_23M BETWEEN @fecha_eval_inicial AND @fecha_eval_final)

INSERT INTO #Nominal_Denominador_Padron
SELECT DISTINCT
CONCAT(pp.num_doc_eval,'-24-35M'),
pp.num_doc_eval,
pp.FE_NAC_MENOR,
pp.CO_UBIGEO_INEI,
'24-35M' [categoria_nino],
_03_730D_35M,
_03_1079D_35M,
'1' [Den]
FROM #Tmp_PadronNominal_Preliminar pp
WHERE
pp.tip_doc_eval IN('DNI','CNV') AND
(pp._03_1079D_35M BETWEEN @fecha_eval_inicial AND @fecha_eval_final)

-- *****************************************************
-- NUMERADOR
-- *****************************************************

-- %%%%%%%%%%%%%%%%%%%%
-- CRITERIO 1
-- %%%%%%%%%%%%%%%%%%%%
-- Del padron nominal, el primer diagnóstico de anemia CIE X: D509 o D649 + Tipo Dx: D para los Niños entre los 6 a 35 meses de edad
IF Object_id(N'tempdb..#Tmp_Numerador_Anemia_Previo',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Anemia_Previo;
SELECT
d.codigo_seguimiento,
d.numero_documento,
d.categoria_nino,
d.fecha_nacimiento,
d.ubigeo,
d.fecha_min_evaluacion,
d.fecha_max_evaluacion,
h.id_cita,
ROW_NUMBER() OVER(PARTITION BY d.codigo_seguimiento ORDER BY h.fecha_atencion ASC) [orden_atencion],
h.fecha_atencion
INTO #Tmp_Numerador_Anemia_Previo
FROM #Nominal_Denominador_Padron d
INNER JOIN #Data_HisMinsa h ON d.numero_documento = h.num_doc
WHERE
h.i_rownum_lab = 1 AND
h.cod_item IN('D509','D649')
AND h.fecha_atencion BETWEEN d.fecha_min_evaluacion AND d.fecha_max_evaluacion

-- CONSIDERA EL PRIMER DX DE ANEMIA
IF Object_id(N'tempdb..#Tmp_Numerador_Anemia',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Anemia;
SELECT
ph.codigo_seguimiento,
ph.numero_documento,
ph.categoria_nino,
ph.fecha_nacimiento,
ph.ubigeo,
ph.fecha_min_evaluacion,
ph.fecha_max_evaluacion,
ph.id_cita,
ph.fecha_atencion [fecha_Dx_Anemia],
1 [Valor_Numerador_Dx_Anemia]
INTO #Tmp_Numerador_Anemia
FROM #Tmp_Numerador_Anemia_Previo ph
WHERE
ph.orden_atencion = 1 -- PRIMER DX ANEMIA DENTRO DEL RANGO

-- %%%%%%%%%%%%%%%%%%%%
-- CRITERIO 2
-- %%%%%%%%%%%%%%%%%%%%
-- A partir del primer diagnóstico definitivo de anemia CIE X: D509 o D649 + Tipo Dx: D
-- entre los 180 a 364 días de edad, 365 a 729 días de edad, 730 a 1079 días de edad
-- con inicio de tratamiento de anemia CPMS: 99199.17 + Tipo Dx: D (MISMA ATENCION)
-- con dosaje de hemoglobina CPMS: 85018 o 85018.01 + Tipo Dx: D hasta 7 días

IF Object_id(N'tempdb..#Tmp_Numerador_Tratamiento_Previo',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Tratamiento_Previo;
SELECT
a.codigo_seguimiento,
a.numero_documento,
ROW_NUMBER() OVER(PARTITION BY a.codigo_seguimiento ORDER BY h.fecha_atencion ASC) [orden_atencion],
h.id_cita,
h.fecha_atencion
INTO #Tmp_Numerador_Tratamiento_Previo
FROM #Tmp_Numerador_Anemia a
INNER JOIN #Data_HisMinsa h ON a.numero_documento = h.num_doc AND a.id_cita = h.id_cita -- ASEGURO QUE EL TTO ESTE EN LA MISMA ATC DONDE SE DX ANEMIA
WHERE
h.i_rownum_lab = 1 AND  -- EL PRIMER LAB DEL DX BUSCADO
h.cod_item = '99199.17' -- TRATAMIENTO ANEMIA


-- OBTIENE EL PRIMER TRATAMIENTO PARA DX ANEMIA
IF Object_id(N'tempdb..#Tmp_Numerador_Tratamiento_Anemia',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Tratamiento_Anemia;
SELECT
ph.codigo_seguimiento,
ph.numero_documento,
ph.id_cita,
ph.fecha_atencion [fecha_Tto_Anemia],
1 [Valor_Numerador_Tratamiento]
INTO #Tmp_Numerador_Tratamiento_Anemia
FROM #Tmp_Numerador_Tratamiento_Previo ph
WHERE
orden_atencion = 1 -- PRIMER TRATAMIENTO DENTRO DEL RANGO

-- %%%%%%%%%%%%%%%%%%%%
-- CRITERIO 3
-- %%%%%%%%%%%%%%%%%%%%
-- Dosaje de hemoglobina CPMS: 85018 o 85018.01 + Tipo Dx: D hasta 7 días (previos al tratamiento y diagnostico de anemia)

IF Object_id(N'tempdb..#Tmp_Numerador_Dosaje_Hb_Previo',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Dosaje_Hb_Previo;
SELECT
a.codigo_seguimiento,
a.numero_documento,
ROW_NUMBER() OVER(PARTITION BY a.codigo_seguimiento ORDER BY h.fecha_atencion ASC) [orden_atencion],
h.id_cita,
h.fecha_atencion
INTO #Tmp_Numerador_Dosaje_Hb_Previo
FROM #Tmp_Numerador_Tratamiento_Anemia a
INNER JOIN #Data_HisMinsa h ON a.numero_documento = h.num_doc
WHERE
h.fecha_atencion BETWEEN DATEADD(DAY,-7,a.fecha_Tto_Anemia) AND a.fecha_Tto_Anemia AND -- 7 dias previos
h.i_rownum_lab = 1 AND -- EL PRIMER LAB DEL DX BUSCADO
h.cod_item IN ('85018','85018.01')


IF Object_id(N'tempdb..#Tmp_Numerador_Dosaje_Hb',N'U') IS NOT NULL DROP TABLE #Tmp_Numerador_Dosaje_Hb;
SELECT
ph.codigo_seguimiento,
ph.numero_documento,
ph.id_cita,
ph.fecha_atencion [fecha_dosaje_hb],
1 [Valor_Numerador_Dosaje_Hb]
INTO #Tmp_Numerador_Dosaje_Hb
FROM #Tmp_Numerador_Dosaje_Hb_Previo ph
WHERE
orden_atencion = 1 -- PRIMER DOSAJE HEMOGLOBINA DENTRO DEL RANGO

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
--IF OBJECT_ID (N'dbo.DL1153_2025_CG00_DxAnemia_A_Medica', N'U') IS NOT NULL DROP TABLE dbo.DL1153_2025_CG00_DxAnemia_A_Medica;

IF Object_id(N'tempdb..#DL1153_2025_CG00_DxAnemia_Tto_Dosaje',N'U') IS NOT NULL DROP TABLE #DL1153_2025_CG00_DxAnemia_Tto_Dosaje;
SELECT
YEAR(a.fecha_max_evaluacion) [Año],
MONTH(a.fecha_max_evaluacion) [Mes],
a.codigo_seguimiento,
a.numero_documento,
a.categoria_nino,
a.fecha_nacimiento,
a.ubigeo,
a.fecha_min_evaluacion,
a.fecha_max_evaluacion,
a.denominador,
ISNULL(ane.Valor_Numerador_Dx_Anemia,0) [Valor_Numerador_Dx_Anemia],
ane.fecha_Dx_Anemia,
ISNULL(ta.Valor_Numerador_Tratamiento,0) [Valor_Numerador_Tratamiento],
ta.fecha_Tto_Anemia,
ISNULL(am.Valor_Numerador_Dosaje_Hb,0) [Valor_Numerador_Dosaje_Hb],
am.fecha_dosaje_hb,
IIF(ane.Valor_Numerador_Dx_Anemia = 1 AND ta.Valor_Numerador_Tratamiento = 1 AND am.Valor_Numerador_Dosaje_Hb = 1 ,1,0) [Numerador]
INTO #DL1153_2025_CG00_DxAnemia_Tto_Dosaje
FROM #Nominal_Denominador_Padron a
LEFT JOIN #Tmp_Numerador_Anemia ane ON a.codigo_seguimiento = ane.codigo_seguimiento
LEFT JOIN #Tmp_Numerador_Tratamiento_Anemia ta ON a.codigo_seguimiento = ta.codigo_seguimiento
LEFT JOIN #Tmp_Numerador_Dosaje_Hb am ON a.codigo_seguimiento = am.codigo_seguimiento

-- LIMPIA DATA UBIGEO
IF Object_id(N'tempdb..#Data_Ubigueo',N'U') IS NOT NULL DROP TABLE #Data_Ubigueo;
SELECT
CONVERT(int,u.ubigeo) [ubigeo],
u.COD_DEP,
CASE 
WHEN u.DESC_DPTO = 'LIMA' AND u.DESC_PROV =  'LIMA' THEN 'LIMA METROPOLITANA'
WHEN u.DESC_DPTO = 'LIMA' AND u.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
ELSE u.DESC_DPTO
END	[DESC_DPTO],
u.DESC_PROV,
u.DESC_DIST,
CASE
WHEN u.DIRIS LIKE '%diris%' THEN u.DIRIS
ELSE u.RED
END	[RED]
INTO #Data_Ubigueo
FROM
BD_HISINDICADORES.dbo.Maestro_UBIGEO_2024 u

UPDATE #Data_Ubigueo SET DESC_DPTO = 'LA LIBERTAD', DESC_PROV = 'TRUJILLO', DESC_DIST = 'ALTO TRUJILLO', RED = 'RED TRU' WHERE ubigeo = '130112'

-- AGREGAMOS UBIGEO: 160109
INSERT INTO #Data_Ubigueo values ('160109','16','LORETO','MAYNAS','PUTUMAYO','MAYNAS PERIFERIE')

-- LIMPIA DATA ENDES
IF Object_id(N'tempdb..#Data_Endes',N'U') IS NOT NULL DROP TABLE #Data_Endes;
SELECT
CONVERT(int,endes.ubigeo) [COD_DEP],
UPPER(LTRIM(RTRIM(endes.region))) [DESC_DPTO],
CONVERT(varchar(6),endes.EQUIVALENCIA) [categoria_nino],
endes.PORCENTAJE [Porcentaje]
INTO #Data_Endes
FROM BD_HISINDICADORES.dbo.ENDES_UBIGEO endes


-- Exportamos el reporte final agrupado
SELECT
r.Año,
r.Mes,
u.DESC_DPTO,
u.DESC_PROV,
u.DESC_DIST,
u.RED,
u.ubigeo,
r.categoria_nino,
CONVERT(decimal(4,1),REPLACE(endes.Porcentaje,',','.')) [Porcentaje],
SUM(r.Denominador) [Denominador PadronNominal],
ROUND((SUM(r.Denominador) * CONVERT(decimal(4,1),REPLACE(endes.Porcentaje,',','.'))/100),0)[Denominador Ajustado],
SUM(r.Valor_Numerador_Dx_Anemia) [Valor_Numerador_Dx_Anemia],
SUM(r.Valor_Numerador_Tratamiento) [Valor_Numerador_Tratamiento],
SUM(r.Valor_Numerador_Dosaje_Hb) [Valor_Numerador_Dosaje_Hb],
SUM(r.Numerador) [Numerador]
FROM #DL1153_2025_CG00_DxAnemia_Tto_Dosaje r
LEFT JOIN #Data_Ubigueo u ON r.ubigeo = u.ubigeo
LEFT JOIN #Data_Endes endes ON u.DESC_DPTO = endes.DESC_DPTO and r.categoria_nino = endes.categoria_nino
GROUP BY
r.Año,
r.Mes,
u.DESC_DPTO,
u.DESC_PROV,
u.DESC_DIST,
u.RED,
u.ubigeo,
r.categoria_nino,
endes.Porcentaje


-- *****************************************************
-- FIN :D
-- *****************************************************
