--===========================================================================================
-- **************** OFICINA DE GESTIóN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N° 02 - DL 1153 - 2025
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Unidad Funcional de Alimentación y Nutrición
--								   Saludable (UFANS) - Dirección ejecutiva de intervenciones
--								   por curso de vida y cuidado integral (DIVICI) - Dirección
--								   Ejecutiva de Inmunizaciones (DMUNI)
-- NOMBRE: Porcentaje de niñas/niños menores de 12 meses, que reciben un paquete integrado de
--		   servicios: CRED, vacunas, dosaje de hemoglobina para descarte de anemia y
--		   suplementación con hierro
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 10/11/2023
-- Motivo              : Revisar el código para el 2024
-- Modificado por	   : Wilson Urviola Zapata (OGEI)
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 10/12/2024
-- Motivo              : Revisar el código para el CG 2025
-- Modificado por	   : Edson Donayre Uchuya (OGEI)
--*******************************************************************************************

USE BD_HISINDICADORES
GO

--Padron Nominal
SELECT pn.*
INTO #PadronNominal_Prev
FROM BD_BACKUP_OGEI.dbo.TramaPadronNominal pn WITH(NOLOCK)

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

-- Base Padron Nominal
IF OBJECT_ID('Tempdb..#padron_nominal') IS NOT NULL DROP TABLE #padron_nominal
SELECT DISTINCT
CONVERT(int,ndoc) ndoc, 
CONVERT(int,tdoc) tdoc,
CONVERT(date,fe_nac_menor) fecha_nac,
CONVERT(int,co_ubigeo_inei) ubigeo,
CASE
	WHEN CONVERT(int,TI_SEGURO_MENOR)='1' THEN 'MINSA'	
	WHEN CONVERT(int,TI_SEGURO_MENOR)='2' THEN 'ESSALUD'
	WHEN CONVERT(int,TI_SEGURO_MENOR)='3' THEN 'SANIDAD FFAA/PNP' 
	WHEN CONVERT(int,TI_SEGURO_MENOR)='4' THEN 'PRIVADO'
ELSE 'SIN REGISTRO' END [seguro]
INTO #padron_nominal
FROM(
		SELECT DISTINCT
		CASE
			-- WHEN nu_dni_menor IS NULL OR nu_dni_menor IN('','NULL') THEN nu_cnv
			WHEN IIF(nu_dni_menor='',NULL,nu_dni_menor) IS NULL THEN nu_cnv
            ELSE nu_dni_menor
        END ndoc,
        CASE
            -- WHEN nu_dni_menor IS NULL OR nu_dni_menor IN('','NULL') THEN 6
            WHEN IIF(nu_dni_menor='',NULL,nu_dni_menor) IS NULL THEN 6
		    ELSE 1
        END tdoc,
        FE_NAC_MENOR,
        CO_UBIGEO_INEI,
        TI_SEGURO_MENOR
		FROM #PadronNominal_Prev WITH(NOLOCK) --Antes era BD_BACKUP_OGEI.dbo.TramaPadronNominal
		WHERE
        convert(int,ti_doc_identidad) IN(1,3) 
		AND (sw_pn=1 OR sw_cnv=1) -- Validacion Dni o CNV	
    ) AS t

DELETE FROM #padron_nominal where ndoc is null

-- ELIMINA DUPLICADOS DEL PADRON NOMINAL
DELETE FROM #padron_nominal
WHERE
ndoc IN(
			SELECT ndoc
			FROM #padron_nominal
			GROUP BY ndoc 
			HAVING COUNT(*)>1
		) AND
tdoc NOT IN (1)


-- Base CNV
IF OBJECT_ID('Tempdb..#cnv_peso') IS NOT NULL DROP TABLE #cnv_peso
SELECT DISTINCT
CONVERT(int,nu_cnv) [ndoc],
PESO_NACIDO [peso],
dur_emb_parto [SemGestacion]
INTO #cnv_peso
FROM BD_BACKUP_OGEI.dbo.TramaCNV WITH (NOLOCK)
WHERE
TRY_CONVERT(int,NU_CNV) IS NOT NULL AND
TRY_CONVERT(int,NU_CNV) <> 0 AND
(
    TRY_CONVERT(int,PESO_NACIDO) <2500 OR
	TRY_CONVERT(int,dur_emb_parto) < 37
)

-- Base HIS 
IF OBJECT_ID('Tempdb..#his') IS NOT NULL DROP TABLE #his
SELECT
id_cita,
CONVERT(date, periodo) [fecha_atendido],
CONVERT(int,num_doc) [ndoc], 
CONVERT(int,id_tipo_doc) [tdoc],
LTRIM(RTRIM(id_tipitem)) [id_tip],
LTRIM(RTRIM(cod_item)) [cod_item], 
LTRIM(RTRIM(valor_lab)) [valor_lab]
INTO #his
FROM BD_BACKUP_OGEI.dbo.TramaHisMinsa WITH(NOLOCK)
WHERE
LTRIM(RTRIM(cod_item)) IN(
    'Z001','99381','99381.01',          -- CRED
	'90670',                            -- ANTINEUMOCOCICA
	'90681',                            -- ROTAVIRUS
	'90712','90713',                    -- ANTIPOLIO
	'90723','90722',                    -- PENTAVALENTE
	'99199.17','99199.19',              -- SUPLEMENTACION/TRATAMIENTO
	'85018','85018.01',                 -- DOSAJE
	'P073','P071','P0711','P0712',      -- BPN	
	'D500','D508','D509','D649','D539'  -- ANEMIA
) AND
CONVERT(int,id_tipo_doc) IN(1,6) AND    -- SE CONSIDERAN SOLO TIPO DE DOCUMENTO: DNI y CNV.
TRY_CONVERT(int,num_doc) IS NOT NULL

-- Base HISMINSA 2023
INSERT INTO #his
SELECT
id_cita,
CONVERT(date, periodo) [fecha_atendido],
CONVERT(int,num_doc) [ndoc], 
CONVERT(int,id_tipo_doc) [tdoc],
LTRIM(RTRIM(id_tipitem)) [id_tip],
LTRIM(RTRIM(cod_item)) [cod_item], 
LTRIM(RTRIM(valor_lab)) [valor_lab]
FROM BD_BACKUP_OGEI_2023.dbo.TramaHisMinsa WITH(NOLOCK)
WHERE
LTRIM(RTRIM(cod_item)) IN(
    'Z001','99381','99381.01',          -- CRED
	'90670',                            -- ANTINEUMOCOCICA
	'90681',                            -- ROTAVIRUS
	'90712','90713',                    -- ANTIPOLIO
	'90723','90722',                    -- PENTAVALENTE
	'99199.17','99199.19',              -- SUPLEMENTACION/TRATAMIENTO
	'85018','85018.01',                 -- DOSAJE
	'P073','P071','P0711','P0712',      -- BPN	
	'D500','D508','D509','D649','D539'  -- ANEMIA
) AND
CONVERT(int,id_tipo_doc) IN(1,6) AND    -- SE CONSIDERAN SOLO TIPO DE DOCUMENTO: DNI y CNV.
TRY_CONVERT(int,num_doc) IS NOT NULL


--***************************************************
--					SINTAXIS
--***************************************************
IF OBJECT_ID('Tempdb..#reporte_final') IS NOT NULL DROP TABLE #reporte_final 
CREATE TABLE #reporte_final(
    ndoc int, tdoc int, fecha_nac date, ubigeo int, seguro nvarchar(30),
    fecha_final date, fecha_inicio date, edad_dias int, anio int, mes int,
    edad_mes int, den int, num_dni30d int, num_cred_mensual int, num_cred int,
    num_neumo int, num_rota int, num_polio int, num_penta int, num_vac int,
    num_sup_4m int, num_sup_6m int, num_sup int, num_dosaje int
)


-- PARAMETROS PARA EL PERIODO DE EVALUACION
DECLARE @mes_eval int, @mes_final int, @year int 

SET @mes_eval=1
SET @mes_final=10
SET @year=2024

WHILE @mes_eval <= @mes_final
BEGIN

PRINT(@mes_eval)
PRINT(@year)

-- %%%%%%%%%%%%%%%%
-- DENOMINADOR
-- %%%%%%%%%%%%%%%%

-- La determinación del corte de edad para cada periodo de medición , será el último día de cada mes.
IF OBJECT_ID('Tempdb..#padron_final_bpn') IS NOT NULL DROP TABLE #padron_final_bpn 
SELECT
*,
@year [anio],
@mes_eval [mes]
INTO #padron_final_bpn 
FROM(
		SELECT
		*,
		DATEDIFF(dd,fecha_nac,fecha_final) [edad_dias]
		FROM(
				SELECT
				*,
				EOMONTH(TRY_CONVERT(date,TRY_CONVERT(varchar(4),@year)+'-'+RIGHT('00'+TRY_CONVERT(varchar(2),@mes_eval),2)+'-'+RIGHT('00'+TRY_CONVERT(varchar(2),1),2))) [fecha_final],
				TRY_CONVERT(date,TRY_CONVERT(varchar(4),@year)+'-'+RIGHT('00'+TRY_CONVERT(varchar(2),@mes_eval),2)+'-'+RIGHT('00'+TRY_CONVERT(varchar(2),1),2)) fecha_inicio
				FROM #padron_nominal
		) AS t0
) AS t1
WHERE
(fecha_final BETWEEN fecha_nac AND DATEADD(dd,364,fecha_nac)) AND
YEAR(fecha_nac) = @year

--- Excluir niños CNV
SELECT DISTINCT
a.tdoc,
a.ndoc,
a.fecha_nac,
1 [bpn]
INTO #bpn 
FROM #padron_final_bpn a
INNER JOIN #cnv_peso b ON a.ndoc=b.ndoc 

--- Padron Final 
SELECT
a.*
INTO #padron_final 
FROM #padron_final_bpn a
LEFT JOIN #bpn b ON a.tdoc=b.tdoc AND a.ndoc=b.ndoc AND a.fecha_nac=b.fecha_nac
WHERE
b.bpn IS NULL

DROP TABLE #bpn
DROP TABLE #padron_final_bpn


--*** GENERACION DE IDENTIFICADOR DE BAJO PESO AL NACER.*****
SELECT DISTINCT
a.tdoc,
a.ndoc,
1 [bd_premat] 
INTO #bd_premat
FROM #padron_final a
INNER JOIN #his b ON a.ndoc=b.ndoc
WHERE
b.cod_item IN ('P073','P071','P0711','P0712') AND
(b.fecha_atendido BETWEEN a.fecha_nac AND dateadd(dd,364,a.fecha_nac)) AND
b.fecha_atendido <= a.fecha_final

--*** HIS SUPLEMENTADOS.*****
SELECT DISTINCT
fecha_atendido,
tdoc,
ndoc,
cod_item,
valor_lab,
dx_anemia
INTO #his_sup
FROM(
		SELECT
		a.fecha_atendido,
		b.tdoc,
		b.ndoc,
		a.cod_item,
		a.valor_lab,
		MAX(IIF(a.cod_item IN('D509','D649'),1,0)) OVER (PARTITION BY id_cita) dx_anemia -- 'D500','D508','D539'
		FROM #his a
		INNER JOIN #padron_final b ON a.ndoc=b.ndoc
		WHERE
		a.fecha_atendido <= b.fecha_final AND
		cod_item IN('99199.17','99199.19','D509','D649') -- 'D500','D508','D539'
) AS t 
WHERE
cod_item IN('99199.17','99199.19')

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

--%%%%%%%%%%%%%%%%
-- I. CRED
--%%%%%%%%%%%%%%%%

	-- �����������������������������
	-- CRED MENSUAL
	-- �����������������������������


	-- Control 1
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes1 INTO #cred_mes1 
	FROM #padron_final a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,fecha_nac,fecha_atendido) BETWEEN 29 AND 59)
	AND b.fecha_atendido<=a.fecha_final

	-- Control 2
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes2 INTO #cred_mes2
	FROM #cred_mes1 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes1,b.fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 3
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes3 INTO #cred_mes3
	FROM #cred_mes2 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes2,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 4
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes4 INTO #cred_mes4
	FROM #cred_mes3 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes3,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 5
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes5 INTO #cred_mes5
	FROM #cred_mes4 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes4,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 6
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes6 INTO #cred_mes6
	FROM #cred_mes5 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes5,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 7
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes7 INTO #cred_mes7
	FROM #cred_mes6 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes6,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 8
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes8 INTO #cred_mes8
	FROM #cred_mes7 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes7,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 9
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes9 INTO #cred_mes9
	FROM #cred_mes8 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes8,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 10
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes10 INTO #cred_mes10
	FROM #cred_mes9 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes9,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

	-- Control 11
	SELECT DISTINCT a.tdoc, a.ndoc, a.fecha_nac, a.fecha_final, b.fecha_atendido cred_mes11 INTO #cred_mes11
	FROM #cred_mes10 a INNER JOIN #his b ON a.ndoc=b.ndoc
	WHERE b.cod_item IN ('Z001','99381') AND (DATEDIFF(dd,a.cred_mes10,fecha_atendido) BETWEEN 28 AND 31) -- 30 AND 31
	AND b.fecha_atendido<=a.fecha_final

----generar tabla Cred

	----------FECHAS CRED. 
	select tdoc, ndoc, fecha_nac, edad_dias
	, max(fecha_cred_mes1) fecha_cred_mes1, max(cred_mes1) cred_mes1
	, max(fecha_cred_mes2) fecha_cred_mes2, max(cred_mes2) cred_mes2
	, max(fecha_cred_mes3) fecha_cred_mes3, max(cred_mes3) cred_mes3
	, max(fecha_cred_mes4) fecha_cred_mes4, max(cred_mes4) cred_mes4
	, max(fecha_cred_mes5) fecha_cred_mes5, max(cred_mes5) cred_mes5
	, max(fecha_cred_mes6) fecha_cred_mes6, max(cred_mes6) cred_mes6
	, max(fecha_cred_mes7) fecha_cred_mes7, max(cred_mes7) cred_mes7
	, max(fecha_cred_mes8) fecha_cred_mes8, max(cred_mes8) cred_mes8
	, max(fecha_cred_mes9) fecha_cred_mes9, max(cred_mes9) cred_mes9
	, max(fecha_cred_mes10) fecha_cred_mes10, max(cred_mes10) cred_mes10
	, max(fecha_cred_mes11) fecha_cred_mes11, max(cred_mes11) cred_mes11	 
	into #CRED_MENSUAL
	from (
			SELECT a.*
			, b.cred_mes1 fecha_cred_mes1	,iif(b.cred_mes1 is not null,1,0) cred_mes1
			, c.cred_mes2 fecha_cred_mes2	,iif(c.cred_mes2 is not null,1,0) cred_mes2
			, d.cred_mes3 fecha_cred_mes3	,iif(d.cred_mes3 is not null,1,0) cred_mes3
			, e.cred_mes4 fecha_cred_mes4	,iif(e.cred_mes4 is not null,1,0) cred_mes4
			, f.cred_mes5 fecha_cred_mes5	,iif(f.cred_mes5 is not null,1,0) cred_mes5
			, g.cred_mes6 fecha_cred_mes6	,iif(g.cred_mes6 is not null,1,0) cred_mes6
			, h.cred_mes7 fecha_cred_mes7	,iif(h.cred_mes7 is not null,1,0) cred_mes7
			, i.cred_mes8 fecha_cred_mes8	,iif(i.cred_mes8 is not null,1,0) cred_mes8
			, j.cred_mes9 fecha_cred_mes9	,iif(j.cred_mes9 is not null,1,0) cred_mes9
			, l.cred_mes10 fecha_cred_mes10	,iif(l.cred_mes10 is not null,1,0) cred_mes10
			, m.cred_mes11 fecha_cred_mes11	,iif(m.cred_mes11 is not null,1,0) cred_mes11	
			FROM #padron_final a
			LEFT JOIN #cred_mes1 b on a.tdoc=b.tdoc and a.ndoc=b.ndoc and a.fecha_nac=b.fecha_nac
			LEFT JOIN #cred_mes2 c on a.tdoc=c.tdoc and a.ndoc=c.ndoc and a.fecha_nac=c.fecha_nac
			LEFT JOIN #cred_mes3 d on a.tdoc=d.tdoc and a.ndoc=d.ndoc and a.fecha_nac=d.fecha_nac
			LEFT JOIN #cred_mes4 e on a.tdoc=e.tdoc and a.ndoc=e.ndoc and a.fecha_nac=e.fecha_nac
			LEFT JOIN #cred_mes5 f on a.tdoc=f.tdoc and a.ndoc=f.ndoc and a.fecha_nac=f.fecha_nac
			LEFT JOIN #cred_mes6 g on a.tdoc=g.tdoc and a.ndoc=g.ndoc and a.fecha_nac=g.fecha_nac
			LEFT JOIN #cred_mes7 h on a.tdoc=h.tdoc and a.ndoc=h.ndoc and a.fecha_nac=h.fecha_nac
			LEFT JOIN #cred_mes8 i on a.tdoc=i.tdoc and a.ndoc=i.ndoc and a.fecha_nac=i.fecha_nac	
			LEFT JOIN #cred_mes9 j on a.tdoc=j.tdoc and a.ndoc=j.ndoc and a.fecha_nac=j.fecha_nac
			LEFT JOIN #cred_mes10 l on a.tdoc=l.tdoc and a.ndoc=l.ndoc and a.fecha_nac=l.fecha_nac
			LEFT JOIN #cred_mes11 m on a.tdoc=m.tdoc and a.ndoc=m.ndoc and a.fecha_nac=m.fecha_nac		
	) as t group by tdoc, ndoc, fecha_nac, edad_dias

		--- CRED MENSUAL FINAL.
	SELECT *
	, CASE WHEN edad_dias<=59 THEN 1
		   WHEN (edad_dias BETWEEN 60 and 89) and cred_mes1>=1 then 1
		   WHEN (edad_dias BETWEEN 90 and 119) and cred_mes1+cred_mes2>=2 then 1
		   WHEN (edad_dias BETWEEN 120 and 149) and cred_mes1+cred_mes2+cred_mes3>=3 then 1
		   WHEN (edad_dias BETWEEN 150 and 179) and cred_mes1+cred_mes2+cred_mes3+cred_mes4>=4 then 1
		   WHEN (edad_dias BETWEEN 180 and 209) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5>=5 then 1
		   WHEN (edad_dias BETWEEN 210 and 239) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6>=6 then 1
		   WHEN (edad_dias BETWEEN 240 and 269) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6+cred_mes7>=7 then 1
		   WHEN (edad_dias BETWEEN 270 and 299) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6+cred_mes7+cred_mes8>=8 then 1
		   WHEN (edad_dias BETWEEN 300 and 329) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6+cred_mes7+cred_mes8+cred_mes9>=9 then 1
		   WHEN (edad_dias BETWEEN 330 and 363) and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6+cred_mes7+cred_mes8+cred_mes9+cred_mes10>=10 then 1
		   WHEN edad_dias>=364 and cred_mes1+cred_mes2+cred_mes3+cred_mes4+cred_mes5+cred_mes6+cred_mes7+cred_mes8+cred_mes9+cred_mes10+cred_mes11>=11 then 1 else 0 end num_cred_mensual
	INTO #num_cred_mensual	
	FROM #CRED_MENSUAL


DROP TABLE #cred_mes1
DROP TABLE #cred_mes2
DROP TABLE #cred_mes3
DROP TABLE #cred_mes4
DROP TABLE #cred_mes5
DROP TABLE #cred_mes6
DROP TABLE #cred_mes7
DROP TABLE #cred_mes8
DROP TABLE #cred_mes9
DROP TABLE #cred_mes10
DROP TABLE #cred_mes11
DROP TABLE #CRED_MENSUAL

--%%%%%%%%%%%%%%%%
-- II. VACUNA
--%%%%%%%%%%%%%%%%

	-- =====================================
	-- 1. vacuna antineumocócica
	-- =====================================

	--1° Dosis.
	select a.*, b.fecha_atendido fecha_neumo1
	into #padron_neumo1 from #padron_final a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item='90670' and ( b.fecha_atendido between dateadd(dd,55,a.fecha_nac) and dateadd(dd,119,a.fecha_nac) )
	and b.fecha_atendido<=a.fecha_final

	--2° Dosis.
	select a.*, b.fecha_atendido fecha_neumo2
	into #padron_neumo2 from #padron_neumo1 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item='90670' and ( b.fecha_atendido between dateadd(dd,28,a.fecha_neumo1) and dateadd(dd,70,a.fecha_neumo1) )
	and b.fecha_atendido<=a.fecha_final

	-- Vacuna completa.
	select distinct ndoc, tdoc, fecha_nac
	, case when edad_dias<=119 then 1
		   when (edad_dias between 120 and 147) and neumo1=1 then 1
		   when edad_dias>=148 and neumo1=1 and neumo2=1 then 1 else 0 end num_neumo
	into #num_neumo
	from (
			select a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
			,max(iif(b.fecha_neumo1 is null,0,1)) neumo1
			,max(iif(c.fecha_neumo2 is null,0,1)) neumo2
			from #padron_final a
			left join #padron_neumo1 b on a.ndoc=b.ndoc and a.tdoc=b.tdoc and a.fecha_nac=b.fecha_nac
			left join #padron_neumo2 c on a.ndoc=c.ndoc and a.tdoc=c.tdoc and a.fecha_nac=c.fecha_nac
			group by a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
	) as t

	drop table #padron_neumo1
	drop table #padron_neumo2


	-- =====================================
	-- 2. vacuna Rotavirus
	-- =====================================

	--1° Dosis.
	select a.*, b.fecha_atendido fecha_rota1
	into #padron_rota1 from #padron_final a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item='90681' and ( b.fecha_atendido between dateadd(dd,55,a.fecha_nac) and dateadd(dd,180,a.fecha_nac) )
	and b.fecha_atendido<=a.fecha_final

	--2° Dosis.
	select a.*, b.fecha_atendido fecha_rota2
	into #padron_rota2 from #padron_rota1 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item='90681' and ( b.fecha_atendido between dateadd(dd,28,a.fecha_rota1) and dateadd(dd,240,a.fecha_nac) )
	and b.fecha_atendido<=a.fecha_final

	-- Vacuna completa.
	select distinct ndoc, tdoc, fecha_nac
	, case when edad_dias<=180 then 1
		   when (edad_dias between 181 and 208) and rota1=1 then 1
		   when edad_dias>=209 and rota1=1 and rota2=1 then 1 else 0 end num_rota
	into #num_rota
	from (
			select a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
			,max(iif(b.fecha_rota1 is null,0,1)) rota1
			,max(iif(c.fecha_rota2 is null,0,1)) rota2
			from #padron_final a
			left join #padron_rota1 b on a.ndoc=b.ndoc and a.tdoc=b.tdoc and a.fecha_nac=b.fecha_nac
			left join #padron_rota2 c on a.ndoc=c.ndoc and a.tdoc=c.tdoc and a.fecha_nac=c.fecha_nac
			group by a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
	) as t

	drop table #padron_rota1
	drop table #padron_rota2

	-- =====================================
	-- 3. vacuna Antipolio
	-- =====================================

	--1° Dosis.
	select a.*, b.fecha_atendido fecha_polio1
	into #padron_polio1 from #padron_final a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90712','90713') and ( b.fecha_atendido between dateadd(dd,55,a.fecha_nac) and dateadd(dd,119,a.fecha_nac) )
	and b.fecha_atendido<=a.fecha_final

	--2° Dosis.
	select a.*, b.fecha_atendido fecha_polio2
	into #padron_polio2 from #padron_polio1 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90712','90713') and ( b.fecha_atendido between dateadd(dd,28,a.fecha_polio1) and dateadd(dd,70,a.fecha_polio1) )
	and b.fecha_atendido<=a.fecha_final

	--3° Dosis.
	select a.*, b.fecha_atendido fecha_polio3
	into #padron_polio3 from #padron_polio2 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90712','90713') and ( b.fecha_atendido between dateadd(dd,28,a.fecha_polio2) and dateadd(dd,70,a.fecha_polio2) )
	and b.fecha_atendido<=a.fecha_final

	-- Vacuna completa.
	select distinct ndoc, tdoc, fecha_nac
	, case when edad_dias<=119 then 1
		   when (edad_dias between 120 and 147) and polio1=1 then 1
		   when (edad_dias between 148 and 217) and polio1=1 and polio2=1 then 1 
		   when edad_dias>=218 and polio1=1 and polio2=1 and polio3=1 then 1 else 0 end num_polio
	into #num_polio
	from (
			select a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
			,max(iif(b.fecha_polio1 is null,0,1)) polio1
			,max(iif(c.fecha_polio2 is null,0,1)) polio2
			,max(iif(d.fecha_polio3 is null,0,1)) polio3
			from #padron_final a
			left join #padron_polio1 b on a.ndoc=b.ndoc and a.tdoc=b.tdoc and a.fecha_nac=b.fecha_nac
			left join #padron_polio2 c on a.ndoc=c.ndoc and a.tdoc=c.tdoc and a.fecha_nac=c.fecha_nac
			left join #padron_polio3 d on a.ndoc=d.ndoc and a.tdoc=d.tdoc and a.fecha_nac=d.fecha_nac
			group by a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
	) as t

	drop table #padron_polio1
	drop table #padron_polio2
	drop table #padron_polio3

	-- =====================================
	-- 4. vacuna Pentavalente
	-- =====================================

	--1° Dosis.
	select a.*, b.fecha_atendido fecha_penta1
	into #padron_penta1 from #padron_final a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90722','90723') and ( b.fecha_atendido between dateadd(dd,55,a.fecha_nac) and dateadd(dd,119,a.fecha_nac) )
	and b.fecha_atendido<=a.fecha_final

	--2° Dosis.
	select a.*, b.fecha_atendido fecha_penta2
	into #padron_penta2 from #padron_penta1 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90722','90723') and ( b.fecha_atendido between dateadd(dd,28,a.fecha_penta1) and dateadd(dd,70,a.fecha_penta1) )
	and b.fecha_atendido<=a.fecha_final

	--3° Dosis.
	select a.*, b.fecha_atendido fecha_penta3
	into #padron_penta3 from #padron_penta2 a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('90722','90723') and ( b.fecha_atendido between dateadd(dd,28,a.fecha_penta2) and dateadd(dd,70,a.fecha_penta2) )
	and b.fecha_atendido<=a.fecha_final

	-- Vacuna completa.
	select distinct ndoc, tdoc, fecha_nac
	, case when edad_dias<=119 then 1
		   when (edad_dias between 120 and 147) and penta1=1 then 1
		   when (edad_dias between 148 and 217) and penta1=1 and penta2=1 then 1 
		   when edad_dias>=218 and penta1=1 and penta2=1 and penta3=1 then 1 else 0 end num_penta
	into #num_penta
	from (
			select a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
			,max(iif(b.fecha_penta1 is null,0,1)) penta1
			,max(iif(c.fecha_penta2 is null,0,1)) penta2
			,max(iif(d.fecha_penta3 is null,0,1)) penta3
			from #padron_final a
			left join #padron_penta1 b on a.ndoc=b.ndoc and a.tdoc=b.tdoc and a.fecha_nac=b.fecha_nac
			left join #padron_penta2 c on a.ndoc=c.ndoc and a.tdoc=c.tdoc and a.fecha_nac=c.fecha_nac
			left join #padron_penta3 d on a.ndoc=d.ndoc and a.tdoc=d.tdoc and a.fecha_nac=d.fecha_nac
			group by a.ndoc, a.tdoc, a.fecha_nac, a.edad_dias
	) as t

	drop table #padron_penta1
	drop table #padron_penta2
	drop table #padron_penta3

--%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- II. TRATAMIENTO / SUPLEMENTACION
--%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-- Padron de Suplementacion!
SELECT
a.*,
isnull(b.bd_premat,0) [bpn]
INTO #padron_sup
FROM #padron_final a
LEFT JOIN #bd_premat b ON a.tdoc=b.tdoc AND a.ndoc=b.ndoc 

-- a] Esquema de niño de 4 meses.

	-- 1° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6') then 1
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 2 else 0 end sup_4m_1 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	into #padron_sup_4m_1 from #padron_sup a
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.cod_item='99199.17' and a.bpn=0
	and b.fecha_atendido between dateadd(dd,110,a.fecha_nac) and dateadd(dd,130,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 2° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6') then 1
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 2 else 0 end sup_4m_2 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	into #padron_sup_4m_2 from #padron_sup a
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.cod_item='99199.17' and a.bpn=0
	and b.fecha_atendido between dateadd(dd,131,a.fecha_nac) and dateadd(dd,179,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- Suplementacion 4 meses.
	select tdoc, ndoc, fecha_nac, max(sup_4m) num_sup_4m
	into #num_sup4m 
	from (
			select distinct a.tdoc, a.ndoc, a.fecha_nac
			, case when ( a.bpn=1 or a.edad_dias<=130 ) then 1
				   when (a.edad_dias between 131 and 179) and a.bpn=0 and b.sup_4m_1>=1 then 1
				   when a.edad_dias>=180 and a.bpn=0 and b.sup_4m_1>=1 and (isnull(c.sup_4m_2,0)+isnull(b.sup_4m_1,0)>=2) then 1 else 0 end sup_4m
			from #padron_sup a
			left join #padron_sup_4m_1 b on a.tdoc=b.tdoc and a.ndoc=b.ndoc
			left join #padron_sup_4m_2 c on a.tdoc=c.tdoc and a.ndoc=c.ndoc
	) as t group by tdoc, ndoc, fecha_nac
		
	drop table #padron_sup_4m_1
	drop table #padron_sup_4m_2

-- b] Esquema de niño de 6 meses.

	-- 1° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6') then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_1
	into #padron_sup_6m_1 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido between dateadd(dd,170,a.fecha_nac) and dateadd(dd,209,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 2° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6')  then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_2
	into #padron_sup_6m_2 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido between dateadd(dd,210,a.fecha_nac) and dateadd(dd,239,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 3° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6')  then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_3
	into #padron_sup_6m_3 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido between dateadd(dd,240,a.fecha_nac) and dateadd(dd,269,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 4° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6')  then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_4
	into #padron_sup_6m_4 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido between dateadd(dd,270,a.fecha_nac) and dateadd(dd,299,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 5° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6')  then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_5
	into #padron_sup_6m_5 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido between dateadd(dd,300,a.fecha_nac) and dateadd(dd,329,a.fecha_nac)
	and b.fecha_atendido<=a.fecha_final

	-- 6° Suplementacion.
	select distinct a.tdoc, a.ndoc,
	case
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('SF1','SF2','SF3','SF4','SF5','SF6')  then 2 
	when cod_item='99199.17' and dx_anemia=0 and valor_lab in ('1','2','3','4','5','6') then 3 -- 'P01','P02','P03','P04','P05','P06','PO1','PO2','PO3','PO4','PO5','PO6'
	when cod_item='99199.19' and try_convert(int,valor_lab) in (1,2,3,4,5,6) then 1 else 0 end sup_6m_6
	into #padron_sup_6m_6 from #padron_sup a 
	inner join #his_sup b on a.ndoc=b.ndoc 
	where b.fecha_atendido>=dateadd(dd,330,a.fecha_nac) 
	and b.fecha_atendido<=a.fecha_final
	
	-- Suplementacion 6 meses.
	select tdoc, ndoc, fecha_nac, max(sup_6m) num_sup_6m
	into #num_sup6m 
	from (
			select distinct a.tdoc, a.ndoc, a.fecha_nac
			, case when a.edad_dias<=209 then 1
				   when (a.edad_dias between 210 and 239) and b.sup_6m_1>=1 then 1
				   when (a.edad_dias between 240 and 269) and b.sup_6m_1>=1 and (isnull(b.sup_6m_1,0)+isnull(c.sup_6m_2,0)>=2) then 1
				   when (a.edad_dias between 270 and 299) and b.sup_6m_1>=1 and (isnull(b.sup_6m_1,0)+isnull(c.sup_6m_2,0)+isnull(d.sup_6m_3,0)>=3) then 1
				   when (a.edad_dias between 300 and 329) and b.sup_6m_1>=1 and (isnull(b.sup_6m_1,0)+isnull(c.sup_6m_2,0)+isnull(d.sup_6m_3,0)+isnull(e.sup_6m_4,0)>=4) then 1
				   when (a.edad_dias between 330 and 363) and b.sup_6m_1>=1 and (isnull(b.sup_6m_1,0)+isnull(c.sup_6m_2,0)+isnull(d.sup_6m_3,0)+isnull(e.sup_6m_4,0)+isnull(f.sup_6m_5,0)>=5) then 1
				   when a.edad_dias>=364 and b.sup_6m_1>=1 and (isnull(b.sup_6m_1,0)+isnull(c.sup_6m_2,0)+isnull(d.sup_6m_3,0)+isnull(e.sup_6m_4,0)+isnull(f.sup_6m_5,0)+isnull(g.sup_6m_6,0)>=6) then 1 else 0 end sup_6m	
			from #padron_sup a
			left join #padron_sup_6m_1 b on a.tdoc=b.tdoc and a.ndoc=b.ndoc
			left join #padron_sup_6m_2 c on a.tdoc=c.tdoc and a.ndoc=c.ndoc
			left join #padron_sup_6m_3 d on a.tdoc=d.tdoc and a.ndoc=d.ndoc
			left join #padron_sup_6m_4 e on a.tdoc=e.tdoc and a.ndoc=e.ndoc
			left join #padron_sup_6m_5 f on a.tdoc=f.tdoc and a.ndoc=f.ndoc
			left join #padron_sup_6m_6 g on a.tdoc=g.tdoc and a.ndoc=g.ndoc
	) as t group by tdoc, ndoc, fecha_nac

	drop table #padron_sup_6m_1
	drop table #padron_sup_6m_2
	drop table #padron_sup_6m_3
	drop table #padron_sup_6m_4
	drop table #padron_sup_6m_5
	drop table #padron_sup_6m_6

-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-- 4. DOSAJE
-- %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

	select a.*, b.fecha_atendido fecha_dosaje
	into #padron_dosaje from #padron_final a
	inner join #his b on a.ndoc=b.ndoc 
	where b.cod_item in ('85018','85018.01') and b.id_tip='D' and ( b.fecha_atendido between dateadd(dd,180,a.fecha_nac) and dateadd(dd,209,a.fecha_nac) ) -- CAMBIO CG2024: 170 - 209
	and b.fecha_atendido<=a.fecha_final
															
	select tdoc, ndoc, fecha_nac
	, case when edad_dias<=209 then 1
		  when edad_dias>=210 and num_dosaje=1 then 1 else 0 end num_dosaje
	into #num_dosaje
	from (
			select distinct a.tdoc, a.ndoc, a.fecha_nac, a.edad_dias
			, max(iif(b.fecha_dosaje is null,0,1)) num_dosaje
			from #padron_sup a
			left join #padron_dosaje b on a.tdoc=b.tdoc and a.ndoc=b.ndoc and a.fecha_nac=b.fecha_nac
			group by a.tdoc, a.ndoc, a.fecha_nac, a.edad_dias
	) as t 

	drop table #padron_dosaje


--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #reporte_final
select a.*
	, BD_HISINDICADORES.dbo.fn_calcula_edadmeses(a.fecha_nac,a.fecha_final) as edad_mes
	, den=1
	, iif(	b.num_cred_mensual=1 and c.num_neumo=1 and d.num_rota=1 and e.num_polio=1 and f.num_penta=1 and g.num_sup_4m=1 and h.num_sup_6m=1 and i.num_dosaje=1,1,0) num_dni30
	, b.num_cred_mensual, iif(/*j.num_cred_rn=1 and*/ b.num_cred_mensual=1,1,0) num_cred
	, c.num_neumo, d.num_rota, e.num_polio, f.num_penta,  iif(c.num_neumo=1 and d.num_rota=1 and e.num_polio=1 and f.num_penta=1,1,0) num_vac
	, g.num_sup_4m, h.num_sup_6m,  iif(g.num_sup_4m=1 and h.num_sup_6m=1,1,0) num_sup
	, i.num_dosaje

from #padron_final a
left join #num_cred_mensual		b on a.tdoc=b.tdoc and a.ndoc=b.ndoc and a.fecha_nac=b.fecha_nac
left join #num_neumo			c on a.tdoc=c.tdoc and a.ndoc=c.ndoc and a.fecha_nac=c.fecha_nac
left join #num_rota				d on a.tdoc=d.tdoc and a.ndoc=d.ndoc and a.fecha_nac=d.fecha_nac
left join #num_polio			e on a.tdoc=e.tdoc and a.ndoc=e.ndoc and a.fecha_nac=e.fecha_nac
left join #num_penta			f on a.tdoc=f.tdoc and a.ndoc=f.ndoc and a.fecha_nac=f.fecha_nac
left join #num_sup4m			g on a.tdoc=g.tdoc and a.ndoc=g.ndoc and a.fecha_nac=g.fecha_nac
left join #num_sup6m			h on a.tdoc=h.tdoc and a.ndoc=h.ndoc and a.fecha_nac=h.fecha_nac
left join #num_dosaje			i on a.tdoc=i.tdoc and a.ndoc=i.ndoc and a.fecha_nac=i.fecha_nac


	drop table #bd_premat
	drop table #his_sup
	drop table #padron_sup
	drop table #padron_final
	drop table #num_cred_mensual
	drop table #num_neumo
	drop table #num_rota
	drop table #num_polio
	drop table #num_penta
	drop table #num_sup4m
	drop table #num_sup6m
	drop table #num_dosaje

SET @mes_eval = @mes_eval + 1
END

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
--- NOMINAL
-- IF OBJECT_ID(N'dbo.DL1153_2025_CG02_pqtniño',N'U') IS NOT NULL DROP TABLE DL1153_2025_CG02_pqtniño
IF OBJECT_ID('Tempdb..#DL1153_2025_CG02_pqtniño') IS NOT NULL DROP TABLE #DL1153_2025_CG02_pqtniño
select
	b.DIRIS Disa,
	b.DESC_DPTO Dpto,
	b.DESC_PROV Prov,
	b.DESC_DIST Dist,
	c.fed Distritos_FED,
	a.*
INTO #DL1153_2025_CG02_pqtniño
-- INTO BD_HISINDICADORES.DBO.DL1153_2025_CG02_pqtniño
from  #reporte_final a
inner join BD_HISINDICADORES.dbo.Maestro_UBIGEO_20200407_2023 b on convert(int,a.ubigeo)=convert(int,b.UBIGEO)
left join BD_HISINDICADORES.dbo.maeubigeo c on convert(int,b.ubigeo)=convert(int,c.UBIGEO)


SELECT * FROM #DL1153_2025_CG02_pqtniño WHERE ndoc is NULL

-- Exportamos el reporte final agrupado
-- CONSOLIDADO 
select Disa, b.RED, Dpto, Prov, Dist, Distritos_FED, anio, mes, tdoc, a.ubigeo, seguro, edad_mes 
, sum(den) den, sum(num_dni30d) num
, sum(num_cred_mensual) num_cred_mensual, sum(num_cred) num_cred
, sum(num_neumo) num_neumo, sum(num_rota) num_rota, sum(num_polio) num_polio, sum(num_penta) num_penta, sum(num_vac) num_vac
, sum(num_sup_4m) num_sup_4m, sum(num_sup_6m) num_sup_6m, sum(num_sup) num_sup
, sum(num_dosaje) num_dosaje
FROM #DL1153_2025_CG02_pqtniño a
-- from BD_HISINDICADORES.DBO.DL1153_2025_CG02_pqtniño a
INNER JOIN BD_HISINDICADORES.dbo.Maestro_UBIGEO_20200407_2023 b on convert(int,a.ubigeo) = convert(int,b.UBIGEO)
group by Disa, b.RED, Dpto, Prov, Dist, Distritos_FED, anio, mes, tdoc, a.ubigeo, seguro, edad_mes 