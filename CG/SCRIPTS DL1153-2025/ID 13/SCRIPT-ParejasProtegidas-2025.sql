




-- 1] BASE DE DATOS.
IF OBJECT_ID('Tempdb..#hisminsa') IS NOT NULL DROP TABLE #hisminsa 
select 
	   id_cita				, 
	   renaes				,
	   fecha_aten = convert(date,periodo),
	   id_genero			,
	   id_tipcond_estab		,
	   num_doc				,
	   id_tipitem			,
	   cod_item				,
	   valor_lab			
into #hisminsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa h with (nolock)
where (cod_item in ('99208',
				   '58300',					--DIU
				   '58300.01',				--SIU
				   '99208.13',				--ORAL COMBINADO
				   '99208.05',				--INYECTABLE TRIMESTRAL 
				   '99208.04',				--INYECTABLE MENSUAL
				   '11975'	 ,				--IMPLANTE
				   '99208.02',				--CONDON MASCULINO	
				   '99208.06',				--CONDON FEMENINO
				   '58600','58605','58611',	--AQV FEMENINO
				   '55250',					--AQV MASCULINO
				   '99402.04','99402.07','Z3002',  --Consejería en salud sexual
				   'Z349','Z3491','Z3492','Z3493', --Embarazadas
				   'Z3591','Z3592','Z3593')
	OR valor_lab = 'G')
	and (id_tipedad_reg = 'A' and edad_reg BETWEEN 15 AND 49)
	AND id_tipo_doc = 1 -- DNI

-- Se excluyen gestantes
	delete from #hisminsa
	where num_doc in (	select
							distinct
							num_doc
						from #hisminsa
						where cod_item in ('Z349','Z3491','Z3492','Z3493',
										   'Z3591','Z3592','Z3593')
							OR valor_lab = 'G'
					 )


SELECT * FROM #hisminsa
WHERE id_cita IN (SELECT id_cita FROM #hisminsa WHERE valor_lab = 'TA')




--%%%%%%%%%%%%%%%%
-- DENOMINADOR
--%%%%%%%%%%%%%%%%
-- Personas de 15 a 49 años que reciben consejería en salud sexual y reproductiva
IF OBJECT_ID('Tempdb..#Den') IS NOT NULL DROP TABLE #Den
SELECT
	h.*
INTO #Den
FROM
	(SELECT
		id = ROW_NUMBER() OVER(PARTITION BY h.num_doc		
								   ORDER BY h.fecha_aten ASC),
		h.*
	FROM
		(select
			distinct
			a.renaes		,
			a.id_genero		,
			a.fecha_aten	,
			Mes				= 10,
			a.num_doc		,
			Den				= 1
		from #hisminsa a 
		WHERE a.cod_item IN ('99402.04','99402.07','Z3002')
			AND MONTH(a.fecha_aten) <= 10  ) h
	) h
WHERE h.id = 1

--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%
IF OBJECT_ID('Tempdb..#Num') IS NOT NULL DROP TABLE #Num
SELECT
	h.Mes			,
	--h.renaes		,
	h.id_genero		,	
	h.num_doc		,
	DIU				= SUM(h.DIU)		,
	SIU				= SUM(h.SIU)		,
	OralComb		= SUM(h.OralComb)	,
	InyTrim			= SUM(h.InyTrim)	,
	InyMens			= SUM(h.InyMens)	,
	Implantes		= SUM(h.Implantes)	,
	PreservMasc		= SUM(h.PreservMasc),
	PreservFem		= SUM(h.PreservFem)	,
	Ligadura		= SUM(h.Ligadura)	,
	Vasectomia		= SUM(h.Vasectomia)	,
	Num				= CONVERT(INT,1)
INTO #Num
FROM 
(SELECT
	DISTINCT
	--a.renaes		,
	Mes				= 10,
	a.id_genero		,
	a.num_doc		,
	DIU			= IIF(a.cod_item = '58300'	  AND id_tipitem IN ('D','R'),1,0),
	SIU			= IIF(a.cod_item = '58300.01' AND id_tipitem IN ('D','R'),1,0),
	OralComb	= IIF(a.cod_item = '99208.13' AND id_tipitem = 'R',1,0),
	InyTrim		= IIF(a.cod_item = '99208.05' AND id_tipitem = 'R',1,0),
	InyMens		= IIF(a.cod_item = '99208.04' AND id_tipitem = 'R',1,0),
	Implantes	= IIF(a.cod_item = '11975'	  AND id_tipitem IN ('D','R'),1,0),
	PreservMasc = IIF(a.cod_item = '99208.02' AND id_tipitem = 'R',1,0),
	PreservFem	= IIF(a.cod_item = '99208.06' AND id_tipitem = 'R',1,0),
	Ligadura	= IIF(a.cod_item IN ('58600','58605','58611') AND id_tipitem IN ('D','R'),1,0),
	Vasectomia	= IIF(a.cod_item = '55250'	  AND id_tipitem IN ('D','R'),1,0)
FROM #hisminsa a
WHERE MONTH(a.fecha_aten) <= 10 
	AND a.id_cita IN (	SELECT
							h.id_cita
						FROM #hisminsa h
						WHERE h.cod_item = '99208'
							AND h.valor_lab = 'TA')
	AND (
			(a.cod_item = '58300'	 AND id_tipitem IN ('D','R')) -- DIU
		OR	(a.cod_item = '58300.01' AND id_tipitem IN ('D','R')) -- SIU
		OR	(a.cod_item = '99208.13' AND id_tipitem = 'R'		) -- Oral Combinado
		OR	(a.cod_item = '99208.05' AND id_tipitem = 'R'		) -- Inyeccion Trimestral
		OR	(a.cod_item = '99208.04' AND id_tipitem = 'R'		) -- Inyección Mensual
		OR	(a.cod_item = '11975'	 AND id_tipitem IN ('D','R')) -- Implantes
		OR	(a.cod_item = '99208.02' AND id_tipitem = 'R'		) -- Preservativos MAsculino
		OR	(a.cod_item = '99208.06' AND id_tipitem = 'R'		) -- Preservativos Femeninos
		OR	(a.cod_item IN ('58600','58605','58611') AND id_tipitem IN ('D','R')) --Ligadura
		OR	(a.cod_item = '55250'	 AND id_tipitem IN ('D','R')) -- Vasectomía
		)
) h
GROUP BY	h.Mes			,
			--h.renaes		,
			h.id_genero		,	
			h.num_doc		






