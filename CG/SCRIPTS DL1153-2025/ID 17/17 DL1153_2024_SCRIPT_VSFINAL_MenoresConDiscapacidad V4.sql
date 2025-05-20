--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°20 - DL 1153 - 2024
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Dirección de Prevención y Control de la
--								   Discapacidad (DSCAP)
-- NOMBRE: Porcentaje de niños y niñas menores de 5 años con deficiencias o factores de riesgo
--		   de discapacidad, con dos o más atenciones en la UPSS Medicina de Rehabilitación
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Piero Romero Marín (OGEI)
-- Fecha creación      : 31/10/2023
---------------------------------------------------------------------------------------------
-- Modificado por      : Erickson Javier Chuquimarca Bernal (OGEI)
-- Fecha modificación  : 11/12/2024
-- Motivo modificación : Cambios en la FICHA que realizó la estrategia(Cambios en ficha con fecha 10/12/24)
--*******************************************************************************************

use BD_HISINDICADORES
go

--***************************************************
--				BASES DE DATOS.
--***************************************************
--HIS MINSA - 2024
IF OBJECT_ID('Tempdb..#his_minsa') IS NOT NULL DROP TABLE #his_minsa


select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab
into #his_minsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK)
where	id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'		        AND --DIAGNOSTICOS DEFINITIVOS
		((h.edad_reg	< 5				AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		  renaes  NOT IN (16918,6210,6217) 
		  AND
		(
		cod_item in ('P073','P072',							-- Prematuridad
					 'P070',							    -- Extremadamente Bajo peso al nacer	
					 'Q796', 'Q798', 
					 'Q799', 'Q871', 'Q872', 'Q874', 
					 'Q875', 'Q878', 'Q992', 'Q998', 
					 'Q999',                                -- Malformaciones congénitas:
					 'A390','A870','A871',					-- Meningitis
					 'A850','A851','A858',					-- Encefalitis
					 'P579',								-- Kernicterus
					 'P90X',								-- Convulsions del RN
					 'P941',                               -- Hipertonía
					 'P942',                               -- Hipotonía
					 'H351',                               -- Retinopatía de la prematuridad
					 'E030','E031','E033',					-- Hipotiroidismo
					 'R629',					            -- Retardo del desarrollo
					 'F800','F801','F802', 'F804', 'F808',
					 'F809','F82', 'F83', 'F840', 'FR41', 
					 'F842','F844', 'F845', 'F848', 'F849',
					 'P360','P361','P362','P363',			-- Sepsis Neonatal
					 'P364','P365','P368','P369',
					 'A400','A401','A402','A403','A408',	-- Septicemia
					 'A409','A410','A411','A412','A413',
					 'A414','A415','A418','A419',
					 'G800','G801','G802','G803',			-- Parálisis cerebral
					 'G804','G808','G809'
					

)           OR		-- Retinopatía de prematuridad
		((substring(cod_item,1,3) BETWEEN  'Q00' AND 'Q09') OR (substring(cod_item,1,3) BETWEEN  'Q31' AND 'Q33')	-- Malformaciones congénitas
		OR (substring(cod_item,1,3) BETWEEN  'Q35' AND 'Q37') OR	(substring(cod_item,1,3) BETWEEN  'Q65' AND 'Q74') 
		OR	(substring(cod_item,1,3) BETWEEN  'Q76' AND 'Q78') OR (substring(cod_item,1,3) BETWEEN  'Q90' AND 'Q93') OR
		(substring(cod_item,1,3) BETWEEN  'Q95' AND 'Q96')) OR	
		substring(cod_item,1,3) = 'A86'				OR      --Encefalitis
		substring(cod_item,1,3) = 'F82'				OR		-- Retardo del desarrollo
		substring(cod_item,1,3) = 'F83'             )       --	Coriorretinitis 
UNION
 --********************TERCER NIVEL DE ATENCION
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab
from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK) 
where	convert(int,renaes) in (select r.COD_ESTAB from  [BD_BACKUP_OGEI].[dbo].[Renaes] r where LEFT(r.CAT_ESTAB,3) like 'III%') AND
		id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'       AND --DIAGNOSTICOS DEFINITIVOS
		((h.edad_reg	< 5				AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		(
		cod_item in ( --********************TERCER NIVEL DE ATENCION
					 'R620'						-- Desarrollo de aptitudes tardío en la infancia.
					))  
UNION
	 --*****************Instituto DE INR
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab

from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK) 
where	convert(int,renaes) = 7734 AND 
		id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'       AND --DIAGNOSTICOS DEFINITIVOS
		((h.edad_reg	< 5			AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		(
		cod_item in (
					 'H903'
					) ) 
			
UNION
	 --*****************Instituto Nacional de Salud del Niño de San Borja
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab

from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK) 
where	convert(int,renaes) = 16918 AND 
		id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'       AND --DIAGNOSTICOS DEFINITIVOS
		((h.edad_reg	< 5			AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		(
		cod_item in (
					 'R620', 'R629', 'F800', 'F801', 'F802', 'F808', 'F809', 'F82X', 'F83X', 'F849',							--RETARDO EN EL DESARROLLO
					 'G800', 'G801', 'G802', 'G803', 'G804', 'G808', 'G809',  --Parálisis cerebral
					 'H903',									-- Hipoacusia Neurosensoria
					 'T950', 'T951', 'T952', 'T953', 'T959'	-- Secuelas de quemaduras
					)  OR		-- Retinopatía de prematuridad
		((substring(cod_item,1,3) BETWEEN  'Q02' AND 'Q07') OR (substring(cod_item,1,3) BETWEEN  'Q32' AND 'Q33')
		OR (substring(cod_item,1,3) BETWEEN  'Q35' AND 'Q38') OR (substring(cod_item,1,3) BETWEEN  'Q65' AND 'Q74')-- Malformaciones congénitas
		OR	(substring(cod_item,1,3)  = 'Q76') OR (substring(cod_item,1,3) BETWEEN  'Q90' AND 'Q91'))) 
		
UNION
 --**************** INEM
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab

from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK) 
where	convert(int,renaes) = 6210 AND 
		id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'       AND --DEFINITIVOS
		((h.edad_reg	< 5				AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		(
		substring(cod_item,1,3) = 'C49'				OR		-- TUMORES
		substring(cod_item,1,3) = 'C71'             OR
		substring(cod_item,1,3) = 'C69'             
		) 
		
UNION
      --***************************Para el Instituto Nacional de Oftalmología 
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab

from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK) 
where	convert(int,renaes) = 6217 AND 
		id_tipo_doc		 IN (1,2)			AND --agregar CE
		sw				 = 1				AND
		id_tipitem       = 'D'       AND --DIAGNOSTICOS DEFINITIVOS
		((h.edad_reg	< 5			AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		(
		cod_item in (
					     --***************************Para el Instituto Nacional de Oftalmología 
					'Q123',                             ---Afaquia congénita
					'H270',                             ---Afaquia     
					'E703',                             ---Albinismo ocular
					'H530',                             ---Aniridia congénita
					'Q111',                             ---Anoftalmia
					'H472',                             ---Atrofia óptica
					'Q120',                             ---Catarata congénita
					'Q130',                             ---Coloboma de iris
					'Q122',                             ---Coloboma del cristalino
					'Q1420',                             ---Coloboma nervio óptico
					'Q1480',                             ---Coloboma retina o del fondo de ojo
					'H549',                             ---Discapacidad visual 
					'Q139',                             ---Disgenesia del segmento anterior
					'H355',                             ---Distrofia hereditaria de la retina
					'H509',                             ---Estrabismo congénito
					'Q150',                             ---Glaucoma congénito
					'E031',                             ---Hipotiroidismo congénito
					'P942',                             ---Hipotonía congénita
					'Q140',                             ---Malformación o alteraciones congénitas del humor vítreo Q14.0
					'Q134',                             ---Microcórnea
					'Q112',                             ---Microftalmos
					'H55X',                             ---Nistagmus congénito
					'G809',                             ---Parálisis cerebral infantil
					'Q100',                             ---Ptosis congénita
					'C6920',                             ---Retinoblastoma                   
					'H351',                             ---Retinopatía de la prematuridad        
					'P350',                             ---Rubeola congénita
					'P369',                             ---Sepsis neonatal
					'O689',                             ---Sufrimiento fetal agudo       
					'P371',                             ---Toxoplasmosis congénita
					'H476',                             ---Ceguera Cortical 
					'H475'                             ---Trastornos de las vías ópticas
					)  OR		-- Retinopatía de prematuridad
		substring(cod_item,1,3) = 'H30'  ) 				


-- CNV - 2024
IF OBJECT_ID('Tempdb..#cnv') IS NOT NULL DROP TABLE #cnv
select
	distinct
	num_doc		= CAST(c.nu_cnv AS varchar),
	prematuro	= IIF(try_convert(int,c.DUR_EMB_PARTO) < 28, 1, 0),
	APGAR5		= IIF(try_convert(int,c.APGAR_5_NACIDO) < 7, 1, 0)
into #cnv
from BD_BACKUP_OGEI.dbo.TramaCNV c WITH(NOLOCK)
where c.sw_cnv = 1
	and ( 
		  try_convert(int,c.dur_emb_parto)  < 28   OR		-- Prematuro al Nacer
		  try_convert(int,c.APGAR_5_NACIDO) < 7	   		-- APGAR
		)
		

-- Atenciones de las IPRESS de Rehab
IF OBJECT_ID('Tempdb..#IPRESS_REHAB') IS NOT NULL DROP TABLE #IPRESS_REHAB
select
	id_cita			,
	fecha_atencion	= convert(date,periodo)	,
	renaes			= convert(int,h.renaes)	,
	id_ups									,
	num_doc									,
	tipo_dx			= id_tipitem			,
	cod_item  								,
	valor_lab
INTO #IPRESS_REHAB
from BD_BACKUP_OGEI.dbo.TramaHisMinsa h WITH(NOLOCK)
INNER JOIN DL1153_2024_ANEXO01_HospitalesRehabilitacion r WITH(NOLOCK) ON h.renaes = r.renaes
where	id_tipo_doc		 IN (1,2)			AND
		sw				 = 1				AND
		((h.edad_reg	< 5			AND	h.id_tipedad_reg = 'A')	OR
		  h.id_tipedad_reg = 'M' OR		  h.id_tipedad_reg = 'D') AND
		  id_tipitem IN ('D','R')

-- Atenciones de las IPRESS de Rehab en UPSS_Rehab
IF OBJECT_ID('Tempdb..#IPRESS_REHAB_UPS') IS NOT NULL DROP TABLE #IPRESS_REHAB_UPS
SELECT
	t.num_doc,
	t.renaes,
	t.Mes,
	Atenciones = COUNT(*)
INTO #IPRESS_REHAB_UPS
FROM 
   (select
		DISTINCT
		h.id_cita			,
		Mes	= MONTH(h.fecha_atencion)	,
		h.renaes			,
		h.num_doc
	from #IPRESS_REHAB h WITH(NOLOCK)
	INNER JOIN DL1153_2024_ANEXO02_UPS_Rehabilitacion r WITH(NOLOCK) ON h.id_ups = r.id_ups COLLATE Modern_Spanish_CI_AS
	) t
GROUP BY	t.num_doc,
			t.renaes,
			t.Mes

--***************************************************
--					SINTAXIS
--***************************************************
-- Crear Tabla
IF OBJECT_ID('Tempdb..#tabla_reporte') IS NOT NULL DROP TABLE #tabla_reporte
create table #tabla_reporte
(
	num_doc		nvarchar(50)	,
	renaes		int			,
	Año			int			,
	mes			int			,
	den			int			,
	num			int			,
	Atenciones	int
)

/* Declaramos para el mensualizado */
declare @mes_inicio int,
		@mes_eval	int,
		@año		int 

set @mes_inicio	= 1		--<========= Mes inicio
set @mes_eval	= 10		--<========= Mes de evaluación
set @año		= 2024	--<========= Año de evaluación 

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%
-- Niños con alguna discapacidad que se atendieron en las IPRESS de REHAB
--========================================================================

-- Niños con alguna discapacidad:
--ESTABLECIMIENTOS NORMALES
IF OBJECT_ID('Tempdb..#Ninos_Discap') IS NOT NULL DROP TABLE #Ninos_Discap
SELECT
	h.num_doc,
	MES = CONVERT(INT,NULL),
	Den = 1,
	Num = 0
INTO #Ninos_Discap
FROM #his_minsa h
INNER JOIN BD_BACKUP_OGEI..Renaes R ON H.renaes=R.COD_ESTAB
WHERE 
MONTH(h.fecha_atencion) <= @mes_inicio and
H.tipo_dx = 'D' 
UNION
SELECT
	c.num_doc,
	Mes = CONVERT(INT,NULL),
	Den = 1,
	Num = 0
FROM #cnv c



-- Niños con alguna discapacidad que se atendieron en las IPRESS de REHAB
IF OBJECT_ID('Tempdb..#Denominador') IS NOT NULL DROP TABLE #Denominador
SELECT
	DISTINCT
	n.num_doc,
	r.renaes,
	MES = @mes_inicio,
	n.Den,
	n.Num
INTO #Denominador
FROM #Ninos_Discap n WITH(NOLOCK)
INNER JOIN #IPRESS_REHAB_UPS r WITH(NOLOCK) ON n.num_doc = r.num_doc
ORDER BY renaes



--%%%%%%%%%%%%%%%%%%%%%%
-- NUMERADOR e INDICADOR
--%%%%%%%%%%%%%%%%%%%%%%
INSERT INTO #tabla_reporte
SELECT
	d.num_doc	,
	d.renaes	,
	Año			= @año,
	Mes			= @mes_inicio				,
	d.Den		,
	Num			= IIF(n.Atenciones >= 2,1,0),
	Atenciones	= ISNULL(n.Atenciones,0)
FROM #Denominador d
LEFT JOIN   (	SELECT
					DISTINCT
					i.num_doc,
					i.renaes,
					Atenciones = SUM(i.atenciones)
				FROM #IPRESS_REHAB_UPS i
				WHERE i.Mes <= @mes_inicio
				GROUP BY i.num_doc,
						 i.renaes					) n	ON  d.num_doc = n.num_doc AND
															d.renaes  = n.renaes

set @mes_inicio = @mes_inicio + 1
end

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
--IF OBJECT_ID(N'dbo.DL1153_2024_CG20_Discapacidad5AniosAtendidos',N'U') IS NOT NULL

IF OBJECT_ID('Tempdb..#DL1153_2024_CG20_Discapacidad5AniosAtendidos') IS NOT NULL DROP TABLE #DL1153_2024_CG20_Discapacidad5AniosAtendidos

--DROP TABLE DL1153_2024_CG20_Discapacidad5AniosAtendidos

select 
	Diris			= r.DESC_DISA,
	Departamento	= r.DESC_DPTO,
	Provincia		= r.DESC_PROV,
	Distrito		= r.DESC_DIST,
	red				= r.DESC_RED ,
	r.DESC_ESTAB,
	r.CAT_ESTAB,
	a.*				
INTO #DL1153_2024_CG20_Discapacidad5AniosAtendidos
from #tabla_reporte a
INNER JOIN BD_BACKUP_OGEI.dbo.Renaes r ON r.COD_ESTAB = a.renaes

-- Exportamos el reporte final agrupado
SELECT
	d.Diris,
	d.Departamento,
	d.Provincia,
	d.RED,
	d.Distrito,
	d.renaes,
	d.CAT_ESTAB,
	d.DESC_ESTAB,
	d.Año,
	d.mes,
	Num = SUM(d.num),
	Den = SUM(d.den),
	Atenciones = SUM(d.atenciones)
FROM #DL1153_2024_CG20_Discapacidad5AniosAtendidos d
GROUP BY	d.Diris,
			d.Departamento,
			d.Provincia,
			d.RED,
			d.Distrito,
			d.renaes,
			d.CAT_ESTAB,
			d.DESC_ESTAB,
			d.Año,
			d.mes
ORDER BY	d.Diris,
			d.Departamento,
			d.Provincia,
			d.Distrito,
			d.renaes
