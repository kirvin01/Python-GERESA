--===========================================================================================
-- **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR N°01 - DL 1153 - 2024
-- ÁREA RESPONSABLE DEL INDICADOR: DGIESP / Unidad Funcional de Alimentación y
--											Nutrición Saludable (UFANS)
-- NOMBRE: Porcentaje de niñas y niños de 12 a 18 meses, con diagnóstico de anemia entre los 
-- 6 y 11 meses, que se han recuperado
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 10/11/2023
-- Motivo              : Revisar el código para el 2024
-- Modificado por	   : Wilson Urviola Zapata (OGEI)
--*******************************************************************************************
use BD_HISINDICADORES
go

--***************************************************
--				BASES DE DATOS.
--***************************************************
--HIS MINSA 2024
select id_cita
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
into #his_minsa
from BD_BACKUP_OGEI.dbo.TramaHisMinsa with (nolock)
where cod_item in (
					'D509','D649'			--Diagnostico de anemia.--
					,'99199.17','99199.11'							--Suplementacion con hierro.
					,'85018','85018.01'						--Dosaje de Hb. --(Cambio Z017 por 85018.01)--
				)
				and id_tipo_doc = 1 and sw = 1

--HIS MINSA 2023
insert into #his_minsa
select id_cita
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
from BD_BACKUP_OGEI_2023.dbo.TramaHisMinsa with (nolock)
where	cod_item in (
					'D509','D649'			--Diagnostico de anemia.--
					,'99199.17','99199.11'							--Suplementacion con hierro.
					,'85018','85018.01'					--Dosaje de Hb. --(Cambio Z017 por 85018.01)--
					) and
		id_tipo_doc = 1 and
				 sw = 1

--HIS MINSA 2022
insert into #his_minsa
select id_cita
	, convert(date,periodo) fecha_atencion
	, convert(int,aniomes) aniomes
	, num_doc
	, id_tipitem tipo_dx
	, cod_item  
	, valor_lab
from BD_FUENTES_HIS.dbo.TramaHisMinsa_2022_20230302 with (nolock)
where cod_item in (
					'D509','D649'			--Diagnostico de anemia.--
					,'99199.17','99199.11'						--Suplementacion con hierro. --
					,'85018','85018.01'						--Dosaje de Hb.--(Cambio Z017 por 85018.01)--
				)
and id_tipo_doc=1 and sw=1

--PADRON NOMINAL 2024
select distinct doc_pnm num_doc,
				co_ubigeo_inei ubigeo,
				ti_seguro_menor seguro,
				convert(date,fe_nac_menor) fecha_nac
into #padron_nominal
from BD_BACKUP_OGEI.dbo.TramaPadronNominal with (nolock)
where	convert(int,ti_doc_identidad) = 1 and
		sw_pn = 1 -- Validacion Dni


--***************************************************
--					SINTAXIS
--***************************************************
create table #tabla_reporte
(
num_doc nvarchar(8)
, fecha_nac date
, ubigeo int 
, seguro varchar(50)
, año int  
, mes int
, fecha_dx date 
, den int 
, num int
, fecha_supt1 date 
, num_supt1 int
, fecha_supt3 date 
, num_supt3 int
, fecha_recup date
, num_recup int 
, fecha_dosaje date
, num_dosaje int 
)

declare @mes_inicio int,
		@mes_eval int,
		@año int ,
		@fec_eval_1 date,
		@fec_eval_2 date

set @mes_inicio	= 1 --<========= Mes inicio
set @mes_eval	= 10 --<=========== Mes de evaluación
set @año		= 2024 --<============= Año de evaluación 

while @mes_inicio <= @mes_eval
begin

set @fec_eval_1=try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2))
set @fec_eval_2=eomonth(try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2)))

--%%%%%%%%%%%%%%%%
--  DENOMINADOR
--%%%%%%%%%%%%%%%%

-- 1.Niños que cumplen entre 360 a 573 días de edad en el período de evaluación
select distinct num_doc, 
				fecha_nac,
				ubigeo,
				seguro,
				@fec_eval_2 fecha_corte
into #padron_den1
from #padron_nominal
where ( ( @fec_eval_2 between dateadd(dd,360,fecha_nac) and dateadd(dd,573,fecha_nac) ) 
or ( @fec_eval_1 between dateadd(dd,360,fecha_nac) and dateadd(dd,573,fecha_nac) )  )

--Anemia de 6 a 11 meses
select a.num_doc
				, a.fecha_nac
				, a.ubigeo
				, a.seguro
				, a.fecha_corte
				, b.fecha_atencion
				, b.cod_item , b.tipo_dx, b.valor_lab
				into #Anemia
				from #padron_den1 a
				inner join #his_minsa b on a.num_doc=b.num_doc
				where b.fecha_atencion<=a.fecha_corte
				and ( datediff(dd,a.fecha_nac,b.fecha_atencion) between 170 and 364 )
				and cod_item in ('D509','D649') and tipo_dx='D' 

-- 2.Cumplen en el mes de evaluacion 209 dias adicionales a partir del DX. 
select num_doc, fecha_nac, ubigeo, seguro, fecha_corte
, max(fecha_atencion) fecha_dx, dx_anemia=1
into #padron_den2
from 	#Anemia	 as t
where month(dateadd(dd,209,fecha_atencion))=month(fecha_corte)
  and year(dateadd(dd,209,fecha_atencion))=year(fecha_corte)
group by num_doc, fecha_nac, ubigeo, seguro, fecha_corte

--3.Descontar del denominador CIEX: D649 + Tipo Dx: R + RF en todo el periodo del tratamiento de anemia.
delete from #padron_den2 
where num_doc in (select num_doc FROM #Anemia where cod_item='D649' AND tipo_dx='R' AND valor_lab='RF')


--%%%%%%%%%%%%%%%%
-- NUMERADOR
--%%%%%%%%%%%%%%%%

--1. Tratamiento/Suplementacion.

	--1.1. Tratamiento Oportuno.
	select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro, a.fecha_corte
	, min(fecha_atencion) fecha_supT1, sup_T1=1
	into #padron_num1_1
	from #padron_den2 a
	inner join #his_minsa b on a.num_doc=b.num_doc
	where b.cod_item IN ('99199.17','99199.11') --and ( valor_lab in ('SF1','SF2','SF3','P01','P02','PO1','PO2','PO3','P03','SF4','SF5','SF6','P04','P05','P06','PO4','PO5','PO6') or try_convert(int,valor_lab) in (1,2,3,4,5,6) ) 
	and ( b.fecha_atencion between a.fecha_dx and dateadd(dd,7,a.fecha_dx) )
	and b.id_cita in (select distinct id_cita from #his_minsa where  cod_item in ('D509','D649') and tipo_dx in ('D')) --(Agrego D539)(Retiro de tipo_dx R, por ser inicio)--
	and b.fecha_atencion<=a.fecha_corte
	group by a.num_doc, a.fecha_nac, a.ubigeo, a.seguro, a.fecha_corte

	--1.2. Continua Tratamiento.
	select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro, a.fecha_corte, a.fecha_supT1, b.fecha_atencion fecha_supT2
	into #padron_num1_1b
	from #padron_num1_1 a
	inner join #his_minsa b on a.num_doc=b.num_doc
	where b.cod_item IN ('99199.17','99199.11') --and ( valor_lab in ('SF1','SF2','SF3','P01','P02','PO1','PO2','PO3','P03','SF4','SF5','SF6','P04','P05','P06','PO4','PO5','PO6') or try_convert(int,valor_lab) in (1,2,3,4,5,6) ) 
	and ( b.fecha_atencion between dateadd(dd,25,a.fecha_supT1) and dateadd(dd,70,a.fecha_supT1) )
	and b.id_cita in (select distinct id_cita from #his_minsa where  cod_item in ('D509','D649') and tipo_dx in ('R')) --(Agrego D539)(Retiro de tipo_dx D, por ser continuador)--
	and b.fecha_atencion<=a.fecha_corte 

	select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro, a.fecha_corte, min(b.fecha_atencion) fecha_supT3, sup_T3=1
	into #padron_num1_2
	from #padron_num1_1b a
	inner join #his_minsa b on a.num_doc=b.num_doc
	where b.cod_item IN ('99199.17','99199.11') --and ( valor_lab in ('SF1','SF2','SF3','P01','P02','PO1','PO2','PO3','P03','SF4','SF5','SF6','P04','P05','P06','PO4','PO5','PO6') or try_convert(int,valor_lab) in (1,2,3,4,5,6) )  
	and ( b.fecha_atencion between dateadd(dd,25,a.fecha_supT2) and dateadd(dd,70,a.fecha_supT2) )
	and b.id_cita in (select distinct id_cita from #his_minsa where  cod_item in ('D509','D649') and tipo_dx in ('R'))  --(Agrego D539)(Retiro de tipo_dx D, por ser continuador)--
	and b.fecha_atencion<=a.fecha_corte	 
	group by a.num_doc, a.fecha_nac, a.ubigeo, a.seguro, a.fecha_corte


--2. Recuperación/Dosaje
	
	--2.1. Recuperacion de anemia.
	select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro
	, max(b.fecha_atencion) fecha_dx_pr, dx_pr=1
	into #padron_num2_1
	from #padron_den2 a
	inner join #his_minsa b on a.num_doc=b.num_doc
	where b.cod_item in ('D509','D649') and b.tipo_dx='R' and b.valor_lab='PR'  --(Agrego D539)--
	and b.fecha_atencion<=a.fecha_corte 
	and ( b.fecha_atencion between dateadd(dd,180,a.fecha_dx) and dateadd(dd,209,a.fecha_dx) )
	group by a.num_doc, a.fecha_nac, a.ubigeo, a.seguro

	--2.1. Dosaje de Hb
	select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro
	, max(b.fecha_atencion) fecha_dosaje, dosaje=1
	into #padron_num2_2
	from #padron_den2 a
	inner join #his_minsa b on a.num_doc=b.num_doc
	where b.cod_item in ('85018.01','85018') and b.tipo_dx='D' --(Cambio Z017 por 85018.01)
	and b.fecha_atencion<=a.fecha_corte 
	and ( b.fecha_atencion between dateadd(dd,180,a.fecha_dx) and dateadd(dd,209,a.fecha_dx) )
	group by a.num_doc, a.fecha_nac, a.ubigeo, a.seguro


--%%%%%%%%%%%%%%%%
--	INDICADOR
--%%%%%%%%%%%%%%%%
insert into #tabla_reporte
select a.num_doc, a.fecha_nac, a.ubigeo, a.seguro
, year(a.fecha_corte) año, month(a.fecha_corte) mes
, b.fecha_dx, b.dx_anemia den														--Dx de anemia
, iif(c.sup_T1=1 and d.sup_T3=1 and e.dx_pr=1 and f.dosaje=1,1,0) num				--Numerador (Tratamiento, Recuperado, Dosaje)
, c.fecha_supT1, isnull(c.sup_T1,0) sup_T1											--Tratamiento Oportuno
, d.fecha_supT3, isnull(d.sup_T3,0) sup_T3											--Tratamiento Continuo
, e.fecha_dx_pr, isnull(e.dx_pr,0) dx_pr											--Dx recuperado
, f.fecha_dosaje, isnull(f.dosaje,0) dosaje											--Dosaje 
from #padron_den1 a
inner join #padron_den2 b on a.num_doc=b.num_doc and a.fecha_nac=b.fecha_nac and a.seguro=b.seguro and a.ubigeo=b.ubigeo
left join #padron_num1_1 c on a.num_doc=c.num_doc and a.fecha_nac=c.fecha_nac and a.seguro=c.seguro and a.ubigeo=c.ubigeo
left join #padron_num1_2 d on a.num_doc=d.num_doc and a.fecha_nac=d.fecha_nac and a.seguro=d.seguro and a.ubigeo=d.ubigeo
left join #padron_num2_1 e on a.num_doc=e.num_doc and a.fecha_nac=e.fecha_nac and a.seguro=e.seguro and a.ubigeo=e.ubigeo
left join #padron_num2_2 f on a.num_doc=f.num_doc and a.fecha_nac=f.fecha_nac and a.seguro=f.seguro and a.ubigeo=f.ubigeo
where b.dx_anemia=1 

drop table #padron_den1
drop table #padron_num1_1
drop table #padron_num1_1b
drop table #padron_num1_2
drop table #padron_den2
drop table #padron_num2_1
drop table #padron_num2_2
drop table #Anemia

set @mes_inicio = @mes_inicio + 1
end

--===================
-- REPORTE
--===================
-- Insertamos los resultados en la Tabla de Resultados del Indicador
IF OBJECT_ID(N'dbo.DL1153_2025_CG01_NiñoAnemiaRecup',N'U') IS NOT NULL
DROP TABLE DL1153_2025_CG01_NiñoAnemiaRecup

select
	Diris		 = B.diris,
	Departamento =	CASE 
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV  = 'LIMA' THEN 'LIMA METROPOLITANA'
						WHEN B.DESC_DPTO = 'LIMA' AND B.DESC_PROV <> 'LIMA' THEN 'LIMA PROVINCIAS'
						ELSE B.DESC_DPTO
					END,
	Provincia	= B.desc_prov,
	Distrito	= B.desc_dist,
	B.Red,
	a.*
into BD_HISINDICADORES.dbo.DL1153_2025_CG01_NiñoAnemiaRecup
from #tabla_reporte a
inner join Maestro_UBIGEO_20200407_2023 b on CONVERT(int,a.ubigeo) = CONVERT(int,b.ubigeo) 

-- Exportamos el reporte final agrupado
select
	Diris		,
	Departamento,
	Provincia	,
	Distrito	,
	Red			,
	ubigeo		,
	seguro		,
	mes			,
	año 		,
	den			= sum(den)			,
	num			= sum(num)			,
	num_t1		= sum(num_supt1)	,
	num_T3		= sum(num_supt3)	,
	num_dosaje	= sum(num_dosaje)	,
	num_recup	= sum(num_recup)
from BD_HISINDICADORES.dbo.DL1153_2025_CG01_NiñoAnemiaRecup
group by	Diris, Departamento, Provincia, Distrito,
			Red, ubigeo, seguro, mes, año 


-- Limpiamos Tablas temporales
drop table #his_minsa
drop table #padron_nominal
drop table #tabla_reporte

drop table #padron_den1
drop table #padron_num1_1
drop table #padron_num1_1b
drop table #padron_num1_2
drop table #padron_den2
drop table #padron_num2_1
drop table #padron_num2_2
