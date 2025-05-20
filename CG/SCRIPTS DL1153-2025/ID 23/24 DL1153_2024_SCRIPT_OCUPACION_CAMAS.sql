/* **************** OFICINA DE GESTIÓN DE LA INFORMACIÓN (OGEI/OGTI/MINSA) ******************
FICHA: INDICADOR N° - DL 1153 - 2024
NOMBRE: Porcentaje de ocupación cama													   */
--===========================================================================================

--*******************************************************************************************
-- Creado por (2023)   : Jhonatan Lavi Casilla (OGEI)
-- Fecha creación      : 23/01/2023
---------------------------------------------------------------------------------------------
-- Fecha Actualización : 25/10/2023
-- Motivo              : Modificaciones en la FICHA para los Indicadores del 2024
-- Modificado por	   : Piero Romero Marín (OGEI)
--*******************************************************************************************

USE BD_HISINDICADORES
GO

--***************************************************
--				BASES DE DATOS.
--***************************************************
--Base Estadistica de la FICHA 500.2 (RENOXI)
select
	renaes						   ,
	fecha						   ,
	Total_camas_disponible		   ,
	Total_camas_ocupadas		   ,
	total_camas		       = Total_camas_disponible +
							 Total_camas_ocupadas				 ,
	Total_camas_ocupadas_adulto    ,
	total_camas_adulto     = Total_camas_disponible_adulto +
							 Total_camas_ocupadas_adulto ,
	Total_camas_ocupadas_pediatrico,
	total_camas_pediatrico = Total_camas_disponible_pediatrico + Total_camas_ocupadas_pediatrico
into #ficha5002_camas_hosp
from (
		SELECT
			renaes = converT(int,a.CODIGO),
		    fecha = convert(date,a.FECHACORTE)			,
		    Total_camas_disponible = isnull(znc_HOSP_ADUL_CAM_DISPONIBLE,0)		+ 
									 isnull(znc_HOSP_NEONATAL_CAM_DISPONIBLE,0) + 
									 isnull(znc_HOSP_PEDIA_CAM_DISPONIBLE,0)				,
		    Total_camas_disponible_adulto	  = isnull(znc_HOSP_ADUL_CAM_DISPONIBLE,0)		,
		    Total_camas_disponible_pediatrico = isnull(znc_HOSP_NEONATAL_CAM_DISPONIBLE,0) +
												isnull(znc_HOSP_PEDIA_CAM_DISPONIBLE,0)		,
		    Total_camas_ocupadas =  isnull(znc_HOSP_ADUL_CAM_OCUPADO,0)		+ 
									isnull(znc_HOSP_NEONATAL_CAM_OCUPADO,0) + 
									isnull(znc_HOSP_PEDIA_CAM_OCUPADO,0)					,
		    Total_camas_ocupadas_adulto		=	isnull(znc_HOSP_ADUL_CAM_OCUPADO,0)			,
		    Total_camas_ocupadas_pediatrico =	isnull(znc_HOSP_NEONATAL_CAM_OCUPADO,0) +
												isnull(znc_HOSP_PEDIA_CAM_OCUPADO,0)		,
			id = row_number() over(partition by a.codigo, a.fechacorte
									   order by a.fecharegistro asc)
		FROM  BD_TRAMA_HISMINSA.dbo.tb_camas_f500_dl1153 a WITH(NOLOCK)
		WHERE	YEAR(CONVERT(DATE,a.FECHACORTE)) = 2024 AND
				CONVERT(VARCHAR,a.FECHAREGISTRO) LIKE '%AM%'
) as t 
where	id = 1 and
		try_convert(int,renaes) is not null
order by renaes, fecha asc

select
	renaes						   ,
	fecha						   ,
	Total_camas_disponible_uci		   ,
	Total_camas_ocupadas_uci		   ,
	total_camas_uci		       = Total_camas_disponible_uci +
								Total_camas_ocupadas_uci				 ,
	Total_camas_ocupadas_adulto_uci    ,
	total_camas_adulto_uci     = Total_camas_disponible_adulto_uci +
								 Total_camas_ocupadas_adulto_uci ,
	Total_camas_ocupadas_pediatrico_uci,
	total_camas_pediatrico_uci = Total_camas_disponible_pediatrico_uci + Total_camas_ocupadas_pediatrico_uci
into #ficha5002_camas_uci
from (
		SELECT
			renaes = converT(int,b.CODIGO),
		    fecha = convert(date,b.FECHACORTE)	,		
			Total_camas_disponible_uci = isnull(b.ZNC_UCI_ADUL_CAM_DISPONIBLE,0)		+ 
										 isnull(b.ZNC_UCI_NEONATAL_CAM_DISPONIBLE,0) + 
										 isnull(b.ZNC_UCI_PEDIA_CAM_DISPONIBLE,0)				,
		    Total_camas_disponible_adulto_uci	  = isnull(b.ZNC_UCI_ADUL_CAM_DISPONIBLE,0)		,
		    Total_camas_disponible_pediatrico_uci = isnull(b.ZNC_UCI_NEONATAL_CAM_DISPONIBLE,0) +
													isnull(b.ZNC_UCI_PEDIA_CAM_DISPONIBLE,0)		,
		    Total_camas_ocupadas_uci =  isnull(b.ZNC_UCI_ADUL_CAM_OCUPADO,0)		+ 
										isnull(b.ZNC_UCI_NEONATAL_CAM_OCUPADO,0) + 
										isnull(b.ZNC_UCI_PEDIA_CAM_OCUPADO,0)					,
		    Total_camas_ocupadas_adulto_uci		=	isnull(b.ZNC_UCI_ADUL_CAM_OCUPADO,0)			,
		    Total_camas_ocupadas_pediatrico_uci =	isnull(b.ZNC_UCI_NEONATAL_CAM_OCUPADO,0) +
													isnull(b.ZNC_UCI_PEDIA_CAM_OCUPADO,0)		,
			id = row_number() over(partition by b.codigo, b.fechacorte
									   order by b.fecharegistro asc)
		FROM BASE_CAMAS_UCI_ENE_A_NOV_2024 b
		WHERE	YEAR(CONVERT(DATE,b.FECHACORTE)) = 2024 --AND
				--CONVERT(VARCHAR,b.FECHAREGISTRO) LIKE '%AM%'
) as t 
where	id = 1 and
		try_convert(int,renaes) is not null
order by renaes, fecha asc
---------------------------------
SELECT		renaes=IIF(a.renaes= null , b.renaes,a.renaes ),
			fecha=IIF(a.fecha = null, b.fecha,a.fecha),
			total_camas_disponible,
			Total_camas_ocupadas,
			total_camas,
			Total_camas_ocupadas_adulto,
			total_camas_adulto,
			Total_camas_ocupadas_pediatrico,
			total_camas_pediatrico,
			Total_camas_disponible_uci,
			Total_camas_ocupadas_uci,
			total_camas_uci,
			Total_camas_ocupadas_adulto_uci,
			total_camas_adulto_uci,
			Total_camas_ocupadas_pediatrico_uci,
			total_camas_pediatrico_uci
INTO #ficha5002_camas
FROM #ficha5002_camas_hosp a
FULL OUTER JOIN #ficha5002_camas_uci b on b.renaes = a.renaes and b.fecha=a.fecha 


--select * from #ficha5002_camas_uci
---------------------------------
--SELECT *,CONVERT(DATE,fecha) FROM #ficha5002_camas
--WHERE CONVERT(DATE,fecha) between '2024-01-06' and '2024-10-31'

--SELECT * FROM #ficha5002_camas
-------------------------------------------
---------------------------------
--***************************************************
--					SINTAXIS
--***************************************************

create table #tabla_reporte
(
	renaes int,
	año    int,
	mes	   int,
	fecha  date,
	den	   int,
	num    int,	
	total_camas_adulto					int,
	total_camas_pediatrico				int,
	Total_camas_ocupadas_adulto			int,
	Total_camas_ocupadas_pediatrico		int,
	den_uci								int,
	num_uci								int,
	total_camas_adulto_uci				int,
	total_camas_pediatrico_uci			int,
	Total_camas_ocupadas_adulto_uci		int,
	Total_camas_ocupadas_pediatrico_uci int
)

declare @mes_inicio int,
		@mes_eval int,
		@año int, 
		@fecha_inicio date,
		@fecha_final date

set @mes_inicio	= 1 --<=========== Mes de inicio		
set @mes_eval	= 10 --<=========== Mes de evaluación
set @año		= 2024 --<============= Año de evaluación 

set @fecha_inicio=try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_inicio),2)+'-'+right('00'+try_convert(varchar(2),1),2))
set @fecha_final=eomonth(try_convert(date,try_convert(varchar(4),@año)+'-'+right('00'+try_convert(varchar(2),@mes_eval),2)+'-'+right('00'+try_convert(varchar(2),1),2)))


while @fecha_inicio<=@fecha_final
begin 

Print @fecha_inicio
Print @fecha_final

		--%%%%%%%%%%%%%%%%
		--  DENOMINADOR
		--%%%%%%%%%%%%%%%%
		-- Número de días-cama-disponible se obtiene de la sumatoria de todas las camas existentes para hospitalización por cada día del mes
		-- Camas Hospitalizaciòn y UCI
		select renaes
			  , @fecha_inicio fecha
			  , total_camas den
			  , total_camas_adulto
			  , total_camas_pediatrico
			  , total_camas_uci den_uci
			  , total_camas_adulto_uci
			  , total_camas_pediatrico_uci			
		into #ficha5002_den
		from #ficha5002_camas
		where fecha= @fecha_inicio


		--%%%%%%%%%%%%%%%%
		--  NUMERADOR
		--%%%%%%%%%%%%%%%%
		-- Número total de pacientes-día del mes se obtiene de la sumatoria de todos los pacientes-día censados día a día durante el mes
		select renaes
			  , @fecha_inicio fecha
			  , Total_camas_ocupadas num
			  , Total_camas_ocupadas_adulto
			  , Total_camas_ocupadas_pediatrico
			  , Total_camas_ocupadas_uci num_uci
			  , Total_camas_ocupadas_adulto_uci		
			  , Total_camas_ocupadas_pediatrico_uci 
		into #ficha5002_num
		from #ficha5002_camas
		where fecha= @fecha_inicio


	--%%%%%%%%%%%%%%%%
	--	INDICADOR
	--%%%%%%%%%%%%%%%%
	insert into #tabla_reporte
	select a.renaes
		, year(a.fecha) año
		, month(a.fecha) mes
		, a.fecha
		, a.den
		, isnull(b.num,0) num
		, a.total_camas_adulto
		, a.total_camas_pediatrico
		, b.Total_camas_ocupadas_adulto
		, b.Total_camas_ocupadas_pediatrico
		, a.den_uci
		, isnull(b.num_uci,0) num_uci
		, a.total_camas_adulto_uci
		, a.total_camas_pediatrico_uci
		, b.Total_camas_ocupadas_adulto_uci		
		, b.Total_camas_ocupadas_pediatrico_uci 
	from #ficha5002_den a
	left join #ficha5002_num b on a.renaes=b.renaes and a.fecha=b.fecha


drop table #ficha5002_den
drop table #ficha5002_num

set @fecha_inicio=dateadd(dd,1,@fecha_inicio)
end 

--===================
-- REPORTE
--===================
--drop table BD_HISINDICADORES.dbo.DL1153_2023_CG22_OcupacionCama

--ALTER TABLE BD_HISINDICADORES.dbo.DL1153_2023_CG22_OcupacionCama 
--ADD Indicador INT NULL, Mes_Corte INT NULL;

--UPDATE a SET
--	a.Indicador = 22,
--	a.Mes_Corte = 6
--FROM DL1153_2023_CG22_OcupacionCama a

--INSERT INTO BD_HISINDICADORES.dbo.DL1153_2023_CG22_OcupacionCama
IF OBJECT_ID('Tempdb..#Reporte') IS NOT NULL DROP TABLE #Reporte
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
--, d.Tipo filtro_aplicacion
, a.*
	--Indicador = 22,
	--Mes_Corte = @mes_eval
INTO #Reporte
from #tabla_reporte a
inner join BD_BACKUP_OGEI.dbo.Renaes b on convert(int,a.renaes)=convert(int,b.COD_ESTAB)
where (b.CAT_ESTAB in ('II-1','II-2','II-E','III-1','III-2','III-E'))
and converT(int,b.AMBITO)=1 and den > 0 AND b.COD_ESTAB NOT IN ('5197','6217','6212','6213')

--Reporte final
select * 
from #Reporte

select * 
from #Reporte
where den_uci>0

drop table #ficha5002_camas
drop table #tabla_reporte
