
--===========================================================================================
-- **************** OFICINA DE GESTI�N DE LA INFORMACI�N (OGEI/OGTI/MINSA) ******************
-- FICHA: INDICADOR - DL 1153 - 2025
-- NOMBRE: Porcentaje de reci�n nacidos del departamento,reciben vacunas BCG, HvB, controles 
--		   CRED y tamizaje neonatal.
--===========================================================================================

--*******************************************************************************************
-- Creado por          : Wilson URVIOLA ZAPATA - ANALISTA ESTADISTICO
-- Fecha creaci�n      : 15/08/2024
-- Modificado por	   : Ing Irvin Condori Champi
---------------------------------------------------------------------------------------------

-- Especificar la Base de datos a utilizar y en donde se guarda la informaci�n. 
use DBCGESTION
go

/* **********************************************************
				Tablas.

En esta parte se procede a seleccionar las Tablas/tramas/Base de datos 
utilizadas para generar el indicador. Las Tablas/tramas/Base de datos utilizados 
para este indicador son:

- 1.HIS MINSA (Mes de evalucaci�n)			| Cambia mes a mes.
- 2.HIS MINSA (A�o Previo)
- 3.Padron Nominal (Mes de Evaluaci�n)		| Cambia mes a mes.
- 4.CNV (Mes de Evaluaci�n)					| Cambia mes a mes.
************************************************************* */

-- 1.Tabla de datos : HIS - MINSA (Mes de evalucaci�n)
if Object_id(N'tempdb..#his_minsa',N'U') is not null drop table #his_minsa;
select id_cita
	, Codigo_Unico renaes
	, fecha_atencion
	, convert(int,CONCAT(YEAR(Fecha_Atencion),MONTH(Fecha_Atencion) )) aniomes
	, Numero_Documento num_doc
	, Tipo_Diagnostico tipo_dx
	, Codigo_Item cod_item  
	, valor_lab
into #his_minsa
from DBGERESA.dbo.NOMINAL_TRAMA_NUEVO t
inner join DBGERESA.dbo.MAESTRO_PACIENTE p on t.Id_Paciente=p.Id_Paciente
inner join DBGERESA.dbo.RENIPRESS r on t.Id_Establecimiento=r.Codigo_Unico*1
where Codigo_Item in (
						'90585'							-- C�digo de Vacuna BCG
						,'90744'						-- C�digo de Vacuna HVB
						,'z001','99381.01'				-- C�digos de CRED
						,'36416'						-- C�digo de Tamizaje Neonatal
				)
--and sw=1					-- Validaci�n del numero de documento.
and Id_Tipo_Documento in (1,6)	-- Tipo de documento: DNI o CNV.
and Anio=2025


-- 2.Tabla de datos : HIS MINSA (A�o Previo)
insert into #his_minsa
select id_cita
	, Codigo_Unico renaes
	, fecha_atencion
	, convert(int,CONCAT(YEAR(Fecha_Atencion),MONTH(Fecha_Atencion) )) aniomes
	, Numero_Documento num_doc
	, Tipo_Diagnostico tipo_dx
	, Codigo_Item cod_item  
	, valor_lab
from DBGERESA.dbo.NOMINAL_TRAMA_NUEVO t
inner join DBGERESA.dbo.MAESTRO_PACIENTE p on t.Id_Paciente=p.Id_Paciente
inner join DBGERESA.dbo.RENIPRESS r on t.Id_Establecimiento=r.Codigo_Unico*1
where Codigo_Item in (
						'90585'							-- C�digo de Vacuna BCG
						,'90744'						-- C�digo de Vacuna HVB
						,'z001','99381.01'				-- C�digos de CRED
						,'36416'						-- C�digo de Tamizaje Neonatal
					)
--and sw=1					-- Validaci�n del numero de documento.
and Id_Tipo_Documento in (1,6)	-- Tipo de documento: DNI o CNV.
and Anio=2025-1

-- 3.Tabla de datos : Padron Nominal (Mes de evaluaci�n)
if Object_id(N'tempdb..#padron_nominal',N'U') is not null drop table #padron_nominal;
select *
into #padron_nominal
from (
	select distinct DNI tipo_doc,
					co_ubigeo_inei ubigeo,
					seguro= case 
					when convert(int,TI_SEGURO_MENOR)='1' then 'MINSA'	
					when convert(int,TI_SEGURO_MENOR)='2' then 'ESSALUD'
					when convert(int,TI_SEGURO_MENOR)='3' then 'SANIDAD FFAA/PNP' 
					when convert(int,TI_SEGURO_MENOR)='4' then 'PRIVADO'
					else 'SIN REGISTRO' END,
					convert(date,fe_nac_menor) fecha_nac,
					case when DE_GENERO_MENOR='FEMENINO' then 'F' else 'M' end sexo
	from DBGERESA.dbo.PadronNominal with (nolock)
) as t 

		--------------------------------------------
		--- Eliminar registros vacios o nulos ---
		delete from #padron_nominal
		where num_doc is null or num_doc=''

		--------------------------------------------
		--- Eliminar registros Duplicados ---
		delete from #padron_nominal
		where num_doc in (
					select num_doc
					from #padron_nominal
					group by num_doc 
					having count(*)>1
					) 
				and tipo_doc not in ('DNI')
		----------------------------------------


-- 4.Tabla de datos : CNV 
if Object_id(N'tempdb..#cnv',N'U') is not null drop table #cnv;
select distinct nu_cnv num_doc ,
				try_convert(int,peso_nacido)	Peso,
				try_convert(int,dur_emb_parto)	Sem_Gestacion
into #cnv 
from BD_BACKUP_OGEI.dbo.TramaCNV with (nolock)
where sw_cnv=1										-- Validaci�n del numero de documento.

/* *************************************************************************************
				........................
					TABLA TEMPORAL
				.......................
En esta parte se procede a generar una tabla temporal donde 
se van a insertar los valores del indicador.
El proceso de la sintaxis es un loop que va corriendo el indicador
mes a mes , para eso se debe colocar en:	
	- @mes_eval : El mes donde inicia la busqueda de informaci�n del indicador.
	- @mes_final: El mes donde finaliza la busqueda de informaci�n (Periodo de evaluaci�n).

					................
						SINTAXIS
					................
Tabmien en esta parte se procede a generar el c�digo que busca construir el denominador y numerador 
del indicador propuesto a partir de las especificaciones en la Ficha Tecnica. 
****************************************************************************************** */

--1. Generaci�n de la tabla temporal.
if Object_id(N'tempdb..#tabla_reporte',N'U') is not null drop table #tabla_reporte;
create table #tabla_reporte
(
a�o						int,
mes						int,
tipo_doc				nvarchar(3),
num_doc					nvarchar(15),
fecha_nac				date,
sexo					nvarchar(1),
ubigeo					int,
seguro					nvarchar(30),
flag_cnv				int,
peso_cnv				int,
flag_BPN				int,
Semana_gest_cnv			int,
flag_prematuro			int,
flag_BPN_Prematuro		int,
flag_indicador			int,
fecha_vac_BCG			date,
num_vac_BCG				int,
fecha_vac_HvB			date,
num_vac_Hvb				int,
num_vac_RN				int,
fecha_cred_RN1			date,
num_cred_RN1			int,
fecha_cred_RN2			date,
num_cred_RN2			int,
fecha_cred_RN3			date,
num_cred_RN3			int,
fecha_cred_RN4			date,
num_cred_RN4			int,
num_cred_RN				int,
fecha_tamizaje_neo		date,
num_tamizaje_neo		int,
numerador				int,
denominador				int
) 

declare @mes_inicio int, 
		@mes_eval int, 
		@a�o int 

set @a�o=2024
set @mes_inicio=1 
set @mes_eval=9 --< Modificar segun mes de evaluaci�n.

while @mes_inicio <= @mes_eval
begin

--%%%%%%%%%%%%%%%%
-- 1.DENOMINADOR
--%%%%%%%%%%%%%%%%

/* 1.1 - Ni�as y ni�os que cumplen 29 d�as de nacido del Padron Nominal en el mes de medici�n. */
if Object_id(N'tempdb..#padron_Rn',N'U') is not null drop table #padron_Rn
select	tipo_doc,
		num_doc,
		fecha_nac,
		sexo,
		ubigeo,
		seguro
into #padron_Rn
from #padron_nominal
where year(dateadd(dd,29,fecha_nac))=@a�o
and month(dateadd(dd,29,fecha_nac))=@mes_inicio					-- cumplen 29 d�as de nacido


/* 1.2 - Se excluye a ni�os y ni�as con bajo peso al nacer (menor de 2500 gramos) y/o prematuros (menor de 37 SG)
, registrados en CNV en l�nea		*/
if Object_id(N'tempdb..#cnv_bpn_premat',N'U') is not null drop table #cnv_bpn_premat;
select num_doc
, max(peso)	peso
, max(sem_gestacion) sem_gestacion
, max(iif(peso<2500,1,0)) BPN											-- Bajo Peso al Nacer
, max(iif(Sem_Gestacion<37,1,0)) Prematuro								-- Prematuro al Nacer
, max(iif( (peso<2500) or (Sem_Gestacion<37),1,0))	BPN_Premat			-- Bajo Peso o Prematuro al Nacer
into #cnv_bpn_premat
from #cnv
group by num_doc

	
/* 1.3 - Uni�n de Informaci�n para armar el denominador del indicador. */
if Object_id(N'tempdb..#denominador',N'U') is not null drop table #denominador;
select a.*
, iif(b.num_doc is null,0,1)	flag_cnv
, isnull(b.Peso,0)				Peso_cnv
, isnull(b.BPN,0)				flag_BPN
, isnull(b.Sem_Gestacion,0)		Semana_gest_cnv
, isnull(b.Prematuro,0)			flag_prematuro
, isnull(b.BPN_Premat,0)		flag_BPN_Prematuro
, iif(b.BPN_Premat=1,0,1)		flag_indicador
into #denominador
from #padron_Rn a
left join #cnv_bpn_premat b on a.num_doc=b.num_doc


--%%%%%%%%%%%%%%%%
-- 2.NUMERADOR
--%%%%%%%%%%%%%%%%    

--==========================================
--		VACUNAS RECIEN NACIDO
--==========================================

/* 2.1 - Ni�os y Ni�as del denominador que Cuentan con vacunas completa para la edad BCG: registrado con c�digo 90585 (desde la fecha de nacimiento + 01 d�a) */
if Object_id(N'tempdb..#Num_Vac_BCG',N'U') is not null drop table #Num_Vac_BCG;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		min(b.fecha_atencion) fecha_vac_BCG,
		num_vac_BCG=1
into #Num_Vac_BCG
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item='90585'													-- C�digos de vacuna BCG.
and ( b.fecha_atencion between a.fecha_nac and dateadd(dd,1,a.fecha_nac) )	-- La vacuna se aplica dentro de las 24 horas de nacidos.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro


/* 2.2 -  Ni�os y Ni�as del denominador que Cuentan con vacunas completa para la edad HVB: registrado con c�digo 90744 (desde la fecha de nacimiento + 01 d�a) */
if Object_id(N'tempdb..#num_vac_HvB',N'U') is not null drop table #num_vac_HvB;
select	a.tipo_doc, 
		a.num_doc, 
		a.fecha_nac, 
		a.ubigeo, 
		a.seguro, 
		min(b.fecha_atencion) fecha_vac_HvB,
		num_vac_HvB=1
into #num_vac_HvB
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item='90744'													-- C�digos de vacuna HVB.
and ( b.fecha_atencion between a.fecha_nac and dateadd(dd,1,a.fecha_nac) )	-- La vacuna se aplica dentro de las 24 horas de nacidos.
group by a.tipo_doc, a.num_doc, a.fecha_nac, 
		a.ubigeo, a.seguro


--==========================================
--		CRED RECIEN NACIDO
--==========================================

/* 2.3 - Ni�os y Ni�as del denominador que Cuentan con 4 controles CRED (hasta los 28 dias) registrado con c�digo: CIE-10 Z001 o CPMS 99381.01 
		* Primer Control
		* El primer control CRED, se realiza a partir del 3er d�a de vida
*/
if Object_id(N'tempdb..#num_cred_RN1',N'U') is not null drop table #num_cred_RN1;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b.fecha_atencion fecha_cred_RN1,
				num_cred_RN1=1
into #num_cred_RN1
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('z001','99381.01') 									-- C�digos de CRED.
and b.fecha_atencion>=dateadd(dd,3,a.fecha_nac) 							-- El control se realiza a partir del 3er d�a de vida.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)


/* 2.4 - Ni�os y Ni�as del denominador que Cuentan con 4 controles CRED (hasta los 28 dias) registrado con c�digo: CIE-10 Z001 o CPMS 99381.01 
		* Segundo Control
		* El segundo control con intervalo m�nimo de 3 dias a partir del 1er control
*/
if Object_id(N'tempdb..#num_cred_RN2',N'U') is not null drop table #num_cred_RN2;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b.fecha_atencion fecha_cred_RN2,
				num_cred_RN2=1
into #num_cred_RN2
from #num_cred_RN1 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('z001','99381.01') 									-- C�digos de CRED.
and b.fecha_atencion>=dateadd(dd,3,a.fecha_cred_RN1) 						-- El control se realiza con intervalo m�nimo de 3 dias a partir del 1er control
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)


/* 2.5 - Ni�os y Ni�as del denominador que Cuentan con 4 controles CRED (hasta los 28 dias) registrado con c�digo: CIE-10 Z001 o CPMS 99381.01 
		* Tercer Control
		* El tercer control CRED, se realiza un control cada semana (07 d�as), m�nimo
*/
if Object_id(N'tempdb..#num_cred_RN3',N'U') is not null drop table #num_cred_RN3;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b.fecha_atencion fecha_cred_RN3,
				num_cred_RN3=1
into #num_cred_RN3
from #num_cred_RN2 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('z001','99381.01') 									-- C�digos de CRED.
and b.fecha_atencion>=dateadd(dd,7,a.fecha_cred_RN2) 						-- El control se realiza cada semana (07 d�as), m�nimo.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)


/* 2.6 - Ni�os y Ni�as del denominador que Cuentan con 4 controles CRED (hasta los 28 dias) registrado con c�digo: CIE-10 Z001 o CPMS 99381.01 
		* Cuarto Control
		* El Cuarto control CRED, se realiza un control cada semana (07 d�as), m�nimo
*/
if Object_id(N'tempdb..#num_cred_RN4',N'U') is not null drop table #num_cred_RN4;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				b.fecha_atencion fecha_cred_RN4,
				num_cred_RN4=1
into #num_cred_RN4
from #num_cred_RN3 a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item in ('z001','99381.01') 									-- C�digos de CRED.
and b.fecha_atencion>=dateadd(dd,7,a.fecha_cred_RN3) 						-- El control se realiza cada semana (07 d�as), m�nimo.
and b.fecha_atencion<=dateadd(dd,28,a.fecha_nac)


/* 2.7 Ni�os y Ni�as del denominador que Cuentan con 4 controles CRED (hasta los 28 dias)  */
if Object_id(N'tempdb..#num_cred_RN',N'U') is not null drop table #num_cred_RN;
select distinct	a.tipo_doc, a.num_doc, a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				min(b1.fecha_cred_RN1)	fecha_cred_RN1,	max(isnull(b1.num_cred_RN1,0))	num_cred_RN1,
				min(b2.fecha_cred_RN2)	fecha_cred_RN2,	max(isnull(b2.num_cred_RN2,0))	num_cred_RN2,
				min(b3.fecha_cred_RN3)	fecha_cred_RN3,	max(isnull(b3.num_cred_RN3,0))	num_cred_RN3,
				min(b4.fecha_cred_RN4)	fecha_cred_RN4,	max(isnull(b4.num_cred_RN4,0))	num_cred_RN4,
				max(iif(b1.num_cred_RN1=1 AND b2.num_cred_RN2=1 AND b3.num_cred_RN3=1 AND b4.num_cred_RN4=1,1,0))	num_cred_RN
into #num_cred_RN
from #denominador a
left join #num_cred_RN1	b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo				-- Tabla CRED RN 1
left join #num_cred_RN2	b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo				-- Tabla CRED RN 2
left join #num_cred_RN3 b3	on a.seguro=b3.seguro and a.num_doc=b3.num_doc and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo				-- Tabla CRED RN 3 
left join #num_cred_RN4 b4	on a.seguro=b4.seguro and a.num_doc=b4.num_doc and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo				-- Tabla CRED RN 4
group by a.tipo_doc, a.num_doc, a.fecha_nac, a.ubigeo, a.seguro


--==========================================
--		TAMIZAJE NEONATAL
--==========================================

/* 2.8 - Cuentan con tamizaje neonatal de Hipotiroidismo, Hiperplasia Suprarrenal Cong�nita, Fenilcetonuria y Fibrosis Qu�stica
		, registrado con c�digo: 36416 a partir del 2do d�a (48 horas) hasta 6 d�as de nacido.
*/
if Object_id(N'tempdb..#num_Tamizaje_neo',N'U') is not null drop table #num_Tamizaje_neo;
select distinct	a.tipo_doc, 
				a.num_doc, 
				a.fecha_nac, 
				a.ubigeo, 
				a.seguro, 
				min(b.fecha_atencion) fecha_Tamizaje_neo,
				num_Tamizaje_neo=1
into #num_Tamizaje_neo
from #denominador a
inner join #his_minsa b on a.num_doc=b.num_doc 
where b.cod_item='36416'																			-- C�digos de Tamizaje Neonatal.
and ( b.fecha_atencion between dateadd(dd,2,a.fecha_nac) and dateadd(dd,6,a.fecha_nac) )			-- A partir del 2do d�a (48 horas) hasta 6 d�as de nacido.
group by a.tipo_doc, a.num_doc
		, a.fecha_nac, a.ubigeo, a.seguro
		

--%%%%%%%%%%%%%%%%
--	3.INDICADOR
--%%%%%%%%%%%%%%%% 
-- 3.1 Reporte de union del denominador y numerador. 
insert into #tabla_reporte
select	
@a�o		a�o,
@mes_inicio mes,
a.*,
b1.fecha_vac_BCG		,	isnull(b1.num_vac_BCG,0) num_vac_BCG,
b2.fecha_vac_HvB		,	isnull(b2.num_vac_HvB,0) num_vac_HvB,
iif(b1.num_vac_BCG=1 and b2.num_vac_HvB=1,1,0) num_vac_rn,
b3.fecha_cred_RN1		,	b3.num_cred_RN1,
b3.fecha_cred_RN2		,	b3.num_cred_RN2,
b3.fecha_cred_RN3		,	b3.num_cred_RN3,
b3.fecha_cred_RN4		,	b3.num_cred_RN4,
b3.num_cred_RN,
b4.fecha_Tamizaje_neo	,	isnull(b4.num_Tamizaje_neo,0)	num_Tamizaje_neo,
iif(b1.num_vac_BCG=1 and b2.num_vac_HvB=1 and b3.num_cred_RN=1 and b4.num_Tamizaje_neo=1,1,0) numerador,
denominador=1
from #denominador a
left join #Num_Vac_BCG				b1	on a.seguro=b1.seguro and a.num_doc=b1.num_doc and a.fecha_nac=b1.fecha_nac and a.ubigeo=b1.ubigeo
left join #num_vac_HvB				b2	on a.seguro=b2.seguro and a.num_doc=b2.num_doc and a.fecha_nac=b2.fecha_nac and a.ubigeo=b2.ubigeo
left join #num_cred_RN				b3	on a.seguro=b3.seguro and a.num_doc=b3.num_doc and a.fecha_nac=b3.fecha_nac and a.ubigeo=b3.ubigeo
left join #num_Tamizaje_neo			b4	on a.seguro=b4.seguro and a.num_doc=b4.num_doc and a.fecha_nac=b4.fecha_nac and a.ubigeo=b4.ubigeo

print(@mes_inicio)
print(@a�o)

set @mes_inicio=@mes_inicio+1
end

/* ************************************************************************
					REPORTE

Reporte Nominal: Se genera el reporte nominal que se guarda en SUMATIKA.
Reporte Consolidado: Se genera el reporte consolidado que se guarda en SUMATIKA
					 y que ademas se guarda como txt para subirlo en el 
					 servidor de MIDIS para Reportes. 

*** Cada reporte es un tabla nueva, por lo tanto cambiar los ultimos 6 digitos del nombre de la tabla
	por el periodo de evaluacion (Ejemplo: si es mayo entonces es _202306, si es octubre es _202312
	, si es febrero del otro a�o entonces 2024_02)
*************************************************************************** */

-- Tabla: REPORTE NOMINAL
if object_id(N'FED25.TRAMAHIS_FED2024_25_MC03_nominal', N'U') is not null drop table FED25.TRAMAHIS_FED2024_25_MC03_nominal;
select convert(char(4),a�o)+right('00'+convert(nvarchar(2),mes),2) Periodo
, b.UBIGEO ubigeo_inei
, b.GR_DIRIS diresa
,case
		When left(b.des_pro,4) = '1501' and b.des_dpo = '15 LIMA' then '15 LIMA METROPOLITANA'
		When b.des_dpo = '15 LIMA'  and left(b.des_pro,4) <>'1501' then '15 LIMA PROVINCIAS'
	Else b.des_dpo  End as departamento
, b.DES_PRO provincia
, b.DES_DIS distrito
, isnull(b.fed,0) Distritos_FED_23_24
, isnull(b.fed_limametro,0) Distritos_FED_23_24_LimaMetro 
, isnull(b.Fed_2018,0) Distritos_FED_24_25
, isnull(b.fed_IC,0) Distritos_FED_IC
, a.*
into FED25.TRAMAHIS_FED2024_25_MC03_nominal
from #tabla_reporte a
inner join MaeUbigeo_20240808 b on convert(int,a.ubigeo)=convert(int,b.ubigeo)

-- Tabla: Reporte Consolidado
select periodo, a�o, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
, sum(denominador)		denominador
, sum(numerador)		numerador
, sum(num_vac_BCG)		num_vac_BCG
, sum(num_vac_Hvb)		num_vac_Hvb
, sum(num_vac_RN)		num_vac_RN
, sum(num_cred_RN1)		num_cred_RN1
, sum(num_cred_RN2)		num_cred_RN2
, sum(num_cred_RN3)		num_cred_RN3
, sum(num_cred_RN4)		num_cred_RN4
, sum(num_cred_RN)		num_cred_RN
, sum(num_tamizaje_neo) num_tamizaje_neo
from FED25.TRAMAHIS_FED2024_25_MC03_nominal
group by periodo, a�o, mes, ubigeo_inei, diresa, departamento, provincia, distrito, flag_indicador, flag_BPN, flag_prematuro, flag_BPN_Prematuro, seguro, sexo
, Distritos_FED_23_24, Distritos_FED_23_24_LimaMetro, Distritos_FED_24_25, Distritos_FED_IC
---------------------------------------------------- =D.