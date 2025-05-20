use iih_jun_2022
DECLARE @anio int
declare @anio_ant int
DECLARE @anio_ref int /*este año indica los valores para sacar los promedios  indicadores IAAS_2019-2021.pdf*/

set @anio=2024
set @anio_ref =2022
set @anio_ant = @anio-1



--DROP TABLE IF EXISTS #iih_mes_neonato_ant
IF OBJECT_ID('tempdb..#iih_mes_neonato_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_neonato_ant; 

SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dias_cvc)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(dias_cvc)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(dias_cvp)>0,CONVERT(FLOAT,sum(cvp_its))/CONVERT(FLOAT,sum(dias_cvp)),0.00)*1000,2)  as cvp_tasa,
round(iif(sum(dias_vm)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(dias_vm)),0.00)*1000,2)  as vm_tasa

into #iih_mes_neonato_ant
FROM salas_iih_mes_neonato_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4')
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_gineco_ant
IF OBJECT_ID('tempdb..#iih_mes_gineco_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_gineco_ant; 


select 
x.nombre as diresa,i.hospital as codigo,anio,y.raz_soc as hospital, y.categoria,
round(iif(sum(pv_pacientes)>0,CONVERT(FLOAT,sum(pv_endometritis))/CONVERT(FLOAT,sum(pv_pacientes)),0.00)*100,2)  as pv_tasa,
round(iif(sum(pc_pacientes)>0,CONVERT(FLOAT,sum(pc_endometritis))/CONVERT(FLOAT,sum(pc_pacientes)),0.00)*100,2)  as pc_tasa,
round(iif(sum(pc_pacientes)>0,CONVERT(FLOAT,sum(pc_iho))/CONVERT(FLOAT,sum(pc_pacientes)),0.00)*100,2)  as tasa_iho
into #iih_mes_gineco_ant
FROM salas_iih_mes_gineco_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est
where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_uci_ant
IF OBJECT_ID('tempdb..#iih_mes_uci_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(cup_expo)>0,CONVERT(FLOAT,sum(cup_its))/CONVERT(FLOAT,sum(cup_expo)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(chd_expo,0 ))>0,CONVERT(FLOAT,sum(COALESCE(chd_its,0)))/CONVERT(FLOAT,sum(COALESCE(chd_expo,0))),0.00)*1000,2)  as chd_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci_ant
FROM salas_iih_mes_uci_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_uci_pedia_ant
IF OBJECT_ID('tempdb..#iih_mes_uci_pedia_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci_pedia_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(cup_expo)>0,CONVERT(FLOAT,sum(cup_its))/CONVERT(FLOAT,sum(cup_expo)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci_pedia_ant
FROM salas_iih_mes_uci_pediatricas_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_uci_neo_ant
IF OBJECT_ID('tempdb..#iih_mes_uci_neo_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci_neo_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(COALESCE(dias_cvc,0))>0,CONVERT(FLOAT,sum(COALESCE(cvc_its,0)))/CONVERT(FLOAT,sum(COALESCE(dias_cvc,0))),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(dias_cvp)>0,CONVERT(FLOAT,sum(cvp_its))/CONVERT(FLOAT,sum(dias_cvp)),0.00)*1000,2)  as cvp_tasa,
round(iif(sum(COALESCE(dias_vm,0))>0,CONVERT(FLOAT,sum(COALESCE(vm_neumonia,0)))/CONVERT(FLOAT,sum(COALESCE(dias_vm,0))),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci_neo_ant
FROM salas_iih_ucineonatal_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_medicina_ant
IF OBJECT_ID('tempdb..#iih_mes_medicina_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_medicina_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_itu
into #iih_mes_medicina_ant
FROM salas_iih_mes_medicina_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_cirugia_ant
IF OBJECT_ID('tempdb..#iih_mes_cirugia_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_cirugia_ant;
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,


round(iif(sum(med_cup_paci)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(med_cup_paci)),0.00)*1000,2)  as tasa_itu,
round(iif(sum(COALESCE(cir_coles_paci,0))>0,CONVERT(FLOAT,sum(COALESCE(cir_coles_iho,0)))/CONVERT(FLOAT,sum(COALESCE(cir_coles_paci,0))),0.00)*100,2)  as tasa_coles,
round(iif(sum(COALESCE(cir_hernio_paci,0))>0,CONVERT(FLOAT,sum(COALESCE(cir_hernio_iho,0)))/CONVERT(FLOAT,sum(COALESCE(cir_hernio_paci,0))),0.00)*100,2)  as tasa_hernio
into #iih_mes_cirugia_ant

FROM salas_iih_mes_cirugia_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_emergencias_ant
IF OBJECT_ID('tempdb..#iih_mes_emergencias_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_emergencias_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa

into #iih_mes_emergencias_ant
FROM salas_iih_mes_emergencias_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_pediatria_ant
IF OBJECT_ID('tempdb..#iih_mes_pediatria_ant', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_pediatria_ant; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(COALESCE(cvc_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(cvc_its,0)))/CONVERT(FLOAT,sum(COALESCE(cvc_expo,0))),0.00)*1000,2)  as cvc_tasa

into #iih_mes_pediatria_ant
FROM salas_iih_pediatria_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio_ant and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria

/*-------------------------------------------------------------------------------------------------
Tablas @anio
-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_gineco
IF OBJECT_ID('tempdb..#iih_mes_gineco', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_gineco; 
select 
x.nombre as diresa,i.hospital as codigo,anio,y.raz_soc as hospital, y.categoria,
round(iif(sum(pv_pacientes)>0,CONVERT(FLOAT,sum(pv_endometritis))/CONVERT(FLOAT,sum(pv_pacientes)),0.00)*100,2)  as pv_tasa,
round(iif(sum(pc_pacientes)>0,CONVERT(FLOAT,sum(pc_endometritis))/CONVERT(FLOAT,sum(pc_pacientes)),0.00)*100,2)  as pc_tasa,
round(iif(sum(pc_pacientes)>0,CONVERT(FLOAT,sum(pc_iho))/CONVERT(FLOAT,sum(pc_pacientes)),0.00)*100,2)  as tasa_iho
into #iih_mes_gineco
FROM salas_iih_mes_gineco_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est
where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_neonato
IF OBJECT_ID('tempdb..#iih_mes_neonato', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_neonato; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dias_cvc)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(dias_cvc)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(dias_cvp)>0,CONVERT(FLOAT,sum(cvp_its))/CONVERT(FLOAT,sum(dias_cvp)),0.00)*1000,2)  as cvp_tasa,
round(iif(sum(dias_vm)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(dias_vm)),0.00)*1000,2)  as vm_tasa

into #iih_mes_neonato
FROM salas_iih_mes_neonato_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4')
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_uci
IF OBJECT_ID('tempdb..#iih_mes_uci', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(cup_expo)>0,CONVERT(FLOAT,sum(cup_its))/CONVERT(FLOAT,sum(cup_expo)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(chd_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(chd_its,0)))/CONVERT(FLOAT,sum(COALESCE(chd_expo,0))),0.00)*1000,2)  as chd_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci
FROM salas_iih_mes_uci_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_uci_pedia
IF OBJECT_ID('tempdb..#iih_mes_uci_pedia', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci_pedia; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(cup_expo)>0,CONVERT(FLOAT,sum(cup_its))/CONVERT(FLOAT,sum(cup_expo)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci_pedia
FROM salas_iih_mes_uci_pediatricas_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_uci_neo
IF OBJECT_ID('tempdb..#iih_mes_uci_neo', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_uci_neo; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,

round(iif(sum(dias_cvc)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(dias_cvc)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(dias_cvp)>0,CONVERT(FLOAT,sum(cvp_its))/CONVERT(FLOAT,sum(dias_cvp)),0.00)*1000,2)  as cvp_tasa,
round(iif(sum(dias_vm)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(dias_vm)),0.00)*1000,2)  as vm_tasa,
round(iif(sum(COALESCE(npt_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(npt_its,0)))/CONVERT(FLOAT,sum(COALESCE(npt_expo,0))),0.00)*1000,2)  as npt_tasa
into #iih_mes_uci_neo
FROM salas_iih_ucineonatal_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_medicina
IF OBJECT_ID('tempdb..#iih_mes_medicina', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_medicina; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_itu
into #iih_mes_medicina
FROM salas_iih_mes_medicina_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_cirugia
IF OBJECT_ID('tempdb..#iih_mes_cirugia', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_cirugia;
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,


round(iif(sum(med_cup_paci)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(med_cup_paci)),0.00)*1000,2)  as tasa_itu,
round(iif(sum(COALESCE(cir_coles_paci,0))>0,CONVERT(FLOAT,sum(COALESCE(cir_coles_iho,0)))/CONVERT(FLOAT,sum(COALESCE(cir_coles_paci,0))),0.00)*100,2)  as tasa_coles,
round(iif(sum(COALESCE(cir_hernio_paci,0))>0,CONVERT(FLOAT,sum(COALESCE(cir_hernio_iho,0)))/CONVERT(FLOAT,sum(COALESCE(cir_hernio_paci,0))),0.00)*100,2)  as tasa_hernio
into #iih_mes_cirugia

FROM salas_iih_mes_cirugia_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
/*and mes in (1,2,9,10,11,12)*/
group by x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/


--DROP TABLE IF EXISTS #iih_mes_emergencias
IF OBJECT_ID('tempdb..#iih_mes_emergencias', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_emergencias; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(cvc_expo)>0,CONVERT(FLOAT,sum(cvc_its))/CONVERT(FLOAT,sum(cvc_expo)),0.00)*1000,2)  as cvc_tasa,
round(iif(sum(vm_expo)>0,CONVERT(FLOAT,sum(vm_neumonia))/CONVERT(FLOAT,sum(vm_expo)),0.00)*1000,2)  as vm_tasa

into #iih_mes_emergencias
FROM salas_iih_mes_emergencias_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria
/*-------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS #iih_mes_pediatria
IF OBJECT_ID('tempdb..#iih_mes_pediatria', 'U') IS NOT NULL 
  DROP TABLE #iih_mes_pediatria; 
SELECT
x.nombre as diresa, i.hospital as codigo,
y.raz_soc as hospital, y.categoria,
anio,
round(iif(sum(dia_cup)>0,CONVERT(FLOAT,sum(med_cup_itu))/CONVERT(FLOAT,sum(dia_cup)),0.00)*1000,2)  as cup_tasa,
round(iif(sum(COALESCE(cvc_expo,0))>0,CONVERT(FLOAT,sum(COALESCE(cvc_its,0)))/CONVERT(FLOAT,sum(COALESCE(cvc_expo,0))),0.00)*1000,2)  as cvc_tasa

into #iih_mes_pediatria
FROM salas_iih_pediatria_mig_jun22_cc i
left join diresas x on i.diresa = x.codigo
left join renace y on i.hospital = y.cod_est

where anio=@anio and categoria in ('II-1', 'II-E', 'II-2', 'III-1', 'III-E', 'III-2','I-4') 
--AND i.servicio IN(select id_servicio from dbo.uci_adultos_servicio)
/*and mes in (1,2,9,10,11,12)*/
group by  x.nombre,i.hospital,y.raz_soc, anio ,y.categoria




/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

--DROP TABLE IF EXISTS trama_resultado_previo
IF OBJECT_ID('trama_resultado_previo', 'U') IS NOT NULL 
  DROP TABLE trama_resultado_previo; 



  --DROP TABLE trama_resultado_previo

select 

       renipress
      ,establecimiento
      ,categoria
      ,camas
      ,cvc_tasa_neo_ant
      ,cvp_tasa_neo_ant
      ,vm_tasa_neo_ant
      ,cvc_tasa_uci_ant
      ,cup_tasa_uci_ant
      ,vm_tasa_uci_ant
      ,chd_tasa_uci_ant
      ,npt_tasa_uci_ant
      ,cvc_tasa_uci_pedia_ant
      ,cup_tasa_uci_pedia_ant
      ,vm_tasa_uci_pedia_ant
      ,npt_tasa_uci_pedia_ant
      ,cvc_tasa_uci_neo_ant
      ,cvp_tasa_uci_neo_ant
      ,vm_tasa_uci_neo_ant
      ,npt_tasa_uci_neo_ant
      ,tasa_itu_cir_ant
      ,tasa_coles_cir_ant
      ,tasa_hernio_cir_ant
      ,cup_itu_med_ant
      ,pv_tasa_gin_ant
      ,pc_tasa_gin_ant
      ,tasa_iho_gin_ant
      ,cup_tasa_emer_ant
      ,cvc_tasa_emer_ant
      ,vm_tasa_emer_ant
      ,cup_tasa_pedia_ant
      ,cvc_tasa_pedia_ant
      ,cvc_tasa_neo_act
      ,cvp_tasa_neo_act
      ,vm_tasa_neo_act
      ,cvc_tasa_uci_act
      ,cup_tasa_uci_act
      ,vm_tasa_uci_act
      ,chd_tasa_uci_act
      ,npt_tasa_uci_act
      ,cvc_tasa_uci_pedia_act
      ,cup_tasa_uci_pedia_act
      ,vm_tasa_uci_pedia_act
      ,npt_tasa_uci_pedia_act
      ,cvc_tasa_uci_neo_act
      ,cvp_tasa_uci_neo_act
      ,vm_tasa_uci_neo_act
      ,npt_tasa_uci_neo_act
      ,tasa_itu_cir_act
      ,tasa_coles_cir_act
      ,tasa_hernio_cir_act
      ,cup_itu_med_act
      ,pv_tasa_gin_act
      ,pc_tasa_gin_act
      ,tasa_iho_gin_act
      ,cup_tasa_emer_act
      ,cvc_tasa_emer_act
      ,vm_tasa_emer_act
      ,cup_tasa_pedia_act
      ,cvc_tasa_pedia_act,




prom_neo_cvc as ref_neo_cvc, prom_neo_cvp as ref_neo_cvp, prom_neo_vm as ref_neo_vm,
prom_uci_cvc as ref_uci_cvc, prom_uci_cup as ref_uci_cup, prom_uci_vm as ref_uci_vm, prom_uci_chd as ref_uci_chd,prom_uci_npt as ref_uci_npt,

prom_uci_pedia_cvc as ref_uci_pedia_cvc, prom_uci_pedia_cup as ref_uci_pedia_cup, prom_uci_pedia_vm as ref_uci_pedia_vm,prom_uci_pedia_npt as ref_uci_pedia_npt,
prom_uci_neo_cvc as ref_uci_neo_cvc, prom_uci_neo_cvp as ref_uci_neo_cvp, prom_uci_neo_vm as ref_uci_neo_vm,prom_uci_neo_npt as ref_uci_neo_npt,

prom_cir_itu as ref_cir_itu, prom_cir_col as ref_cir_col, prom_cir_her as ref_cir_her,
prom_med_cup as ref_med_cup, prom_gin_epv as ref_gin_epv, prom_gin_epc as ref_gin_epc,
prom_gin_iho as ref_gin_iho,
prom_emer_cup as ref_emer_cup,
prom_emer_cvc  as ref_emer_cvc,
prom_emer_vm  as ref_emer_vm,
 prom_pedia_cup as ref_pedia_cup,
 prom_pedia_cvc  as ref_pedia_cvc
into trama_resultado_previo

from
(

select x.renipress, x.raz_soc as establecimiento, x.categoria, x.camas,
coalesce(z1.cvc_tasa, 0.00) as cvc_tasa_neo_ant,
coalesce(z1.cvp_tasa, 0.00) as cvp_tasa_neo_ant,
coalesce(z1.vm_tasa, 0.00) as vm_tasa_neo_ant,
coalesce(a1.cvc_tasa, 0.00) as cvc_tasa_uci_ant,
coalesce(a1.cup_tasa, 0.00) as cup_tasa_uci_ant,
coalesce(a1.vm_tasa, 0.00) as vm_tasa_uci_ant,
coalesce(a1.chd_tasa, 0.00) as chd_tasa_uci_ant,
coalesce(a1.npt_tasa, 0.00) as npt_tasa_uci_ant,

coalesce(a11.cvc_tasa, 0.00) as cvc_tasa_uci_pedia_ant,
coalesce(a11.cup_tasa, 0.00) as cup_tasa_uci_pedia_ant,
coalesce(a11.vm_tasa, 0.00) as vm_tasa_uci_pedia_ant,
coalesce(a11.npt_tasa, 0.00) as npt_tasa_uci_pedia_ant,

coalesce(a111.cvc_tasa, 0.00) as cvc_tasa_uci_neo_ant,
coalesce(a111.cvp_tasa, 0.00) as cvp_tasa_uci_neo_ant,
coalesce(a111.vm_tasa, 0.00) as vm_tasa_uci_neo_ant,
coalesce(a111.npt_tasa, 0.00) as npt_tasa_uci_neo_ant,

coalesce(y1.tasa_itu,0.00) as tasa_itu_cir_ant,
coalesce(y1.tasa_coles, 0.00) as tasa_coles_cir_ant,
coalesce(y1.tasa_hernio, 0.00) as tasa_hernio_cir_ant,
coalesce(w1.cup_itu, 0.00) as cup_itu_med_ant,
coalesce(v1.pv_tasa, 0.00) as pv_tasa_gin_ant,
coalesce(v1.pc_tasa, 0.00) as pc_tasa_gin_ant,
coalesce(v1.tasa_iho, 0.00) as tasa_iho_gin_ant,

coalesce(n3.cup_tasa, 0.00) as cup_tasa_emer_ant,
coalesce(n3.cvc_tasa, 0.00) as cvc_tasa_emer_ant,
coalesce(n3.vm_tasa, 0.00) as vm_tasa_emer_ant,

coalesce(n4.cup_tasa, 0.00) as cup_tasa_pedia_ant,
coalesce(n4.cvc_tasa, 0.00) as cvc_tasa_pedia_ant,

coalesce(z.cvc_tasa, 0.00) as cvc_tasa_neo_act,
coalesce(z.cvp_tasa, 0.00) as cvp_tasa_neo_act,
coalesce(z.vm_tasa, 0.00) as vm_tasa_neo_act,
coalesce(a.cvc_tasa, 0.00) as cvc_tasa_uci_act,
coalesce(a.cup_tasa, 0.00) as cup_tasa_uci_act,
coalesce(a.vm_tasa, 0.00) as vm_tasa_uci_act,
coalesce(a.chd_tasa, 0.00) as chd_tasa_uci_act,
coalesce(a.npt_tasa, 0.00) as npt_tasa_uci_act,

coalesce(up.cvc_tasa, 0.00) as cvc_tasa_uci_pedia_act,
coalesce(up.cup_tasa, 0.00) as cup_tasa_uci_pedia_act,
coalesce(up.vm_tasa, 0.00) as vm_tasa_uci_pedia_act,
coalesce(up.npt_tasa, 0.00) as npt_tasa_uci_pedia_act,

coalesce(un.cvc_tasa, 0.00) as cvc_tasa_uci_neo_act,
coalesce(un.cvp_tasa, 0.00) as cvp_tasa_uci_neo_act,
coalesce(un.vm_tasa, 0.00) as vm_tasa_uci_neo_act,
coalesce(un.npt_tasa, 0.00) as npt_tasa_uci_neo_act,



coalesce(y.tasa_itu,0.00) as tasa_itu_cir_act,
coalesce(y.tasa_coles, 0.00) as tasa_coles_cir_act,
coalesce(y.tasa_hernio, 0.00) as tasa_hernio_cir_act,
coalesce(w.cup_itu, 0.00) as cup_itu_med_act,
coalesce(v.pv_tasa, 0.00) as pv_tasa_gin_act,
coalesce(v.pc_tasa, 0.00) as pc_tasa_gin_act,
coalesce(v.tasa_iho, 0.00) as tasa_iho_gin_act,

coalesce(em.cup_tasa, 0.00) as cup_tasa_emer_act,
coalesce(em.cvc_tasa, 0.00) as cvc_tasa_emer_act,
coalesce(em.vm_tasa, 0.00) as vm_tasa_emer_act,

coalesce(ped.cup_tasa, 0.00) as cup_tasa_pedia_act,
coalesce(ped.cvc_tasa, 0.00) as cvc_tasa_pedia_act,

b.prom_neo_cvc, b.prom_neo_cvp, b.prom_neo_vm,
b.prom_uci_cvc, b.prom_uci_cup, b.prom_uci_vm,b.prom_uci_chd,b.prom_uci_npt,
b.prom_uci_pedia_cvc, b.prom_uci_pedia_cup, b.prom_uci_pedia_vm,b.prom_uci_pedia_npt,

b.prom_uci_neo_cvc, b.prom_uci_neo_cvp, b.prom_uci_neo_vm,b.prom_uci_neo_npt,

b.prom_cir_itu, b.prom_cir_col, b.prom_cir_her,
b.prom_med_cup, b.prom_gin_epv, b.prom_gin_epc,
b.prom_gin_iho,

b.prom_emer_cup ,
      b.prom_emer_cvc ,
      b.prom_emer_vm ,
b.prom_pedia_cup ,
      b.prom_pedia_cvc 

from
(select x.registroId as id, x.cod_est, convert(int,x.renaes) as renipress, x.raz_soc, x.subregion, x.red,
x.microred,x.notifica, x.tipo, x.nivel, x.categoria, z.camas 
from iaas_ipress y
inner join renace x on x.cod_est = y.cod_est
left join iih_camas z on z.codigo = convert(int,x.renaes)
where y.activo = '1') x
left join #iih_mes_gineco_ant v1 on x.cod_est = v1.codigo
left join #iih_mes_neonato_ant z1 on x.cod_est = z1.codigo
left join #iih_mes_uci_ant a1 on x.cod_est = a1.codigo
left join #iih_mes_uci_pedia_ant a11 on x.cod_est = a11.codigo
left join #iih_mes_uci_neo_ant a111 on x.cod_est = a111.codigo
left join #iih_mes_medicina_ant w1 on x.cod_est = w1.codigo
left join #iih_mes_cirugia_ant y1 on x.cod_est = y1.codigo
left join #iih_mes_emergencias_ant n3 on x.cod_est = n3.codigo
left join #iih_mes_pediatria_ant n4 on x.cod_est = n4.codigo
left join #iih_mes_gineco v on x.cod_est = v.codigo
left join #iih_mes_neonato z on x.cod_est = z.codigo
left join #iih_mes_uci a on x.cod_est = a.codigo
left join #iih_mes_uci_pedia up on x.cod_est = up.codigo
left join #iih_mes_uci_neo un on x.cod_est = un.codigo
left join #iih_mes_medicina w on x.cod_est = w.codigo
left join #iih_mes_cirugia y on x.cod_est = y.codigo
left join #iih_mes_emergencias em on x.cod_est = em.codigo
left join #iih_mes_pediatria ped on x.cod_est = ped.codigo
left join promedios_iih b on x.categoria = b.categoria
where x.cod_est like '%A%' and
b.anio = @anio_ref

)a

where (
cvc_tasa_neo_act+cvp_tasa_neo_act+vm_tasa_neo_act+cvc_tasa_uci_act+cup_tasa_uci_act+
vm_tasa_uci_act+chd_tasa_uci_act+npt_tasa_uci_act+
cvc_tasa_uci_pedia_act+cup_tasa_uci_pedia_act+vm_tasa_uci_pedia_act+npt_tasa_uci_pedia_act+
cvc_tasa_uci_neo_act+cvp_tasa_uci_neo_act+vm_tasa_uci_neo_act+npt_tasa_uci_neo_act+
tasa_itu_cir_act+tasa_coles_cir_act+tasa_hernio_cir_act+cup_itu_med_act+
pv_tasa_gin_act+pc_tasa_gin_act+tasa_iho_gin_act+
cup_tasa_emer_act+cvc_tasa_emer_act+vm_tasa_emer_act+cup_tasa_pedia_act+cvc_tasa_pedia_act) != 0.00 
and
(cvc_tasa_neo_ant+cvp_tasa_neo_ant+vm_tasa_neo_ant+cvc_tasa_uci_ant+cup_tasa_uci_ant+
vm_tasa_uci_ant+chd_tasa_uci_ant+npt_tasa_uci_ant+
cvc_tasa_uci_pedia_ant+cup_tasa_uci_pedia_ant+vm_tasa_uci_pedia_ant+npt_tasa_uci_pedia_ant+
cvc_tasa_uci_neo_ant+cvp_tasa_uci_neo_ant+vm_tasa_uci_neo_ant+npt_tasa_uci_neo_ant+
tasa_itu_cir_ant+tasa_coles_cir_ant+tasa_hernio_cir_ant+cup_itu_med_ant+
pv_tasa_gin_ant+pc_tasa_gin_ant+tasa_iho_gin_ant+
cup_tasa_emer_ant+cvc_tasa_emer_ant+vm_tasa_emer_ant+cup_tasa_pedia_ant+cvc_tasa_pedia_ant) != 0.00
order by renipress


--DROP TABLE IF EXISTS trama_resultado_previo_final
IF OBJECT_ID('trama_resultado_previo_final', 'U') IS NOT NULL 
  DROP TABLE trama_resultado_previo_final; 



  select 
convert(varchar(20),renipress)as renipress	,establecimiento	,categoria	,camas	,

      cvc_tasa_neo_ant
      ,cvp_tasa_neo_ant
      ,vm_tasa_neo_ant
      ,cvc_tasa_uci_ant
      ,cup_tasa_uci_ant
      ,vm_tasa_uci_ant
      ,chd_tasa_uci_ant
      ,npt_tasa_uci_ant
      ,cvc_tasa_uci_pedia_ant
      ,cup_tasa_uci_pedia_ant
      ,vm_tasa_uci_pedia_ant
      ,npt_tasa_uci_pedia_ant
      ,cvc_tasa_uci_neo_ant
      ,cvp_tasa_uci_neo_ant
      ,vm_tasa_uci_neo_ant
      ,npt_tasa_uci_neo_ant
      ,tasa_itu_cir_ant
      ,tasa_coles_cir_ant
      ,tasa_hernio_cir_ant
      ,cup_itu_med_ant
      ,pv_tasa_gin_ant
      ,pc_tasa_gin_ant
      ,tasa_iho_gin_ant
      ,cup_tasa_emer_ant
      ,cvc_tasa_emer_ant
      ,vm_tasa_emer_ant
      ,cup_tasa_pedia_ant
      ,cvc_tasa_pedia_ant
      ,cvc_tasa_neo_act
      ,cvp_tasa_neo_act
      ,vm_tasa_neo_act
      ,cvc_tasa_uci_act
      ,cup_tasa_uci_act
      ,vm_tasa_uci_act
      ,chd_tasa_uci_act
      ,npt_tasa_uci_act
      ,cvc_tasa_uci_pedia_act
      ,cup_tasa_uci_pedia_act
      ,vm_tasa_uci_pedia_act
      ,npt_tasa_uci_pedia_act
      ,cvc_tasa_uci_neo_act
      ,cvp_tasa_uci_neo_act
      ,vm_tasa_uci_neo_act
      ,npt_tasa_uci_neo_act
      ,tasa_itu_cir_act
      ,tasa_coles_cir_act
      ,tasa_hernio_cir_act
      ,cup_itu_med_act
      ,pv_tasa_gin_act
      ,pc_tasa_gin_act
      ,tasa_iho_gin_act
      ,cup_tasa_emer_act
      ,cvc_tasa_emer_act
      ,vm_tasa_emer_act
      ,cup_tasa_pedia_act
      ,cvc_tasa_pedia_act
      ,ref_neo_cvc
      ,ref_neo_cvp
      ,ref_neo_vm
      ,ref_uci_cvc
      ,ref_uci_cup
      ,ref_uci_vm
      ,ref_uci_chd
      ,ref_uci_npt
      ,ref_uci_pedia_cvc
      ,ref_uci_pedia_cup
      ,ref_uci_pedia_vm
      ,ref_uci_pedia_npt
      ,ref_uci_neo_cvc
      ,ref_uci_neo_cvp
      ,ref_uci_neo_vm
      ,ref_uci_neo_npt
      ,ref_cir_itu
      ,ref_cir_col
      ,ref_cir_her
      ,ref_med_cup
      ,ref_gin_epv
      ,ref_gin_epc
      ,ref_gin_iho
      ,ref_emer_cup
      ,ref_emer_cvc
      ,ref_emer_vm
      ,ref_pedia_cup
      ,ref_pedia_cvc,



iif(umbral1=0,null, tasa1_desc)as tasa1_desc,iif(umbral1=0,null, umbral1)as umbral1	,iif(umbral2=0,null, tasa2_desc)as tasa2_desc,iif(umbral2=0,null, umbral2)as umbral2	,
iif(logro_esperado1=0,null, logro_esperado1)as logro_esperado1,iif(logro_esperado2=0,null, logro_esperado2)as logro_esperado2,
logro_alcanzado1,	logro_alcanzado2,
iif(logro_esperado1-umbral1=0,null,((logro_alcanzado1-umbral1)/(logro_esperado1-umbral1))*100) as cumplimiento1,
iif(logro_esperado2-umbral2=0,null, ((logro_alcanzado2-umbral2)/(logro_esperado2-umbral2))*100) as cumplimiento2

into trama_resultado_previo_final
from
(
SELECT *,
dbo.selecciona_tasa_desc(renipress,0) tasa1_desc,dbo.selecciona_tasa(renipress,0)umbral1,dbo.selecciona_tasa_desc(renipress,1)tasa2_desc,
dbo.selecciona_tasa(renipress,1)umbral2,dbo.logro_esperado(renipress,0)logro_esperado1,dbo.logro_esperado(renipress,1)logro_esperado2,
logro_alcanzado1 =  
      CASE dbo.selecciona_tasa_desc(renipress,0)
        WHEN 'cvc_tasa_neo_ant' THEN cvc_tasa_neo_act
         WHEN 'cvp_tasa_neo_ant' THEN cvp_tasa_neo_act
         WHEN 'vm_tasa_neo_ant' THEN vm_tasa_neo_act
         WHEN 'cvc_tasa_uci_ant' THEN cvc_tasa_uci_act
		 WHEN 'cup_tasa_uci_ant' THEN cup_tasa_uci_act
		 WHEN 'vm_tasa_uci_ant' THEN vm_tasa_uci_act
		 WHEN 'chd_tasa_uci_ant' THEN chd_tasa_uci_act
		 WHEN 'npt_tasa_uci_ant' THEN npt_tasa_uci_act
		 WHEN 'cvc_tasa_uci_pedia_ant' THEN cvc_tasa_uci_pedia_act
		 WHEN 'cup_tasa_uci_pedia_ant' THEN cup_tasa_uci_pedia_act
		 WHEN 'vm_tasa_uci_pedia_ant' THEN vm_tasa_uci_pedia_act
		 WHEN 'npt_tasa_uci_pedia_ant' THEN npt_tasa_uci_pedia_act
		 WHEN 'cvc_tasa_uci_neo_ant' THEN cvc_tasa_uci_neo_act
		 WHEN 'cvp_tasa_uci_neo_ant' THEN cvp_tasa_uci_neo_act
		 WHEN 'vm_tasa_uci_neo_ant' THEN vm_tasa_uci_neo_act
		 WHEN 'npt_tasa_uci_neo_ant' THEN npt_tasa_uci_neo_act
		 WHEN 'tasa_itu_cir_ant' THEN tasa_itu_cir_act
		 WHEN 'tasa_coles_cir_ant' THEN tasa_coles_cir_act
		 WHEN 'tasa_hernio_cir_ant' THEN tasa_hernio_cir_act
		 WHEN 'cup_itu_med_ant' THEN cup_itu_med_act
		 WHEN 'pv_tasa_gin_ant' THEN pv_tasa_gin_act
		 WHEN 'pc_tasa_gin_ant' THEN pc_tasa_gin_act
		 WHEN 'tasa_iho_gin_ant' THEN tasa_iho_gin_act
		 WHEN 'cup_tasa_emer_ant' THEN cup_tasa_emer_act
		 WHEN 'cvc_tasa_emer_ant' THEN cvc_tasa_emer_act
		 WHEN 'vm_tasa_emer_ant' THEN vm_tasa_emer_act
		 WHEN 'cup_tasa_pedia_ant' THEN cup_tasa_pedia_act
		 WHEN 'cvc_tasa_pedia_ant' THEN cvc_tasa_pedia_act
         ELSE null
      END,
logro_alcanzado2 =  
      CASE dbo.selecciona_tasa_desc(renipress,1)
       WHEN 'cvc_tasa_neo_ant' THEN cvc_tasa_neo_act
         WHEN 'cvp_tasa_neo_ant' THEN cvp_tasa_neo_act
         WHEN 'vm_tasa_neo_ant' THEN vm_tasa_neo_act
         WHEN 'cvc_tasa_uci_ant' THEN cvc_tasa_uci_act
		 WHEN 'cup_tasa_uci_ant' THEN cup_tasa_uci_act
		 WHEN 'vm_tasa_uci_ant' THEN vm_tasa_uci_act
		 WHEN 'chd_tasa_uci_ant' THEN chd_tasa_uci_act
		 WHEN 'npt_tasa_uci_ant' THEN npt_tasa_uci_act
		 WHEN 'cvc_tasa_uci_pedia_ant' THEN cvc_tasa_uci_pedia_act
		 WHEN 'cup_tasa_uci_pedia_ant' THEN cup_tasa_uci_pedia_act
		 WHEN 'vm_tasa_uci_pedia_ant' THEN vm_tasa_uci_pedia_act
		 WHEN 'npt_tasa_uci_pedia_ant' THEN npt_tasa_uci_pedia_act
		 WHEN 'cvc_tasa_uci_neo_ant' THEN cvc_tasa_uci_neo_act
		 WHEN 'cvp_tasa_uci_neo_ant' THEN cvp_tasa_uci_neo_act
		 WHEN 'vm_tasa_uci_neo_ant' THEN vm_tasa_uci_neo_act
		 WHEN 'npt_tasa_uci_neo_ant' THEN npt_tasa_uci_neo_act
		 WHEN 'tasa_itu_cir_ant' THEN tasa_itu_cir_act
		 WHEN 'tasa_coles_cir_ant' THEN tasa_coles_cir_act
		 WHEN 'tasa_hernio_cir_ant' THEN tasa_hernio_cir_act
		 WHEN 'cup_itu_med_ant' THEN cup_itu_med_act
		 WHEN 'pv_tasa_gin_ant' THEN pv_tasa_gin_act
		 WHEN 'pc_tasa_gin_ant' THEN pc_tasa_gin_act
		 WHEN 'tasa_iho_gin_ant' THEN tasa_iho_gin_act
		 WHEN 'cup_tasa_emer_ant' THEN cup_tasa_emer_act
		 WHEN 'cvc_tasa_emer_ant' THEN cvc_tasa_emer_act
		 WHEN 'vm_tasa_emer_ant' THEN vm_tasa_emer_act
		 WHEN 'cup_tasa_pedia_ant' THEN cup_tasa_pedia_act
		 WHEN 'cvc_tasa_pedia_ant' THEN cvc_tasa_pedia_act
         ELSE null
      END
from trama_resultado_previo  
)x order by renipress


select *,
cumplimiento_final =  
      CASE   
         WHEN round((isnull(cumplimiento1,cumplimiento2)+isnull(cumplimiento2,cumplimiento1))/2,2) >=100 THEN 100
		 WHEN round((isnull(cumplimiento1,cumplimiento2)+isnull(cumplimiento2,cumplimiento1))/2,2) <=0 THEN 0
         ELSE round((isnull(cumplimiento1,cumplimiento2)+isnull(cumplimiento2,cumplimiento1))/2,2)
      END




from trama_resultado_previo_final order by renipress


