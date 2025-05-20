select 
dd_nombre,
red_nombre,
microred_nombre,
ee_nombre,
renipres,
year_tp,
month_tp,
sum(numerator) as numerator,
sum(denominator) as denominator
from
(--numerador tptb
select 
dd_nombre,
red_nombre,
microred_nombre,
ee_nombre,
renipres,
month_tp,
year_tp,
count(paciente_id) as numerator,
0 as denominator
from (
select
distinct on (tt.paciente_id)
dd.nombre as dd_nombre,
ee.red_nombre,
ee.microred_nombre,
ee.nombre as ee_nombre,
ee.renipres,
extract(month from coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) ) as month_tp,
extract(year from coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) ) as year_tp,
tt.paciente_id
from taratencion_taratencion tt 
inner join paciente_paciente pp on tt.paciente_id = pp.uuid 
inner join establecimiento_establecimiento ee on ee.id = tt.eess_id
left join diresa_diresa dd on ee.diresa_id = dd.id
where 
coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) between '11/01/2023' and '10/31/2024'
and tt.fecha_fin_tp between '01/01/2024' and '12/31/2024' --periodo de evaluacion
and (case when 
	(select tc.codigo_ciex from taratencion_enfermedadtaratencion te join taratencion_comorbilidades tc on te.enfermedad_id = tc.id where te.tar_atencion_id = tt.id and tc.codigo_ciex = 'B200' limit 1) is not null then 'X' 
	else null end) isnull --no tuberculosis
and pp.tipo_condicion_actual_vih_id not in (3,4) --no fallecidos
and pp.tipo_poblacion_id <> 5 --no gestantes
) as sub_qs_num
group by 1,2,3,4,5,6,7
union
--denominador tptb
select 
dd_nombre,
red_nombre,
microred_nombre,
ee_nombre,
renipres,
month_tp,
year_tp,
0 as numerator,
count(paciente_id) as denominator
from (
select
distinct on (tt.paciente_id)
dd.nombre as dd_nombre,
ee.red_nombre,
ee.microred_nombre,
ee.nombre as ee_nombre,
ee.renipres,
extract(month from coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) ) as month_tp,
extract(year from coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) ) as year_tp,
tt.paciente_id
from taratencion_taratencion tt 
inner join paciente_paciente pp on tt.paciente_id = pp.uuid 
inner join establecimiento_establecimiento ee on ee.id = tt.eess_id
left join diresa_diresa dd on ee.diresa_id = dd.id
where 
coalesce(tt.fecha_inicio_tar, tt.fecha_esquema_tratamiento) between '11/01/2023' and '12/31/2024' --periodo de evaluacion
and (case when 
	(select tc.codigo_ciex from taratencion_enfermedadtaratencion te join taratencion_comorbilidades tc on te.enfermedad_id = tc.id where te.tar_atencion_id = tt.id and tc.codigo_ciex = 'B200' limit 1) is not null then 'X' 
	else null end) isnull --no tuberculosis
and pp.tipo_condicion_actual_vih_id not in (3,4) --no fallecidos
and pp.tipo_poblacion_id <> 5 --no gestantes
) as sub_qs_den
group by 1,2,3,4,5,6,7) as tab
where tab.year_tp = '2024'
group by 1,2,3,4,5,6,7