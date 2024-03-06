# %%
import pandas as pd
import numpy as np
import os
import glob as gb
import sys
import datetime
sys.path.insert(0, "../../")
from clases.bd.conexion2 import MyDatabase2
conn = MyDatabase2()
anio=2024

pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# %%
def consulta_adolescentes(conn,anio):
    adolescentes=conn.df(f"""select t.provincia,t.distrito,t.red,t.microred,t.nombre_eess ,t.id_establecimiento, t.numero_documento,t.edad,t.mes,t.periodo,t.condicio,t.fec_1era_ate,t.id_personal, t.id_registrador from(
select
    mp.numero_documento ,
	nt.id_establecimiento,
	concat(nt.mes,	' - ',	nt.anio) as periodo,
    nt.mes,
	nt.fecha_atencion as fec_1era_ate,
	nt.anio_actual_paciente edad,
	nt.id_condicion_establecimiento condicio,
	egc.provincia ,
	egc.distrito ,
	egc.red ,
	egc.microred ,
	egc.nombre_eess,
	nt.id_personal,
	nt.id_registrador,
	row_number() over (partition by mp.numero_documento,nt.id_establecimiento order by nt.anio,nt.fecha_atencion) as rn
from
	maestros.nominal_trama nt
inner join maestros.maestro_paciente mp on	mp.id_paciente = nt.id_paciente
inner join maestros.eess_geresa_cusco egc on egc.id_eess =nt.id_establecimiento 
where
	nt.anio = {anio}
	AND (EXTRACT(YEAR FROM nt.fecha_atencion) - EXTRACT(YEAR FROM mp.fecha_nacimiento)) between 12 and 17
	and nt.id_condicion_establecimiento in('N', 'R')
	and mp.id_tipo_documento = 1
    and mp.genero ='F'
    and egc.cod_ue !=0
    and egc.id_eess not in('35937','35938','36087','36090','36147','36834','39165','39185','39188')
    AND egc.cat IN ('I-1','I-2','I-3','I-4')   
	)t where t.rn=1 and t.mes in(1) """)
   
    return adolescentes

def consulta_hemoglobina(conn,anio):
    hemoglobina=conn.df(f"""select t.numero_documento,t.fecha_atencion as fecha_hb,t.id_establecimiento from(
select
	mp.numero_documento,
	nt.id_establecimiento,
	nt.fecha_atencion,
	nt.codigo_item ,
	row_number() over (partition by mp.numero_documento,nt.id_establecimiento order by nt.anio,nt.fecha_atencion) as rn
from
	maestros.nominal_trama nt
inner join maestros.maestro_paciente mp on
	mp.id_paciente = nt.id_paciente
where
	nt.anio = {anio}
	AND (EXTRACT(YEAR FROM nt.fecha_atencion) - EXTRACT(YEAR FROM mp.fecha_nacimiento)) between 12 and 17
	and mp.id_tipo_documento = 1
	and nt.codigo_item in ('85018','85018.01') and nt.tipo_diagnostico='D'	
    and mp.genero ='F'
	)t where t.rn=1
 """)
    return hemoglobina
    
def consulta_gestante(conn,anio):
    gestante=conn.df(f"""
select t.numero_documento  
from(
select
	mp.numero_documento,
	nt.id_establecimiento,
	nt.fecha_atencion,
	nt.codigo_item ,
	row_number() over (partition by mp.numero_documento order by nt.anio,nt.fecha_atencion) as rn
from
	maestros.nominal_trama nt
inner join maestros.maestro_paciente mp on
	mp.id_paciente = nt.id_paciente
where
	nt.anio = {anio}
    AND (EXTRACT(YEAR FROM nt.fecha_atencion) - EXTRACT(YEAR FROM mp.fecha_nacimiento)) between 12 and 17
	and mp.id_tipo_documento = 1
	and nt.codigo_item in('Z3491','Z3492','Z3493','Z3591','Z3592','Z3593','Z359','Z349','90749.01','90749.02')
	)t where t.rn=1 """)
    return gestante
    
def consulta_personal(conn):
	personal =conn.df(f"""select id_personal, concat(nombres_personal,' ',apellido_materno_personal,' ',apellido_materno_personal) as personal  from maestros.maestro_personal mp 
                       """)
	return personal

def consulta_registrador(conn):
	registrador =conn.df(f"""select id_registrador, concat(nombres_registrador,' ',apellido_materno_registrador,' ', apellido_materno_registrador) as registrador  from maestros.maestro_registrador mr  """)
	return registrador



# %%
adolescentes = consulta_adolescentes(conn,anio)
hemoglobina=consulta_hemoglobina(conn,anio)
gestante=consulta_gestante(conn,anio)
personal =consulta_personal(conn)
registrador =consulta_registrador(conn)

# %%
# Eliminar gestantes
df_adolescentes = adolescentes[~adolescentes['numero_documento'].isin(gestante['numero_documento'])]

# Agregar fecha a hemoglobina
adolescentes_con_hb = pd.merge(df_adolescentes, hemoglobina, on=['numero_documento', 'id_establecimiento'], how='left')

# Crear nuevas columnas 'den', 'nun' de manera eficiente
adolescentes_con_hb['den'] = 1
adolescentes_con_hb['nun'] = adolescentes_con_hb['fecha_hb'].notnull().astype(int)

# Rellenar NaN en 'fecha_hb' con '-'
adolescentes_con_hb['fecha_hb'] = adolescentes_con_hb['fecha_hb'].fillna('-')


#====================================================================================
adolescentes_con_hb  = pd.merge(adolescentes_con_hb, personal, on=['id_personal'], how='left')
adolescentes_con_hb  = pd.merge(adolescentes_con_hb, registrador, on=['id_registrador'], how='left')
#==================================================================================
conn.insert_df(adolescentes_con_hb,'fed_adolescentes','indicadores')

#adolescentes_con_hb.to_excel('resultado_adolescentes_con_hb.xlsx', index=False)





