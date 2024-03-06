# %%
from decouple import config
import pandas as pd
import numpy as np
import os
import glob as gb
import sys
import datetime
from decimal import Decimal
sys.path.insert(0, config('PROYECTO_DIR'))
from clases.bd.conexion2 import MyDatabase2
conn = MyDatabase2()

anio=config('SYS_ANIO')

#funciones 


# %%
def consulta_sql(conn,st):
    q=''
    if st==1:
        q='AND nt.hemoglobina IS NOT NULL' #tiene hemoglobina
    else :
        q='' #no tiene hemoglobina 

    menores = conn.df(f""" SELECT * FROM (   
    SELECT  nt.id_paciente ,
nt.anio ,
nt.mes ,
nt.id_establecimiento,
mhe.nombre_eess  ,
nt.anio_actual_paciente as edad_anio,
ROUND((nt.fecha_atencion - mp.fecha_nacimiento)/30.44,1) AS edad_mes,
(nt.fecha_atencion - mp.fecha_nacimiento) AS edad_dia,
nt.fecha_atencion,
nt.lote,
nt.num_pag,
nt.hemoglobina,
nt.fecha_resultado_hb,
nt.id_centro_poblado ,
mhcp.descripcion_centro_poblado,
mhcp.altitud_centro_poblado  ,
nt.codigo_item,
mhtd.abrev_tipo_doc,
mp.numero_documento,
mp.fecha_nacimiento,
mp.genero,
mhe.cod_red  ,
mhe.red ,
mhe.cod_mred ,
mhe.microred,
mhe.provincia,
mhe.distrito,
mhe.cod_eess,
nt.id_pais,
concat(mp2.numero_documento,' - ', mp2.nombres_personal,' ', mp2.apellido_paterno_personal,' ', mp2.apellido_materno_personal ) AS personal  ,
concat(mr.numero_documento,' - ',mr.nombres_registrador,' ',mr.apellido_paterno_registrador,' ',mr.apellido_materno_registrador) AS registrador,
ROW_NUMBER() OVER (PARTITION BY mhtd.abrev_tipo_doc, mp.numero_documento ORDER BY nt.fecha_atencion DESC) AS rn
FROM maestros.nominal_trama nt 
INNER JOIN maestros.maestro_paciente mp ON mp.id_paciente =nt.id_paciente 
LEFT JOIN maestros.eess_geresa_cusco mhe ON mhe.id_eess =nt.id_establecimiento 
LEFT JOIN maestros.maestro_his_tipo_doc mhtd ON mhtd.id_tipo_documento =mp.id_tipo_documento 
LEFT JOIN maestros.maestro_personal mp2 ON mp2.id_personal=nt.id_personal  
LEFT JOIN maestros.maestro_registrador mr ON mr.id_registrador =nt.id_registrador 
LEFT JOIN maestros.maestro_his_centro_poblado mhcp ON nt.id_centro_poblado = mhcp.id_centro_poblado 
WHERE nt.anio_actual_paciente<5 AND nt.codigo_item IN('85018','85018.01') 
--AND nt.mes in(1) 
 and nt.anio ={anio}
 AND mp.fecha_nacimiento IS  NOT NULL  %s
AND mhe.cat NOT IN('III-1')
 )AS t   WHERE t.rn=1;"""% q)
    return menores
# menores.head(2)


# %%
#consulta a la base de datos altitud de establecimiento 
def consulta_altitud_eess(conn):
    altitud=conn.df("SELECT cod_unico,altitud as altitud_eess FROM renaes_altitud_eess")
    altitud = altitud.rename(columns={'cod_unico': 'cod_eess'})
    return altitud
#consulta a la base de datos nombre y apellido de los menores 
def consulta_covid(cpaciente):
    nombres=cpaciente.df("SELECT * FROM paciente()")
    nombres = nombres.rename(columns={'num_doc': 'numero_documento'})
    return nombres
#consulta a la base de datos sobre las observaciones
def consulta_observaciones(conn):
    obs=conn.df("SELECT * FROM observaciones_his oh")   
    obs = obs.rename(columns={'id': 'id_obs','descricion': 'diagnostico'})
    return   obs

# %% [markdown]
# <center><h2>Estructura del Reporte </h2></center>

# %%
def estructura2_df(menores, altitud):
    menores['fecha_atencion'] = pd.to_datetime(
        menores['fecha_atencion'], format='%Y-%m-%d')
    # menor = menores[['id_paciente','anio','mes','id_establecimiento','nombre_establecimiento','anio_actual_paciente','mes_actual_paciente','dia_actual_paciente','fecha_atencion','abrev_tipo_doc','numero_documento','fecha_nacimiento','genero','codigo_red','red','codigo_microred','microred','personal','registrador','codigo_unico','provincia','distrito','id_pais']]
    menor = menores
    menor = menor.drop(['rn'], axis=1)
    # menor=menor[menor.id_paciente.isin(['100012242536','100001212420'])]
    # ordenar de ac
    menor = menor.sort_values(by=['fecha_atencion'], inplace=False)
    menor = menor[(menor['edad_mes'] >= 6)&(menor['edad_mes'] < 60)]
    # convertir a numero
    #menor['codigo_unico'] = menor['codigo_unico'].astype(int)

    menor = menor.merge(altitud, how='inner', on=['cod_eess'])
# Reemplazar por = 0 los registros enblanco
    menor['hemoglobina'] = menor['hemoglobina'].fillna(0)
    menor['altitud_centro_poblado'] = menor['altitud_centro_poblado'].fillna(0)

    # INDEXAR
    menor = menor.set_index('id_paciente')
    # extraer el valosr maximo
    menor = menor.groupby(menor.index).tail(1)
    menor=menor.reset_index() 
    return menor


# %%


# %% [markdown]
# <center><h2>Funciones Para el Calculo</h2></center>

# %%
def fun_registros_obs(row):
    obs = 0
    HemObs = row['hemoglobina']
    altitud = row['altitud_centro_poblado']

    if altitud == 0:
        obs = 6
    if HemObs < 4.0 or HemObs > 18.5:
        obs = 2
    if HemObs == 0:
        obs = 1
    return obs


def fun_Hemoglobina(row):
    HemObs = row['hemoglobina']
    altitud = row['altitud_eess']

    Hemoglobina = HemObs-(altitud*3.3/1000)*(-0.032+(0.022*altitud*3.3/1000))
    return round(Hemoglobina, 2)


def fun_diagnostico(row, obs):
    dx = ''
    Hem_Ajustada = row['hemo_ajustada']
    meses = row['edad_mes']
    id_obs = row['id_obs']
    if id_obs == 0 or id_obs == 6:
        if meses < 2:
            if Hem_Ajustada < 13.5:
                dx = 'Anemia'
            if Hem_Ajustada >= 13.5 and Hem_Ajustada <= 18.5:
                dx = 'Normal'
        if meses >= 2 and meses < 6:
            if Hem_Ajustada < 9.5:
                dx = 'Anemia'
            if Hem_Ajustada >= 9.5 and Hem_Ajustada <= 13.5:
                dx = 'Normal'
        if meses >= 6 and meses < 60:
            if Hem_Ajustada < 7:
                dx = 'Anemia Severa'
            if Hem_Ajustada >= 7 and Hem_Ajustada < 10:
                dx = 'Anemia Moderada'
            if Hem_Ajustada >= 10 and Hem_Ajustada < 11:
                dx = 'Anemia Leve'
            if Hem_Ajustada >= 11:
                dx = 'Normal'
    # else:
        # dx = obs.loc[obs['id_obs'] == id_obs, 'diagnostico'].values[0]
    return dx

def actualizado_nombres(menor, covid):
    menor = menor.reset_index()
   # menor.head(1)

    menor = menor.merge(covid, how='left', on=[
                        'abrev_tipo_doc', 'numero_documento'])
    return menor


# %%
menores = consulta_sql(conn, 0) #0= observaciones ! 1= rpt de hemoglobina 

obs = consulta_observaciones(conn)
#pacientes_covid = consulta_covid(cpaciente)
altitud = consulta_altitud_eess(conn)
Estructura = estructura2_df(menores, altitud)


# crear las columnas
Estructura['hemo_ajustada'] = Estructura.apply(fun_Hemoglobina, axis=1)
Estructura['id_obs'] = Estructura.apply(fun_registros_obs, axis=1)
Estructura['diagnostico'] = Estructura.apply(
    lambda row: fun_diagnostico(row, obs), axis=1)
#Estructura.to_excel(f"anemia-ojo.xlsx", index=False)
#Estructura.head(2)


# %%
def fun_df_observados(Estructura):
  df_observados=Estructura[Estructura['id_obs']>0]
  df_observados = df_observados.drop(['diagnostico'], axis=1)
 # df_observados.loc[df_observados['id_obs'] == 6, 'diagnostico'] = 'ERROR: CENTRO POBLADO EN BLANCO'  
  df_observados=df_observados.merge(obs, how='inner', on=['id_obs'])
  #df_observados.to_excel('obserbados.xlsx', index=False)
  return df_observados

def fun_df_anemia(Estructura):
  anemia=Estructura[(Estructura['id_obs']==0)|(Estructura['id_obs']==6)]
  anemia['edad_mes']=anemia['edad_mes'].astype(int) 
  #anemia.to_excel('anemia_22.xlsx', index=False)
  return anemia


# %%
observados=fun_df_observados(Estructura)
anemia=fun_df_anemia(Estructura)

# %%
observados=fun_df_observados(Estructura)
#conn = MyDatabase2()
conn.sql('delete from public.excluidos_5_his;')
d=conn.sqli(observados,'excluidos_5_his')
#conn.close()


anemia=fun_df_anemia(Estructura)
#conn = MyDatabase2()
#conn.sql('delete from public.anemia_nenores_5;')
#d=conn.sqli(anemia,'anemia_nenores_5')
#conn.close()
#print(d)

# %%


# Obtener la fecha actual
fecha_actual = datetime.datetime.now()
nombre_mes = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}

# Obtener el nombre abreviado del mes en español
nombre_mes_abreviado = nombre_mes[fecha_actual.month]

# Formatear la fecha con el nombre abreviado del mes en español
fecha_formateada = fecha_actual.strftime(f"{nombre_mes_abreviado}-%d-%Y")

anemia.to_excel(f"excel/Anemia-menor-5_{fecha_formateada}.xlsx", index=False)

conn.close()



