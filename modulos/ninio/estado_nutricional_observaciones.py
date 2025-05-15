# %%
from decouple import config
import pandas as pd
import numpy as np
import os
import glob as gb
import sys
from decimal import Decimal
import datetime


sys.path.insert(0, config('PROYECTO_DIR'))
from clases.bd.conexion2 import MyDatabase2
conn = MyDatabase2()

anio=config('SYS_ANIO')


# %%

def consulta_sql(conn):

    menores = conn.df(f"""
SELECT
	*
FROM
	(
	SELECT
		DISTINCT nt.id_paciente ,
		nt.anio ,
		nt.mes ,
		nt.id_establecimiento,
		mhe.nombre_eess ,
		nt.anio_actual_paciente AS edad_anio,
		ROUND((nt.fecha_atencion - mp.fecha_nacimiento)/ 30.44,
		1) AS edad_mes,
		(nt.fecha_atencion - mp.fecha_nacimiento) AS edad_dia,
		nt.fecha_atencion,
		nt.peso,
		ROUND(nt.talla,
		2) AS talla,
		nt.lote,
		nt.num_pag,
		nt.codigo_item,
		mhtd.abrev_tipo_doc,
		mp.numero_documento,
		mp.fecha_nacimiento,
		mp.genero,
		mhe.cod_red ,
		mhe.red ,
		mhe.cod_mred ,
		mhe.microred,
		mhe.provincia,
		mhe.distrito,
		mhe.cod_eess ,
		nt.id_pais,
		concat(mp2.numero_documento,
		' - ',
		mp2.nombres_personal,
		' ',
		mp2.apellido_paterno_personal,
		' ',
		mp2.apellido_materno_personal ) AS personal ,
		concat(mr.numero_documento,
		' - ',
		mr.nombres_registrador,
		' ',
		mr.apellido_paterno_registrador,
		' ',
		mr.apellido_materno_registrador) AS registrador ,
		ROW_NUMBER() OVER (PARTITION BY mhtd.abrev_tipo_doc,
		mp.numero_documento
	ORDER BY
		nt.fecha_atencion DESC) AS rn
	FROM
		maestros.nominaltrama2024 nt
	INNER JOIN maestros.nominal_trama nt2 ON
		nt2.id_cita = nt.id_cita
	INNER JOIN maestros.maestro_paciente mp ON
		mp.id_paciente = nt.id_paciente
	LEFT JOIN maestros.eess_geresa_cusco mhe ON
		mhe.id_eess = nt.id_establecimiento
	LEFT JOIN maestros.maestro_his_tipo_doc mhtd ON
		mhtd.id_tipo_documento = mp.id_tipo_documento
	LEFT JOIN maestros.maestro_personal mp2 ON
		mp2.id_personal = nt.id_personal
	LEFT JOIN maestros.maestro_registrador mr ON
		mr.id_registrador = nt.id_registrador
	WHERE
		nt.anio_actual_paciente<5
		AND nt.anio = {anio}
		--AND nt.mes in(1,2,4,5,6,7,8,9,10,11) 
	AND mp.fecha_nacimiento IS NOT NULL
		AND mhe.cat NOT IN('III-1')-- hospital
		AND mhe.id_eess NOT IN('35937', '35938', '36087', '36090', '36147', '36834', '39165', '39185', '39188')	--salud mental	
		AND nt2.codigo_item NOT in('Z001','99381','99381.01','99382','99383','C0011','99499.08','99499.09','99499.10','99499.01') -- codigos excluidos del 2do dx
		AND nt.codigo_item in('Z001','99381','99381.01','99382','99383')--registros evaluados para las observaciones 
)AS t
WHERE
	t.rn = 1 
""")
    return menores

#menores=consulta_sql(conn)
#consulta a la base de datos nombre y apellido de los menores 
def consulta_covid(cpaciente):
    nombres=cpaciente.df("SELECT * FROM maestros.paciente_covid()")
    nombres = nombres.rename(columns={'num_doc': 'numero_documento'})
    return nombres
#consulta a la base de datos sobre las observacionesÑ
def consulta_observaciones(conn):
    obs=conn.df("SELECT * FROM observaciones_his oh")   
    obs = obs.rename(columns={'id': 'id_obs','descricion': 'diagnostico'})
    return   obs

#consulta a la base de datos altitud de establecimiento 
def consulta_peso_edad(conn):
    peso_edad=conn.df("select * from oms_peso_edad ope")  
    return peso_edad

def consulta_talla_edad(conn):
    talla_edad=conn.df("select * from oms_talla_edad ote ")  
    return talla_edad

def consulta_peso_talla(conn):
    peso_talla=conn.df("select * from oms_peso_talla opt ")  
    return peso_talla

# %%
#menores=consulta_sql(conn)
def estructura_df(menores):
    
    #menores = menores[['id_paciente','edad_anio',  'edad_mes' , 'edad_dia','peso', 'talla','genero','fecha_atencion','fecha_nacimiento']]
    #menor=menor[menor.id_paciente.isin(['100012242536','100001212420'])]
    # ordenar de ac
    menores = menores.drop(['rn'], axis=1)
    menores = menores.sort_values(by=['fecha_atencion'], inplace=False)
    menores['fecha_atencion'] = pd.to_datetime(menores['fecha_atencion'],format='%Y-%m-%d')
    menores['peso'] = menores['peso'].fillna(0).apply(float)   
    menores['talla'] = menores['talla'].fillna(0).apply(float)  
    # INDEXAR
    menores = menores.set_index('id_paciente')
    # extraer el valosr maximo
    menores = menores.groupby(menores.index).tail(1)
    #menor.head(2)
    menores=menores.reset_index()   
    
    return menores
    
#Estructura = estructura_df(menores)
#Estructura.head()
def fun_observados_1(menores,obs):
    df_observados_1=menores[(menores['peso']==0)|(menores['talla']==0)]
   # df_observados_1=df_observados_1[df_observados_1['codigo_item'].isin(['Z001','99381','99381.01','99382','99383'])]
    df_observados_1['id_obs']=7   
    df_observados_1=df_observados_1.merge(obs, how='inner', on=['id_obs'])
    return df_observados_1

def fun_estructura_df(menores):
    estructura=menores[(menores['peso']>0)|(menores['talla']>0)]
    return estructura


# %%


def fun_registros_obs(row):
    obs = 0
    dia = row['edad_dia']
    sexo = row['genero']
    talla = row['talla']
    peso = row['peso']
    p_e = row['Zp_e']
    t_e = row['Zt_e']
    p_t = row['Zp_t']
    # t=str(talla).split(".")[1][1]
    t = str(talla).split(".")

    if talla == 0 or peso == 0:
        obs = 7
    if len(t) >= 2:
        # print(len(t[1]))
        if len(t[1]) >= 2:
            obs = 8
    if p_e < -6 or p_e > 5:
        obs = 3
    if t_e < -6 or t_e > 6:
        obs = 4
    if p_t < -5 or p_t > 5:
        obs = 5
    if p_t == 100:
        obs = 9
    return obs


def ZPeso_edad(row, df_p_e):
    dia = row['edad_dia']
    sexo = row['genero']
    # talla = row['talla']
    peso = row['peso']
    df_p_e = df_p_e.loc[(df_p_e['edad_dias'] == dia) & (
        df_p_e['sexo'] == sexo), ['l', 'm', 's']]  # .values[0]
    # df_p_e.to_float()
    Z = 0
    if len(df_p_e) > 0:
        L = df_p_e['l'].values[0].astype(Decimal)
        M = df_p_e['m'].values[0].astype(Decimal)
        S = df_p_e['s'].values[0].astype(Decimal)
        if peso > 0:
            Z = (((peso/M) ** L)-1)/(L*S)
    else:
        Z = 100

    return round(Z, 2)


def ZTalla_edad(row, df_t_e):
    dia = row['edad_dia']
    sexo = row['genero']
    talla = row['talla']
    peso = row['peso']
    df_t_e = df_t_e.loc[(df_t_e['edad_dias'] == dia) & (
        df_t_e['sexo'] == sexo), ['l', 'm', 's']]  # .values[0]
    Z = 0
    # df_p_e.to_float()
    if len(df_t_e) > 0:
        L = df_t_e['l'].values[0].astype(Decimal)
        M = df_t_e['m'].values[0].astype(Decimal)
        S = df_t_e['s'].values[0].astype(Decimal)

        if talla > 0:
            Z = (((talla/M) ** L)-1)/(L*S)
    else:
        Z = 100
       # Z=peso/M

    return round(Z, 2)


def ZPeso_Talla(row, df_p_t):
    anio = row['edad_anio']
    dia = row['edad_dia']
    sexo = row['genero']
    talla = row['talla']
    peso = row['peso']
    if anio < 2:
        df_p_t = df_p_t.loc[(df_p_t['talla'] == talla) & (df_p_t['sexo'] == sexo) & (
            df_p_t['grupo'] == '0 - 1'), ['l', 'm', 's']]  # .values[0]
    if anio >= 2 and anio < 5:
        df_p_t = df_p_t.loc[(df_p_t['talla'] == talla) & (df_p_t['sexo'] == sexo) & (
            df_p_t['grupo'] == '2 - 5'), ['l', 'm', 's']]  # .values[0]
    # df_p_e.to_float()
    Z = 0
    if len(df_p_t) > 0:
        L = df_p_t['l'].values[0].astype(Decimal)
        M = df_p_t['m'].values[0].astype(Decimal)
        S = df_p_t['s'].values[0].astype(Decimal)

        if peso > 0:
            Z = (((peso/M) ** L)-1)/(L*S)
        # Z=peso/M
    else:
        Z = 100

    return round(Z, 2)


# Calculo de diagnostico

def Dx_peso_edad(row):
    p_e = row['Zp_e']
    Dx = ''
    if p_e >= -6 and p_e < -2:
        Dx = 'Desnutrición Global'

    if p_e >= -2 and p_e <= 5:
        Dx = 'Normal'

    if p_e < -6 or p_e > 5:
        Dx = 'Excluído: Peso para la talla - Fuera de rango'

    return Dx


def Dx_talla_edad(row):
    t_e = row['Zt_e']
    Dx = ''
    if t_e >= -6 and t_e < -2:
        Dx = 'Desnutrición Crónica'

    if t_e >= -2 and t_e <= 6:
        Dx = 'Normal'

    if t_e < -6 or t_e > 6:
        Dx = 'Excluído: Talla para la Edad - Fuera de rango'

    return Dx


def Dx_peso_talla(row):
    p_t = row['Zp_t']
    Dx = ''
    if p_t >= -5 and p_t < -2:
        Dx = 'Desnutrición Aguda.'

    if p_t >= -2 and p_t < -1:
        Dx = 'Riesgo de Desnutrición Aguda'

    if p_t >= -1 and p_t <= 2:
        Dx = 'Normal'

    if p_t > 2 and p_t <= 3:
        Dx = 'Sobrepeso'

    if p_t > 3 and p_t <= 5:
        Dx = 'Obesidad'

    if p_t < -5 or p_t > 5:
        Dx = 'Excluído: Peso para la talla - fuera de rango'

    return Dx

# Preparar carga para base de datos


def fun_df_observados(Estructura, obs, observados_1):
    df_observados = Estructura[Estructura['id_obs'] != 0]
    
    # ------------------------------------------------------- conservar solo estos codigos ----------------------------------------------------
   # df_observados = df_observados[df_observados['codigo_item'].isin(['99381', '99381.01', '99382', '99383'])]
    df_observados = df_observados.drop( ['Zp_e', 'Zt_e', 'Zp_t', 'Dx_pe', 'Dx_te', 'Dx_pt'], axis=1)
    #obs = obs.rename(columns={'id': 'id_obs', 'descricion': 'diagnostico'})
    ##df_observados.to_excel('observados.xlsx', index=False)
    df_observados = df_observados.merge(obs, how='inner', on=['id_obs'])
    
    
    df_observados = pd.concat([observados_1, df_observados])
  
    #df_observados.to_excel('observados.xlsx', index=False)
    return df_observados


def fun_df_EstadoNutricional(Estructura):
    df_observados = Estructura[Estructura['id_obs'] == 0]
    # df_observados.to_excel('anemia.xlsx', index=False)
    return df_observados


# %%
#conn = MyDatabase2()

# consultas SQLs

#pacientes_covid=consulta_covid(conn)
obs=consulta_observaciones(conn)
peso_edad=consulta_peso_edad(conn)
talla_edad=consulta_talla_edad(conn)
peso_talla=consulta_peso_talla(conn)

# %%
menores_obs=consulta_sql(conn) #0=observados; 1=reporte 

# Filtrar las filas que solo están en `menores_obs`

#obs.head(1)
#Estructura
menores_obs = estructura_df(menores_obs)
observados_obs_1=fun_observados_1(menores_obs,obs)
Estructura_obs=fun_estructura_df(menores_obs)

#calculo Zscore-----------------------------------------
Estructura_obs['Zp_e'] = Estructura_obs.apply(lambda row:ZPeso_edad(row,peso_edad), axis=1)
Estructura_obs['Zt_e'] = Estructura_obs.apply(lambda row:ZTalla_edad(row,talla_edad), axis=1)
Estructura_obs['Zp_t'] = Estructura_obs.apply(lambda row:ZPeso_Talla(row, peso_talla), axis=1)
Estructura_obs['id_obs'] = Estructura_obs.apply(fun_registros_obs, axis=1)

#Estructura.to_excel('obserbados.xlsx', index=False)

#Diagnosticos --------------------------------------------
Estructura_obs['Dx_pe'] = Estructura_obs.apply(Dx_peso_edad, axis=1)
Estructura_obs['Dx_te'] = Estructura_obs.apply(Dx_talla_edad, axis=1)
Estructura_obs['Dx_pt'] = Estructura_obs.apply(Dx_peso_talla, axis=1)
#Estructura.to_excel('obserbados.xlsx', index=False)
Estructura_obs.head(1)

#cargar base de datos 
observados=fun_df_observados(Estructura_obs,obs,observados_obs_1)
#----df_EstadoNutricional=fun_df_EstadoNutricional(Estructura)

# %%
#observados=fun_df_observados(Estructura)
#conn = MyDatabase2()
conn.sql('delete from public.excluidos_5_his;')
print("insertando en tabla de observados ojo antes ejecute Anemia")
d=conn.sqli(observados,'excluidos_5_his')


# %% [markdown]
# <marquee> BAsE DE DATOS</marquee>

# %%
#df_EstadoNutricional.to_excel('nutricional_nenores_5.xlsx', index=False)
#observados.to_excel('observados_ESTADO_NUTRICIONAL.xlsx', index=False)
# Obtener la fecha actual

print("creando excel......")
fecha_actual = datetime.datetime.now()
nombre_mes = {
    1: "Ene", 2: "Feb", 3: "Mar", 4: "Abr",
    5: "May", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Sep", 10: "Oct", 11: "Nov", 12: "Dic"
}

# Obtener el nombre abreviado del mes en español
nombre_mes_abreviado = nombre_mes[fecha_actual.month]

# Formatear la fecha con el nombre abreviado del mes en español
fecha_formateada = fecha_actual.strftime(f"{nombre_mes_abreviado}_%d_%Y")

sql=conn.df("SELECT id_paciente,anio,mes,red,microred,nombre_eess ,fecha_atencion,abrev_tipo_doc,numero_documento,paciente,fecha_nacimiento,edad_anio,edad_mes,edad_dia,genero,peso,talla,hemoglobina,fecha_resultado_hb,descripcion_centro_poblado,cod_eess ,provincia,distrito,personal,registrador,lote,num_pag,codigo_item,id_obs,diagnostico as observaciones FROM excluidos_5_his eh") 
#sql.to_excel(f"observados_EN_A_ene_{fecha_formateada}.xlsx", index=False)
sql.to_excel(f"excel/observados_EN_A.xlsx", index=False)
#observados.to_excel(f"observados_EN_A_ene_{fecha_formateada}2.xlsx", index=False)

# %%



