# %%
import pandas as pd
import numpy as np
import os
import glob as gb
import sys

sys.path.insert(0, "../../")
from clases.bd.conexion2 import MyDatabase2
conn = MyDatabase2()
id=0
#from clases.bd.paciente import CPaciente
#cpaciente = CPaciente()


# %%
def filtro(**kwargs):
    #conn = MyDatabase2()
    conn= kwargs['conn']
    id = kwargs['id']
    df_grupo= kwargs['grupo']
    df_codigo= kwargs['codigo']
    df_subgrupo= kwargs['subgrupo']
    df_detalles= kwargs['detalles']
    codigo_item= kwargs['dx1']
    tipo_diagnostico= kwargs['t_dx']

    lista = codigo_item.split(",")
    dx=''
    for indice, elemento in enumerate(lista):
        dx += "'%s'"%elemento if indice==0 else ",'%s'"%elemento
#---------------------Config-------------------------------
    columnas = 'nt.id_cita,nt.anio,nt.mes,nt.fecha_atencion,nt.id_paciente, nt.id_personal, nt.id_registrador,nt.anio_actual_paciente, nt.tipo_diagnostico,nt.codigo_item, mhe.id_establecimiento,mhe.red,mhe.microred,mhe.nombre_establecimiento,mhe.departamento,mhe.distrito,mhe.provincia'
    lab = """, max(CASE  WHEN nt.id_correlativo_lab='1' THEN nt.valor_lab ELSE null END ) AS "lab1", max(CASE  WHEN nt.id_correlativo_lab='2' THEN nt.valor_lab ELSE null END ) AS "lab2", max(CASE  WHEN nt.id_correlativo_lab='3' THEN nt.valor_lab ELSE null END ) AS "lab3" """
    GROUP_by = "GROUP by " + columnas
#---------------------Cabeceras -------------------------------
    df_head = """ '%s' as df_id,'%s' as df_grupo,'%s' as df_codigo,'%s' as df_subgrupo,'%s' as df_detalles,""" % (
        id,df_grupo, df_codigo,df_subgrupo, df_detalles)
#---------------------sql -------------------------------
    sql = """SELECT %s %s %s FROM maestros.nominal_trama nt 
    INNER JOIN maestros.maestro_his_establecimiento mhe ON mhe.id_establecimiento = nt.id_establecimiento  
    WHERE  nt.tipo_diagnostico ='%s' AND codigo_item in(%s)AND nt.anio_actual_paciente BETWEEN 12 AND 17 %s
    """ % (df_head, columnas ,lab,tipo_diagnostico,dx,GROUP_by)
    print(sql)
    sql = conn.df(sql)
  
    
    return sql




# %%
def diagnostico2(**kwargs):
    #conn = MyDatabase2() 
    conn= kwargs['conn']
    codigo_item= kwargs['dx2']
    tipo_diagnostico= kwargs['t_dx2']

    lista = codigo_item.split(",")
    dx=''
    for indice, elemento in enumerate(lista):
        dx += "'%s'"%elemento if indice==0 else ",'%s'"%elemento
#---------------------sql -------------------------------
    sql = """SELECT nt.id_cita,nt.codigo_item as codigo_item_2,nt.tipo_diagnostico tipo_diagnostico_2  , max(CASE WHEN nt.id_correlativo_lab = '1' THEN nt.valor_lab ELSE NULL END ) AS "lab1_2", max(CASE WHEN nt.id_correlativo_lab = '2' THEN nt.valor_lab ELSE NULL END ) AS "lab2_2", max(CASE WHEN nt.id_correlativo_lab = '3' THEN nt.valor_lab ELSE NULL END ) AS "lab3_2"
FROM maestros.nominal_trama nt
WHERE nt.tipo_diagnostico ='%s' AND nt.codigo_item ='%s'
GROUP by  nt.id_cita,nt.codigo_item,nt.tipo_diagnostico 
    """ % (tipo_diagnostico,codigo_item)
   # print(sql)
    sql = conn.df(sql)
  
    
    return sql
def filtro2(**kwargs):
        conn= kwargs['conn']
        id = kwargs['id']
        df_grupo= kwargs['grupo']
        df_codigo= kwargs['codigo']
        df_subgrupo= kwargs['subgrupo']
        df_detalles= kwargs['detalles']
        codigo_item= kwargs['dx1']
        tipo_diagnostico= kwargs['t_dx1']

        codigo_item2= kwargs['dx2']
        tipo_diagnostico2= kwargs['t_dx2']

        df =filtro(id=id,grupo=df_grupo,codigo=df_codigo,subgrupo=df_subgrupo,detalles=df_detalles,dx1=codigo_item,t_dx=tipo_diagnostico,conn=conn)
        df2 = diagnostico2(dx2=codigo_item2,t_dx2=tipo_diagnostico2,conn=conn)
    
        return df.merge(df2, how='inner', on='id_cita')


# %%
#diagnostico2(t_dx2='D',dx2='C8002')
#filtro2=filtro2(id=26,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='C8002',subgrupo='Examen médico general',detalles='',dx1='C8002',t_dx1='D',dx2='99384',t_dx2='D',conn=conn)
#filtro2.head(3)
#filtro2.to_excel('archivo_excel.xlsx', index=False)

Infecciones_Respiratorias_Agudas_Complicadas=filtro(id=id,grupo='MORBILIDAD DEL ADOLESCENTE',codigo='',subgrupo='INFECCIONES RESPIRATORIAS AGUDAS COMPLICADAS',detalles='',dx1='A37,J180,J050,J051,J90,J851,J86,J18,J11,J939,J188,J189,J181,J18,J13,J29,J18,J16,J229,J450,J459,J46,J448,J449',t_dx='D',conn=conn)
print("Carga corecta: Infecciones_Respiratorias_Agudas_Complicadas")


# %% [markdown]
# <center><h1>MORBILIDAD DEL ADOLESCENTE</h1></center>

# %%
id+=1
Infecciones_Respiratorias_Agudas_Complicadas=filtro(id=id,grupo='MORBILIDAD DEL ADOLESCENTE',codigo='',subgrupo='INFECCIONES RESPIRATORIAS AGUDAS COMPLICADAS',detalles='',dx1='A37,J180,J050,J051,J90,J851,J86,J18,J11,J939,J188,J189,J181,J18,J13,J29,J18,J16,J229,J450,J459,J46,J448,J449',t_dx='D',conn=conn)
print("Carga corecta: Infecciones_Respiratorias_Agudas_Complicadas")

# ___------------------------------------------------------------------------------------------------------------------------------------------------
id+=1
Infecciones_Respiratorias_Agudas_No_Complicadas=filtro(id=id,grupo='MORBILIDAD DEL ADOLESCENTE',codigo='',subgrupo='INFECCIONES RESPIRATORIAS AGUDAS NO COMPLICADAS',detalles='',dx1='J020,J0309,H660,H669,J00,J01,J028,J029,J040,J041,J042,J069,J209',t_dx='D',conn=conn)
print("Carga corecta: Infecciones_Respiratorias_Agudas_No_Complicadas")

# ___------------------------------------------------------------------------------------------------------------------------------------------------
id+=1
Enfermedades_diarreicas_Agudas_Complicadas=filtro(id=id,grupo='MORBILIDAD DEL ADOLESCENTE',codigo='',subgrupo='ENFERMEDADES DIARREICAS AGUDAS COMPLICADAS',detalles='',dx1='A018,E86,A011,E86,A018,E86,A013,E86,A029,E86,A020,E86,A05,E86,A07,E86 ,A080,E86 ,A082,E86,A083,E86,A084,E86,A00,E86,A049,E86,A03,E86,A039,E86,A045,E86,A060,E86,A062,E86,A018,R571,A011,R571,A018,R571,A01,R571,A029,R571,A020,R571,A05,R571,A07,R571,A080,R571,A082,R571,A083,R571,A084,R571,A00,R571,A099,R571',t_dx='D',conn=conn)
print("Carga corecta: Enfermedades_diarreicas_Agudas_Complicadas")
# ___------------------------------------------------------------------------------------------------------------------------------------------------

id+=1
Enfermedades_diarreicas_Agudas_No_Complicadas=filtro(id=id,grupo='MORBILIDAD DEL ADOLESCENTE',codigo='',subgrupo='ENFERMEDADES DIARREICAS AGUDAS COMPLICADAS',detalles='',dx1='A018,A011,A018,A013,A029,A020,A05,A07,A080,A082,A083,A084,A00,A049,A030,A039,A045,A060,A062',t_dx='D',conn=conn)
print("Carga corecta: Enfermedades_diarreicas_Agudas_Complicadas")

# ___------------------------------------------------------------------------------------------------------------------------------------------------

df = pd.concat([Infecciones_Respiratorias_Agudas_Complicadas,Infecciones_Respiratorias_Agudas_No_Complicadas,Enfermedades_diarreicas_Agudas_Complicadas,Enfermedades_diarreicas_Agudas_No_Complicadas])
df.head(1)


# %% [markdown]
# <center><h2>CONTROL Y SEGUIMIENTO DE ADOLESCENTES </h2></center>

# %%
id+=1
xd_C8002=filtro2(id=id,grupo='CONTROL Y SEGUIMIENTO DE ADOLESCENTES',codigo='C8002',subgrupo='Plan de Atención Integral de Salud',detalles='',dx1='C8002',t_dx1='D',dx2='99384',t_dx2='D',conn=conn)


# convertir a numeros y eliminar texto
#xd_C8002['lab1'] = pd.to_numeric(xd_C8002['lab1'], errors='coerce')
xd_C8002['lab2'] = pd.to_numeric(xd_C8002['lab2'], errors='coerce')
xd_C8002['lab3'] = pd.to_numeric(xd_C8002['lab3'], errors='coerce')
xd_C8002['lab1_2'] = pd.to_numeric(xd_C8002['lab1_2'], errors='coerce')


# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_C8002_1 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==1) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']==1)]
xd_C8002_1['df_detalles'] = xd_C8002_1['df_detalles'].replace('', 'Iniciado - Paquete Basico - EESS')

# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_C8002_2 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==2) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']==1)]
xd_C8002_2['df_detalles'] = xd_C8002_2['df_detalles'].replace('', 'Iniciado - Paquete Completo - EESS')
xd_C8002_2['df_id'] = xd_C8002_2['df_id'].replace('%s'% id,id + 1)

# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_C8002_3 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==3) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']==1)]
xd_C8002_3['df_detalles'] = xd_C8002_3['df_detalles'].replace('', 'Iniciado - Paquete Especializado- EESS')
xd_C8002_3['df_id'] = xd_C8002_3['df_id'].replace('%s'% id, id + 2)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_C8002_4 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==1) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']==1)]
xd_C8002_4['df_detalles'] = xd_C8002_4['df_detalles'].replace('', 'Iniciado - Paquete Basico - IE')
xd_C8002_4['df_id'] = xd_C8002_4['df_id'].replace('%s'% id, id + 3)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_C8002_5 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==2) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']==1)]
xd_C8002_5['df_detalles'] = xd_C8002_5['df_detalles'].replace('', 'Iniciado - Paquete Completo - IE')
xd_C8002_5['df_id'] = xd_C8002_5['df_id'].replace('%s'% id, id + 4)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_C8002_6 = xd_C8002[(xd_C8002['lab1']=='1') & (xd_C8002['lab2']==3) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']==1)]
xd_C8002_6['df_detalles'] = xd_C8002_6['df_detalles'].replace('', 'Iniciado - Paquete Especializado- IE')
xd_C8002_6['df_id'] = xd_C8002_6['df_id'].replace('%s'% id, id + 5)

# ------------------------------------- Concluido EESS -----------------------------------------------------
xd_C8002_7 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==1) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']>=3)]
xd_C8002_7['df_detalles'] = xd_C8002_7['df_detalles'].replace('', 'Concluido - Paquete Basico - EESS')
xd_C8002_7['df_id'] = xd_C8002_7['df_id'].replace('%s'% id, id + 6)

# ------------------------------------- Concluido EESS -----------------------------------------------------
xd_C8002_8 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==2) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']>=3)]
xd_C8002_8['df_detalles'] = xd_C8002_8['df_detalles'].replace('', 'Concluido - Paquete Completo - EESS')
xd_C8002_8['df_id'] = xd_C8002_8['df_id'].replace('%s'% id, id + 7)

# ------------------------------------- Concluido EESS -----------------------------------------------------
xd_C8002_9 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==3) & (xd_C8002['lab3']==1) & (xd_C8002['lab1_2']>=3)]
xd_C8002_9['df_detalles'] = xd_C8002_9['df_detalles'].replace('', 'Concluido - Paquete Especializado- EESS')
xd_C8002_9['df_id'] = xd_C8002_9['df_id'].replace('%s'% id, id + 8)

# ------------------------------------- Concluido IE -----------------------------------------------------
xd_C8002_10 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==1) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']>=3)]
xd_C8002_10['df_detalles'] = xd_C8002_10['df_detalles'].replace('', 'Concluido - Paquete Basico - IE')
xd_C8002_10['df_id'] = xd_C8002_10['df_id'].replace('%s'% id, id + 9)

# ------------------------------------- Concluido IE -----------------------------------------------------
xd_C8002_11 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==2) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']>=3)]
xd_C8002_11['df_detalles'] = xd_C8002_11['df_detalles'].replace('', 'Concluido - Paquete Completo - IE')
xd_C8002_11['df_id'] = xd_C8002_11['df_id'].replace('%s'% id, id + 10)

# ------------------------------------- Concluido IE -----------------------------------------------------
xd_C8002_12 = xd_C8002[(xd_C8002['lab1']=='TA') & (xd_C8002['lab2']==3) & (xd_C8002['lab3']==2) & (xd_C8002['lab1_2']>=3)]
xd_C8002_12['df_detalles'] = xd_C8002_12['df_detalles'].replace('', 'Concluido - Paquete Especializado- IE')
xd_C8002_12['df_id'] = xd_C8002_12['df_id'].replace('%s'% id, id + 11)

#xd_C8002_7.head(5)
df = pd.concat([df, xd_C8002_1,xd_C8002_2,xd_C8002_3,xd_C8002_4,xd_C8002_5,xd_C8002_6,xd_C8002_7,xd_C8002_8,xd_C8002_9,xd_C8002_10,xd_C8002_11,xd_C8002_12], ignore_index=True)


id+=12
xd_Z003=filtro2(id=id,grupo='CONTROL Y SEGUIMIENTO DE ADOLESCENTES',codigo='Z003',subgrupo='Examen del estado de desarrollo del adolescente',detalles='',dx1='C8002',t_dx1='D',dx2='Z003',t_dx2='D',conn=conn)

xd_Z003['lab2'] = pd.to_numeric(xd_Z003['lab2'], errors='coerce')
xd_Z003['lab3'] = pd.to_numeric(xd_Z003['lab3'], errors='coerce')

# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_Z003_1 = xd_Z003[(xd_Z003['lab2']==1) & (xd_Z003['lab3']==1)]
xd_Z003_1['df_detalles'] = xd_Z003_1['df_detalles'].replace('', 'Paquete Basico - EESS')

# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_Z003_2 = xd_Z003[ (xd_Z003['lab2']==2) & (xd_Z003['lab3']==1)]
xd_Z003_2['df_detalles'] = xd_Z003_2['df_detalles'].replace('', 'Paquete Completo - EESS')
xd_Z003_2['df_id'] = xd_Z003_2['df_id'].replace('%s'% id,id + 1)

# ------------------------------------- Iniciado EESS -----------------------------------------------------
xd_Z003_3 = xd_Z003[(xd_Z003['lab2']==3) & (xd_Z003['lab3']==1)]
xd_Z003_3['df_detalles'] = xd_Z003_3['df_detalles'].replace('', 'Paquete Especializado- EESS')
xd_Z003_3['df_id'] = xd_Z003_3['df_id'].replace('%s'% id, id + 2)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_Z003_4 = xd_Z003[(xd_Z003['lab2']==1) & (xd_Z003['lab3']==2)]
xd_Z003_4['df_detalles'] = xd_Z003_4['df_detalles'].replace('', 'Paquete Basico - IE')
xd_Z003_4['df_id'] = xd_Z003_4['df_id'].replace('%s'% id, id + 3)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_Z003_5 = xd_Z003[(xd_Z003['lab2']==2) & (xd_Z003['lab3']==2) ]
xd_Z003_5['df_detalles'] = xd_Z003_5['df_detalles'].replace('', 'Paquete Completo - IE')
xd_Z003_5['df_id'] = xd_Z003_5['df_id'].replace('%s'% id, id + 4)

# ------------------------------------- Iniciado IE -----------------------------------------------------
xd_Z003_6 = xd_Z003[(xd_Z003['lab2']==3) & (xd_Z003['lab3']==2) ]
xd_Z003_6['df_detalles'] = xd_Z003_6['df_detalles'].replace('', 'Paquete Especializado - IE')
xd_Z003_6['df_id'] = xd_Z003_6['df_id'].replace('%s'% id, id + 5)

df = pd.concat([df, xd_Z003_1,xd_Z003_2,xd_Z003_3,xd_Z003_4,xd_Z003_5,xd_Z003_6], ignore_index=True)

id+=6
xd_99384=filtro2(id=id,grupo='CONTROL Y SEGUIMIENTO DE ADOLESCENTES',codigo='99384',subgrupo='Evaluación Integral del Adolescente',detalles='',dx1='99384',t_dx1='D',dx2='C8002',t_dx2='D',conn=conn)

# convertir a numeros y eliminar texto
xd_99384['lab1'] = pd.to_numeric(xd_99384['lab1'], errors='coerce')
xd_99384['lab2'] = pd.to_numeric(xd_99384['lab2'], errors='coerce')
xd_99384['lab3'] = pd.to_numeric(xd_99384['lab3'], errors='coerce')
#xd_99384['lab1_2'] = pd.to_numeric(xd_99384['lab1_2'], errors='coerce')
xd_99384['lab2_2'] = pd.to_numeric(xd_99384['lab2_2'], errors='coerce')
xd_99384['lab3_2'] = pd.to_numeric(xd_99384['lab3_2'], errors='coerce')

xd_99384_1 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==1)& (xd_99384['lab3_2']==1)]
xd_99384_1['df_detalles'] = xd_99384_1['df_detalles'].replace('', '1º Control - Paquete Basico - EESS')

xd_99384_2 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==2)& (xd_99384['lab3_2']==1)]
xd_99384_2['df_detalles'] = xd_99384_2['df_detalles'].replace('', '1º Control - Paquete Completo - EESS')
xd_99384_2['df_id'] = xd_99384_2['df_id'].replace('%s'% id,id + 1)

xd_99384_3 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==3)& (xd_99384['lab3_2']==1)]
xd_99384_3['df_detalles'] = xd_99384_3['df_detalles'].replace('', '1º Control - Paquete Especializado - EESS')
xd_99384_3['df_id'] = xd_99384_3['df_id'].replace('%s'% id,id + 2)


xd_99384_4 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==1)& (xd_99384['lab3_2']==2)]
xd_99384_4['df_detalles'] = xd_99384_4['df_detalles'].replace('', '1º Control - Paquete Basico - IE')
xd_99384_4['df_id'] = xd_99384_4['df_id'].replace('%s'% id,id + 3)

xd_99384_5 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==2)& (xd_99384['lab3_2']==2)]
xd_99384_5['df_detalles'] = xd_99384_5['df_detalles'].replace('', '1º Control - Paquete Completo - IE')
xd_99384_5['df_id'] = xd_99384_5['df_id'].replace('%s'% id,id + 4)

xd_99384_6 = xd_99384[(xd_99384['lab1']==1) & (xd_99384['lab1_2']=='1') & (xd_99384['lab2_2']==3)& (xd_99384['lab3_2']==2)]
xd_99384_6['df_detalles'] = xd_99384_6['df_detalles'].replace('', '1º Control - Paquete Especializado - IE')
xd_99384_6['df_id'] = xd_99384_6['df_id'].replace('%s'% id,id + 5)

##-------------------------------------------------------------!3º Control a + controles!---------------------------------------------------------

xd_99384_7 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==1)& (xd_99384['lab3_2']==1)]
xd_99384_7['df_detalles'] = xd_99384_7['df_detalles'].replace('', '3º Control a mas - Paquete Basico - EESS')
xd_99384_7['df_id'] = xd_99384_7['df_id'].replace('%s'% id,id + 7)

xd_99384_8 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==2)& (xd_99384['lab3_2']==1)]
xd_99384_8['df_detalles'] = xd_99384_8['df_detalles'].replace('', '3º Control a mas - Paquete Completo - EESS')
xd_99384_8['df_id'] = xd_99384_8['df_id'].replace('%s'% id,id + 8)

xd_99384_9 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==3)& (xd_99384['lab3_2']==1)]
xd_99384_9['df_detalles'] = xd_99384_9['df_detalles'].replace('', '3º Control a mas - Paquete Especializado - EESS')
xd_99384_9['df_id'] = xd_99384_9['df_id'].replace('%s'% id,id + 9)


xd_99384_10 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==1)& (xd_99384['lab3_2']==2)]
xd_99384_10['df_detalles'] = xd_99384_10['df_detalles'].replace('', '3º Control a mas - Paquete Basico - IE')
xd_99384_10['df_id'] = xd_99384_10['df_id'].replace('%s'% id,id + 10)

xd_99384_11 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==2)& (xd_99384['lab3_2']==2)]
xd_99384_11['df_detalles'] = xd_99384_11['df_detalles'].replace('', '3º Control a mas - Paquete Completo - IE')
xd_99384_11['df_id'] = xd_99384_11['df_id'].replace('%s'% id,id + 11)

xd_99384_12 = xd_99384[(xd_99384['lab1']>=3) & (xd_99384['lab1_2']=='TA') & (xd_99384['lab2_2']==3)& (xd_99384['lab3_2']==2)]
xd_99384_12['df_detalles'] = xd_99384_12['df_detalles'].replace('', '3º Control a mas - Paquete Especializado - IE')
xd_99384_12['df_id'] = xd_99384_12['df_id'].replace('%s'% id,id + 12)

id+=12
xd_99384_13=filtro(id=id,grupo='CONTROL Y SEGUIMIENTO DE ADOLESCENTES',codigo='99384',subgrupo='Evaluación Integral del Adolescente',detalles='2º Control',dx1='99384',t_dx='D',conn=conn)
#xd_99384_13.to_excel('xd_99384_13.xlsx',index=False)
xd_99384_13 = xd_99384_13[(xd_99384_13['lab1']=='2')]

##-------------------------------------------------------------!2º Control!---------------------------------------------------------

df = pd.concat([df, xd_99384_1,xd_99384_2,xd_99384_3,xd_99384_4,xd_99384_5,xd_99384_6,xd_99384_13,xd_99384_7,xd_99384_8,xd_99384_9,xd_99384_10,xd_99384_11,xd_99384_12], ignore_index=True)
#Adolescentes.to_csv('Adolescentes.csv')
#xd_Z003.to_excel('xd_Z003.xlsx',index=False)

#35

# %% [markdown]
# <center><h2>VISITA DOMICILIARIA </center></h2>

# %%



# ___------------------------------------------------------------------------------------------------------------------------------------------------
id+=1
Visita_Domiciliaria=filtro(id=id,grupo='VISITA DOMICILIARIA',codigo='C0011',subgrupo='Visita Domiciliaria',detalles='',dx1='C0011',t_dx='D',conn=conn)



Visita_Domiciliaria['lab1'] = pd.to_numeric(
    Visita_Domiciliaria['lab1'], errors='coerce')  # convertir a numeros y eliminar texto
# ------------------------------------- Primera Visita-----------------------------------------------------
Visita_Domiciliaria_1 = Visita_Domiciliaria.loc[Visita_Domiciliaria['lab1'].isin([1])]
Visita_Domiciliaria_1['df_detalles'] = Visita_Domiciliaria_1['df_detalles'].replace('', '1º visita')

# ------------------------------------- segunda Visita-----------------------------------------------------
Visita_Domiciliaria_2 = Visita_Domiciliaria.loc[Visita_Domiciliaria['lab1'].isin([2])]
Visita_Domiciliaria_2['df_detalles'] = Visita_Domiciliaria_2['df_detalles'].replace( '', '2º visita')
Visita_Domiciliaria_2['df_id'] = Visita_Domiciliaria_2['df_id'].replace('%s'% id,id + 1) 

# ------------------------------------- Tercera Visita-----------------------------------------------------
Visita_Domiciliaria_3 = Visita_Domiciliaria.loc[Visita_Domiciliaria['lab1'] >= 3]
Visita_Domiciliaria_3['df_detalles'] = Visita_Domiciliaria_3['df_detalles'].replace('', '3º visita')
Visita_Domiciliaria_3['df_id'] = Visita_Domiciliaria_3['df_id'].replace('%s'% id,id + 2) 

df = pd.concat([df, Visita_Domiciliaria_1,Visita_Domiciliaria_2,Visita_Domiciliaria_3], ignore_index=True)



#------------------------------------------------------------------------------------------------
id+=3
xd_99509=filtro(id=id,grupo='VISITA DOMICILIARIA',codigo='99509',subgrupo='Visita domiciliaria para la ayuda con actividades de la vida diaria y del cuidado personal',detalles='',dx1='99509',t_dx='D',conn=conn)


# convertir a numeros y eliminar texto
xd_99509['lab1'] = pd.to_numeric(xd_99509['lab1'], errors='coerce')
# ------------------------------------- Primera Visita-----------------------------------------------------
xd_99509_1 = xd_99509.loc[xd_99509['lab1'].isin([1])]
xd_99509_1['df_detalles'] = xd_99509_1['df_detalles'].replace('', '1º visita')

# ------------------------------------- segunda Visita-----------------------------------------------------
xd_99509_2 = xd_99509.loc[xd_99509['lab1'].isin([2])]
xd_99509_2['df_detalles'] = xd_99509_2['df_detalles'].replace('', '2º visita')
xd_99509_2['df_id'] = xd_99509_2['df_id'].replace('%s'% id,id + 1) 
# ------------------------------------- Tercera Visita-----------------------------------------------------
xd_99509_3 = xd_99509.loc[xd_99509['lab1'] >= 3]
xd_99509_3['df_detalles'] = xd_99509_3['df_detalles'].replace('', '3º visita')
xd_99509_3['df_id'] = xd_99509_3['df_id'].replace('%s'% id,id + 2) 

df = pd.concat([df, xd_99509_1,xd_99509_2,xd_99509_3], ignore_index=True)

print(xd_99509_1)
# df.info()
#xd_99509.head(1)


# %% [markdown]
# <center><h1>EVALUACIÓN FÍSICA NUTRICIONAL </h1></center>

# %%


id+=3
xd_Z000=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='Z000',subgrupo='Examen médico general',detalles='',dx1='Z000',t_dx='D',conn=conn)
df = pd.concat([df, xd_Z000], ignore_index=True)
##-----------------------------------------------------------------------------------------------------
id+=1
xd_Z019=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='Z019',subgrupo='Valoración de Factores de Riesgo (DNT)',detalles='',dx1='Z019',t_dx='D',conn=conn)
df = pd.concat([df, xd_Z019], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_99209_04=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='99209.04',subgrupo='Evaluación Nutricional Antropometrica (PAB)',detalles='',dx1='99209.04',t_dx='D',conn=conn)

#------------------------------------- Primera -----------------------------------------------------
xd_99209_04_1=xd_99209_04.loc[xd_99209_04['lab1'].isin(['RSM'])]
xd_99209_04_1['df_detalles']=xd_99209_04_1['df_detalles'].replace('','RSM= Riesgo Bajo')
#xd_99209_04_1['df_id'] = xd_99209_04_1['df_id'].replace( '27', '27')

#------------------------------------- segunda -----------------------------------------------------
xd_99209_04_2=xd_99209_04.loc[xd_99209_04['lab1'].isin(['RSA'])]
xd_99209_04_2['df_detalles']=xd_99209_04_2['df_detalles'].replace('','RSA= Riesgo Alto')
xd_99209_04_2['df_id'] = xd_99209_04_2['df_id'].replace('%s'% id,id + 1) 

#------------------------------------- Tercera -----------------------------------------------------
xd_99209_04_3=xd_99209_04.loc[xd_99209_04['lab1'].isin(['RMA'])]
xd_99209_04_3['df_detalles']=xd_99209_04_3['df_detalles'].replace('','RMA= Riesgo Muy Alto')
xd_99209_04_3['df_id'] = xd_99209_04_3['df_id'].replace('%s'% id,id + 2) 

df = pd.concat([df, xd_99209_04_1,xd_99209_04_2,xd_99209_04_3], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=3
xd_E660=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E660',subgrupo='Índice de Masa Corporal (IMC)',detalles='Obesidad debido a exceso de caloría (Sobrepeso)',dx1='E660',t_dx='D',conn=conn)
df = pd.concat([df, xd_E660], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E669=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E669',subgrupo='Índice de Masa Corporal (IMC)',detalles='Obesidad no especificada (Obesidad)',dx1='E669',t_dx='D',conn=conn)
df = pd.concat([df, xd_E669], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_Z006=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='Z006',subgrupo='Índice de Masa Corporal (IMC)',detalles='Peso Normal',dx1='Z006',t_dx='D',conn=conn)
df = pd.concat([df, xd_Z006], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E440=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E440',subgrupo='Índice de Masa Corporal (IMC)',detalles='Desnutrición Proteico Calórica Moderada(Desnutrición Aguda)',dx1='E440',t_dx='D',conn=conn)
df = pd.concat([df, xd_E440], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E43X=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E43X',subgrupo='Índice de Masa Corporal (IMC)',detalles='Desnutrición Proteico Calórica Severa No Especificada',dx1='E43X',t_dx='D',conn=conn)
df = pd.concat([df, xd_E43X], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E344=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E344',subgrupo='Talla/Edad',detalles='Estatura Alta Constitucional (Talla Alta)',dx1='E344',t_dx='D',conn=conn)
df = pd.concat([df, xd_E344], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_Z006=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='Z006',subgrupo='Talla/Edad',detalles='Talla Normal',dx1='E344',t_dx='D',conn=conn)
xd_Z006=xd_Z006.loc[xd_Z006['lab1'].isin(['TE'])]
df = pd.concat([df, xd_Z006], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E45X=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E45X',subgrupo='Talla/Edad',detalles='Desnutrición Crónica T/E (Talla Baja)',dx1='E45X',t_dx='D',conn=conn)
df = pd.concat([df, xd_E45X], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E785=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E785',subgrupo='Hiperlipidemia No Especificada (Dislipidemia)',detalles='',dx1='E785',t_dx='D',conn=conn)
df = pd.concat([df, xd_E785], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_E65X=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='E65X',subgrupo='Adiposidad Localizada',detalles='',dx1='E65X',t_dx='D',conn=conn)
df = pd.concat([df, xd_E65X], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_Z728=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='Z728',subgrupo='Otros Problemas relacionados al Estilo de Vida',detalles='',dx1='Z728',t_dx='D',conn=conn)
df = pd.concat([df, xd_Z728], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_85018=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='85018',subgrupo='Dosaje de Hemoglobina',detalles='',dx1='85018,85018.01',t_dx='D',conn=conn)
xd_85018=xd_85018.loc[xd_85018['lab1'].isin(['1'])]
df = pd.concat([df, xd_85018], ignore_index=True)

##-----------------------------------------------------------------------------------------------------
id+=1
xd_99199_26=filtro(id=id,grupo='EVALUACIÓN FÍSICA NUTRICIONAL',codigo='99199.26',subgrupo='Suplementación de sulfato ferroso y ácido fólico',detalles='',dx1='99199.26',t_dx='D',conn=conn)
df = pd.concat([df, xd_99199_26], ignore_index=True)

print(id)



# %% [markdown]
# <CENTER> <H2>EVALUACIÓN DE LA AGUDEZA VISUAL</H2><CENTER>

# %%
id+=1
xd_99173=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA VISUAL',codigo='99173',subgrupo='Prueba  de la Agudeza Visual cuantitativa bilateral',detalles='',dx1='99173',t_dx='D',conn=conn)
id+=1
xd_Z010=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA VISUAL',codigo='Z010',subgrupo='Examen de Ojos y de la Visión (Normal)',detalles='',dx1='Z010',t_dx='D',conn=conn)
xd_Z010=xd_Z010.loc[xd_Z010['lab1'].isin(['N'])]
id+=1
xd_Z010_1=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA VISUAL',codigo='Z010',subgrupo='Examen de Ojos y de la Visión (Anormal)',detalles='',dx1='Z010',t_dx='D',conn=conn)
xd_Z006_1=xd_Z010_1.loc[xd_Z010_1['lab1'].isin(['A'])]
id+=1
xd_99401_16=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA VISUAL',codigo='99401.16',subgrupo='Consejería ocular',detalles='',dx1='99401.16',t_dx='D',conn=conn)


df = pd.concat([df, xd_99173,xd_Z010,xd_Z010_1,xd_99401_16], ignore_index=True)



# %% [markdown]
# <CENTER> <H2>EVALUACIÓN DE LA AGUDEZA AUDITIVA</H2><CENTER>

# %%
id+=1
dx_H919=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA AUDITIVA',codigo='H919',subgrupo='Disminución de la Agudeza Auditiva sin Especificación',detalles='',dx1='H919',t_dx='D',conn=conn)
id+=1
dx_Z011=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA AUDITIVA',codigo='Z011',subgrupo='Examen de Oídos y de la Audición (Normal)',detalles='',dx1='Z011',t_dx='D',conn=conn)
dx_Z011=dx_Z011.loc[dx_Z011['lab1'].isin(['N'])]
id+=1
dx_Z011_2=filtro(id=id,grupo='EVALUACIÓN DE LA AGUDEZA AUDITIVA',codigo='Z011',subgrupo='Examen de Oídos y de la Audición (Anormal)',detalles='',dx1='Z011',t_dx='D',conn=conn)
dx_Z011_2=dx_Z011_2.loc[dx_Z011_2['lab1'].isin(['A'])]

df = pd.concat([df, dx_H919,dx_Z011,dx_Z011_2], ignore_index=True)

# %% [markdown]
# <CENTER> <H2>ATENCIÓN ODONTOLÓGICA</H2><CENTER>

# %%
id+=1
dx_D1310=filtro(id=id,grupo='ATENCIÓN ODONTOLÓGICA',codigo='D1310',subgrupo='Asesoría Nutricional para el Control de Enfermedades Dentales',detalles='',dx1='D1310',t_dx='D',conn=conn)
id+=1
dx_D1330=filtro(id=id,grupo='ATENCIÓN ODONTOLÓGICA',codigo='D1330',subgrupo='Instrucción de Higiene Oral',detalles='',dx1='D1310',t_dx='D',conn=conn)
id+=1
dx_D0120=filtro(id=id,grupo='ATENCIÓN ODONTOLÓGICA',codigo='D0120',subgrupo='Examen Estomatológico',detalles='',dx1='D0120',t_dx='D',conn=conn)
id+=1
dx_D1110=filtro(id=id,grupo='ATENCIÓN ODONTOLÓGICA',codigo='D1110',subgrupo='Profilaxis Dental',detalles='',dx1='D1110',t_dx='D',conn=conn)
id+=1
dx_U510=filtro(id=id,grupo='ATENCIÓN ODONTOLÓGICA',codigo='U510',subgrupo='Alta Básica Odontológica',detalles='',dx1='U510',t_dx='D',conn=conn)
df = pd.concat([df, dx_D1310,dx_D1330,dx_D0120,dx_D1110,dx_U510], ignore_index=True)

# %% [markdown]
# <CENTER> <H2>EVALUACIÓN FÍSICO POSTURAL</H2><CENTER>

# %%
id+=1
dx_96008=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='96008',subgrupo='Análisis Postural Estático ( NORMAL)',detalles='',dx1='96008',t_dx='D',conn=conn)
id+=1
dx_M400=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='M400',subgrupo='Cifosis postural',detalles='',dx1='M400',t_dx='D',conn=conn)
id+=1
dx_M402=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='M402',subgrupo='Otras cifosis y las no especificadas',detalles='',dx1='M402',t_dx='D',conn=conn)
id+=1
dx_M403=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='M403',subgrupo='Síndrome de espalda plana',detalles='',dx1='M403',t_dx='D',conn=conn)
id+=1
dx_M405=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='M405',subgrupo='Lordosis no especifica',detalles='',dx1='M405',t_dx='D',conn=conn)
id+=1
dx_M419=filtro(id=id,grupo='EVALUACIÓN FÍSICO POSTURAL',codigo='M419',subgrupo='Escoliosis, no especificada',detalles='',dx1='M419',t_dx='D',conn=conn)

df = pd.concat([df, dx_96008,dx_M400,dx_M402,dx_M403,dx_M405,dx_M419], ignore_index=True)

# %% [markdown]
# <CENTER> <H2>INMUNIZACIONES</H2><CENTER>

# %%
id+=1
dx_90746=filtro(id=id,grupo='INMUNIZACIONES',codigo='90746',subgrupo='Vacunación Antihepatitis Viral B (HvB)',detalles='',dx1='90746',t_dx='D',conn=conn)
#------------------------------------- Primera -----------------------------------------------------
dx_90746_1=dx_90746.loc[dx_90746['lab1'].isin(['1'])]
dx_90746_1['df_detalles']=dx_90746_1['df_detalles'].replace('','DOSIS 1')

#------------------------------------- segunda -----------------------------------------------------
dx_90746_2=dx_90746.loc[dx_90746['lab1'].isin(['2'])]
dx_90746_2['df_detalles']=dx_90746_2['df_detalles'].replace('','DOSIS 2')
dx_90746_2['df_id'] = dx_90746_2['df_id'].replace('%s'% id,id + 1) 

#------------------------------------- Tercera -----------------------------------------------------
dx_90746_3=dx_90746.loc[dx_90746['lab1'].isin(['3'])]
dx_90746_3['df_detalles']=dx_90746_3['df_detalles'].replace('','DOSIS 3')
dx_90746_3['df_id'] = dx_90746_3['df_id'].replace('%s'% id,id + 2) 

df = pd.concat([df, dx_90746_1,dx_90746_2,dx_90746_3], ignore_index=True)

id+=3
dx_90658=filtro(id=id,grupo='INMUNIZACIONES',codigo='90658',subgrupo='Vacuna contra la Influenza  (Estacional)',detalles='',dx1='90658',t_dx='D',conn=conn)
df = pd.concat([df, dx_90658], ignore_index=True)


id+=1
dx_90714=filtro(id=id,grupo='INMUNIZACIONES',codigo='90714',subgrupo='Vacunación Diftotetánica (dT) (Incluye varones y gestantes)',detalles='',dx1='90714',t_dx='D',conn=conn)
#------------------------------------- Primera -----------------------------------------------------
dx_90714_1=dx_90714.loc[dx_90714['lab1'].isin(['1'])]
dx_90714_1['df_detalles']=dx_90714_1['df_detalles'].replace('','DOSIS 1')

#------------------------------------- segunda -----------------------------------------------------
dx_90714_2=dx_90714.loc[dx_90714['lab1'].isin(['2'])]
dx_90714_2['df_detalles']=dx_90714_2['df_detalles'].replace('','DOSIS 2')
dx_90714_2['df_id'] = dx_90714_2['df_id'].replace('%s'% id,id + 1) 

#------------------------------------- Tercera -----------------------------------------------------
dx_90714_3=dx_90714.loc[dx_90714['lab1'].isin(['3'])]
dx_90714_3['df_detalles']=dx_90714_3['df_detalles'].replace('','DOSIS 3')
dx_90714_3['df_id'] = dx_90714_3['df_id'].replace('%s'% id,id + 2) 

df = pd.concat([df, dx_90714_1,dx_90714_2,dx_90714_3], ignore_index=True)

id+=3
dx_90649=filtro(id=id,grupo='INMUNIZACIONES',codigo='90649',subgrupo='Vacunación Diftotetánica (dT) (Incluye varones y gestantes)',detalles='',dx1='90649',t_dx='D',conn=conn)
#------------------------------------- Primera -----------------------------------------------------
dx_90649_1=dx_90649.loc[dx_90649['lab1'].isin(['1'])]
dx_90649_1['df_detalles']=dx_90649_1['df_detalles'].replace('','DOSIS 1')

#------------------------------------- segunda -----------------------------------------------------
dx_90649_2=dx_90649.loc[dx_90649['lab1'].isin(['2'])]
dx_90649_2['df_detalles']=dx_90649_2['df_detalles'].replace('','DOSIS 2')
dx_90649_2['df_id'] = dx_90649_2['df_id'].replace('%s'% id,id + 1) 
df = pd.concat([df, dx_90746_1,dx_90649_1,dx_90649_2], ignore_index=True)

id+=2
dx_90717=filtro(id=id,grupo='INMUNIZACIONES',codigo='90717',subgrupo='Vacuna Antiamarilica',detalles='',dx1='90717',t_dx='D',conn=conn)
df = pd.concat([df, dx_90717], ignore_index=True)


# %% [markdown]
# <CENTER> <H2>EVALUACIÓN DEL DESARROLLO SEXUAL/ PLANIFICACIÓN FAMILIAR</H2><CENTER>

# %%
grupo='EVALUACIÓN DEL DESARROLLO SEXUAL/ PLANIFICACIÓN FAMILIAR'
id+=1
dx_99384_02=filtro(id=id,grupo=grupo,codigo='99384.02',subgrupo='Evaluación del desarrollo sexual según Tanner.',detalles='',dx1='99384.02',t_dx='D',conn=conn)
df = pd.concat([df, dx_99384_02], ignore_index=True)
id+=1
dx_99208=filtro(id=id,grupo=grupo,codigo='99208',subgrupo='Atención en Planificación Familiar y SSR',detalles='',dx1='99208',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208], ignore_index=True)
id+=1
dx_58300=filtro(id=id,grupo=grupo,codigo='3',subgrupo='DIU',detalles='',dx1='58300',t_dx='D',conn=conn)
df = pd.concat([df, dx_58300], ignore_index=True)
id+=1
dx_99208_13=filtro(id=id,grupo=grupo,codigo='4',subgrupo='HORMONAL',detalles='ORAL COMBINADO',dx1='99208.13',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_13], ignore_index=True)
id+=1
dx_99208_05=filtro(id=id,grupo=grupo,codigo='5',subgrupo='HORMONAL',detalles='INYECTABLE TRIMESTRAL',dx1='99208.05',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_05], ignore_index=True)
id+=1
dx_99208_04=filtro(id=id,grupo=grupo,codigo='6',subgrupo='HORMONAL',detalles='INYECTABLE MENSUAL',dx1='99208.04',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_04], ignore_index=True)
id+=1
dx_11975=filtro(id=id,grupo=grupo,codigo='7',subgrupo='HORMONAL',detalles='IMPLANTE',dx1='11975',t_dx='D',conn=conn)
df = pd.concat([df, dx_11975], ignore_index=True)
id+=1
dx_99208_02=filtro(id=id,grupo=grupo,codigo='8',subgrupo='BARRERA',detalles='CONDON MASCULINO',dx1='99208.02',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_02], ignore_index=True)
id+=1
dx_99208_06=filtro(id=id,grupo=grupo,codigo='9',subgrupo='BARRERA',detalles='CONDON FEMENINO',dx1='99208.06',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_06], ignore_index=True)
id+=1
dx_99208_07=filtro(id=id,grupo=grupo,codigo='10',subgrupo='MELA',detalles='',dx1='99208.07',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_07], ignore_index=True)
id+=1
dx_99208_09=filtro(id=id,grupo=grupo,codigo='11',subgrupo='ABSTINECIA PERIODICA',detalles='BILLINGS',dx1='99208.09',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_09], ignore_index=True)
id+=1
dx_99208_08=filtro(id=id,grupo=grupo,codigo='12',subgrupo='ABSTINECIA PERIODICA',detalles='RITMO',dx1='99208.08',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_08], ignore_index=True)
id+=1
dx_99208_10=filtro(id=id,grupo=grupo,codigo='13',subgrupo='ABSTINECIA PERIODICA',detalles='DIAS FIJO',dx1='99208.10',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_10], ignore_index=True)
id+=1
dx_99208_11=filtro(id=id,grupo=grupo,codigo='14',subgrupo='ADMINISTRACIÓN Y USO AOE/ YUSPE',detalles='Administración y uso de anticonceptivo oral de emergencia/YUZPE',dx1='99208.11',t_dx='D',conn=conn)
df = pd.concat([df, dx_99208_11], ignore_index=True)



# %% [markdown]
# <CENTER> <H2>EVALUACIÓN CLÍNICA</H2><CENTER>

# %%
grupo='EVALUACIÓN CLÍNICA'
id+=1
dx_L709 =filtro(id=id,grupo=grupo,codigo='L709 ',subgrupo='Acné no especificado',detalles='',dx1='L709',t_dx='D',conn=conn)
id+=1
dx_D509 =filtro(id=id,grupo=grupo,codigo='D509 ',subgrupo=' Anemia por Deficiencia de Hierro sin Especificación',detalles='',dx1='D509',t_dx='D',conn=conn)
id+=1
dx_O990 =filtro(id=id,grupo=grupo,codigo='O990 ',subgrupo=' Anemia que afecta al Embarazo, Parto o Puerperio',detalles='',dx1='O990',t_dx='D',conn=conn)
id+=1
dx_U310 =filtro(id=id,grupo=grupo,codigo='U310 ',subgrupo=' Administración de Tratamiento (Anemia)',detalles='',dx1='U310',t_dx='D',conn=conn)
id+=1
dx_J459 =filtro(id=id,grupo=grupo,codigo='J459 ',subgrupo=' Asma No Especificada',detalles='',dx1='J459',t_dx='D',conn=conn)
id+=1
dx_I10X =filtro(id=id,grupo=grupo,codigo='I10X ',subgrupo='Hipertensión Esencial Primaria (Hipertensión Arterial)',detalles='',dx1='I10X',t_dx='D',conn=conn)
id+=1
dx_R51X =filtro(id=id,grupo=grupo,codigo='R51X ',subgrupo=' Cefalea',detalles='',dx1='R51X',t_dx='D',conn=conn)
id+=1
dx_B829 =filtro(id=id,grupo=grupo,codigo='B829 ',subgrupo=' Parasitosis Intestinal, sin otra Especificación',detalles='',dx1='B829',t_dx='D',conn=conn)
id+=1
dx_N63X =filtro(id=id,grupo=grupo,codigo='N63X ',subgrupo=' Masa No Especificada en la Mama',detalles='',dx1='N63X',t_dx='D',conn=conn)
id+=1
dx_A084 =filtro(id=id,grupo=grupo,codigo='A084 ',subgrupo=' Infección Intestinal Viral, Sin Otra Especificación',detalles='',dx1='A084',t_dx='D',conn=conn)
id+=1
dx_B373 =filtro(id=id,grupo=grupo,codigo='B373 ',subgrupo=' Candidiasis de la Vulva y de la Vagina (Candidiasis Vaginal) ',detalles='',dx1='B373',t_dx='D',conn=conn)
id+=1
dx_B853 =filtro(id=id,grupo=grupo,codigo='B853 ',subgrupo=' Pediculosis del Pubis ',detalles='',dx1='B853',t_dx='D',conn=conn)
id+=1
dx_B968 =filtro(id=id,grupo=grupo,codigo='B968 ',subgrupo=' Vaginosis Bacteriana',detalles='',dx1='B968',t_dx='D',conn=conn)
id+=1
dx_A64X9 =filtro(id=id,grupo=grupo,codigo='A64X9 ',subgrupo=' Síndrome de Flujo Vaginal',detalles='',dx1='A64X9',t_dx='D',conn=conn)
id+=1
dx_A64X5 =filtro(id=id,grupo=grupo,codigo='A64X5 ',subgrupo=' Síndrome de Dolor Abdominal Bajo',detalles='',dx1='A64X5',t_dx='D',conn=conn)
id+=1
dx_A64X6 =filtro(id=id,grupo=grupo,codigo='A64X6',subgrupo=' Síndrome de Secreción Uretral',detalles='',dx1='A64X6',t_dx='D',conn=conn)
id+=1
dx_99199_11 =filtro(id=id,grupo=grupo,codigo='99199.11 ',subgrupo=' Administración de Tratamiento (ITS)',detalles='',dx1='99199.11',t_dx='D',conn=conn)

df = pd.concat([df, dx_L709,dx_D509,dx_O990,dx_U310,dx_J459,dx_I10X,dx_R51X,dx_B829,dx_N63X,dx_A084,dx_B373,dx_B853,dx_B968,dx_A64X9,dx_A64X5,dx_A64X6,dx_99199_11], ignore_index=True)



# %% [markdown]
# <h2>CONSEJERÍAS</h2>

# %%
grupo='CONSEJERÍAS'
id+=1
dx_99401 =filtro(id=id,grupo=grupo,codigo='99401',subgrupo='Consejería Integral',detalles='',dx1='99401',t_dx='D',conn=conn)
id+=1
dx_99402_03 =filtro(id=id,grupo=grupo,codigo='99402.03',subgrupo='Consejería/Orientación en Salud Sexual y Reproductiva',detalles='',dx1='99402.03',t_dx='D',conn=conn)
id+=1
dx_99402_04 =filtro(id=id,grupo=grupo,codigo='99402.04',subgrupo='Orientación/Consejería en Planificación Familiar',detalles='',dx1='99402.04',t_dx='D',conn=conn)
id+=1
dx_99402_09 =filtro(id=id,grupo=grupo,codigo='99402.09',subgrupo='Consejería de Prevención en Riesgos de Salud Mental',detalles='',dx1='99402.09',t_dx='D',conn=conn)
id+=1
dx_99401_31 =filtro(id=id,grupo=grupo,codigo='99401.31',subgrupo='Consejería en Prevención de Enfermedades No transmisibles (Salud Física)',detalles='',dx1='99401.31',t_dx='D',conn=conn)
id+=1
dx_99403 =filtro(id=id,grupo=grupo,codigo='99403',subgrupo='Consejería Nutricional',detalles='',dx1='99403',t_dx='D',conn=conn)
id+=1
dx_99403_01 =filtro(id=id,grupo=grupo,codigo='99403.01',subgrupo='Consejería Nutricional: Alimentación Saludable',detalles='',dx1='99403.01',t_dx='D',conn=conn)
id+=1
dx_99401_33 =filtro(id=id,grupo=grupo,codigo='99401.33',subgrupo='Consejería Pre Test para VIH',detalles='',dx1='99401.33',t_dx='D',conn=conn)
id+=1
dx_99401_34 =filtro(id=id,grupo=grupo,codigo='99401.34',subgrupo='Consejería Post Test para VIH - Resultado No Reactivo',detalles='',dx1='99401.34',t_dx='D',conn=conn)
id+=1
dx_99403_03 =filtro(id=id,grupo=grupo,codigo='99403.03',subgrupo='Consejería Post Test para VIH - Resultado Reactivo',detalles='',dx1='99403.03',t_dx='D',conn=conn)
id+=1
dx_99402_05 =filtro(id=id,grupo=grupo,codigo='99402.05',subgrupo='Consejería/Orientación en Prevención de ITS, VIH, Hepatitis',detalles='',dx1='99402.05',t_dx='D',conn=conn)
id+=1
dx_99401_24 =filtro(id=id,grupo=grupo,codigo='99401.24',subgrupo='Consejerìa higiene de manos',detalles='',dx1='99401.24',t_dx='D',conn=conn)
id+=1
dx_99401_19 =filtro(id=id,grupo=grupo,codigo='99401.19',subgrupo='Consejerìa para el autocuidado',detalles='',dx1='99401.19',t_dx='D',conn=conn)


df = pd.concat([df, dx_99401,dx_99402_03,dx_99402_04,dx_99402_09,dx_99401_31,dx_99403,dx_99403_01,dx_99401_33,dx_99401_34,dx_99403_03,dx_99402_05,dx_99401_24,dx_99401_19], ignore_index=True)



# %% [markdown]
# <h2>TOMA DE PRUEBA RÁPIDA (en caso lo requiera) PARA LA DETECCIÓN DE </h2>

# %%
grupo='TOMA DE PRUEBA RÁPIDA (en caso lo requiera) PARA LA DETECCIÓN DE'
id+=1
dx_86703 =filtro(id=id,grupo=grupo,codigo='86703',subgrupo='Anticuerpos; HIV-1 y HIV-2, análisis único (Tamizaje de VIH por Prueba Rápida)',detalles='',dx1='86703,86703.01',t_dx='D',conn=conn)
id+=1
dx_86780 =filtro(id=id,grupo=grupo,codigo='86780',subgrupo='Anticuerpo: Treponema Pallidum(Tamizaje de Sífilis por Prueba Rápida)',detalles='',dx1='86780,86780.01',t_dx='D',conn=conn)
id+=1
dx_86593 =filtro(id=id,grupo=grupo,codigo='86593',subgrupo='Prueba de Sífilis; anticuerpo no treponémico cuantitativa ()',detalles='',dx1='86593',t_dx='D',conn=conn)
id+=1
dx_87342 =filtro(id=id,grupo=grupo,codigo='87342',subgrupo='Tamizaje de Hepatitis B por Prueba Rápida',detalles='',dx1='87342',t_dx='D',conn=conn)
id+=1
dx_Z320 =filtro(id=id,grupo=grupo,codigo='Z320',subgrupo='Embarazo No Confirmado: toma de pruebas para el descarte de embarazo en caso la adolescente lo requiera. (examen de orina)',detalles='',dx1='Z320',t_dx='D',conn=conn)

df = pd.concat([df, dx_86703,dx_86780,dx_86593,dx_87342,dx_Z320], ignore_index=True)


# %% [markdown]
# <h2>SALUD MENTAL</h2>

# %%
grupo='SALUD MENTAL'
id+=1
dx_96150 =filtro(id=id,grupo=grupo,codigo='96150',subgrupo='Entrevista de Tamizaje',detalles='',dx1='96150',t_dx='D',conn=conn)
id+=1
dx_F419 =filtro(id=id,grupo=grupo,codigo='F419',subgrupo='Trastorno de ansiedad, no Especificado',detalles='',dx1='F419',t_dx='D',conn=conn)
id+=1
dx_Z553 =filtro(id=id,grupo=grupo,codigo='Z553',subgrupo=' Problemas Relacionados con el Bajo Rendimiento Escolar',detalles='',dx1='Z553',t_dx='D',conn=conn)
id+=1
dx_Z7281 =filtro(id=id,grupo=grupo,codigo='Z7281',subgrupo='Riesgos de Lesiones o Accidentes',detalles='',dx1='Z7281',t_dx='D',conn=conn)
id+=1
dx_96150_01 =filtro(id=id,grupo=grupo,codigo='96150.01',subgrupo='Entrevista de Tamizaje',detalles='Tamizaje de Salud Mental en Violencia',dx1='96150.01',t_dx='D',conn=conn)
id+=1
dx_96150_02 =filtro(id=id,grupo=grupo,codigo='96150.02',subgrupo='Entrevista de Tamizaje',detalles='Tamizaje de Salud Mental en Alcohol y Drogas',dx1='96150.02',t_dx='D',conn=conn)
id+=1
dx_96150_03 =filtro(id=id,grupo=grupo,codigo='96150.03',subgrupo='Entrevista de Tamizaje',detalles='Tamizaje de Salud Mental en Trastornos Depresivos',dx1='96150.03',t_dx='D',conn=conn)
id+=1
dx_96150_04 =filtro(id=id,grupo=grupo,codigo='96150.04',subgrupo='Entrevista de Tamizaje',detalles='Tamizaje de Salud Mental en Psicosis',dx1='96150.04',t_dx='D',conn=conn)
id+=1
dx_96150_05 =filtro(id=id,grupo=grupo,codigo='96150.05',subgrupo='Entrevista de Tamizaje',detalles='Tamizaje de Salud Mental en Habilidades Sociales (Aplicación de cuestinario de habilidades sociales)',dx1='96150.05',t_dx='D',conn=conn)

df = pd.concat([df, dx_96150,dx_F419,dx_Z553,dx_Z7281,dx_96150_01,dx_96150_02,dx_96150_03,dx_96150_04,dx_96150_05])



# %% [markdown]
# <h2>TAMIZAJES POSITIVOS</h2>

# %%
grupo='TAMIZAJES POSITIVOS'
id+=1
dx_R456 =filtro(id=id,grupo=grupo,codigo='R456',subgrupo='Problemas relacionados con violencia',detalles='',dx1='R456',t_dx='D',conn=conn)
id+=1
dx_Z734 =filtro(id=id,grupo=grupo,codigo='Z734',subgrupo='Problemas Relacionados con Habilidades Sociales Inadecuadas',detalles='',dx1='Z734',t_dx='D',conn=conn)
id+=1
dx_Z720 =filtro(id=id,grupo=grupo,codigo='Z720',subgrupo='Problemas Relacionados con el Uso de Tabaco',detalles='',dx1='Z720',t_dx='D',conn=conn)
id+=1
dx_Z721 =filtro(id=id,grupo=grupo,codigo='Z721',subgrupo='Problemas Sociales Relacionados con el Uso de Alcohol',detalles='',dx1='Z721',t_dx='D',conn=conn)
id+=1
dx_Z722 =filtro(id=id,grupo=grupo,codigo='Z722',subgrupo='Problemas Sociales Relacionados con el Uso de drogas',detalles='',dx1='Z722',t_dx='D',conn=conn)
id+=1
dx_Z726 =filtro(id=id,grupo=grupo,codigo='Z726',subgrupo='Problemas relacionados con el Juego y las apuestas',detalles='',dx1='Z726',t_dx='D',conn=conn)
id+=1
dx_Z619 =filtro(id=id,grupo=grupo,codigo='Z619',subgrupo='Problemas Relacionados con Experiencia Negativa no Especificada en la Infancia',detalles='',dx1='Z619',t_dx='D',conn=conn)
id+=1
dx_Z639 =filtro(id=id,grupo=grupo,codigo='Z639',subgrupo=' Otros Problemas Relacionados con el Grupo Primario de Apoyo, Inclusive  Circunstancias Familiares (Relaciones Familiares)',detalles='',dx1='Z639',t_dx='D',conn=conn)

df = pd.concat([df, dx_R456,dx_Z734,dx_Z720,dx_Z721,dx_Z722,dx_Z726,dx_Z619,dx_Z639])





# %% [markdown]
# <h2>ACTIVIDADES PREVENTIVAS PROMOCIONALES</h2>

# %%
grupo='ACTIVIDADES PREVENTIVAS PROMOCIONALES'
id+=1
dx_C0009 =filtro(id=id,grupo=grupo,codigo='C0009',subgrupo='Sesión Educativa',detalles='',dx1='C0009',t_dx='D',conn=conn)
id+=1
dx_C2111_02 =filtro(id=id,grupo=grupo,codigo='C2111.02',subgrupo='Taller en prevención de conducta de riesgo en adolescentes y sus familias - familias fuertes amor y límite',detalles='',dx1='C2111.02',t_dx='D',conn=conn)
#Falta integras los APP
id+=1
dx_C1021 =filtro(id=id,grupo=grupo,codigo='C1021',subgrupo=' Organización de Charla para Abogacía y Políticas Públicas',detalles='',dx1='C1021',t_dx='D',conn=conn)
id+=1
dx_C0007 =filtro(id=id,grupo=grupo,codigo='C0007',subgrupo='Taller para la  familia',detalles='',dx1='C0007',t_dx='D',conn=conn)
id+=1
dx_C0010 =filtro(id=id,grupo=grupo,codigo='C0010',subgrupo='Sesión Demostrativa',detalles='',dx1='C0010',t_dx='D',conn=conn)
id+=1
dx_90872 =filtro(id=id,grupo=grupo,codigo='90872',subgrupo='Taller de habilidades sociales',detalles='',dx1='90872',t_dx='D',conn=conn)
id+=1
dx_C0008 =filtro(id=id,grupo=grupo,codigo='C0008',subgrupo='Taller para personal de salud',detalles='',dx1='C0008',t_dx='D',conn=conn)
id+=1
dx_C2121_01 =filtro(id=id,grupo=grupo,codigo='C2121.01',subgrupo='Animación Sociocultural',detalles='',dx1='C2121.01',t_dx='D',conn=conn)
id+=1
dx_C2121 =filtro(id=id,grupo=grupo,codigo='C2121',subgrupo='Teatros Populares',detalles='',dx1='C2121',t_dx='D',conn=conn)
id+=1
dx_C3152 =filtro(id=id,grupo=grupo,codigo='C3152',subgrupo='Formación de Educadores de Pares',detalles='',dx1='C3152',t_dx='D',conn=conn)
id+=1
dx_C7001 =filtro(id=id,grupo=grupo,codigo='C7001',subgrupo='Monitoreo',detalles='',dx1='C7001',t_dx='D',conn=conn)
id+=1
dx_C7002 =filtro(id=id,grupo=grupo,codigo='C7002',subgrupo='Supervisión',detalles='',dx1='C7002',t_dx='D',conn=conn)
id+=1
dx_C7003 =filtro(id=id,grupo=grupo,codigo='C7003',subgrupo='Evaluación',detalles='',dx1='C7003',t_dx='D',conn=conn)
id+=1
dx_C7004 =filtro(id=id,grupo=grupo,codigo='C7004',subgrupo='Asistencia Técnica',detalles='',dx1='C7004',t_dx='D',conn=conn)


df = pd.concat([df,dx_C0009,dx_C2111_02,dx_C7004,dx_C7003,dx_C7002,dx_C7001,dx_C3152,dx_C2121,dx_C2121_01,dx_C0008,dx_90872,dx_C0010,dx_C0007,dx_C1021])

# %% [markdown]
# 

# %%
grupo='SESIONES EDUCATIVAS - Salud Física Nutricional'
dx_C0009=filtro(id=id,grupo=grupo,codigo='C0009',subgrupo='Actividad Física',detalles='',dx1='C0009',t_dx='D',conn=conn)
#------------------------------------- Primera -----------------------------------------------------
dx_C0009['lab1'] = pd.to_numeric(dx_C0009['lab1'], errors='coerce')

 
dx_C0009_1=dx_C0009[(dx_C0009['lab1']==1) &(dx_C0009['lab2']=='PSA')]
dx_C0009_1['df_subgrupo'] = 'Actividad Física y deporte'
dx_C0009_2=dx_C0009[(dx_C0009['lab1']==2) &(dx_C0009['lab2']=='PSA')]
dx_C0009_2['df_subgrupo'] = 'Alimentación Saludable'
dx_C0009_3=dx_C0009[(dx_C0009['lab1']==3) &(dx_C0009['lab2']=='PSA')]
dx_C0009_3['df_subgrupo'] = 'Higiene'
dx_C0009_4=dx_C0009[(dx_C0009['lab1']==4) &(dx_C0009['lab2']=='PSA')]
dx_C0009_4['df_subgrupo'] = 'Prvención de transtornos posturales '
dx_C0009_5=dx_C0009[(dx_C0009['lab1']==5) &(dx_C0009['lab2']=='PSA')]
dx_C0009_5['df_subgrupo'] = 'Protección solar'
dx_C0009_6=dx_C0009[(dx_C0009['lab1']==6) &(dx_C0009['lab2']=='PSA')]
dx_C0009_6['df_subgrupo'] = 'Salud Bucal '
dx_C0009_7=dx_C0009[(dx_C0009['lab1']==7) &(dx_C0009['lab2']=='PSA')]
dx_C0009_7['df_subgrupo'] = 'Salud ocular'
dx_C0009_8=dx_C0009[(dx_C0009['lab1']==8) &(dx_C0009['lab2']=='PSA')]
dx_C0009_8['df_subgrupo'] = 'Salud respiratoria y tuberculosis: Higiene y salud bucal.'
dx_C0009_9=dx_C0009[(dx_C0009['lab1']==9) &(dx_C0009['lab2']=='PSA')]
dx_C0009_9['df_subgrupo'] = 'Cuidado del medio ambiente'
dx_C0009_10=dx_C0009[(dx_C0009['lab1']==10) &(dx_C0009['lab2']=='PSA')]
dx_C0009_10['df_subgrupo'] = 'Prevención de enfermedades transmisibles prevalentes: Dengue, malaria, bartonellosis etc.'
dx_C0009_11=dx_C0009[(dx_C0009['lab1']==11) &(dx_C0009['lab2']=='PSA')]
dx_C0009_11['df_subgrupo'] = 'Medidas de Seguridad y prevención de accidentes '
dx_C0009_12=dx_C0009[(dx_C0009['lab1']==12) &(dx_C0009['lab2']=='PSA')]
dx_C0009_12['df_subgrupo'] = 'Primeros auxilios. Resucitación cardiopulmonar'
df = pd.concat([df,dx_C0009_1,dx_C0009_2,dx_C0009_3,dx_C0009_4,dx_C0009_5,dx_C0009_6,dx_C0009_7,dx_C0009_8,dx_C0009_9,dx_C0009_10,dx_C0009_11,dx_C0009_12])

dx_C0009_1

# %% [markdown]
# <CENTER> <H2> CARGA BD</H2><CENTER>

# %%


conn = MyDatabase2()
conn.sql('delete from public.adolescentes_his;')
d=conn.sqli(df,'adolescentes_his')
conn.close()
print(d)

# %%
#df.to_excel('Adolescentes_Ene_Jul_2023.xlsx', index=False)
df.to_csv('Adolescentes_Ene_Jul_2023.csv', index=False) 


