# %%
#from clases.bd.conexion2 import MyDatabase2
from decouple import config
import pandas as pd
import numpy as np
import os
import glob as gb
import sys
from zipfile import ZipFile
from os import remove



# %%
import gzip
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import psycopg2
host = config('DB_HOST')
user = config('DB_USER')
password = config('DB_PASSWORD')
database = config('DB_DATABASE')
options="-c search_path=maestros"
port = config('DB_PORT')
pgconn = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=database,
    port=port,
    options=options
)
# cursor
pgcursor = pgconn.cursor()
# codigo requerido
pgconn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


# %%
#pgcursor.execute("DELETE FROM nominal_trama_completo WHERE extract(year from fecha_registro) = 2023;")
pgcursor.execute("DELETE FROM maestros.nominal_trama WHERE anio= 2023 AND mes IN (1,2,3,4,5,6,7,8,9,10,11)")

ruta_datos = os.path.abspath("../../../data/2023")
# print(ruta_datos)

# carpeta contenedor de datos Zip
dx = gb.glob(ruta_datos + '/*.zip')
print(dx)

# lista de df
list_df = []
for f in dx:
    ruta = f
    # print(f)
    name = os.path.join(ruta_datos, "csv", os.path.splitext(
        os.path.split(ruta)[1])[0])+'.csv'
    # print(os.path.dirname(f))
   # print(name)
    name2 = ''
    # print(os.path.isdir(name))
    with ZipFile(ruta, 'r') as myzip:
        name2 = os.path.join(ruta_datos, "csv", myzip.infolist()[0].filename)
       # print(name)
       # print(name2)
        if os.path.exists(name):
            remove(name)
        # Extraer archivo
        myzip.extractall(os.path.join(ruta_datos, "csv"))
        print("Archivo Extraido:"+ruta)
        os.rename(name2, name)
        print("Archivo Renombrado:"+name2)

        #pgcursor.execute("DELETE FROM nominal_trama nt WHERE nt.anio ='2023'")
        with open(name, mode='r', encoding="ISO-8859-1") as f:
            next(f)
            pgcursor.copy_from(f, 'nominal_trama',
            #pgcursor.copy_from(f, 'nominal_trama',
                               sep=',', null='', columns=None)
            print("Carga Completada: "+name)
print("Termino el Proceso")


# %%


# %%
# pgcursor.execute("DELETE FROM maestro_paciente")
ruta_maestro = os.path.abspath("../../../data/Maestro")

dx = gb.glob(ruta_maestro + '/*.zip')
print(dx)
# lista de df
list_df = []
for f in dx:
    ruta = f
    # print(f)
    name = os.path.join(ruta_maestro, "csv", os.path.splitext(
        os.path.split(ruta)[1])[0])+'.csv'
    name2 = ''
    # print(os.path.isdir(name))
    with ZipFile(ruta, 'r') as myzip:
        name2 = os.path.join(ruta_maestro, "csv", myzip.infolist()[0].filename)
       # print(name)
       # print(name2)
        if os.path.exists(name):
            remove(name)
        # Extraer archivo
        myzip.extractall(os.path.join(ruta_maestro, "csv"))
        print("Archivo Extraido:"+ruta)
        os.rename(name2, name)
        print("Archivo Renombrado:"+name2)

    # compara el nombre del archivo
    nombre = os.path.splitext(os.path.split(f)[1])[0][0:10]
    match nombre:
        case "MaestroPac":
            pgcursor.execute("TRUNCATE TABLE maestros.maestro_paciente")
            
            with open(name, mode='r', encoding="ISO-8859-1") as f:
                next(f)
                tabla = "maestro_paciente"
                pgcursor.copy_from(f, tabla, sep=',', null='', columns=None)
                print("Carga Completada maestro_paciente:"+name2)
        case "MaestroPer":
            pgcursor.execute("TRUNCATE TABLE maestros.maestro_personal")
            with open(name, mode='r', encoding="ISO-8859-1") as f:
                next(f)
                pgcursor.copy_from(f, 'maestro_personal',
                                   sep=',', null='', columns=None)
                print("Carga Completada maestro_personal:"+name2)
        case "MaestroReg":
            pgcursor.execute("TRUNCATE TABLE maestros.maestro_registrador")
            with open(name, mode='r', encoding="ISO-8859-1") as f:
                next(f)
                pgcursor.copy_from(f, 'maestro_registrador',
                                   sep=',', null='', columns=None)
                print("Carga Completada maestro_registrador:"+name2)
    # print(nombre)
    # print(f)
   


# %%

pgcursor.execute("DELETE FROM trama_vacuna_covid tvc WHERE EXTRACT(YEAR FROM tvc.fecha_registro) = '2023'")

ruta_covid = os.path.abspath("../../../../../HISMINSA/2023")
dx = gb.glob(ruta_covid + '/*.zip')
print(dx)
# lista de df
list_df = []
for f in dx:
    ruta = f
    name = os.path.join(ruta_covid, "csv", os.path.splitext( os.path.split(ruta)[1])[0])+'.csv'
    name2 = ''

    if os.path.exists(name):
        remove(name)
        print("Se elimino: "+name)
    # print(os.path.isdir(name))
    with ZipFile(ruta, 'r') as myzip:
        name2 = os.path.join(ruta_covid, "csv", myzip.infolist()[0].filename)

    # Extraer archivo
        myzip.extractall(os.path.join(ruta_covid, "csv"))
        print("Archivo Extraido:"+ruta)

        os.rename(name2, name)
        print("Archivo Renombrado:"+name2)

        with open(name, mode='r', encoding="ISO-8859-1") as f:
            next(f)
            pgcursor.copy_from(f, 'trama_vacuna_covid', sep=',',null='',columns=None)
            print("Carga Completada:"+name2)
print("Termino el Proceso")

pgcursor.execute("SELECT crea_vacunahisminsa()")
# print(conn2.cur)

# with open(name,mode='r') as f:
#       next(f)
# pgcursor.copy_from(f, 'nominal_trama_completo', sep=',',null='',columns=None)
#      print("Carga Completada:"+name)


# %% [markdown]
# <H1>VACUNAS REGULARES</H1>

# %%
#pgcursor.execute("DELETE FROM trama_vacuna_regular tvc WHERE EXTRACT(YEAR FROM tvc.fecha_registro) = '2023'")

#ruta_covid = os.path.abspath("../../../../../HISMINSA/Covid/2023")
ruta_covid = os.path.abspath("../../../../../HISMINSA/Regular/2023")
dx = gb.glob(ruta_covid + '/*.zip')
print(dx)
# lista de df
list_df = []
for f in dx:
    ruta = f
    name = os.path.join(ruta_covid, "csv", os.path.splitext( os.path.split(ruta)[1])[0])+'.csv'
    name2 = ''

    if os.path.exists(name):
        remove(name)
        print("Se elimino: "+name)
    # print(os.path.isdir(name))
    with ZipFile(ruta, 'r') as myzip:
        name2 = os.path.join(ruta_covid, "csv", myzip.infolist()[0].filename)

    # Extraer archivo
        myzip.extractall(os.path.join(ruta_covid, "csv"))
        print("Archivo Extraido:"+ruta)

        os.rename(name2, name)
        print("Archivo Renombrado:"+name2)

        with open(name, mode='r', encoding="ISO-8859-1") as f:
            next(f)
            pgcursor.copy_from(f, 'trama_vacuna_regular', sep=',',null='',columns=None)
            print("Carga Completada:"+name2)
print("Termino el Proceso") 

#pgcursor.execute("SELECT crea_vacunahisminsa()")



# %%
pgcursor.execute("SELECT * FROM crea_vacunahisminsa() ")
#pgcursor.execute("SELECT maestros.crea_nominal_trama2()")

# %%
pgcursor.execute("TRUNCATE TABLE maestros.nominal_trama_2024")
#ruta_datos = os.path.abspath("../../../data/2023/csv/")
# print(ruta_datos)

# carpeta contenedor de datos Zip
#dx = gb.glob(ruta_datos + '/*.csv')
dx = gb.glob("D:/Irvin/Irvin/Python/data/2024/csv/*.csv")
print(dx)
# 
# lista de df
list_df = []
for f in dx:
    ruta = f
    print(f)  

    #pgcursor.execute("TRUNCATE TABLE maestros.nominal_trama_2023")
    with open(f, mode='r', encoding="ISO-8859-1") as f:
        next(f)
        pgcursor.copy_from(f, 'nominal_trama',    sep=',', null='', columns=None)
    #   print("Carga Completada: "+name)
print("Termino el Proceso")

# %%


# Ruta de la carpeta que contiene archivos .rar
#carpeta_rar = "D:/Irvin/Irvin/Python/data/2023/"

# Filtra los archivos que tienen extensión .rar
#archivos_rar = gb.glob("D:/Irvin/Irvin/Python/data/2023/*.rar")

# Define la carpeta de destino para la extracción
#carpeta_destino = "D:/Irvin/Irvin/Python/data/2023/csv/"








import rarfile
import os
import glob as gb


# Ruta de la carpeta que contiene archivos .rar
carpeta_rar = "D:/Irvin/Irvin/Python/data/2023/"

# Lista todos los archivos en la carpeta
archivos_rar = gb.glob(os.path.join(carpeta_rar, "*.rar"))

# Carpeta de destino para los archivos extraídos
carpeta_destino = "D:/Irvin/Irvin/Python/data/2023/csv"

# Crear la carpeta de destino si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)

# Itera sobre los archivos .rar y los extrae
for archivo_rar in archivos_rar:
    try:
        # Abre el archivo RAR
        with rarfile.RarFile(archivo_rar, 'r') as rar:
            # Extrae los archivos al directorio de destino
            rar.extractall(carpeta_destino)
        print(f"Archivos extraídos de {archivo_rar} a {carpeta_destino}")
    except rarfile.Error as e:
        print(f"Error al extraer {archivo_rar}: {e}")

print("Proceso completado.")




# %%


# %%



